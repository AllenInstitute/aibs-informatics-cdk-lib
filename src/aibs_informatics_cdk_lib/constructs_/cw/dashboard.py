import re
from collections import defaultdict
from copy import deepcopy
from math import ceil
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_sns as sns

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins
from aibs_informatics_cdk_lib.constructs_.cw.types import (
    GroupedGraphMetricConfig,
    to_comparison_operator,
)


class DashboardMixins(EnvBaseConstructMixins):
    @property
    def dashboard(self) -> cw.Dashboard:
        return self._dashboard

    @dashboard.setter
    def dashboard(self, dashboard: cw.Dashboard) -> None:
        self._dashboard = dashboard

    def add_graphs(
        self,
        grouped_metric_configs: List[GroupedGraphMetricConfig],
        namespace: str,
        period: cdk.Duration,
        alarm_id_discriminator: str,
        alarm_topic: Optional[sns.Topic],
        dimensions: Dict[str, Any],
    ) -> None:
        """Adds graphs to a dashboard based on metrics configs.

        Args:
            grouped_metric_configs (List[GroupedGraphMetricConfig]): config
            namespace (str): default namespace for metrics, can be overridden in configs
            period (cdk.Duration): default duration
            alarm_id_discriminator (str): a non token disciminator for alarms
            alarm_name_discriminator (str): common name alarm discriminator
            alarm_topic (Optional[sns.Topic]): optional SNS topic to send alarm notifications
            dimensions (Dict[str, Any]): dimensions to generate metrics from
        """
        # First, calculate widths dynamically
        MAX_PER_ROW = 4
        TOTAL_WIDTH = 24
        grouped_metric_configs = deepcopy(grouped_metric_configs)
        for idx in range(0, len(grouped_metric_configs), MAX_PER_ROW):
            grouped_metric_configs_subset = grouped_metric_configs[idx : idx + MAX_PER_ROW]
            requested_widget_widths = [_.get("width", 0) for _ in grouped_metric_configs_subset]
            remaining_width = TOTAL_WIDTH - sum(requested_widget_widths)
            widgets_without_width = sum([_ == 0 for _ in requested_widget_widths])
            if not remaining_width or not widgets_without_width:
                continue
            default_widget_width = remaining_width // widgets_without_width
            for grouped_metric_config in grouped_metric_configs_subset:
                grouped_metric_config["width"] = default_widget_width

        # Next generate the graph widgets and alarms
        graph_widgets, metric_alarms = self.create_widgets_and_alarms(
            grouped_metric_configs=grouped_metric_configs,
            namespace=namespace,
            period=period,
            alarm_id_discriminator=alarm_id_discriminator,
            alarm_topic=alarm_topic,
            dimensions=dimensions,
        )

        for idx in range(0, len(graph_widgets), MAX_PER_ROW):
            self.dashboard.add_widgets(*graph_widgets[idx : idx + MAX_PER_ROW])
        if metric_alarms:
            max_alarms_per_row = 6  # This is how many fit with full screen (improve me)
            num_alarms = len(metric_alarms)
            alarm_widget_height = ceil(ceil(num_alarms // max_alarms_per_row) * 1.5)
            self.dashboard.add_widgets(
                cw.AlarmStatusWidget(
                    alarms=metric_alarms,
                    height=alarm_widget_height,
                    width=24,
                )
            )

    def create_widgets_and_alarms(
        self,
        grouped_metric_configs: List[GroupedGraphMetricConfig],
        namespace: str,
        period: cdk.Duration,
        alarm_id_discriminator: str,
        alarm_topic: Optional[sns.Topic],
        dimensions: Dict[str, Any],
    ) -> Tuple[List[cw.IWidget], List[cw.IAlarm]]:
        """Create graph widgets and alarms from configs

        Args:
            grouped_metric_configs (List[GroupedGraphMetricConfig]): configs
            namespace (str): default metric namespace
            period (cdk.Duration): default duration
            alarm_name_discriminator (str): alarm discriminator name
            alarm_topic (Optional[sns.Topic]): optional sns topic for alarms
            dimensions (Dict[str, Any]):

        Returns:
            Tuple[List[cw.IWidget], List[cw.IAlarm]]: List of widgets and list of alarms
        """
        self_stack = cdk.Stack.of(self.dashboard)

        graph_widgets: List[cw.IWidget] = []
        metric_alarms: List[cw.IAlarm] = []
        for grouped_metric_config in grouped_metric_configs:
            lr_graph_metrics: Dict[Literal["left", "right"], List[cw.Metric]] = defaultdict(list)
            lr_annotations: Dict[
                Literal["left", "right"], List[cw.HorizontalAnnotation]
            ] = defaultdict(list)

            graph_metric_namespace = grouped_metric_config.get("namespace", namespace)
            graph_dimension_map = {**dimensions, **grouped_metric_config.get("dimension_map", {})}
            for metric_config in grouped_metric_config["metrics"]:
                if isinstance(metric_config["metric"], (cw.Metric, cw.MathExpression)):
                    graph_metric = metric_config["metric"]
                    metric_label = metric_config.get("label", graph_metric.label)
                    if isinstance(graph_metric, cw.Metric):
                        metric_name = graph_metric.metric_name
                    else:
                        metric_name = graph_metric.label or metric_config["statistic"]
                else:
                    metric = metric_config["metric"]
                    if isinstance(metric, cw.Metric):
                        metric_name = metric.metric_name
                    else:
                        metric_name = str(metric)
                    metric_label = metric_config.get(
                        "label",
                        re.sub(
                            r"([a-z])([A-Z])",
                            r"\1 \2",
                            re.sub(r"([A-Z])([a-z])", r" \1\2", metric_name.replace(".", " ")),
                        ),
                    )

                    metric_expression = metric_config.get("metric_expression")
                    if metric_expression:
                        graph_metric = cw.MathExpression(
                            expression=metric_expression,
                            using_metrics=metric_config.get("using_metrics", {}),
                            label=metric_label,
                        )
                    else:
                        graph_metric = cw.Metric(
                            metric_name=metric_name,
                            namespace=metric_config.get("namespace", graph_metric_namespace),
                            label=metric_label,
                            statistic=metric_config["statistic"],
                            period=period,
                            dimensions_map={
                                **graph_dimension_map,
                                **metric_config.get("dimension_map", {}),
                            },
                            unit=metric_config.get("unit"),
                        )

                metric_axis = metric_config.get("axis_side", "left")
                lr_graph_metrics[metric_axis].append(graph_metric)  # type: ignore # MathExpression implements IMetric

                metric_alarm_config = metric_config.get("alarm")
                if metric_alarm_config:
                    alarm_name = metric_alarm_config["name"]
                    alarm = graph_metric.create_alarm(
                        self_stack,
                        self.get_construct_id(alarm_name, alarm_id_discriminator),
                        # TODO: every time a change is made to these alarms, Cfn throws an error
                        #       for trying to modify what is a custom resource. So instead, let
                        #       the name be autogenerated.
                        # alarm_name=alarm_name,
                        alarm_description=f"Alarm for {alarm_name}",
                        threshold=metric_alarm_config["threshold"],
                        evaluation_periods=metric_alarm_config["evaluation_periods"],
                        datapoints_to_alarm=metric_alarm_config["datapoints_to_alarm"],
                        comparison_operator=to_comparison_operator(
                            metric_alarm_config["comparison_operator"]
                        ),
                    )
                    lr_annotations[metric_axis].append(
                        cw.HorizontalAnnotation(
                            value=metric_alarm_config["threshold"],
                            color=graph_metric.color,
                        )
                    )
                    metric_alarms.append(alarm)
                    if alarm_topic:
                        alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))  # type: ignore # SnsAction implements IAlarmAction

            graph_widgets.append(
                cw.GraphWidget(
                    title=grouped_metric_config["title"],
                    left=lr_graph_metrics["left"],
                    left_annotations=lr_annotations["left"],
                    left_y_axis=grouped_metric_config.get("left_y_axis"),
                    right=lr_graph_metrics["right"],
                    right_annotations=lr_annotations["right"],
                    right_y_axis=grouped_metric_config.get("right_y_axis"),
                    height=grouped_metric_config.get("height", 10),
                    width=grouped_metric_config.get("width"),
                )
            )
        return graph_widgets, metric_alarms

    def add_text_widget(
        self,
        header: str,
        header_level: Optional[int],
        body: Optional[str] = None,
        height: int = 2,
        width: int = 24,
    ):
        self.dashboard.add_widgets(
            self.build_text_widget(
                header=header, header_level=header_level, body=body, height=height, width=width
            )
        )

    @classmethod
    def build_text_widget(
        cls,
        header: str,
        header_level: Optional[int] = None,
        body: Optional[str] = None,
        height: int = 2,
        width: int = 24,
    ) -> cw.TextWidget:
        markdown = f"{'#' * max(min(header_level, 6), 1) if header_level else ''} {header}"
        if body:
            markdown += f"\n{body}\n"
        return cw.TextWidget(markdown=markdown, height=height, width=width)


class EnhancedDashboard(DashboardMixins, cw.Dashboard):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        dashboard_name: str,
        **dashboard_kwargs,
    ) -> None:
        self.env_base = env_base
        super().__init__(
            scope,
            id or self.get_name_with_env(dashboard_name),
            dashboard_name=self.get_name_with_env(dashboard_name),
            **dashboard_kwargs,
        )
        self.dashboard = self


class DashboardTools(DashboardMixins):
    def __init__(self, dashboard: cw.Dashboard) -> None:
        self.dashboard = dashboard
