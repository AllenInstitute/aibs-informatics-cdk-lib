from typing import List, Optional, Tuple, Union, cast

from aibs_informatics_core.env import EnvBase, ResourceNameBaseEnum
from aws_cdk import Duration
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from aibs_informatics_cdk_lib.common.aws.core_utils import build_sfn_arn
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.cw import (
    AlarmMetricConfig,
    DashboardTools,
    EnhancedDashboard,
    GraphMetricConfig,
    GroupedGraphMetricConfig,
)


class MonitoringConstruct(EnvBaseConstruct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
    ) -> None:
        super().__init__(scope, id, env_base)
        self.monitoring_name = self.construct_id
        self.notify_on_alarms = None
        self._alarm_topic = None

    @property
    def monitoring_name(self) -> str:
        return self._monitoring_name

    @monitoring_name.setter
    def monitoring_name(self, value: str):
        self._monitoring_name = value

    @property
    def notify_on_alarms(self) -> bool:
        if self._notify_on_alarms is None:
            return self.is_test_or_prod
        return self._notify_on_alarms

    @notify_on_alarms.setter
    def notify_on_alarms(self, value: bool):
        self._notify_on_alarms = value

    @property
    def alarm_topic(self) -> sns.Topic:
        if self._alarm_topic is None:
            self._alarm_topic = sns.Topic(
                self, self.get_construct_id(self.monitoring_name, "alarm-topic")
            )
        return self._alarm_topic

    def create_dashboard(
        self, start: Optional[str] = "-P1W", end: Optional[str] = None
    ) -> EnhancedDashboard:
        return EnhancedDashboard(
            self,
            f"{self.monitoring_name}-dashboard",
            self.env_base,
            dashboard_name=self.get_name_with_env(self.monitoring_name, "Dashboard"),
            start=start,
            end=end,
        )

    def add_function_widgets(
        self,
        group_name: str,
        *function_names_groups: Union[Tuple[str, List[str]], List[str]],
        include_min_max_duration: bool = False,
    ):
        self.dashboard_tools.add_text_widget(f"{group_name} Lambda Functions", 1)

        function_names_groups: List[Tuple[str, List[str]]] = [
            cast(Tuple[str, List[str]], _)
            if isinstance(_, tuple) and len(_) == 2 and isinstance(_[-1], list)
            else ("/".join(_), _)
            for _ in function_names_groups
        ]

        for sub_group_name, function_names in function_names_groups:
            grouped_invocation_metrics: List[GraphMetricConfig] = []
            grouped_error_metrics: List[GraphMetricConfig] = []
            grouped_timing_metrics: List[GraphMetricConfig] = []

            for idx, raw_function_name in enumerate(function_names):
                function_name = self.get_name_with_env(raw_function_name)
                fn = lambda_.Function.from_function_name(
                    self, f"{function_name}-from-name", function_name=function_name
                )
                dimension_map = {"FunctionName": function_name}

                grouped_invocation_metrics.append(
                    GraphMetricConfig(
                        metric="Invocations",
                        statistic="Sum",
                        dimension_map=dimension_map,
                        label=f"{raw_function_name} Count",
                    )
                )

                grouped_error_metrics.append(
                    GraphMetricConfig(
                        metric="Errors",
                        statistic="Sum",
                        dimension_map=dimension_map,
                        label=f"{raw_function_name} Errors",
                    )
                )
                grouped_error_metrics.append(
                    GraphMetricConfig(
                        metric="Availability",
                        statistic="Average",
                        dimension_map=dimension_map,
                        label=f"{raw_function_name} %",
                        metric_expression=f"100 - 100 * errors_{idx} / MAX([errors_{idx}, invocations_{idx}])",
                        using_metrics={
                            f"errors_{idx}": fn.metric_errors(),
                            f"invocations_{idx}": fn.metric_invocations(),
                        },
                        axis_side="right",
                    )
                )

                grouped_timing_metrics.append(
                    GraphMetricConfig(
                        metric="Duration",
                        statistic="Average",
                        dimension_map=dimension_map,
                        label=f"{raw_function_name} Avg",
                    )
                )
                if include_min_max_duration:
                    grouped_timing_metrics.append(
                        GraphMetricConfig(
                            metric="Duration",
                            statistic="Minimum",
                            dimension_map=dimension_map,
                            label=f"{raw_function_name} Min",
                        )
                    )
                    grouped_timing_metrics.append(
                        GraphMetricConfig(
                            metric="Duration",
                            statistic="Maximum",
                            dimension_map=dimension_map,
                            label=f"{raw_function_name} Max",
                        )
                    )

            grouped_metrics: List[GroupedGraphMetricConfig] = [
                GroupedGraphMetricConfig(
                    title="Function Invocations", metrics=grouped_invocation_metrics
                ),
                GroupedGraphMetricConfig(
                    title="Function Successes / Failures", metrics=grouped_error_metrics
                ),
                GroupedGraphMetricConfig(
                    title="Function Duration",
                    namespace="AWS/Lambda",
                    metrics=grouped_timing_metrics,
                ),
            ]
            self.dashboard_tools.add_text_widget(sub_group_name, 2)

            self.dashboard_tools.add_graphs(
                grouped_metric_configs=grouped_metrics,
                namespace="AWS/Lambda",
                period=Duration.minutes(5),
                alarm_id_discriminator=sub_group_name,
                alarm_topic=self.alarm_topic if self.notify_on_alarms else None,
                dimensions={},
            )

    def add_state_machine_widgets(
        self,
        group_name: str,
        *grouped_state_machine_names: Union[Tuple[str, List[str]], List[str]],
    ):
        grouped_state_machine_names: List[Tuple[str, List[str]]] = [
            cast(Tuple[str, List[str]], _)
            if isinstance(_, tuple) and len(_) == 2 and isinstance(_[-1], list)
            else ("/".join(_), _)
            for _ in grouped_state_machine_names
        ]
        self.dashboard_tools.add_text_widget(f"{group_name} State Machines", 1)

        for sub_group_name, raw_state_machine_names in grouped_state_machine_names:
            grouped_invocation_metrics: List[GraphMetricConfig] = []
            grouped_error_metrics: List[GraphMetricConfig] = []
            grouped_timing_metrics: List[GraphMetricConfig] = []

            for idx, raw_state_machine_name in enumerate(raw_state_machine_names):
                state_machine_name = self.get_state_machine_name(raw_state_machine_name)
                state_machine_arn = self.get_state_machine_arn(raw_state_machine_name)

                state_machine = sfn.StateMachine.from_state_machine_name(
                    self, f"{state_machine_name}-from-name", state_machine_name
                )

                dimension_map = {"StateMachineArn": state_machine_arn}
                grouped_invocation_metrics.append(
                    GraphMetricConfig(
                        metric="ExecutionsSucceeded",
                        label=f"{raw_state_machine_name} Completed",
                        statistic="Sum",
                        dimension_map=dimension_map,
                    )
                )
                grouped_invocation_metrics.append(
                    GraphMetricConfig(
                        metric="ExecutionsStarted",
                        label=f"{raw_state_machine_name} Started",
                        statistic="Sum",
                        dimension_map=dimension_map,
                    )
                )
                grouped_error_metrics.append(
                    GraphMetricConfig(
                        metric="ExecutionErrors",
                        statistic="Sum",
                        label=f"{raw_state_machine_name} Errors",
                        dimension_map=dimension_map,
                        metric_expression=(
                            f"failed_{idx} + aborted_{idx} + timed_out_{idx} + throttled_{idx}"
                        ),
                        using_metrics={
                            f"failed_{idx}": state_machine.metric_failed(),
                            f"aborted_{idx}": state_machine.metric_aborted(),
                            f"timed_out_{idx}": state_machine.metric_timed_out(),
                            f"throttled_{idx}": state_machine.metric_throttled(),
                        },
                        alarm=AlarmMetricConfig(
                            name=f"{raw_state_machine_name}-errors",
                            threshold=1,
                            evaluation_periods=3,
                            datapoints_to_alarm=1,
                            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                        ),
                    ),
                )

                grouped_timing_metrics.append(
                    GraphMetricConfig(
                        metric="ExecutionTime",
                        statistic="Average",
                        label=f"{raw_state_machine_name} Time",
                        dimension_map=dimension_map,
                        metric_expression=f"time_sec_{idx} / 1000 / 60",
                        using_metrics={f"time_sec_{idx}": state_machine.metric_time()},
                    ),
                )

            grouped_metrics = [
                GroupedGraphMetricConfig(
                    title="Execution Invocations", metrics=grouped_invocation_metrics
                ),
                GroupedGraphMetricConfig(title="Execution Errors", metrics=grouped_error_metrics),
                GroupedGraphMetricConfig(
                    title="Execution Time",
                    metrics=grouped_timing_metrics,
                    left_y_axis=cw.YAxisProps(label="Time (min)"),
                ),
            ]

            self.dashboard_tools.add_text_widget(sub_group_name, 2)
            self.dashboard_tools.add_graphs(
                grouped_metric_configs=grouped_metrics,
                namespace="AWS/States",
                period=Duration.minutes(5),
                alarm_id_discriminator=sub_group_name,
                alarm_topic=self.alarm_topic,
                dimensions={},
            )

    def get_state_machine_name(self, name: Union[str, ResourceNameBaseEnum]) -> str:
        if isinstance(name, ResourceNameBaseEnum):
            return name.get_name(self.env_base)
        else:
            return self.env_base.get_state_machine_name(name)

    def get_state_machine_arn(self, name) -> str:
        state_machine_name = self.get_state_machine_name(name)
        return build_sfn_arn(resource_type="stateMachine", resource_id=state_machine_name)


class ResourceMonitoring(MonitoringConstruct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        notify_on_alarms: Optional[bool] = None,
    ) -> None:
        super().__init__(scope, id, env_base)
        self.notify_on_alarms = notify_on_alarms
        if self.notify_on_alarms:
            sns.Subscription(
                self,
                self.get_construct_id(f"{id}-alarm-subscription"),
                topic=self.alarm_topic,
                endpoint="marmotdev@alleninstitute.org",
                protocol=sns.SubscriptionProtocol("EMAIL"),
            )

        self.dashboard = self.create_dashboard(start="-P1D")
