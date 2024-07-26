from dataclasses import dataclass
from typing import List, Optional

from aibs_informatics_core.env import EnvBase
from attr import field
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.cw.types import (
    AlarmMetricConfig,
    GraphMetricConfig,
    GroupedGraphMetricConfig,
)


@dataclass
class LambdaFunctionMetricConfigGenerator:
    lambda_function: lambda_.IFunction
    lambda_function_name: str = field(default=None)
    dimension_map: dict = field(init=False)

    def __post_init__(self):
        if self.lambda_function_name is None:
            self.lambda_function_name = self.lambda_function.function_name

        self.dimension_map = {"FunctionName": self.lambda_function_name}

    def get_invocations_metric(
        self,
        name_override: Optional[str] = None,
    ) -> GraphMetricConfig:
        return GraphMetricConfig(
            metric="Invocations",
            label=f"{name_override or self.lambda_function_name} Invocations",
            statistic="Sum",
            dimension_map=self.dimension_map,
        )

    def get_errors_metric(
        self,
        name_override: Optional[str] = None,
        discriminator: Optional[str] = None,
        include_alarm: bool = False,
        alarm_threshold: int = 1,
        alarm_evaluation_periods: int = 3,
        alarm_datapoints_to_alarm: int = 1,
    ) -> GraphMetricConfig:
        name = name_override or self.lambda_function_name
        idx = discriminator or "0"
        config = GraphMetricConfig(
            metric="Errors",
            statistic="Sum",
            label=f"{name} Errors",
            dimension_map=self.dimension_map,
        )
        if include_alarm:
            config["alarm"] = AlarmMetricConfig(
                name=f"{name} Errors Alarm {idx}",
                threshold=alarm_threshold,
                evaluation_periods=alarm_evaluation_periods,
                datapoints_to_alarm=alarm_datapoints_to_alarm,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            )
        return config

    def get_availability_metric(
        self,
        name_override: Optional[str] = None,
        discriminator: Optional[str] = None,
    ) -> GraphMetricConfig:
        name = name_override or self.lambda_function_name
        idx = discriminator or "0"

        return GraphMetricConfig(
            metric="Availability",
            statistic="Average",
            dimension_map=self.dimension_map,
            label=f"{name} %",
            metric_expression=f"100 - 100 * errors_{idx} / MAX([errors_{idx}, invocations_{idx}])",
            using_metrics={
                f"errors_{idx}": self.lambda_function.metric_errors(),
                f"invocations_{idx}": self.lambda_function.metric_invocations(),
            },
        )

    def get_duration_avg_metric(
        self,
        name_override: Optional[str] = None,
    ) -> GraphMetricConfig:
        name = name_override or self.lambda_function_name
        return GraphMetricConfig(
            metric="Duration",
            statistic="Average",
            dimension_map=self.dimension_map,
            label=f"{name} Avg",
        )

    def get_duration_max_metric(
        self,
        name_override: Optional[str] = None,
    ) -> GraphMetricConfig:
        name = name_override or self.lambda_function_name
        return GraphMetricConfig(
            metric="Duration",
            statistic="Maximum",
            dimension_map=self.dimension_map,
            label=f"{name} Max",
        )

    def get_duration_min_metric(
        self,
        name_override: Optional[str] = None,
    ) -> GraphMetricConfig:
        name = name_override or self.lambda_function_name
        return GraphMetricConfig(
            metric="Duration",
            statistic="Minimum",
            dimension_map=self.dimension_map,
            label=f"{name} Min",
        )

    def get_duration_metric_group(
        self,
        name_override: Optional[str] = None,
        title: Optional[str] = None,
        include_min_max_duration: bool = False,
    ) -> GroupedGraphMetricConfig:
        name = name_override or self.lambda_function_name

        avg = self.get_duration_avg_metric(name_override)
        if include_min_max_duration:
            min_ = self.get_duration_min_metric(name_override)
            max_ = self.get_duration_max_metric(name_override)

        return GroupedGraphMetricConfig(
            title=title or f"{name} Duration",
            namespace="AWS/Lambda",
            metrics=[avg, min_, max_],
        )

    def get_success_failure_metrics(
        self,
        name_override: Optional[str] = None,
        success_as_percent: bool = True,
    ) -> List[GraphMetricConfig]:
        name = name_override or self.lambda_function_name

        failures = self.get_errors_metric(name)
        if success_as_percent:
            success = self.get_availability_metric(name)
        else:
            success = self.get_invocations_metric(name)
        success["axis_side"] = "right"
        failures["axis_side"] = "left"
        return [success, failures]

    def get_success_failure_metric_group(
        self,
        name_override: Optional[str] = None,
        title: Optional[str] = None,
        success_as_percent: bool = True,
    ) -> GroupedGraphMetricConfig:
        name = name_override or self.lambda_function_name

        failures = self.get_errors_metric(name_override)
        if success_as_percent:
            success = self.get_availability_metric(name_override)
        else:
            success = self.get_invocations_metric(name_override)
        success["axis_side"] = "right"
        failures["axis_side"] = "left"

        return GroupedGraphMetricConfig(
            title=title or f"{name} Invocations",
            namespace="AWS/Lambda",
            metrics=[success, failures],
        )
