from dataclasses import dataclass
from typing import Literal, Optional

from aibs_informatics_core.env import EnvBase
from attr import field
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.cw.types import AlarmMetricConfig, GraphMetricConfig

SFN_TIME_UNITS = Literal["hours", "minutes", "seconds", "milliseconds"]


@dataclass
class StateMachineMetricConfigGenerator:
    state_machine: sfn.IStateMachine
    state_machine_name: str
    dimension_map: dict = field(init=False)

    def __post_init__(self):
        self.dimension_map = {"StateMachineArn": self.state_machine.state_machine_arn}

    def get_execution_completion_metric(
        self, name_override: Optional[str] = None
    ) -> GraphMetricConfig:
        """get the execution completion metric for the state machine

        Args:
            name_override (Optional[str], optional): override for name used.
                Defaults to None.

        Returns:
            GraphMetricConfig
        """
        return GraphMetricConfig(
            metric="ExecutionsSucceeded",
            label=f"{name_override or self.state_machine_name} Completed",
            statistic="Sum",
            dimension_map=self.dimension_map,
        )

    def get_execution_invocations_metric(
        self, name_override: Optional[str] = None
    ) -> GraphMetricConfig:
        """get the execution invocations metric for the state machine

        Args:
            name_override (Optional[str], optional): override for name used.
                Defaults to None.

        Returns:
            GraphMetricConfig
        """
        return GraphMetricConfig(
            metric="ExecutionsStarted",
            label=f"{name_override or self.state_machine_name} Started",
            statistic="Sum",
            dimension_map=self.dimension_map,
        )

    def get_execution_failures_metric(
        self,
        name_override: Optional[str] = None,
        discriminator: Optional[str] = None,
        alarm_threshold: int = 1,
        alarm_evaluation_periods: int = 3,
        alarm_datapoints_to_alarm: int = 1,
    ) -> GraphMetricConfig:
        """get the execution failures metric for the state machine

        Args:
            name_override (Optional[str], optional): override for name used.
                Defaults to state machine name.
            discriminator (Optional[str], optional): Required if grouping with other metric configs that specify the same metric math.
                Defaults to "0".
            alarm_threshold (int, optional): Alarm threshold used. Defaults to 1.
            alarm_evaluation_periods (int, optional): Alarm evaluation periods. Defaults to 3.
            alarm_datapoints_to_alarm (int, optional): Alarm datapoints to alarm. Defaults to 1.

        Returns:
            GraphMetricConfig: _description_
        """
        name = name_override or self.state_machine_name
        idx = discriminator or "0"
        return GraphMetricConfig(
            metric="ExecutionErrors",
            statistic="Sum",
            label=f"{name} Errors",
            dimension_map=self.dimension_map,
            metric_expression=(
                f"failed_{idx} + aborted_{idx} + timed_out_{idx} + throttled_{idx}"
            ),
            using_metrics={
                f"failed_{idx}": self.state_machine.metric_failed(),
                f"aborted_{idx}": self.state_machine.metric_aborted(),
                f"timed_out_{idx}": self.state_machine.metric_timed_out(),
                f"throttled_{idx}": self.state_machine.metric_throttled(),
            },
            alarm=AlarmMetricConfig(
                name=f"{name}-errors",
                threshold=alarm_threshold,
                evaluation_periods=alarm_evaluation_periods,
                datapoints_to_alarm=alarm_datapoints_to_alarm,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            ),
        )

    def get_execution_timing_metric(
        self,
        name_override: Optional[str] = None,
        discriminator: Optional[str] = None,
        time_unit: SFN_TIME_UNITS = "minutes",
    ) -> GraphMetricConfig:
        """get the execution time metric for the state machine

        Args:
            name_override (Optional[str], optional): override for name used.
                Defaults to state machine name.
            discriminator (Optional[str], optional): Required if grouping with other metric configs that specify the same metric math.
                Defaults to "0".
            time_unit (SFN_TIME_UNITS, optional): unit of time to use for metric.
                Defaults to "minutes".

        Returns:
            GraphMetricConfig
        """
        name = name_override or self.state_machine_name
        idx = discriminator or "0"
        if time_unit == "seconds":
            divisor = " / 1000"
        elif time_unit == "minutes":
            divisor = " / 1000 / 60"
        elif time_unit == "hours":
            divisor = " / 1000 / 60 / 60"
        else:
            divisor = ""

        return GraphMetricConfig(
            metric="ExecutionTime",
            statistic="Average",
            label=f"{name} Execution Time",
            dimension_map=self.dimension_map,
            metric_expression=f"time_msec_{idx} {divisor}",
            using_metrics={f"time_msec_{idx}": self.state_machine.metric_time()},
        )
