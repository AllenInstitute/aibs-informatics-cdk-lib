from signal import alarm
from typing import List, Literal, Optional, Union

from aibs_informatics_core.env import EnvBase, ResourceNameBaseEnum
from aibs_informatics_core.models.email_address import EmailAddress
from aibs_informatics_core.utils.hashing import uuid_str
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
from aibs_informatics_cdk_lib.constructs_.cw.config_generators.lambda_ import (
    LambdaFunctionMetricConfigGenerator,
)
from aibs_informatics_cdk_lib.constructs_.cw.config_generators.sfn import (
    SFN_TIME_UNITS,
    StateMachineMetricConfigGenerator,
)


class MonitoringConstruct(EnvBaseConstruct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        name: Optional[str] = None,
        notify_on_alarms: Optional[bool] = None,
        alarm_topic: Optional[sns.Topic] = None,
    ) -> None:
        super().__init__(scope, id, env_base)
        self.monitoring_name = name or self.construct_id
        self.notify_on_alarms = notify_on_alarms
        self.alarm_topic = alarm_topic

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
    def notify_on_alarms(self, value: Optional[bool]):
        self._notify_on_alarms = value

    @property
    def alarm_topic(self) -> sns.Topic:
        if self._alarm_topic is None:
            self.alarm_topic = sns.Topic(
                self, self.get_construct_id(self.monitoring_name, "alarm-topic")
            )
            return self.alarm_topic
        else:
            return self._alarm_topic

    @alarm_topic.setter
    def alarm_topic(self, value: Optional[sns.Topic]):
        self._alarm_topic = value

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

    def add_function_widget(
        self,
        dashboard: cw.Dashboard,
        function_name: str,
        title: Optional[str] = None,
        title_header_level: int = 1,
        prefix_name_with_env: bool = True,
        include_min_max_duration: bool = False,
    ):
        self.add_function_widgets(
            dashboard,
            function_name,
            title=title,
            title_header_level=title_header_level,
            prefix_name_with_env=prefix_name_with_env,
            include_min_max_duration=include_min_max_duration,
        )

    def add_function_widgets(
        self,
        dashboard: cw.Dashboard,
        *function_names: str,
        title: Optional[str] = None,
        title_header_level: int = 1,
        prefix_name_with_env: bool = True,
        include_min_max_duration: bool = False,
        include_alarm: bool = False,
    ):
        dashboard_tools = (
            dashboard if isinstance(dashboard, EnhancedDashboard) else DashboardTools(dashboard)
        )
        if title:
            dashboard_tools.add_text_widget(title, title_header_level)

        grouped_invocation_metrics: List[GraphMetricConfig] = []
        grouped_error_metrics: List[GraphMetricConfig] = []
        grouped_timing_metrics: List[GraphMetricConfig] = []
        group_name = uuid_str(str(function_names))

        for idx, raw_function_name in enumerate(function_names):
            if prefix_name_with_env:
                function_name = self.get_name_with_env(raw_function_name)
            else:
                function_name = raw_function_name

            fn_config_generator = LambdaFunctionMetricConfigGenerator(
                lambda_function=lambda_.Function.from_function_name(
                    self, f"{function_name}-from-name", function_name=function_name
                ),
                lambda_function_name=function_name,
            )

            grouped_invocation_metrics.append(fn_config_generator.get_invocations_metric())
            grouped_error_metrics.append(
                fn_config_generator.get_errors_metric(
                    discriminator=str(idx), include_alarm=include_alarm
                )
            )

            # Availability metric - make sure to set the axis_side to "right"
            avail_metric = fn_config_generator.get_availability_metric(discriminator=str(idx))
            avail_metric["axis_side"] = "right"
            grouped_error_metrics.append(avail_metric)

            duration_avg_metric = fn_config_generator.get_duration_avg_metric()
            grouped_timing_metrics.append(duration_avg_metric)
            if include_min_max_duration:
                grouped_timing_metrics.append(fn_config_generator.get_duration_max_metric())
                grouped_timing_metrics.append(fn_config_generator.get_duration_min_metric())

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

        dashboard_tools.add_graphs(
            grouped_metric_configs=grouped_metrics,
            namespace="AWS/Lambda",
            period=Duration.minutes(5),
            alarm_id_discriminator=group_name,
            alarm_topic=self.alarm_topic if self.notify_on_alarms else None,
            dimensions={},
        )

    def add_state_machine_widget(
        self,
        dashboard: cw.Dashboard,
        state_machine_name: str,
        title: Optional[str] = None,
        title_header_level: int = 1,
        prefix_name_with_env: bool = True,
    ):
        self.add_state_machine_widgets(
            dashboard,
            state_machine_name,
            title=title,
            title_header_level=title_header_level,
            prefix_name_with_env=prefix_name_with_env,
        )

    def add_state_machine_widgets(
        self,
        dashboard: cw.Dashboard,
        *state_machine_names: str,
        title: Optional[str] = None,
        title_header_level: int = 1,
        prefix_name_with_env: bool = True,
        time_unit: SFN_TIME_UNITS = "minutes",
    ):
        dashboard_tools = (
            dashboard if isinstance(dashboard, EnhancedDashboard) else DashboardTools(dashboard)
        )

        if title:
            dashboard_tools.add_text_widget(title, title_header_level)

        grouped_invocation_metrics: List[GraphMetricConfig] = []
        grouped_error_metrics: List[GraphMetricConfig] = []
        grouped_timing_metrics: List[GraphMetricConfig] = []
        group_name = uuid_str(str(state_machine_names))
        for idx, raw_state_machine_name in enumerate(state_machine_names):
            state_machine_name = self.get_state_machine_name(
                raw_state_machine_name, prefix_name_with_env=prefix_name_with_env
            )

            sm_config_generator = StateMachineMetricConfigGenerator(
                state_machine=sfn.StateMachine.from_state_machine_name(
                    self, f"{state_machine_name}-from-name", state_machine_name
                ),
                state_machine_name=state_machine_name,
            )
            grouped_invocation_metrics.append(
                sm_config_generator.get_execution_invocations_metric(raw_state_machine_name)
            )
            grouped_invocation_metrics.append(
                sm_config_generator.get_execution_completion_metric(raw_state_machine_name)
            )
            grouped_error_metrics.append(
                sm_config_generator.get_execution_failures_metric(
                    raw_state_machine_name, discriminator=str(idx)
                )
            )
            grouped_timing_metrics.append(
                sm_config_generator.get_execution_timing_metric(
                    raw_state_machine_name,
                    discriminator=str(idx),
                    time_unit=time_unit,
                )
            )

        grouped_metrics = [
            GroupedGraphMetricConfig(
                title="Execution Invocations", metrics=grouped_invocation_metrics
            ),
            GroupedGraphMetricConfig(title="Execution Errors", metrics=grouped_error_metrics),
            GroupedGraphMetricConfig(
                title="Execution Time",
                metrics=grouped_timing_metrics,
                left_y_axis=cw.YAxisProps(label=f"Time ({time_unit})"),
            ),
        ]

        dashboard_tools.add_graphs(
            grouped_metric_configs=grouped_metrics,
            namespace="AWS/States",
            period=Duration.minutes(5),
            alarm_id_discriminator=group_name,
            alarm_topic=self.alarm_topic if self.notify_on_alarms else None,
            dimensions={},
        )

    def add_alarm_subscription(self, email: Union[str, EmailAddress]):
        if not isinstance(email, EmailAddress):
            email = EmailAddress(email)

        return sns.Subscription(
            self,
            self.get_construct_id(f"{email}-alarm-subscription"),
            topic=self.alarm_topic,  # type: ignore  # Topic implements ITopic
            endpoint=email,
            protocol=sns.SubscriptionProtocol.EMAIL,
        )

    def get_state_machine_name(
        self, name: Union[str, ResourceNameBaseEnum], prefix_name_with_env: bool = True
    ) -> str:
        if isinstance(name, ResourceNameBaseEnum):
            return name.get_name(self.env_base)
        elif prefix_name_with_env:
            return self.env_base.get_state_machine_name(name)
        else:
            return name

    def get_state_machine_arn(
        self, name: Union[str, ResourceNameBaseEnum], prefix_name_with_env: bool = True
    ) -> str:
        state_machine_name = self.get_state_machine_name(name, prefix_name_with_env)
        return build_sfn_arn(resource_type="stateMachine", resource_id=state_machine_name)


class ResourceMonitoring(MonitoringConstruct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        notify_on_alarms: Optional[bool] = None,
        alarm_email: Optional[str] = None,
    ) -> None:
        super().__init__(scope, id, env_base)
        self.notify_on_alarms = notify_on_alarms
        if self.notify_on_alarms:
            email = alarm_email or "marmotdev@alleninstitute.org"
            self.add_alarm_subscription(email)

        self.dashboard = self.create_dashboard(start="-P1D")
