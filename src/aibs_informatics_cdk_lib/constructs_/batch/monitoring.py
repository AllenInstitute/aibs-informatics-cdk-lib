from typing import List

from aibs_informatics_core.env import EnvBase
from aws_cdk import Duration
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_sns as sns
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import BatchEnvironment
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import CloudWatchConfigBuilder
from aibs_informatics_cdk_lib.constructs_.cw import EnhancedDashboard


class BatchMonitoring(EnvBaseConstruct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        batch_environments: List[BatchEnvironment],
    ) -> None:
        super().__init__(scope, id, env_base)
        self.alarm_topic = sns.Topic(self, self.get_construct_id("batch-alarm-topic"))
        if self.is_test_or_prod:
            sns.Subscription(
                self,
                self.get_construct_id("batch-alarm-subscription"),
                topic=self.alarm_topic,
                endpoint="marmotdev@alleninstitute.org",
                protocol=sns.SubscriptionProtocol("EMAIL"),
            )

        dashboard = cw.Dashboard(
            self,
            "batch-dashboard",
            dashboard_name=f"{self.env_base}-Batch-Dashboard",
            start="-P1D",
        )
        self.dashboard_tools = EnhancedDashboard(self, "cwdb", env_base, dashboard)

        self.add_ecs_widgets(batch_environments)

    def add_ecs_widgets(self, batch_environments: List[BatchEnvironment]):
        self.dashboard_tools.add_text_widget(f"Elastic Container Service ({self.env_base})", 1)

        for batch_environment in batch_environments:
            self.dashboard_tools.add_text_widget(
                f"Batch Environment {batch_environment.descriptor}", 2
            )
            builder = CloudWatchConfigBuilder(
                self.env_base, batch_environment.descriptor.get_name()
            )
            self.dashboard_tools.add_graphs(
                grouped_metric_configs=builder.get_grouped_graph_metric_configs(),
                namespace=builder.metric_namespace,
                period=Duration.minutes(5),
                alarm_id_discriminator=batch_environment.descriptor.get_name(),
                alarm_topic=self.alarm_topic,
                dimensions={
                    "env_base": self.env_base,
                    "batch_env_name": batch_environment.descriptor.get_name(),
                },
            )
