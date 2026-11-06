import json
from enum import Enum

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk.aws_ec2 import Vpc

from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import (
    Batch,
    BatchEnvironmentConfig,
)
from test.aibs_informatics_cdk_lib.base import CdkBaseTest


class MyBatchEnvironmentName(str, Enum):
    A = "a"
    B = "b"

    def get_job_queue_name(self, env_base: EnvBase) -> str:
        return env_base.get_resource_name(self, "job-queue")

    def get_compute_environment_name(self, env_base: EnvBase) -> str:
        return env_base.get_resource_name(self, "ce")

    def get_name(self) -> str:
        return self.value


class BatchTests(CdkBaseTest):
    def test__init__simple(self):
        stack = self.get_dummy_stack("test")
        vpc = Vpc(stack, "vpc")
        config = BatchEnvironmentConfig(
            allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
            instance_types=["t2.micro"],
            use_public_subnets=False,
            use_spot=True,
        )
        batch_construct = Batch(
            stack,
            "batch",
            env_base=self.env_base,
            vpc=vpc,
        )

        batch_construct.setup_batch_environment(
            descriptor=MyBatchEnvironmentName.A,
            config=config,
        )

        batch_construct.setup_batch_environment(
            descriptor=MyBatchEnvironmentName.B,
            config=config,
        )

        template = self.get_template(stack)

        template.resource_count_is("AWS::Batch::JobQueue", 2)
        template.resource_count_is("AWS::Batch::ComputeEnvironment", 2)

    def _setup_environment(self, **config_kwargs):
        stack = self.get_dummy_stack("test")
        vpc = Vpc(stack, "vpc")
        config = BatchEnvironmentConfig(
            allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
            instance_types=["t2.micro"],
            use_public_subnets=False,
            use_spot=True,
            **config_kwargs,
        )
        batch_construct = Batch(stack, "batch", env_base=self.env_base, vpc=vpc)
        batch_construct.setup_batch_environment(descriptor=MyBatchEnvironmentName.A, config=config)
        return self.get_template(stack)

    def test__container_insights__not_configured_by_default(self):
        template = self._setup_environment()

        template.resource_count_is("Custom::AWS", 0)

    def test__container_insights__creates_describe_and_update_resources(self):
        template = self._setup_environment(container_insights="enhanced")

        template.resource_count_is("Custom::AWS", 2)
        create_calls = [
            json.dumps(resource["Properties"]["Create"])
            for resource in template.find_resources("Custom::AWS").values()
        ]
        assert any("describeComputeEnvironments" in call for call in create_calls)
        assert any("ecsClusterArn" in call for call in create_calls)
        assert any(
            "updateClusterSettings" in call and "containerInsights" in call and "enhanced" in call
            for call in create_calls
        )

    def test__container_insights__iam_is_scoped_to_batch_clusters(self):
        template = self._setup_environment(container_insights="enabled")

        policies = json.dumps(template.find_resources("AWS::IAM::Policy"))
        assert "batch:DescribeComputeEnvironments" in policies
        assert "ecs:UpdateClusterSettings" in policies
        assert "cluster/AWSBatch-*" in policies
