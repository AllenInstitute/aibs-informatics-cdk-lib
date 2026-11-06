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
        for resource in template.find_resources("AWS::Batch::ComputeEnvironment").values():
            assert "EcsSettings" not in resource["Properties"]

    def _assert_ecs_settings(self, value: str) -> None:
        template = self._setup_environment(container_insights=value)

        template.has_resource_properties(
            "AWS::Batch::ComputeEnvironment",
            {"EcsSettings": {"ContainerInsights": value.upper()}},
        )
        template.resource_count_is("Custom::AWS", 0)

    def test__container_insights__enabled(self):
        self._assert_ecs_settings("enabled")

    def test__container_insights__enhanced(self):
        self._assert_ecs_settings("enhanced")

    def test__container_insights__disabled(self):
        self._assert_ecs_settings("disabled")

    def test__container_insights__fargate(self):
        template = self._setup_environment(use_fargate=True, container_insights="enhanced")

        template.has_resource_properties(
            "AWS::Batch::ComputeEnvironment",
            {"EcsSettings": {"ContainerInsights": "ENHANCED"}},
        )
        template.resource_count_is("Custom::AWS", 0)
