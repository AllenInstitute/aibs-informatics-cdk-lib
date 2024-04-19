from enum import Enum
from test.aibs_informatics_cdk_lib.base import CdkBaseTest

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha
from aws_cdk.aws_ec2 import Vpc

from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import (
    Batch,
    BatchEnvironment,
    BatchEnvironmentConfig,
    IBatchEnvironmentDescriptor,
)


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
            allocation_strategy=aws_batch_alpha.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
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
