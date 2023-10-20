from typing import Iterable, List, Optional, Union

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_s3 as s3
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import Batch, BatchEnvironmentConfig
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import (
    ON_DEMAND_INSTANCE_TYPES,
    SPOT_INSTANCE_TYPES,
)
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import BatchLaunchTemplateBuilder
from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.ec2 import EnvBaseVpc
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EFSEcosystem, EnvBaseFileSystem
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, LifecycleRuleGenerator
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class NetworkStack(EnvBaseStack):
    def __init__(self, scope: Construct, id: Optional[str], env_base: EnvBase, **kwargs) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self._vpc = EnvBaseVpc(self, "Vpc", self.env_base, max_azs=4)

    @property
    def vpc(self) -> EnvBaseVpc:
        return self._vpc


class StorageStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        name: str,
        vpc: ec2.Vpc,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

        self._bucket = EnvBaseBucket(
            self,
            "Bucket",
            self.env_base,
            bucket_name=name,
            removal_policy=self.removal_policy,
            lifecycle_rules=[
                LifecycleRuleGenerator.expire_files_under_prefix(),
                LifecycleRuleGenerator.expire_files_with_scratch_tags(),
                LifecycleRuleGenerator.use_storage_class_as_default(),
            ],
        )

        self._efs_ecosystem = EFSEcosystem(self, "EFS", self.env_base, name, vpc=vpc)
        self._file_system = self._efs_ecosystem.file_system

    @property
    def bucket(self) -> EnvBaseBucket:
        return self._bucket

    @property
    def file_system(self) -> EnvBaseFileSystem:
        return self._file_system


class ComputeStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        vpc: ec2.Vpc,
        buckets: Optional[Iterable[s3.Bucket]] = None,
        file_system: Optional[Iterable[efs.FileSystem]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

        self.batch = Batch(self, "Batch", self.env_base, vpc=vpc)

        self.create_batch_environments()

        self.grant_storage_access(*list(buckets or []), *list(file_system or []))

    def grant_storage_access(self, *resources: Union[s3.Bucket, efs.FileSystem]):
        self.batch.grant_instance_role_permissions(read_write_resources=list(resources))

        for batch_environment in self.batch.environments:
            for resource in resources:
                if isinstance(resource, efs.FileSystem):
                    batch_environment.grant_file_system_access(resource)

    def create_batch_environments(self):
        lt_builder = BatchLaunchTemplateBuilder(self, "lt-builder", env_base=self.env_base)
        self.on_demand_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("on-demand"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=ON_DEMAND_INSTANCE_TYPES,
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.spot_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("spot"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=SPOT_INSTANCE_TYPES,
                use_spot=True,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.fargate_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("fargate"),
            config=BatchEnvironmentConfig(
                allocation_strategy=None,
                instance_types=None,
                use_spot=False,
                use_fargate=True,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )
