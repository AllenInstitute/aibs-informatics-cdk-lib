from abc import abstractmethod
from typing import Iterable, List, Optional, Union

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import (
    Batch,
    BatchEnvironment,
    BatchEnvironmentConfig,
)
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import (
    LAMBDA_LARGE_INSTANCE_TYPES,
    LAMBDA_MEDIUM_INSTANCE_TYPES,
    LAMBDA_SMALL_INSTANCE_TYPES,
    ON_DEMAND_INSTANCE_TYPES,
    SPOT_INSTANCE_TYPES,
)
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import BatchLaunchTemplateBuilder
from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration


class BaseBatchComputeConstruct(EnvBaseConstruct):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        vpc: ec2.Vpc,
        batch_name: str,
        buckets: Optional[Iterable[s3.Bucket]] = None,
        file_systems: Optional[Iterable[Union[efs.FileSystem, efs.IFileSystem]]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
        instance_role_name: Optional[str] = None,
        instance_role_policy_statements: Optional[List[iam.PolicyStatement]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self.batch_name = batch_name
        self.batch = Batch(
            self,
            batch_name,
            self.env_base,
            vpc=vpc,
            instance_role_name=instance_role_name,
            instance_role_policy_statements=instance_role_policy_statements,
        )

        self.create_batch_environments()

        bucket_list = list(buckets or [])

        file_system_list = list(file_systems or [])

        if mount_point_configs:
            mount_point_config_list = list(mount_point_configs)
            file_system_list = self._update_file_systems_from_mount_point_configs(
                file_system_list, mount_point_config_list
            )
        else:
            mount_point_config_list = self._get_mount_point_configs(file_system_list)

        # Validation to ensure that the file systems are not duplicated
        self._validate_mount_point_configs(mount_point_config_list)

        self.grant_storage_access(*bucket_list, *file_system_list)

    @property
    @abstractmethod
    def primary_batch_environment(self) -> BatchEnvironment:
        raise NotImplementedError()

    @abstractmethod
    def create_batch_environments(self):
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return self.batch_name

    def grant_storage_access(self, *resources: Union[s3.Bucket, efs.FileSystem, efs.IFileSystem]):
        self.batch.grant_instance_role_permissions(read_write_resources=list(resources))

        for batch_environment in self.batch.environments:
            for resource in resources:
                if isinstance(resource, efs.FileSystem):
                    batch_environment.grant_file_system_access(resource)

    def _validate_mount_point_configs(self, mount_point_configs: List[MountPointConfiguration]):
        _ = {}
        for mpc in mount_point_configs:
            if mpc.mount_point in _ and _[mpc.mount_point] != mpc:
                raise ValueError(
                    f"Mount point {mpc.mount_point} is duplicated. "
                    "Cannot have multiple mount points configurations with the same name."
                )
            _[mpc.mount_point] = mpc

    def _get_mount_point_configs(
        self, file_systems: Optional[List[Union[efs.FileSystem, efs.IFileSystem]]]
    ) -> List[MountPointConfiguration]:
        mount_point_configs = []
        if file_systems:
            for fs in file_systems:
                mount_point_configs.append(MountPointConfiguration.from_file_system(fs))
        return mount_point_configs

    def _update_file_systems_from_mount_point_configs(
        self,
        file_systems: List[Union[efs.FileSystem, efs.IFileSystem]],
        mount_point_configs: List[MountPointConfiguration],
    ) -> List[Union[efs.FileSystem, efs.IFileSystem]]:
        file_system_map: dict[str, Union[efs.FileSystem, efs.IFileSystem]] = {
            fs.file_system_id: fs for fs in file_systems
        }
        for mpc in mount_point_configs:
            if mpc.file_system_id not in file_system_map:
                if not mpc.file_system and mpc.access_point:
                    file_system_map[mpc.file_system_id] = mpc.access_point.file_system
                elif mpc.file_system:
                    file_system_map[mpc.file_system_id] = mpc.file_system
                else:
                    raise ValueError(
                        "Mount point configuration must have a file system or access point."
                    )

        return list(file_system_map.values())


class BatchCompute(BaseBatchComputeConstruct):
    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        return self.on_demand_batch_environment

    def create_batch_environments(self):
        lt_builder = BatchLaunchTemplateBuilder(
            self, f"{self.name}-lt-builder", env_base=self.env_base
        )
        self.on_demand_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-on-demand"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT,
                instance_types=[ec2.InstanceType(_) for _ in ON_DEMAND_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.spot_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-spot"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
                instance_types=[ec2.InstanceType(_) for _ in SPOT_INSTANCE_TYPES],
                use_spot=True,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.fargate_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-fargate"),
            config=BatchEnvironmentConfig(
                allocation_strategy=None,
                instance_types=None,
                use_spot=False,
                use_fargate=True,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )


class LambdaCompute(BatchCompute):
    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        return self.lambda_batch_environment

    def create_batch_environments(self):
        lt_builder = BatchLaunchTemplateBuilder(
            self, f"{self.name}-lt-builder", env_base=self.env_base
        )
        self.lambda_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-lambda"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT,
                instance_types=[
                    *LAMBDA_SMALL_INSTANCE_TYPES,
                    *LAMBDA_MEDIUM_INSTANCE_TYPES,
                    *LAMBDA_LARGE_INSTANCE_TYPES,
                ],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
                minv_cpus=2,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_small_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-lambda-small"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT,
                instance_types=[*LAMBDA_SMALL_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_medium_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-lambda-medium"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT,
                instance_types=[*LAMBDA_MEDIUM_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
                minv_cpus=2,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_large_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor(f"{self.name}-lambda-large"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT,
                instance_types=[*LAMBDA_LARGE_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )
