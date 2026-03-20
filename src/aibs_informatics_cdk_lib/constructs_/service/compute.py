"""Compute service constructs for AWS Batch.

This module provides high-level constructs for creating Batch compute
environments with various configurations.
"""

from abc import abstractmethod
from collections.abc import Iterable

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
    """Base class for Batch compute constructs.

    Abstract base class that provides common functionality for creating
    and managing AWS Batch compute environments.

    """

    def __init__(
        self,
        scope: Construct,
        id: str | None,
        env_base: EnvBase,
        vpc: ec2.Vpc,
        batch_name: str,
        buckets: Iterable[s3.Bucket] | None = None,
        file_systems: Iterable[efs.FileSystem | efs.IFileSystem] | None = None,
        mount_point_configs: Iterable[MountPointConfiguration] | None = None,
        instance_role_name: str | None = None,
        instance_role_policy_statements: list[iam.PolicyStatement] | None = None,
        **kwargs,
    ) -> None:
        """Initialize a Batch compute construct.

        Args:
            scope (Construct): The construct scope.
            id (Optional[str]): The construct ID.
            env_base (EnvBase): Environment base for resource naming.
            vpc (ec2.Vpc): VPC for the compute environments.
            batch_name (str): Name for the batch infrastructure.
            buckets (Optional[Iterable[s3.Bucket]]): S3 buckets to grant access to.
            file_systems (Optional[Iterable[Union[efs.FileSystem, efs.IFileSystem]]]):
                EFS file systems to grant access to.
            mount_point_configs (Optional[Iterable[MountPointConfiguration]]):
                Mount point configurations for EFS.
            instance_role_name (Optional[str]): Name for the instance role.
            instance_role_policy_statements (Optional[List[iam.PolicyStatement]]):
                Additional IAM policy statements.
            **kwargs: Additional arguments passed to parent.
        """
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
        """Get the primary batch environment.

        Returns:
            The primary BatchEnvironment for this compute construct.
        """
        raise NotImplementedError()

    @abstractmethod
    def create_batch_environments(self) -> None:
        """Create the batch environments.

        Subclasses must implement this to create their specific
        batch environment configurations.
        """
        raise NotImplementedError()

    @property
    def name(self) -> str:
        """Get the batch name.

        Returns:
            The batch infrastructure name.
        """
        return self.batch_name

    def grant_storage_access(
        self, *resources: s3.Bucket | efs.FileSystem | efs.IFileSystem
    ) -> None:
        """Grant access to storage resources.

        Args:
            *resources (Union[s3.Bucket, efs.FileSystem, efs.IFileSystem]):
                Variable number of storage resources to grant access to.
        """
        self.batch.grant_instance_role_permissions(read_write_resources=list(resources))

        for batch_environment in self.batch.environments:
            for resource in resources:
                if isinstance(resource, efs.FileSystem):
                    batch_environment.grant_file_system_access(resource)

    def _validate_mount_point_configs(
        self, mount_point_configs: list[MountPointConfiguration]
    ) -> None:
        """Validate mount point configurations for duplicates.

        Args:
            mount_point_configs (List[MountPointConfiguration]): Configs to validate.

        Raises:
            ValueError: If duplicate mount points are found.
        """
        _ = {}
        for mpc in mount_point_configs:
            if mpc.mount_point in _ and _[mpc.mount_point] != mpc:
                raise ValueError(
                    f"Mount point {mpc.mount_point} is duplicated. "
                    "Cannot have multiple mount points configurations with the same name."
                )
            _[mpc.mount_point] = mpc

    def _get_mount_point_configs(
        self, file_systems: list[efs.FileSystem | efs.IFileSystem] | None
    ) -> list[MountPointConfiguration]:
        """Get mount point configurations from file systems.

        Args:
            file_systems (Optional[List[Union[efs.FileSystem, efs.IFileSystem]]]):
                File systems to create mount configs for.

        Returns:
            List of MountPointConfiguration objects.
        """
        mount_point_configs = []
        if file_systems:
            for fs in file_systems:
                mount_point_configs.append(MountPointConfiguration.from_file_system(fs))
        return mount_point_configs

    def _update_file_systems_from_mount_point_configs(
        self,
        file_systems: list[efs.FileSystem | efs.IFileSystem],
        mount_point_configs: list[MountPointConfiguration],
    ) -> list[efs.FileSystem | efs.IFileSystem]:
        """Update file systems list from mount point configurations.

        Args:
            file_systems (List[Union[efs.FileSystem, efs.IFileSystem]]): Existing file systems.
            mount_point_configs (List[MountPointConfiguration]): Mount configs to process.

        Returns:
            Updated list of file systems.

        Raises:
            ValueError: If mount config has neither file system nor access point.
        """
        file_system_map: dict[str, efs.FileSystem | efs.IFileSystem] = {
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
    """Standard Batch compute construct with on-demand, spot, and Fargate environments.

    Provides a complete Batch compute setup with three environment types:
    on-demand, spot, and Fargate.

    Attributes:
        on_demand_batch_environment: On-demand compute environment.
        spot_batch_environment: Spot compute environment.
        fargate_batch_environment: Fargate compute environment.
    """

    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        """Get the primary batch environment.

        Returns:
            The on-demand batch environment.
        """
        return self.on_demand_batch_environment

    def create_batch_environments(self) -> None:
        """Create on-demand, spot, and Fargate batch environments."""
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
    """Lambda-optimized Batch compute construct.

    Provides Batch environments optimized for Lambda-like workloads with
    small, medium, and large instance type configurations.

    """

    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        """Get the primary batch environment.

        Returns:
            The main Lambda batch environment.
        """
        return self.lambda_batch_environment

    def create_batch_environments(self) -> None:
        """Create Lambda-optimized batch environments."""
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
