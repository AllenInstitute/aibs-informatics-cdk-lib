from abc import abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from aibs_informatics_aws_utils import AWS_REGION_VAR
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    batch_policy_statement,
    s3_policy_statement,
)
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
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import create_state_machine
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import (
    AWSBatchMixins,
    SubmitJobWithDefaultsFragment,
)
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics import (
    BatchInvokedLambdaFunction,
)
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class BaseComputeStack(EnvBaseStack):
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
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self.batch_name = batch_name
        self.batch = Batch(self, batch_name, self.env_base, vpc=vpc)

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


class ComputeStack(BaseComputeStack):
    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        return self.on_demand_batch_environment

    def create_batch_environments(self):
        lt_builder = BatchLaunchTemplateBuilder(
            self, f"{self.name}-lt-builder", env_base=self.env_base
        )
        self.on_demand_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("on-demand"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=[ec2.InstanceType(_) for _ in ON_DEMAND_INSTANCE_TYPES],
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
                instance_types=[ec2.InstanceType(_) for _ in SPOT_INSTANCE_TYPES],
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


class LambdaComputeStack(ComputeStack):
    @property
    def primary_batch_environment(self) -> BatchEnvironment:
        return self.lambda_batch_environment

    def create_batch_environments(self):
        lt_builder = BatchLaunchTemplateBuilder(
            self, f"${self.name}-lt-builder", env_base=self.env_base
        )
        self.lambda_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("lambda"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=[
                    *LAMBDA_SMALL_INSTANCE_TYPES,
                    *LAMBDA_MEDIUM_INSTANCE_TYPES,
                    *LAMBDA_LARGE_INSTANCE_TYPES,
                ],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_small_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("lambda-small"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=[*LAMBDA_SMALL_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_medium_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("lambda-medium"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=[*LAMBDA_MEDIUM_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )

        self.lambda_large_batch_environment = self.batch.setup_batch_environment(
            descriptor=BatchEnvironmentDescriptor("lambda-large"),
            config=BatchEnvironmentConfig(
                allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                instance_types=[*LAMBDA_LARGE_INSTANCE_TYPES],
                use_spot=False,
                use_fargate=False,
                use_public_subnets=False,
            ),
            launch_template_builder=lt_builder,
        )


class ComputeWorkflowStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        batch_environment: BatchEnvironment,
        primary_bucket: s3.Bucket,
        buckets: Optional[Iterable[s3.Bucket]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self.primary_bucket = primary_bucket
        self.buckets = list(buckets or [])
        self.batch_environment = batch_environment
        self.mount_point_configs = list(mount_point_configs) if mount_point_configs else None

        self.create_submit_job_step_function()
        self.create_lambda_invoke_step_function()

    def create_submit_job_step_function(self):
        fragment = SubmitJobWithDefaultsFragment(
            self,
            "submit-job-fragment",
            self.env_base,
            job_queue=self.batch_environment.job_queue.job_queue_arn,
            mount_point_configs=self.mount_point_configs,
        )
        state_machine_name = self.get_resource_name("submit-job")

        self.batch_submit_job_state_machine = fragment.to_state_machine(
            state_machine_name=state_machine_name,
            role=iam.Role(
                self,
                self.env_base.get_construct_id(state_machine_name, "role"),
                assumed_by=iam.ServicePrincipal("states.amazonaws.com"),  # type: ignore
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonAPIGatewayInvokeFullAccess"
                    ),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchEventsFullAccess"),
                ],
                inline_policies={
                    "default": iam.PolicyDocument(
                        statements=[
                            batch_policy_statement(self.env_base),
                        ]
                    ),
                },
            ),
        )

    def create_lambda_invoke_step_function(
        self,
    ):
        defaults: dict[str, Any] = {}

        defaults["job_queue"] = self.batch_environment.job_queue_name
        defaults["memory"] = "1024"
        defaults["vcpus"] = "1"
        defaults["gpu"] = "0"
        defaults["platform_capabilities"] = ["EC2"]

        if self.mount_point_configs:
            mount_points, volumes = AWSBatchMixins.convert_to_mount_point_and_volumes(
                self.mount_point_configs
            )
            defaults["mount_points"] = mount_points
            defaults["volumes"] = volumes

        start = sfn.Pass(
            self,
            "Start",
            parameters={
                "image": sfn.JsonPath.string_at("$.image"),
                "handler": sfn.JsonPath.string_at("$.handler"),
                "payload": sfn.JsonPath.object_at("$.request"),
                # We will merge the rest with the defaults
                "input": sfn.JsonPath.object_at("$"),
                "default": defaults,
            },
        )

        merge = sfn.Pass(
            self,
            "Merge",
            parameters={
                "image": sfn.JsonPath.string_at("$.image"),
                "handler": sfn.JsonPath.string_at("$.handler"),
                "payload": sfn.JsonPath.string_at("$.payload"),
                "compute": sfn.JsonPath.json_merge(
                    sfn.JsonPath.object_at("$.default"), sfn.JsonPath.object_at("$.input")
                ),
            },
        )

        batch_invoked_lambda = BatchInvokedLambdaFunction(
            self,
            "Data Chain",
            env_base=self.env_base,
            image="$.image",
            name="run-lambda-function",
            handler="$.handler",
            payload_path="$.payload",
            bucket_name=self.primary_bucket.bucket_name,
            job_queue=self.batch_environment.job_queue_name,
            environment={
                EnvBase.ENV_BASE_KEY: self.env_base,
                "AWS_REGION": self.aws_region,
                "AWS_ACCOUNT_ID": self.aws_account,
            },
            memory=sfn.JsonPath.string_at("$.compute.memory"),
            vcpus=sfn.JsonPath.string_at("$.compute.vcpus"),
            mount_points=sfn.JsonPath.string_at("$.compute.mount_points"),
            volumes=sfn.JsonPath.string_at("$.compute.volumes"),
            platform_capabilities=sfn.JsonPath.string_at("$.compute.platform_capabilities"),
        )

        # fmt: off
        definition = (
            start
            .next(merge)
            .next(batch_invoked_lambda.to_single_state())
        )
        # fmt: on

        self.batch_invoked_lambda_state_machine = create_state_machine(
            self,
            env_base=self.env_base,
            name=self.env_base.get_state_machine_name("batch-invoked-lambda"),
            definition=definition,
            role=iam.Role(
                self,
                self.env_base.get_construct_id("batch-invoked-lambda", "role"),
                assumed_by=iam.ServicePrincipal("states.amazonaws.com"),  # type: ignore
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonAPIGatewayInvokeFullAccess"
                    ),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchEventsFullAccess"),
                ],
                inline_policies={
                    "default": iam.PolicyDocument(
                        statements=[
                            batch_policy_statement(self.env_base),
                            s3_policy_statement(self.env_base),
                        ]
                    ),
                },
            ),
        )
