import json
from email.policy import default
from typing import Any, Iterable, List, Optional, TypeVar, Union
from urllib import request

import aws_cdk as cdk
from aibs_informatics_aws_utils.batch import to_mount_point, to_volume
from aibs_informatics_aws_utils.constants.efs import (
    EFS_ROOT_ACCESS_POINT_TAG,
    EFS_ROOT_PATH,
    EFS_SCRATCH_ACCESS_POINT_TAG,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_TAG,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_TAG,
    EFS_TMP_PATH,
)
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from aibs_informatics_cdk_lib.common.aws.iam_utils import batch_policy_statement
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import Batch, BatchEnvironmentConfig
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import (
    ON_DEMAND_INSTANCE_TYPES,
    SPOT_INSTANCE_TYPES,
)
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import BatchLaunchTemplateBuilder
from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.ec2 import EnvBaseVpc
from aibs_informatics_cdk_lib.constructs_.efs.file_system import (
    EFSEcosystem,
    EnvBaseFileSystem,
    MountPointConfiguration,
)
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, LifecycleRuleGenerator
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import SubmitJobFragment
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
        file_systems: Optional[Iterable[Union[efs.FileSystem, efs.IFileSystem]]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
        create_state_machine: bool = True,
        state_machine_name: Optional[str] = "submit-job",
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

        self.batch = Batch(self, "Batch", self.env_base, vpc=vpc)

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

        self.create_step_functions(
            name=state_machine_name, mount_point_configs=mount_point_config_list
        )

        self.export_values()

    def grant_storage_access(self, *resources: Union[s3.Bucket, efs.FileSystem, efs.IFileSystem]):
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
                instance_types=list(map(ec2.InstanceType, ON_DEMAND_INSTANCE_TYPES)),
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
                instance_types=list(map(ec2.InstanceType, SPOT_INSTANCE_TYPES)),
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

    def create_step_functions(
        self,
        name: Optional[str] = None,
        mount_point_configs: Optional[list[MountPointConfiguration]] = None,
    ):
        state_machine_core_name = name or "submit-job"
        defaults: dict[str, Any] = {}
        defaults["command"] = []
        defaults["job_queue"] = self.on_demand_batch_environment.job_queue.job_queue_arn
        defaults["environment"] = []
        defaults["memory"] = "1024"
        defaults["vcpus"] = "1"
        defaults["gpu"] = "0"
        defaults["platform_capabilities"] = ["EC2"]

        if mount_point_configs:
            defaults["mount_points"] = [
                convert_key_case(mpc.to_batch_mount_point(f"efs-vol{i}"), pascalcase)
                for i, mpc in enumerate(mount_point_configs)
            ]
            defaults["volumes"] = [
                convert_key_case(mpc.to_batch_volume(f"efs-vol{i}"), pascalcase)
                for i, mpc in enumerate(mount_point_configs)
            ]

        start = sfn.Pass(
            self,
            "Start",
            parameters={
                "input": sfn.JsonPath.string_at("$"),
                "default": defaults,
            },
        )

        merge = sfn.Pass(
            self,
            "Merge",
            parameters={
                "request": sfn.JsonPath.json_merge(
                    sfn.JsonPath.object_at("$.default"), sfn.JsonPath.object_at("$.input")
                ),
            },
        )

        submit_job = SubmitJobFragment(
            self,
            "SubmitJob",
            env_base=self.env_base,
            name="SubmitJobCore",
            image=sfn.JsonPath.string_at("$.request.image"),
            command=sfn.JsonPath.string_at("$.request.command"),
            job_queue=sfn.JsonPath.string_at("$.request.job_queue"),
            environment=sfn.JsonPath.string_at("$.request.environment"),
            memory=sfn.JsonPath.string_at("$.request.memory"),
            vcpus=sfn.JsonPath.string_at("$.request.vcpus"),
            # TODO: Handle GPU parameter better - right now, we cannot handle cases where it is
            # not specified. Setting to zero causes issues with the Batch API.
            # If it is set to zero, then the json list of resources are not properly set.
            # gpu=sfn.JsonPath.string_at("$.request.gpu"),
            mount_points=sfn.JsonPath.string_at("$.request.mount_points"),
            volumes=sfn.JsonPath.string_at("$.request.volumes"),
            platform_capabilities=sfn.JsonPath.string_at("$.request.platform_capabilities"),
        ).to_single_state()

        definition = start.next(merge).next(submit_job)

        state_machine_name = self.get_resource_name(state_machine_core_name)
        self.batch_submit_job_state_machine = sfn.StateMachine(
            self,
            self.env_base.get_construct_id(state_machine_name, "state-machine"),
            state_machine_name=state_machine_name,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    self.get_construct_id(state_machine_name, "state-loggroup"),
                    log_group_name=self.env_base.get_state_machine_log_group_name("submit-job"),
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH,
                )
            ),
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
                        statements=[batch_policy_statement(self.env_base)]
                    ),
                },
            ),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
        )

    def export_values(self) -> None:
        self.export_value(self.on_demand_batch_environment.job_queue.job_queue_arn)
        self.export_value(self.spot_batch_environment.job_queue.job_queue_arn)
        self.export_value(self.fargate_batch_environment.job_queue.job_queue_arn)

    ## Private methods

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
