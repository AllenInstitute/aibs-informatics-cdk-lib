from pathlib import Path
from typing import TYPE_CHECKING, List, Mapping, Optional, Union, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_aws_utils.batch import to_mount_point, to_volume
from aibs_informatics_aws_utils.efs import EFS_MOUNT_PATH_VAR
from aibs_informatics_core.env import EnvBase, ResourceNameBaseEnum
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as stepfn_tasks

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    batch_policy_statement,
    dynamodb_policy_statement,
    lambda_policy_statement,
    s3_policy_statement,
)
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    GLOBAL_GLOB_EXCLUDES,
    PYTHON_GLOB_EXCLUDES,
    CodeAsset,
)
from aibs_informatics_cdk_lib.constructs_.batch.types import IBatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.ec2.network import EnvBaseVpc
from aibs_informatics_cdk_lib.constructs_.efs.file_system import (
    EnvBaseFileSystem,
    create_access_point,
    grant_file_system_access,
)
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, grant_bucket_access
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import SubmitJobFragment
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics import (
    BatchInvokedLambdaFunction,
)
from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3Operation
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import (
        KeyValuePairTypeDef,
        MountPointTypeDef,
        RegisterJobDefinitionRequestRequestTypeDef,
        ResourceRequirementTypeDef,
        VolumeTypeDef,
    )
else:
    ResourceRequirementTypeDef = dict
    MountPointTypeDef = dict
    VolumeTypeDef = dict
    KeyValuePairTypeDef = dict
    RegisterJobDefinitionRequestRequestTypeDef = dict


DATA_SYNC_ASSET_NAME = "aibs_informatics_aws_lambda"

EFS_MOUNT_PATH = "/opt/efs"
EFS_VOLUME_NAME = "efs-file-system"


class DataSyncFunctions(ResourceNameBaseEnum):
    PUT_JSON_TO_FILE = "put-json-to-file"
    GET_JSON_FROM_FILE = "get-json-from-file"
    DATA_SYNC = "data-sync"
    BATCH_DATA_SYNC = "batch-data-sync"
    PREPARE_BATCH_DATA_SYNC = "prep-batch-data-sync"


class DataSyncStack(EnvBaseStack):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        asset_directory: Union[Path, str],
        vpc: ec2.Vpc,
        primary_bucket: s3.Bucket,
        file_system: efs.FileSystem,
        batch_job_queue: batch.JobQueue,
        s3_buckets: List[s3.Bucket],
        **kwargs,
    ):
        super().__init__(scope, id, env_base, **kwargs)

        self._vpc = vpc
        if self._vpc is None:
            self._vpc = EnvBaseVpc(self, "vpc", env_base=self.env_base)

        self._primary_bucket = primary_bucket
        self._buckets = [primary_bucket, *s3_buckets]

        self._file_system = file_system

        if isinstance(self._file_system, EnvBaseFileSystem):
            self._lambda_file_system = self._file_system.as_lambda_file_system()
        else:
            root_ap = create_access_point(self, self._file_system, "data-sync", "/")
            self._lambda_file_system = lambda_.FileSystem.from_efs_access_point(
                root_ap, "/mnt/efs"
            )

        self._job_queue = batch_job_queue

        # Create code and docker asset for AWS lambda package
        asset_directory = Path(asset_directory)
        self._code_asset = CodeAsset.create_py_code_asset(
            path=asset_directory,
            context_path=None,
            runtime=lambda_.Runtime.PYTHON_3_11,
            environment={self.env_base.ENV_BASE_KEY: self.env_base},
        )
        # self._docker_asset = ecr_assets.DockerImageAsset(
        #     self,
        #     "docker",
        #     asset_name=DATA_SYNC_ASSET_NAME,
        #     directory=asset_directory.resolve().as_posix(),
        #     file="docker/Dockerfile",
        #     build_ssh="default",
        #     platform=ecr_assets.Platform.LINUX_AMD64,
        #     exclude=[*GLOBAL_GLOB_EXCLUDES, *PYTHON_GLOB_EXCLUDES]
        # )

        self.create_lambda_functions()
        self.create_step_functions()

    @property
    def buckets(self) -> List[s3.Bucket]:
        return self._buckets

    @property
    def file_system(self) -> efs.FileSystem:
        return self._file_system

    @property
    def lambda_file_system(self) -> lambda_.FileSystem:
        return self._lambda_file_system

    @property
    def vpc(self) -> ec2.Vpc:
        return self._vpc

    # @property
    # def docker_image_asset(self) -> ecr_assets.DockerImageAsset:
    #     return self._docker_asset

    @property
    def code_asset(self) -> CodeAsset:
        return self._code_asset

    def create_lambda_functions(self) -> None:

        # ----------------------------------------------------------
        # Data Transfer functions
        # ----------------------------------------------------------

        data_sync_efs_lambda_arguments = dict(
            filesystem=self.lambda_file_system,
            vpc=self.vpc,
        )

        # self.put_json_to_file_fn: lambda_.Function = lambda_.Function(
        #     self,
        #     self.get_construct_id(DataSyncFunctions.PUT_JSON_TO_FILE),
        #     description=f"Lambda to put content to EFS/S3 [{self.env_base}]",
        #     function_name=self.get_resource_name(DataSyncFunctions.PUT_JSON_TO_FILE),
        #     handler="aibs_informatics_aws_lambda.handlers.data_sync.put_json_to_file_handler",
        #     runtime=self.code_asset.default_runtime,
        #     code=self.code_asset.as_code,
        #     memory_size=128,
        #     timeout=cdk.Duration.seconds(30),
        #     environment=self.code_asset.environment,
        #     **data_sync_efs_lambda_arguments,
        # )
        # grant_bucket_access(self.buckets, self.put_json_to_file_fn.role, "rw")
        # grant_file_system_access(self.file_system, self.put_json_to_file_fn)

        # self.get_json_from_file_fn: lambda_.Function = lambda_.Function(
        #     self,
        #     self.get_construct_id(DataSyncFunctions.GET_JSON_FROM_FILE),
        #     description=f"Lambda to get content from EFS/S3 [{self.env_base}]",
        #     function_name=self.get_resource_name(DataSyncFunctions.GET_JSON_FROM_FILE),
        #     handler="aibs_informatics_aws_lambda.handlers.data_sync.get_json_from_file_handler",
        #     runtime=self.code_asset.default_runtime,
        #     code=self.code_asset.as_code,
        #     memory_size=128,
        #     timeout=cdk.Duration.seconds(30),
        #     environment=self.code_asset.environment,
        #     **data_sync_efs_lambda_arguments,
        # )
        # grant_bucket_access(self.buckets, self.get_json_from_file_fn.role, "rw")
        # grant_file_system_access(self.file_system, self.get_json_from_file_fn)

        self.data_sync_fn = lambda_.Function(
            self,
            self.get_construct_id(DataSyncFunctions.DATA_SYNC),
            description=f"Lambda to transfer data between Local (e.g. EFS) and remote (e.g. S3) [{self.env_base}]",
            function_name=self.get_resource_name(DataSyncFunctions.DATA_SYNC),
            handler="aibs_informatics_aws_lambda.handlers.data_sync.data_sync_handler",
            runtime=self.code_asset.default_runtime,
            code=self.code_asset.as_code,
            memory_size=10240,
            timeout=cdk.Duration.minutes(15),
            environment=self.code_asset.environment,
            **data_sync_efs_lambda_arguments,
        )
        grant_bucket_access(self.buckets, self.data_sync_fn.role, "rw")
        grant_file_system_access(self.file_system, self.data_sync_fn)

        self.batch_data_sync_fn_handler = (
            "aibs_informatics_aws_lambda.handlers.common.data_sync.batch_data_sync_handler"
        )
        self.batch_data_sync_fn = lambda_.Function(
            self,
            self.get_construct_id(DataSyncFunctions.BATCH_DATA_SYNC),
            description=f"Lambda to sync data between local (e.g. EFS) and remote (e.g. S3) in batch [{self.env_base}]",
            function_name=self.get_resource_name(DataSyncFunctions.BATCH_DATA_SYNC),
            handler=self.batch_data_sync_fn_handler,
            runtime=self.code_asset.default_runtime,
            code=self.code_asset.as_code,
            memory_size=10240,
            timeout=cdk.Duration.minutes(15),
            environment=self.code_asset.environment,
            **data_sync_efs_lambda_arguments,
        )
        grant_bucket_access(self.buckets, self.batch_data_sync_fn.role, "rw")
        grant_file_system_access(self.file_system, self.batch_data_sync_fn)

        self.prepare_batch_data_sync_fn = lambda_.Function(
            self,
            self.get_construct_id(DataSyncFunctions.PREPARE_BATCH_DATA_SYNC),
            description=f"Lambda to prepare batch data transfer [{self.env_base}]",
            function_name=self.get_resource_name(DataSyncFunctions.PREPARE_BATCH_DATA_SYNC),
            handler="aibs_informatics_aws_lambda.handlers.data_sync.prep_batch_data_sync_handler",
            runtime=self.code_asset.default_runtime,
            code=self.code_asset.as_code,
            memory_size=1024,
            timeout=cdk.Duration.minutes(10),
            environment=self.code_asset.environment,
            **data_sync_efs_lambda_arguments,
        )
        grant_bucket_access(self.buckets, self.prepare_batch_data_sync_fn.role, "rw")
        grant_file_system_access(self.file_system, self.prepare_batch_data_sync_fn)
        # allow read access to S3
        self.add_managed_policies(self.prepare_batch_data_sync_fn.role, "AmazonS3ReadOnlyAccess")

    def create_step_functions(self):

        start_pass_state = sfn.Pass(
            self,
            "Data Sync: Start",
            parameters={
                "request": sfn.JsonPath.string_at("$"),
            },
        )
        prep_batch_sync_task_name = "prep-batch-data-sync-requests"
        prep_batch_sync_lambda_task = stepfn_tasks.LambdaInvoke(
            self,
            "Prep Batch Data Sync",
            lambda_function=self.prepare_batch_data_sync_fn,
            payload=sfn.TaskInput.from_json_path_at("$.request"),
            payload_response_only=False,
            result_path=f"$.tasks.{prep_batch_sync_task_name}.response",
        )

        batch_sync_map_state = sfn.Map(
            self,
            "Batch Data Sync: Map Start",
            comment="Runs requests for batch sync in parallel",
            items_path=f"$.tasks.{prep_batch_sync_task_name}.response.Payload.requests",
            result_path=sfn.JsonPath.DISCARD,
        )
        batch_sync_map_state.iterator(
            BatchInvokedLambdaFunction(
                self,
                "Batch Data Sync Chain",
                env_base=self.env_base,
                image=f"{self.account}.dkr.ecr.{self.region}.amazonaws.com/aibs-informatics-aws-lambda:latest",
                name="batch-data-sync",
                handler=self.batch_data_sync_fn_handler,
                bucket_name=self._primary_bucket.bucket_name,
                job_queue=self._job_queue.job_queue_name,
                environment={EFS_MOUNT_PATH_VAR: EFS_MOUNT_PATH},
                memory=2048,
                vcpus=1,
                mount_points=[to_mount_point(EFS_MOUNT_PATH, False, EFS_VOLUME_NAME)],
                volumes=[
                    to_volume(
                        None,
                        EFS_VOLUME_NAME,
                        {
                            "fileSystemId": self.file_system.file_system_id,
                            "rootDirectory": "/",
                        },
                    )
                ],
                platform_capabilities=["FARGATE"]
                if any(
                    [
                        isinstance(ce.compute_environment, batch.FargateComputeEnvironment)
                        for ce in self._job_queue.compute_environments
                    ]
                )
                else None,
            )
        )
        # fmt: off
        definition = (
            start_pass_state
            .next(prep_batch_sync_lambda_task)
            .next(batch_sync_map_state)
        )
        # fmt: on

        data_sync_state_machine_name = self.get_resource_name("data-sync")
        self.data_sync_state_machine = sfn.StateMachine(
            self,
            self.env_base.get_construct_id("data-sync", "state-machine"),
            state_machine_name=data_sync_state_machine_name,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    self.get_construct_id(data_sync_state_machine_name, "state-loggroup"),
                    log_group_name=self.env_base.get_state_machine_log_group_name("data-sync"),
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH,
                )
            ),
            role=iam.Role(
                self,
                self.env_base.get_construct_id(data_sync_state_machine_name, "role"),
                assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
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
                            dynamodb_policy_statement(self.env_base),
                            s3_policy_statement(self.env_base),
                            lambda_policy_statement(self.env_base),
                        ]
                    ),
                },
            ),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.hours(12),
        )

    def create_rclone_step_function(self):

        start_pass_state = sfn.Pass(
            self,
            "Rclone: Start",
            parameters={
                "request": sfn.JsonPath.string_at("$"),
            },
        )
        default_args = {
            "command": "sync",
        }
        default_rclone_image = "rclone/rclone:latest"
        prep_batch_sync_task_name = "prep-batch-data-sync-requests"
        prep_batch_sync_lambda_task = stepfn_tasks.LambdaInvoke(
            self,
            "Prep Batch Data Sync",
            lambda_function=self.prepare_batch_data_sync_fn,
            payload=sfn.TaskInput.from_json_path_at("$.request"),
            payload_response_only=False,
            result_path=f"$.tasks.{prep_batch_sync_task_name}.response",
        )

        batch_sync_map_state = sfn.Map(
            self,
            "Batch Data Sync: Map Start",
            comment="Runs requests for batch sync in parallel",
            items_path=f"$.tasks.{prep_batch_sync_task_name}.response.Payload.requests",
            result_path=sfn.JsonPath.DISCARD,
        )
        batch_sync_map_state.iterator(
            BatchInvokedLambdaFunction(
                self,
                "Batch Data Sync Chain",
                env_base=self.env_base,
                image=f"{self.account}.dkr.ecr.{self.region}.amazonaws.com/aibs-informatics-aws-lambda:latest",
                name="batch-data-sync",
                handler=self.batch_data_sync_fn_handler,
                bucket_name=self._primary_bucket.bucket_name,
                job_queue=self._job_queue.job_queue_name,
                environment={EFS_MOUNT_PATH_VAR: EFS_MOUNT_PATH},
                memory=2048,
                vcpus=1,
                mount_points=[to_mount_point(EFS_MOUNT_PATH, False, EFS_VOLUME_NAME)],
                volumes=[
                    to_volume(
                        None,
                        EFS_VOLUME_NAME,
                        {
                            "fileSystemId": self.file_system.file_system_id,
                            "rootDirectory": "/",
                        },
                    )
                ],
                platform_capabilities=["FARGATE"]
                if any(
                    [
                        isinstance(ce.compute_environment, batch.FargateComputeEnvironment)
                        for ce in self._job_queue.compute_environments
                    ]
                )
                else None,
            )
        )
        # fmt: off
        definition = (
            start_pass_state
            .next(prep_batch_sync_lambda_task)
            .next(batch_sync_map_state)
        )
        # fmt: on

        data_sync_state_machine_name = self.get_resource_name("data-sync")
        self.data_sync_state_machine = sfn.StateMachine(
            self,
            self.env_base.get_construct_id("data-sync", "state-machine"),
            state_machine_name=data_sync_state_machine_name,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    self.get_construct_id(data_sync_state_machine_name, "state-loggroup"),
                    log_group_name=self.env_base.get_state_machine_log_group_name("data-sync"),
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH,
                )
            ),
            role=iam.Role(
                self,
                self.env_base.get_construct_id(data_sync_state_machine_name, "role"),
                assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
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
                            dynamodb_policy_statement(self.env_base),
                            s3_policy_statement(self.env_base),
                            lambda_policy_statement(self.env_base),
                        ]
                    ),
                },
            ),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.hours(12),
        )
