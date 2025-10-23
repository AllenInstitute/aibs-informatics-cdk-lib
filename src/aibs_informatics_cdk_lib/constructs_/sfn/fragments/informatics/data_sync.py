from typing import Iterable, List, Optional, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    SFN_STATES_EXECUTION_ACTIONS,
    SFN_STATES_READ_ACCESS_ACTIONS,
    sfn_policy_statement,
)
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins
from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics.batch import (
    BatchInvokedBaseFragment,
    BatchInvokedLambdaFunction,
)


class DataSyncFragment(BatchInvokedBaseFragment, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
        batch_job_queue: Union[batch.JobQueue, str],
        scaffolding_bucket: s3.Bucket,
        batch_job_role: Optional[Union[iam.Role, str]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
    ) -> None:
        """Sync data from one s3 bucket to another


        Args:
            scope (Construct): construct scope
            id (str): id
            env_base (EnvBase): env base
            aibs_informatics_docker_asset (DockerImageAsset|str): Docker image asset or image uri
                str for the aibs informatics aws lambda
            batch_job_queue (JobQueue|str): Default batch job queue or job queue name str that
                the batch job will be submitted to. This can be override by the payload.
            scaffolding_bucket (Bucket): Primary bucket used for request/response json blobs used
                in the batch invoked lambda function.
            batch_job_role (Optional[IRole|str], optional): Optional role to use for the batch job.
                If not provided, the default role created by the batch compute construct will be
                used.
            mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional):
                List of mount point configurations to use. These can be overridden in the payload.

        """
        super().__init__(scope, id, env_base)

        aibs_informatics_image_uri = (
            aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri
        )

        self.batch_job_queue_name = (
            batch_job_queue if isinstance(batch_job_queue, str) else batch_job_queue.job_queue_name
        )

        start = sfn.Pass(
            self,
            "Input Restructure",
            parameters={
                "handler": "aibs_informatics_aws_lambda.handlers.data_sync.data_sync_handler",
                "image": aibs_informatics_image_uri,
                "payload": sfn.JsonPath.object_at("$"),
            },
        )

        self.fragment = BatchInvokedLambdaFunction.with_defaults(
            self,
            "Data Sync",
            env_base=self.env_base,
            name="data-sync",
            job_queue=self.batch_job_queue_name,
            bucket_name=scaffolding_bucket.bucket_name,
            handler_path="$.handler",
            image_path="$.image",
            payload_path="$.payload",
            memory="1024",
            vcpus="1",
            mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
            job_role_arn=(
                batch_job_role if isinstance(batch_job_role, str) else batch_job_role.role_arn
            )
            if batch_job_role
            else None,
            environment={
                EnvBase.ENV_BASE_KEY: self.env_base,
                "AWS_REGION": self.aws_region,
                "AWS_ACCOUNT_ID": self.aws_account,
            },
        )

        self.definition = start.next(self.fragment.to_single_state())

    @property
    def required_managed_policies(self) -> List[Union[iam.IManagedPolicy, str]]:
        return [
            *super().required_managed_policies,
            *[_ for _ in self.fragment.required_managed_policies],
        ]

    @property
    def required_inline_policy_statements(self) -> List[iam.PolicyStatement]:
        return [
            *self.fragment.required_inline_policy_statements,
            *super().required_inline_policy_statements,
            sfn_policy_statement(
                self.env_base,
                actions=SFN_STATES_EXECUTION_ACTIONS + SFN_STATES_READ_ACCESS_ACTIONS,
            ),
        ]


class DistributedDataSyncFragment(BatchInvokedBaseFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
        batch_job_queue: Union[batch.JobQueue, str],
        scaffolding_bucket: s3.Bucket,
        batch_job_role: Optional[Union[str, iam.Role]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
    ) -> None:
        """Sync data from one s3 bucket to another using distributed batch jobs

        Args:
            scope (constructs.Construct): construct scope
            id (str): id
            env_base (EnvBase): env base
            aibs_informatics_docker_asset (DockerImageAsset|str): Docker image asset or image uri
                str for the aibs informatics aws lambda
            batch_job_queue (JobQueue|str): Default batch job queue or job queue name str that
                the batch job will be submitted to. This can be override by the payload.
            scaffolding_bucket (Bucket): Primary bucket used for request/response json blobs used
                in the batch invoked lambda function.
            batch_job_role (Optional[IRole|str], optional): Optional role to use for the batch job.
                If not provided, the default role created by the batch compute construct will be
                used.
            mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional):
                List of mount point configurations to use. These can be overridden in the payload.
        """
        super().__init__(scope, id, env_base)
        start_pass_state = sfn.Pass(
            self,
            f"{id}: Start",
            parameters={
                "request": sfn.JsonPath.object_at("$"),
            },
        )
        prep_batch_sync_task_name = "prep-batch-data-sync-requests"

        prep_batch_sync = BatchInvokedLambdaFunction(
            scope=scope,
            id=f"{id}: Prep Batch Data Sync",
            env_base=env_base,
            name=prep_batch_sync_task_name,
            payload_path="$.request",
            image=(
                aibs_informatics_docker_asset
                if isinstance(aibs_informatics_docker_asset, str)
                else aibs_informatics_docker_asset.image_uri
            ),
            handler="aibs_informatics_aws_lambda.handlers.data_sync.prepare_batch_data_sync_handler",
            job_queue=(
                batch_job_queue
                if isinstance(batch_job_queue, str)
                else batch_job_queue.job_queue_name
            ),
            bucket_name=scaffolding_bucket.bucket_name,
            memory=1024,
            vcpus=1,
            mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
            job_role_arn=(
                batch_job_role if isinstance(batch_job_role, str) else batch_job_role.role_arn
            )
            if batch_job_role
            else None,
        ).enclose(result_path=f"$.tasks.{prep_batch_sync_task_name}.response")

        batch_sync_map_state = sfn.Map(
            self,
            f"{id}: Batch Data Sync: Map Start",
            comment="Runs requests for batch sync in parallel",
            items_path=f"$.tasks.{prep_batch_sync_task_name}.response.requests",
            result_path=sfn.JsonPath.DISCARD,
        )

        batch_sync_map_state.iterator(
            BatchInvokedLambdaFunction(
                scope=scope,
                id=f"{id}: Batch Data Sync",
                env_base=env_base,
                name="batch-data-sync",
                payload_path="$",
                image=(
                    aibs_informatics_docker_asset
                    if isinstance(aibs_informatics_docker_asset, str)
                    else aibs_informatics_docker_asset.image_uri
                ),
                handler="aibs_informatics_aws_lambda.handlers.data_sync.batch_data_sync_handler",
                job_queue=(
                    batch_job_queue
                    if isinstance(batch_job_queue, str)
                    else batch_job_queue.job_queue_name
                ),
                bucket_name=scaffolding_bucket.bucket_name,
                memory=4096,
                vcpus=2,
                mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
                job_role_arn=(
                    batch_job_role if isinstance(batch_job_role, str) else batch_job_role.role_arn
                )
                if batch_job_role
                else None,
            )
        )
        # fmt: off
        self.definition = (
            start_pass_state
            .next(prep_batch_sync)
            .next(batch_sync_map_state)
        )
        # fmt: on
