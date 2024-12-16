from dataclasses import dataclass
from typing import Iterable, List, Optional, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_efs as efs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics.batch import (
    BatchInvokedBaseFragment,
    BatchInvokedLambdaFunction,
)


def get_data_path_stats_fragment(
    scope: constructs.Construct,
    id: str,
    env_base: EnvBase,
    aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
    batch_job_queue: Union[batch.JobQueue, str],
    scaffolding_bucket: s3.Bucket,
    mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
    memory: int = 1024,
    vcpus: int = 1,
) -> BatchInvokedLambdaFunction:
    """Returns a BatchInvokedLambdaFunction fragment for getting data path stats of EFS/S3 path

    Args:
        scope (constructs.Construct): scope
        id (str): id of the fragment
        env_base (EnvBase): env base
        aibs_informatics_docker_asset (Union[ecr_assets.DockerImageAsset, str]): docker image asset or image uri
            that has the get_data_path_stats_handler function
        batch_job_queue (Union[batch.JobQueue, str]): default batch job queue or job queue name str that
            the batch job will be submitted to. This can be override by the payload.
        scaffolding_bucket (s3.Bucket): primary bucket used for request/response json blobs used in
        mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional): Default EFS volumes to mount.
            Defaults to None.
        memory (int, optional): memory needed. Defaults to 1024.
        vcpus (int, optional): vcpus needed. Defaults to 1.

    Returns:
        BatchInvokedLambdaFunction fragment for getting data path stats
    """
    fragment = BatchInvokedLambdaFunction(
        scope=scope,
        id=id,
        env_base=env_base,
        name="get-data-path-stats",
        image=(
            aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri
        ),
        handler="aibs_informatics_aws_lambda.handlers.data_sync.get_data_path_stats_handler",
        job_queue=(
            batch_job_queue if isinstance(batch_job_queue, str) else batch_job_queue.job_queue_name
        ),
        bucket_name=scaffolding_bucket.bucket_name,
        memory=memory,
        vcpus=vcpus,
        mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
    )
    return fragment


def outdated_data_path_scanner_fragment(
    scope: constructs.Construct,
    id: str,
    env_base: EnvBase,
    aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
    batch_job_queue: Union[batch.JobQueue, str],
    scaffolding_bucket: s3.Bucket,
    mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
    memory: int = 1024,
    vcpus: int = 1,
) -> BatchInvokedLambdaFunction:
    """Returns a BatchInvokedLambdaFunction fragment for scanning outdated data paths of EFS/S3 path root

    Args:
        scope (constructs.Construct): scope
        id (str): id of the fragment
        env_base (EnvBase): env base
        aibs_informatics_docker_asset (Union[ecr_assets.DockerImageAsset, str]): docker image asset or image uri
            that has the outdated_data_path_scanner_handler function
        batch_job_queue (Union[batch.JobQueue, str]): default batch job queue or job queue name str that
            the batch job will be submitted to. This can be override by the payload.
        scaffolding_bucket (s3.Bucket): primary bucket used for request/response json blobs used in
        mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional): Default EFS volumes to mount.
            Defaults to None.
        memory (int, optional): memory needed. Defaults to 1024.
        vcpus (int, optional): vcpus needed. Defaults to 1.

    Returns:
        BatchInvokedLambdaFunction fragment for scanning outdated data paths
    """

    fragment = BatchInvokedLambdaFunction(
        scope=scope,
        id=id,
        env_base=env_base,
        name="outdated-data-path-scanner",
        image=(
            aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri
        ),
        handler="aibs_informatics_aws_lambda.handlers.data_sync.outdated_data_path_scanner_handler",
        job_queue=(
            batch_job_queue if isinstance(batch_job_queue, str) else batch_job_queue.job_queue_name
        ),
        bucket_name=scaffolding_bucket.bucket_name,
        memory=memory,
        vcpus=vcpus,
        # mount_points=mount_points,
        # volumes=volumes,
        mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
    )
    return fragment


def remove_data_paths_fragment(
    scope: constructs.Construct,
    id: str,
    env_base: EnvBase,
    aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
    batch_job_queue: Union[batch.JobQueue, str],
    scaffolding_bucket: s3.Bucket,
    mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
    memory: int = 1024,
    vcpus: int = 1,
) -> BatchInvokedLambdaFunction:
    """Returns a BatchInvokedLambdaFunction fragment for removing data paths (EFS / S3) during execution of a Step Function

    Args:
        scope (constructs.Construct): scope
        id (str): id of the fragment
        env_base (EnvBase): env base
        aibs_informatics_docker_asset (Union[ecr_assets.DockerImageAsset, str]): docker image asset or image uri
            that has the remove_data_paths_handler function
        batch_job_queue (Union[batch.JobQueue, str]): default batch job queue or job queue name str that
            the batch job will be submitted to. This can be override by the payload.
        scaffolding_bucket (s3.Bucket): primary bucket used for request/response json blobs used in
        mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional): Default EFS volumes to mount.
            Defaults to None.
        memory (int, optional): memory needed. Defaults to 1024.
        vcpus (int, optional): vcpus needed. Defaults to 1.

    Returns:
        BatchInvokedLambdaFunction fragment for removing data paths
    """
    fragment = BatchInvokedLambdaFunction(
        scope=scope,
        id=id,
        env_base=env_base,
        name="remove-data-paths",
        image=(
            aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri
        ),
        handler="aibs_informatics_aws_lambda.handlers.data_sync.remove_data_paths_handler",
        job_queue=(
            batch_job_queue if isinstance(batch_job_queue, str) else batch_job_queue.job_queue_name
        ),
        bucket_name=scaffolding_bucket.bucket_name,
        memory=memory,
        vcpus=vcpus,
        mount_point_configs=list(mount_point_configs) if mount_point_configs else None,
    )
    return fragment


@dataclass
class CleanFileSystemTriggerConfig:
    file_system: efs.FileSystem
    path: str
    days_since_last_accessed: float = 3.0
    max_depth: Optional[int] = None
    min_depth: Optional[int] = None
    min_size_bytes_allowed: int = 0

    schedule: events.Schedule = events.Schedule.cron(minute="0", hour="9")

    def to_dict(self):
        d = {
            "path": f"{self.file_system.file_system_id}:{self.path}",
            "days_since_last_accessed": self.days_since_last_accessed,
        }
        if self.max_depth is not None:
            d["max_depth"] = self.max_depth
        if self.min_depth is not None:
            d["min_depth"] = self.min_depth
        if self.min_size_bytes_allowed > 0:
            d["min_size_bytes_allowed"] = self.min_size_bytes_allowed
        return d


@dataclass
class CleanFileSystemTriggerRuleConfig:
    rule_name: str
    trigger_configs: List[CleanFileSystemTriggerConfig]
    # https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html#cron-expressions
    # Want to run around 00:00 in PST by default
    schedule: events.Schedule = events.Schedule.cron(minute="0", hour="9")

    @property
    def description(self):
        return (
            "Daily trigger for EFS file cleanup (older than "
            f"{sorted({_.days_since_last_accessed for _ in self.trigger_configs})} "
            "days) for target subdirectories"
        )

    def create_rule(
        self,
        scope: constructs.Construct,
        clean_file_system_state_machine: sfn.StateMachine,
    ):
        return events.Rule(
            scope,
            self.rule_name,
            rule_name=self.rule_name,
            description=self.description,
            enabled=True,
            schedule=self.schedule,
            targets=[
                events_targets.SfnStateMachine(
                    clean_file_system_state_machine,
                    input=events.RuleTargetInput.from_object(config.to_dict()),
                )
                for config in self.trigger_configs
            ],  # type: ignore[arg-type]  # jsii implementation issue - https://github.com/aws/jsii/issues/4531
        )


class CleanFileSystemFragment(BatchInvokedBaseFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
        batch_job_queue: Union[batch.JobQueue, str],
        scaffolding_bucket: s3.Bucket,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
        memory: int = 1024,
        vcpus: int = 1,
    ) -> None:
        """Clean up the file system by scanning for outdated data paths and removing them

        Args:
            scope (Construct): construct scope
            id (str): id
            env_base (EnvBase): env base
            aibs_informatics_docker_asset (DockerImageAsset|str): Docker image asset or image uri
                str for the aibs informatics aws lambda
            batch_job_queue (JobQueue|str): Default batch job queue or job queue name str that
                the batch job will be submitted to. This can be override by the payload.
            primary_bucket (Bucket): Primary bucket used for request/response json blobs used in
                the batch invoked lambda function.
            mount_point_configs (Optional[Iterable[MountPointConfiguration]], optional):
                List of mount point configurations to use. These can be overridden in the payload.
            memory (int, optional): memory needed. Defaults to 1024.
                This memory value is used for both the outdated path scanner and removal of data paths.
            vcpus (int, optional): vcpus needed. Defaults to 1.
                This memory value is used for both the outdated path scanner and removal of data paths.
        """
        super().__init__(scope, id, env_base)

        aibs_informatics_image_uri = (
            aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri
        )

        start_pass_state = sfn.Pass(
            self,
            "Data Cleanup: Start",
        )

        self.outdated_data_path_scanner = outdated_data_path_scanner_fragment(
            self,
            "Scan for Outdated Data Paths",
            env_base=self.env_base,
            aibs_informatics_docker_asset=aibs_informatics_image_uri,
            batch_job_queue=batch_job_queue,
            scaffolding_bucket=scaffolding_bucket,
            mount_point_configs=mount_point_configs,
            memory=memory,
            vcpus=vcpus,
        )

        self.remove_data_paths = remove_data_paths_fragment(
            self,
            "Remove Data Paths",
            env_base=self.env_base,
            aibs_informatics_docker_asset=aibs_informatics_image_uri,
            batch_job_queue=batch_job_queue,
            scaffolding_bucket=scaffolding_bucket,
            mount_point_configs=mount_point_configs,
            memory=memory,
            vcpus=vcpus,
        )

        # fmt: off
        self.definition = (
            start_pass_state
            .next(self.outdated_data_path_scanner.enclose())
            .next(self.remove_data_paths.enclose())
        )
        # fmt: on
