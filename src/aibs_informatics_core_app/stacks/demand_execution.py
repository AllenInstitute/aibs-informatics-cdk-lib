from typing import Iterable, Optional, Union

import constructs
from aibs_informatics_aws_utils.constants.efs import (
    EFS_SCRATCH_PATH,
    EFS_SHARED_PATH,
    EFS_TMP_PATH,
)
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    BATCH_FULL_ACCESS_ACTIONS,
    batch_policy_statement,
)
from aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions import (
    AIBSInformaticsAssets,
)
from aibs_informatics_cdk_lib.constructs_.efs.file_system import (
    EFSEcosystem,
    MountPointConfiguration,
)
from aibs_informatics_cdk_lib.constructs_.service.compute import BatchCompute, LambdaCompute
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics import (
    BatchInvokedLambdaFunction,
    DataSyncFragment,
    DemandExecutionFragment,
)
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics.efs import (
    CleanFileSystemFragment,
    CleanFileSystemTriggerConfig,
    CleanFileSystemTriggerRuleConfig,
)
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack

DATA_SYNC_ASSET_NAME = "aibs_informatics_aws_lambda"

EFS_MOUNT_PATH = "/opt/efs"
EFS_VOLUME_NAME = "efs-file-system"


class DemandExecutionInfrastructureStack(EnvBaseStack):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        vpc: ec2.Vpc,
        buckets: Optional[Iterable[s3.Bucket]] = None,
        mount_point_configs: Optional[Iterable[MountPointConfiguration]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self.execution_compute = BatchCompute(
            self,
            id="demand",
            env_base=env_base,
            batch_name="demand",
            vpc=vpc,
            buckets=buckets,
            mount_point_configs=mount_point_configs,
        )
        self.infra_compute = LambdaCompute(
            self,
            id="demand-infra",
            env_base=env_base,
            vpc=vpc,
            batch_name="demand-infra",
            buckets=buckets,
            mount_point_configs=mount_point_configs,
            instance_role_policy_statements=[
                batch_policy_statement(env_base=env_base, actions=BATCH_FULL_ACCESS_ACTIONS),
            ],
        )


class DemandExecutionStack(EnvBaseStack):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        assets: AIBSInformaticsAssets,
        scaffolding_bucket: s3.Bucket,
        efs_ecosystem: EFSEcosystem,
        scaffolding_job_queue: Union[batch.JobQueue, str],
        data_sync_job_queue: Union[batch.JobQueue, str],
        execution_job_queue: Union[batch.JobQueue, str],
        **kwargs,
    ):
        super().__init__(scope, id, env_base, **kwargs)

        self.efs_ecosystem = efs_ecosystem

        self.execution_job_queue = (
            execution_job_queue
            if isinstance(execution_job_queue, str)
            else execution_job_queue.job_queue_arn
        )
        self.data_sync_job_queue = (
            data_sync_job_queue
            if isinstance(data_sync_job_queue, str)
            else data_sync_job_queue.job_queue_arn
        )
        self.scaffolding_job_queue = (
            scaffolding_job_queue
            if isinstance(scaffolding_job_queue, str)
            else scaffolding_job_queue.job_queue_arn
        )

        self._assets = assets

        root_mount_point_config = MountPointConfiguration.from_access_point(
            self.efs_ecosystem.root_access_point, EFS_MOUNT_PATH
        )
        shared_mount_point_config = MountPointConfiguration.from_access_point(
            self.efs_ecosystem.shared_access_point, "/opt/shared", read_only=True
        )
        scratch_mount_point_config = MountPointConfiguration.from_access_point(
            self.efs_ecosystem.scratch_access_point, "/opt/scratch"
        )

        batch_invoked_lambda_fragment = BatchInvokedLambdaFunction.with_defaults(
            self,
            "batch-invoked-lambda",
            env_base=self.env_base,
            name="batch-invoked-lambda",
            job_queue=self.scaffolding_job_queue,
            bucket_name=scaffolding_bucket.bucket_name,
            mount_point_configs=[root_mount_point_config],
        )
        self.batch_invoked_lambda_state_machine = batch_invoked_lambda_fragment.to_state_machine(
            "batch-invoked-lambda-state-machine"
        )

        data_sync = DataSyncFragment(
            self,
            "data-sync",
            env_base=self.env_base,
            aibs_informatics_docker_asset=self._assets.docker_assets.AIBS_INFORMATICS_AWS_LAMBDA,
            batch_job_queue=self.data_sync_job_queue,
            scaffolding_bucket=scaffolding_bucket,
            mount_point_configs=[root_mount_point_config],
        )

        self.data_sync_state_machine = data_sync.to_state_machine("data-sync-v2")

        demand_execution = DemandExecutionFragment(
            self,
            "demand-execution",
            env_base=self.env_base,
            aibs_informatics_docker_asset=self._assets.docker_assets.AIBS_INFORMATICS_AWS_LAMBDA,
            scaffolding_bucket=scaffolding_bucket,
            scaffolding_job_queue=self.scaffolding_job_queue,
            batch_invoked_lambda_state_machine=self.batch_invoked_lambda_state_machine,
            data_sync_state_machine=self.data_sync_state_machine,
            shared_mount_point_config=shared_mount_point_config,
            scratch_mount_point_config=scratch_mount_point_config,
            tags={
                "ai:cost-allocation:aibs-informatics-service": "n/a",
                "ai:cost-allocation:aibs-informatics-workflow-type": "$.execution_type",
                "ai:cost-allocation:aibs-informatics-workflow-id": "$.execution_id",
            },
        )
        self.demand_execution_state_machine = demand_execution.to_state_machine("demand-execution")

        ## EFS Cleanup

        clean_fs = CleanFileSystemFragment(
            self,
            "clean-file-system",
            env_base=self.env_base,
            aibs_informatics_docker_asset=self._assets.docker_assets.AIBS_INFORMATICS_AWS_LAMBDA,
            batch_job_queue=self.execution_job_queue,
            scaffolding_bucket=scaffolding_bucket,
            mount_point_configs=[root_mount_point_config],
        )
        self.clean_file_system_state_machine = clean_fs.to_state_machine("clean-file-system")

        CleanFileSystemTriggerRuleConfig(
            rule_name="clean-file-system-trigger",
            trigger_configs=[
                CleanFileSystemTriggerConfig(
                    file_system=self.efs_ecosystem.file_system,
                    path=path,
                    days_since_last_accessed=days_since_last_accessed,
                    max_depth=max_depth,
                    min_depth=min_depth,
                    min_size_bytes_allowed=0,
                )
                for path, days_since_last_accessed, min_depth, max_depth in [
                    (EFS_TMP_PATH, 3.0, 1, 1),
                    (EFS_SCRATCH_PATH, 3.0, 1, 1),
                    (f"{EFS_SCRATCH_PATH}/tmp", 3.0, 1, 1),
                    (EFS_SHARED_PATH, 3.0, 1, 1),
                ]
            ],
        ).create_rule(self, clean_file_system_state_machine=self.clean_file_system_state_machine)
