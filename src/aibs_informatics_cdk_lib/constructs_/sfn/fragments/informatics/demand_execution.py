from typing import Any, Dict, List, Literal, Optional, Sequence, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch as batch
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    SFN_STATES_EXECUTION_ACTIONS,
    SFN_STATES_READ_ACCESS_ACTIONS,
    batch_policy_statement,
    s3_policy_statement,
    sfn_policy_statement,
)
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins
from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment
from aibs_informatics_cdk_lib.constructs_.sfn.states.common import CommonOperation


class DemandExecutionFragment(EnvBaseStateMachineFragment, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
        scaffolding_bucket: s3.Bucket,
        scaffolding_job_queue: Union[batch.JobQueue, str],
        batch_invoked_lambda_state_machine: sfn.StateMachine,
        data_sync_state_machine: sfn.StateMachine,
        shared_mount_point_config: Union[
            MountPointConfiguration, Sequence[MountPointConfiguration]
        ],
        scratch_mount_point_config: Union[
            MountPointConfiguration, Sequence[MountPointConfiguration]
        ],
        tmp_mount_point_config: Optional[
            Union[MountPointConfiguration, Sequence[MountPointConfiguration]]
        ] = None,
        file_system_selection_strategy: Optional[Literal["RANDOM", "LEAST_UTILIZED"]] = None,
        context_manager_configuration: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(scope, id, env_base)

        # ----------------- Validation -----------------

        shared_mount_point_configs = (
            [shared_mount_point_config]
            if isinstance(shared_mount_point_config, MountPointConfiguration)
            else shared_mount_point_config
        )

        scratch_mount_point_configs = (
            [scratch_mount_point_config]
            if isinstance(scratch_mount_point_config, MountPointConfiguration)
            else scratch_mount_point_config
        )
        tmp_mount_point_configs = (
            [tmp_mount_point_config]
            if isinstance(tmp_mount_point_config, MountPointConfiguration)
            else tmp_mount_point_config
        )

        # ------------------- Setup -------------------

        config_scaffolding_path = "config.scaffolding"
        config_setup_results_path = f"{config_scaffolding_path}.setup_results"
        config_batch_args_path = f"{config_setup_results_path}.batch_args"

        config_cleanup_results_path = "tasks.cleanup.cleanup_results"

        # Create common kwargs for the batch invoked lambda functions
        # - specify the bucket name and job queue
        # - specify the mount points and volumes if provided
        batch_invoked_lambda_kwargs: dict[str, Any] = {
            "bucket_name": scaffolding_bucket.bucket_name,
            "image": aibs_informatics_docker_asset
            if isinstance(aibs_informatics_docker_asset, str)
            else aibs_informatics_docker_asset.image_uri,
            "job_queue": scaffolding_job_queue
            if isinstance(scaffolding_job_queue, str)
            else scaffolding_job_queue.job_queue_name,
        }

        # Create request input for the demand scaffolding
        file_system_configurations: Dict[str, Any] = {
            "selection_strategy": file_system_selection_strategy or "LEAST_UTILIZED",
        }

        mount_points = []
        volumes = []

        # update file system configurations for scaffolding function
        file_system_configurations["shared"] = []
        for i, shared_mount_point_config in enumerate(shared_mount_point_configs):
            file_system_configurations["shared"].append(
                {
                    "file_system": shared_mount_point_config.file_system_id,
                    "access_point": shared_mount_point_config.access_point_id,
                    "container_path": shared_mount_point_config.mount_point,
                }
            )

            # add to mount point and volumes list for batch invoked lambda functions
            mount_points.append(
                shared_mount_point_config.to_batch_mount_point(f"shared{i}", sfn_format=True)
            )
            volumes.append(
                shared_mount_point_config.to_batch_volume(f"shared{i}", sfn_format=True)
            )

        file_system_configurations["scratch"] = []
        for i, scratch_mount_point_config in enumerate(scratch_mount_point_configs):
            file_system_configurations["scratch"].append(
                {
                    "file_system": scratch_mount_point_config.file_system_id,
                    "access_point": scratch_mount_point_config.access_point_id,
                    "container_path": scratch_mount_point_config.mount_point,
                }
            )

            # add to mount point and volumes list for batch invoked lambda functions
            mount_points.append(
                scratch_mount_point_config.to_batch_mount_point(f"scratch{i}", sfn_format=True)
            )
            volumes.append(
                scratch_mount_point_config.to_batch_volume(f"scratch{i}", sfn_format=True)
            )

        if tmp_mount_point_configs:
            file_system_configurations["tmp"] = []
            for i, tmp_mount_point_config in enumerate(tmp_mount_point_configs):
                file_system_configurations["tmp"].append(
                    {
                        "file_system": tmp_mount_point_config.file_system_id,
                        "access_point": tmp_mount_point_config.access_point_id,
                        "container_path": tmp_mount_point_config.mount_point,
                    }
                )

                # add to mount point and volumes list for batch invoked lambda functions
                mount_points.append(
                    tmp_mount_point_config.to_batch_mount_point(f"tmp{i}", sfn_format=True)
                )
                volumes.append(
                    tmp_mount_point_config.to_batch_volume(f"tmp{i}", sfn_format=True)
                )

        batch_invoked_lambda_kwargs["mount_points"] = mount_points
        batch_invoked_lambda_kwargs["volumes"] = volumes

        request = {
            "demand_execution": sfn.JsonPath.object_at("$"),
            "file_system_configurations": file_system_configurations,
        }
        if context_manager_configuration:
            request["context_manager_configuration"] = context_manager_configuration

        start_state = sfn.Pass(
            self,
            "Start Demand Batch Task",
            parameters={
                "request": request,
            },
        )

        prep_scaffolding_task = CommonOperation.enclose_chainable(
            self,
            "Prepare Demand Scaffolding",
            sfn.Pass(
                self,
                "Pass: Prepare Demand Scaffolding",
                parameters={
                    "handler": "aibs_informatics_aws_lambda.handlers.demand.scaffolding.handler",
                    "payload": sfn.JsonPath.object_at("$"),
                    **batch_invoked_lambda_kwargs,
                },
            ).next(
                sfn_tasks.StepFunctionsStartExecution(
                    self,
                    "SM: Prepare Demand Scaffolding",
                    state_machine=batch_invoked_lambda_state_machine,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    associate_with_parent=False,
                    input_path="$",
                    output_path="$.Output",
                )
            ),
            input_path="$.request",
            result_path=f"$.{config_scaffolding_path}",
        )

        create_def_and_prepare_job_args_task = CommonOperation.enclose_chainable(
            self,
            "Create Definition and Prep Job Args",
            sfn.Pass(
                self,
                "Pass: Create Definition and Prep Job Args",
                parameters={
                    "handler": "aibs_informatics_aws_lambda.handlers.batch.create.handler",
                    "payload": sfn.JsonPath.object_at("$"),
                    **batch_invoked_lambda_kwargs,
                },
            ).next(
                sfn_tasks.StepFunctionsStartExecution(
                    self,
                    "SM: Create Definition and Prep Job Args",
                    state_machine=batch_invoked_lambda_state_machine,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    associate_with_parent=False,
                    input_path="$",
                    output_path="$.Output",
                )
            ),
            input_path="$.batch_create_request",
            result_path="$",
        )

        setup_tasks = (
            sfn.Parallel(
                self,
                "Execution Setup Steps",
                input_path=f"$.{config_scaffolding_path}.setup_configs",
                result_path=f"$.{'.'.join(config_batch_args_path.split('.')[:-1])}",
                result_selector={f'{config_batch_args_path.split(".")[-1]}.$': "$[0]"},
            )
            .branch(create_def_and_prepare_job_args_task)
            .branch(
                sfn.Map(
                    self,
                    "Transfer Inputs TO Batch Job",
                    items_path="$.data_sync_requests",
                ).iterator(
                    sfn_tasks.StepFunctionsStartExecution(
                        self,
                        "Transfer Input",
                        state_machine=data_sync_state_machine,
                        integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                        associate_with_parent=False,
                        result_path=sfn.JsonPath.DISCARD,
                    )
                )
            )
        )

        execution_task = sfn.CustomState(
            self,
            "Submit Batch Job",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                # fmt: off
                "Parameters": {
                    "JobName.$": sfn.JsonPath.string_at(f"$.{config_batch_args_path}.job_name"),
                    "JobDefinition.$": sfn.JsonPath.string_at(f"$.{config_batch_args_path}.job_definition_arn"),
                    "JobQueue.$": sfn.JsonPath.string_at(f"$.{config_batch_args_path}.job_queue_arn"),
                    "Parameters.$": sfn.JsonPath.object_at(f"$.{config_batch_args_path}.parameters"),
                    "ContainerOverrides.$": sfn.JsonPath.object_at(f"$.{config_batch_args_path}.container_overrides"),
                },
                # fmt: on
                "ResultPath": "$.tasks.batch_submit_task",
            },
        )

        cleanup_tasks = sfn.Chain.start(
            sfn.Map(
                self,
                "Transfer Results FROM Batch Job",
                input_path=f"$.{config_scaffolding_path}.cleanup_configs.data_sync_requests",
                result_path=f"$.{config_cleanup_results_path}.transfer_results",
            ).iterator(
                sfn_tasks.StepFunctionsStartExecution(
                    self,
                    "Transfer Result",
                    state_machine=data_sync_state_machine,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    associate_with_parent=False,
                    result_path=sfn.JsonPath.DISCARD,
                )
            )
        ).next(
            sfn.Choice(self, "Cleanup Choice")
            .when(
                condition=sfn.Condition.is_present(
                    f"$.{config_scaffolding_path}.cleanup_configs.remove_data_paths_requests"
                ),
                next=sfn.Chain.start(
                    sfn.Map(
                        self,
                        "Map: Cleanup Data Paths",
                        input_path=f"$.{config_scaffolding_path}.cleanup_configs.remove_data_paths_requests",
                        result_path=f"$.{config_cleanup_results_path}.remove_data_paths_results",
                    ).iterator(
                        CommonOperation.enclose_chainable(
                            self,
                            "Cleanup Data Path",
                            definition=sfn.Pass(
                                self,
                                "Pass: Cleanup Data Path",
                                parameters={
                                    "handler": "aibs_informatics_aws_lambda.handlers.data_sync.remove_data_paths_handler",
                                    "payload": sfn.JsonPath.object_at("$"),
                                    **batch_invoked_lambda_kwargs,
                                },
                            ).next(
                                sfn_tasks.StepFunctionsStartExecution(
                                    self,
                                    "SM: Cleanup Data Path",
                                    state_machine=batch_invoked_lambda_state_machine,
                                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                    associate_with_parent=False,
                                    input_path="$",
                                    output_path="$.Output",
                                )
                            ),
                        )
                    )
                ),
            )
            .otherwise(sfn.Pass(self, "No Data Paths to Cleanup"))
        )

        # fmt: off
        definition = (
            start_state
            .next(prep_scaffolding_task)
            .next(setup_tasks)
            .next(execution_task)
            .next(cleanup_tasks)
        )
        # fmt: on
        self.definition = definition

    @property
    def required_inline_policy_statements(self) -> List[iam.PolicyStatement]:
        return [
            *super().required_inline_policy_statements,
            batch_policy_statement(self.env_base),
            s3_policy_statement(self.env_base),
            sfn_policy_statement(
                self.env_base,
                actions=SFN_STATES_EXECUTION_ACTIONS + SFN_STATES_READ_ACCESS_ACTIONS,
            ),
        ]
