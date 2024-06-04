from typing import TYPE_CHECKING, Any, Iterable, List, Literal, Mapping, Optional, Sequence, Union

import constructs
from aibs_informatics_aws_utils.constants.lambda_ import (
    AWS_LAMBDA_EVENT_PAYLOAD_KEY,
    AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_KEY,
    AWS_LAMBDA_FUNCTION_NAME_KEY,
)
from aibs_informatics_aws_utils.constants.s3 import S3_SCRATCH_KEY_PREFIX
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha as batch
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
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import (
    AWSBatchMixins,
    SubmitJobFragment,
)
from aibs_informatics_cdk_lib.constructs_.sfn.states.common import CommonOperation
from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3Operation

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import MountPointTypeDef, VolumeTypeDef
else:
    MountPointTypeDef = dict
    VolumeTypeDef = dict


class BatchInvokedBaseFragment(EnvBaseStateMachineFragment, EnvBaseConstructMixins):
    @property
    def required_managed_policies(self) -> Sequence[Union[iam.IManagedPolicy, str]]:
        return super().required_managed_policies

    @property
    def required_inline_policy_statements(self) -> List[iam.PolicyStatement]:
        return [
            *super().required_inline_policy_statements,
            batch_policy_statement(self.env_base),
            s3_policy_statement(self.env_base),
        ]


class BatchInvokedLambdaFunction(BatchInvokedBaseFragment, AWSBatchMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        image: str,
        handler: str,
        job_queue: str,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        payload_path: Optional[str] = None,
        command: Optional[Union[List[str], str]] = None,
        environment: Optional[Mapping[str, str]] = None,
        memory: Optional[Union[int, str]] = None,
        vcpus: Optional[Union[int, str]] = None,
        mount_points: Optional[Union[List[MountPointTypeDef], str]] = None,
        volumes: Optional[Union[List[VolumeTypeDef], str]] = None,
        mount_point_configs: Optional[List[MountPointConfiguration]] = None,
        platform_capabilities: Optional[Union[List[Literal["EC2", "FARGATE"]], str]] = None,
    ) -> None:
        """Invoke a command on image via batch with a payload from s3

        This fragment creates a state machine fragment that:
            1. Puts a payload to s3
            2. Submits a batch job
            3. Gets the response from s3

        The payload is written to s3://<bucket_name>/<key_prefix>/<execution_name>/request.json
        The response is read from s3://<bucket_name>/<key_prefix>/<execution_name>/response.json

        The batch job will be fed the following environment variables:
            - AWS_LAMBDA_FUNCTION_NAME: name of lambda function
            - AWS_LAMBDA_FUNCTION_HANDLER: handler of lambda function
            - AWS_LAMBDA_EVENT_PAYLOAD: The s3 location of the event payload
            - AWS_LAMBDA_EVENT_RESPONSE_LOCATION: The s3 location to write the response to

        IMPORTANT:
            - Batch job queue / compute environment must have permissions to read/write to the bucket.


        Args:
            scope (Construct): construct scope
            id (str): id
            env_base (EnvBase): env base
            name (str): Name of the lambda function. This can be a reference path (e.g. "$.name")
            image (str): Image URI or name. This can be a reference path (e.g. "$.image")
            handler (str): handler of lambda function. This should describe a fully qualified path to function handler. This can be a reference path (e.g. "$.handler")
            job_queue (str): Job queue to submit job to. This can be a reference path (e.g. "$.job_queue")
            bucket_name (str): S3 Bucket name to write payload to and read response from. This can be a reference path (e.g. "$.bucket_name")
            key_prefix (str | None): Key prefix to write payload to and read response from. If not provided, `scratch/` is used. Can be a reference path (e.g. "$.key_prefix")
            payload_path (str | None): Optionally specify the reference path of the event payload. Defaults to "$".
            command (List[str] | str | None): Command to run in container. Can be a reference path (e.g. "$.command"). If unspecified, the container's CMD is used.
            environment (Mapping[str, str] | None): Additional environment variables to specify. These are added to default environment variables.
            memory (int | str | None): Memory in MiB (either int or reference path str). Defaults to None.
            vcpus (int | str | None): Number of vCPUs (either int or reference path str). Defaults to None.
            mount_points (List[MountPointTypeDef] | None): List of mount points to add to state machine. Defaults to None.
            volumes (List[VolumeTypeDef] | None): List of volumes to add to state machine. Defaults to None.
        """
        super().__init__(scope, id, env_base)
        key_prefix = key_prefix or S3_SCRATCH_KEY_PREFIX
        request_key = sfn.JsonPath.format(
            f"{key_prefix}{{}}/request.json", sfn.JsonPath.execution_name
        )
        response_key = sfn.JsonPath.format(
            f"{key_prefix}{{}}/response.json", sfn.JsonPath.execution_name
        )

        if mount_point_configs:
            if mount_points or volumes:
                raise ValueError("Cannot specify both mount_point_configs and mount_points")
            mount_points, volumes = self.convert_to_mount_point_and_volumes(mount_point_configs)

        put_payload = S3Operation.put_payload(
            self,
            f"{id} Put Request to S3",
            payload=payload_path or sfn.JsonPath.entire_payload,
            bucket_name=bucket_name,
            key=request_key,
            result_path="$.taskResult.put",
        )

        default_environment = {
            AWS_LAMBDA_FUNCTION_NAME_KEY: name,
            AWS_LAMBDA_FUNCTION_HANDLER_KEY: handler,
            AWS_LAMBDA_EVENT_PAYLOAD_KEY: sfn.JsonPath.format(
                "s3://{}/{}",
                sfn.JsonPath.string_at("$.taskResult.put.Bucket"),
                sfn.JsonPath.string_at("$.taskResult.put.Key"),
            ),
            AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY: sfn.JsonPath.format(
                "s3://{}/{}", bucket_name, response_key
            ),
            EnvBase.ENV_BASE_KEY: self.env_base,
            "AWS_REGION": self.aws_region,
            "AWS_ACCOUNT_ID": self.aws_account,
        }

        submit_job = SubmitJobFragment(
            self,
            f"{id} Batch",
            env_base=env_base,
            name=name,
            job_queue=job_queue,
            command=command or [],
            image=image,
            environment={
                **(environment if environment else {}),
                **default_environment,
            },
            memory=memory,
            vcpus=vcpus,
            mount_points=mount_points or [],
            volumes=volumes or [],
            platform_capabilities=platform_capabilities,
        )

        get_response = S3Operation.get_payload(
            self,
            f"{id}",
            bucket_name=bucket_name,
            key=response_key,
        ).to_single_state(
            f"{id} Get Response from S3",
            output_path="$[0]",
        )

        self.definition = put_payload.next(submit_job).next(get_response)

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states

    @classmethod
    def with_defaults(
        cls,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        job_queue: str,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        image_path: Optional[str] = None,
        handler_path: Optional[str] = None,
        payload_path: Optional[str] = None,
        command: Optional[List[str]] = None,
        memory: str = "1024",
        vcpus: str = "1",
        environment: Optional[Mapping[str, str]] = None,
        mount_point_configs: Optional[List[MountPointConfiguration]] = None,
        platform_capabilities: Optional[List[Literal["EC2", "FARGATE"]]] = None,
    ) -> "BatchInvokedLambdaFunction":
        defaults: dict[str, Any] = {}

        defaults["job_queue"] = job_queue
        defaults["memory"] = memory
        defaults["vcpus"] = vcpus
        defaults["environment"] = environment or {}
        defaults["platform_capabilities"] = platform_capabilities or ["EC2"]
        defaults["bucket_name"] = bucket_name

        defaults["command"] = command if command else []

        if mount_point_configs:
            mount_points, volumes = cls.convert_to_mount_point_and_volumes(mount_point_configs)
            defaults["mount_points"] = mount_points
            defaults["volumes"] = volumes

        fragment = BatchInvokedLambdaFunction(
            scope,
            id,
            env_base=env_base,
            name=name,
            image=sfn.JsonPath.string_at("$.image"),
            handler=sfn.JsonPath.string_at("$.handler"),
            job_queue=sfn.JsonPath.string_at("$.merged.job_queue"),
            bucket_name=sfn.JsonPath.string_at("$.merged.bucket_name"),
            key_prefix=key_prefix,
            payload_path=sfn.JsonPath.string_at("$.payload"),
            command=sfn.JsonPath.string_at("$.merged.command"),
            environment=environment,
            memory=sfn.JsonPath.string_at("$.merged.memory"),
            vcpus=sfn.JsonPath.string_at("$.merged.vcpus"),
            # TODO: Handle GPU parameter better - right now, we cannot handle cases where it is
            # not specified. Setting to zero causes issues with the Batch API.
            # If it is set to zero, then the json list of resources are not properly set.
            # gpu=sfn.JsonPath.string_at("$.merged.gpu"),
            mount_points=sfn.JsonPath.string_at("$.merged.mount_points"),
            volumes=sfn.JsonPath.string_at("$.merged.volumes"),
            platform_capabilities=sfn.JsonPath.string_at("$.merged.platform_capabilities"),
        )

        start = sfn.Pass(
            fragment,
            f"Start {id}",
            parameters={
                "image": sfn.JsonPath.string_at(image_path or "$.image"),
                "handler": sfn.JsonPath.string_at(handler_path or "$.handler"),
                "payload": sfn.JsonPath.object_at(payload_path or "$.payload"),
                # We will merge the rest with the defaults
                "input": sfn.JsonPath.object_at("$"),
            },
        )

        merge_chain = CommonOperation.merge_defaults(
            fragment,
            f"Merge {id}",
            input_path="$.input",
            defaults=defaults,
            result_path="$.merged",
        )

        fragment.definition = start.next(merge_chain).next(fragment.definition)
        return fragment


class BatchInvokedExecutorFragment(BatchInvokedBaseFragment, AWSBatchMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        image: str,
        executor: str,
        job_queue: str,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        payload_path: Optional[str] = None,
        environment: Optional[Union[Mapping[str, str], str]] = None,
        memory: Optional[Union[int, str]] = None,
        vcpus: Optional[Union[int, str]] = None,
        mount_point_configs: Optional[List[MountPointConfiguration]] = None,
        mount_points: Optional[List[MountPointTypeDef]] = None,
        volumes: Optional[List[VolumeTypeDef]] = None,
    ) -> None:
        """Invoke an executor in an image via batch with a payload from s3

        This targets any subclassing of `aibs_informatics_core.executors.base.ExecutorBase`
        - https://github.com/AllenInstitute/aibs-informatics-core/blob/main/src/aibs_informatics_core/executors/base.py


        This fragment creates a state machine fragment that:
            1. Puts a payload to s3
            2. Submits a batch job
            3. Gets the response from s3

        The payload is written to s3://<bucket_name>/<key_prefix>/<execution_name>/request.json
        The response is read from s3://<bucket_name>/<key_prefix>/<execution_name>/response.json

        IMPORTANT:
            - Batch job queue / compute environment must have permissions to read/write to the bucket.

        Args:
            scope (Construct): construct scope
            id (str): id
            env_base (EnvBase): env base
            name (str): Name of the lambda function. This can be a reference path (e.g. "$.name")
            image (str): Image URI or name. This can be a reference path (e.g. "$.image")
            executor (str): qualified name of executor class. This should describe a fully qualified path to function handler. This can be a reference path (e.g. "$.handler")
            job_queue (str): Job queue to submit job to. This can be a reference path (e.g. "$.job_queue")
            bucket_name (str): S3 Bucket name to write payload to and read response from. This can be a reference path (e.g. "$.bucket_name")
            key_prefix (str | None): Key prefix to write payload to and read response from. If not provided, `scratch/` is used. Can be a reference path (e.g. "$.key_prefix")
            payload_path (str | None): Optionally specify the reference path of the event payload. Defaults to "$".
            command (List[str] | str | None): Command to run in container. Can be a reference path (e.g. "$.command"). If unspecified, the container's CMD is used.
            environment (Mapping[str, str] | str | None): environment variables to specify. This can be a reference path (e.g. "$.environment")
            memory (int | str | None): Memory in MiB (either int or reference path str). Defaults to None.
            vcpus (int | str | None): Number of vCPUs (either int or reference path str). Defaults to None.
            mount_points (List[MountPointTypeDef] | None): List of mount points to add to state machine. Defaults to None.
            volumes (List[VolumeTypeDef] | None): List of volumes to add to state machine. Defaults to None.
        """
        super().__init__(scope, id, env_base)
        key_prefix = key_prefix or S3_SCRATCH_KEY_PREFIX
        request_key = sfn.JsonPath.format(
            f"{key_prefix}{{}}/request.json", sfn.JsonPath.execution_name
        )
        response_key = sfn.JsonPath.format(
            f"{key_prefix}{{}}/response.json", sfn.JsonPath.execution_name
        )

        if mount_point_configs:
            if mount_points or volumes:
                raise ValueError("Cannot specify both mount_point_configs and mount_points")
            mount_points, volumes = self.convert_to_mount_point_and_volumes(mount_point_configs)

        put_payload = S3Operation.put_payload(
            self,
            f"{id} Put Request to S3",
            payload=payload_path or sfn.JsonPath.entire_payload,
            bucket_name=bucket_name,
            key=request_key,
            result_path="$.taskResult.put",
        )

        submit_job = SubmitJobFragment(
            self,
            id + "Batch",
            env_base=env_base,
            name=name,
            job_queue=job_queue,
            command=[
                "run_cli_executor",
                "--executor",
                executor,
                "--input",
                sfn.JsonPath.format("s3://{}/{}", "$.Bucket", "$.Key"),
                "--output-location",
                sfn.JsonPath.format("s3://{}/{}", bucket_name, response_key),
            ],
            image=image,
            environment=environment,
            memory=memory,
            vcpus=vcpus,
            mount_points=mount_points or [],
            volumes=volumes or [],
        )

        get_response = S3Operation.get_payload(
            self,
            f"{id}",
            bucket_name=bucket_name,
            key=response_key,
        ).to_single_state(
            f"{id} Get Response from S3",
            output_path="$[0]",
        )

        self.definition = put_payload.next(submit_job).next(get_response)

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states


class DataSyncFragment(BatchInvokedBaseFragment, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        aibs_informatics_docker_asset: Union[ecr_assets.DockerImageAsset, str],
        batch_job_queue: Union[batch.JobQueue, str],
        scaffolding_bucket: s3.Bucket,
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
            primary_bucket (Bucket): Primary bucket used for request/response json blobs used in
                the batch invoked lambda function.
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
        shared_mount_point_config: Optional[MountPointConfiguration],
        scratch_mount_point_config: Optional[MountPointConfiguration],
    ) -> None:
        super().__init__(scope, id, env_base)

        # ----------------- Validation -----------------
        if not (shared_mount_point_config and scratch_mount_point_config) or not (
            shared_mount_point_config or scratch_mount_point_config
        ):
            raise ValueError(
                "If shared or scratch mount point configurations are provided,"
                "Both shared and scratch mount point configurations must be provided."
            )

        # ------------------- Setup -------------------

        config_scaffolding_path = "config.scaffolding"
        config_setup_results_path = f"{config_scaffolding_path}.setup_results"
        config_batch_args_path = f"{config_setup_results_path}.batch_args"

        config_cleanup_results_path = f"tasks.cleanup.cleanup_results"

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
        file_system_configurations = {}

        # Update arguments with mount points and volumes if provided
        if shared_mount_point_config or scratch_mount_point_config:
            mount_points = []
            volumes = []
            if shared_mount_point_config:
                # update file system configurations for scaffolding function
                file_system_configurations["shared"] = {
                    "file_system": shared_mount_point_config.file_system_id,
                    "access_point": shared_mount_point_config.access_point_id,
                    "container_path": shared_mount_point_config.mount_point,
                }
                # add to mount point and volumes list for batch invoked lambda functions
                mount_points.append(
                    shared_mount_point_config.to_batch_mount_point("shared", sfn_format=True)
                )
                volumes.append(
                    shared_mount_point_config.to_batch_volume("shared", sfn_format=True)
                )

            if scratch_mount_point_config:
                # update file system configurations for scaffolding function
                file_system_configurations["scratch"] = {
                    "file_system": scratch_mount_point_config.file_system_id,
                    "access_point": scratch_mount_point_config.access_point_id,
                    "container_path": scratch_mount_point_config.mount_point,
                }
                # add to mount point and volumes list for batch invoked lambda functions
                mount_points.append(
                    scratch_mount_point_config.to_batch_mount_point("scratch", sfn_format=True)
                )
                volumes.append(
                    scratch_mount_point_config.to_batch_volume("scratch", sfn_format=True)
                )

            batch_invoked_lambda_kwargs["mount_points"] = mount_points
            batch_invoked_lambda_kwargs["volumes"] = volumes

        start_state = sfn.Pass(
            self,
            f"Start Demand Batch Task",
            parameters={
                "request": {
                    "demand_execution": sfn.JsonPath.object_at("$"),
                    "file_system_configurations": file_system_configurations,
                }
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
                    output_path=f"$.Output",
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
                    output_path=f"$.Output",
                )
            ),
            input_path="$.batch_create_request",
            result_path=f"$",
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
            f"Submit Batch Job",
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
        ).to_single_state("Execution Cleanup Steps", output_path="$[0]")

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
