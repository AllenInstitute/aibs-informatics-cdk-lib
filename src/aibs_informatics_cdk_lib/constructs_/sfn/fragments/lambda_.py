from typing import TYPE_CHECKING, List, Mapping, Optional

import constructs
from aibs_informatics_aws_utils.constants.lambda_ import (
    AWS_LAMBDA_EVENT_PAYLOAD_KEY,
    AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_KEY,
    AWS_LAMBDA_FUNCTION_NAME_KEY,
)
from aibs_informatics_aws_utils.constants.s3 import S3_SCRATCH_KEY_PREFIX, S3BucketName
from aibs_informatics_aws_utils.core import get_account_id, get_region
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as stepfn_tasks

from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentName
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import BatchJobFragment
from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3Operation

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


class LambdaFunctionStateMachine(EnvBaseStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        lambda_function: lambda_.Function,
    ) -> None:
        """Creates a single task state machine fragment for a generic lambda function

                    [Pass]          # Start State. Augments input.
                      ||            #
                  [lambda fn]
               (x)  //   \\ (o)     #
                [Fail]    ||        # Catch state for failed lambda execution
                          ||        #
                        [Pass]      # End state. Reforms output of lambda as sfn output

        Returns:
            StateMachineFragment: the state machine fragment
        """
        super().__init__(scope, id, env_base)

        lambda_task = stepfn_tasks.LambdaInvoke(
            self,
            f"{lambda_function.function_name} Function Execution",
            lambda_function=lambda_function,
            payload_response_only=True,
        )

        self.definition = sfn.Chain.start(lambda_task)
        # fmt: on


class BatchInvokedLambdaFunction(EnvBaseStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        handler: str,
        job_queue: str,
        bucket_name: str,
        key_prefix: Optional[str] = None,
        payload_path: Optional[str] = None,
        environment: Optional[Mapping[str, str]] = None,
        memory: Optional[int] = None,
        vcpus: Optional[int] = None,
        mount_points: Optional[List[MountPointTypeDef]] = None,
        volumes: Optional[List[VolumeTypeDef]] = None,
    ) -> None:
        """Creates a single task state machine fragment for a generic lambda function

                   [ Pass ]             # Start State. Augments input.
                      ||                #
          [ create / prep batch args ]  # Creates Job def and prepares batch job args
                      ||
                [ batch submit ]        # invokes batch submit job
                      ||
            [ get response from s3 ]    # retrieves response written to s3
                      ||
                    [ End ]

        Returns:
            StateMachineFragment: the state machine fragment
        """

        super().__init__(scope, id, env_base)
        task_name = name
        key_prefix = (
            f"States.Format('{key_prefix or S3_SCRATCH_KEY_PREFIX}/{{}}', $$.Execution.Name)"
        )
        request_key = f"{key_prefix}/request.json"
        response_key = f"{key_prefix}/response.json"

        put_payload = S3Operation.put_payload(
            self,
            f"Put Request to S3",
            payload=sfn.JsonPath.string_at("$.request"),
            bucket_name=bucket_name,
            key=request_key,
        )

        BatchJobFragment(self, id + "Batch", env_base, name, handler, job_queue, environment)

        put_request_to_s3_result_path = "tasks.put_request_to_s3"

        prep_submit_job_args_result_path = "tasks.prep.response"
        prep_submit_job_args_lambda = stepfn_tasks.LambdaInvoke(
            self,
            f"Preparing Job Arguments",
            lambda_function=self.get_fn(GWOLambdaFunctions.CREATE_DEF_AND_PREPARE_JOB_ARGS),
            payload=sfn.TaskInput.from_object(
                # PrepareSubmitJobArgsRequest
                # fmt: off
                dict(
                    image=f'{self.env_base.get_repository_name("gcs_lambda_functions")}:latest',
                    job_definition_name=f"{task_name}-job",
                    job_queue_name=job_queue,
                    command=[],
                    environment={
                        **(environment if environment else {}),
                        AWS_LAMBDA_FUNCTION_NAME_KEY: name,
                        AWS_LAMBDA_FUNCTION_HANDLER_KEY: sfn.JsonPath.string_at("$.config.handler"),
                        AWS_LAMBDA_EVENT_PAYLOAD_KEY: sfn.JsonPath.string_at(f"$.{put_request_to_s3_result_path}.path"),
                        AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY: sfn.JsonPath.string_at("$.response"),
                    },
                    resource_requirements=sfn.JsonPath.object_at("$.config.resource_requirements"),
                    mount_points=mount_points or [],
                    volumes=volumes or [],
                )
                # fmt: on
            ),
            payload_response_only=True,
            result_path=f"$.{prep_submit_job_args_result_path}",
        )

        batch_submit_task = sfn.CustomState(
            self,
            f"Submit Job",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                # PrepareSubmitJobArgsResponse
                # fmt: off
                "Parameters": {
                    "JobName.$": sfn.JsonPath.string_at(f"$.{prep_submit_job_args_result_path}.job_name"),
                    "JobDefinition.$": sfn.JsonPath.string_at(f"$.{prep_submit_job_args_result_path}.job_definition_arn"),
                    "JobQueue.$": sfn.JsonPath.string_at(f"$.{prep_submit_job_args_result_path}.job_queue_arn"),
                    "Parameters.$": sfn.JsonPath.object_at(f"$.{prep_submit_job_args_result_path}.parameters"),
                    "ContainerOverrides.$": sfn.JsonPath.object_at(f"$.{prep_submit_job_args_result_path}.container_overrides"),
                },
                # fmt: on
                "ResultPath": "$.debug.batch_submit_task.output",
            },
        )

        get_response_from_s3_lambda = stepfn_tasks.LambdaInvoke(
            self,
            f"Fetch Response from File",
            lambda_function=self.get_fn(GWOLambdaFunctions.GET_JSON_FROM_FILE),
            payload=sfn.TaskInput.from_object(dict(path=sfn.JsonPath.string_at("$.response"))),
            payload_response_only=True,
            result_path=f"$.response",
        )

        success_state = sfn.Pass(
            self, f"Success", input_path="$.response.content", output_path="$"
        )

        # fmt: off
        self.definition = (
            start_pass_state
            .next(put_request_to_s3_lambda)
            .next(prep_submit_job_args_lambda)
            .next(batch_submit_task)
            .next(get_response_from_s3_lambda)
            .next(success_state)
        )
        # fmt: on

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states
