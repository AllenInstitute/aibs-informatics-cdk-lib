from typing import TYPE_CHECKING, List, Mapping, Optional, Union

import constructs
from aibs_informatics_aws_utils.constants.lambda_ import (
    AWS_LAMBDA_EVENT_PAYLOAD_KEY,
    AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY,
    AWS_LAMBDA_FUNCTION_HANDLER_KEY,
    AWS_LAMBDA_FUNCTION_NAME_KEY,
)
from aibs_informatics_aws_utils.constants.s3 import S3_SCRATCH_KEY_PREFIX
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import SubmitJobFragment
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation
from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3Operation

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import MountPointTypeDef, VolumeTypeDef
else:
    MountPointTypeDef = dict
    VolumeTypeDef = dict


class BatchInvokedLambdaFunction(EnvBaseStateMachineFragment):
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
        mount_points: Optional[List[MountPointTypeDef]] = None,
        volumes: Optional[List[VolumeTypeDef]] = None,
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
        request_key = sfn.JsonPath.format(
            f"{key_prefix or S3_SCRATCH_KEY_PREFIX}/{{}}/request.json", "$$.Execution.Name"
        )
        response_key = sfn.JsonPath.format(
            f"{key_prefix or S3_SCRATCH_KEY_PREFIX}/{{}}/response.json", "$$.Execution.Name"
        )

        put_payload = S3Operation.put_payload(
            self,
            f"Put Request to S3",
            payload=payload_path or "$",
            bucket_name=bucket_name,
            key=request_key,
        )

        submit_job = SubmitJobFragment(
            self,
            id + "Batch",
            env_base=env_base,
            name=name,
            job_queue=job_queue,
            command=command or [],
            image=image,
            environment={
                **(environment if environment else {}),
                AWS_LAMBDA_FUNCTION_NAME_KEY: name,
                AWS_LAMBDA_FUNCTION_HANDLER_KEY: handler,
                AWS_LAMBDA_EVENT_PAYLOAD_KEY: sfn.JsonPath.format(
                    "s3://{}/{}", "$.Bucket", "$.Key"
                ),
                AWS_LAMBDA_EVENT_RESPONSE_LOCATION_KEY: sfn.JsonPath.format(
                    "s3://{}/{}", bucket_name, response_key
                ),
            },
            memory=memory,
            vcpus=vcpus,
            mount_points=mount_points or [],
            volumes=volumes or [],
        )

        get_response = S3Operation.get_payload(
            self,
            f"Get Response from S3",
            bucket_name=bucket_name,
            key=response_key,
        )

        self.definition = put_payload.next(submit_job).next(get_response)

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states


class BatchInvokedExecutorFragment(EnvBaseStateMachineFragment):
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
        request_key = sfn.JsonPath.format(
            f"{key_prefix or S3_SCRATCH_KEY_PREFIX}/{{}}/request.json", "$$.Execution.Name"
        )
        response_key = sfn.JsonPath.format(
            f"{key_prefix or S3_SCRATCH_KEY_PREFIX}/{{}}/response.json", "$$.Execution.Name"
        )

        put_payload = S3Operation.put_payload(
            self,
            f"Put Request to S3",
            payload=payload_path or "$",
            bucket_name=bucket_name,
            key=request_key,
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
            f"Get Response from S3",
            bucket_name=bucket_name,
            key=response_key,
        )

        self.definition = put_payload.next(submit_job).next(get_response)

    @property
    def start_state(self) -> sfn.State:
        return self.definition.start_state

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self.definition.end_states
