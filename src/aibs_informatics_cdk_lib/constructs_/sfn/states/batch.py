from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

import constructs
from aibs_informatics_aws_utils.batch import (
    build_retry_strategy,
    to_key_value_pairs,
    to_resource_requirements,
)
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.utils import convert_reference_paths

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import (
        ContainerOverridesTypeDef,
        KeyValuePairTypeDef,
        MountPointTypeDef,
        RegisterJobDefinitionRequestTypeDef,
        VolumeTypeDef,
    )
else:
    ContainerOverridesTypeDef = dict
    KeyValuePairTypeDef = dict
    MountPointTypeDef = dict
    VolumeTypeDef = dict
    RegisterJobDefinitionRequestTypeDef = dict


class BatchOperation:
    @classmethod
    def register_job_definition(
        cls,
        scope: constructs.Construct,
        id: str,
        command: list[str] | str | None,
        image: str,
        job_definition_name: str,
        job_role_arn: str | None = None,
        environment: Mapping[str, str] | str | None = None,
        memory: int | str | None = None,
        vcpus: int | str | None = None,
        gpu: int | str | None = None,
        mount_points: list[MountPointTypeDef] | str | None = None,
        volumes: list[VolumeTypeDef] | str | None = None,
        platform_capabilities: list[Literal["EC2", "FARGATE"]] | str | None = None,
        result_path: str | None = "$",
        output_path: str | None = "$",
    ) -> sfn.Chain:
        """Creates chain to register new job definition

        Following parameters support reference paths:
        - command
        - image
        - job_definition_name
        - environment
        - memory
        - vcpus
        - gpu

        Args:
            scope (constructs.Construct): scope
            id (str): ID prefix
            command (Union[List[str], str]): List of strings or string representing command to run
                Supports reference paths (e.g. "$.foo.bar")
            image (str): image URI or name.
                Supports reference paths (e.g. "$.foo.bar")
            job_definition_name (str): name of job definition.
                Supports reference paths (e.g. "$.foo.bar")
            job_role_arn (Optional[str], optional): Optional job role arn to use for the job.
                Supports reference paths (e.g. "$.foo.bar")
            environment (Optional[Union[Mapping[str, str], str]], optional): Optional environment variables.
                Supports reference paths both as individual values as well as for the entire list of variables.
                However, if a reference path is used for the entire list, the list must be a list of mappings with Name/Value keys".
            memory (Optional[Union[int, str]], optional): Optionally specify memory.
                Supports reference paths (e.g. "$.foo.bar")
            vcpus (Optional[Union[int, str]], optional): Optionally specify . Defaults to None.
            gpu (Optional[Union[int, str]], optional): Optionally specify GPU. Defaults to None.
            mount_points (Optional[List[MountPointTypeDef]], optional): Optionally specify mount points. Defaults to None.
            volumes (Optional[List[VolumeTypeDef]], optional): Optionally specify volumes. Defaults to None.

        Returns:
            sfn.Chain: chain to register job definition
        """  # noqa: E501

        job_definition_name = sfn.JsonPath.format(
            "{}-{}", job_definition_name, sfn.JsonPath.uuid()
        )
        environment_pairs: list[KeyValuePairTypeDef] | str | None
        if not isinstance(environment, str):
            environment_pairs = to_key_value_pairs(dict(environment or {}))
        else:
            environment_pairs = environment

        request: RegisterJobDefinitionRequestTypeDef = {
            "jobDefinitionName": job_definition_name,
            "type": "container",
            "containerProperties": {
                "image": image,
                "command": command,
                "environment": environment_pairs,
                "resourceRequirements": to_resource_requirements(gpu, memory, vcpus),  # type: ignore # must be string
                "mountPoints": mount_points,
                "volumes": volumes,
            },
            "retryStrategy": build_retry_strategy(include_default_evaluate_on_exit_configs=True),
        }  # type: ignore
        if platform_capabilities:
            request["platformCapabilities"] = platform_capabilities  # type: ignore[typeddict-item] # the typing does not understand that this can be a reference path
        if job_role_arn:
            assert "containerProperties" in request  # mollifies mypy
            request["containerProperties"]["jobRoleArn"] = job_role_arn
        parameters = convert_key_case(request, pascalcase)

        start = sfn.Pass(
            scope,
            id + " RegisterJobDefinition Prep",
            parameters=convert_reference_paths(parameters),  # type: ignore  # misundertands type
            result_path=result_path or "$",
        )
        register = sfn.CustomState(
            scope,
            id + " RegisterJobDefinition API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:batch:registerJobDefinition",
                "Parameters": {
                    f"{k}.$": f"{result_path if result_path else '$'}.{k}"
                    for k in parameters.keys()
                },
                "ResultSelector": {
                    "JobDefinitionArn.$": "$.JobDefinitionArn",
                    "JobDefinitionName.$": "$.JobDefinitionName",
                    "Revision.$": "$.Revision",
                },
                "ResultPath": result_path,
                "OutputPath": output_path,
                "Retry": [
                    {
                        "ErrorEquals": ["Batch.BatchException"],
                        # Interval at attempt n = IntervalSeconds x BackoffRate ^(n-1)
                        # Total time from first try: 3 + 6 + 12 + 24 + 48 + 96 = 189 seconds
                        "IntervalSeconds": 3,
                        "MaxAttempts": 7,
                        "BackoffRate": 2.0,
                        "JitterStrategy": "FULL",
                    },
                ],
            },
        )
        chain: sfn.Chain | sfn.Pass = start
        if gpu is not None:
            chain = chain.next(
                sfn.Pass(
                    scope,
                    id + " Register Definition Filter Resource Requirements",
                    input_path=f"{result_path or '$'}.ContainerProperties.ResourceRequirements[?(@.Value != 0 && @.Value != '0')]",  # noqa: E501
                    result_path=f"{result_path or '$'}.ContainerProperties.ResourceRequirements",
                )
            )
        return chain.next(register)

    @classmethod
    def submit_job(
        cls,
        scope: constructs.Construct,
        id: str,
        job_name: str,
        job_definition: str,
        job_queue: str,
        parameters: Mapping[str, str] | None = None,
        command: list[str] | str | None = None,
        environment: Mapping[str, str] | str | None = None,
        memory: int | str | None = None,
        vcpus: int | str | None = None,
        gpu: int | str | None = None,
        result_path: str | None = "$",
        output_path: str | None = "$",
    ) -> sfn.Chain:
        job_name = sfn.JsonPath.format(f"{job_name}-{{}}", sfn.JsonPath.uuid())
        environment_pairs: list[KeyValuePairTypeDef] | str | None
        if not isinstance(environment, str):
            environment_pairs = to_key_value_pairs(dict(environment or {}))
        else:
            environment_pairs = environment

        container_overrides: ContainerOverridesTypeDef = {
            "command": command,
            "environment": environment_pairs,  # type: ignore  # mypy can't verify this is the correct type with the current handling of reference paths
            "resourceRequirements": to_resource_requirements(gpu, memory, vcpus),  # type: ignore # must be string
        }  # type: ignore

        request = {
            "JobName": job_name,
            "JobDefinition": job_definition,
            "JobQueue": job_queue,
            "Parameters": parameters or {},
            "ContainerOverrides": container_overrides,
        }
        start = sfn.Pass(
            scope,
            id + " SubmitJob Prep",
            parameters=convert_reference_paths(
                pass_params := convert_key_case(request, pascalcase)  # type: ignore  # misundertands type
            ),
            result_path=result_path or "$",
        )

        submit = sfn.CustomState(
            scope,
            f"{id} SubmitJob API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                    f"{k}.$": f"{result_path if result_path else '$'}.{k}"
                    for k in pass_params.keys()
                },
                "ResultSelector": {
                    "JobName.$": "$.JobName",
                    "JobId.$": "$.JobId",
                    "JobArn.$": "$.JobArn",
                },
                "ResultPath": result_path,
                "OutputPath": output_path,
                "Retry": [
                    {
                        "ErrorEquals": ["Batch.BatchException"],
                        # Interval at attempt n = IntervalSeconds x BackoffRate ^(n-1)
                        # Total time from first try: 3 + 6 + 12 + 24 + 48 + 96 = 189 seconds
                        "IntervalSeconds": 3,
                        "MaxAttempts": 7,
                        "BackoffRate": 2.0,
                        "JitterStrategy": "FULL",
                    },
                ],
            },
        )
        chain: sfn.Chain | sfn.Pass = start
        if gpu is not None:
            chain = chain.next(
                sfn.Pass(
                    scope,
                    id + " SubmitJob Filter Resource Requirements",
                    input_path=f"{result_path or '$'}.ContainerOverrides.ResourceRequirements[?(@.Value != 0 && @.Value != '0')]",  # noqa: E501
                    result_path=f"{result_path or '$'}.ContainerOverrides.ResourceRequirements",
                )
            )
        return chain.next(submit)

    @classmethod
    def deregister_job_definition(
        cls,
        scope: constructs.Construct,
        id: str,
        job_definition: str,
        result_path: str | None = "$",
        output_path: str | None = "$",
    ) -> sfn.Chain:
        request = {"jobDefinition": job_definition}
        start = sfn.Pass(
            scope,
            id + " DeregisterJobDefinition Prep",
            parameters=(parameters := convert_key_case(request, pascalcase)),
            result_path=result_path or "$",
        )
        deregister = sfn.CustomState(
            scope,
            f"{id} DeregisterJobDefinition API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:batch:deregisterJobDefinition",
                "Parameters": {
                    f"{k}.$": f"{result_path if result_path else '$'}.{k}"
                    for k in parameters.keys()
                },
                "ResultPath": result_path,
                "OutputPath": output_path,
                "Retry": [
                    {
                        "ErrorEquals": ["Batch.BatchException"],
                        # Interval at attempt n = IntervalSeconds x BackoffRate ^(n-1)
                        # Total time from first try: 3 + 6 + 12 + 24 + 48 + 96 = 189 seconds
                        "IntervalSeconds": 3,
                        "MaxAttempts": 7,
                        "BackoffRate": 2.0,
                        "JitterStrategy": "FULL",
                    },
                ],
            },
        )
        return start.next(deregister)
