import re
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union, cast

import constructs
from aibs_informatics_aws_utils.batch import (
    build_retry_strategy,
    to_key_value_pairs,
    to_mount_point,
    to_resource_requirements,
    to_volume,
)
from aibs_informatics_core.utils.tools.dicttools import convert_key_case, remove_null_values
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as stepfn_tasks

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import (
        ContainerOverridesTypeDef,
        MountPointTypeDef,
        RegisterJobDefinitionRequestRequestTypeDef,
        VolumeTypeDef,
    )
else:
    MountPointTypeDef = dict
    VolumeTypeDef = dict
    RegisterJobDefinitionRequestRequestTypeDef = dict


class BatchOperation:
    @classmethod
    def register_job_definition(
        cls,
        scope: constructs.Construct,
        id: str,
        command: Union[List[str], str],
        image: str,
        job_definition_name: str,
        environment: Optional[Union[Mapping[str, str], str]] = None,
        memory: Optional[Union[int, str]] = None,
        vcpus: Optional[Union[int, str]] = None,
        gpu: Optional[Union[int, str]] = None,
        mount_points: Optional[List[MountPointTypeDef]] = None,
        volumes: Optional[List[VolumeTypeDef]] = None,
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
            environment (Optional[Union[Mapping[str, str], str]], optional): Optional environment variables.
                Supports reference paths both as individual values as well as for the entire list of variables.
                However, if a reference path is used for the entire list, the list must be a list of mappings with Name/Value keys".
            memory (Optional[Union[int, str]], optional): Optionally specify memory.
                Supports reference paths (e.g. "$.foo.bar")
            vcpus (Optional[Union[int, str]], optional): Optionally specify . Defaults to None.
            gpu (Optional[Union[int, str]], optional): _description_. Defaults to None.
            mount_points (Optional[List[MountPointTypeDef]], optional): _description_. Defaults to None.
            volumes (Optional[List[VolumeTypeDef]], optional): _description_. Defaults to None.

        Returns:
            sfn.Chain: _description_
        """

        job_definition_name = f"States.format('{{}}-{{}}-{{}}', $$.State.Name, job_definition_name, $$.Execution.Name)"
        if not isinstance(environment, str):
            environment_pairs = to_key_value_pairs(environment or {})
        else:
            environment_pairs = environment

        request: RegisterJobDefinitionRequestRequestTypeDef = {
            "jobDefinitionName": job_definition_name,
            "type": "container",
            "containerProperties": {
                "image": image,
                "command": command,
                "environment": environment_pairs,
                "resourceRequirements": to_resource_requirements(gpu, memory, vcpus),
                "mountPoints": mount_points,
                "volumes": volumes,
            },
            "retryStrategy": build_retry_strategy(include_default_evaluate_on_exit_configs=True),
        }
        parameters = convert_key_case(request, pascalcase)

        start = sfn.Pass(scope, id + " RegisterJobDefinition Prep", parameters=parameters)
        register = sfn.CustomState(
            scope,
            id + " RegisterJobDefinition API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:batch:registerJobDefinition",
                "Parameters": {f"{k}.$": f"$.{k}" for k in parameters.keys()},
                "ResultSelector": {
                    "JobDefinitionArn.$": "$.JobDefinitionArn",
                    "JobDefinitionName.$": "$.JobDefinitionName",
                    "Revision.$": "$.Revision",
                },
                "ResultPath": "$",
                "OutputPath": "$",
            },
        )
        return start.next(register)

    @classmethod
    def submit_job(
        cls,
        scope: constructs.Construct,
        id: str,
        job_name: str,
        job_definition: str,
        job_queue: str,
        parameters: Optional[Mapping[str, str]] = None,
        command: Union[List[str], str] = None,
        environment: Optional[Union[Mapping[str, str], str]] = None,
        memory: Optional[Union[int, str]] = None,
        vcpus: Optional[Union[int, str]] = None,
        gpu: Optional[Union[int, str]] = None,
    ) -> sfn.Chain:
        job_name = f"States.format('{{}}-{job_name}-{{}}', $$.State.Name. $$.Execution.Name)"

        if not isinstance(environment, str):
            environment_pairs = to_key_value_pairs(environment or {})
        else:
            environment_pairs = environment

        container_overrides: ContainerOverridesTypeDef = {
            "command": command,
            "environment": environment_pairs,
            "resourceRequirements": to_resource_requirements(gpu, memory, vcpus),
        }

        request = {
            "JobName": job_name,
            "JobDefinition": job_definition,
            "JobQueue": job_queue,
            "Parameters": parameters,
            "ContainerOverrides": container_overrides,
        }
        start = sfn.Pass(
            scope,
            id + " SubmitJob Prep",
            parameters=(parameters := convert_key_case(request, pascalcase)),
        )

        submit = sfn.CustomState(
            scope,
            id + f" SubmitJob API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {f"{k}.$": f"$.{k}" for k in parameters.keys()},
                "ResultSelector": {
                    "JobName.$": "$.JobName",
                    "JobId.$": "$.JobId",
                    "JobArn.$": "$.JobArn",
                    "JobQueue.$": "$.JobQueue",
                },
                "ResultPath": "$",
                "OutputPath": "$",
            },
        )
        return start.next(submit)

    @classmethod
    def deregister_job_definition(
        cls, scope: constructs.Construct, id: str, job_definition: str
    ) -> sfn.Chain:
        request = {"jobDefinition": job_definition}
        start = sfn.Pass(
            scope,
            id + " DeregisterJobDefinition Prep",
            parameters=(parameters := convert_key_case(request, pascalcase)),
        )
        deregister = sfn.CustomState(
            scope,
            id + f" DeregisterJobDefinition API Call",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:batch:deregisterJobDefinition",
                "Parameters": {f"{k}.$": f"$.{k}" for k in parameters.keys()},
                "ResultPath": None,
            },
        )
        return start.next(deregister)
