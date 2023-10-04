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
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation
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


class BatchJobFragment(EnvBaseStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        command: List[str],
        image: str,
        job_queue: str,
        environment: Optional[Mapping[str, str]] = None,
        memory: Optional[int] = None,
        vcpus: Optional[int] = None,
        gpu: Optional[int] = None,
        mount_points: Optional[List[MountPointTypeDef]] = None,
        volumes: Optional[List[VolumeTypeDef]] = None,
    ) -> None:
        super().__init__(scope, id, env_base)

        register_chain = BatchOperation.register_job_definition(
            self,
            id,
            command=command,
            image=image,
            job_definition_name=name,
            memory=memory,
            vcpus=vcpus,
            gpu=gpu,
            mount_points=mount_points,
            volumes=volumes,
        )

        submit_chain = BatchOperation.submit_job(
            self,
            id,
            job_name=name,
            job_definition="$.JobDefinitionArn",
            job_queue=job_queue,
            command=command,
            environment=environment,
            vcpus=vcpus,
            memory=memory,
            gpu=gpu,
        )

        deregister_chain = BatchOperation.deregister_job_definition(
            self,
            id,
            job_definition="$.JobDefinitionArn",
        )

        try_catch_deregister = BatchOperation.deregister_job_definition(
            self,
            id + " FAIL",
            job_definition="$.JobDefinitionArn",
        )

        submit = submit_chain.to_single_state(id, output_path="$[0]", result_path="$.")

        submit.add_catch(
            try_catch_deregister.next(
                sfn.Fail(
                    self,
                    id + " FAIL",
                    cause_path=sfn.JsonPath.string_at("$.Cause"),
                    error_path=sfn.JsonPath.string_at("$.Error"),
                )
            ),
            result_path="$.Cause",
            errors=["States.ALL"],
        )

        self.definition = register_chain.next(submit).next(deregister_chain)
