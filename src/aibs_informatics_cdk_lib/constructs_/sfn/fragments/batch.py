from typing import TYPE_CHECKING, List, Literal, Mapping, Optional, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import (
    EnvBaseStateMachineFragment,
    StateMachineFragment,
)
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation
from aibs_informatics_cdk_lib.constructs_.sfn.utils import enclosed_chain

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import MountPointTypeDef, VolumeTypeDef
else:
    MountPointTypeDef = dict
    VolumeTypeDef = dict


class SubmitJobFragment(EnvBaseStateMachineFragment):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        job_queue: str,
        image: str,
        command: Optional[Union[List[str], str]] = None,
        environment: Optional[Union[Mapping[str, str], str]] = None,
        memory: Optional[Union[int, str]] = None,
        vcpus: Optional[Union[int, str]] = None,
        gpu: Optional[Union[int, str]] = None,
        mount_points: Optional[Union[List[MountPointTypeDef], str]] = None,
        volumes: Optional[Union[List[VolumeTypeDef], str]] = None,
        platform_capabilities: Optional[Union[List[Literal["EC2", "FARGATE"]], str]] = None,
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
            platform_capabilities=platform_capabilities,
            # result_path="$.taskResult.register",
        )

        submit_chain = BatchOperation.submit_job(
            self,
            id,
            job_name=name,
            job_definition=sfn.JsonPath.string_at("$.taskResult.register.JobDefinitionArn"),
            job_queue=job_queue,
            command=command,
            environment=environment,
            vcpus=vcpus,
            memory=memory,
            gpu=gpu,
            # result_path="$.taskResult.submit",
        )

        deregister_chain = BatchOperation.deregister_job_definition(
            self,
            id,
            job_definition=sfn.JsonPath.string_at("$.taskResult.register.JobDefinitionArn"),
        )

        try_catch_deregister = BatchOperation.deregister_job_definition(
            self,
            id + " FAIL",
            job_definition=sfn.JsonPath.string_at("$.taskResult.register.JobDefinitionArn"),
        )

        register = StateMachineFragment.enclose(
            self, id + " Register", register_chain, result_path="$.taskResult.register"
        )
        submit = StateMachineFragment.enclose(
            self, id + " Submit", submit_chain, result_path="$.taskResult.submit"
        ).to_single_state(output_path="$[0]")
        deregister = StateMachineFragment.enclose(
            self, id + " Deregister", deregister_chain, result_path="$.taskResult.deregister"
        )
        submit.add_catch(
            try_catch_deregister.next(
                sfn.Fail(
                    self,
                    id + " FAIL",
                    cause_path=sfn.JsonPath.string_at("$.taskResult.submit.cause"),
                    error_path=sfn.JsonPath.string_at("$.taskResult.submit.error"),
                )
            ),
            result_path="$.taskResult.submit",
            errors=["States.ALL"],
        )

        self.definition = register.next(submit).next(deregister)
