from typing import TYPE_CHECKING, List, Mapping, Optional, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation

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
