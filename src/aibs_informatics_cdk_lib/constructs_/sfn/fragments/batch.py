from typing import TYPE_CHECKING, Any, List, Literal, Mapping, Optional, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.base import EnvBaseStateMachineFragment
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation
from aibs_informatics_cdk_lib.constructs_.sfn.states.common import CommonOperation

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import MountPointTypeDef, VolumeTypeDef
else:  # pragma: no cover
    MountPointTypeDef = dict
    VolumeTypeDef = dict


class AWSBatchMixins:
    @classmethod
    def convert_to_mount_point_and_volumes(
        cls,
        mount_point_configs: List[MountPointConfiguration],
    ) -> tuple[List[MountPointTypeDef], List[VolumeTypeDef]]:
        mount_points = []
        volumes = []
        for i, mpc in enumerate(mount_point_configs):
            mount_points.append(
                convert_key_case(mpc.to_batch_mount_point(f"efs-vol{i}"), pascalcase)
            )
            volumes.append(convert_key_case(mpc.to_batch_volume(f"efs-vol{i}"), pascalcase))
        return mount_points, volumes


class SubmitJobFragment(EnvBaseStateMachineFragment, AWSBatchMixins):
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
        job_role_arn: Optional[str] = None,
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
            job_role_arn=job_role_arn,
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
        )

        deregister_chain = BatchOperation.deregister_job_definition(
            self,
            id,
            job_definition=sfn.JsonPath.string_at("$.taskResult.register.JobDefinitionArn"),
        )

        try_catch_deregister_chain = BatchOperation.deregister_job_definition(
            self,
            id + " FAIL",
            job_definition=sfn.JsonPath.string_at("$.taskResult.register.JobDefinitionArn"),
        )

        register = CommonOperation.enclose_chainable(
            self, id + " Register", register_chain, result_path="$.taskResult.register"
        )
        # submit = StateMachineFragment.enclose(
        submit = CommonOperation.enclose_chainable(
            self, id + " Submit", submit_chain, result_path="$.taskResult.submit"
        ).to_single_state(id=f"{id} Enclosure", output_path="$[0]")
        deregister = CommonOperation.enclose_chainable(
            self, id + " Deregister", deregister_chain, result_path="$.taskResult.deregister"
        )
        try_catch_deregister = CommonOperation.enclose_chainable(
            self,
            f"{id} Deregister FAIL",
            try_catch_deregister_chain,
            result_path="$.taskResult.deregister",
        )
        submit.add_catch(
            try_catch_deregister.next(
                sfn.Fail(
                    self,
                    id + " FAIL",
                    cause_path=sfn.JsonPath.string_at("$.taskResult.submit.Cause"),
                    error_path=sfn.JsonPath.string_at("$.taskResult.submit.Error"),
                )
            ),
            result_path="$.taskResult.submit",
            errors=["States.ALL"],
        )

        self.definition = register.next(submit).next(deregister)

    @classmethod
    def from_defaults(
        cls,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: str,
        job_queue: str,
        image: str,
        command: str = "",
        memory: str = "1024",
        vcpus: str = "1",
        environment: Optional[Mapping[str, str]] = None,
        mount_point_configs: Optional[List[MountPointConfiguration]] = None,
        job_role_arn: Optional[str] = None,
    ) -> "SubmitJobFragment":
        defaults: dict[str, Any] = {}
        defaults["command"] = command
        defaults["job_queue"] = job_queue
        defaults["environment"] = environment or {}
        defaults["memory"] = memory
        defaults["image"] = image
        defaults["vcpus"] = vcpus
        defaults["gpu"] = "0"
        defaults["platform_capabilities"] = ["EC2"]
        defaults["job_role_arn"] = job_role_arn

        if mount_point_configs:
            mount_points, volumes = cls.convert_to_mount_point_and_volumes(mount_point_configs)
            defaults["mount_points"] = mount_points
            defaults["volumes"] = volumes

        submit_job = SubmitJobFragment(
            scope,
            id,
            env_base=env_base,
            name=name,
            image=sfn.JsonPath.string_at("$.request.image"),
            command=sfn.JsonPath.string_at("$.request.command"),
            job_queue=sfn.JsonPath.string_at("$.request.job_queue"),
            environment=sfn.JsonPath.string_at("$.request.environment"),
            memory=sfn.JsonPath.string_at("$.request.memory"),
            vcpus=sfn.JsonPath.string_at("$.request.vcpus"),
            # TODO: Handle GPU parameter better - right now, we cannot handle cases where it is
            # not specified. Setting to zero causes issues with the Batch API.
            # If it is set to zero, then the json list of resources are not properly set.
            # gpu=sfn.JsonPath.string_at("$.request.gpu"),
            mount_points=sfn.JsonPath.string_at("$.request.mount_points"),
            volumes=sfn.JsonPath.string_at("$.request.volumes"),
            platform_capabilities=sfn.JsonPath.string_at("$.request.platform_capabilities"),
            job_role_arn=sfn.JsonPath.string_at("$.request.job_role_arn"),
        )

        # Now we need to add the start and merge states and add to the definition
        start = sfn.Pass(
            submit_job,
            "Start",
            parameters={
                "input": sfn.JsonPath.string_at("$"),
                "default": defaults,
            },
        )
        merge = sfn.Pass(
            submit_job,
            "Merge",
            parameters={
                "request": sfn.JsonPath.json_merge(
                    sfn.JsonPath.object_at("$.default"), sfn.JsonPath.object_at("$.input")
                ),
            },
        )

        submit_job.definition = start.next(merge).next(submit_job.definition)
        return submit_job


class SubmitJobWithDefaultsFragment(EnvBaseStateMachineFragment, AWSBatchMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        job_queue: str,
        command: str = "",
        memory: str = "1024",
        vcpus: str = "1",
        environment: Optional[Mapping[str, str]] = None,
        mount_point_configs: Optional[List[MountPointConfiguration]] = None,
        platform_capabilities: Optional[Union[List[Literal["EC2", "FARGATE"]], str]] = None,
        job_role_arn: Optional[str] = None,
    ):
        super().__init__(scope, id, env_base)
        defaults: dict[str, Any] = {}
        defaults["command"] = command
        defaults["job_queue"] = job_queue
        defaults["environment"] = environment or {}
        defaults["memory"] = memory
        defaults["vcpus"] = vcpus
        defaults["gpu"] = "0"
        defaults["platform_capabilities"] = platform_capabilities or ["EC2"]
        defaults["job_role_arn"] = job_role_arn

        if mount_point_configs:
            mount_points, volumes = self.convert_to_mount_point_and_volumes(mount_point_configs)
            defaults["mount_points"] = mount_points
            defaults["volumes"] = volumes

        start = sfn.Pass(
            self,
            "Start",
            parameters={
                "input": sfn.JsonPath.object_at("$"),
                "default": defaults,
            },
        )
        merge_chain = CommonOperation.merge_defaults(
            self, f"{id}", defaults=defaults, input_path="$.input", result_path="$.request"
        )

        submit_job = SubmitJobFragment(
            self,
            "SubmitJobCore",
            env_base=self.env_base,
            name="SubmitJobCore",
            image=sfn.JsonPath.string_at("$.request.image"),
            command=sfn.JsonPath.string_at("$.request.command"),
            job_queue=sfn.JsonPath.string_at("$.request.job_queue"),
            environment=sfn.JsonPath.string_at("$.request.environment"),
            memory=sfn.JsonPath.string_at("$.request.memory"),
            vcpus=sfn.JsonPath.string_at("$.request.vcpus"),
            # TODO: Handle GPU parameter better - right now, we cannot handle cases where it is
            # not specified. Setting to zero causes issues with the Batch API.
            # If it is set to zero, then the json list of resources are not properly set.
            # gpu=sfn.JsonPath.string_at("$.request.gpu"),
            mount_points=sfn.JsonPath.string_at("$.request.mount_points"),
            volumes=sfn.JsonPath.string_at("$.request.volumes"),
            platform_capabilities=sfn.JsonPath.string_at("$.request.platform_capabilities"),
            job_role_arn=sfn.JsonPath.string_at("$.request.job_role_arn"),
        ).to_single_state()

        self.definition = start.next(merge_chain).next(submit_job)
