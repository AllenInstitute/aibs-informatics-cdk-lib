from typing import Dict, List, Optional, Tuple

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct

LATEST_TAG = "latest"


class BatchTools(EnvBaseConstruct):
    def __init__(self, scope: constructs.Construct, id: str, env_base: EnvBase):
        super().__init__(scope, id, env_base=env_base)
        self._repo_cache: Dict[str, ecr.Repository] = {}
        self._image_cache: Dict[Tuple[str, str], ecs.EcrImage] = {}
        self._job_def_cache: Dict[str, batch.IJobDefinition] = {}

    def create_job_definition(
        self,
        container_name: str,
        container_tag: str = LATEST_TAG,
        command: Optional[List[str]] = None,
        memory_limit_mib: int = 1024,
        vcpus: int = 1,
        mount_points: Optional[List[ecs.MountPoint]] = None,
        volumes: Optional[List[ecs.Volume]] = None,
        **kwargs,
    ) -> batch.IJobDefinition:
        job_definition_name = self.get_job_definition_name(container_name)
        cache_key = job_definition_name
        if cache_key not in self._job_def_cache:
            jobdef_container = batch.EcsEc2ContainerDefinition(
                image=self.get_ecr_image(container_name, container_tag),
                command=command,
                memory_limit_mib=memory_limit_mib,
                vcpus=vcpus,
                mount_points=mount_points,
                volumes=volumes,
                **kwargs,
            )
            # TODO: We should use CfnJobDefinition instead because we have more
            #       granular control over the configurations (such as defining a more
            #       complex retry strategy as opposed to specifying an arbitrary number)
            self._job_def_cache[cache_key] = batch.EcsJobDefinition(
                self,
                job_definition_name,
                container=jobdef_container,
                job_definition_name=job_definition_name,
                retry_attempts=3,
            )
        return self._job_def_cache[cache_key]

    def get_job_definition(self, container_name: str) -> batch.IJobDefinition:
        job_definition_name = self.get_job_definition_name(container_name)
        cache_key = job_definition_name
        if cache_key not in self._job_def_cache:
            self._job_def_cache[cache_key] = batch.EcsJobDefinition.from_job_definition_arn(
                self, job_definition_name, job_definition_name=job_definition_name
            )
        return self._job_def_cache[cache_key]

    def get_ecr_image(self, container_name: str, container_tag: str = LATEST_TAG) -> ecs.EcrImage:
        cache_key = (container_name, container_tag)
        if cache_key not in self._image_cache:
            repo = self.get_ecr_repo(container_name)
            self._image_cache[cache_key] = ecs.EcrImage(repo, container_tag)
        return self._image_cache[cache_key]

    def get_ecr_repo(self, container_name: str) -> ecr.Repository:
        cache_key = container_name
        if cache_key not in self._repo_cache:
            repo_name = self.env_base.get_repository_name(container_name)
            self._repo_cache[cache_key] = ecr.Repository.from_repository_name(
                self, self.env_base.get_construct_id(container_name, "ecr-repo"), repo_name
            )
        return self._repo_cache[cache_key]

    def get_job_definition_name(self, container_name: str) -> str:
        return self.env_base.prefixed(container_name, "job-definition")
