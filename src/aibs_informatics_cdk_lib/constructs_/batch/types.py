from dataclasses import dataclass
from typing import Protocol

from aibs_informatics_core.env import EnvBase


class IBatchEnvironmentDescriptor(Protocol):
    def get_job_queue_name(self, env_base: EnvBase) -> str:
        ...

    def get_compute_environment_name(self, env_base: EnvBase) -> str:
        ...

    def get_name(self) -> str:
        ...


@dataclass
class BatchEnvironmentDescriptor:
    name: str

    def get_job_queue_name(self, env_base: EnvBase) -> str:
        return env_base.get_resource_name(self.name, "job-queue")

    def get_compute_environment_name(self, env_base: EnvBase) -> str:
        return env_base.get_resource_name(self.name, "ce")

    def get_name(self) -> str:
        return self.name
