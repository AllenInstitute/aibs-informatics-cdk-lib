from typing import Protocol

from aibs_informatics_core.env import EnvBase


class BatchEnvironmentName(Protocol):
    def get_job_queue_name(self, env_base: EnvBase) -> str:
        ...

    def get_compute_environment_name(self, env_base: EnvBase) -> str:
        ...

    def __str__(self) -> str:
        ...
