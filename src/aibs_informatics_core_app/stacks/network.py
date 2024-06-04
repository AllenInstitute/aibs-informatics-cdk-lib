from typing import Optional

from aibs_informatics_core.env import EnvBase
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.ec2 import EnvBaseVpc
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class NetworkStack(EnvBaseStack):
    def __init__(self, scope: Construct, id: Optional[str], env_base: EnvBase, **kwargs) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self._vpc = EnvBaseVpc(self, "Vpc", self.env_base, max_azs=4)

    @property
    def vpc(self) -> EnvBaseVpc:
        return self._vpc
