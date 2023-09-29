from typing import List, Optional, Type, Union

import aws_cdk as cdk
import constructs

from aibs_informatics_cdk_lib.project.config import StageConfig
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack, EnvBaseStackMixins


class ConfigBasedStage(cdk.Stage, EnvBaseStackMixins):
    def __init__(
        self, scope: constructs.Construct, id: Optional[str], config: StageConfig, **kwargs
    ) -> None:
        super().__init__(
            scope, id or config.env.env_base.get_construct_id(self.__class__.__name__), **kwargs
        )
        self.config = config
        self.env_base = config.env.env_base
        self.add_tags(*self.stage_tags)

    def get_stack_name(self, stack_class: Union[Type[cdk.Stack], str], *names: str) -> str:
        return self.env_base.get_stage_name(
            stack_class.__name__ if not isinstance(stack_class, str) else stack_class, *names
        )

    @property
    def stage_tags(self) -> List[cdk.Tag]:
        return [
            *self.construct_tags,
            cdk.Tag(key=self.env_base.ENV_BASE_KEY, value=self.env_base),
        ]

    @property
    def env_base_stacks(self) -> List[EnvBaseStack]:
        return [
            node
            for node in self.node.find_all(constructs.ConstructOrder.PREORDER)
            if isinstance(node, EnvBaseStack)
        ]

    @property
    def env(self) -> cdk.Environment:
        try:
            return self._env  # type: ignore
        except AttributeError:
            self._env = cdk.Environment(
                account=self.config.env.account,
                region=self.config.env.region,
            )
            return self.env

    @env.setter
    def env(self, env: cdk.Environment):
        self._env = env
