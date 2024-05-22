__all__ = [
    "EnvBaseStack",
    "EnvBaseStackMixins",
    "add_stack_dependencies",
    "get_all_stacks",
]

from typing import List, Optional, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase, EnvType

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins


def get_all_stacks(scope: constructs.Construct) -> List[cdk.Stack]:
    children = scope.node.children
    return [cast(cdk.Stack, child) for child in children if isinstance(child, cdk.Stack)]


def add_stack_dependencies(source_stack: cdk.Stack, dependent_stacks: List[cdk.Stack]):
    """Add dependencies between stacks

    Args:
        source_stack (Stack): target stack on which a dependency is made
        dependent_stacks (List[Stack]): the stacks adding the dependency
    """
    for dependent_stack in dependent_stacks:
        dependent_stack.add_dependency(source_stack)


class EnvBaseStackMixins(EnvBaseConstructMixins):
    pass


class EnvBaseStack(cdk.Stack, EnvBaseStackMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        env: Optional[cdk.Environment] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            id or env_base.get_construct_id(str(self.__class__)),
            env=env,
            **kwargs,
        )
        self.env_base = env_base
        self.add_tags(*self.stack_tags)

    @property
    def aws_region(self) -> str:
        return cdk.Stack.of(self).region

    @property
    def aws_account(self) -> str:
        return cdk.Stack.of(self).account

    @property
    def stack_tags(self) -> List[cdk.Tag]:
        return [
            *self.construct_tags,
            cdk.Tag(key=self.env_base.ENV_BASE_KEY, value=self.env_base),
        ]

    @property
    def removal_policy(self) -> cdk.RemovalPolicy:
        if self.env_base.env_type == EnvType.DEV:
            return cdk.RemovalPolicy.DESTROY
        return cdk.RemovalPolicy.RETAIN
