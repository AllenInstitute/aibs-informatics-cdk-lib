"""Base stack classes for environment-aware CDK stacks.

This module provides the foundational stack classes that enable
environment-aware deployments with automatic tagging and resource naming.
"""

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


def get_all_stacks(scope: constructs.Construct) -> list[cdk.Stack]:
    """Get all CDK stacks from a construct scope.

    Args:
        scope (constructs.Construct): The construct scope to search.

    Returns:
        List of all Stack children in the scope.
    """
    children = scope.node.children
    return [cast(cdk.Stack, child) for child in children if isinstance(child, cdk.Stack)]


def add_stack_dependencies(source_stack: cdk.Stack, dependent_stacks: list[cdk.Stack]):
    """Add dependencies between stacks.

    Makes the dependent stacks depend on the source stack,
    ensuring proper deployment order.

    Args:
        source_stack (cdk.Stack): The stack that others depend on.
        dependent_stacks (List[cdk.Stack]): Stacks that depend on the source.
    """
    for dependent_stack in dependent_stacks:
        dependent_stack.add_dependency(source_stack)


class EnvBaseStackMixins(EnvBaseConstructMixins):
    """Mixin class for environment-aware stack functionality.

    Inherits all functionality from EnvBaseConstructMixins for use in stacks.
    """

    pass


class EnvBaseStack(cdk.Stack, EnvBaseStackMixins):
    """Base stack class with environment awareness.

    Provides automatic tagging with environment information and
    environment-specific removal policies.

    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str | None,
        env_base: EnvBase,
        env: cdk.Environment | None = None,
        **kwargs,
    ) -> None:
        """Initialize an environment-aware stack.

        Args:
            scope (constructs.Construct): The parent construct scope.
            id (Optional[str]): The stack ID. Auto-generated if None.
            env_base (EnvBase): The environment base configuration.
            env (Optional[cdk.Environment]): AWS environment settings.
            **kwargs: Additional arguments passed to cdk.Stack.
        """
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
    def stack_tags(self) -> list[cdk.Tag]:
        return [
            *self.construct_tags,
            cdk.Tag(key=self.env_base.ENV_BASE_KEY, value=self.env_base),
        ]

    @property
    def removal_policy(self) -> cdk.RemovalPolicy:
        if self.env_base.env_type == EnvType.DEV:
            return cdk.RemovalPolicy.DESTROY
        return cdk.RemovalPolicy.RETAIN
