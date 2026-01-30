"""Base construct classes and mixins for environment-aware CDK constructs.

This module provides the foundational classes and mixins that enable
environment-aware CDK constructs throughout the library.
"""

import hashlib
import re
from typing import List, Optional, Union

import aws_cdk as cdk
from aibs_informatics_core.env import EnvBase, EnvBaseMixins, EnvType, ResourceNameBaseEnum
from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from aibs_informatics_cdk_lib.common.aws.iam_utils import grant_managed_policies


class EnvBaseConstructMixins(EnvBaseMixins):
    """Mixin class providing environment-aware utilities for CDK constructs.

    This mixin extends EnvBaseMixins with CDK-specific functionality including
    resource naming, tagging, and IAM utilities.

    Attributes:
        env_base: The environment base configuration.
    """

    @property
    def is_dev(self) -> bool:
        return self.env_base.env_type == EnvType.DEV

    @property
    def is_test(self) -> bool:
        return self.env_base.env_type == EnvType.TEST

    @property
    def is_prod(self) -> bool:
        return self.env_base.env_type == EnvType.PROD

    @property
    def is_test_or_prod(self) -> bool:
        return self.is_prod or self.env_base.env_type == EnvType.TEST

    @property
    def construct_tags(self) -> List[cdk.Tag]:
        return []

    @property
    def aws_region(self) -> str:
        return cdk.Stack.of(self.as_construct()).region

    @property
    def aws_account(self) -> str:
        return cdk.Stack.of(self.as_construct()).account

    def add_tags(self, *tags: cdk.Tag):
        """Add tags to the construct.

        Args:
            *tags (cdk.Tag): Tags to add to the construct.
        """
        for tag in tags:
            cdk.Tags.of(self.as_construct()).add(key=tag.key, value=tag.value)

    def normalize_construct_id(
        self, construct_id: str, max_size: int = 64, hash_size: int = 8
    ) -> str:
        """Normalize a construct ID to fit within size constraints.

        Replaces special characters with dashes and truncates long IDs
        by appending a hash suffix.

        Args:
            construct_id (str): The original construct ID.
            max_size (int): Maximum allowed length. Defaults to 64.
            hash_size (int): Length of hash suffix for truncation. Defaults to 8.

        Returns:
            The normalized construct ID.

        Raises:
            ValueError: If max_size is less than hash_size.
        """
        if max_size < hash_size:
            raise ValueError(f"max_size must be greater than hash_size: ")

        # Replace special characters with dashes
        string = re.sub(r"\W+", "-", construct_id)

        # Check if the string exceeds the allowed size
        if len(string) > max_size:
            # Generate a hexdigest of the string

            digest = hashlib.sha256(string.encode()).hexdigest()
            digest = digest[:hash_size]
            string = string[: -len(digest)] + digest

        return string

    def get_construct_id(self, *names: str) -> str:
        """Build a construct ID from name components.

        Args:
            *names (str): Name components to include in the construct ID.

        Returns:
            The constructed ID with environment prefix.
        """
        return self.build_construct_id(self.env_base, *names)

    def get_child_id(self, scope: Construct, *names: str) -> str:
        """Build a child construct ID, avoiding duplicate env_base prefixes.

        Args:
            scope (Construct): The parent construct scope.
            *names (str): Name components for the child ID.

        Returns:
            The child construct ID.
        """
        if scope.node.id.startswith(self.env_base):
            return "-".join(names)
        return self.build_construct_id(self.env_base, *names)

    def get_name_with_env(self, *names: str) -> str:
        """Get a name prefixed with the environment base.

        Args:
            *names (str): Name components to prefix.

        Returns:
            The environment-prefixed name.
        """
        return self.env_base.prefixed(*names)

    def get_resource_name(self, name: Union[ResourceNameBaseEnum, str]) -> str:
        """Get the full resource name with environment prefix.

        Args:
            name (Union[ResourceNameBaseEnum, str]): The resource name or enum.

        Returns:
            The fully qualified resource name.
        """
        if isinstance(name, ResourceNameBaseEnum):
            return name.get_name(self.env_base)
        return self.env_base.get_resource_name(name)

    def get_stack_of(self, construct: Optional[Construct] = None) -> Stack:
        """Get the stack containing a construct.

        Args:
            construct (Optional[Construct]): The construct to find the stack for.
                Defaults to self.

        Returns:
            The CDK Stack containing the construct.
        """
        if construct is None:
            construct = self.as_construct()
        return cdk.Stack.of(construct)

    def as_construct(self) -> Construct:
        """Return this object as a Construct.

        Returns:
            This object cast as a Construct.

        Raises:
            AssertionError: If this object is not a Construct instance.
        """
        assert isinstance(self, Construct)
        return self

    @classmethod
    def build_construct_id(cls, env_base: EnvBase, *names: str) -> str:
        """Build a construct ID from environment base and name components.

        Args:
            env_base (EnvBase): The environment base configuration.
            *names (str): Name components to include in the ID.

        Returns:
            The constructed ID string.
        """
        return env_base.get_construct_id(*names)  # , cls.__name__)

    # ---------------------------------------------------------------
    #                       Resource Helpers
    # ---------------------------------------------------------------

    # ----------
    #   IAM

    @classmethod
    def add_managed_policies(
        cls, role: Optional[iam.IRole], *managed_policies: Union[str, iam.ManagedPolicy]
    ):
        """Add managed policies to an IAM role.

        Args:
            role (Optional[iam.IRole]): The IAM role to add policies to.
            *managed_policies (Union[str, iam.ManagedPolicy]): Managed policies
                to add, either by name or as ManagedPolicy objects.
        """
        grant_managed_policies(role, *managed_policies)


class EnvBaseConstruct(Construct, EnvBaseConstructMixins):
    """Base construct class with environment awareness.

    Provides a foundation for creating CDK constructs that are aware of
    the deployment environment and support automatic tagging.

    Attributes:
        env_base (EnvBase): The environment base configuration.
    """

    def __init__(self, scope: Construct, id: Optional[str], env_base: EnvBase) -> None:
        """Initialize an environment-aware construct.

        Args:
            scope (Construct): The parent construct scope.
            id (Optional[str]): The construct ID. Defaults to class name if None.
            env_base (EnvBase): The environment base configuration.
        """
        super().__init__(scope, id or self.__class__.__name__)
        self.env_base = env_base
        self.add_tags(*self.construct_tags)

    @property
    def construct_id(self) -> str:
        return self.node.id
