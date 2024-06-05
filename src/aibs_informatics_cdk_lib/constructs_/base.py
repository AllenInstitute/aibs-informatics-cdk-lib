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
        for tag in tags:
            cdk.Tags.of(self.as_construct()).add(key=tag.key, value=tag.value)

    def normalize_construct_id(
        self, construct_id: str, max_size: int = 64, hash_size: int = 8
    ) -> str:
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
        return self.build_construct_id(self.env_base, *names)

    def get_child_id(self, scope: Construct, *names: str) -> str:
        if scope.node.id.startswith(self.env_base):
            return "-".join(names)
        return self.build_construct_id(self.env_base, *names)

    def get_name_with_env(self, *names: str) -> str:
        return self.env_base.prefixed(*names)

    def get_resource_name(self, name: Union[ResourceNameBaseEnum, str]) -> str:
        if isinstance(name, ResourceNameBaseEnum):
            return name.get_name(self.env_base)
        return self.env_base.get_resource_name(name)

    def get_stack_of(self, construct: Optional[Construct] = None) -> Stack:
        if construct is None:
            construct = self.as_construct()
        return cdk.Stack.of(construct)

    def as_construct(self) -> Construct:
        assert isinstance(self, Construct)
        return self

    @classmethod
    def build_construct_id(cls, env_base: EnvBase, *names: str) -> str:
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
        grant_managed_policies(role, *managed_policies)


class EnvBaseConstruct(Construct, EnvBaseConstructMixins):
    def __init__(self, scope: Construct, id: Optional[str], env_base: EnvBase) -> None:
        super().__init__(scope, id or self.__class__.__name__)
        self.env_base = env_base
        self.add_tags(*self.construct_tags)

    @property
    def construct_id(self) -> str:
        return self.node.id
