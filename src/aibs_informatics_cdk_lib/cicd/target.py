from enum import Enum
from typing import Optional, Type, TypeVar, Union

import constructs
from aibs_informatics_core.utils.os_operations import get_env_var

from aibs_informatics_cdk_lib.project.utils import _get_from_context

CDK_STACK_TARGET_ENV_VAR = "CDK_STACK_TARGET"

T = TypeVar("T", bound="CDKStackTargetBaseEnum")


class CDKStackTargetBaseEnum(Enum):
    """Base class for CDK stack target types

    Usage:
        class MyCDKStackTarget(str, CDKStackTargetBaseEnum):
            INFRA = "pipeline"

    """

    @classmethod
    def from_env(cls: Type[T], default: Union[str, T]) -> T:
        target = get_env_var(CDK_STACK_TARGET_ENV_VAR)
        target = target or default
        return cls(target)

    @classmethod
    def from_context(
        cls: Type[T],
        node: constructs.Node,
        default: Union[str, T],
        context_keys: Optional[list[str]] = None,
    ) -> T:
        """Resolves the CDK stack target type from context

        Args:
            cls (Type[T]): subclassed CDKStackTargetBase
            node (constructs.Node): cdk construct node
            default (str): default to use.
            context_keys (Optional[list[str]], optional): overrides for context names.
                Defaults to None.

        Returns:
            T: CDKStackTargetBase instance
        """
        context_keys = context_keys or ["target", "stack_target"]

        target = _get_from_context(node, context_keys) or default

        return cls(target)

    @classmethod
    def from_context_or_env(
        cls: Type[T],
        node: constructs.Node,
        default: Union[str, T],
        context_keys: Optional[list[str]] = None,
    ) -> T:
        """Resolves the CDK stack target type from context or environment

        Order of resolution:
            1. CDK context value (specifying -c K=V)
            2. env variable
            3. default value ("dev")

        Args:
            cls (Type[T]): subclassed CDKStackTargetBase
            node (constructs.Node): cdk construct node
            default (str): default to use.
            context_keys (Optional[list[str]], optional): overrides for context names.
                Defaults to None.

        """

        return cls.from_context(
            node=node, default=cls.from_env(default), context_keys=context_keys
        )
