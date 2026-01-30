"""Base stage classes for CDK Pipeline deployments.

This module provides stage classes for use with CDK Pipelines,
enabling environment-aware deployment stages.
"""

from typing import List, Optional, Type, Union

import aws_cdk as cdk
import constructs

from aibs_informatics_cdk_lib.project.config import StageConfig
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack, EnvBaseStackMixins


class ConfigBasedStage(cdk.Stage, EnvBaseStackMixins):
    """CDK Stage configured from a StageConfig object.

    Provides environment-aware stage functionality with automatic
    tagging and configuration-driven setup.

    Attributes:
        config (StageConfig): The stage configuration.
        env_base (EnvBase): The environment base from config.
    """

    def __init__(
        self, scope: constructs.Construct, id: Optional[str], config: StageConfig, **kwargs
    ) -> None:
        """Initialize a configuration-based stage.

        Args:
            scope (constructs.Construct): The parent construct scope.
            id (Optional[str]): The stage ID. Auto-generated if None.
            config (StageConfig): The stage configuration object.
            **kwargs: Additional arguments passed to cdk.Stage.
        """
        super().__init__(
            scope, id or config.env.env_base.get_construct_id(self.__class__.__name__), **kwargs
        )
        self.config = config
        self.env_base = config.env.env_base
        self.add_tags(*self.stage_tags)

    def get_stack_name(self, stack_class: Union[Type[cdk.Stack], str], *names: str) -> str:
        """Get a stage-qualified stack name.

        Args:
            stack_class (Union[Type[cdk.Stack], str]): Stack class or name.
            *names (str): Additional name components.

        Returns:
            The fully qualified stack name for this stage.
        """
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
