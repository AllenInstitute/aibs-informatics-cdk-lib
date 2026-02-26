"""Project configuration models for CDK applications.

This module provides Pydantic models for defining project, stage, and pipeline
configurations for CDK applications.
"""

__all__ = [
    "Env",
    "EnvBase",
    "EnvType",
    "CodePipelineSourceConfig",
    "CodePipelineNotificationsConfig",
    "GlobalConfig",
    "StageConfig",
    "PipelineConfig",
    "BaseProjectConfig",
    "ProjectConfig",
    "ConfigProvider",
]

import logging
from collections.abc import MutableMapping
from pathlib import Path
from typing import Annotated, Dict, Generic, List, Optional, Type, TypeVar, Union

import yaml
from aibs_informatics_core.collections import DeepChainMap
from aibs_informatics_core.env import ENV_BASE_KEY, ENV_LABEL_KEY, ENV_TYPE_KEY, EnvBase, EnvType
from aibs_informatics_core.models.unique_ids import UniqueID
from aibs_informatics_core.utils.file_operations import find_paths
from aibs_informatics_core.utils.os_operations import expandvars
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from pydantic import BaseModel, PlainSerializer, PlainValidator, model_validator

UniqueIDType = Annotated[
    UniqueID, PlainValidator(lambda x: UniqueID(x)), PlainSerializer(lambda x: str(x))
]


class EnvVarStr(str):
    """String type that expands environment variables on validation.

    Automatically expands $VAR and ${VAR} patterns when validated.
    """

    @classmethod
    def __get_validators__(cls):
        """Yield validators for Pydantic.

        Yields:
            The validate method.
        """
        yield cls.validate

    @classmethod
    def validate(cls, v):
        """Validate and expand environment variables.

        Args:
            v: The value to validate.

        Returns:
            The expanded string, or None.

        Raises:
            TypeError: If value is not a string.
        """
        if v is not None and not isinstance(v, str):
            raise TypeError("string required")
        return expandvars(v, "", False) if v else v

    def __repr__(self):
        return f"{self.__class__.__name__}({super().__repr__()})"


class Env(BaseModel):
    """Environment configuration model.

    Attributes:
        env_type (EnvType): The environment type (dev, prod, etc.).
        label (Optional[str]): Optional label for the environment.
        account (Optional[str]): AWS account ID.
        region (Optional[str]): AWS region.
    """

    env_type: EnvType
    label: Annotated[str | None, PlainValidator(EnvVarStr.validate)] = None
    account: Annotated[str | None, PlainValidator(EnvVarStr.validate)] = None
    region: Annotated[str | None, PlainValidator(EnvVarStr.validate)] = None

    @property
    def env_name(self) -> str:
        """Get the environment name.

        Returns:
            The environment type value as a string.
        """
        return self.env_type.value

    @property
    def env_base(self) -> EnvBase:
        """Get the environment base identifier.

        Returns:
            EnvBase in format <env_name>[-<env_label>].
        """
        return EnvBase.from_type_and_label(self.env_type, self.label)

    @property
    def is_configured(self) -> bool:
        """Check if the environment has an AWS account configured.

        Returns:
            True if account is set, False otherwise.
        """
        return self.account is not None

    def to_env_var_map(self) -> MutableMapping[str, str]:
        """Convert to environment variable mapping.

        Returns:
            Dictionary of environment variable names to values.
        """
        return remove_null_values(
            dict(
                [
                    (ENV_TYPE_KEY, self.env_type),
                    (ENV_LABEL_KEY, self.label),
                    (ENV_BASE_KEY, self.env_base),
                ]
            ),  # type: ignore
            in_place=True,
        )


class CodePipelineBuildConfig(BaseModel):
    """Configuration for CodePipeline build settings.

    Attributes:
        ssh_key_secret_name (str): Name of the secret containing SSH key.
        docker_hub_credentials_secret_name (str): Name of the secret with
            Docker Hub credentials.
    """

    ssh_key_secret_name: str
    docker_hub_credentials_secret_name: str


class CodePipelineSourceConfig(BaseModel):
    """Configuration for CodePipeline source settings.

    Attributes:
        repository (str): The source repository.
        branch (str): The branch to build from.
        codestar_connection (Optional[UniqueIDType]): CodeStar connection ARN.
        oauth_secret_name (Optional[str]): Name of OAuth secret.

    Note:
        Either codestar_connection or oauth_secret_name must be set.
    """

    repository: str
    branch: Annotated[str, PlainValidator(EnvVarStr.validate)]
    codestar_connection: UniqueIDType | None = None
    oauth_secret_name: str | None = None

    @model_validator(mode="after")
    @classmethod
    def check_source_config(cls, v):
        if not v.codestar_connection and not v.oauth_secret_name:
            raise ValueError("Either codestar_connection or oauth_secret_name must be set")
        if v.codestar_connection and v.oauth_secret_name:
            logging.warning("Only one of codestar_connection or oauth_secret_name can be set")
        return v


class CodePipelineNotificationsConfig(BaseModel):
    """Configuration for CodePipeline notifications.

    Attributes:
        slack_channel_configuration_arn (Optional[str]): ARN of Slack channel.
        notify_on_failure (bool): Send notification on failure.
        notify_on_success (bool): Send notification on success.
    """

    slack_channel_configuration_arn: str | None
    notify_on_failure: bool = False
    notify_on_success: bool = False

    @property
    def notify_on_any(self) -> bool:
        """Check if any notifications are enabled.

        Returns:
            True if either failure or success notifications are enabled.
        """
        return self.notify_on_failure or self.notify_on_success


class PipelineConfig(BaseModel):
    """Complete pipeline configuration.

    Attributes:
        enable (bool): Whether the pipeline is enabled.
        build (CodePipelineBuildConfig): Build configuration.
        source (CodePipelineSourceConfig): Source configuration.
        notifications (CodePipelineNotificationsConfig): Notification settings.
    """

    enable: bool
    build: CodePipelineBuildConfig
    source: CodePipelineSourceConfig
    notifications: CodePipelineNotificationsConfig


class GlobalConfig(BaseModel):
    """Global project configuration.

    Attributes:
        pipeline_name (str): Name of the pipeline.
        stage_promotions (Dict[EnvType, EnvType]): Stage promotion mappings.
    """

    pipeline_name: str
    stage_promotions: dict[EnvType, EnvType]


class StageConfig(BaseModel):
    """Configuration for a deployment stage.

    Attributes:
        env (Env): Environment configuration.
        pipeline (Optional[PipelineConfig]): Optional pipeline configuration.
    """

    env: Env
    pipeline: PipelineConfig | None = None


DEFAULT_CONFIG_PATH = "configuration/project.yaml"


G = TypeVar("G", bound=GlobalConfig)
S = TypeVar("S", bound=StageConfig)
P = TypeVar("P", bound="BaseProjectConfig")


class BaseProjectConfig(BaseModel, Generic[G, S]):
    """Base class for project configuration.

    Generic base class that can be subclassed with custom global and stage
    config types.

    Attributes:
        global_config (G): Global configuration settings.
        default_config (S): Default stage configuration.
        default_config_overrides (Dict[EnvType, dict]): Per-environment overrides.
    """

    global_config: G
    default_config: S
    default_config_overrides: dict[EnvType, dict]

    @classmethod
    def get_global_config_cls(cls) -> type[G]:
        """Get the global config class type.

        Returns:
            The GlobalConfig type used by this project config.
        """
        return cls.model_fields["global_config"].annotation  # type: ignore

    @classmethod
    def get_stage_config_cls(cls) -> type[S]:
        """Get the stage config class type.

        Returns:
            The StageConfig type used by this project config.
        """
        return cls.model_fields["default_config"].annotation  # type: ignore

    def get_stage_config(self, env_type: str | EnvType, env_label: str | None = None) -> S:
        """Get stage config with environment type overrides.

        Args:
            env_type (Union[str, EnvType]): The environment type for the stage config.
            env_label (Optional[str]): Optional environment label override.
                Defaults to None (no override).

        Returns:
            A stage config object with applied overrides.

        Raises:
            Exception: If stage config model validation fails.
        """
        try:
            stage_config = self.get_stage_config_cls().model_validate(
                {
                    **DeepChainMap(
                        self.default_config_overrides[EnvType(env_type)],
                        self.default_config.model_dump(mode="json", exclude_unset=True),
                    ),
                }
            )
        except Exception as e:
            raise e

        if env_label is None:
            return stage_config
        else:
            stage_config.env.label = env_label
            return stage_config

    @classmethod
    def parse_file(cls: type[P], path: str | Path) -> P:
        """Parse configuration from a file.

        Args:
            path (Union[str, Path]): Path to the configuration file.
                Supports YAML and JSON formats.

        Returns:
            The parsed project configuration.
        """
        path = Path(path)

        if path.suffix in (".yml", ".yaml"):
            with open(path) as f:
                return cls.model_validate(yaml.safe_load(f))
        return cls.model_validate_json(json_data=path.read_text())

    @classmethod
    def load_config(cls: type[P], path: str | Path | None = None) -> P:
        """Load configuration from a file.

        Args:
            path (Optional[Union[str, Path]]): Path to config file.
                If None, searches for project.yaml in current directory.

        Returns:
            The loaded project configuration.

        Raises:
            AssertionError: If multiple or no project.yaml files found.
        """
        if path is None:
            paths = find_paths(
                Path.cwd(), include_dirs=False, include_files=True, includes=[r".*/project.yaml"]
            )
            assert (
                len(paths) == 1
            ), f"Expected to find exactly one project.yaml file, but found {len(paths)}: {paths}"
            path = paths[0]
        return cls.parse_file(path=path)

    @classmethod
    def load_stage_config(
        cls: type["BaseProjectConfig[G, S]"],
        env_type: str | EnvType,
        path: str | Path | None = None,
    ) -> S:
        """Load stage configuration for an environment type.

        Args:
            env_type (Union[str, EnvType]): The environment type.
            path (Optional[Union[str, Path]]): Path to config file.

        Returns:
            The stage configuration for the specified environment.
        """
        proj_config = cls.load_config(path)
        return proj_config.get_stage_config(env_type=env_type)


class ProjectConfig(BaseProjectConfig[GlobalConfig, StageConfig]):
    """Default project configuration using standard global and stage configs."""

    pass


class ConfigProvider:
    """Utility class for loading stage configurations."""

    @classmethod
    def get_stage_config(
        cls,
        env_type: str | EnvType,
        path: str | Path | None = None,
        project_config_cls: type[BaseProjectConfig[G, S]] = ProjectConfig,
    ) -> S:
        """Get stage configuration for an environment type.

        Args:
            env_type (Union[str, EnvType]): The environment type.
            path (Optional[Union[str, Path]]): Path to config file.
            project_config_cls (Type[BaseProjectConfig[G, S]]): Project config class
                to use. Defaults to ProjectConfig.

        Returns:
            The stage configuration.
        """
        proj_config = project_config_cls.load_config(path)
        return proj_config.get_stage_config(env_type=env_type)
