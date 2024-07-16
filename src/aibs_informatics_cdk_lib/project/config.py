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
from pathlib import Path
from typing import Annotated, Dict, Generic, List, MutableMapping, Optional, Type, TypeVar, Union

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
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if v is not None and not isinstance(v, str):
            raise TypeError("string required")
        return expandvars(v, "", False) if v else v

    def __repr__(self):
        return f"{self.__class__.__name__}({super().__repr__()})"


class Env(BaseModel):
    env_type: EnvType
    label: Annotated[Optional[str], PlainValidator(EnvVarStr.validate)] = None
    account: Annotated[Optional[str], PlainValidator(EnvVarStr.validate)] = None
    region: Annotated[Optional[str], PlainValidator(EnvVarStr.validate)] = None

    @property
    def env_name(self) -> str:
        return self.env_type.value

    @property
    def env_base(self) -> EnvBase:
        """Returns <env_name>[-<env_label>]"""
        return EnvBase.from_type_and_label(self.env_type, self.label)

    @property
    def is_configured(self) -> bool:
        return self.account is not None

    def to_env_var_map(self) -> MutableMapping[str, str]:
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
    ssh_key_secret_name: str
    docker_hub_credentials_secret_name: str


class CodePipelineSourceConfig(BaseModel):
    repository: str
    branch: Annotated[str, PlainValidator(EnvVarStr.validate)]
    codestar_connection: Optional[UniqueIDType] = None
    oauth_secret_name: Optional[str] = None

    @model_validator(mode="after")
    @classmethod
    def check_source_config(cls, v):
        if not v.codestar_connection and not v.oauth_secret_name:
            raise ValueError("Either codestar_connection or oauth_secret_name must be set")
        if v.codestar_connection and v.oauth_secret_name:
            logging.warning("Only one of codestar_connection or oauth_secret_name can be set")
        return v


class CodePipelineNotificationsConfig(BaseModel):
    slack_channel_configuration_arn: Optional[str]
    notify_on_failure: bool = False
    notify_on_success: bool = False

    @property
    def notify_on_any(self) -> bool:
        return self.notify_on_failure or self.notify_on_success


class PipelineConfig(BaseModel):
    enable: bool
    build: CodePipelineBuildConfig
    source: CodePipelineSourceConfig
    notifications: CodePipelineNotificationsConfig


class GlobalConfig(BaseModel):
    pipeline_name: str
    stage_promotions: Dict[EnvType, EnvType]


class StageConfig(BaseModel):
    env: Env
    pipeline: Optional[PipelineConfig] = None


DEFAULT_CONFIG_PATH = "configuration/project.yaml"


G = TypeVar("G", bound=GlobalConfig)
S = TypeVar("S", bound=StageConfig)
P = TypeVar("P", bound="BaseProjectConfig")


class BaseProjectConfig(BaseModel, Generic[G, S]):
    global_config: G
    default_config: S
    default_config_overrides: Dict[EnvType, dict]

    @classmethod
    def get_global_config_cls(cls) -> Type[G]:
        return cls.model_fields["global_config"].annotation  # type: ignore

    @classmethod
    def get_stage_config_cls(cls) -> Type[S]:
        return cls.model_fields["default_config"].annotation  # type: ignore

    def get_stage_config(
        self, env_type: Union[str, EnvType], env_label: Optional[str] = None
    ) -> S:
        """Get default config with `EnvType` overrides (and optional env_label override)

        Args:
            env_type (Union[str, EnvType]): The EnvType override for the stage config to
                be retrieved.
            env_label (Optional[str], optional): An optional env_label override for
                the stage config to be retrieved. Defaults to None (no override).

        Raises:
            e: If the stage config model validation fails (or any other error)

        Returns:
            S: A stage config object
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
    def parse_file(cls: Type[P], path: Union[str, Path]) -> P:
        path = Path(path)

        if path.suffix in (".yml", ".yaml"):
            with open(path, "r") as f:
                return cls.model_validate(yaml.safe_load(f))
        return cls.model_validate_json(json_data=path.read_text())

    @classmethod
    def load_config(cls: Type[P], path: Optional[Union[str, Path]] = None) -> P:
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
        cls: Type["BaseProjectConfig[G, S]"],
        env_type: Union[str, EnvType],
        path: Optional[Union[str, Path]] = None,
    ) -> S:
        proj_config = cls.load_config(path)
        return proj_config.get_stage_config(env_type=env_type)


class ProjectConfig(BaseProjectConfig[GlobalConfig, StageConfig]):
    pass


class ConfigProvider:
    @classmethod
    def get_stage_config(
        cls,
        env_type: Union[str, EnvType],
        path: Optional[Union[str, Path]] = None,
        project_config_cls: Type[BaseProjectConfig[G, S]] = ProjectConfig,
    ) -> S:
        proj_config = project_config_cls.load_config(path)
        return proj_config.get_stage_config(env_type=env_type)
