__all__ = [
    "Env",
    "EnvBase",
    "EnvType",
    "CodePipelineSourceConfig",
    "CodePipelineNotificationsConfig",
    "GlobalConfig",
    "StageConfig",
    "PipelineConfig",
    "ProjectConfig",
    "ConfigProvider",
]

from pathlib import Path
from typing import Dict, MutableMapping, Optional, Type, Union

import pydantic
import yaml
from aibs_informatics_core.collections import DeepChainMap
from aibs_informatics_core.env import ENV_BASE_KEY, ENV_LABEL_KEY, ENV_TYPE_KEY, EnvBase, EnvType
from aibs_informatics_core.models.unique_ids import UniqueID
from aibs_informatics_core.utils.os_operations import expandvars
from aibs_informatics_core.utils.tools.dicttools import remove_null_values


class EnvVarStr(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(
            examples=["prefix{USER}suffix", "{ENV_VAR2}"],
        )

    @classmethod
    def validate(cls, v):
        if v is not None and not isinstance(v, str):
            raise TypeError("string required")
        return expandvars(v, "", False) if v else v

    def __repr__(self):
        return f"{self.__class__.__name__}({super().__repr__()})"


class Env(pydantic.BaseModel):
    env_type: EnvType
    label: Optional[EnvVarStr] = None
    account: Optional[EnvVarStr] = None
    region: Optional[EnvVarStr] = None

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
            ),
            in_place=True,
        )


class CodePipelineBuildConfig(pydantic.BaseModel):
    ssh_key_secret_name: str
    docker_hub_credentials_secret_name: str


class CodePipelineSourceConfig(pydantic.BaseModel):
    repository: str
    branch: EnvVarStr
    codestar_connection: UniqueID
    oauth_secret_name: str


class CodePipelineNotificationsConfig(pydantic.BaseModel):
    slack_channel_configuration_arn: Optional[str]
    notify_on_failure: bool = False
    notify_on_success: bool = False

    @property
    def notify_on_any(self) -> bool:
        return self.notify_on_failure or self.notify_on_success


class PipelineConfig(pydantic.BaseModel):
    enable: bool
    build: CodePipelineBuildConfig
    source: CodePipelineSourceConfig
    notifications: CodePipelineNotificationsConfig


class GlobalConfig(pydantic.BaseModel):
    pipeline_name: str
    stage_promotions: Dict[EnvType, EnvType]


class StageConfig(pydantic.BaseModel):
    env: Env
    pipeline: PipelineConfig


DEFAULT_CONFIG_PATH = "configuration/project.yaml"


class ProjectConfig(pydantic.BaseModel):
    global_config: GlobalConfig
    default_config: StageConfig
    default_config_overrides: Dict[EnvType, dict]

    def get_stage_config(self, env_type: Union[str, EnvType]) -> StageConfig:
        """Get default config with `EnvType` overrides"""

        try:
            return StageConfig.parse_obj(
                {
                    **DeepChainMap(
                        self.default_config_overrides[EnvType(env_type)],
                        self.default_config.dict(exclude_unset=True),
                    ),
                }
            )
        except Exception as e:
            raise e

    @classmethod
    def parse_file(
        cls: Type["ProjectConfig"],
        path: Union[str, Path],
        *,
        content_type: str = None,
        encoding: str = "utf8",
        proto: pydantic.Protocol = None,
        allow_pickle: bool = False,
    ) -> "ProjectConfig":
        path = Path(path)

        if path.suffix in (".yml", ".yaml"):
            with open(path, "r") as f:
                return cls.parse_obj(yaml.safe_load(f))
        return super().parse_file(
            path,
            content_type=content_type,
            encoding=encoding,
            proto=proto,
            allow_pickle=allow_pickle,
        )

    @classmethod
    def load_config(cls, path: Union[str, Path] = DEFAULT_CONFIG_PATH) -> "ProjectConfig":
        return cls.parse_file(path=path)


class ConfigProvider:
    @classmethod
    def get_stage_config(
        cls, env_type: Union[str, EnvType], path: Union[str, Path] = DEFAULT_CONFIG_PATH
    ) -> "ProjectConfig":
        proj_config = ProjectConfig.parse_file(path)
        return proj_config.get_stage_config(env_type=env_type)
