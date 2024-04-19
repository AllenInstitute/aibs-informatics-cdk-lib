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
from typing import Annotated, Dict, MutableMapping, Optional, Type, Union

import yaml
from aibs_informatics_core.collections import DeepChainMap
from aibs_informatics_core.env import ENV_BASE_KEY, ENV_LABEL_KEY, ENV_TYPE_KEY, EnvBase, EnvType
from aibs_informatics_core.models.unique_ids import UniqueID
from aibs_informatics_core.utils.os_operations import expandvars
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from pydantic import BaseModel, PlainSerializer, PlainValidator

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
    codestar_connection: UniqueIDType
    oauth_secret_name: str


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


class ProjectConfig(BaseModel):
    global_config: GlobalConfig
    default_config: StageConfig
    default_config_overrides: Dict[EnvType, dict]

    def get_stage_config(self, env_type: Union[str, EnvType]) -> StageConfig:
        """Get default config with `EnvType` overrides"""

        try:
            return StageConfig.model_validate(
                {
                    **DeepChainMap(
                        self.default_config_overrides[EnvType(env_type)],
                        self.default_config.model_dump(mode="json", exclude_unset=True),
                    ),
                }
            )
        except Exception as e:
            raise e

    @classmethod
    def parse_file(
        cls: Type["ProjectConfig"],
        path: Union[str, Path],
    ) -> "ProjectConfig":
        path = Path(path)

        if path.suffix in (".yml", ".yaml"):
            with open(path, "r") as f:
                return cls.model_validate(yaml.safe_load(f))
        return cls.model_validate_json(json_data=path.read_text())

    @classmethod
    def load_config(cls, path: Union[str, Path] = DEFAULT_CONFIG_PATH) -> "ProjectConfig":
        return cls.parse_file(path=path)


class ConfigProvider:
    @classmethod
    def get_stage_config(
        cls, env_type: Union[str, EnvType], path: Union[str, Path] = DEFAULT_CONFIG_PATH
    ) -> "StageConfig":
        proj_config = ProjectConfig.load_config(path)
        return proj_config.get_stage_config(env_type=env_type)
