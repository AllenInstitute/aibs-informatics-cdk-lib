import logging
from abc import abstractmethod
from typing import Dict, Generic, List, Mapping, TypeVar

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_codepipeline_actions
from aws_cdk import aws_codestarnotifications as codestarnotifications
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import pipelines
from aws_cdk.aws_codebuild import (
    BuildEnvironment,
    BuildEnvironmentVariable,
    BuildSpec,
    LinuxBuildImage,
)

from aibs_informatics_cdk_lib.common.aws.core_utils import build_arn
from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    CODE_BUILD_IAM_POLICY,
    DYNAMODB_READ_ACTIONS,
    S3_FULL_ACCESS_ACTIONS,
)
from aibs_informatics_cdk_lib.project.config import (
    BaseProjectConfig,
    CodePipelineSourceConfig,
    Env,
    GlobalConfig,
    PipelineConfig,
    ProjectConfig,
    StageConfig,
)
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


STAGE_CONFIG = TypeVar("STAGE_CONFIG", bound=StageConfig)
GLOBAL_CONFIG = TypeVar("GLOBAL_CONFIG", bound=GlobalConfig)


class BasePipelineStack(EnvBaseStack, Generic[STAGE_CONFIG, GLOBAL_CONFIG]):
    """Defines the CI/CD Pipeline for the an Environment.

    https://docs.aws.amazon.com/cdk/api/v1/docs/pipelines-readme.html

    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        config: BaseProjectConfig[GLOBAL_CONFIG, STAGE_CONFIG],
        **kwargs,
    ) -> None:
        self.project_config = config
        self.stage_config = config.get_stage_config(env_base.env_type)
        env = cdk.Environment(
            account=self.stage_config.env.account, region=self.stage_config.env.region
        )
        super().__init__(scope, id, config=config, env=env, **kwargs)
        self.pipeline = self.initialize_pipeline()

    @property
    def project_config(self) -> BaseProjectConfig[GLOBAL_CONFIG, STAGE_CONFIG]:
        return self._project_config

    @project_config.setter
    def project_config(self, value: BaseProjectConfig[GLOBAL_CONFIG, STAGE_CONFIG]):
        self._project_config = value

    @property
    def global_config(self) -> GLOBAL_CONFIG:
        return self.project_config.global_config

    @property
    def stage_config(self) -> STAGE_CONFIG:
        return self._stage_config

    @stage_config.setter
    def stage_config(self, value: STAGE_CONFIG):
        self._stage_config = value

    @property
    def pipeline_config(self) -> PipelineConfig:
        assert self.stage_config.pipeline is not None
        return self.stage_config.pipeline

    @property
    def codebuild_environment_variables(self) -> Mapping[str, BuildEnvironmentVariable]:
        defaults = {
            k: BuildEnvironmentVariable(value=v)
            for k, v in self.stage_config.env.to_env_var_map().items()
        }
        return {
            **self.custom_codebuild_environment_variables,
            **defaults,
        }

    @property
    def custom_codebuild_environment_variables(self) -> Mapping[str, BuildEnvironmentVariable]:
        return {}

    @property
    def source_cache(self) -> Dict[str, pipelines.CodePipelineSource]:
        try:
            return self._source_cache
        except AttributeError:
            self.source_cache = {}
        return self._source_cache

    @source_cache.setter
    def source_cache(self, value: Dict[str, pipelines.CodePipelineSource]):
        self._source_cache = value

    def get_pipeline_source(
        self, source_config: CodePipelineSourceConfig
    ) -> pipelines.CodePipelineSource:
        """
        Constructs a Github Repo source from a config

        Args:
            source_config (CodePipelineSourceConfig): config

        Returns:
            pipelines.CodePipelineSource:
        """
        # CDK doesnt like when we reconstruct code pipeline source with the same repo name.
        # So we need to cache the results for a given result if config has same repo name.

        if source_config.repository not in self.source_cache:
            if source_config.codestar_connection:
                source = pipelines.CodePipelineSource.connection(
                    repo_string=source_config.repository,
                    branch=source_config.branch,
                    connection_arn=build_arn(
                        service="codestar-connections",
                        resource_type="connection",
                        resource_delim="/",
                        resource_id=source_config.codestar_connection,
                    ),
                    code_build_clone_output=True,
                    trigger_on_push=True,
                )
            elif source_config.oauth_secret_name:
                source = pipelines.CodePipelineSource.git_hub(
                    repo_string=source_config.repository,
                    branch=source_config.branch,
                    authentication=cdk.SecretValue.secrets_manager(
                        secret_id=source_config.oauth_secret_name
                    ),
                    trigger=aws_codepipeline_actions.GitHubTrigger.WEBHOOK,
                )
            else:
                raise ValueError(
                    "Invalid source config. Must have codestar_connection or oauth_secret_name"
                )
            self.source_cache[source_config.repository] = source
        return self.source_cache[source_config.repository]

    @abstractmethod
    def initialize_pipeline(self) -> pipelines.CodePipeline:
        raise NotImplementedError("Subclasses must implement this method")
