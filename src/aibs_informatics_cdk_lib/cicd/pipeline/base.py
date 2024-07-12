import base64
import logging
from abc import abstractmethod
from importlib.resources import files
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_codepipeline as aws_codepipeline
from aws_cdk import aws_codepipeline_actions
from aws_cdk import aws_codestarnotifications as codestarnotifications
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sns as sns
from aws_cdk import pipelines
from aws_cdk.aws_codebuild import BuildEnvironment, BuildEnvironmentVariable, BuildSpec

from aibs_informatics_cdk_lib.common.aws.core_utils import build_arn
from aibs_informatics_cdk_lib.common.aws.iam_utils import CODE_BUILD_IAM_POLICY
from aibs_informatics_cdk_lib.project.config import (
    BaseProjectConfig,
    CodePipelineSourceConfig,
    GlobalConfig,
    PipelineConfig,
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

PIPELINE_STACK = TypeVar("PIPELINE_STACK", bound="BasePipelineStack")


import functools
from dataclasses import dataclass


@dataclass
class PipelineStageInfo:
    order: int
    name: str
    pre_steps: Optional[List[pipelines.Step]] = None
    post_steps: Optional[List[pipelines.Step]] = None


def pipeline_stage(
    order: int,
    name: str,
    pre_steps: Optional[List[pipelines.Step]] = None,
    post_steps: Optional[List[pipelines.Step]] = None,
):
    """Method decorator for defining a pipeline stage in a BasePipelineStack subclass.

    you can decorate two types of methods:
    1. A method that returns a cdk.Stage
    2. A method that returns a tuple of pre_steps, cdk.Stage, post_steps
        where pre_steps and post_steps are lists of pipelines.Step objects.

    Example:

    ```python
    class PipelineStack(BasePipelineStack):

        ...

        @pipeline_stage(order=0, name="Source", pre_steps=[...])
        def source_stage(self) -> cdk.Stage:
            return SourceStage(self, self.get_construct_id("source-stage"))

        @pipeline_stage(order=1, name="Build")
        def build_stage(self) -> cdk.Stage:
            return BuildStage(self, self.get_construct_id("build-stage"))

        @pipeline_stage(order=2, name="Deploy")
        def deploy_stage(self) -> Tuple[List[pipelines.Step], cdk.Stage, List[pipelines.Step]]:
            pre_steps = [...]
            post_steps = [...]
            stage = DeployStage(self, self.get_construct_id("deploy-stage"))
            return pre_steps, stage, post_steps

    ```

    Args:
        order (int): Order of the stage. Lower numbers are executed first. E.g. 1, 2, 3, ...
            You can repeat numbers, however, the order will be arbitrary.
        name (str): Name of the stage
        pre_steps (Optional[List[pipelines.Step]], optional): Optional pre steps to add before the stage.
            Defaults to None.
        post_steps (Optional[List[pipelines.Step]], optional): Optional post steps to add after the stage.
            Defaults to None.
    """

    def decorator_pipeline_stage(
        func: Callable[[PIPELINE_STACK], Union[cdk.Stage, Tuple[cdk.Stage]]]
    ) -> Callable[
        [PIPELINE_STACK],
        Tuple[Optional[Sequence[pipelines.Step]], cdk.Stage, Optional[Sequence[pipelines.Step]]],
    ]:
        @functools.wraps(func)
        def wrapper_pipeline_stage(
            *args, **kwargs
        ) -> Tuple[
            Optional[Sequence[pipelines.Step]], cdk.Stage, Optional[Sequence[pipelines.Step]]
        ]:
            results = func(*args, **kwargs)
            if isinstance(results, cdk.Stage):
                return None, results, None
            assert isinstance(results, tuple) and len(results) == 3
            assert isinstance(results[0], list) or results[0] is None
            assert isinstance(results[1], cdk.Stage)
            assert isinstance(results[2], list) or results[2] is None
            return cast(
                Tuple[
                    Optional[Sequence[pipelines.Step]],
                    cdk.Stage,
                    Optional[Sequence[pipelines.Step]],
                ],
                results,
            )

        wrapper_pipeline_stage._pipeline_stage_info = PipelineStageInfo(  # type: ignore[attr-defined]
            order=order, name=name, pre_steps=pre_steps, post_steps=post_steps
        )
        return wrapper_pipeline_stage

    return decorator_pipeline_stage


class BasePipelineStack(EnvBaseStack, Generic[STAGE_CONFIG, GLOBAL_CONFIG]):
    """Defines the CI/CD Pipeline for the an Environment.

    This class is meant to be subclassed to define the pipeline for a specific project.

    You are required to implement the `initialize_pipeline` method which should return
    a `pipelines.CodePipeline` object. You can then add stages to the pipeline by defining
    methods that are decorated with the `@pipeline_stage` decorator.

    Example:

    ```python

    class MyPipelineStack(BasePipelineStack):

        @pipeline_stage(order=1, pre_steps=[...], post_steps=[...])
        def build_stage(self) -> cdk.Stage:
            # Define the steps for the build stage
            build_steps = [
                pipelines.CodeBuildStep(
                    "Build",
                    input=self.get_pipeline_source(self.pipeline_config.source),
                    commands=[
                        "echo 'Building the project'",
                        "npm install",
                        "npm run build",
                    ],
                    role_policy_statements=[
                        self.get_policy_with_secrets(self.pipeline_config.source.oauth_secret_name),
                    ],
                ),
            ]

            # Create the build stage
            build_stage = pipelines.Stage(
                self,
                "BuildStage",
                stage_name="Build",
                actions=build_steps,
            )

            return build_stage


        The following steps are available for pipelines inheriting from this class:
            promotion_stage:    A stage that is added after all other stages. This stage
                                is used to promote the deployment to another environment.
            notifications:      A notification topic that is used to send notifications. You can
                                enable notifications for pipeline failures and successes. This
                                can be configured in the `pipeline_config.notifications` attribute.
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
        self.stage_config.env.label = env_base.env_label
        env = cdk.Environment(
            account=self.stage_config.env.account, region=self.stage_config.env.region
        )
        super().__init__(scope, id, env_base=env_base, env=env, **kwargs)
        self.build_pipeline()

    @abstractmethod
    def initialize_pipeline(self) -> pipelines.CodePipeline:
        raise NotImplementedError("Subclasses must implement this method")

    def build_pipeline(self):
        """Builds the pipeline. This method should be called after the pipeline is initialized.

        This should not be overridden by subclasses unless you know what you are doing.

        This method will:
            1. Initialize the pipeline
            2. Add stages to the pipeline
            3. Add a promotion stage
            4. Build the pipeline
            5. Setup notifications

        """
        # Initialize Pipeline
        self.pipeline = self.initialize_pipeline()

        # Add Stages
        for stage_method in self.get_stage_methods():
            stage_info: PipelineStageInfo = stage_method._pipeline_stage_info  # type: ignore[attr-defined]
            pre_steps, stage, post_steps = stage_method()

            if stage_info.pre_steps is not None:
                pre_steps = [*stage_info.pre_steps, *(pre_steps or [])]
            if stage_info.post_steps is not None:
                post_steps = [*stage_info.post_steps, *(post_steps or [])]

            self.pipeline.add_stage(stage, pre=pre_steps, post=post_steps)

        # Add Promotion Stage
        self.add_promotion_stage(self.pipeline)

        # Build the pipeline
        self.pipeline.build_pipeline()

        # Post Build Setup
        self.setup_notifications(self.pipeline)

    def add_promotion_stage(self, pipeline: pipelines.CodePipeline):
        """Adds a promotion stage to a CodePipeline

        Promotion stages are used to promote the deployment to another environment.
        These promotions are done through github pull requests. This is a major foundation
        for the deployment process.

        The environment promotion definitions are defined in the `global_config.stage_promotions`.
        This is a mapping of source environment types to target environment types.

        The branch that is used for the promotion is defined in the `pipeline_config.source.branch`.

        Args:
            pipeline (pipelines.CodePipeline): Code Pipeline
        """
        global_config = self.global_config
        pipeline_config = self.pipeline_config

        # In order to add a CodePipeline Stage without stacks, we must use `add_wave`
        # https://github.com/aws/aws-cdk/issues/15945#issuecomment-895392052
        promote_wave = pipeline.add_wave("Release")

        # POST Steps
        if (source_env_type := self.stage_config.env.env_type) in global_config.stage_promotions:
            promotion_target_env_type = global_config.stage_promotions[source_env_type]
            promotion_target_pipeline_config = self.project_config.get_stage_config(
                promotion_target_env_type
            ).pipeline
            assert promotion_target_pipeline_config is not None
            create_pull_request_step = pipelines.CodeBuildStep(
                "CreateReleasePullRequest",
                input=self.get_pipeline_source(pipeline_config.source),
                # Environment needs to have privelaged access
                build_environment=BuildEnvironment(privileged=True),
                # By default bin/sh is used, so lets set to bash
                # https://docs.aws.amazon.com/codebuild/latest/userguide/build-spec-ref.html#build-spec.shell
                partial_build_spec=BuildSpec.from_object(
                    {
                        "env": {
                            "shell": "bash",
                            "variables": {
                                "CICD_RELEASE_REVIEWER": "AllenInstitute/marmot",
                                "CICD_RELEASE_SOURCE_ENV_TYPE": source_env_type,
                                "CICD_RELEASE_TARGET_ENV_TYPE": promotion_target_env_type,
                                "CICD_RELEASE_TARGET_BRANCH": promotion_target_pipeline_config.source.branch,
                            },
                            # https://docs.aws.amazon.com/codebuild/latest/userguide/build-spec-ref.html#build-spec.env.secrets-manager
                            "secrets-manager": {
                                "GITHUB_TOKEN": pipeline_config.source.oauth_secret_name,
                            },
                            "git-credential-helper": "yes",
                        },
                    }
                ),
                install_commands=[
                    # Installing Github CLI (via https://github.com/cli/cli/blob/trunk/docs/install_linux.md)
                    #   1. Resolve Download URL via GH API
                    #   2. Download binary archive
                    #   3. Unarchive and move binary into /usr/local/bin
                    #   4. Verify command is available
                    # Step 1:
                    'GH_CLI_DOWNLOAD_LINK=$(curl -H "Authorization:token $GITHUB_TOKEN" -sSL "https://api.github.com/repos/cli/cli/releases/latest" | jq -r \'.assets[] | select(.name|test(".*_linux_amd64.tar.gz")) | .browser_download_url\')',
                    "GH_CLI_TAR_GZ_PATH=$(basename $GH_CLI_DOWNLOAD_LINK)",
                    "GH_CLI_DIR=$(basename $GH_CLI_TAR_GZ_PATH .tar.gz)",
                    # Step 2:
                    'curl -H "Authorization:token $GITHUB_TOKEN" -sSL $GH_CLI_DOWNLOAD_LINK -o $GH_CLI_TAR_GZ_PATH',
                    # Step 3:
                    "tar -xf $GH_CLI_TAR_GZ_PATH",
                    "sudo cp $GH_CLI_DIR/bin/gh /usr/local/bin/",
                    # Step 4:
                    "gh --version &> /dev/null",
                ],
                commands=[
                    # Setting up repository WITH git metadata
                    #   Why?
                    #   because Github Version 1 CodePipeline Source does not support
                    #   option for including git metadata. Github Version 2 does this,
                    #   but we cannot use this configuration currently.
                    #   What is going on below?
                    #   1. clone the git repository and work off of that.
                    #   2. Enable caching and store credentials
                    #   3. Checkout branch based on source commit
                    #   4. Run our CI/CD release script
                    "export REPO_DIR=$(mktemp -d)",
                    "cd $REPO_DIR",
                    f"git clone https://${{GITHUB_TOKEN}}@github.com/{pipeline_config.source.repository}.git .",
                    # Enables credential caching
                    "git config credential.helper store",
                    # Supposed to force the caching of the credentials
                    "git pull",
                    # Creates a temporary branch using the source commit as its head.
                    #   This ensures that we use the release branch.
                    "git checkout -b $(basename $REPO_DIR) $CODEBUILD_RESOLVED_SOURCE_VERSION",
                    ## Step: Download and run release script
                    # Create a temporary directory and file to store the release script
                    "export RELEASE_SCRIPT_PATH=$(mktemp -d)/cicd-release.sh",
                    "mkdir -p $(dirname $RELEASE_SCRIPT_PATH)",
                    # The release script will not be available to us unless we set up
                    # a virtual environment and install our source package. This is because the
                    # release script is in a dependent package (aibs-informatics-cdk-lib) and
                    # is not included in the source package used as input for this step.
                    # Assuming we want to avoid having to install the package, We have two options here:
                    # TODO: Decide which approach is better (prefer 2)
                    #   1. Download the release script from the source repository (using gh cli)
                    #       - This requires the use of the Github CLI
                    #       - This does not couple changes being deployed with the script in repo
                    #       - This is the most direct approach
                    #   2. Base64 encode the release script and decode it on the other side
                    #       - This is a bit more complex
                    #       - This couples changes being deployed with the script in repo
                    (
                        # Download the release script from the source repository (using gh cli)
                        'gh api repos/AllenInstitute/aibs-informatics-cdk-lib/contents/src/aibs_informatics_cdk_lib/cicd/pipeline/scripts/cicd-release.sh --raw -H "Accept: application/vnd.github.v3.raw" > $RELEASE_SCRIPT_PATH'
                        if False
                        else
                        # Here we are base64 encoding the release script and decoding it on the other side
                        # Steps:
                        #   1. Read the release script file
                        #   2. Base64 encode the file
                        #   3. Decode the base64 encoded file and write it to the release script path
                        # TODO: i think `importlib.resources.files` is preferred way to go here, but
                        #       it requires specifying the package path. This is a bit more
                        #       difficult to do in this context. So we are using the Path approach.
                        f"echo {base64.b64encode((Path(__file__).parent / 'scripts' / 'cicd-release.sh').read_text().encode()).decode()} | base64 --decode > $RELEASE_SCRIPT_PATH"
                    ),
                    # Run the release script
                    "bash $RELEASE_SCRIPT_PATH",
                ],
                role_policy_statements=[
                    CODE_BUILD_IAM_POLICY,
                    self.get_policy_with_secrets(self.pipeline_config.source.oauth_secret_name),
                ],
            )
            # Add dependencies to all other "post" steps
            if promote_wave.post:
                for post_step in promote_wave.post:
                    create_pull_request_step.add_step_dependency(post_step)

            promote_wave.add_post(create_pull_request_step)

    def setup_notifications(self, pipeline: pipelines.CodePipeline):
        notifications_config = self.pipeline_config.notifications
        if notifications_config.notify_on_any:
            sns_notifications_topic = sns.Topic(
                self,
                self.get_construct_id("sns-notifications"),
                display_name=f"Deployment Pipeline Notifications ({self.env_base})",
                topic_name=f"{self.env_base}-deployment-pipeline-notifications",
            )

            # Pipeline/Stage/Action Failure Notifications
            pipeline.pipeline.notify_on(
                self.get_construct_id("any-failures"),
                target=sns_notifications_topic,  # type: ignore # Topic should match ITopic
                enabled=notifications_config.notify_on_failure,
                events=[
                    aws_codepipeline.PipelineNotificationEvents.PIPELINE_EXECUTION_FAILED,
                ],
                notification_rule_name=f"{self.env_base}-Deployment-Pipeline-Failures",
                detail_type=codestarnotifications.DetailType.FULL,
            )

            # Pipeline Completion Notifications
            pipeline.pipeline.notify_on(
                self.get_construct_id("pipeline-complete"),
                target=sns_notifications_topic,  # type: ignore # Topic should match ITopic
                enabled=notifications_config.notify_on_success,
                events=[
                    aws_codepipeline.PipelineNotificationEvents.PIPELINE_EXECUTION_SUCCEEDED,
                ],
                detail_type=codestarnotifications.DetailType.BASIC,
                notification_rule_name=f"{self.env_base}-Deployment-Pipeline-Success",
            )

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

    def get_stage_methods(
        self,
    ) -> List[Callable[[], Tuple[Sequence[pipelines.Step], cdk.Stage, Sequence[pipelines.Step]]]]:
        # Get all methods of the instance
        methods = [
            getattr(self, method_name)
            for method_name in dir(self)
            if callable(getattr(self, method_name))
        ]

        # Filter methods that have the _pipeline_stage_info attribute
        stage_methods = [method for method in methods if hasattr(method, "_pipeline_stage_info")]

        # Sort methods by their order attribute
        stage_methods.sort(key=lambda method: method._pipeline_stage_info.order)  # type: ignore[attr-defined]

        # Return the sorted methods
        return stage_methods

    @staticmethod
    def get_policy_with_secrets(*secret_names: Optional[str]) -> iam.PolicyStatement:
        return iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetRandomPassword",
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds",
            ],
            resources=[
                build_arn(
                    service="secretsmanager",
                    resource_type="secret",
                    resource_delim=":",
                    resource_id=f"{secret_name}-??????",
                )
                for secret_name in secret_names
                if secret_name is not None
            ],
        )
