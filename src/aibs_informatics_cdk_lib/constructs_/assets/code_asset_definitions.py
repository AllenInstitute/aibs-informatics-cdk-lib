import logging
import os
from pathlib import Path
from typing import Optional

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.hashing import generate_path_hash
from aws_cdk import aws_ecr_assets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_assets

from aibs_informatics_cdk_lib.common.git import clone_repo, is_local_repo, is_repo_url
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    GLOBAL_GLOB_EXCLUDES,
    PYTHON_GLOB_EXCLUDES,
    PYTHON_REGEX_EXCLUDES,
    CodeAsset,
)

AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR = "AIBS_INFORMATICS_AWS_LAMBDA_REPO"
AIBS_INFORMATICS_AWS_LAMBDA_REPO = "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git"

logger = logging.getLogger(__name__)


class AssetsMixin:
    @classmethod
    def resolve_repo_path(cls, repo_url: str, repo_path_env_var: Optional[str]) -> Path:
        """Resolves the repo path from the environment or clones the repo from the url

        This method is useful to quickly swapping between locally modified changes and the remote repo.

        This should typically be used in the context of defining a code asset for a static name
        (e.g. AIBS_INFORMATICS_AWS_LAMBDA). You can then use the env var option to point to a local
        repo path for development.

        Args:
            repo_url (str): The git repo url. This is required.
                If the repo path is not in the environment, the repo will be cloned from this url.
            repo_path_env_var (Optional[str]): The environment variable that contains the
                repo path or alternative repo url. This is optional.
                This is useful for local development.

        Returns:
            Path: The path to the repo
        """
        if repo_path_env_var and (repo_path := os.getenv(repo_path_env_var)) is not None:
            logger.info(f"Using {repo_path_env_var} from environment")
            if is_local_repo(repo_path):
                return Path(repo_path)
            elif is_repo_url(str(repo_path)):
                return clone_repo(repo_path, skip_if_exists=True)
            else:
                raise ValueError(f"Env variable {repo_path_env_var} is not a valid git repo")
        else:
            return clone_repo(repo_url, skip_if_exists=True)


class AIBSInformaticsCodeAssets(constructs.Construct, AssetsMixin):
    def __init__(
        self,
        scope: constructs.Construct,
        construct_id: str,
        env_base: EnvBase,
        runtime: Optional[lambda_.Runtime] = None,
        aibs_informatics_aws_lambda_repo: Optional[str] = None,
    ) -> None:
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self.runtime = runtime or lambda_.Runtime.PYTHON_3_11
        self.AIBS_INFORMATICS_AWS_LAMBDA_REPO = (
            aibs_informatics_aws_lambda_repo or AIBS_INFORMATICS_AWS_LAMBDA_REPO
        )

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> CodeAsset:
        """Returns a NEW code asset for aibs-informatics-aws-lambda

        Returns:
            CodeAsset: The code asset
        """

        repo_path = self.resolve_repo_path(
            self.AIBS_INFORMATICS_AWS_LAMBDA_REPO, AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR
        )

        asset_hash = generate_path_hash(
            path=str(repo_path.resolve()),
            excludes=PYTHON_REGEX_EXCLUDES,
        )
        logger.info(f"aibs-informatics-aws-lambda asset hash={asset_hash}")
        bundling_image = self.runtime.bundling_image
        host_ssh_dir = str(Path.home() / ".ssh")
        asset_props = aws_s3_assets.AssetProps(
            # CDK bundles lambda assets in a docker container. This causes issues for our local
            # path dependencies. In order to resolve the relative local path dependency,
            # we need to specify the path to the root of the repo.
            path=str(repo_path),
            asset_hash=asset_hash,
            # It is important to exclude files from the git repo, because
            #   1. it effectively makes our caching for assets moot
            #   2. we also don't want to include certain files for size reasons.
            exclude=[
                *PYTHON_GLOB_EXCLUDES,
                "**/cdk.out/",
                "**/scripts/**",
            ],
            bundling=cdk.BundlingOptions(
                image=bundling_image,
                working_directory="/asset-input",
                entrypoint=["/bin/bash", "-c"],
                command=[
                    # This makes the following commands run together as one
                    # WARNING Make sure not to modify {host_ssh_dir} in any way, in this set of commands!
                    " && ".join(
                        [
                            "set -x",
                            # Copy in host ssh keys that are needed to clone private git repos
                            f"cp -r {host_ssh_dir} /root/.ssh",
                            # Useful debug if anything goes wrong with github SSH related things
                            "ssh -vT git@github.com || true",
                            # Must make sure that the package is not installing using --editable mode
                            "python3 -m pip install --upgrade pip --no-cache",
                            "pip3 install . --no-cache -t /asset-output",
                            # TODO: remove botocore and boto3 from asset output
                            # Must make asset output permissions accessible to lambda
                            "find /asset-output -type d -print0 | xargs -0 chmod 755",
                            "find /asset-output -type f -print0 | xargs -0 chmod 644",
                        ]
                    ),
                ],
                user="root:root",
                volumes=[
                    cdk.DockerVolume(
                        host_path=host_ssh_dir,
                        container_path=host_ssh_dir,
                    ),
                ],
            ),
        )
        return CodeAsset(
            asset_name=os.path.basename(repo_path.resolve()),
            asset_props=asset_props,
            default_runtime=self.runtime,
            environment={
                self.env_base.ENV_BASE_KEY: self.env_base,
            },
        )


class AIBSInformaticsDockerAssets(constructs.Construct, AssetsMixin):
    def __init__(
        self,
        scope: constructs.Construct,
        construct_id: str,
        env_base: EnvBase,
        aibs_informatics_aws_lambda_repo: Optional[str] = None,
    ) -> None:
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self.AIBS_INFORMATICS_AWS_LAMBDA_REPO = (
            aibs_informatics_aws_lambda_repo or AIBS_INFORMATICS_AWS_LAMBDA_REPO
        )

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> aws_ecr_assets.DockerImageAsset:
        """Returns a NEW docker asset for aibs-informatics-aws-lambda

        Returns:
            aws_ecr_assets.DockerImageAsset: The docker asset
        """
        repo_path = self.resolve_repo_path(
            self.AIBS_INFORMATICS_AWS_LAMBDA_REPO, AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR
        )

        return aws_ecr_assets.DockerImageAsset(
            self,
            "aibs-informatics-aws-lambda",
            directory=repo_path.as_posix(),
            build_ssh="default",
            platform=aws_ecr_assets.Platform.LINUX_AMD64,
            asset_name="aibs-informatics-aws-lambda",
            file="docker/Dockerfile",
            extra_hash=generate_path_hash(
                path=str(repo_path.resolve()),
                excludes=PYTHON_REGEX_EXCLUDES,
            ),
            exclude=[
                *PYTHON_GLOB_EXCLUDES,
                *GLOBAL_GLOB_EXCLUDES,
            ],
        )


class AIBSInformaticsAssets(constructs.Construct):
    def __init__(
        self,
        scope: constructs.Construct,
        construct_id: str,
        env_base: EnvBase,
        runtime: Optional[lambda_.Runtime] = None,
    ) -> None:
        super().__init__(scope, construct_id)
        self.env_base = env_base

        self.code_assets = AIBSInformaticsCodeAssets(self, "CodeAssets", env_base, runtime=runtime)
        self.docker_assets = AIBSInformaticsDockerAssets(self, "DockerAssets", env_base)
