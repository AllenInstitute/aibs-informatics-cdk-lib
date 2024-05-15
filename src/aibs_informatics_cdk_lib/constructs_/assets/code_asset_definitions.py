import logging
import os
from pathlib import Path
from typing import Optional

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.hashing import generate_path_hash
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_assets

from aibs_informatics_cdk_lib.common.git import clone_repo, is_local_repo
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    PYTHON_GLOB_EXCLUDES,
    PYTHON_REGEX_EXCLUDES,
    CodeAsset,
)

AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR = "AIBS_INFORMATICS_AWS_LAMBDA_REPO"
AIBS_INFORMATICS_AWS_LAMBDA_REPO = "git@github.com/AllenInstitute/aibs-informatics-aws-lambda.git"

logger = logging.getLogger(__name__)


class AIBSInformaticsCodeAssets(constructs.Construct):
    def __init__(
        self,
        scope: constructs.Construct,
        construct_id: str,
        env_base: EnvBase,
        runtime: Optional[lambda_.Runtime] = None,
    ) -> None:
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self.runtime = runtime or lambda_.Runtime.PYTHON_3_11

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> CodeAsset:
        """Returns a NEW code asset for aibs-informatics-aws-lambda

        Returns:
            CodeAsset: The code asset
        """

        if AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR in os.environ:
            logger.info(f"Using {AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR} from environment")
            repo_path = os.getenv(AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR)
            if not repo_path or not is_local_repo(repo_path):
                raise ValueError(
                    f"Environment variable {AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR} is not a valid git repo"
                )
            repo_path = Path(repo_path)
        else:
            repo_path = clone_repo(AIBS_INFORMATICS_AWS_LAMBDA_REPO, skip_if_exists=True)

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
                working_directory=f"/asset-input",
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
                            "pip3 install --no-cache -r requirements-lambda.txt -t /asset-output",
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
