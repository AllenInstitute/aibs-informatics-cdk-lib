import logging
import os
import warnings
from pathlib import Path

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.hashing import generate_path_hash
from aws_cdk import aws_ecr_assets, aws_s3_assets
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.common.git import clone_repo, is_local_repo, is_repo_url
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    GLOBAL_GLOB_EXCLUDES,
    PYTHON_GLOB_EXCLUDES,
    PYTHON_REGEX_EXCLUDES,
    CodeAsset,
)
from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    GitSource,
    PackageSource,
)

AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR = "AIBS_INFORMATICS_AWS_LAMBDA_REPO"
AIBS_INFORMATICS_AWS_LAMBDA_REPO = "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git"

AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM = "aibs_informatics_aws_lambda_source"
DEPRECATED_AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM = "aibs_informatics_aws_lambda_repo"

logger = logging.getLogger(__name__)


def _deprecated_repo_url(source: PackageSource) -> str | None:
    """Back-compat shim for the ``AIBS_INFORMATICS_AWS_LAMBDA_REPO`` attribute.

    Args:
        source: The source the construct was configured with.

    Returns:
        The repo URL for a GitSource, or None for a ContainerImageSource, which has no
        repo URL to report.
    """
    warnings.warn(
        "`AIBS_INFORMATICS_AWS_LAMBDA_REPO` is deprecated; use `source` instead. "
        "It returns None when the source is a ContainerImageSource.",
        DeprecationWarning,
        stacklevel=3,
    )
    return source.url if isinstance(source, GitSource) else None


class AssetsMixin:
    _source: PackageSource

    @property
    def source(self) -> PackageSource:
        """The source these assets are built from."""
        return self._source

    @classmethod
    def _normalize_source(
        cls, source: PackageSource | str | None, default_repo_url: str
    ) -> PackageSource:
        """Normalize a source parameter into a supported PackageSource instance.

        Args:
            source: A GitSource or ContainerImageSource, a string (git URL, local path, or
                image ref), or None.
            default_repo_url: Default git repo URL to use when source is None.

        Returns:
            A resolved PackageSource instance.

        Raises:
            TypeError: If source is a PackageSource subclass this code path does not
                support. Callers downstream reach for source-kind-specific properties
                (e.g. GitSource.repo_url_with_ref), so an unrecognized subclass is
                rejected here rather than failing later with an AttributeError.
        """
        if source is None:
            return GitSource(url=default_repo_url)
        if isinstance(source, str):
            return PackageSource.from_str(source)
        if isinstance(source, (GitSource, ContainerImageSource)):
            return source
        raise TypeError(
            f"Unsupported package source type: {type(source).__name__}. "
            "Expected GitSource or ContainerImageSource."
        )

    @classmethod
    def _resolve_deprecated_source(
        cls,
        source: PackageSource | str | None,
        deprecated_source: PackageSource | str | None,
        param_name: str = AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM,
        deprecated_param_name: str = DEPRECATED_AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM,
    ) -> PackageSource | str | None:
        """Collapse a source parameter and its deprecated alias into one value.

        Args:
            source: The value passed under the current parameter name.
            deprecated_source: The value passed under the deprecated parameter name.
            param_name: The current parameter name, for messages.
            deprecated_param_name: The deprecated parameter name, for messages.

        Returns:
            Whichever of the two was supplied, or None if neither was.

        Raises:
            ValueError: If both parameters were supplied.
        """
        if deprecated_source is None:
            return source
        if source is not None:
            raise ValueError(
                f"Cannot specify both `{param_name}` and `{deprecated_param_name}`. "
                f"Use `{param_name}`."
            )
        warnings.warn(
            f"`{deprecated_param_name}` is deprecated; use `{param_name}` instead. "
            "A source may be a git URL, a local repo path, a container image reference, "
            "or a PackageSource instance.",
            DeprecationWarning,
            stacklevel=3,
        )
        return deprecated_source

    @classmethod
    def resolve_repo_path(cls, repo_url: str, repo_path_env_var: str | None) -> Path:
        """Resolves the repo path from the environment or clones the repo from the url

        This method is useful to quickly swapping between locally modified changes and the remote
        repo.

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
            The path to the repo
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
        runtime: lambda_.Runtime | None = None,
        aibs_informatics_aws_lambda_source: PackageSource | str | None = None,
        aibs_informatics_aws_lambda_repo: PackageSource | str | None = None,
    ) -> None:
        """Code assets for the aibs-informatics packages.

        Args:
            scope: The parent construct.
            construct_id: The construct id.
            env_base: The environment base.
            runtime: The lambda runtime to build against. Defaults to Python 3.11.
            aibs_informatics_aws_lambda_source: The source for the aibs-informatics-aws-lambda
                asset. May be a git URL, a local repo path, or a PackageSource. Defaults to
                the public repo.
            aibs_informatics_aws_lambda_repo: Deprecated alias for
                ``aibs_informatics_aws_lambda_source``.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self.runtime = runtime or lambda_.Runtime.PYTHON_3_11
        self._source = self._normalize_source(
            self._resolve_deprecated_source(
                aibs_informatics_aws_lambda_source, aibs_informatics_aws_lambda_repo
            ),
            AIBS_INFORMATICS_AWS_LAMBDA_REPO,
        )

    @property
    def AIBS_INFORMATICS_AWS_LAMBDA_REPO(self) -> str | None:
        """The git URL of the aibs-informatics-aws-lambda source repo.

        .. deprecated::
            Use ``source`` instead.

        Returns:
            The repo URL for a GitSource, or None for a ContainerImageSource.
        """
        return _deprecated_repo_url(self._source)

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> CodeAsset:
        """Returns a NEW code asset for aibs-informatics-aws-lambda

        Returns:
            The code asset

        Raises:
            TypeError: If the source is a ContainerImageSource (code assets require a git repo).
        """
        if isinstance(self._source, ContainerImageSource):
            raise TypeError(
                "AIBSInformaticsCodeAssets requires a GitSource. "
                "ContainerImageSource cannot be used for Lambda code assets."
            )
        if not isinstance(self._source, GitSource):
            raise TypeError(
                f"AIBSInformaticsCodeAssets requires a GitSource, got "
                f"{type(self._source).__name__}."
            )

        repo_path = self.resolve_repo_path(
            self._source.repo_url_with_ref, AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR
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
                    # WARNING Make sure not to modify {host_ssh_dir} in any way, in this set of commands!  # noqa: E501
                    " && ".join(
                        [
                            "set -x",
                            # Copy in host ssh keys that are needed to clone private git repos
                            f"cp -r {host_ssh_dir} /root/.ssh",
                            # Useful debug if anything goes wrong with github SSH related things
                            "ssh -vT git@github.com || true",
                            # Must make sure that the package is not installing using --editable mode  # noqa: E501
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
        aibs_informatics_aws_lambda_source: PackageSource | str | None = None,
        aibs_informatics_aws_lambda_repo: PackageSource | str | None = None,
    ) -> None:
        """Docker assets for the aibs-informatics packages.

        Args:
            scope: The parent construct.
            construct_id: The construct id.
            env_base: The environment base.
            aibs_informatics_aws_lambda_source: The source for the aibs-informatics-aws-lambda
                asset. May be a git URL, a local repo path, a container image reference, or a
                PackageSource. Defaults to the public repo.
            aibs_informatics_aws_lambda_repo: Deprecated alias for
                ``aibs_informatics_aws_lambda_source``.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self._source = self._normalize_source(
            self._resolve_deprecated_source(
                aibs_informatics_aws_lambda_source, aibs_informatics_aws_lambda_repo
            ),
            AIBS_INFORMATICS_AWS_LAMBDA_REPO,
        )

    @property
    def AIBS_INFORMATICS_AWS_LAMBDA_REPO(self) -> str | None:
        """The git URL of the aibs-informatics-aws-lambda source repo.

        .. deprecated::
            Use ``source`` instead.

        Returns:
            The repo URL for a GitSource, or None for a ContainerImageSource.
        """
        return _deprecated_repo_url(self._source)

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> aws_ecr_assets.DockerImageAsset | str:
        """Returns a docker asset for aibs-informatics-aws-lambda.

        When the source is a GitSource, returns a DockerImageAsset built from the repo.
        When the source is a ContainerImageSource, returns the image URI string.

        Returns:
            The docker image asset or image URI string.

        Raises:
            TypeError: If the source is neither a ContainerImageSource nor a GitSource.
        """
        if isinstance(self._source, ContainerImageSource):
            return self._source.image_uri
        if not isinstance(self._source, GitSource):
            raise TypeError(
                f"AIBSInformaticsDockerAssets requires a GitSource or ContainerImageSource, "
                f"got {type(self._source).__name__}."
            )

        repo_path = self.resolve_repo_path(
            self._source.repo_url_with_ref, AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR
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
        runtime: lambda_.Runtime | None = None,
        code_source: GitSource | str | None = None,
        docker_source: PackageSource | str | None = None,
        aibs_informatics_aws_lambda_repo: PackageSource | str | None = None,
    ) -> None:
        """Code and docker assets for the aibs-informatics packages.

        Code and docker assets take separate sources because they are not interchangeable:
        a Code Asset requires a checkout to build, so ``code_source`` cannot be a Container
        Image Source. ``docker_source`` accepts either.

        Args:
            scope: The parent construct.
            construct_id: The construct id.
            env_base: The environment base.
            runtime: The lambda runtime to build code assets against.
            code_source: The source for the aibs-informatics-aws-lambda code asset. May be a
                git URL, a local repo path, or a GitSource. Defaults to the public repo.
            docker_source: The source for the aibs-informatics-aws-lambda docker asset. May
                additionally be a container image reference or a ContainerImageSource.
                Defaults to the public repo.
            aibs_informatics_aws_lambda_repo: Deprecated. Sets both ``code_source`` and
                ``docker_source``.

        Raises:
            ValueError: If the deprecated parameter is combined with either new parameter.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base

        resolved_code_source: PackageSource | str | None = code_source
        resolved_docker_source: PackageSource | str | None = docker_source
        if aibs_informatics_aws_lambda_repo is not None:
            if code_source is not None or docker_source is not None:
                raise ValueError(
                    "Cannot specify both `aibs_informatics_aws_lambda_repo` and "
                    "`code_source`/`docker_source`. Use the latter."
                )
            warnings.warn(
                "`aibs_informatics_aws_lambda_repo` is deprecated; use `code_source` and "
                "`docker_source` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            resolved_code_source = aibs_informatics_aws_lambda_repo
            resolved_docker_source = aibs_informatics_aws_lambda_repo

        self.code_assets = AIBSInformaticsCodeAssets(
            self,
            "CodeAssets",
            env_base,
            runtime=runtime,
            aibs_informatics_aws_lambda_source=resolved_code_source,
        )
        self.docker_assets = AIBSInformaticsDockerAssets(
            self,
            "DockerAssets",
            env_base,
            aibs_informatics_aws_lambda_source=resolved_docker_source,
        )
