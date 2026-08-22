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
from aibs_informatics_cdk_lib.constructs_.assets.docker_asset import DockerAsset
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


class AssetsMixin:
    _source: PackageSource

    @property
    def source(self) -> PackageSource:
        """The source these assets are built from."""
        return self._source

    @property
    def AIBS_INFORMATICS_AWS_LAMBDA_REPO(self) -> str | None:
        """The git URL of the source repo, or None for a ContainerImageSource.

        .. deprecated::
            Use ``source`` instead.
        """
        warnings.warn(
            "`AIBS_INFORMATICS_AWS_LAMBDA_REPO` is deprecated; use `source` instead. "
            "It returns None when the source is a ContainerImageSource.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._source.url if isinstance(self._source, GitSource) else None

    @classmethod
    def _normalize_source(
        cls, source: PackageSource | str | None, default_repo_url: str
    ) -> PackageSource:
        """Normalize a source parameter into a supported PackageSource instance.

        An unsupported PackageSource subclass is rejected here because downstream callers
        reach for source-kind-specific properties (e.g. GitSource.repo_url_with_ref) and
        would otherwise fail later with an AttributeError.
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
        kwargs: dict[str, PackageSource | str | None],
    ) -> PackageSource | str | None:
        """Collapse the source parameter and its deprecated alias, consuming ``kwargs``.

        The alias lives in ``**kwargs`` rather than in the signature so it stays out of the
        public API and the generated reference. That means this must raise on any other
        keyword: a typo like ``aibs_informatics_aws_lambda_rep=...`` would otherwise be
        swallowed, leaving the construct on its default source while the caller believed
        they had pinned a version.

        ``stacklevel=3`` targets a subclass forwarding the alias through
        ``super().__init__()``, which is how every downstream consumer passes it. A direct
        call lands on jsii's metaclass instead; no single value serves both, and
        ``skip_file_prefixes`` needs Python 3.12.
        """
        deprecated_source = kwargs.pop(DEPRECATED_AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM, None)
        if kwargs:
            unexpected = ", ".join(f"'{name}'" for name in sorted(kwargs))
            raise TypeError(
                f"{cls.__name__}.__init__() got an unexpected keyword argument {unexpected}"
            )
        if deprecated_source is None:
            return source
        if source is not None:
            raise ValueError(
                f"Cannot specify both `{AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM}` and "
                f"`{DEPRECATED_AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM}`. "
                f"Use `{AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM}`."
            )
        warnings.warn(
            f"`{DEPRECATED_AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM}` is deprecated; use "
            f"`{AIBS_INFORMATICS_AWS_LAMBDA_SOURCE_PARAM}` instead. A source may be a git URL, "
            "a local repo path, a container image reference, or a PackageSource instance.",
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
        **kwargs: PackageSource | str | None,
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
            **kwargs: Accepts only the deprecated ``aibs_informatics_aws_lambda_repo`` alias
                for ``aibs_informatics_aws_lambda_source``. Any other keyword is a TypeError.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self.runtime = runtime or lambda_.Runtime.PYTHON_3_11
        self._source = self._normalize_source(
            self._resolve_deprecated_source(aibs_informatics_aws_lambda_source, kwargs),
            AIBS_INFORMATICS_AWS_LAMBDA_REPO,
        )

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
        **kwargs: PackageSource | str | None,
    ) -> None:
        """Docker assets for the aibs-informatics packages.

        Args:
            scope: The parent construct.
            construct_id: The construct id.
            env_base: The environment base.
            aibs_informatics_aws_lambda_source: The source for the aibs-informatics-aws-lambda
                asset. May be a git URL, a local repo path, a container image reference, or a
                PackageSource. Defaults to the public repo.
            **kwargs: Accepts only the deprecated ``aibs_informatics_aws_lambda_repo`` alias
                for ``aibs_informatics_aws_lambda_source``. Any other keyword is a TypeError.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base
        self._source = self._normalize_source(
            self._resolve_deprecated_source(aibs_informatics_aws_lambda_source, kwargs),
            AIBS_INFORMATICS_AWS_LAMBDA_REPO,
        )

    @cached_property
    def AIBS_INFORMATICS_AWS_LAMBDA(self) -> DockerAsset:
        """Returns a docker asset for aibs-informatics-aws-lambda.

        When the source is a GitSource, the image is built locally from the repo. When
        the source is a ContainerImageSource, the published image is used as-is.

        Returns:
            The docker asset, which knows how to present itself to each AWS service.

        Raises:
            TypeError: If the source is neither a ContainerImageSource nor a GitSource.
        """
        if isinstance(self._source, ContainerImageSource):
            return DockerAsset.from_registry(self, "aibs-informatics-aws-lambda", self._source)
        if not isinstance(self._source, GitSource):
            raise TypeError(
                f"AIBSInformaticsDockerAssets requires a GitSource or ContainerImageSource, "
                f"got {type(self._source).__name__}."
            )

        repo_path = self.resolve_repo_path(
            self._source.repo_url_with_ref, AIBS_INFORMATICS_AWS_LAMBDA_REPO_ENV_VAR
        )

        return DockerAsset.from_local_build(
            self,
            "aibs-informatics-aws-lambda",
            directory=repo_path.as_posix(),
            source=self._source,
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
        aibs_informatics_aws_lambda_code_source: GitSource | str | None = None,
        aibs_informatics_aws_lambda_docker_source: PackageSource | str | None = None,
    ) -> None:
        """Code and docker assets for the aibs-informatics packages.

        The code and docker assets take separate sources because they are not
        interchangeable: a Code Asset requires a checkout to build, so its source cannot be
        a Container Image Source. The docker asset accepts either.

        Args:
            scope: The parent construct.
            construct_id: The construct id.
            env_base: The environment base.
            runtime: The lambda runtime to build code assets against.
            aibs_informatics_aws_lambda_code_source: The source for the
                aibs-informatics-aws-lambda code asset. May be a git URL, a local repo path,
                or a GitSource. Defaults to the public repo.
            aibs_informatics_aws_lambda_docker_source: The source for the
                aibs-informatics-aws-lambda docker asset. May additionally be a container
                image reference or a ContainerImageSource. Defaults to the public repo.
        """
        super().__init__(scope, construct_id)
        self.env_base = env_base

        self.code_assets = AIBSInformaticsCodeAssets(
            self,
            "CodeAssets",
            env_base,
            runtime=runtime,
            aibs_informatics_aws_lambda_source=aibs_informatics_aws_lambda_code_source,
        )
        self.docker_assets = AIBSInformaticsDockerAssets(
            self,
            "DockerAssets",
            env_base,
            aibs_informatics_aws_lambda_source=aibs_informatics_aws_lambda_docker_source,
        )
