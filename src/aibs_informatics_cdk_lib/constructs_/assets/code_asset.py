import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence, Tuple

import aws_cdk as cdk
import constructs
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.hashing import generate_path_hash
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_assets, aws_s3_deployment

logger = logging.getLogger(__name__)


PYTHON_GLOB_EXCLUDES = [
    f"**/.git/**",
    f"**/*.{{egg,egg-info,pyc,pyo}}",
    f"**/{{.egg,.egg-info,.eggs}}/**",
    f"**/{{.venv,__pycache__,build,dist}}/**",
]

GLOBAL_GLOB_EXCLUDES = [
    "**/.git/**",
    "**/*.egg",
    "**/.egg-info",
    "**/.pyc",
    "**/.pyo",
    "**/.egg/**",
    "**/.egg-info/**",
    "**/.eggs/**",
    "**/.venv/**",
    "**/__pycache__/**",
    "**/build/**",
    "**/dist/**",
]


PYTHON_REGEX_EXCLUDES = [
    r".*(.git)/.*",
    r".*/(.eggs|.venv|__pycache__|build|dist)/.*",
    r".*.(.egg|.egg-info)/.*",
    r".*.(egg|egg-info|pyc|pyo)/.*",
    r".*/test/.*",
]


@dataclass
class CodeAsset:
    asset_name: str
    asset_props: aws_s3_assets.AssetProps
    default_runtime: lambda_.Runtime
    supported_runtimes: Optional[Sequence[lambda_.Runtime]] = None
    environment: Optional[Mapping[str, str]] = None

    def __post_init__(self):
        if not self.supported_runtimes:
            self.supported_runtimes = [self.default_runtime]

        if self.default_runtime.name not in [_.name for _ in self.supported_runtimes]:
            raise ValueError(
                f"default runtime {self.default_runtime.name} not in list of"
                f"supported runtimes {[_.name for _ in self.supported_runtimes]}"
            )

    @cached_property
    def as_source(self) -> aws_s3_deployment.Source:
        return self.get_source()

    @property
    def as_code(self) -> lambda_.AssetCode:
        return self.get_code()

    def get_environment(self, *overrides: Tuple[str, str]) -> Mapping[str, str]:
        environment = {**(self.environment or {})}
        environment.update(overrides)
        return environment

    def get_asset(self, scope: constructs.Construct, id: str) -> aws_s3_assets.Asset:
        return aws_s3_assets.Asset(
            scope,
            id,
            path=self.asset_props.path,
            readers=self.asset_props.readers,
            asset_hash=self.asset_props.asset_hash,
            asset_hash_type=self.asset_props.asset_hash_type,
            bundling=self.asset_props.bundling,
            exclude=self.asset_props.exclude,
            follow_symlinks=self.asset_props.follow_symlinks,
            ignore_mode=self.asset_props.ignore_mode,
        )

    def get_source(self) -> aws_s3_deployment.Source:
        return aws_s3_deployment.Source.asset(
            path=self.asset_props.path,
            readers=self.asset_props.readers,
            asset_hash=self.asset_props.asset_hash,
            asset_hash_type=self.asset_props.asset_hash_type,
            bundling=self.asset_props.bundling,
            exclude=self.asset_props.exclude,
            follow_symlinks=self.asset_props.follow_symlinks,
            ignore_mode=self.asset_props.ignore_mode,
        )  # type: ignore

    def get_source_zip(self, archive_filename: str) -> aws_s3_deployment.Source:
        if not self.asset_props.bundling:
            raise ValueError(
                f"Cannot create nesed zip of source {self.asset_name} with "
                f"asset props = {self.asset_props}, because no bundlings options set!"
            )
        elif not self.asset_props.bundling.command:
            raise ValueError(
                f"Cannot create nesed zip of source {self.asset_name} with "
                f"asset props = {self.asset_props}, because no bundling command set!"
            )
        bundling_command = self.asset_props.bundling.command
        bundling_command[-1] = " && ".join(
            [
                bundling_command[-1],
                "cd /asset-output",
                # Zips everything in the directory, excluding the archive file
                f"zip -r {archive_filename} . -x {archive_filename} -q",
                # deletes everything in the directory, except the archive file
                f"find . ! \\( -name '{archive_filename}' -o -name '.' -o -name '..' \\) -prune -exec rm -rf {{}} +",
            ]
        )
        return aws_s3_deployment.Source.asset(
            path=self.asset_props.path,
            readers=self.asset_props.readers,
            asset_hash=self.asset_props.asset_hash,
            asset_hash_type=self.asset_props.asset_hash_type,
            bundling=cdk.BundlingOptions(
                image=self.asset_props.bundling.image,
                command=bundling_command,
                entrypoint=self.asset_props.bundling.entrypoint,
                environment=self.asset_props.bundling.environment,
                local=self.asset_props.bundling.local,
                output_type=cdk.BundlingOutput.NOT_ARCHIVED,
                user=self.asset_props.bundling.user,
                security_opt=self.asset_props.bundling.security_opt,
                volumes=self.asset_props.bundling.volumes,
                working_directory=self.asset_props.bundling.working_directory,
            ),
            follow_symlinks=self.asset_props.follow_symlinks,
            exclude=self.asset_props.exclude,
            ignore_mode=self.asset_props.ignore_mode,
        )  # type: ignore

    def get_code(self) -> lambda_.AssetCode:
        return lambda_.Code.from_asset(
            path=self.asset_props.path,
            readers=self.asset_props.readers,
            asset_hash=self.asset_props.asset_hash,
            asset_hash_type=self.asset_props.asset_hash_type,
            bundling=self.asset_props.bundling,
            exclude=self.asset_props.exclude,
            follow_symlinks=self.asset_props.follow_symlinks,
            ignore_mode=self.asset_props.ignore_mode,
        )

    @classmethod
    def create_py_code_asset(
        cls,
        path: Path,
        context_path: Optional[Path],
        requirements_file: Optional[Path] = None,
        includes: Optional[Sequence[str]] = None,
        excludes: Optional[Sequence[str]] = None,
        runtime: lambda_.Runtime = lambda_.Runtime.PYTHON_3_11,
        platform: Optional[str] = "linux/amd64",
        environment: Optional[Mapping[str, str]] = None,
    ) -> "CodeAsset":
        """Returns a NEW code asset

        Returns:
            CodeAsset: The code asset
        """

        if context_path is None:
            context_path = path

        # This is relative to the repo root copied in asset props below
        package_path = path.relative_to(context_path)
        full_path = context_path / package_path

        asset_hash = generate_path_hash(
            path=str(context_path.resolve()),
            includes=list(includes) if includes else None,
            excludes=[*(excludes or []), *PYTHON_REGEX_EXCLUDES],
        )
        host_ssh_dir = str(Path.home() / ".ssh")
        asset_props = aws_s3_assets.AssetProps(
            # CDK bundles lambda assets in a docker container. This causes issues for our local
            # path dependencies. In order to resolve the relative local path dependency,
            # we need to specify the path to the root of the repo.
            path=str(context_path.resolve()),
            asset_hash=asset_hash,
            # It is important to exclude files from the git repo, because
            #   1. it effectively makes our caching for assets moot
            #   2. we also don't want to include certain files for size reasons.
            exclude=[
                *PYTHON_GLOB_EXCLUDES,
                "**/cdk.out/",
            ],
            # ignore_mode=cdk.IgnoreMode.GIT,
            bundling=cdk.BundlingOptions(
                image=runtime.bundling_image,
                working_directory=f"/asset-input/{package_path}",
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
                            f"pip3 install {'-r ' + requirements_file.as_posix() if requirements_file else '.'} --no-cache -t /asset-output",
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
                platform=platform,
            ),
        )

        return CodeAsset(
            asset_name=os.path.basename(full_path.resolve()),
            asset_props=asset_props,
            default_runtime=runtime,
            environment=environment,
        )
