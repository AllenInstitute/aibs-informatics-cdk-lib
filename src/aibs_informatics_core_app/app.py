#!/usr/bin/env python3
import logging
from pathlib import Path
from typing import List, Optional

import aws_cdk as cdk
from constructs import Construct

from aibs_informatics_cdk_lib.project.config import StageConfig
from aibs_informatics_cdk_lib.project.utils import get_config, get_package_root
from aibs_informatics_cdk_lib.stacks import data_sync
from aibs_informatics_cdk_lib.stacks.core import ComputeStack, NetworkStack, StorageStack
from aibs_informatics_cdk_lib.stacks.data_sync import DataSyncStack
from aibs_informatics_cdk_lib.stages.base import ConfigBasedStage


class InfraStage(ConfigBasedStage):
    def __init__(self, scope: Construct, config: StageConfig, **kwargs) -> None:
        super().__init__(scope, "Infra", config, **kwargs)
        network = NetworkStack(self, "Network", self.env_base, env=self.env)
        storage = StorageStack(
            self, "Storage", self.env_base, "core", vpc=network.vpc, env=self.env
        )
        compute = ComputeStack(
            self,
            "Compute",
            self.env_base,
            vpc=network.vpc,
            buckets=[storage.bucket],
            file_system=[storage.file_system],
            env=self.env,
        )
        data_sync = DataSyncStack(
            self,
            "DataSync",
            self.env_base,
            asset_directory=Path(get_package_root()) / "aibs-informatics-aws-lambda",
            vpc=network.vpc,
            primary_bucket=storage.bucket,
            s3_buckets=[],
            file_system=storage.file_system,
            batch_job_queue=compute.fargate_batch_environment.job_queue,
            env=self.env,
        )


def main():
    app = cdk.App()

    config = get_config(app.node)

    InfraStage(app, config)

    app.synth()


if __name__ == "__main__":
    main()
