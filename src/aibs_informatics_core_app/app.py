#!/usr/bin/env python3
import logging
from pathlib import Path
from typing import List, Optional

import aws_cdk as cdk
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.project.config import StageConfig
from aibs_informatics_cdk_lib.project.utils import get_config
from aibs_informatics_cdk_lib.stacks.assets import AIBSInformaticsAssetsStack
from aibs_informatics_cdk_lib.stacks.compute import (
    ComputeStack,
    ComputeWorkflowStack,
    LambdaComputeStack,
)
from aibs_informatics_cdk_lib.stacks.network import NetworkStack
from aibs_informatics_cdk_lib.stacks.storage import StorageStack
from aibs_informatics_cdk_lib.stages.base import ConfigBasedStage


class InfraStage(ConfigBasedStage):
    def __init__(self, scope: Construct, config: StageConfig, **kwargs) -> None:
        super().__init__(scope, "Infra", config, **kwargs)
        assets = AIBSInformaticsAssetsStack(
            self,
            self.get_stack_name("Assets"),
            self.env_base,
            env=self.env,
        )
        network = NetworkStack(
            self,
            self.get_stack_name("Network"),
            self.env_base,
            env=self.env,
        )
        storage = StorageStack(
            self,
            self.get_stack_name("Storage"),
            self.env_base,
            "core",
            vpc=network.vpc,
            env=self.env,
        )

        efs_ecosystem = storage.efs_ecosystem

        ap_mount_point_configs = [
            MountPointConfiguration.from_access_point(
                efs_ecosystem.scratch_access_point, "/opt/scratch"
            ),
            MountPointConfiguration.from_access_point(efs_ecosystem.tmp_access_point, "/opt/tmp"),
            MountPointConfiguration.from_access_point(
                efs_ecosystem.shared_access_point, "/opt/shared", read_only=True
            ),
        ]
        fs_mount_point_configs = [
            MountPointConfiguration.from_file_system(storage.file_system, None, "/opt/efs"),
        ]

        compute = ComputeStack(
            self,
            self.get_stack_name("Compute"),
            self.env_base,
            batch_name="batch",
            vpc=network.vpc,
            buckets=[storage.bucket],
            file_systems=[storage.file_system],
            mount_point_configs=fs_mount_point_configs,
            env=self.env,
        )

        lambda_compute = LambdaComputeStack(
            self,
            self.get_stack_name("LambdaCompute"),
            self.env_base,
            batch_name="lambda-batch",
            vpc=network.vpc,
            buckets=[storage.bucket],
            file_systems=[storage.file_system],
            mount_point_configs=fs_mount_point_configs,
            env=self.env,
        )

        compute_workflow = ComputeWorkflowStack(
            self,
            self.get_stack_name("ComputeWorkflow"),
            env_base=self.env_base,
            batch_environment=lambda_compute.primary_batch_environment,
            primary_bucket=storage.bucket,
            mount_point_configs=fs_mount_point_configs,
            env=self.env,
        )


def main():
    app = cdk.App()

    config = get_config(app.node)

    InfraStage(app, config)

    app.synth()


if __name__ == "__main__":
    main()
