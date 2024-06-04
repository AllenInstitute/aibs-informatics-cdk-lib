#!/usr/bin/env python3

import aws_cdk as cdk
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.project.config import StageConfig
from aibs_informatics_cdk_lib.project.utils import get_config
from aibs_informatics_cdk_lib.stages.base import ConfigBasedStage
from aibs_informatics_core_app.stacks.assets import AIBSInformaticsAssetsStack
from aibs_informatics_core_app.stacks.demand_execution import (
    DemandExecutionInfrastructureStack,
    DemandExecutionStack,
)
from aibs_informatics_core_app.stacks.network import NetworkStack
from aibs_informatics_core_app.stacks.storage import StorageStack


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

        demand_execution_infra = DemandExecutionInfrastructureStack(
            self,
            self.get_stack_name("DemandExecutionInfra"),
            self.env_base,
            vpc=network.vpc,
            buckets=[storage.bucket],
            mount_point_configs=[
                MountPointConfiguration.from_file_system(storage.file_system, None, "/opt/efs"),
            ],
            env=self.env,
        )

        demand_execution = DemandExecutionStack(
            self,
            self.get_stack_name("DemandExecution"),
            env_base=self.env_base,
            assets=assets.assets,
            scaffolding_bucket=storage.bucket,
            efs_ecosystem=storage.efs_ecosystem,
            data_sync_job_queue=demand_execution_infra.infra_compute.lambda_medium_batch_environment.job_queue_name,
            scaffolding_job_queue=demand_execution_infra.infra_compute.primary_batch_environment.job_queue_name,
            execution_job_queue=demand_execution_infra.execution_compute.primary_batch_environment.job_queue_name,
            env=self.env,
        )


def main():
    app = cdk.App()

    config: StageConfig = get_config(app.node)

    InfraStage(app, config)

    app.synth()


if __name__ == "__main__":
    main()
