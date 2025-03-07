from typing import List, Optional

import aws_cdk as cdk
from aibs_informatics_core.env import EnvBase
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.ec2.network import EnvBaseVpc
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EFSEcosystem, EnvBaseFileSystem
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, LifecycleRuleGenerator
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class CoreStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)
        self._vpc = EnvBaseVpc(self, "Vpc", self.env_base, max_azs=4)

        self._bucket = EnvBaseBucket(
            self,
            "Bucket",
            self.env_base,
            bucket_name=name,
            removal_policy=self.removal_policy,
            lifecycle_rules=[
                LifecycleRuleGenerator.expire_files_under_prefix(),
                LifecycleRuleGenerator.expire_files_with_scratch_tags(),
                LifecycleRuleGenerator.use_storage_class_as_default(),
            ],
        )

        self._efs_ecosystems = []

        self._efs_ecosystems.append(
            EFSEcosystem(
                self, id="EFS", env_base=self.env_base, file_system_name=name, vpc=self.vpc
            )
        )

        for i in range(1, 5):
            self._efs_ecosystems.append(
                EFSEcosystem(
                    self,
                    id=f"EFS-{i}",
                    env_base=self.env_base,
                    file_system_name=f"{name}-part{i}",
                    vpc=self.vpc,
                )
            )
        
        self.create_tmp_cfn_outputs()
    
    def create_tmp_cfn_outputs(self):
        cdk.CfnOutput(
            self,
            "ExportsOutputFnGetAttEFSfsEfsSecurityGroup5507AE02GroupId85196D34",
            value="sg-024523e4c7d66b1b3",
            export_name="Infra-dev-Core:ExportsOutputFnGetAttEFSfsEfsSecurityGroup5507AE02GroupId85196D34",
        )
        cdk.CfnOutput(
            self,
            "ExportsOutputRefEFSfs83D2FD07F890E1CF",
            value="fs-0154ef4f25db18c9b",
            export_name="Infra-dev-Core:ExportsOutputRefEFSfs83D2FD07F890E1CF",
        )
        cdk.CfnOutput(
            self,
            "ExportsOutputFnGetAttdevscratchcfnapAccessPointIdD80AF13C",
            value="fsap-05a37b5674def5c27",
            export_name="Infra-dev-Core:ExportsOutputFnGetAttdevscratchcfnapAccessPointIdD80AF13C",   
        )
        cdk.CfnOutput(
            self,
            "ExportsOutputFnGetAttdevrootcfnapAccessPointIdE493928B",
            value="fsap-036fe99dd5de7f7fe",
            export_name="Infra-dev-Core:ExportsOutputFnGetAttdevrootcfnapAccessPointIdE493928B",
        )
        cdk.CfnOutput(
            self,
            "ExportsOutputFnGetAttdevsharedcfnapAccessPointIdF74082A0",
            value="fsap-020a0538b9edce70a",
            export_name="Infra-dev-Core:ExportsOutputFnGetAttdevsharedcfnapAccessPointIdF74082A0",
        )

    @property
    def vpc(self) -> EnvBaseVpc:
        return self._vpc

    @property
    def bucket(self) -> EnvBaseBucket:
        return self._bucket

    @property
    def efs_ecosystems(self) -> List[EFSEcosystem]:
        return self._efs_ecosystems

    @property
    def file_systems(self) -> List[EnvBaseFileSystem]:
        return [efs_ecosystem.file_system for efs_ecosystem in self.efs_ecosystems]
