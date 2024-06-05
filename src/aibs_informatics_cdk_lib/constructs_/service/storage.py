from typing import Optional

import aws_cdk as cdk
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ec2 as ec2
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EFSEcosystem, EnvBaseFileSystem
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, LifecycleRuleGenerator


class Storage(EnvBaseConstruct):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        name: str,
        vpc: ec2.Vpc,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.RETAIN,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

        self._bucket = EnvBaseBucket(
            self,
            "Bucket",
            self.env_base,
            bucket_name=name,
            removal_policy=removal_policy,
            lifecycle_rules=[
                LifecycleRuleGenerator.expire_files_under_prefix(),
                LifecycleRuleGenerator.expire_files_with_scratch_tags(),
                LifecycleRuleGenerator.use_storage_class_as_default(),
            ],
        )

        self._efs_ecosystem = EFSEcosystem(
            self, id="EFS", env_base=self.env_base, file_system_name=name, vpc=vpc
        )
        self._file_system = self._efs_ecosystem.file_system

    @property
    def bucket(self) -> EnvBaseBucket:
        return self._bucket

    @property
    def efs_ecosystem(self) -> EFSEcosystem:
        return self._efs_ecosystem

    @property
    def file_system(self) -> EnvBaseFileSystem:
        return self._file_system
