from typing import Optional

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

        self._efs_ecosystem = EFSEcosystem(
            self, id="EFS", env_base=self.env_base, file_system_name=name, vpc=self.vpc
        )
        self._file_system = self._efs_ecosystem.file_system

    @property
    def vpc(self) -> EnvBaseVpc:
        return self._vpc

    @property
    def bucket(self) -> EnvBaseBucket:
        return self._bucket

    @property
    def efs_ecosystem(self) -> EFSEcosystem:
        return self._efs_ecosystem

    @property
    def file_system(self) -> EnvBaseFileSystem:
        return self._file_system
