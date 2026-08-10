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
        id: str | None,
        env_base: EnvBase,
        name: str,
        extra_efs_count: int = 0,
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

        self._efs_ecosystems = [
            EFSEcosystem(
                self, id="EFS", env_base=self.env_base, file_system_name=name, vpc=self.vpc
            )
        ]
        # Extra file systems let concurrent demand executions spread their scratch/working
        # directory I/O across multiple EFS burst-credit pools. Only prod carries the load
        # (and cost) that justifies them, and callers opt in via extra_efs_count.
        if self.is_prod:
            self._efs_ecosystems.extend(
                EFSEcosystem(
                    self,
                    id=f"EFS-{i}",
                    env_base=self.env_base,
                    file_system_name=f"{name}-part{i}",
                    vpc=self.vpc,
                )
                for i in range(1, extra_efs_count + 1)
            )

    @property
    def vpc(self) -> EnvBaseVpc:
        return self._vpc

    @property
    def bucket(self) -> EnvBaseBucket:
        return self._bucket

    @property
    def primary_efs_ecosystem(self) -> EFSEcosystem:
        """The primary EFS ecosystem."""
        return self._efs_ecosystems[0]

    @property
    def efs_ecosystems(self) -> list[EFSEcosystem]:
        return self._efs_ecosystems

    @property
    def primary_file_system(self) -> EnvBaseFileSystem:
        """The primary EFS file system."""
        return self.primary_efs_ecosystem.file_system

    @property
    def file_systems(self) -> list[EnvBaseFileSystem]:
        return [ecosystem.file_system for ecosystem in self._efs_ecosystems]
