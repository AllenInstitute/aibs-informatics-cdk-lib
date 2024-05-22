import json
from email.policy import default
from typing import Any, Iterable, List, Optional, TypeVar, Union
from urllib import request

import aws_cdk as cdk
from aibs_informatics_aws_utils.batch import to_mount_point, to_volume
from aibs_informatics_aws_utils.constants.efs import (
    EFS_ROOT_ACCESS_POINT_TAG,
    EFS_ROOT_PATH,
    EFS_SCRATCH_ACCESS_POINT_TAG,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_TAG,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_TAG,
    EFS_TMP_PATH,
)
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from aibs_informatics_cdk_lib.common.aws.iam_utils import batch_policy_statement
from aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions import (
    AIBSInformaticsCodeAssets,
)
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import Batch, BatchEnvironmentConfig
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import (
    ON_DEMAND_INSTANCE_TYPES,
    SPOT_INSTANCE_TYPES,
)
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import BatchLaunchTemplateBuilder
from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.ec2 import EnvBaseVpc
from aibs_informatics_cdk_lib.constructs_.efs.file_system import (
    EFSEcosystem,
    EnvBaseFileSystem,
    MountPointConfiguration,
)
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket, LifecycleRuleGenerator
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import (
    SubmitJobFragment,
    SubmitJobWithDefaultsFragment,
)
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class StorageStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        name: str,
        vpc: ec2.Vpc,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

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
