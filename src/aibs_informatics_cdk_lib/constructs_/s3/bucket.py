from typing import Literal, Optional, Sequence, Union

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins


class EnvBaseBucket(s3.Bucket, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        bucket_name: Optional[str],
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.RETAIN,
        account_id: str = cdk.Aws.ACCOUNT_ID,
        region: str = cdk.Aws.REGION,
        lifecycle_rules: Optional[Sequence[s3.LifecycleRule]] = None,
        inventories: Optional[Sequence[s3.Inventory]] = None,
        auto_delete_objects: bool = False,
        bucket_key_enabled: bool = False,
        block_public_access: Optional[s3.BlockPublicAccess] = s3.BlockPublicAccess.BLOCK_ALL,
        public_read_access: bool = False,
        **kwargs,
    ):
        self.env_base = env_base
        self._full_bucket_name = bucket_name
        if bucket_name is not None:
            self._full_bucket_name = env_base.get_bucket_name(
                base_name=bucket_name, account_id=account_id, region=region
            )
        super().__init__(
            scope,
            id,
            access_control=s3.BucketAccessControl.PRIVATE,
            auto_delete_objects=auto_delete_objects,
            block_public_access=block_public_access,
            bucket_key_enabled=bucket_key_enabled,
            bucket_name=self.bucket_name,
            public_read_access=public_read_access,
            removal_policy=removal_policy,
            lifecycle_rules=lifecycle_rules,
            inventories=inventories,
            **kwargs,
        )

    @property
    def bucket_name(self) -> str:
        return self._full_bucket_name or super().bucket_name

    def grant_permissions(
        self,
        role: Optional[iam.IRole],
        *permissions: Literal["rw", "r", "w", "d"],
    ):
        grant_bucket_access(self, role, *permissions)


def grant_bucket_access(
    bucket: Union[s3.Bucket, Sequence[s3.Bucket]],
    role: Optional[iam.IRole],
    *permissions: Literal["rw", "r", "w", "d"],
):
    if not role:
        return
    for bucket in [bucket] if isinstance(bucket, s3.Bucket) else bucket:
        for bucket_permission in permissions:
            if bucket_permission == "rw":
                bucket.grant_read_write(role)
            elif bucket_permission == "r":
                bucket.grant_read(role)
            elif bucket_permission == "w":
                bucket.grant_write(role)
            elif bucket_permission == "d":
                bucket.grant_delete(role)
