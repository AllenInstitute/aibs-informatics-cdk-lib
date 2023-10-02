# © 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
#
# This AWS Content is provided subject to the terms of the AWS Customer
# Agreement available at http://aws.amazon.com/agreement or other written
# agreement between Customer and either Amazon Web Services, Inc. or
# Amazon Web Services EMEA SARL or both.

"""
Secure S3 Bucket

CDK Construct implementing common settings for a best practice secure
bucket.
"""
from typing import Optional, Sequence

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_s3 as s3

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins


class SecureS3Bucket(s3.Bucket, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        bucket_name: str,
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
        return self._full_bucket_name
