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


class SecureS3Bucket(constructs.Construct):
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
    ):

        super().__init__(scope, id)
        self.env_base = env_base

        # Construct the bucket name to be like:
        # dev-xxx-<GIVEN NAME>-us-east-1-1234567890
        self._full_bucket_name = self.env_base.get_bucket_name(
            base_name=bucket_name, account_id=account_id, region=region
        )

        self.s3_bucket = s3.Bucket(
            self,
            "-".join([self.env_base, bucket_name]),
            access_control=s3.BucketAccessControl.PRIVATE,
            auto_delete_objects=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=False,
            bucket_name=self.bucket_name,
            public_read_access=False,
            removal_policy=removal_policy,
            lifecycle_rules=lifecycle_rules,
            inventories=inventories,
        )

    @property
    def bucket_name(self) -> str:
        return self._full_bucket_name
