import os
from pathlib import Path
from typing import Optional, Tuple, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.aws.s3 import S3URI, S3KeyPrefix
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aws_cdk import aws_s3_deployment as s3deployment
from aws_cdk import aws_ssm as ssm

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket


class SSMTools(EnvBaseConstruct):
    def __init__(self, scope: constructs.Construct, id: str, env_base: EnvBase):
        super().__init__(scope, id, env_base)
        self.CODE_ASSETS_PREFIX = "code_assets"
        self.FILE_ASSETS_PREFIX = "file_assets"

    def upload_asset(
        self,
        asset_name: str,
        asset_source: s3deployment.ISource,
        destination_bucket: EnvBaseBucket,
        destination_key_prefix: Optional[S3KeyPrefix] = None,
        param_name: Optional[str] = None,
    ) -> Tuple[ssm.StringParameter, str]:
        param_name_components = [self.CODE_ASSETS_PREFIX, asset_name]
        if destination_key_prefix is None:
            destination_key_prefix = S3KeyPrefix(self.CODE_ASSETS_PREFIX)

        if param_name:
            parameter_name = self.env_base.get_ssm_param_name(param_name)
        else:
            parameter_name = self.env_base.get_ssm_param_name(*param_name_components)

        filename = f"{asset_name}-asset.zip"

        s3deployment.BucketDeployment(
            self,
            "-".join([*param_name_components, "s3-deployment"]),
            sources=[asset_source],
            destination_bucket=destination_bucket,
            destination_key_prefix=destination_key_prefix + "/",
            retain_on_delete=False,
        )

        param = ssm.StringParameter(
            self,
            "-".join([*param_name_components, "ssm-parameter"]),
            # Store the S3 URI location in an SSM parameter
            string_value=S3URI.build(
                bucket_name=destination_bucket.bucket_name,
                key=f"{destination_key_prefix}/{filename}",
                full_validate=False,
            ),
            description=f"Location of asset {asset_name}",
            parameter_name=parameter_name,
        )
        return param, parameter_name

    def upload_file(
        self,
        path: Union[str, Path],
        destination_bucket: EnvBaseBucket,
        destination_key_prefix: str,
    ) -> Tuple[ssm.StringParameter, str]:
        if not os.path.isfile(path):
            raise ValueError(
                f"Cannot upload file and create SSM reference for {path}. Must be a valid file"
            )
        filename = os.path.basename(path)
        name = os.path.splitext(filename)[0]
        destination_key_prefix = os.path.join(self.FILE_ASSETS_PREFIX, destination_key_prefix)
        destination_key = os.path.join(destination_key_prefix, filename)
        param_name_components = [
            *destination_key_prefix.split(os.path.sep),
            name,
        ]

        s3deployment.BucketDeployment(
            self,
            "-".join([name, "s3-deployment"]),
            sources=[s3deployment.Source.asset(os.path.dirname(path))],
            destination_bucket=destination_bucket,
            destination_key_prefix=destination_key_prefix,
        )

        # Store the S3 URI location in an SSM parameter
        s3_uri = S3URI.build(destination_bucket.bucket_name, destination_key, full_validate=False)

        parameter_name = self.env_base.get_ssm_param_name(*param_name_components)
        cmd_wrapper_param = ssm.StringParameter(
            self,
            "-".join([name, "ssm-parameter"]),
            string_value=s3_uri,
            description=f"Location of {filename}",
            parameter_name=parameter_name,
        )
        return cmd_wrapper_param, parameter_name
