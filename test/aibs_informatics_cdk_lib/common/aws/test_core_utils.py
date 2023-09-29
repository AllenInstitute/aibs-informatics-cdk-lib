import aws_cdk as cdk
import constructs
import pytest
from aibs_informatics_test_resources.base import BaseTest

from aibs_informatics_cdk_lib.common.aws.core_utils import build_arn

build_arn_test_cases = [
    pytest.param(
        dict(service="s3"),
        f"arn:aws:s3:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:*",
        id="Default",
    ),
    pytest.param(
        dict(service="s3", resource_type="Fetch", resource_id="Blah"),
        f"arn:aws:s3:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:Fetch:Blah",
        id="Defaults with resource type:id",
    ),
    pytest.param(
        dict(service="s3", resource_type="Fetch", resource_id="Blah", resource_delim="/"),
        f"arn:aws:s3:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:Fetch/Blah",
        id="Defaults with resource type/id",
    ),
]


@pytest.mark.parametrize("build_arn_args,expected_arn", build_arn_test_cases)
def test__build_arn__all_defaults(build_arn_args, expected_arn):
    constructed_arn = build_arn(**build_arn_args)
    assert constructed_arn == expected_arn
