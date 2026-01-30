"""Core AWS utility functions for building ARNs.

This module provides functions for constructing AWS ARNs for various services.
"""

from typing import Literal, Optional, cast

import aws_cdk as cdk


def build_arn(
    partition: str = "aws",
    service: Optional[str] = None,
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_delim: Literal["/", ":"] = ":",
) -> str:
    """Build an AWS ARN string.

    Args:
        partition (str): AWS partition. Defaults to "aws".
        service (Optional[str]): AWS service name. Defaults to "*".
        region (Optional[str]): AWS region. Defaults to current region.
        account (Optional[str]): AWS account ID. Defaults to current account.
        resource_id (Optional[str]): Resource identifier. Defaults to "*".
        resource_type (Optional[str]): Resource type prefix.
        resource_delim (Literal["/", ":"]): Delimiter between type and ID.

    Returns:
        The constructed ARN string.
    """
    service = service or "*"
    region = region if region is not None else cast(str, cdk.Aws.REGION)
    account = account if account is not None else cast(str, cdk.Aws.ACCOUNT_ID)
    resource_id = resource_id or "*"

    root_arn = f"arn:{partition}:{service}:{region}:{account}"
    if resource_type is not None:
        return f"{root_arn}:{resource_type}{resource_delim}{resource_id}"
    else:
        return f"{root_arn}:{resource_id}"


def build_batch_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[
        Literal["compute-environment", "job", "job-definition", "job-queue"]
    ] = None,
) -> str:
    """Build an AWS Batch ARN.

    Args:
        region (Optional[str]): AWS region.
        account (Optional[str]): AWS account ID.
        resource_id (Optional[str]): Resource identifier.
        resource_type (Optional[Literal[...]]): Batch resource type.

    Returns:
        The constructed Batch ARN string.
    """
    return build_arn(
        service="batch",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim="/",
    )


def build_dynamodb_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[Literal["table"]] = None,
) -> str:
    """Build an AWS DynamoDB ARN.

    Args:
        region (Optional[str]): AWS region.
        account (Optional[str]): AWS account ID.
        resource_id (Optional[str]): Table name or resource identifier.
        resource_type (Optional[Literal["table"]]): DynamoDB resource type.

    Returns:
        The constructed DynamoDB ARN string.
    """
    return build_arn(
        service="dynamodb",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim="/",
    )


def build_ecr_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[Literal["repository"]] = None,
) -> str:
    """Build an AWS ECR ARN.

    Args:
        region (Optional[str]): AWS region.
        account (Optional[str]): AWS account ID.
        resource_id (Optional[str]): Repository name or resource identifier.
        resource_type (Optional[Literal["repository"]]): ECR resource type.

    Returns:
        The constructed ECR ARN string.
    """
    return build_arn(
        service="ecr",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim="/",
    )


def build_sfn_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[Literal["*", "activity", "execution", "stateMachine"]] = None,
) -> str:
    """Build an AWS Step Functions ARN.

    Args:
        region (Optional[str]): AWS region.
        account (Optional[str]): AWS account ID.
        resource_id (Optional[str]): State machine name or resource identifier.
        resource_type (Optional[Literal[...]]): Step Functions resource type.

    Returns:
        The constructed Step Functions ARN string.
    """
    return build_arn(
        service="states",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim=":",
    )


def build_lambda_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[Literal["function", "event-source-mapping", "layer"]] = None,
) -> str:
    """Build an AWS Lambda ARN.

    Args:
        region (Optional[str]): AWS region.
        account (Optional[str]): AWS account ID.
        resource_id (Optional[str]): Function name or resource identifier.
        resource_type (Optional[Literal[...]]): Lambda resource type.

    Returns:
        The constructed Lambda ARN string.
    """
    return build_arn(
        service="lambda",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim=":",
    )


def build_s3_arn(
    region: Optional[str] = None,
    account: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_type: Optional[Literal["bucket", "object", "accesspoint", "job"]] = None,
) -> str:
    """Build an AWS S3 ARN.

    Note:
        S3 bucket and object ARNs do not include region or account.

    Args:
        region (Optional[str]): AWS region (ignored for bucket/object).
        account (Optional[str]): AWS account ID (ignored for bucket/object).
        resource_id (Optional[str]): Bucket name or object path.
        resource_type (Optional[Literal[...]]): S3 resource type.

    Returns:
        The constructed S3 ARN string.
    """
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/list_amazons3.html#amazons3-resources-for-iam-policies
    # See table above to see why resource type is set to None
    if resource_type in ["bucket", "object"]:
        resource_type = None
        # ARNs for buckets and objects CANNOT have REGION information
        region = ""
        account = ""

    return build_arn(
        service="s3",
        region=region,
        account=account,
        resource_id=resource_id,
        resource_type=resource_type,
        resource_delim=":",
    )
