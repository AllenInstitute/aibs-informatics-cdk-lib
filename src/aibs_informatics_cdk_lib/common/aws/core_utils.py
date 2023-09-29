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
