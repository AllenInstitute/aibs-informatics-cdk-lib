from typing import List, Optional, Union

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_iam as iam

from aibs_informatics_cdk_lib.common.aws.core_utils import (
    build_arn,
    build_batch_arn,
    build_dynamodb_arn,
    build_lambda_arn,
    build_s3_arn,
    build_sfn_arn,
)

BATCH_FULL_ACCESS_ACTIONS = [
    "batch:RegisterJobDefinition",
    "batch:DeregisterJobDefinition",
    "batch:DescribeJobDefinitions",
    "batch:List*",
    "batch:Describe*",
    "batch:*",
]

DYNAMODB_READ_ACTIONS = [
    "dynamodb:BatchGet*",
    "dynamodb:DescribeStream",
    "dynamodb:DescribeTable",
    "dynamodb:Get*",
    "dynamodb:Query",
    "dynamodb:Scan",
]

DYNAMODB_WRITE_ACTIONS = [
    "dynamodb:BatchWrite*",
    "dynamodb:CreateTable",
    "dynamodb:Delete*",
    "dynamodb:Update*",
    "dynamodb:PutItem",
]

DYNAMODB_READ_WRITE_ACTIONS = [
    *DYNAMODB_READ_ACTIONS,
    *DYNAMODB_WRITE_ACTIONS,
]


EC2_ACTIONS = ["ec2:DescribeAvailabilityZones"]

ECR_READ_ACTIONS = [
    "ecr:GetAuthorizationToken",
    "ecr:BatchCheckLayerAvailability",
    "ecr:GetDownloadUrlForLayer",
    "ecr:GetRepositoryPolicy",
    "ecr:DescribeRepositories",
    "ecr:ListImages",
    "ecr:DescribeImages",
    "ecr:BatchGetImage",
    "ecr:ListTagsForResource",
    "ecr:DescribeImageScanFindings",
]

ECR_WRITE_ACTIONS = [
    "ecr:CreateRepository",
    "ecr:DeleteRepository",
    "ecr:InitiateLayerUpload",
    "ecr:UploadLayerPart",
    "ecr:CompleteLayerUpload",
    "ecr:PutImage",
    "ecr:PutLifecyclePolicy",
]

ECR_TAGGING_ACTIONS = [
    "ecr:TagResource",
    "ecr:UntagResource",
]

ECR_FULL_ACCESS_ACTIONS = [*ECR_READ_ACTIONS, *ECR_WRITE_ACTIONS, *ECR_TAGGING_ACTIONS]

ECS_READ_ACTIONS = ["ecs:DescribeContainerInstances"]


CODE_BUILD_IAM_POLICY = iam.PolicyStatement(
    actions=[
        *EC2_ACTIONS,
        *ECR_FULL_ACCESS_ACTIONS,
    ],
    resources=["*"],
)


LAMBDA_FULL_ACCESS_ACTIONS = ["lambda:*"]

S3_FULL_ACCESS_ACTIONS = ["s3:*"]
S3_READ_ONLY_ACCESS_ACTIONS = [
    "s3:Get*",
    "s3:List*",
    "s3-object-lambda:Get*",
    "s3-object-lambda:List*",
]

SES_FULL_ACCESS_ACTIONS = ["ses:*"]

SFN_STATES_READ_ACCESS_ACTIONS = [
    "states:DescribeActivity",
    "states:DescribeExecution",
    "states:DescribeStateMachine",
    "states:DescribeStateMachineForExecution",
    "states:ListExecutions",
    "states:GetExecutionHistory",
    "states:ListStateMachines",
    "states:ListActivities",
]

SFN_STATES_EXECUTION_ACTIONS = [
    "states:StartExecution",
    "states:StopExecution",
]

SNS_FULL_ACCESS_ACTIONS = ["sns:*"]

SSM_READ_ACTIONS = [
    "ssm:GetParameter",
    "ssm:GetParameters",
    "ssm:GetParametersByPath",
]


def batch_policy_statement(
    env_base: Optional[EnvBase] = None,
    actions: List[str] = BATCH_FULL_ACCESS_ACTIONS,
    sid: str = "BatchReadWrite",
) -> iam.PolicyStatement:
    resource_id = f"{env_base or ''}*"

    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_batch_arn(
                resource_id=resource_id,
                resource_type="compute-environment",
            ),
            build_batch_arn(
                resource_id=resource_id,
                resource_type="job",
            ),
            build_batch_arn(
                resource_id=resource_id,
                resource_type="job-definition",
            ),
            build_batch_arn(
                resource_id=resource_id,
                resource_type="job-queue",
            ),
            # ERROR: An error occurred (AccessDeniedException) when calling the
            # DescribeJobDefinitions operation:
            # User: arn:aws:sts::051791135335:assumed-role/Infrastructure.../dev-ryan-gwo-create-job-definition-fn
            # is not authorized to perform: batch:DescribeJobDefinitions on resource: "*"
            # TODO: WTF why does this not work... adding "*" resource for now
            "*",
        ],
    )


def dynamodb_policy_statement(
    env_base: Optional[EnvBase] = None,
    actions: List[str] = DYNAMODB_READ_WRITE_ACTIONS,
    sid: str = "DynamoDBReadWrite",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_dynamodb_arn(
                resource_id=f"{env_base or ''}*",
                resource_type="table",
            ),
        ],
    )


def ecs_policy_statement(
    actions: List[str] = ECS_READ_ACTIONS, sid: str = "ECSDescribe"
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_arn(
                service="ecs",
                resource_id="*/*",
                resource_type="container-instance",
                resource_delim="/",
            ),
        ],
    )


def lambda_policy_statement(
    env_base: Optional[EnvBase] = None,
    actions: List[str] = LAMBDA_FULL_ACCESS_ACTIONS,
    sid: str = "LambdaReadWrite",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_lambda_arn(
                resource_id=f"{env_base or ''}*",
                resource_type="function",
            ),
        ],
    )


def s3_policy_statement(
    env_base: Optional[EnvBase] = None,
    actions: List[str] = S3_FULL_ACCESS_ACTIONS,
    sid: str = "S3FullAccess",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_s3_arn(
                resource_id=f"{env_base or ''}*",
                resource_type="bucket",
            ),
        ],
    )


def ses_policy_statement(
    actions: List[str] = SES_FULL_ACCESS_ACTIONS,
    sid: str = "SESFullAccess",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_arn(
                service="ses",
            ),
        ],
    )


def sfn_policy_statement(
    env_base: Optional[EnvBase] = None,
    actions: List[str] = SFN_STATES_READ_ACCESS_ACTIONS,
    sid: str = "SfnFullAccess",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_sfn_arn(
                resource_id=f"{env_base or ''}*",
                resource_type="*",
            ),
        ],
    )


def sns_policy_statement(
    actions: List[str] = SNS_FULL_ACCESS_ACTIONS,
    sid: str = "SNSFullAccess",
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_arn(
                service="sns",
            ),
        ],
    )


def ssm_policy_statement(
    actions: List[str] = SSM_READ_ACTIONS, sid: str = "SSMParamReadActions"
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid, actions=actions, effect=iam.Effect.ALLOW, resources=[build_arn(service="ssm")]
    )


def grant_managed_policies(
    role: Optional[iam.IRole],
    *managed_policies: Union[str, iam.ManagedPolicy],
):
    if not role:
        return

    for mp in managed_policies:
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(mp) if isinstance(mp, str) else mp
        )
