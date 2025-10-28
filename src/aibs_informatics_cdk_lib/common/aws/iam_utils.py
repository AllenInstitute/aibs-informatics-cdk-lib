"""
The list of actions for each service is incomplete and based on our needs so far.
A helpful resource to research actions is:
https://www.awsiamactions.io/
"""

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

#
# utils
#


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


#
# policy action lists
#

BATCH_READ_ONLY_ACTIONS = [
    "batch:Describe*",
    "batch:List*",
]

BATCH_FULL_ACCESS_ACTIONS = [
    "batch:RegisterJobDefinition",
    "batch:DeregisterJobDefinition",
    "batch:DescribeJobDefinitions",
    *BATCH_READ_ONLY_ACTIONS,
    "batch:*",
]

CLOUDWATCH_READ_ACTIONS = [
    "logs:DescribeLogGroups",
    "logs:GetLogEvents",
    "logs:GetLogGroupFields",
    "logs:GetLogRecord",
    "logs:GetQueryResults",
]

CLOUDWATCH_WRITE_ACTIONS = [
    "logs:CreateLogGroup",
    "logs:CreateLogStream",
    "logs:PutLogEvents",
]

CLOUDWATCH_FULL_ACCESS_ACTIONS = [
    *CLOUDWATCH_READ_ACTIONS,
    *CLOUDWATCH_WRITE_ACTIONS,
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

ECS_READ_ACTIONS = [
    "ecs:DescribeContainerInstances",
    "ecs:DescribeTaskDefinition",
    "ecs:DescribeTasks",
    "ecs:ListTasks",
]

ECS_WRITE_ACTIONS = [
    "ecs:RegisterTaskDefinition",
]

ECS_RUN_ACTIONS = [
    "ecs:RunTask",
]

ECS_FULL_ACCESS_ACTIONS = [
    *ECS_READ_ACTIONS,
    *ECS_WRITE_ACTIONS,
    *ECS_RUN_ACTIONS,
]

ECR_READ_ACTIONS = [
    "ecr:BatchCheckLayerAvailability",
    "ecr:BatchGetImage",
    "ecr:DescribeImageScanFindings",
    "ecr:DescribeImages",
    "ecr:DescribeRepositories",
    "ecr:GetAuthorizationToken",
    "ecr:GetDownloadUrlForLayer",
    "ecr:GetRepositoryPolicy",
    "ecr:ListImages",
    "ecr:ListTagsForResource",
]

ECR_WRITE_ACTIONS = [
    "ecr:CompleteLayerUpload",
    "ecr:CreateRepository",
    "ecr:DeleteRepository",
    "ecr:InitiateLayerUpload",
    "ecr:PutImage",
    "ecr:PutLifecyclePolicy",
    "ecr:UploadLayerPart",
]

ECR_TAGGING_ACTIONS = [
    "ecr:TagResource",
    "ecr:UntagResource",
]

ECR_FULL_ACCESS_ACTIONS = [
    *ECR_READ_ACTIONS,
    *ECR_TAGGING_ACTIONS,
    *ECR_WRITE_ACTIONS,
]

KMS_READ_ACTIONS = [
    "kms:Decrypt",
    "kms:DescribeKey",
]

KMS_WRITE_ACTIONS = [
    "kms:Encrypt",
    "kms:GenerateDataKey*",
    "kms:PutKeyPolicy",
]

KMS_FULL_ACCESS_ACTIONS = [
    *KMS_READ_ACTIONS,
    *KMS_WRITE_ACTIONS,
]


LAMBDA_FULL_ACCESS_ACTIONS = ["lambda:*"]
LAMBDA_READ_ONLY_ACTIONS = [
    "lambda:Get*",
    "lambda:List*",
]

S3_FULL_ACCESS_ACTIONS = ["s3:*"]

S3_READ_ONLY_ACCESS_ACTIONS = [
    "s3:Get*",
    "s3:List*",
    "s3-object-lambda:Get*",
    "s3-object-lambda:List*",
]

SECRETSMANAGER_READ_ONLY_ACTIONS = [
    "secretsmanager:DescribeSecret",
    "secretsmanager:GetRandomPassword",
    "secretsmanager:GetResourcePolicy",
    "secretsmanager:GetSecretValue",
    "secretsmanager:ListSecretVersionIds",
    "secretsmanager:ListSecrets",
]

SECRETSMANAGER_WRITE_ACTIONS = [
    "secretsmanager:CreateSecret",
    "secretsmanager:PutSecretValue",
    "secretsmanager:ReplicateSecretToRegions",
    "secretsmanager:RestoreSecret",
    "secretsmanager:RotateSecret",
    "secretsmanager:UpdateSecret",
    "secretsmanager:UpdateSecretVersionStage",
]

SECRETSMANAGER_DELETE_ACTIONS = [
    "secretsmanager:CancelRotateSecret",
    "secretsmanager:DeleteSecret",
    "secretsmanager:RemoveRegionsFromReplication",
    "secretsmanager:StopReplicationToReplica",
]

SECRETSMANAGER_READ_WRITE_ACTIONS = [
    *SECRETSMANAGER_READ_ONLY_ACTIONS,
    *SECRETSMANAGER_WRITE_ACTIONS,
]

SECRETSMANAGER_FULL_ADMIN_ACTIONS = [
    *SECRETSMANAGER_READ_ONLY_ACTIONS,
    *SECRETSMANAGER_WRITE_ACTIONS,
    *SECRETSMANAGER_DELETE_ACTIONS,
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

SQS_READ_ACTIONS = [
    "sqs:GetQueueAttributes",
    "sqs:GetQueueUrl",
    "sqs:ReceiveMessage",
    "sqs:SendMessage",
]

SQS_WRITE_ACTIONS = [
    "sqs:ChangeMessageVisibility",
    "sqs:DeleteMessage",
]

SQS_FULL_ACCESS_ACTIONS = [
    *SQS_READ_ACTIONS,
    *SQS_WRITE_ACTIONS,
]

SSM_READ_ACTIONS = [
    "ssm:GetParameter",
    "ssm:GetParameters",
    "ssm:GetParametersByPath",
]


#
# policy statement constants and builders
#

CODE_BUILD_IAM_POLICY = iam.PolicyStatement(
    actions=[
        *EC2_ACTIONS,
        *ECR_FULL_ACCESS_ACTIONS,
    ],
    resources=["*"],
)


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


def secretsmanager_policy_statement(
    actions: List[str] = SECRETSMANAGER_READ_ONLY_ACTIONS,
    sid: str = "SecretsManagerReadOnly",
    resource_id: str = "*",
    region: str = None,
    account: str = None,
) -> iam.PolicyStatement:
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_arn(
                service="secretsmanager",
                resource_id=resource_id,
                region=region,
                account=account,
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
