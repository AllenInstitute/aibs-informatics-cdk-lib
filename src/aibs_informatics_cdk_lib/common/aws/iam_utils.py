"""IAM utilities for building policy statements and granting permissions.

This module provides predefined IAM actions lists and helper functions for
creating policy statements for various AWS services.

Note:
    The list of actions for each service is incomplete and based on project needs.
    A helpful resource to research actions is: https://www.awsiamactions.io/
"""

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
    role: iam.IRole | None,
    *managed_policies: str | iam.ManagedPolicy,
) -> None:
    """Grant managed policies to an IAM role.

    Args:
        role (Optional[iam.IRole]): The IAM role to grant policies to.
            If None, no action is taken.
        *managed_policies (Union[str, iam.ManagedPolicy]): Variable number of
            managed policies to grant. Can be policy names (str) or
            ManagedPolicy objects.
    """
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
    env_base: EnvBase | None = None,
    actions: list[str] = BATCH_FULL_ACCESS_ACTIONS,
    sid: str = "BatchReadWrite",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for AWS Batch.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of Batch actions to allow.
            Defaults to BATCH_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "BatchReadWrite".

    Returns:
        IAM policy statement for Batch resources.
    """
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
            # User: arn:aws:sts::051791135335:assumed-role/Infrastructure.../dev-ryan-gwo-create-job-definition-fn  # noqa: E501
            # is not authorized to perform: batch:DescribeJobDefinitions on resource: "*"
            # TODO: WTF why does this not work... adding "*" resource for now
            "*",
        ],
    )


def dynamodb_policy_statement(
    env_base: EnvBase | None = None,
    actions: list[str] = DYNAMODB_READ_WRITE_ACTIONS,
    sid: str = "DynamoDBReadWrite",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for DynamoDB.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of DynamoDB actions to allow.
            Defaults to DYNAMODB_READ_WRITE_ACTIONS.
        sid (str): Statement ID. Defaults to "DynamoDBReadWrite".

    Returns:
        IAM policy statement for DynamoDB tables.
    """
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
    actions: list[str] = ECS_READ_ACTIONS, sid: str = "ECSDescribe"
) -> iam.PolicyStatement:
    """Create an IAM policy statement for ECS.

    Args:
        actions (List[str]): List of ECS actions to allow.
            Defaults to ECS_READ_ACTIONS.
        sid (str): Statement ID. Defaults to "ECSDescribe".

    Returns:
        IAM policy statement for ECS resources.
    """
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
    env_base: EnvBase | None = None,
    actions: list[str] = LAMBDA_FULL_ACCESS_ACTIONS,
    sid: str = "LambdaReadWrite",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for Lambda.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of Lambda actions to allow.
            Defaults to LAMBDA_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "LambdaReadWrite".

    Returns:
        IAM policy statement for Lambda functions.
    """
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
    env_base: EnvBase | None = None,
    actions: list[str] = S3_FULL_ACCESS_ACTIONS,
    sid: str = "S3FullAccess",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for S3.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of S3 actions to allow.
            Defaults to S3_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "S3FullAccess".

    Returns:
        IAM policy statement for S3 buckets.
    """
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
    actions: list[str] = SECRETSMANAGER_READ_ONLY_ACTIONS,
    sid: str = "SecretsManagerReadOnly",
    resource_id: str = "*",
    region: str = None,
    account: str = None,
) -> iam.PolicyStatement:
    """Create an IAM policy statement for Secrets Manager.

    Args:
        actions (List[str]): List of Secrets Manager actions to allow.
            Defaults to SECRETSMANAGER_READ_ONLY_ACTIONS.
        sid (str): Statement ID. Defaults to "SecretsManagerReadOnly".
        resource_id (str): Resource identifier. Defaults to "*".
        region (str): AWS region. Defaults to None (current region).
        account (str): AWS account ID. Defaults to None (current account).

    Returns:
        IAM policy statement for Secrets Manager resources.
    """
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
    actions: list[str] = SES_FULL_ACCESS_ACTIONS,
    sid: str = "SESFullAccess",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for SES.

    Args:
        actions (List[str]): List of SES actions to allow.
            Defaults to SES_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "SESFullAccess".

    Returns:
        IAM policy statement for SES resources.
    """
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
    env_base: EnvBase | None = None,
    actions: list[str] = SFN_STATES_READ_ACCESS_ACTIONS,
    sid: str = "SfnFullAccess",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for Step Functions.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of Step Functions actions to allow.
            Defaults to SFN_STATES_READ_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "SfnFullAccess".

    Returns:
        IAM policy statement for Step Functions resources.
    """
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
    actions: list[str] = SNS_FULL_ACCESS_ACTIONS,
    sid: str = "SNSFullAccess",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for SNS.

    Args:
        actions (List[str]): List of SNS actions to allow.
            Defaults to SNS_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "SNSFullAccess".

    Returns:
        IAM policy statement for SNS resources.
    """
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
    actions: list[str] = SSM_READ_ACTIONS, sid: str = "SSMParamReadActions"
) -> iam.PolicyStatement:
    """Create an IAM policy statement for SSM Parameter Store.

    Args:
        actions (List[str]): List of SSM actions to allow.
            Defaults to SSM_READ_ACTIONS.
        sid (str): Statement ID. Defaults to "SSMParamReadActions".

    Returns:
        IAM policy statement for SSM resources.
    """
    return iam.PolicyStatement(
        sid=sid, actions=actions, effect=iam.Effect.ALLOW, resources=[build_arn(service="ssm")]
    )


def sqs_policy_statement(
    env_base: EnvBase | None = None,
    actions: list[str] = SQS_FULL_ACCESS_ACTIONS,
    sid: str = "SQSFullAccess",
) -> iam.PolicyStatement:
    """Create an IAM policy statement for SQS.

    Args:
        env_base (Optional[EnvBase]): Environment base for resource prefix.
            Defaults to None (matches all).
        actions (List[str]): List of SQS actions to allow.
            Defaults to SQS_FULL_ACCESS_ACTIONS.
        sid (str): Statement ID. Defaults to "SQSFullAccess".

    Returns:
        IAM policy statement for SQS queues.
    """
    return iam.PolicyStatement(
        sid=sid,
        actions=actions,
        effect=iam.Effect.ALLOW,
        resources=[
            build_arn(
                service="sqs",
                resource_id=f"{env_base or ''}*",
            )
        ],
    )
