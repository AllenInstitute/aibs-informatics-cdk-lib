import re
from typing import Optional

import aws_cdk as cdk
import pytest
from aibs_informatics_core.env import EnvBase

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    SECRETSMANAGER_READ_ONLY_ACTIONS,
    SECRETSMANAGER_READ_WRITE_ACTIONS,
    SQS_FULL_ACCESS_ACTIONS,
    secretsmanager_policy_statement,
    sqs_policy_statement,
)


def test_secretsmanager_policy_statement_default():
    statement = secretsmanager_policy_statement()

    assert statement.to_statement_json()["Effect"] == "Allow"
    assert set(statement.actions) == set(SECRETSMANAGER_READ_ONLY_ACTIONS)
    assert statement.resources == [
        f"arn:aws:secretsmanager:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:*"
    ]


generate_policy_test_cases = [
    pytest.param(
        dict(resource_id="airflow/connections/*"),
        f"arn:aws:secretsmanager:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:airflow/connections/*",
        SECRETSMANAGER_READ_ONLY_ACTIONS,
    ),
    pytest.param(
        dict(resource_id="my-secret", region="*"),
        f"arn:aws:secretsmanager:*:{cdk.Aws.ACCOUNT_ID}:my-secret",
        SECRETSMANAGER_READ_ONLY_ACTIONS,
    ),
    pytest.param(
        dict(
            resource_id="stage/db-password",
            account="123456789012",
            actions=SECRETSMANAGER_READ_WRITE_ACTIONS,
        ),
        f"arn:aws:secretsmanager:{cdk.Aws.REGION}:123456789012:stage/db-password",
        SECRETSMANAGER_READ_WRITE_ACTIONS,
    ),
]


@pytest.mark.parametrize(
    "generate_policy_args,expected_resource,expected_actions", generate_policy_test_cases
)
def test__secrets_manager_policy_args(generate_policy_args, expected_resource, expected_actions):
    generated_policy_statement = secretsmanager_policy_statement(**generate_policy_args)
    assert generated_policy_statement.resources == [expected_resource]
    assert set(generated_policy_statement.actions) == set(expected_actions)


# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html#sqs-resource-and-operations
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-basic-examples-of-sqs-policies.html
@pytest.mark.parametrize(
    "env_base, expected_actions, expected_resource_patterns",
    [
        pytest.param(
            # env_base
            None,
            # expected_actions
            SQS_FULL_ACCESS_ACTIONS,
            # expected_resource_patterns
            [
                r"arn:aws:sqs:\$\{Token\[AWS\.Region\.[\d]+\]\}:\$\{Token\[AWS\.AccountId\.[\d]+\]\}:\*"
            ],
            id="Test SQS policystatement (env_base=None)",
        ),
        pytest.param(
            # env_base
            EnvBase("dev"),
            # expected_actions
            SQS_FULL_ACCESS_ACTIONS,
            # expected_resource_patterns
            [
                r"arn:aws:sqs:\$\{Token\[AWS\.Region\.[\d]+\]\}:\$\{Token\[AWS\.AccountId\.[\d]+\]\}:dev\*"
            ],
            id="Test SQS policystatement (env_base=dev)",
        ),
        pytest.param(
            # env_base
            EnvBase("test"),
            # expected_actions
            SQS_FULL_ACCESS_ACTIONS,
            # expected_resource_patterns
            [
                r"arn:aws:sqs:\$\{Token\[AWS\.Region\.[\d]+\]\}:\$\{Token\[AWS\.AccountId\.[\d]+\]\}:test\*"
            ],
            id="Test SQS policystatement (env_base=test)",
        ),
    ],
)
def test__sqs_policy_statement(
    env_base: Optional[EnvBase], expected_actions, expected_resource_patterns
):
    obt = sqs_policy_statement(env_base=env_base)

    assert expected_actions == obt.actions
    for indx, expected_pattern in enumerate(expected_resource_patterns):
        obt_resource = obt.resources[indx]
        assert re.fullmatch(
            expected_pattern, obt_resource
        ), f"expected_pattern: {expected_pattern}, obtained: {obt_resource}"
