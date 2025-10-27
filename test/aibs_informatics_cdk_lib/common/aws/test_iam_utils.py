import aws_cdk as cdk
import pytest

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    SECRETSMANAGER_FULL_ACCESS_ACTIONS,
    SECRETSMANAGER_READ_ONLY_ACTIONS,
    secretsmanager_policy_statement,
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
            actions=SECRETSMANAGER_FULL_ACCESS_ACTIONS,
        ),
        f"arn:aws:secretsmanager:{cdk.Aws.REGION}:123456789012:stage/db-password",
        SECRETSMANAGER_FULL_ACCESS_ACTIONS,
    ),
]


@pytest.mark.parametrize(
    "generate_policy_args,expected_resource,expected_actions", generate_policy_test_cases
)
def test__secrets_manager_policy_args(generate_policy_args, expected_resource, expected_actions):
    generated_policy_statement = secretsmanager_policy_statement(**generate_policy_args)
    assert generated_policy_statement.resources == [expected_resource]
    assert set(generated_policy_statement.actions) == set(expected_actions)
