import aws_cdk as cdk
import constructs
import pytest
from aibs_informatics_core.env import (
    ENV_BASE_KEY,
    ENV_BASE_KEY_ALIAS,
    ENV_LABEL_KEY,
    ENV_LABEL_KEY_ALIAS,
    ENV_TYPE_KEY,
    ENV_TYPE_KEY_ALIAS,
    LABEL_KEY,
    LABEL_KEY_ALIAS,
    EnvBase,
    EnvType,
)

from aibs_informatics_cdk_lib.project.utils import (
    ENV_BASE_KEYS,
    ENV_LABEL_KEYS,
    ENV_TYPE_KEYS,
    get_env_base,
)

USER = "marmotdev"


@pytest.fixture(scope="function")
def dummy_node():
    app = cdk.App(analytics_reporting=False, auto_synth=False)
    construct = constructs.Construct(app, "construct")
    node = construct.node
    return node


@pytest.fixture(scope="function")
def env_vars(monkeypatch):
    for key in ENV_BASE_KEYS + ENV_TYPE_KEYS + ENV_LABEL_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("USER", USER)
    return monkeypatch


def test__get_env_base__no_context_no_env_vars(env_vars, dummy_node):
    assert get_env_base(dummy_node) == EnvBase("dev-marmotdev")

    env_vars.setenv("USER", "marmot.dev")
    assert get_env_base(dummy_node) == EnvBase("dev-marmot")


def test__get_env_base__env_vars_only(env_vars, dummy_node):
    # Label env var priority tests
    for i, key in enumerate([LABEL_KEY_ALIAS, LABEL_KEY, ENV_LABEL_KEY_ALIAS, ENV_LABEL_KEY]):
        label = f"marmot{i}"
        env_vars.setenv(key, label)
        assert get_env_base(dummy_node) == EnvBase(f"dev-{label}")
    env_vars.setenv(ENV_LABEL_KEY, "marmot")

    # Type env var priority tests
    env_vars.setenv(ENV_TYPE_KEY_ALIAS, "prod")
    assert get_env_base(dummy_node) == EnvBase("prod-marmot")
    env_vars.setenv(ENV_TYPE_KEY, "test")
    assert get_env_base(dummy_node) == EnvBase("test-marmot")

    # Base env var priority tests
    env_vars.setenv(ENV_BASE_KEY_ALIAS, "dev")
    assert get_env_base(dummy_node) == EnvBase("dev")

    env_vars.setenv(ENV_BASE_KEY, "prod")
    assert get_env_base(dummy_node) == EnvBase("prod")


def test__get_env_base__context_only(env_vars, dummy_node):
    # Label env var priority tests
    for i, key in enumerate([LABEL_KEY_ALIAS, LABEL_KEY, ENV_LABEL_KEY_ALIAS, ENV_LABEL_KEY]):
        label = f"marmot{i}"
        dummy_node.set_context(key, label)
        assert get_env_base(dummy_node) == EnvBase(f"dev-{label}")
    dummy_node.set_context(ENV_LABEL_KEY, "marmot")

    # Type env var priority tests
    dummy_node.set_context(ENV_TYPE_KEY_ALIAS, "prod")
    assert get_env_base(dummy_node) == EnvBase("prod-marmot")
    dummy_node.set_context(ENV_TYPE_KEY, "test")
    assert get_env_base(dummy_node) == EnvBase("test-marmot")

    # Base env var priority tests
    dummy_node.set_context("env", "dev")
    assert get_env_base(dummy_node) == EnvBase("dev")
    dummy_node.set_context(ENV_BASE_KEY, "prod")
    assert get_env_base(dummy_node) == EnvBase("prod")


def test__get_env_base__context_and_env_vars(env_vars, dummy_node):
    # Type/Label not used from context if both not present
    dummy_node.set_context(ENV_LABEL_KEY, "marmot")
    env_vars.setenv(ENV_TYPE_KEY_ALIAS, "prod")
    assert get_env_base(dummy_node) == EnvBase("prod")

    # Type/Label from context supercedes env base from env
    dummy_node.set_context(ENV_TYPE_KEY, "test")
    env_vars.setenv(ENV_BASE_KEY_ALIAS, "dev")
    assert get_env_base(dummy_node) == EnvBase("test-marmot")

    # Base from context supercedes type/label
    dummy_node.set_context(ENV_BASE_KEY, "dev")
    assert get_env_base(dummy_node) == EnvBase("dev")
