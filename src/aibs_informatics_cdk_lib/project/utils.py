__all__ = [
    "get_package_root",
    "resolve_repo_root",
    "get_env_base",
    "get_config",
]

import logging
import os
import pathlib
from typing import List, Optional, Tuple, Type, Union

import constructs
from aibs_informatics_core.env import (
    ENV_BASE_KEY_ALIAS,
    ENV_LABEL_KEY_ALIAS,
    ENV_TYPE_KEY_ALIAS,
    LABEL_KEY,
    LABEL_KEY_ALIAS,
    EnvBase,
    EnvType,
)
from aibs_informatics_core.utils.os_operations import get_env_var, set_env_var

from aibs_informatics_cdk_lib.project.config import BaseProjectConfig, G, P, ProjectConfig, S

logger = logging.getLogger(__name__)


def resolve_repo_root(start_path: Optional[Union[str, pathlib.Path]] = None) -> str:
    """Find the root of the git repository

    Returns:
        str: Absolute root path
    """
    return str(
        next(
            filter(
                lambda p: (p / ".git").is_dir(),  # type: ignore
                pathlib.Path(start_path or __file__).absolute().parents,
            )
        )
    )


def get_package_root() -> str:
    """Find the root package

    ASSUMPTION: the infrastructure package name is "aibs-informatics-cdk-lib"

    Returns:
        str: Absolute root path
    """
    return str(
        next(
            filter(
                lambda p: (p / "aibs-informatics-cdk-lib").is_dir(),  # type: ignore
                pathlib.Path(__file__).absolute().parents,
            )
        )
    )


ENV_BASE_KEYS = [EnvBase.ENV_BASE_KEY, ENV_BASE_KEY_ALIAS, "env"]
ENV_TYPE_KEYS = [EnvBase.ENV_TYPE_KEY, ENV_TYPE_KEY_ALIAS]
ENV_LABEL_KEYS = [EnvBase.ENV_LABEL_KEY, ENV_LABEL_KEY_ALIAS, LABEL_KEY, LABEL_KEY_ALIAS]


def get_env_base(node: constructs.Node) -> EnvBase:
    """Resolves EnvBase from cdk context or environment

    Order of resolution:
        1. CDK context value (specifying -c K=V)
            1. look for EnvBase
            2. look for EnvType/EnvLabel
        2. env variable
            1. Look for EnvBase
            2. Look for EnvType/EnvLabel

    Args:
        node (constructs.Node): cdk construct node

    Returns:
        EnvBase: environment base
    """
    # We need to check from context node FIRST before reading from env variables
    env_base_str = _get_from_context(node, ENV_BASE_KEYS)
    env_type_str = _get_from_context(node, ENV_TYPE_KEYS)
    env_label__from_context = _get_from_context(node, ENV_LABEL_KEYS)
    env_label__from_env = EnvBase.load_env_label__from_env()
    if env_base_str:
        env_base = EnvBase(env_base_str)
        logger.info(f"Loading EnvBase from CDK CONTEXT: env_base={env_base}")
        return env_base

    if env_type_str:
        env_type = EnvType(env_type_str)
        env_label = env_label__from_context or env_label__from_env
        env_base = EnvBase.from_type_and_label(env_type=env_type, env_label=env_label)
        logger.info(f"Loading EnvBase from type/label CDK CONTEXT: env_base={env_base}")
        return env_base

    # Now we try to load env base from environment
    try:
        env_base = EnvBase.from_env()
        logger.info(f"Loading EnvBase from ENV_VARs: env_base={env_base}")
        return env_base
    except:
        env_type = EnvType.DEV

        env_label = env_label__from_context
        if env_label is None:
            if env_label__from_env:
                env_label = env_label__from_env
            else:
                env_label = (get_env_var("USER") or "anonymous").split(".")[0]
        logger.info(
            f"No EnvBase defaults set by CDK CONTEXT or ENV VAR. "
            f"Using env_type={env_type}, env_label={env_label}"
        )
        return EnvBase.from_type_and_label(env_type=env_type, env_label=env_label)


def set_env_base(env_base: EnvBase) -> None:
    """Set the environment base

    Args:
        env_base (EnvBase): environment base
    """
    set_env_var(EnvBase.ENV_BASE_KEY, env_base)
    set_env_var(EnvBase.ENV_TYPE_KEY, env_base.env_type)
    if env_base.env_label:
        set_env_var(EnvBase.ENV_LABEL_KEY, env_base.env_label)
    else:
        os.environ.pop(EnvBase.ENV_LABEL_KEY, None)


def get_project_config_and_env_base(
    node: constructs.Node, project_config_cls: Type[P] = ProjectConfig
) -> Tuple[P, EnvBase]:
    env_base = get_env_base(node)

    config = project_config_cls.load_config()
    return config, env_base


def get_config(
    node: constructs.Node, project_config_cls: Type[BaseProjectConfig[G, S]] = ProjectConfig
) -> S:
    """
    Retrieves the stage configuration for a given node.

    Args:
        node (constructs.Node): The node for which to retrieve the configuration.
        project_config_cls (Type[BaseProjectConfig[G, S]], optional): The project configuration class to use.
            Defaults to ProjectConfig.

    Returns:
        S: The stage configuration object.

    """
    project_config, env_base = get_project_config_and_env_base(  # type: ignore
        node, project_config_cls=project_config_cls
    )
    set_env_base(env_base)

    stage_config: S = project_config.get_stage_config(env_type=env_base.env_type)
    stage_config.env.label = env_base.env_label

    return stage_config


def _get_from_context(
    node: constructs.Node, keys: List[str], default: Optional[str] = None
) -> Optional[str]:
    for key in keys:
        value = node.try_get_context(key)
        if value is not None:
            return value
    else:
        return default
