"""Environment-aware CloudWatch alarms with collision-safe physical names.

CloudFormation replaces an `AWS::CloudWatch::Alarm` only when its `AlarmName` changes or the
alarm moves to a new logical ID (a renamed or relocated construct). A replacement creates the
new alarm before deleting the old one, so a fixed name fails with "already exists".

`EnvBaseAlarm` names alarms `<env_base>-<name>-<hash>`, where the hash is taken from the
construct path. Any move that would trigger a replacement also changes the path, and therefore
the name, so the replacement never collides with the alarm it replaces.
"""

__all__ = [
    "EnvBaseAlarm",
    "build_alarm_name",
]

import hashlib

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_cloudwatch as cw

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins

ALARM_NAME_HASH_SIZE = 8


def build_alarm_name(env_base: EnvBase, scope: constructs.Construct, id: str, name: str) -> str:
    """Build `<env_base>-<name>-<hash of construct path>` for an alarm about to be created.

    Args:
        env_base (EnvBase): environment base used as the name prefix.
        scope (constructs.Construct): scope the alarm will be created in.
        id (str): construct id the alarm will be created with.
        name (str): human readable part of the name.

    Returns:
        The alarm name.
    """
    path = "/".join(part for part in (scope.node.path, id) if part)
    digest = hashlib.sha256(path.encode()).hexdigest()[:ALARM_NAME_HASH_SIZE]
    return env_base.get_resource_name(name, digest)


class EnvBaseAlarm(cw.Alarm, EnvBaseConstructMixins):
    """A `cw.Alarm` whose name is env-prefixed and safe to refactor or move between stacks.

    Leaving `alarm_name` unset keeps CloudFormation's generated name.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        metric: cw.IMetric,
        threshold: float,
        evaluation_periods: int,
        alarm_name: str | None = None,
        **kwargs,
    ) -> None:
        self.env_base = env_base
        super().__init__(
            scope,
            id,
            metric=metric,
            threshold=threshold,
            evaluation_periods=evaluation_periods,
            alarm_name=(
                build_alarm_name(env_base, scope, id, alarm_name) if alarm_name else None
            ),
            **kwargs,
        )
