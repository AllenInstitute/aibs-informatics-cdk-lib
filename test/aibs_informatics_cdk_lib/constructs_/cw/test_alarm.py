import hashlib

import aws_cdk as cdk
from aws_cdk import aws_cloudwatch as cw
from aws_cdk.assertions import Match

from aibs_informatics_cdk_lib.constructs_.cw import EnvBaseAlarm
from aibs_informatics_cdk_lib.constructs_.monitoring import MonitoringConstruct
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack
from test.aibs_informatics_cdk_lib.base import CdkBaseTest


def _metric() -> cw.Metric:
    return cw.Metric(namespace="AWS/RDS", metric_name="CPUUtilization")


class TestEnvBaseAlarm(CdkBaseTest):
    def test__alarm_name__env_prefixed_with_construct_path_hash(self):
        stack = self.get_dummy_stack("test")

        alarm = EnvBaseAlarm(
            stack,
            "alarm",
            self.env_base,
            metric=_metric(),
            threshold=80,
            evaluation_periods=3,
            alarm_name="db-cpu",
        )

        digest = hashlib.sha256(alarm.node.path.encode()).hexdigest()[:8]
        self.get_template(stack).has_resource_properties(
            "AWS::CloudWatch::Alarm", {"AlarmName": f"{self.env_base}-db-cpu-{digest}"}
        )

    def test__alarm_name__differs_between_construct_paths(self):
        names = []
        for stack_name in ("stack-a", "stack-b"):
            stack = EnvBaseStack(cdk.App(), stack_name, self.env_base)
            EnvBaseAlarm(
                stack,
                "alarm",
                self.env_base,
                metric=_metric(),
                threshold=80,
                evaluation_periods=3,
                alarm_name="db-cpu",
            )
            (alarm,) = self.get_template(stack).find_resources("AWS::CloudWatch::Alarm").values()
            names.append(alarm["Properties"]["AlarmName"])

        self.assertNotEqual(names[0], names[1])
        for name in names:
            self.assertTrue(name.startswith(f"{self.env_base}-db-cpu-"))

    def test__alarm_name__omitted_keeps_generated_name(self):
        stack = self.get_dummy_stack("test")

        EnvBaseAlarm(
            stack, "alarm", self.env_base, metric=_metric(), threshold=80, evaluation_periods=3
        )

        self.get_template(stack).has_resource_properties(
            "AWS::CloudWatch::Alarm", {"AlarmName": Match.absent()}
        )


class TestMonitoringAlarmStability(CdkBaseTest):
    """Adding to a widget group must not replace the alarms already in it."""

    def _alarms(self, add_widgets) -> dict[str, str]:
        stack = EnvBaseStack(cdk.App(), "monitoring", self.env_base)
        monitoring = MonitoringConstruct(stack, "monitoring", self.env_base)
        add_widgets(monitoring, monitoring.create_dashboard())
        alarms = self.get_template(stack).find_resources("AWS::CloudWatch::Alarm")
        return {
            logical_id: resource["Properties"]["AlarmName"]
            for logical_id, resource in alarms.items()
        }

    def test__add_function_widgets__existing_alarms_unchanged(self):
        before = self._alarms(
            lambda m, d: m.add_function_widgets(d, "fn-a", include_alarm=True)
        )
        after = self._alarms(
            lambda m, d: m.add_function_widgets(d, "fn-a", "fn-b", include_alarm=True)
        )

        self.assertEqual(len(before), 1)
        self.assertEqual(len(after), 2)
        self.assertLessEqual(before.items(), after.items())

    def test__add_state_machine_widgets__existing_alarms_unchanged(self):
        before = self._alarms(lambda m, d: m.add_state_machine_widgets(d, "sm-a"))
        after = self._alarms(lambda m, d: m.add_state_machine_widgets(d, "sm-a", "sm-b"))

        self.assertEqual(len(before), 1)
        self.assertEqual(len(after), 2)
        self.assertLessEqual(before.items(), after.items())

    def test__add_function_widgets__alarm_named_with_env_base(self):
        alarms = self._alarms(lambda m, d: m.add_function_widgets(d, "fn-a", include_alarm=True))

        (name,) = alarms.values()
        self.assertTrue(name.startswith(f"{self.env_base}-fn-a-errors-"), name)
