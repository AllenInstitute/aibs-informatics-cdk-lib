from test.aibs_informatics_cdk_lib.base import CdkBaseTest

from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.external_sns_trigger import ExternalSnsTrigger


class TestExternaSnsTriggerStack(CdkBaseTest):
    def test__init__simple(self):
        stack = self.get_dummy_stack("dummy-test-stack")

        trigger_construct = ExternalSnsTrigger(
            scope=stack,
            id="test-external-sns-trigger",
            env_base=self.env_base,
            triggered_lambda_fn=None,
            external_sns_event_name="test-event",
            external_sns_topic_arn="arn:aws:sns:us-west-2:123456789012:TestTopic",
        )

        template = self.get_template(stack)
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 0)
        template.resource_count_is("AWS::SNS::Subscription", 1)
        template.resource_count_is("AWS::SQS::Queue", 2)
        template.resource_count_is("AWS::SQS::QueuePolicy", 1)
        template.resource_count_is("AWS::CloudWatch::Alarm", 1)

    def test__init__with_triggered_lambda_fn(self):
        stack = self.get_dummy_stack("dummy-test-stack")

        triggered_lambda_fn = lambda_.Function(
            scope=stack,
            id="dummy-lambda-fn",
            code=lambda_.Code.from_inline("print('hello world')"),
            handler="index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
        )

        trigger_construct = ExternalSnsTrigger(
            scope=stack,
            id="test-external-sns-trigger",
            env_base=self.env_base,
            triggered_lambda_fn=triggered_lambda_fn,
            external_sns_event_name="test-event",
            external_sns_topic_arn="arn:aws:sns:us-west-2:123456789012:TestTopic",
        )

        template = self.get_template(stack)
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 1)
        template.resource_count_is("AWS::SNS::Subscription", 1)
        template.resource_count_is("AWS::SQS::Queue", 2)
        template.resource_count_is("AWS::SQS::QueuePolicy", 1)
        template.resource_count_is("AWS::CloudWatch::Alarm", 1)
