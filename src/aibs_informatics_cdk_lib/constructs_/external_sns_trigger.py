from typing import Any, Mapping, Optional, Sequence, Union

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase, EnvType, ResourceNameBaseEnum
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_sns_subscriptions import SqsSubscription


class ExternalSnsTrigger(constructs.Construct):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        triggered_lambda_fn: lambda_.Function,
        external_sns_event_name: Union[str, ResourceNameBaseEnum],
        external_sns_topic_arn: str,
        external_sns_event_queue_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        external_sns_event_queue_name: Optional[str] = None,
        external_sns_event_dlq_name: Optional[str] = None,
        external_sns_event_queue_retention_period: Optional[cdk.Duration] = cdk.Duration.days(7),
        sqs_event_source_enabled: Optional[bool] = None,
        **kwargs,
    ) -> None:
        """This intended to be a generic CDK construct that defines resources necessary to
        implement a trigger system that listens for events from an external
        (some other AWS account) SNS Topic and fires off a lambda in the
        account where this stack is deployed.


                              ┌---- ExternalSNSTrigger ----┐
                              |                            |
        (other AWS account)   |           (SQS)            |       (provided Lambda)
        external_sns_topic -----> external_sns_event_queue -----> triggered_lambda_fn
                              |             |              |
                              |             v              |
                              |         (SQS DLQ)          |
                              |   external_sns_event_dlq   |
                              |             |              |
                              |             v              |
                              |        (Cloudwatch)        |
                              | triggered_lambda_dlq_alarm |
                              |                            |
                              └----------------------------┘

        The main intended use case is to provide a simple template for setting up automation
        based on PTS SNS Topic notifications in a separate AWS account (e.g. in an informatics processing pipeline)

        Example usage:
        self.external_sns_trigger_construct = ExternalSnsTrigger(
            scope=self,
            id=self.env_base.get_construct_id("merscope-imaging-process-sns-trigger"),
            env_base=self.env_base,
            triggered_lambda_fn=self.pts_listener_fn,
            external_sns_event_name="merscope-imaging-process",
            external_sns_topic_arn=f"arn:aws:sns:us-west-2:{account_id}:{sns_topic_name}",
        )
        """
        super().__init__(scope=scope, id=id)

        if external_sns_event_queue_name is None:
            external_sns_event_queue_name = env_base.prefixed(
                external_sns_event_name, "sns-event-queue"
            )
        if external_sns_event_dlq_name is None:
            external_sns_event_dlq_name = env_base.prefixed(
                external_sns_event_name, "sns-event-dlq"
            )
        if sqs_event_source_enabled is None:
            sqs_event_source_enabled = env_base.env_type is EnvType.PROD

        self.external_sns_event_dlq = sqs.Queue(
            scope=self,
            id=env_base.get_construct_id(external_sns_event_name, "sns-event-dlq"),
            queue_name=external_sns_event_dlq_name,
            retention_period=cdk.Duration.days(14),
        )

        self.external_sns_event_queue = sqs.Queue(
            scope=self,
            id=env_base.get_construct_id(external_sns_event_name, "sns-event-queue"),
            queue_name=env_base.prefixed(external_sns_event_name, "sns-event-queue"),
            retention_period=external_sns_event_queue_retention_period,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=2,
                queue=self.external_sns_event_dlq,
            ),
            # visibility_timeout must be longer than the `timeout` of the `triggered_lambda_fn` or deploy will fail
            visibility_timeout=cdk.Duration.seconds(330),
        )

        # The *owning* AWS account of the SNS topic needs to give Subscribe permissions to the
        # AWS account where this stack will be deployed
        # See: https://docs.aws.amazon.com/sns/latest/dg/sns-send-message-to-sqs-cross-account.html
        self.external_sns_topic = sns.Topic.from_topic_arn(
            scope=self,
            id=env_base.get_construct_id(external_sns_event_name, "external-sns-topic"),
            topic_arn=external_sns_topic_arn,
        )

        # Useful reference:
        # https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_sns/Subscription.html#aws_cdk.aws_sns.Subscription
        self.external_sns_topic.add_subscription(
            SqsSubscription(
                queue=self.external_sns_event_queue,
                raw_message_delivery=True,
            )
        )

        triggered_lambda_fn.add_event_source(
            source=SqsEventSource(
                queue=self.external_sns_event_queue,
                report_batch_item_failures=True,
                filters=external_sns_event_queue_filters,
                enabled=sqs_event_source_enabled,
            )
        )
        self.external_sns_event_queue.grant_consume_messages(triggered_lambda_fn)

        # Alarm that fires if external_sns_event_queue fails delivery or if lambda fails to process
        # Further actions can be configured by accessing ExternalSnsTrigger.triggered_lambda_dlq_alarm resource
        # TODO: Revisit making alarms even more configurable in the future
        self.triggered_lambda_dlq_alarm = cloudwatch.Alarm(
            scope=self,
            id=env_base.get_construct_id(external_sns_event_name, "sns-event-dlq-alarm"),
            alarm_description=(
                f"Alarm if more than 1 message in {external_sns_event_dlq_name} in 10 minute period"
            ),
            metric=self.external_sns_event_dlq.metric_approximate_number_of_messages_visible(
                statistic=cloudwatch.Stats.MAXIMUM,
                period=cdk.Duration.minutes(10),
            ),
            evaluation_periods=1,
            threshold=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
