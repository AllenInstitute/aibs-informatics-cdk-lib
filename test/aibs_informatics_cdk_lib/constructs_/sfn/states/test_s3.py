from test.aibs_informatics_cdk_lib.base import CdkBaseTest
from typing import cast

from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3Operation


class TestS3Operation(CdkBaseTest):
    def test_put_object_with_literals(self):
        stack = self.get_dummy_stack("PutLiteralStack")

        chain = S3Operation.put_object(
            stack,
            "PutLiteral",
            bucket_name="my-bucket",
            key="path/key.txt",
            body="hello world",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket": "my-bucket",
                "Key": "path/key.txt",
                "Body": "hello world",
            },
            "Next": "PutLiteral PutObject API Call",
        }

        custom_state = cast(
            sfn.CustomState, stack.node.try_find_child("PutLiteral PutObject API Call")
        )
        assert custom_state is not None
        custom_state_json = custom_state.to_state_json()
        assert custom_state_json == {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters": {
                "Bucket.$": "$.Bucket",
                "Key.$": "$.Key",
                "Body.$": "$.Body",
            },
            "ResultPath": sfn.JsonPath.DISCARD,
            "Next": "PutLiteral PutObject Post",
        }

        end_state = cast(sfn.State, chain.end_states[0])
        assert end_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket.$": "$.Bucket",
                "Key.$": "$.Key",
            },
            "OutputPath": "$",
            "End": True,
        }

    def test_put_object_with_reference_paths(self):
        stack = self.get_dummy_stack("PutRefsStack")

        chain = S3Operation.put_object(
            stack,
            "PutRefs",
            bucket_name="$.bucket",
            key="$.key",
            body="$.body",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket.$": "$.bucket",
                "Key.$": "$.key",
                "Body.$": "$.body",
            },
            "Next": "PutRefs PutObject API Call",
        }

    def test_get_object_flow(self):
        stack = self.get_dummy_stack("GetStack")

        chain = S3Operation.get_object(
            stack,
            "Get",
            bucket_name="my-bucket",
            key="file.txt",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket": "my-bucket",
                "Key": "file.txt",
            },
            "Next": "Get GetObject API Call",
        }

        custom_state = cast(sfn.CustomState, stack.node.try_find_child("Get GetObject API Call"))
        assert custom_state is not None
        custom_state_json = custom_state.to_state_json()
        assert custom_state_json == {
            "End": True,
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters": {
                "Bucket.$": "$.Bucket",
                "Key.$": "$.Key",
            },
            "ResultSelector": {
                "Body.$": "$.Body",
            },
            "ResultPath": "$",
            "OutputPath": "$",
        }

    def test_put_payload_generates_key_when_missing(self):
        stack = self.get_dummy_stack("PutPayloadGenStack")

        chain = S3Operation.put_payload(
            stack,
            "PutPayloadGen",
            payload="$.data",
            bucket_name="bucket",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket": "bucket",
                "Key.$": "States.Format('{}/{}', $$.Execution.Name, States.UUID())",
                "Body.$": "$.data",
            },
            "Next": "PutPayloadGen PutObject API Call",
        }

    def test_put_payload_respects_explicit_key(self):
        stack = self.get_dummy_stack("PutPayloadKeyStack")

        chain = S3Operation.put_payload(
            stack,
            "PutPayloadKey",
            payload='{"foo": "bar"}',
            bucket_name="bucket",
            key="explicit/key.json",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket": "bucket",
                "Key": "explicit/key.json",
                "Body": '{"foo": "bar"}',
            },
            "Next": "PutPayloadKey PutObject API Call",
        }

    def test_get_payload_parses_string_body(self):
        stack = self.get_dummy_stack("GetPayloadStack")

        chain = S3Operation.get_payload(
            stack,
            "GetPayload",
            bucket_name="bucket",
            key="file.json",
        )

        start_state = cast(sfn.State, chain.start_state)
        assert start_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Bucket": "bucket",
                "Key": "file.json",
            },
            "Next": "GetPayload GetObject API Call",
        }

        custom_state = cast(
            sfn.CustomState, stack.node.try_find_child("GetPayload GetObject API Call")
        )
        assert custom_state is not None
        assert custom_state.to_state_json() == {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters": {
                "Bucket.$": "$.Bucket",
                "Key.$": "$.Key",
            },
            "ResultSelector": {
                "Body.$": "$.Body",
            },
            "ResultPath": "$",
            "OutputPath": "$",
            "Next": "GetPayload Post",
        }

        post_state = cast(sfn.Pass, stack.node.try_find_child("GetPayload Post"))
        assert post_state is not None
        assert post_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "Payload.$": "States.StringToJson($.Body)",
            },
            "Next": "GetPayload Restructure",
        }

        restructure_state = cast(sfn.State, chain.end_states[0])
        assert restructure_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$",
            "InputPath": "$.Payload",
            "End": True,
        }
