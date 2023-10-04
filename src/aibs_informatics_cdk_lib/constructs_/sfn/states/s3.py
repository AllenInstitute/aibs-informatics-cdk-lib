from typing import Any, Dict, List, Mapping, Optional, cast

import constructs
from aws_cdk import aws_stepfunctions as sfn


class S3Operation:
    @classmethod
    def put_object(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
        body: Any,
    ) -> sfn.Chain:
        init = sfn.Pass(
            scope,
            id + " PutObject Prep",
            parameters={
                "Location": {
                    "Bucket": bucket_name,
                    "Key": key,
                },
                "Body": body,
            },
        )

        state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters": {
                "Bucket.$": "$.Location.Bucket",
                "Key.$": "$.Location.Key",
                "Body.$": "$.Body",
            },
            "ResultPath": None,
            "OutputPath": "$.Location",
        }
        put_object = sfn.CustomState(scope, id + " PutObject API Call", state_json=state_json)

        return sfn.Chain.start(init).next(put_object)

    @classmethod
    def get_object(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
    ) -> sfn.Chain:
        init = sfn.Pass(
            scope,
            id + " GetObject Prep",
            parameters={
                "Bucket": bucket_name,
                "Key": key,
            },
        )

        state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters": {
                "Bucket.$": "$.Bucket",
                "Key.$": "$.Key",
            },
            "ResultSelector": {"Body.$": "$.Body"},
            "ResultPath": "$",
        }

        get_object = sfn.CustomState(scope, id + " GetObject API Call", state_json=state_json)
        return sfn.Chain.start(init).next(get_object)

    @classmethod
    def put_payload(
        cls,
        scope: constructs.Construct,
        id: str,
        payload: str,
        bucket_name: str,
        key: Optional[str] = None,
    ) -> sfn.Chain:
        """Puts a payload to s3 and returns the location of the payload in s3

        Args:
            scope (constructs.Construct): cdk construct
            id (str): id
            payload (str): payload
            bucket_name (str): bucket name
            key_prefix (Optional[str], optional): key prefix. Defaults to None.

        Returns:
            sfn.Parallel: parallel state
        """

        key = key or f"States.Format('{{}}/{{}}', $$.Execution.Name, States.UUID())"

        put_chain = S3Operation.put_object(scope, id, bucket_name, key, payload)
        return put_chain

    @classmethod
    def get_payload(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
    ) -> sfn.Chain:
        """Gets a payload from s3

        Args:
            scope (constructs.Construct): cdk construct
            id (str): id
            bucket_name (str): bucket name
            key (str): key

        Returns:
            sfn.Chain: chain
        """

        get_chain = S3Operation.get_object(scope, id, bucket_name, key)
        post = sfn.Pass(
            scope,
            id + " Post",
            parameters={"Payload.$": "States.StringToJson($.Body)"},
            output_path="$.Payload",
        )
        return get_chain.next(post)
