from typing import Any, Optional

import constructs
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.utils import convert_reference_paths_in_mapping


class S3Operation:
    @classmethod
    def put_object(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
        body: Any,
        result_path: Optional[str] = "$",
        output_path: Optional[str] = "$",
    ) -> sfn.Chain:
        """Create a chain to put a body of text to S3

        This chain consists of two states:
            1. Pass state (resolving references and resctructuring inputs)
            2. API Call to put object.

        The context at the end state of the chain should contain the following fields:
            - Bucket
            - Key

        All parameters can be either a reference (e.g. $.path.to.my.value)
        or an explicit value.

        Examples:
            Context:
                {}
            Definition:
                S3Operation.put_object(..., bucket_name="bucket", key="key", body="body")
            Result:
                # text "body" is put at s3://bucket/key
                {"Bucket": "bucket", "Key": "key"}

            Context:
                {"bucket": "woah", "key": "wait/what", "nested": {"a": "b"}}
            Definition:
                S3Operation.put_object(..., bucket_name="$.bucket", key="$.key", body="$.nested")
            Result:
                # text '{"a": "b"}' is put at s3://woah/wait/what
                {"Bucket": "woah", "Key": "wait/what"}

        Args:
            scope (constructs.Construct): scope construct
            id (str): An ID prefix
            bucket_name (str): explicit or reference to name of bucket
            key (str): explicit or reference to name of key
            body (Any): explicit or reference to body to upload

        Returns:
            sfn.Chain
        """
        init = sfn.Pass(
            scope,
            id + " PutObject Prep",
            parameters=convert_reference_paths_in_mapping(
                {
                    "Bucket": bucket_name,
                    "Key": key,
                    "Body": body,
                }
            ),
            result_path=result_path or "$",
        )

        state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters": {
                "Bucket.$": f"{result_path or '$'}.Bucket",
                "Key.$": f"{result_path or '$'}.Key",
                "Body.$": f"{result_path or '$'}.Body",
            },
            # "ResultSelector": {
            #     "ETag.$": "$.Bucket",
            #     "Key.$": "$.Key",
            # },
            "ResultPath": sfn.JsonPath.DISCARD,
        }
        put_object = sfn.CustomState(scope, id + " PutObject API Call", state_json=state_json)

        end = sfn.Pass(
            scope,
            id + " PutObject Post",
            parameters=convert_reference_paths_in_mapping(
                {
                    "Bucket": f"{result_path or '$'}.Bucket",
                    "Key": f"{result_path or '$'}.Key",
                }
            ),
            result_path=result_path,
            output_path=output_path,
        )

        return sfn.Chain.start(init).next(put_object).next(end)

    @classmethod
    def get_object(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
        result_path: Optional[str] = "$",
        output_path: Optional[str] = "$",
    ) -> sfn.Chain:
        """Creates a chain to get a body of text from S3

        This chain consists of two states:
            1. Pass state (resolving references and resctructuring inputs)
            2. API Call to get object.

        The context at the end state of the chain should contain the following fields:
            - Body
            - Bucket
            - Key

        All parameters can be either a reference (e.g. $.path.to.my.value)
        or an explicit value.

        Examples:
            Context:
                {}
            Definition:
                S3Operation.get_object(..., bucket_name="bucket", key="key")
            Result:
                # text "body" is fetched from s3://bucket/key
                {"Body": "body"}

            Context:
                {"bucket": "woah", "key": "wait/what"}
            Definition:
                S3Operation.get_object(..., bucket_name="$.bucket", key="$.key")
            Result:
                # text '{"a": "b"}' is fetched from s3://woah/wait/what
                {"Body": '{"a": "b"}'}

        Args:
            scope (constructs.Construct): scope construct
            id (str): An ID prefix
            bucket_name (str): explicit or reference to name of bucket
            key (str): explicit or reference to name of key

        Returns:
            sfn.Chain
        """
        init = sfn.Pass(
            scope,
            id + " GetObject Prep",
            parameters=convert_reference_paths_in_mapping(
                {
                    "Bucket": bucket_name,
                    "Key": key,
                }
            ),
            result_path=result_path or "$",
        )

        if result_path:
            bucket_path = f"{result_path}.Bucket"
            key_path = f"{result_path}.Key"
        else:
            bucket_path = "$.Bucket"
            key_path = "$.Key"

        state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters": {
                "Bucket.$": bucket_path,
                "Key.$": key_path,
            },
            "ResultSelector": {
                "Body.$": "$.Body",
            },
            "ResultPath": result_path,
            "OutputPath": output_path,
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
        result_path: Optional[str] = "$",
        output_path: Optional[str] = "$",
    ) -> sfn.Chain:
        """Puts a payload to s3 and returns the location of the payload in s3

        All parameters can be either a reference (e.g. $.path.to.my.value)
        or an explicit value.

        Examples:
            Context:
                {"a": "b"}
            Definition:
                S3Operation.put_payload(..., bucket_name="bucket", payload="$")
            Result:
                # text '{"a": "b"}' is written to s3://bucket/1234.../...1234
                {"Bucket": "bucket", "Key": "1234.../...1234"}

            Context:
                {"bucket": "woah", "key": "wait/what", "data": {"a": "b"}}
            Definition:
                S3Operation.put_payload(..., bucket_name="$.bucket", payload="$.data")
            Result:
                # text '{"a": "b"}' is written to s3://woah/wait/what
                {"Bucket": "woah", "Key": "wait/what"}


        Args:
            scope (constructs.Construct): cdk construct
            id (str): id
            payload (str): explicit value or reference path in the context object (e.g. "$", "$.path")
            bucket_name (str): explicit value or reference path for bucket name
            key (Optional[str], optional): explicit value or reference path for key.
                If not provided, the following Key is generated:
                    {$$.Execution.Name}/{UUID}

        Returns:
            sfn.Chain
        """
        key = key or sfn.JsonPath.format(
            f"{{}}/{{}}", sfn.JsonPath.execution_name, sfn.JsonPath.uuid()
        )

        put_chain = S3Operation.put_object(
            scope, id, bucket_name, key, payload, result_path, output_path
        )
        return put_chain

    @classmethod
    def get_payload(
        cls,
        scope: constructs.Construct,
        id: str,
        bucket_name: str,
        key: str,
        result_path: Optional[str] = "$",
    ) -> sfn.Chain:
        """Gets a payload from s3

        This chain fetches object and then passes the body through a json parser

        The resulting payload will be stored to path specified by result_path

        Examples:
            Use Case #1 - No result path
                Context:
                    {"bucket": "woah", "key": "wait/what"}
                Definition:
                    S3Operation.get_payload(..., bucket_name="$.bucket", key="$.key")
                Result:
                    # text '{"a": "b"}' is fetched from s3://woah/wait/what
                    {"a": "b"}

            Use Case #2 - result path specified
                Context:
                    {"bucket": "woah", "key": "wait/what"}
                Definition:
                    S3Operation.get_payload(
                        ..., bucket_name="$.bucket", key="$.key", result_path="$.payload"
                    )
                Result:
                    # text '{"a": "b"}' is fetched from s3://woah/wait/what
                    {
                        "bucket": "woah",
                        "key": "wait/what",
                        "payload": {"a": "b"}
                    }

        Args:
            scope (constructs.Construct): cdk construct
            id (str): id
            bucket_name (str): bucket name. Can be a reference path
            key (str): key name. Can be a reference path
            result_path (Optional[str], optional): path to store the payload. Defaults to "$".

        Returns:
            sfn.Chain: chain
        """

        get_chain = S3Operation.get_object(scope, id, bucket_name, key, result_path)
        post = sfn.Pass(
            scope,
            id + " Post",
            parameters={
                "Payload": sfn.JsonPath.string_to_json(
                    sfn.JsonPath.string_at(f"{result_path}.Body")
                )
            },
            result_path=result_path,
        )
        restructure = sfn.Pass(
            scope,
            id + " Restructure",
            input_path=f"{result_path}.Payload",
            result_path=result_path,
        )

        return get_chain.next(post).next(restructure)
