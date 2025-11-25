from test.aibs_informatics_cdk_lib.base import CdkBaseTest
from typing import cast

from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchOperation

DEFAULT_EVALUATE_ON_EXIT = [
    {
        "Action": "RETRY",
        "OnStatusReason": "Task failed to start",
        "OnReason": "DockerTimeoutError*",
    },
    {
        "Action": "RETRY",
        "OnStatusReason": "Host EC2*",
    },
    {
        "Action": "EXIT",
        "OnStatusReason": "*",
    },
]

DEFAULT_BATCH_RETRY = [
    {
        "ErrorEquals": ["Batch.BatchException"],
        "IntervalSeconds": 3,
        "MaxAttempts": 5,
        "BackoffRate": 2,
    }
]


class TestBatchOperation(CdkBaseTest):
    def test__register_job_definition__with_literals(self):
        stack = self.get_dummy_stack("RegisterStack")

        chain = BatchOperation.register_job_definition(
            stack,
            "Register",
            command=["echo", "hello"],
            image="test-image",
            job_definition_name="test-def",
            environment={"FOO": "bar", "ABC": "xyz"},
            memory=2048,
            vcpus=2,
            gpu=1,
            job_role_arn="arn:aws:iam::123456789012:role/test",
            mount_points=[{"containerPath": "/data", "readOnly": False, "sourceVolume": "vol"}],
            volumes=[{"name": "vol", "host": {"sourcePath": "/host"}}],
            platform_capabilities=["EC2"],
        )

        start_state = cast(sfn.State, chain.start_state)
        start_state_json = start_state.to_state_json()
        assert start_state_json == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "JobDefinitionName.$": "States.Format('{}-{}', 'test-def', States.UUID())",
                "Type": "container",
                "ContainerProperties": {
                    "Image": "test-image",
                    "Command": ["echo", "hello"],
                    "Environment": [
                        {"Name": "ABC", "Value": "xyz"},
                        {"Name": "FOO", "Value": "bar"},
                    ],
                    "ResourceRequirements": [
                        {"Type": "GPU", "Value": "1"},
                        {"Type": "MEMORY", "Value": "2048"},
                        {"Type": "VCPU", "Value": "2"},
                    ],
                    "MountPoints": [
                        {
                            "ContainerPath": "/data",
                            "ReadOnly": False,
                            "SourceVolume": "vol",
                        }
                    ],
                    "Volumes": [
                        {
                            "Name": "vol",
                            "Host": {"SourcePath": "/host"},
                        }
                    ],
                    "JobRoleArn": "arn:aws:iam::123456789012:role/test",
                },
                "RetryStrategy": {
                    "Attempts": 5,
                    "EvaluateOnExit": DEFAULT_EVALUATE_ON_EXIT,
                },
                "PlatformCapabilities": ["EC2"],
            },
            "Next": "Register RegisterJobDefinition API Call",
        }

        end_state = cast(sfn.State, chain.end_states[0])
        end_state_json = end_state.to_state_json()
        assert end_state_json == {
            "End": True,
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:batch:registerJobDefinition",
            "Parameters": {
                "JobDefinitionName.$": "$.JobDefinitionName",
                "Type.$": "$.Type",
                "ContainerProperties.$": "$.ContainerProperties",
                "RetryStrategy.$": "$.RetryStrategy",
                "PlatformCapabilities.$": "$.PlatformCapabilities",
            },
            "ResultSelector": {
                "JobDefinitionArn.$": "$.JobDefinitionArn",
                "JobDefinitionName.$": "$.JobDefinitionName",
                "Revision.$": "$.Revision",
            },
            "ResultPath": "$",
            "OutputPath": "$",
            "Retry": DEFAULT_BATCH_RETRY,
        }

    def test__register_job_definition__accepts_reference_paths(self):
        stack = self.get_dummy_stack("RegisterRefsStack")

        chain = BatchOperation.register_job_definition(
            stack,
            "RegisterRefs",
            command="$.command",
            image="$.image",
            job_definition_name="$.name",
            environment="$.env",
            memory="$.memory",
            vcpus="$.vcpus",
            gpu="$.gpu",
        )

        start_state = cast(sfn.State, chain.start_state)
        start_state_json = start_state.to_state_json()
        assert start_state_json == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "JobDefinitionName.$": "States.Format('{}-{}', '$.name', States.UUID())",
                "Type": "container",
                "ContainerProperties": {
                    "Image.$": "$.image",
                    "Command.$": "$.command",
                    "Environment.$": "$.env",
                    "ResourceRequirements": [
                        {"Type": "GPU", "Value.$": "$.gpu"},
                        {"Type": "MEMORY", "Value.$": "$.memory"},
                        {"Type": "VCPU", "Value.$": "$.vcpus"},
                    ],
                },
                "RetryStrategy": {
                    "Attempts": 5,
                    "EvaluateOnExit": DEFAULT_EVALUATE_ON_EXIT,
                },
            },
            "Next": "RegisterRefs RegisterJobDefinition API Call",
        }

    def test__submit_job__includes_gpu_filter_state(self):
        stack = self.get_dummy_stack("SubmitStack")

        chain = BatchOperation.submit_job(
            stack,
            "Submit",
            job_name="job",
            job_definition="job-def",
            job_queue="job-queue",
            parameters={"foo": "bar"},
            command=["echo", "hello"],
            environment={"ALPHA": "beta"},
            memory=1024,
            vcpus=2,
            gpu=1,
        )

        start_state = cast(sfn.State, chain.start_state)
        start_state_json = start_state.to_state_json()
        assert start_state_json == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "JobName.$": "States.Format('job-{}', States.UUID())",
                "JobDefinition": "job-def",
                "JobQueue": "job-queue",
                "Parameters": {
                    "Foo": "bar",
                },
                "ContainerOverrides": {
                    "Command": ["echo", "hello"],
                    "Environment": [{"Name": "ALPHA", "Value": "beta"}],
                    "ResourceRequirements": [
                        {"Type": "GPU", "Value": "1"},
                        {"Type": "MEMORY", "Value": "1024"},
                        {"Type": "VCPU", "Value": "2"},
                    ],
                },
            },
            "Next": "Submit SubmitJob Filter Resource Requirements",
        }

        filter_state_construct = stack.node.try_find_child(
            "Submit SubmitJob Filter Resource Requirements"
        )
        assert filter_state_construct is not None
        filter_state = cast(sfn.Pass, filter_state_construct)
        assert filter_state.to_state_json() == {
            "Type": "Pass",
            "ResultPath": "$.ContainerOverrides.ResourceRequirements",
            "InputPath": "$.ContainerOverrides.ResourceRequirements[?(@.Value != 0 && @.Value != '0')]",
            "Next": "Submit SubmitJob API Call",
        }

        end_state = cast(sfn.State, chain.end_states[0])
        end_state_json = end_state.to_state_json()
        assert end_state_json == {
            "End": True,
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
                "JobName.$": "$.JobName",
                "JobDefinition.$": "$.JobDefinition",
                "JobQueue.$": "$.JobQueue",
                "Parameters.$": "$.Parameters",
                "ContainerOverrides.$": "$.ContainerOverrides",
            },
            "ResultSelector": {
                "JobName.$": "$.JobName",
                "JobId.$": "$.JobId",
                "JobArn.$": "$.JobArn",
            },
            "ResultPath": "$",
            "OutputPath": "$",
            "Retry": DEFAULT_BATCH_RETRY,
        }

    def test__submit_job__without_gpu_skips_filter_state(self):
        stack = self.get_dummy_stack("SubmitNoGpuStack")

        chain = BatchOperation.submit_job(
            stack,
            "SubmitNoGpu",
            job_name="job",
            job_definition="job-def",
            job_queue="job-queue",
            memory=2048,
            vcpus=2,
        )

        start_state = cast(sfn.State, chain.start_state)
        start_state_json = start_state.to_state_json()
        assert start_state_json == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {
                "JobName.$": "States.Format('job-{}', States.UUID())",
                "JobDefinition": "job-def",
                "JobQueue": "job-queue",
                "Parameters": {},
                "ContainerOverrides": {
                    "Environment": [],
                    "ResourceRequirements": [
                        {"Type": "MEMORY", "Value": "2048"},
                        {"Type": "VCPU", "Value": "2"},
                    ],
                },
            },
            "Next": "SubmitNoGpu SubmitJob API Call",
        }

        assert (
            stack.node.try_find_child("SubmitNoGpu SubmitJob Filter Resource Requirements") is None
        )

    def test__deregister_job_definition(self):
        stack = self.get_dummy_stack("DeregisterStack")

        chain = BatchOperation.deregister_job_definition(
            stack,
            "Deregister",
            job_definition="my-job:1",
        )

        start_state = cast(sfn.State, chain.start_state)
        start_state_json = start_state.to_state_json()
        assert start_state_json == {
            "Type": "Pass",
            "ResultPath": "$",
            "Parameters": {"JobDefinition": "my-job:1"},
            "Next": "Deregister DeregisterJobDefinition API Call",
        }

        end_state = cast(sfn.State, chain.end_states[0])
        end_state_json = end_state.to_state_json()
        assert end_state_json == {
            "End": True,
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:batch:deregisterJobDefinition",
            "Parameters": {"JobDefinition.$": "$.JobDefinition"},
            "ResultPath": "$",
            "OutputPath": "$",
            "Retry": DEFAULT_BATCH_RETRY,
        }
