# Quick Start

This guide will help you get started with the AIBS Informatics CDK Library.

## Prerequisites

- Python 3.9+
- AWS CDK CLI installed (`npm install -g aws-cdk`)
- AWS credentials configured

## Creating Your First Stack

### 1. Set Up Your CDK App

Create a new CDK application:

```python
# app.py
from aws_cdk import App, Environment
from aibs_informatics_core.env import EnvBase

from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack

app = App()

# Define your environment
env_base = EnvBase.DEV  # or EnvBase.from_env() to read from environment

# Create a stack
stack = EnvBaseStack(
    app,
    "my-service-stack",
    env_base=env_base,
    env=Environment(
        account="123456789012",
        region="us-west-2"
    )
)

app.synth()
```

### 2. Add Constructs

Add constructs to your stack:

```python
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import BatchInfrastructure
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EfsConstruct

# Add EFS file system
efs = EfsConstruct(
    stack,
    "shared-storage",
    env_base=env_base,
    vpc=vpc,  # Your VPC construct
)

# Add Batch infrastructure
batch = BatchInfrastructure(
    stack,
    "batch-compute",
    env_base=env_base,
    vpc=vpc,
)
```

### 3. Use Step Function Fragments

Create state machines using reusable fragments:

```python
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import SubmitJobFragment
from aws_cdk import aws_stepfunctions as sfn

# Create a batch job submission fragment
submit_job = SubmitJobFragment(
    stack,
    "submit-job",
    env_base=env_base,
    job_queue=batch.job_queue,
    job_definition=batch.job_definition,
)

# create a state machine
state_machine = submit_job.to_state_machine("BatchJobStateMachine")
```

## Environment Configuration

The library supports multiple environments through `EnvBase`:

```python
from aibs_informatics_core.env import EnvBase, EnvType

# Development environment
dev_env = EnvBase.DEV

# Test environment
test_env = EnvBase.TEST

# Production environment
prod_env = EnvBase.PROD

# Custom environment
custom_env = EnvBase.from_type_and_label(EnvType.DEV, "my-feature")
```

## Synthesizing and Deploying

```bash
# Synthesize CloudFormation template
cdk synth

# Deploy the stack
cdk deploy
```

## Next Steps

- Explore the [User Guide](../user-guide/overview.md) for detailed usage information
- Browse the [API Reference](../api/index.md) for complete documentation
- Check out the [Constructs Guide](../user-guide/constructs.md) for available constructs
