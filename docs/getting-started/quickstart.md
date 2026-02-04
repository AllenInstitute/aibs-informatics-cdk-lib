# Quick Start

This guide will help you get started with the AIBS Informatics CDK Library.

## Prerequisites

- Python 3.10+
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


class MyStack(EnvBaseStack):
    def __init__(self, scope: App, id: str, *, env_base: EnvBase, **kwargs) -> None:
        super().__init__(scope, id, env_base=env_base, **kwargs)
        # Add your constructs here



# Create a stack
stack = MyStack(
    app,
    "my-service-stack",
    env_base=EnvBase("dev-myservice"),
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
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import Batch
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EFSEcosystem

# Add EFS ecosystem (file system with access points)
efs_ecosystem = EFSEcosystem(
    stack,
    "shared-storage",
    env_base=env_base,
    file_system_name="shared",
    vpc=vpc,  # Your VPC construct
)

# Add Batch infrastructure
batch = Batch(
    stack,
    "batch-compute",
    env_base=env_base,
    vpc=vpc,
)
```

### 3. Use Step Function Fragments

Create state machines using reusable fragments:

```python
from aibs_informatics_cdk_lib.constructs_.sfn.fragments import SubmitJobFragment

# Create a batch job submission fragment
submit_job = SubmitJobFragment(
    stack,
    "submit-job",
    env_base=env_base,
    job_queue=batch.job_queue,
    job_definition=batch.job_definition,
)

# Create a state machine from the fragment
state_machine = submit_job.to_state_machine("BatchJobStateMachine")
```

## Environment Configuration

The library supports multiple environments through `EnvBase`:

```python
from aibs_informatics_core.env import EnvBase, EnvType

# Development environment
dev_env = EnvBase(EnvType.DEV)

# Test environment
test_env = EnvBase(EnvType.TEST)

# Production environment
prod_env = EnvBase(EnvType.PROD)

# Custom environment
custom_env = EnvBase.from_type_and_label(EnvType.DEV, "my-feature")
```

## Next Steps

- Explore the [User Guide](../user-guide/overview.md) for detailed usage information
- Browse the [API Reference](../api/index.md) for complete documentation
- Check out the [Constructs Guide](../user-guide/constructs.md) for available constructs
