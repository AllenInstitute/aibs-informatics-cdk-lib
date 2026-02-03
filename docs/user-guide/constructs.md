# Constructs Guide

This guide covers the CDK constructs available in the library.

## Base Constructs

All constructs inherit from base classes that provide environment awareness:

```python
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins

class MyConstruct(Construct, EnvBaseConstructMixins):
    def __init__(self, scope, id, env_base: EnvBase, **kwargs):
        super().__init__(scope, id)
        self.env_base = env_base
```

## Batch Constructs

### BatchInfrastructure

Complete AWS Batch infrastructure setup:

```python
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import BatchInfrastructure

batch = BatchInfrastructure(
    stack,
    "batch",
    env_base=env_base,
    vpc=vpc,
)
```

### Batch Instance Types

Utilities for selecting EC2 instance types:

```python
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import get_instance_types

instances = get_instance_types(
    memory_min=8192,
    vcpu_min=4,
)
```

## EFS Constructs

### EfsConstruct

Elastic File System with access points:

```python
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EfsConstruct

efs = EfsConstruct(
    stack,
    "efs",
    env_base=env_base,
    vpc=vpc,
)
```

## Step Function Constructs

### State Machine Fragments

Reusable state machine fragments:

```python
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.batch import SubmitJobFragment

fragment = SubmitJobFragment(
    stack,
    "submit-job",
    env_base=env_base,
    job_queue=job_queue,
    job_definition=job_definition,
)
```

### Custom States

Pre-built Step Function states:

```python
from aibs_informatics_cdk_lib.constructs_.sfn.states.batch import BatchJobState
from aibs_informatics_cdk_lib.constructs_.sfn.states.s3 import S3GetObjectState
```

## Service Constructs

### ServiceCompute

Combined compute resources for services:

```python
from aibs_informatics_cdk_lib.constructs_.service.compute import ServiceCompute

compute = ServiceCompute(
    stack,
    "compute",
    env_base=env_base,
    vpc=vpc,
)
```

### ServiceStorage

Storage infrastructure for services:

```python
from aibs_informatics_cdk_lib.constructs_.service.storage import ServiceStorage

storage = ServiceStorage(
    stack,
    "storage",
    env_base=env_base,
)
```

## CloudWatch Constructs

### Monitoring Dashboards

Create CloudWatch dashboards:

```python
from aibs_informatics_cdk_lib.constructs_.cw.dashboard import MonitoringDashboard

dashboard = MonitoringDashboard(
    stack,
    "dashboard",
    env_base=env_base,
    dashboard_name="my-service",
)
```

## Asset Constructs

### Code Assets

Manage Lambda function and Docker image assets:

```python
from aibs_informatics_cdk_lib.constructs_.assets import LambdaAsset, DockerImageAsset

lambda_asset = LambdaAsset(
    stack,
    "lambda-code",
    path="./lambda",
)

docker_asset = DockerImageAsset(
    stack,
    "docker-image",
    directory="./docker",
)
```

## Best Practices

1. **Always pass `env_base`**: All constructs should receive the environment base
2. **Use construct IDs consistently**: Follow naming conventions
3. **Leverage mixins**: Use `EnvBaseConstructMixins` for common functionality
4. **Tag resources**: Use the built-in tagging support
