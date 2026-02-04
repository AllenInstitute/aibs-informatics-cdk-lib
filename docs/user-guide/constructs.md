# Popular Constructs Guide

This guide covers some of the CDK constructs available in the library.

## Base Constructs

All constructs inherit from base classes that provide environment awareness:

```python
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct

class MyConstruct(EnvBaseConstruct):
    def __init__(self, scope, id, env_base: EnvBase, **kwargs):
        super().__init__(scope, id, env_base, **kwargs)
```

## Batch Constructs

### Batch

AWS Batch infrastructure setup with shared instance roles and security groups:

```python
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import Batch

batch = Batch(
    stack,
    "batch",
    env_base=env_base,
    vpc=vpc,
)
```

## EFS Constructs

### EnvBaseFileSystem

Elastic File System with access points:

```python
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EnvBaseFileSystem

efs = EnvBaseFileSystem(
    stack,
    "efs",
    env_base=env_base,
    vpc=vpc,
)
```

### EFSEcosystem

Complete EFS ecosystem with file system and access points:

```python
from aibs_informatics_cdk_lib.constructs_.efs.file_system import EFSEcosystem

efs_ecosystem = EFSEcosystem(
    stack,
    "efs-ecosystem",
    env_base=env_base,
    file_system_name="shared",
    vpc=vpc,
)
```

## Step Function Constructs

### State Machine Fragments

Reusable state machine fragments:

```python
from aibs_informatics_cdk_lib.constructs_.sfn.fragments import SubmitJobFragment

fragment = SubmitJobFragment(
    stack,
    "submit-job",
    env_base=env_base,
    job_queue=job_queue,
    job_definition=job_definition,
)
```

## Service Constructs

### BatchCompute

Combined compute resources for services with on-demand, spot, and Fargate environments:

```python
from aibs_informatics_cdk_lib.constructs_.service.compute import BatchCompute

compute = BatchCompute(
    stack,
    "compute",
    env_base=env_base,
    vpc=vpc,
    batch_name="my-batch",
)
```

### LambdaCompute

Lambda-optimized Batch compute with small, medium, and large instance configurations:

```python
from aibs_informatics_cdk_lib.constructs_.service.compute import LambdaCompute

compute = LambdaCompute(
    stack,
    "lambda-compute",
    env_base=env_base,
    vpc=vpc,
    batch_name="lambda-batch",
)
```

### Storage

Storage infrastructure for services:

```python
from aibs_informatics_cdk_lib.constructs_.service.storage import Storage

storage = Storage(
    stack,
    "storage",
    env_base=env_base,
    name="my-storage",
    vpc=vpc,
)
```

## CloudWatch Constructs

### EnhancedDashboard

Create CloudWatch dashboards:

```python
from aibs_informatics_cdk_lib.constructs_.cw import EnhancedDashboard

dashboard = EnhancedDashboard(
    stack,
    "dashboard",
    env_base=env_base,
    dashboard_name="my-service",
)
```

## Asset Constructs

### CodeAsset

Manage code assets for Lambda functions:

```python
from aibs_informatics_cdk_lib.constructs_.assets.code_asset import CodeAsset

code_asset = CodeAsset(
    name="my-lambda",
    path="./lambda",
)
```

## Best Practices

1. **Always pass `env_base`**: All constructs should receive the environment base
2. **Use construct IDs consistently**: Follow naming conventions
3. **Leverage base constructs**: Use `EnvBaseConstruct` for common functionality
4. **Tag resources**: Use the built-in tagging support
