# Stacks Guide

This guide covers creating and configuring CDK stacks.

## EnvBaseStack

The base stack class for all environment-aware stacks:

```python
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack

class MyServiceStack(EnvBaseStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        **kwargs
    ):
        super().__init__(scope, id, env_base=env_base, **kwargs)
        
        # Add your constructs here
```

## Stack Properties

`EnvBaseStack` provides several useful properties:

```python
class MyStack(EnvBaseStack):
    def __init__(self, ...):
        super().__init__(...)
        
        # Access AWS account and region
        print(self.aws_account)
        print(self.aws_region)
        
        # Environment-aware removal policy
        # DEV: DESTROY, PROD: RETAIN
        bucket = s3.Bucket(
            self, "bucket",
            removal_policy=self.removal_policy,
        )
```

## Stack Tags

Stacks automatically receive environment tags:

```python
# Automatic tags include:
# - env_base: The environment base string
# - Additional custom tags from construct_tags property
```

## Stack Dependencies

Manage dependencies between stacks:

```python
from aibs_informatics_cdk_lib.stacks.base import add_stack_dependencies

# Create stacks
infra_stack = InfraStack(app, "infra", env_base=env_base)
service_stack = ServiceStack(app, "service", env_base=env_base)

# Service depends on infra
add_stack_dependencies(infra_stack, [service_stack])
```

## Getting All Stacks

Retrieve all stacks in an app:

```python
from aibs_informatics_cdk_lib.stacks.base import get_all_stacks

all_stacks = get_all_stacks(app)
```

## Stack Organization

### Single Stack

For simple applications:

```python
app = App()
stack = MyServiceStack(app, "my-service", env_base=env_base)
app.synth()
```

### Multiple Stacks

For complex applications with separation of concerns:

```python
app = App()

# Infrastructure stack
infra = InfrastructureStack(app, "infra", env_base=env_base)

# Compute stack (depends on infra)
compute = ComputeStack(
    app, "compute",
    env_base=env_base,
    vpc=infra.vpc,
)
compute.add_dependency(infra)

# Application stack (depends on compute)
application = ApplicationStack(
    app, "app",
    env_base=env_base,
    batch_job_queue=compute.job_queue,
)
application.add_dependency(compute)

app.synth()
```

## Environment-Specific Configuration

Use `EnvBase` properties for environment-specific behavior:

```python
class MyStack(EnvBaseStack):
    def __init__(self, ...):
        super().__init__(...)
        
        if self.is_prod:
            # Production-specific configuration
            instance_count = 3
            enable_multi_az = True
        elif self.is_test:
            # Test-specific configuration
            instance_count = 2
            enable_multi_az = True
        else:
            # Development configuration
            instance_count = 1
            enable_multi_az = False
```
