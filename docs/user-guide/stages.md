# Stages Guide

This guide covers using CDK Pipeline stages for deployment.

## EnvBaseStage

The base stage class for environment-aware deployments:

```python
from aibs_informatics_cdk_lib.stages.base import EnvBaseStage

class MyServiceStage(EnvBaseStage):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_base: EnvBase,
        **kwargs
    ):
        super().__init__(scope, id, env_base=env_base, **kwargs)
        
        # Add stacks to this stage
        MyServiceStack(self, "service", env_base=env_base)
```

## Using Stages with Pipelines

Stages are designed for use with CDK Pipelines:

```python
from aws_cdk.pipelines import CodePipeline, ShellStep

# Create the pipeline
pipeline = CodePipeline(
    self,
    "Pipeline",
    synth=ShellStep(
        "Synth",
        commands=["npm ci", "cdk synth"],
    ),
)

# Add stages for different environments
pipeline.add_stage(
    MyServiceStage(
        self, "Dev",
        env_base=EnvBase.DEV,
    )
)

pipeline.add_stage(
    MyServiceStage(
        self, "Test",
        env_base=EnvBase.TEST,
    ),
    pre=[
        # Add manual approval for test
        ManualApprovalStep("Approve-Test"),
    ],
)

pipeline.add_stage(
    MyServiceStage(
        self, "Prod",
        env_base=EnvBase.PROD,
    ),
    pre=[
        # Add manual approval for production
        ManualApprovalStep("Approve-Prod"),
    ],
)
```

## Stage Organization

### Multiple Stacks per Stage

```python
class MyServiceStage(EnvBaseStage):
    def __init__(self, ...):
        super().__init__(...)
        
        # Infrastructure stack
        infra = InfraStack(self, "infra", env_base=self.env_base)
        
        # Application stack
        app = AppStack(
            self, "app",
            env_base=self.env_base,
            vpc=infra.vpc,
        )
        app.add_dependency(infra)
```

### Conditional Resources

```python
class MyServiceStage(EnvBaseStage):
    def __init__(self, ...):
        super().__init__(...)
        
        # Always create the main stack
        main = MainStack(self, "main", env_base=self.env_base)
        
        # Only create monitoring in non-dev environments
        if self.env_base.is_prod:
            MonitoringStack(self, "monitoring", env_base=self.env_base)
```


## Best Practices

1. **One Stage Per Environment**: Create separate stages for dev, test, and prod
2. **Use Manual Approvals**: Add approval steps before production deployments
3. **Stack Dependencies**: Properly define dependencies between stacks within a stage
4. **Environment Isolation**: Use `env_base` to ensure resource isolation
