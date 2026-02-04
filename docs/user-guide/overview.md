# Overview

The AIBS Informatics CDK Library provides a comprehensive set of AWS CDK constructs designed for building cloud infrastructure at the Allen Institute for Brain Science.

## Architecture

The library is organized into several key modules:

```
aibs_informatics_cdk_lib/
├── common/          # Shared utilities and AWS helpers
├── constructs_/     # CDK constructs for AWS services
├── stacks/          # Stack base classes
├── stages/          # CDK Pipeline stage definitions
├── cicd/            # CI/CD pipeline constructs
└── project/         # Project configuration utilities
```

## Key Concepts

### Environment Base (EnvBase)

All constructs in this library are environment-aware through `EnvBase`:

```python
from aibs_informatics_core.env import EnvBase, EnvType

# EnvBase provides:
# - Environment type (DEV, TEST, PROD)
# - Resource naming conventions
# - Tagging support
```

### Construct Mixins

The `EnvBaseConstructMixins` class provides common functionality:

- Resource naming with environment prefixes
- Stack-level utilities
- Tag management
- IAM policy helpers

### Constructs

Constructs are the building blocks for your infrastructure:

- **Base constructs**: Foundation classes with environment support
- **Service constructs**: Higher-level constructs for complete services
- **Fragment constructs**: Reusable Step Function fragments

## Design Principles

1. **Environment Isolation**: Resources are named and tagged based on environment
2. **Reusability**: Constructs are designed to be composable and reusable
3. **Type Safety**: Full type hints for IDE support and validation
4. **Best Practices**: Constructs implement AWS best practices by default

## Module Overview

| Module | Description |
|--------|-------------|
| `common` | AWS utilities, IAM helpers, and build tools |
| `constructs_` | CDK constructs for various AWS services |
| `stacks` | Base stack classes with environment configuration |
| `stages` | Pipeline stage definitions |
| `cicd` | CI/CD pipeline constructs |
| `project` | Project configuration and utilities |

## Next Steps

- [Constructs Guide](constructs.md) - Learn about available constructs
- [Stacks Guide](stacks.md) - Creating and configuring stacks
- [Stages Guide](stages.md) - Using pipeline stages
