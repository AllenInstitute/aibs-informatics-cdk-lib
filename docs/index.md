# AIBS Informatics CDK Library

[![Build Status](https://github.com/AllenInstitute/aibs-informatics-cdk-lib/actions/workflows/build.yml/badge.svg)](https://github.com/AllenInstitute/aibs-informatics-cdk-lib/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AllenInstitute/aibs-informatics-cdk-lib/graph/badge.svg?token=5XCVULUK3E)](https://codecov.io/gh/AllenInstitute/aibs-informatics-cdk-lib)

---

## Overview

The AIBS Informatics CDK Library is a collection of AWS Cloud Development Kit (CDK) constructs and utilities designed to facilitate the deployment and management of cloud infrastructure for the Allen Institute for Brain Science. This library provides reusable and configurable components to streamline the development and deployment of cloud-based applications and services.

## Key Features

- **Reusable CDK Constructs**: Pre-built constructs for common AWS services
- **Environment-aware**: Built-in support for dev, test, and production environments
- **Step Function Fragments**: Reusable state machine fragments for batch processing and data sync
- **Monitoring**: CloudWatch dashboard and alarm constructs
- **Service Infrastructure**: Complete service compute and storage constructs

## Modules

### Constructs

- **Batch**: Constructs for setting up and managing AWS Batch environments, including job queues, compute environments, and monitoring
- **EFS**: Utilities and constructs for configuring and managing Elastic File System (EFS) resources
- **CloudWatch**: Tools for creating and managing CloudWatch dashboards and alarms
- **Service**: Constructs for defining compute resources, including Lambda functions and Batch compute environments
- **Step Functions**: Reusable fragments for AWS Step Functions, including batch job submission and data synchronization
- **Assets**: Definitions and utilities for managing code assets, including Lambda functions and Docker images

### Core

- **Stacks**: Base stack classes with environment configuration support
- **Stages**: Stage definitions for CDK pipelines
- **CI/CD**: Pipeline constructs for continuous integration and deployment
- **Project**: Configuration and utility functions for CDK projects

## Quick Start

```python
from aws_cdk import App, Environment
from aibs_informatics_core.env import EnvBase

from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack
from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import BatchInfrastructure

app = App()
env_base = EnvBase.from_env()

# Create a stack with environment awareness
stack = EnvBaseStack(
    app,
    "my-stack",
    env_base=env_base,
    env=Environment(account="123456789012", region="us-west-2")
)

# Add batch infrastructure
batch = BatchInfrastructure(
    stack,
    "batch-infra",
    env_base=env_base,
)
```

## Installation

```bash
pip install aibs-informatics-cdk-lib
```

Or with development dependencies:

```bash
pip install aibs-informatics-cdk-lib[dev]
```

## Contributing

Any and all PRs are welcome. Please see [CONTRIBUTING.md](https://github.com/AllenInstitute/aibs-informatics-cdk-lib/blob/main/CONTRIBUTING.md) for more information.

## License

This software is licensed under the Allen Institute Software License, which is the 2-clause BSD license plus a third clause that prohibits redistribution and use for commercial purposes without further permission. For more information, please visit [Allen Institute Terms of Use](https://alleninstitute.org/terms-of-use/).
