# AIBS Informatics CDK Library

[![Build Status](https://github.com/AllenInstitute/aibs-informatics-cdk-lib/actions/workflows/build.yml/badge.svg)](https://github.com/AllenInstitute/aibs-informatics-cdk-lib/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AllenInstitute/aibs-informatics-cdk-lib/graph/badge.svg?token=5XCVULUK3E)](https://codecov.io/gh/AllenInstitute/aibs-informatics-cdk-lib)

---

## Overview

The AIBS Informatics CDK Library is a collection of AWS Cloud Development Kit (CDK) constructs and utilities designed to facilitate the deployment and management of cloud infrastructure for the Allen Institute for Brain Science. This library includes constructs for managing AWS Batch environments, Elastic File System (EFS) configurations, CloudWatch dashboards, and more. It aims to provide reusable and configurable components to streamline the development and deployment of cloud-based applications and services.

### Modules

- **Batch**: Constructs for setting up and managing AWS Batch environments, including job queues, compute environments, and monitoring.
- **EFS**: Utilities and constructs for configuring and managing Elastic File System (EFS) resources.
- **CloudWatch**: Tools for creating and managing CloudWatch dashboards and alarms.
- **Service Compute**: Constructs for defining compute resources, including Lambda functions and Batch compute environments.
- **State Machine Fragments**: Reusable fragments for AWS Step Functions, including batch job submission and data synchronization.
- **Assets**: Definitions and utilities for managing code assets, including Lambda functions and Docker images.
- **Core**: Base constructs and utilities used across the library, including environment configurations and common IAM policies.

## Contributing

Any and all PRs are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Licensing

This software is licensed under the Allen Institute Software License, which is the 2-clause BSD license plus a third clause that prohibits redistribution and use for commercial purposes without further permission. For more information, please visit [Allen Institute Terms of Use](https://alleninstitute.org/terms-of-use/).