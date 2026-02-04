# API Reference

Welcome to the AIBS Informatics CDK Library API Reference.

## Module Overview

### Common

Shared utilities and AWS helpers used across constructs.

- [AWS Utilities](common/aws.md) - IAM, S3, and other AWS helpers
- [Build Utilities](common/build.md) - Build and asset helpers
- [Git Utilities](common/git.md) - Git operations for CI/CD

### Constructs

CDK constructs for various AWS services.

- [Base](constructs/base.md) - Base construct classes and mixins
- [Assets](constructs/assets.md) - Code and Docker assets
- [Batch](constructs/batch.md) - AWS Batch infrastructure
- [CloudWatch](constructs/cw.md) - Monitoring and dashboards
- [DynamoDB](constructs/dynamodb.md) - DynamoDB tables
- [EC2](constructs/ec2.md) - EC2 instances and networking
- [EFS](constructs/efs.md) - Elastic File System
- [S3](constructs/s3.md) - S3 buckets and policies
- [Service](constructs/service.md) - Service infrastructure
- [SSM](constructs/ssm.md) - Systems Manager parameters
- [Step Functions](constructs/sfn.md) - State machine constructs

### Stacks

Stack base classes with environment configuration.

- [Base](stacks/base.md) - EnvBaseStack and utilities

### Stages

Pipeline stage definitions.

- [Base](stages/base.md) - EnvBaseStage

### CI/CD

Pipeline constructs for continuous deployment.

- [Pipeline](cicd/pipeline.md) - CDK Pipeline constructs
- [Target](cicd/target.md) - Deployment targets

### Project

Configuration and utility functions.

- [Config](project/config.md) - Project configuration
- [Utils](project/utils.md) - Utility functions
