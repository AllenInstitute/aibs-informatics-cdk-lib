# Installation

## Requirements

- Python 3.9 or higher
- AWS CDK CLI (optional, for synthesis and deployment)

## Installing from PyPI

```bash
pip install aibs-informatics-cdk-lib
```

## Installing with Development Dependencies

For development work, install with the `dev` extras:

```bash
pip install aibs-informatics-cdk-lib[dev]
```

This includes:

- Testing utilities
- Type checking tools (mypy)
- Documentation tools (mkdocs)

## Installing from Source

Clone the repository and install in development mode:

```bash
git clone https://github.com/AllenInstitute/aibs-informatics-cdk-lib.git
cd aibs-informatics-cdk-lib
make install
```

## Dependencies

This library depends on:

- `aibs-informatics-core` - Core utilities and data models
- `aibs-informatics-aws-utils` - AWS service utilities
- `aws-cdk-lib` - AWS CDK core library
- `constructs` - CDK constructs base library
- `pydantic` - Data validation

## Verifying Installation

```python
import aibs_informatics_cdk_lib
print(aibs_informatics_cdk_lib.__version__)
```
