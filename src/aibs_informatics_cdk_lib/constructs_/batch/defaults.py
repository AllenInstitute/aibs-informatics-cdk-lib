from aws_cdk import aws_batch_alpha as batch

from aibs_informatics_cdk_lib.constructs_.batch.infrastructure import BatchEnvironmentConfig
from aibs_informatics_cdk_lib.constructs_.batch.instance_types import (
    LAMBDA_LARGE_INSTANCE_TYPES,
    LAMBDA_MEDIUM_INSTANCE_TYPES,
    LAMBDA_SMALL_INSTANCE_TYPES,
    ON_DEMAND_INSTANCE_TYPES,
    SPOT_INSTANCE_TYPES,
    TRANSFER_INSTANCE_TYPES,
)

LOW_PRIORITY_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
    instance_types=SPOT_INSTANCE_TYPES,
    use_spot=True,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
NORMAL_PRIORITY_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=SPOT_INSTANCE_TYPES,
    use_spot=True,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
HIGH_PRIORITY_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=ON_DEMAND_INSTANCE_TYPES,
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
PUBLIC_SUBNET_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=TRANSFER_INSTANCE_TYPES,
    use_spot=False,
    use_fargate=False,
    use_public_subnets=True,
    attach_file_system=True,
)

LAMBDA_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=[
        *LAMBDA_SMALL_INSTANCE_TYPES,
        *LAMBDA_MEDIUM_INSTANCE_TYPES,
        *LAMBDA_LARGE_INSTANCE_TYPES,
    ],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
LAMBDA_SMALL_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=LAMBDA_SMALL_INSTANCE_TYPES,
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
LAMBDA_MEDIUM_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=LAMBDA_MEDIUM_INSTANCE_TYPES,
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
LAMBDA_LARGE_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
    instance_types=LAMBDA_LARGE_INSTANCE_TYPES,
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
    attach_file_system=True,
)
