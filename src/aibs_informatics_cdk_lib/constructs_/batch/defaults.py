from aws_cdk import aws_batch as batch

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
    allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
    instance_types=[*SPOT_INSTANCE_TYPES],
    use_spot=True,
    use_fargate=False,
    use_public_subnets=False,
)
NORMAL_PRIORITY_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
    instance_types=[*SPOT_INSTANCE_TYPES],
    use_spot=True,
    use_fargate=False,
    use_public_subnets=False,
)
HIGH_PRIORITY_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[*ON_DEMAND_INSTANCE_TYPES],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
)
PUBLIC_SUBNET_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[*TRANSFER_INSTANCE_TYPES],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=True,
)

LAMBDA_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[
        *LAMBDA_SMALL_INSTANCE_TYPES,
        *LAMBDA_MEDIUM_INSTANCE_TYPES,
        *LAMBDA_LARGE_INSTANCE_TYPES,
    ],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
)
LAMBDA_SMALL_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[*LAMBDA_SMALL_INSTANCE_TYPES],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
)
LAMBDA_MEDIUM_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[*LAMBDA_MEDIUM_INSTANCE_TYPES],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
)
LAMBDA_LARGE_BATCH_ENV_CONFIG = BatchEnvironmentConfig(
    allocation_strategy=batch.AllocationStrategy.BEST_FIT,
    instance_types=[*LAMBDA_LARGE_INSTANCE_TYPES],
    use_spot=False,
    use_fargate=False,
    use_public_subnets=False,
)
