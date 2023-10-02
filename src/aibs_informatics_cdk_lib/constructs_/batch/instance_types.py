from typing import List

# Resources:
# - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html
# - https://instances.vantage.sh/


# Defines instance types to use in the
# default OnDemand Compute Environment
ON_DEMAND_INSTANCE_TYPES: List[str] = [
    "m4.large",
    "m4.xlarge",
    "m4.2xlarge",
    "m4.4xlarge",
    "m4.10xlarge",
    "m5.large",
    "m5.xlarge",
    "m5.2xlarge",
    "m5.4xlarge",
    "m5.8xlarge",
    "m5.12xlarge",
    "m5.16xlarge",
    "m6i.large",
    "m6i.xlarge",
    "m6i.2xlarge",
    "m6i.4xlarge",
    "m6i.8xlarge",
    "m6i.12xlarge",
    "m6i.16xlarge",
    "c4.large",
    "c4.xlarge",
    "c4.2xlarge",
    "c4.4xlarge",
    "c4.8xlarge",
    "c5.large",
    "c5.xlarge",
    "c5.2xlarge",
    "c5.4xlarge",
    "c5.9xlarge",
    "c5.12xlarge",
    "c5.18xlarge",
    "c6i.large",
    "c6i.xlarge",
    "c6i.2xlarge",
    "c6i.4xlarge",
    "c6i.8xlarge",
    "c6i.12xlarge",
    "c6i.16xlarge",
    "r4.large",
    "r4.xlarge",
    "r4.2xlarge",
    "r4.4xlarge",
    "r4.8xlarge",
    "r4.16xlarge",
    "r5.large",
    "r5.xlarge",
    "r5.2xlarge",
    "r5.4xlarge",
    "r5.8xlarge",
    "r5.12xlarge",
    "r5.16xlarge",
]

# Defines instance types to use in the default
# Spot compute environment. Same as OnDemand
# for now.
SPOT_INSTANCE_TYPES: List[str] = ON_DEMAND_INSTANCE_TYPES


# Defines EC2 instance types to be used for TRANSFER AWS Batch jobs
# Jobs that transfer files from globus/seqmatic/etc... to our staging S3 bucket
TRANSFER_INSTANCE_TYPES: List[str] = [
    # 4 GB MEM | 2 vCPUs
    "c5.large",
    "c5a.large",
    "c5ad.large",
    "c5d.large",
    "c5n.large",  # MEM 5.25 GB
    "c6a.large",
    "c6i.large",
    "c6id.large",
    "c6in.large",
    # 8 GB MEM | 2 vCPUs
    "m5.large",
    "m5a.large",
    "m5ad.large",
    "m5d.large",
    "m5dn.large",
    "m5n.large",
    "m6a.large",
    "m6i.large",
    "m6id.large",
    "m6idn.large",
    "m6in.large",
    # 8 GB MEM | 4 vCPUs
    "c5.xlarge",
    "c5a.xlarge",
    "c5ad.xlarge",
    "c5d.xlarge",
    "c5n.xlarge",
    "c6a.xlarge",
    "c6i.xlarge",
    "c6id.xlarge",
    "c6in.xlarge",
]


# ~4 GB MEM OR LESS
LAMBDA_SMALL_INSTANCE_TYPES: List[str] = [
    # "t3.nano",
    # "t3a.nano",
    # "t3.micro",
    # "t3a.micro",
    # "t3.small",
    # "t3a.small",
    # "t3.medium",
    # "t3a.medium",
    # 4 GB MEM | 2 vCPUs
    # "c5.large",
    "c5a.large",
    # "c5ad.large",
    # "c5d.large",
    # "c5n.large",  # MEM 5.25 GB
    "c6a.large",
    # "c6i.large",
    "c6id.large",
    # "c6in.large",
]


# ~8 GB MEM OR LESS
LAMBDA_MEDIUM_INSTANCE_TYPES: List[str] = [
    # 8 GB MEM | 2 vCPUs
    "m5.large",
    # "m5a.large",
    # "m5ad.large",
    # "m5d.large",
    # "m5dn.large",
    "m5n.large",
    # "m6a.large",
    # "m6i.large",
    # "m6id.large",
    "m6idn.large",
    "m6in.large",
    # 8 GB MEM | 4 vCPUs
    # "c5.xlarge",
    "c5a.xlarge",
    "c5ad.xlarge",
    # "c5d.xlarge",
    "c5n.xlarge",  # 10.5 GB MEM
    # "c6a.xlarge",
    # "c6i.xlarge",
    # "c6id.xlarge",
    "c6in.xlarge",
]


# 16 GB MEM OR LESS
LAMBDA_LARGE_INSTANCE_TYPES: List[str] = [
    # 16 GB MEM | 2 vCPUs
    "r5.xlarge",
    "r5a.xlarge",
    "r5ad.xlarge",
    # "r5d.xlarge",
    # "r5dn.xlarge",
    # "r5n.xlarge",
    # "r6a.xlarge",
    # "r6i.xlarge",
    "r6id.xlarge",
    # "r6idn.xlarge",
    "r6in.xlarge",
    # 16 GB MEM | 4 vCPUs
    # "m5.xlarge",
    # "m5a.xlarge",
    # "m5ad.xlarge",
    # "m5d.xlarge",
    "m5dn.xlarge",
    "m5n.xlarge",
    # "m6a.xlarge",
    "m6i.xlarge",
    # "m6id.xlarge",
    "m6idn.xlarge",
    "m6in.xlarge",
    # 16 GB MEM | 8 vCPUs
    # "c5.2xlarge",
    "c5a.2xlarge",
    # "c5ad.2xlarge",
    "c5d.2xlarge",
    "c5n.2xlarge",  # 21 GB MEM
    "c6a.2xlarge",
    # "c6i.2xlarge",
    # "c6id.2xlarge",
    # "c6in.2xlarge",
]
