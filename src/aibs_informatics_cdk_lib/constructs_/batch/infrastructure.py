from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Union

import constructs
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from aws_cdk import aws_batch_alpha as batch
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam

from aibs_informatics_cdk_lib.common.aws.iam_utils import (
    S3_READ_ONLY_ACCESS_ACTIONS,
    dynamodb_policy_statement,
    lambda_policy_statement,
)
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.batch.launch_template import IBatchLaunchTemplateBuilder
from aibs_informatics_cdk_lib.constructs_.batch.types import BatchEnvironmentName
from aibs_informatics_cdk_lib.constructs_.efs import EFSFileSystem
from aibs_informatics_cdk_lib.constructs_.s3 import SecureS3Bucket


class Batch(EnvBaseConstruct):
    """
    Defines:
        - Batch Compute Environment (Spot and OnDemand)
        - Instance Role
        - Launch Template
        - Queue(s)
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        vpc: ec2.IVpc,
    ) -> None:

        super().__init__(scope, id, env_base)
        self.vpc = vpc

        # ---------------------------------------------------------------------
        # Shared EC2 pieces between the Compute Environments
        #  - instance role/profile
        #  - security group
        #  - launch template
        # ---------------------------------------------------------------------
        self.instance_role = self.create_instance_role()
        self.instance_profile = self.create_instance_profile()
        self.security_group = self.create_security_group()

        self.batch_environments: Dict[str, BatchEnvironment] = {}
        for _, batch_env in self.batch_environments.items():
            batch_env.node.add_dependency(self.instance_profile)

    def create_instance_role(self) -> iam.Role:
        instance_role = iam.Role(
            self,
            self.get_child_id(self, f"instance-role"),
            description="Role used by ec2 instance in batch compute environment",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
        )
        managed_policy_names = [
            "service-role/AmazonEC2ContainerServiceforEC2Role",
            "AmazonS3ReadOnlyAccess",
            "AmazonSSMManagedInstanceCore",
            "AmazonElasticFileSystemClientReadWriteAccess",
            "CloudWatchAgentServerPolicy",
        ]
        for mpn in managed_policy_names:
            instance_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(mpn))
        # Used for EBS autoscaling.
        iam.Policy(
            self,
            f"ebs-autoscale-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "ec2:AttachVolume",
                        "ec2:DescribeVolumeStatus",
                        "ec2:DescribeVolumes",
                        "ec2:ModifyInstanceAttribute",
                        "ec2:DescribeVolumeAttribute",
                        "ec2:CreateVolume",
                        "ec2:DeleteVolume",
                        "ec2:CreateTags",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                )
            ],
            roles=[instance_role],
        )

        # Used for S3 / Lambda
        iam.Policy(
            self,
            f"s3-lambda-env",
            statements=[
                # allow read access from all buckets
                iam.PolicyStatement(
                    sid="AllReadObjectActions",
                    actions=S3_READ_ONLY_ACCESS_ACTIONS,
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:s3:::*",
                        f"arn:aws:s3:::*/*",
                    ],
                ),
                iam.PolicyStatement(
                    sid="AllObjectActions",
                    actions=[
                        "s3:*Object",
                        "s3:GetBucket*",
                        "s3:List*",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:s3:::{self.env_base}-*",
                        f"arn:aws:s3:::{self.env_base}-*/*",
                    ],
                ),
                iam.PolicyStatement(
                    sid="AllowCallDescribeInstances",
                    actions=["ecs:DescribeContainerInstances"],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                lambda_policy_statement(actions=["lambda:InvokeFunction"], env_base=self.env_base),
                dynamodb_policy_statement(
                    env_base=self.env_base,
                    sid="DynamoDBReadWrite",
                    actions=[
                        "dynamodb:BatchGet*",
                        "dynamodb:DescribeStream",
                        "dynamodb:DescribeTable",
                        "dynamodb:Get*",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchWrite*",
                        "dynamodb:Delete*",
                        "dynamodb:Update*",
                        "dynamodb:PutItem",
                    ],
                ),
            ],
            roles=[instance_role],
        )
        return instance_role

    def create_instance_profile(self) -> iam.CfnInstanceProfile:
        return iam.CfnInstanceProfile(
            self,
            f"instance-profile",
            roles=[self.instance_role.role_name],
        )

    def create_security_group(self) -> ec2.SecurityGroup:
        security_group = ec2.SecurityGroup(
            self,
            self.get_construct_id("batch", "sg"),
            vpc=self.vpc,
            allow_all_outbound=True,
            description=f"Batch instance security group for {self.env_base}",
        )
        security_group.add_ingress_rule(peer=security_group, connection=ec2.Port.all_traffic())
        return security_group

    def grant_instance_role_permissions(
        self,
        read_write_file_systems: Optional[Iterable[EFSFileSystem]] = None,
        read_only_buckets: Optional[Iterable[SecureS3Bucket]] = None,
        read_write_buckets: Optional[Iterable[SecureS3Bucket]] = None,
    ):
        if read_write_file_systems:
            for rw_fs in read_write_file_systems:
                rw_fs.grant_role_access(self.instance_role)

        if read_only_buckets is not None:
            for read_only_bucket in read_only_buckets:
                read_only_bucket.s3_bucket.grant_read(self.instance_role)

        if read_write_buckets is not None:
            for read_write_bucket in read_write_buckets:
                read_write_bucket.s3_bucket.grant_read_write(self.instance_role)

    def setup_batch_environment(
        self,
        batch_env_name: BatchEnvironmentName,
        batch_env_config: "BatchEnvironmentConfig",
        file_system: Optional[EFSFileSystem] = None,
        launch_template_builder: Optional[IBatchLaunchTemplateBuilder] = None,
    ):
        if launch_template_builder:
            launch_template_builder.grant_instance_role_permissions(self.instance_role)

        if file_system:
            file_system.grant_role_access(self.instance_role)

        # ---------------------------------------------------------------------
        # Batch Environments
        # ---------------------------------------------------------------------
        self.batch_environments[str(batch_env_name)] = BatchEnvironment(
            self,
            f"{batch_env_name}-batch-env",
            env_base=self.env_base,
            name=batch_env_name,
            config=batch_env_config,
            vpc=self.vpc,
            instance_role=self.instance_role,
            security_group=self.security_group,
            file_system=file_system,
            launch_template_builder=launch_template_builder,
        )


DEFAULT_MAXV_CPUS = 10240
DEFAULT_MINV_CPUS = 0


@dataclass
class BatchEnvironmentConfig:
    allocation_strategy: Optional[batch.AllocationStrategy]
    instance_types: Optional[List[Union[str, ec2.InstanceType]]]
    use_public_subnets: bool
    attach_file_system: bool
    use_spot: bool
    use_fargate: bool = False
    maxv_cpus: Optional[int] = DEFAULT_MAXV_CPUS
    minv_cpus: Optional[int] = DEFAULT_MINV_CPUS

    def __post_init__(self):
        self.maxv_cpus = min(DEFAULT_MAXV_CPUS, max(1, self.maxv_cpus))
        self.minv_cpus = min(1, max(0, self.minv_cpus))
        if self.use_fargate:
            # https://docs.aws.amazon.com/batch/latest/userguide/fargate.html
            self.minv_cpus = None
            self.instance_types = None

    @property
    def spot_bid_percentage(self) -> Optional[int]:
        spot_bid_percentage = None
        if self.use_spot:
            # We get a 25% discount currently for on-demand compute
            spot_bid_percentage = 75
        return spot_bid_percentage


class BatchEnvironment(EnvBaseConstruct):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        name: BatchEnvironmentName,
        config: BatchEnvironmentConfig,
        vpc: ec2.IVpc,
        instance_role: iam.IRole,
        security_group: ec2.SecurityGroup,
        file_system: Optional[EFSFileSystem] = None,
        launch_template_builder: Optional[IBatchLaunchTemplateBuilder] = None,
    ):
        super().__init__(scope, id, env_base)
        self._name = name
        self.config = config
        self.vpc = vpc
        self.instance_role = instance_role
        self.security_group = security_group
        self.file_system = file_system
        self.launch_template_builder = launch_template_builder

        # Set up max number of compute environments
        compute_env = batch.OrderedComputeEnvironment(
            compute_environment=self.create_compute_environment(), order=1
        )
        if file_system and self.config.attach_file_system:
            file_system.grant_connectable_access(compute_env.compute_environment)

        job_queue = batch.JobQueue(
            self,
            self.job_queue_name,
            job_queue_name=self.job_queue_name,
            compute_environments=[compute_env],
        )
        self.job_queue = job_queue

    @property
    def name(self) -> BatchEnvironmentName:
        return self._name

    @property
    def job_queue_name(self) -> str:
        return self.name.get_job_queue_name(self.env_base)

    @property
    def instance_types(self) -> Optional[Sequence[ec2.InstanceType]]:
        if self.config.instance_types:
            return [
                ec2.InstanceType(it) if isinstance(it, str) else it
                for it in self.config.instance_types
            ]
        return None

    @property
    def vpc_subnets(self) -> Optional[ec2.SubnetSelection]:
        vpc_subnets = None
        if self.config.use_public_subnets:
            vpc_subnets = ec2.SubnetSelection(subnets=self.vpc.public_subnets)
        return vpc_subnets

    @cached_property
    def launch_template(self) -> Optional[ec2.LaunchTemplate]:
        launch_template = None
        if self.launch_template_builder and not self.config.use_fargate:
            launch_template = self.launch_template_builder.create_launch_template(
                self.name, self.security_group
            )
        return launch_template

    @property
    def launch_template_user_data_hash(self) -> str:
        lt = self.launch_template
        return sha256_hexdigest(lt.user_data.render()) if lt and lt.user_data else None

    @property
    def compute_resource_tags(self) -> Optional[Mapping[str, str]]:
        if not self.config.use_fargate:
            return remove_null_values(
                {
                    "env_base": self.env_base,
                    "batch_queue": self.job_queue_name,
                    "compute_resource_type": self.compute_resource_type,
                    # Get a hash of the launch template data. This is to ensure that when the launch template
                    # changes the ComputeEnvironment will be recreated. This is required by Batch since any
                    # updates to a launch template user data will not take effect until the Compute Environment
                    # itself is destroyed and recreated.
                    # Related: https://github.com/hashicorp/terraform-provider-aws/issues/15535
                    "launch_template_user_data_hash": self.launch_template_user_data_hash,
                }
            )
        return None

    @property
    def compute_resource_type(self) -> Literal["ON_DEMAND", "SPOT", "FARGATE", "FARGATE_SPOT"]:
        if self.config.use_fargate and self.config.use_spot:
            return "FARGATE_SPOT"
        elif self.config.use_fargate and not self.config.use_spot:
            return "FARGATE"
        elif not self.config.use_fargate and self.config.use_spot:
            return "SPOT"
        else:
            return "ON_DEMAND"

    def create_compute_environment(self) -> batch.IComputeEnvironment:
        if self.config.use_fargate:
            ce = batch.FargateComputeEnvironment(
                self,
                f"{self.name}-ce",
                maxv_cpus=self.config.maxv_cpus,
                spot=self.config.use_spot,
                vpc_subnets=self.vpc_subnets,
            )
        else:
            ce = batch.ManagedEc2EcsComputeEnvironment(
                self,
                f"{self.name}-ce",
                vpc=self.vpc,
                allocation_strategy=self.config.allocation_strategy,
                maxv_cpus=self.config.maxv_cpus,
                minv_cpus=self.config.minv_cpus,
                instance_role=self.instance_role,
                launch_template=self.launch_template,
                instance_types=self.instance_types,
                spot=self.config.use_spot,
                spot_bid_percentage=self.config.spot_bid_percentage,
                vpc_subnets=self.vpc_subnets,
            )

        if tags := self.compute_resource_tags:
            for tag_key, tag_value in tags.items():
                ce.tags.set_tag(key=tag_key, value=tag_value)
        return ce
