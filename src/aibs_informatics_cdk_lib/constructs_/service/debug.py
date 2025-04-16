from typing import List, Optional, Union, cast

from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from constructs import Construct

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.efs.file_system import (
    EnvBaseFileSystem,
    grant_connectable_file_system_access,
)


class DebugInstanceConstruct(EnvBaseConstruct):
    """
    DebugInstanceConstruct is a CDK construct that creates an EC2 instance pre-configured for debugging and troubleshooting purposes within a given VPC. This instance is designed primarily to facilitate runtime inspection, diagnostics, and interactions with attached resources, including optional file system mounts (EFS) for scenarios such as shared storage debugging or configuration verification.

    The construct provisions:
      - A dedicated security group for the instance.
      - An IAM role with necessary policies (including AmazonSSMManagedInstanceCore and AmazonS3ReadOnlyAccess), optionally supplemented by user-specified inline policies.
      - A Linux-based EC2 instance (defaulting to the latest Amazon Linux 2) with user data commands to perform system updates, install EFS utilities, and debugging tools (e.g., jq, tree).

    Parameters:
      - scope (Construct): The scope in which this construct is defined.
      - id (Optional[str]): The unique identifier for the construct.
      - env_base (EnvBase): The environment configuration required for the construct.
      - vpc (ec2.Vpc): The VPC within which the instance is launched.
      - name (str, optional): Base name for resources. Defaults to "DebugInstance".
      - efs_filesystems (Optional[List[Union[efs.IFileSystem, EnvBaseFileSystem]]], optional): A list of EFS file systems to mount on the instance. If provided, each file system is mounted at a dedicated path under /mnt/efs/.
      - instance_type (ec2.InstanceType, optional): The EC2 instance type for the debug instance. Defaults to t3.medium.
      - machine_image (Optional[ec2.IMachineImage], optional): The machine image used for the instance. Defaults to the latest Amazon Linux 2 image.
      - instance_name (Optional[str], optional): A custom name for the EC2 instance. If not provided, a name is generated based on the base name.
      - instance_role_name (Optional[str], optional): The name of the IAM role assigned to the instance. If not provided, a default name derived from the base name is used.
      - instance_role_policy_statements (Optional[List[iam.PolicyStatement]], optional): Additional IAM policy statements to attach inline to the instance role.

    Usage Examples:
        1. Minimal usage:
         >>> instance = DebugInstanceConstruct(
                 scope=app,
                 id="DebugInstance",
                 env_base=env,
                 vpc=my_vpc
             )
         This creates an EC2 instance with default parameters and without mounting any EFS file systems.

        2. Advanced usage with custom instance details and EFS mounting:
         >>> instance = DebugInstanceConstruct(
                 scope=app,
                 id="CustomDebugInstance",
                 env_base=env,
                 vpc=my_vpc,
                 name="CustomDebug",
                 instance_type=ec2.InstanceType("t3.large"),
                 instance_name="CustomDebugEC2",
                 instance_role_policy_statements=[custom_policy_statement],
                 efs_filesystems=[my_efs]
             )
         This creates an EC2 instance with a custom instance type, a specified instance name, an inline policy, and mounts the provided EFS file system.

        3. Using a custom machine image:
         >>> custom_image = ec2.MachineImage.from_lookup(name="MyCustomAMI")
         >>> instance = DebugInstanceConstruct(
                 scope=app,
                 id="CustomImageInstance",
                 env_base=env,
                 vpc=my_vpc,
                 machine_image=custom_image
             )
        This example shows how to specify a custom machine image instead of using the default Amazon Linux 2 image.
    """

    def __init__(
        self,
        scope: Construct,
        id: Optional[str],
        env_base: EnvBase,
        vpc: ec2.Vpc,
        name: str = "DebugInstance",
        efs_filesystems: Optional[List[Union[efs.IFileSystem, EnvBaseFileSystem]]] = None,
        instance_type: ec2.InstanceType = ec2.InstanceType("t3.medium"),
        machine_image: Optional[ec2.IMachineImage] = None,
        instance_name: Optional[str] = None,
        instance_role_name: Optional[str] = None,
        instance_role_policy_statements: Optional[List[iam.PolicyStatement]] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, env_base, **kwargs)

        # Security group for the instance
        self.debug_sg = ec2.SecurityGroup(
            self,
            f"{name}-SG",
            vpc=vpc,
            allow_all_outbound=True,
            description="Security group for Debug EC2 Instance",
        )

        # IAM role for the instance
        self.instance_role = iam.Role(
            self,
            f"{name}-InstanceRole",
            assumed_by=cast(iam.IPrincipal, iam.ServicePrincipal("ec2.amazonaws.com")),
            role_name=self.get_resource_name(instance_role_name or f"{name}-InstanceRole"),
            description="Role for Debug EC2 Instance",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
            ],
            inline_policies={
                "UserSpecifiedPolicies": iam.PolicyDocument(
                    statements=instance_role_policy_statements,
                )
            }
            if instance_role_policy_statements
            else None,
        )

        if machine_image is None:
            machine_image = ec2.MachineImage.latest_amazon_linux2(
                storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE,
                cpu_type=ec2.AmazonLinuxCpuType.X86_64,
                edition=ec2.AmazonLinuxEdition.STANDARD,
                user_data=(user_data := ec2.UserData.for_linux()),
            )
            user_data.add_commands(
                "yum -y update",
                # These are necessary for EFS mounting
                "yum -y install amazon-efs-utils",
                # These are useful for debugging
                "yum -y install jq tree",
            )

        # Create the EC2 Instance
        self.instance = ec2.Instance(
            self,
            f"{name}-Instance",
            instance_type=instance_type,
            machine_image=cast(ec2.IMachineImage, machine_image),
            instance_name=self.get_resource_name(instance_name or f"{name}-Instance"),
            vpc=vpc,
            role=cast(iam.IRole, self.instance_role),
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_group=self.debug_sg,
        )

        # If any EFS file systems are passed in, mount each one
        if efs_filesystems:
            for i, filesystem in enumerate(efs_filesystems):
                # Allow our instance to connect on the EFS mount target port
                filesystem.connections.allow_default_port_from(self.debug_sg)

                # A path for each EFS mount
                if isinstance(filesystem, EnvBaseFileSystem):
                    mount_path = f"/mnt/efs/{filesystem.file_system_name}"
                else:
                    mount_path = f"/mnt/efs/{filesystem.file_system_id}"

                self.instance.user_data.add_commands(
                    f"mkdir -p {mount_path}",
                    f"mount -t efs -o tls {filesystem.file_system_id}:/ {mount_path}",
                )
                grant_connectable_file_system_access(filesystem, self.instance, "rw")
