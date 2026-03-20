"""EFS file system constructs and utilities.

This module provides CDK constructs for creating and managing EFS file systems,
access points, and mount point configurations.
"""

import logging
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

import aws_cdk as cdk
import constructs
from aibs_informatics_aws_utils.batch import to_mount_point, to_volume
from aibs_informatics_aws_utils.constants.efs import (
    EFS_ROOT_PATH,
    EFS_SCRATCH_PATH,
    EFS_SHARED_PATH,
    EFS_TMP_PATH,
    EFSTag,
)
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk.aws_efs import (
    LifecyclePolicy,
    OutOfInfrequentAccessPolicy,
    PerformanceMode,
    ThroughputMode,
)

from aibs_informatics_cdk_lib.common.aws.iam_utils import grant_managed_policies
from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct, EnvBaseConstructMixins
from aibs_informatics_cdk_lib.constructs_.sfn.utils import convert_to_sfn_api_action_case

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EnvBaseFileSystem(efs.FileSystem, EnvBaseConstructMixins):
    """Environment-aware EFS file system construct.

    Extends the standard EFS FileSystem with environment base naming conventions
    and helper methods for access point creation.

    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        vpc: ec2.IVpc,
        file_system_name: str,
        allow_anonymous_access: bool | None = None,
        enable_automatic_backups: bool | None = None,
        encrypted: bool | None = None,
        lifecycle_policy: LifecyclePolicy | None = None,
        out_of_infrequent_access_policy: OutOfInfrequentAccessPolicy | None = None,
        performance_mode: PerformanceMode | None = None,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.DESTROY,
        throughput_mode: ThroughputMode | None = ThroughputMode.BURSTING,
        **kwargs,
    ) -> None:
        """Initialize an environment-aware EFS file system.

        Args:
            scope (constructs.Construct): The construct scope.
            id (str): The construct ID.
            env_base (EnvBase): Environment base for resource naming.
            vpc (ec2.IVpc): VPC for the file system.
            file_system_name (str): Name for the file system.
            allow_anonymous_access (Optional[bool]): Allow anonymous access.
            enable_automatic_backups (Optional[bool]): Enable automatic backups.
            encrypted (Optional[bool]): Enable encryption.
            lifecycle_policy (Optional[LifecyclePolicy]): Lifecycle policy.
            out_of_infrequent_access_policy (Optional[OutOfInfrequentAccessPolicy]):
                Policy for moving files out of infrequent access.
            performance_mode (Optional[PerformanceMode]): Performance mode.
            removal_policy (cdk.RemovalPolicy): Removal policy.
                Defaults to DESTROY.
            throughput_mode (Optional[ThroughputMode]): Throughput mode.
                Defaults to BURSTING.
            **kwargs: Additional arguments passed to parent.
        """
        self.env_base = env_base
        super().__init__(
            scope,
            id,
            vpc=vpc,
            file_system_name=(full_file_system_name := self.get_name_with_env(file_system_name)),
            allow_anonymous_access=allow_anonymous_access,
            enable_automatic_backups=enable_automatic_backups,
            encrypted=encrypted,
            lifecycle_policy=lifecycle_policy,
            out_of_infrequent_access_policy=out_of_infrequent_access_policy,
            performance_mode=performance_mode,
            removal_policy=removal_policy,
            throughput_mode=throughput_mode,
            **kwargs,
        )
        self._file_system_name = full_file_system_name

    @property
    def file_system_name(self) -> str:
        """Get the full file system name including environment prefix.

        Returns:
            The file system name.
        """
        return self._file_system_name

    def create_access_point(
        self, name: str, path: str, *tags: EFSTag | tuple[str, str]
    ) -> efs.AccessPoint:
        """Create an EFS access point.

        Uses CfnAccessPoint because the AccessPoint construct does not support
        tagging or naming. Tags are used to set the name.

        Args:
            name (str): Name used for construct ID and as default Name tag.
            path (str): Access point path within the file system.
            *tags (Union[EFSTag, Tuple[str, str]]): Variable number of tags
                to add to the access point.

        Returns:
            The created access point.
        """
        ap_tags = [tag if isinstance(tag, EFSTag) else EFSTag(*tag) for tag in tags]
        if not any(tag.key == "Name" for tag in ap_tags):
            ap_tags.insert(0, EFSTag("Name", name))

        cfn_access_point = efs.CfnAccessPoint(
            self.get_stack_of(self),
            self.get_construct_id(self.node.id, name, "cfn-ap"),
            file_system_id=self.file_system_id,
            access_point_tags=[
                efs.CfnAccessPoint.AccessPointTagProperty(key=tag.key, value=tag.value)
                for tag in ap_tags
            ],
            posix_user=efs.CfnAccessPoint.PosixUserProperty(
                gid="0",
                uid="0",
            ),
            root_directory=efs.CfnAccessPoint.RootDirectoryProperty(
                creation_info=efs.CfnAccessPoint.CreationInfoProperty(
                    owner_gid="0",
                    owner_uid="0",
                    permissions="0777",
                ),
                path=path,
            ),
        )
        return efs.AccessPoint.from_access_point_attributes(
            self,
            self.get_construct_id(name, "access-point"),
            access_point_id=cfn_access_point.attr_access_point_id,
            file_system=self,
        )  # type: ignore

    def as_lambda_file_system(self, access_point: efs.AccessPoint) -> lambda_.FileSystem:
        """Convert to a Lambda file system configuration.

        Args:
            access_point (efs.AccessPoint): The access point to use.

        Returns:
            Lambda FileSystem configured for the access point.
        """
        ap = access_point
        return lambda_.FileSystem.from_efs_access_point(
            ap=ap,
            # Must start with `/mnt` per lambda regex requirements
            mount_path="/mnt/efs",
        )

    def grant_lambda_access(self, resource: lambda_.Function) -> None:
        """Grant a Lambda function access to this file system.

        Args:
            resource (lambda_.Function): The Lambda function to grant access to.
        """
        grant_file_system_access(self, resource)


class EFSEcosystem(EnvBaseConstruct):
    """Complete EFS ecosystem with predefined access points.

    Creates an EFS file system with root, shared, scratch, and tmp access points.

    Attributes:
        root_access_point: Access point for the root directory.
        shared_access_point: Access point for shared data.
        scratch_access_point: Access point for scratch data.
        tmp_access_point: Access point for temporary data.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str | None,
        env_base: EnvBase,
        file_system_name: str,
        vpc: ec2.Vpc,
        efs_lifecycle_policy: efs.LifecyclePolicy | None = None,
    ) -> None:
        """Initialize an EFS ecosystem.

        Args:
            scope (constructs.Construct): The construct scope.
            id (Optional[str]): The construct ID.
            env_base (EnvBase): Environment base for resource naming.
            file_system_name (str): Name for the file system.
            vpc (ec2.Vpc): VPC for the file system.
            efs_lifecycle_policy (Optional[efs.LifecyclePolicy]): Lifecycle policy.

        Note:
            If the EFS filesystem is intended to be deployed in BURSTING throughput mode,
            it may be counterproductive to set an efs_lifecycle_policy other than None
            because EFS files in IA tier DO NOT count towards burst credit accumulation.
            See: https://docs.aws.amazon.com/efs/latest/ug/performance.html#bursting
        """
        super().__init__(scope, id, env_base)
        self._file_system = EnvBaseFileSystem(
            scope=self,
            id=f"{file_system_name}-fs",
            env_base=self.env_base,
            file_system_name=self.get_name_with_env(file_system_name),
            lifecycle_policy=efs_lifecycle_policy,
            out_of_infrequent_access_policy=efs.OutOfInfrequentAccessPolicy.AFTER_1_ACCESS,
            enable_automatic_backups=False,
            throughput_mode=efs.ThroughputMode.BURSTING,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            vpc=vpc,
        )

        self.root_access_point = self.file_system.create_access_point(
            name="root", path=EFS_ROOT_PATH
        )
        self.shared_access_point = self.file_system.create_access_point(
            name="shared", path=EFS_SHARED_PATH
        )
        self.scratch_access_point = self.file_system.create_access_point(
            name="scratch", path=EFS_SCRATCH_PATH
        )
        self.tmp_access_point = self.file_system.create_access_point(name="tmp", path=EFS_TMP_PATH)
        self.file_system.add_tags(cdk.Tag("blah", self.env_base))

    @property
    def file_system(self) -> EnvBaseFileSystem:
        """Get the underlying EFS file system.

        Returns:
            The EFS file system instance.
        """
        return self._file_system

    @property
    def as_lambda_file_system(self) -> lambda_.FileSystem:
        """Get the file system configured for Lambda.

        Returns:
            Lambda FileSystem using the root access point.
        """
        return self.file_system.as_lambda_file_system(self.root_access_point)


@dataclass
class MountPointConfiguration:
    """Configuration for mounting an EFS file system.

    Attributes:
        file_system (Optional[Union[efs.FileSystem, efs.IFileSystem]]):
            The EFS file system to mount.
        access_point (Optional[Union[efs.AccessPoint, efs.IAccessPoint]]):
            The access point to use for mounting.
        mount_point (str): The path where the file system will be mounted.
        root_directory (Optional[str]): Root directory within the file system.
        read_only (bool): Whether to mount as read-only. Defaults to False.

    Raises:
        ValueError: If neither file system nor access point is provided,
            or if access point's file system doesn't match the provided file system.
    """

    file_system: efs.FileSystem | efs.IFileSystem | None
    access_point: efs.AccessPoint | efs.IAccessPoint | None
    mount_point: str
    root_directory: str | None = None
    read_only: bool = False

    def __post_init__(self):
        if not self.access_point and not self.file_system:
            raise ValueError("Must provide either file system or access point")
        if (
            self.access_point
            and self.file_system
            and self.access_point.file_system.file_system_id != self.file_system.file_system_id
        ):
            raise ValueError("File system of Access point and file system must be the same")
        if not self.mount_point.startswith("/"):
            raise ValueError("Mount point must start with /")

    @classmethod
    def from_file_system(
        cls,
        file_system: efs.FileSystem | efs.IFileSystem,
        root_directory: str | None = None,
        mount_point: str | None = None,
        read_only: bool = False,
    ) -> "MountPointConfiguration":
        """Create configuration from a file system.

        Args:
            file_system (Union[efs.FileSystem, efs.IFileSystem]): The file system.
            root_directory (Optional[str]): Root directory. Defaults to "/".
            mount_point (Optional[str]): Mount point path.
                Defaults to /opt/efs/{file_system_id}.
            read_only (bool): Mount as read-only. Defaults to False.

        Returns:
            MountPointConfiguration for the file system.
        """
        if not root_directory:
            root_directory = "/"
        if not mount_point:
            mount_point = f"/opt/efs/{file_system.file_system_id}"
        return cls(
            mount_point=mount_point,
            file_system=file_system,
            access_point=None,
            root_directory=root_directory,
            read_only=read_only,
        )

    @classmethod
    def from_access_point(
        cls,
        access_point: efs.AccessPoint | efs.IAccessPoint,
        mount_point: str | None = None,
        read_only: bool = False,
    ) -> "MountPointConfiguration":
        """Create configuration from an access point.

        Args:
            access_point (Union[efs.AccessPoint, efs.IAccessPoint]): The access point.
            mount_point (Optional[str]): Mount point path.
                Defaults to /opt/efs/{access_point_id}.
            read_only (bool): Mount as read-only. Defaults to False.

        Returns:
            MountPointConfiguration for the access point.
        """
        if not mount_point:
            mount_point = f"/opt/efs/{access_point.access_point_id}"
        return cls(
            mount_point=mount_point,
            access_point=access_point,
            file_system=None,
            root_directory=None,
            read_only=read_only,
        )

    @property
    def file_system_id(self) -> str:
        """Get the file system ID.

        Returns:
            The EFS file system ID.

        Raises:
            ValueError: If no file system or access point is configured.
        """
        if self.access_point:
            return self.access_point.file_system.file_system_id
        elif self.file_system:
            return self.file_system.file_system_id
        else:
            raise ValueError("No file system or access point provided")

    @property
    def access_point_id(self) -> str | None:
        """Get the access point ID.

        Returns:
            The access point ID, or None if using file system directly.
        """
        if self.access_point:
            return self.access_point.access_point_id
        return None

    def to_batch_mount_point(self, name: str, sfn_format: bool = False) -> dict[str, Any]:
        """Convert to Batch mount point configuration.

        Args:
            name (str): Name of the volume.
            sfn_format (bool): Use Step Functions API case. Defaults to False.

        Returns:
            Dictionary containing the mount point configuration.
        """
        mount_point: dict[str, Any] = to_mount_point(
            self.mount_point, self.read_only, source_volume=name
        )  # type: ignore[arg-type, assignment]  # typed dict should be accepted
        if sfn_format:
            return convert_to_sfn_api_action_case(mount_point)
        return mount_point

    def to_batch_volume(self, name: str, sfn_format: bool = False) -> dict[str, Any]:
        """Convert to Batch volume configuration.

        Args:
            name (str): Name of the volume.
            sfn_format (bool): Use Step Functions API case. Defaults to False.

        Returns:
            Dictionary containing the volume configuration.
        """
        efs_volume_configuration: dict[str, Any] = {
            "fileSystemId": self.file_system_id,
        }
        if self.access_point:
            efs_volume_configuration["transitEncryption"] = "ENABLED"
            # TODO: Consider adding IAM
            efs_volume_configuration["authorizationConfig"] = {
                "accessPointId": self.access_point.access_point_id,
                "iam": "DISABLED",
            }
        else:
            efs_volume_configuration["rootDirectory"] = self.root_directory or "/"
        volume: dict[str, Any] = to_volume(
            None,
            name=name,
            efs_volume_configuration=efs_volume_configuration,  # type: ignore
        )
        if sfn_format:
            return convert_to_sfn_api_action_case(volume)
        return volume


def create_access_point(
    scope: constructs.Construct,
    file_system: efs.FileSystem | efs.IFileSystem,
    name: str,
    path: str,
    *tags: EFSTag | tuple[str, str],
) -> efs.AccessPoint:
    """Create an EFS access point.

    Uses CfnAccessPoint because the AccessPoint construct does not support
    tagging or naming. Tags are used to set the name.

    Args:
        scope (constructs.Construct): The construct scope.
        file_system (Union[efs.FileSystem, efs.IFileSystem]): The file system.
        name (str): Name used for construct ID and as default Name tag.
        path (str): Access point path within the file system.
        *tags (Union[EFSTag, Tuple[str, str]]): Variable number of tags.

    Returns:
        The created access point.
    """
    ap_tags = [tag if isinstance(tag, EFSTag) else EFSTag(*tag) for tag in tags]

    if not any(tag.key == "Name" for tag in ap_tags):
        ap_tags.insert(0, EFSTag("Name", name))

    cfn_access_point = efs.CfnAccessPoint(
        scope,
        f"{name}-cfn-ap",
        file_system_id=file_system.file_system_id,
        access_point_tags=[
            efs.CfnAccessPoint.AccessPointTagProperty(key=tag.key, value=tag.value)
            for tag in ap_tags
        ],
        posix_user=efs.CfnAccessPoint.PosixUserProperty(
            gid="0",
            uid="0",
        ),
        root_directory=efs.CfnAccessPoint.RootDirectoryProperty(
            creation_info=efs.CfnAccessPoint.CreationInfoProperty(
                owner_gid="0",
                owner_uid="0",
                permissions="0777",
            ),
            path=path,
        ),
    )
    return efs.AccessPoint.from_access_point_attributes(
        scope,
        f"{name}-access-point",
        access_point_id=cfn_access_point.attr_access_point_id,
        file_system=file_system,
    )  # type: ignore


def grant_connectable_file_system_access(
    file_system: efs.IFileSystem | efs.FileSystem,
    connectable: ec2.IConnectable,
    permissions: Literal["r", "rw"] = "rw",
) -> None:
    """Grant a connectable resource access to an EFS file system.

    Args:
        file_system (Union[efs.IFileSystem, efs.FileSystem]): The file system.
        connectable (ec2.IConnectable): The connectable resource.
        permissions (Literal["r", "rw"]): Permission level. Defaults to "rw".
    """
    file_system.connections.allow_default_port_from(connectable)
    repair_connectable_efs_dependency(file_system, connectable)


def grant_role_file_system_access(
    file_system: efs.IFileSystem | efs.FileSystem,
    role: iam.IRole | None,
    permissions: Literal["r", "rw"] = "rw",
) -> None:
    """Grant an IAM role access to an EFS file system.

    Args:
        file_system (Union[efs.IFileSystem, efs.FileSystem]): The file system.
        role (Optional[iam.IRole]): The IAM role to grant access to.
        permissions (Literal["r", "rw"]): Permission level. Defaults to "rw".
    """
    grant_managed_policies(role, "AmazonElasticFileSystemReadOnlyAccess")
    if "w" in permissions:
        grant_managed_policies(role, "AmazonElasticFileSystemClientReadWriteAccess")


def grant_grantable_file_system_access(
    file_system: efs.IFileSystem | efs.FileSystem,
    grantable: iam.IGrantable,
    permissions: Literal["r", "rw"] = "rw",
) -> None:
    """Grant a grantable principal access to an EFS file system.

    Args:
        file_system (Union[efs.IFileSystem, efs.FileSystem]): The file system.
        grantable (iam.IGrantable): The grantable principal.
        permissions (Literal["r", "rw"]): Permission level. Defaults to "rw".
    """
    actions = []
    if "w" in permissions:
        actions.append("elasticfilesystem:ClientWrite")
    file_system.grant(grantable, *actions)


def grant_file_system_access(
    file_system: efs.IFileSystem | efs.FileSystem, resource: lambda_.Function
) -> None:
    """Grant a Lambda function full access to an EFS file system.

    Grants grantable, role, and connectable access.

    Args:
        file_system (Union[efs.IFileSystem, efs.FileSystem]): The file system.
        resource (lambda_.Function): The Lambda function.
    """
    grant_grantable_file_system_access(file_system, resource)
    grant_role_file_system_access(file_system, resource.role)
    grant_connectable_file_system_access(file_system, resource)


def repair_connectable_efs_dependency(
    file_system: efs.IFileSystem | efs.FileSystem, connectable: ec2.IConnectable
) -> None:
    """Repair cyclical dependency between EFS and dependent connectable.

    When an EFS filesystem is added to a Lambda Function (via the file_system= param)
    it automatically sets up networking access between the two by adding an ingress
    rule on the EFS security group. However, the ingress rule resource gets attached
    to whichever CDK Stack the EFS security group is defined on.

    If the Lambda Function is defined on a different stack, it creates a circular
    dependency issue, where the EFS stack is dependent on the Lambda security group's
    ID and the Lambda stack is dependent on the EFS stack's file system object.

    To resolve this, we manually remove the ingress rule that gets automatically
    created and recreate it on the Lambda's stack instead.

    Based on: https://github.com/aws/aws-cdk/issues/18759#issuecomment-1268689132

    Args:
        file_system (Union[efs.IFileSystem, efs.FileSystem]): The EFS file system.
        connectable (ec2.IConnectable): The connectable resource.

    Raises:
        RuntimeError: If unable to remove the child node.
    """
    connections = connectable.connections
    # Collect IDs of all security groups attached to the connections
    connection_sgs = {sg.security_group_id for sg in connections.security_groups}
    # Iterate over the security groups attached to EFS
    for efs_sg in file_system.connections.security_groups:
        # Iterate over the security group's child nodes
        for child in efs_sg.node.find_all():
            # If this is an ingress rule with a "source" equal to one of
            # the connections' security groups
            if (
                isinstance(child, ec2.CfnSecurityGroupIngress)
                and child.source_security_group_id in connection_sgs
            ):
                # Try to remove the node (raise an error if removal fails)
                node_id = child.node.id
                if not efs_sg.node.try_remove_child(node_id):
                    raise RuntimeError(f"Could not remove child node: {node_id}")

    # Finally, configure the connection between the connections object
    # and the EFS file system which will define the new ingress rule on
    # the stack defining the connection object instead.
    connections.allow_to_default_port(file_system)
