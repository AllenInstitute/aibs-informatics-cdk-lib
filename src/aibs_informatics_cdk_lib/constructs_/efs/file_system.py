import logging
from dataclasses import dataclass
from typing import Any, Literal, Optional, Tuple, TypeVar, Union

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
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        vpc: ec2.IVpc,
        file_system_name: str,
        allow_anonymous_access: Optional[bool] = None,
        enable_automatic_backups: Optional[bool] = None,
        encrypted: Optional[bool] = None,
        lifecycle_policy: Optional[LifecyclePolicy] = None,
        out_of_infrequent_access_policy: Optional[OutOfInfrequentAccessPolicy] = None,
        performance_mode: Optional[PerformanceMode] = None,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.DESTROY,
        throughput_mode: Optional[ThroughputMode] = ThroughputMode.BURSTING,
        **kwargs,
    ) -> None:
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
        return self._file_system_name

    def create_access_point(
        self, name: str, path: str, *tags: Union[EFSTag, Tuple[str, str]]
    ) -> efs.AccessPoint:
        """Create an EFS access point

        Note:   We use CfnAccessPoint because the AccessPoint construct does not support tagging
                or naming. We use tags to name it.

        Args:
            name (str): name used for construct id
            path (str): access point path
            tags (List[EFSTag]): tags to add to the access point

        Returns:
            efs.AccessPoint: _description_
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
        ap = access_point or self.root_access_point
        return lambda_.FileSystem.from_efs_access_point(
            ap=ap,
            # Must start with `/mnt` per lambda regex requirements
            mount_path="/mnt/efs",
        )

    def grant_lambda_access(self, resource: lambda_.Function):
        grant_file_system_access(self, resource)


class EFSEcosystem(EnvBaseConstruct):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        file_system_name: str,
        vpc: ec2.Vpc,
        efs_lifecycle_policy: Optional[efs.LifecyclePolicy] = None,
    ) -> None:
        """Construct for setting up an EFS file system

        NOTE: If the EFS filesystem is intended to be deployed in efs.ThroughputMode.BURSTING
              it may be counterproductive to set an efs_lifecycle_policy other than None because
              EFS files in IA tier DO NOT count towards burst credit accumulation calculations.
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
        return self._file_system

    @property
    def as_lambda_file_system(self) -> lambda_.FileSystem:
        return self.file_system.as_lambda_file_system(self.root_access_point)


@dataclass
class MountPointConfiguration:
    file_system: Optional[Union[efs.FileSystem, efs.IFileSystem]]
    access_point: Optional[Union[efs.AccessPoint, efs.IAccessPoint]]
    mount_point: str
    root_directory: Optional[str] = None
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
        file_system: Union[efs.FileSystem, efs.IFileSystem],
        root_directory: Optional[str] = None,
        mount_point: Optional[str] = None,
        read_only: bool = False,
    ) -> "MountPointConfiguration":
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
        access_point: Union[efs.AccessPoint, efs.IAccessPoint],
        mount_point: Optional[str] = None,
        read_only: bool = False,
    ) -> "MountPointConfiguration":
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
        if self.access_point:
            return self.access_point.file_system.file_system_id
        elif self.file_system:
            return self.file_system.file_system_id
        else:
            raise ValueError("No file system or access point provided")

    @property
    def access_point_id(self) -> Optional[str]:
        if self.access_point:
            return self.access_point.access_point_id
        return None

    def to_batch_mount_point(self, name: str, sfn_format: bool = False) -> dict[str, Any]:
        mount_point: dict[str, Any] = to_mount_point(
            self.mount_point, self.read_only, source_volume=name
        )  # type: ignore[arg-type]  # typed dict should be accepted
        if sfn_format:
            return convert_to_sfn_api_action_case(mount_point)
        return mount_point

    def to_batch_volume(self, name: str, sfn_format: bool = False) -> dict[str, Any]:
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
    file_system: Union[efs.FileSystem, efs.IFileSystem],
    name: str,
    path: str,
    *tags: Union[EFSTag, Tuple[str, str]],
) -> efs.AccessPoint:
    """Create an EFS access point

    Note:   We use CfnAccessPoint because the AccessPoint construct does not support tagging
            or naming. We use tags to name it.

    Args:
        name (str): name used for construct id
        path (str): access point path
        tags (List[EFSTag]): tags to add to the access point

    Returns:
        efs.AccessPoint: _description_
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
    file_system: Union[efs.IFileSystem, efs.FileSystem],
    connectable: ec2.IConnectable,
    permissions: Literal["r", "rw"] = "rw",
):
    file_system.connections.allow_default_port_from(connectable)
    repair_connectable_efs_dependency(file_system, connectable)


def grant_role_file_system_access(
    file_system: Union[efs.IFileSystem, efs.FileSystem],
    role: Optional[iam.IRole],
    permissions: Literal["r", "rw"] = "rw",
):
    grant_managed_policies(role, "AmazonElasticFileSystemReadOnlyAccess")
    if "w" in permissions:
        grant_managed_policies(role, "AmazonElasticFileSystemClientReadWriteAccess")


def grant_grantable_file_system_access(
    file_system: Union[efs.IFileSystem, efs.FileSystem],
    grantable: iam.IGrantable,
    permissions: Literal["r", "rw"] = "rw",
):
    actions = []
    if "w" in permissions:
        actions.append("elasticfilesystem:ClientWrite")
    file_system.grant(grantable, *actions)


def grant_file_system_access(
    file_system: Union[efs.IFileSystem, efs.FileSystem], resource: lambda_.Function
):
    grant_grantable_file_system_access(file_system, resource)
    grant_role_file_system_access(file_system, resource.role)
    grant_connectable_file_system_access(file_system, resource)


def repair_connectable_efs_dependency(
    file_system: Union[efs.IFileSystem, efs.FileSystem], connectable: ec2.IConnectable
):
    """Repairs cyclical dependency between EFS and dependent connectable

    Reusing code written in this comment

    https://github.com/aws/aws-cdk/issues/18759#issuecomment-1268689132

    From the github comment:

        When an EFS filesystem is added to a Lambda Function (via the file_system= param)
        it automatically sets up networking access between the two by adding
        an ingress rule on the EFS security group. However, the ingress rule resource
        gets attached to whichever CDK Stack the EFS security group is defined on.
        If the Lambda Function is defined on a different stack, it then creates
        a circular dependency issue, where the EFS stack is dependent on the Lambda
        security group's ID and the Lambda stack is dependent on the EFS stack's file
        system object.

        To resolve this, we manually remove the ingress rule that gets automatically created
        and recreate it on the Lambda's stack instead.


    Args:
        connectable (ec2.IConnectable): Connectable

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
