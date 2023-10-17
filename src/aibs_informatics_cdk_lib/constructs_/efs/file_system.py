import logging
from typing import Literal, Optional, Tuple, TypeVar, Union

import aws_cdk as cdk
import constructs
from aibs_informatics_aws_utils.constants.efs import (
    EFS_MOUNT_PATH_VAR,
    EFS_ROOT_ACCESS_POINT_TAG,
    EFS_ROOT_PATH,
    EFS_SCRATCH_ACCESS_POINT_TAG,
    EFS_SCRATCH_PATH,
    EFS_SHARED_ACCESS_POINT_TAG,
    EFS_SHARED_PATH,
    EFS_TMP_ACCESS_POINT_TAG,
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

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EnvBaseFileSystem(efs.FileSystem, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        vpc: ec2.IVpc,
        file_system_name: Optional[str] = None,
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
            file_system_name=self.get_name_with_env(file_system_name)
            if file_system_name
            else None,
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

        self._root_access_point = self.create_access_point("root", EFS_ROOT_PATH)

    @property
    def root_access_point(self) -> efs.AccessPoint:
        return self._root_access_point

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
            self,
            self.get_construct_id(name, "cfn-ap"),
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
        )

    def as_lambda_file_system(
        self, access_point: Optional[efs.AccessPoint] = None
    ) -> lambda_.FileSystem:
        ap = access_point or self.root_access_point
        return lambda_.FileSystem.from_efs_access_point(
            ap=ap,
            # Must start with `/mnt` per lambda regex requirements
            mount_path=f"/mnt/efs",
        )

    def grant_lambda_access(self, resource: lambda_.Function):
        grant_file_system_access(self, resource)


class EFSEcosystem(EnvBaseConstruct):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        file_system_name: Optional[str],
        vpc: ec2.Vpc,
    ) -> None:
        super().__init__(scope, id, env_base)
        self._file_system = EnvBaseFileSystem(
            self,
            f"{file_system_name + '-' if file_system_name else ''}fs",
            env_base=self.env_base,
            file_system_name=self.get_name_with_env(file_system_name)
            if file_system_name
            else None,
            lifecycle_policy=efs.LifecyclePolicy.AFTER_7_DAYS,
            out_of_infrequent_access_policy=efs.OutOfInfrequentAccessPolicy.AFTER_1_ACCESS,
            enable_automatic_backups=False,
            throughput_mode=efs.ThroughputMode.BURSTING,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            vpc=vpc,
        )

        self.shared_access_point = self.file_system.create_access_point("shared", EFS_SHARED_PATH)
        self.scratch_access_point = self.file_system.create_access_point(
            "scratch", EFS_SCRATCH_PATH
        )
        self.tmp_access_point = self.file_system.create_access_point("tmp", EFS_TMP_PATH)

    @property
    def file_system(self) -> EnvBaseFileSystem:
        return self._file_system

    @property
    def as_lambda_file_system(self) -> lambda_.FileSystem:
        return self.file_system.as_lambda_file_system()


def create_access_point(
    scope: constructs.Construct,
    file_system: efs.FileSystem,
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
    )


def grant_connectable_file_system_access(
    file_system: efs.FileSystem,
    connectable: ec2.IConnectable,
    permissions: Literal["r", "rw"] = "rw",
):
    file_system.connections.allow_default_port_from(connectable)
    repair_connectable_efs_dependency(file_system, connectable)


def grant_role_file_system_access(
    file_system: efs.FileSystem, role: Optional[iam.IRole], permissions: Literal["r", "rw"] = "rw"
):
    grant_managed_policies(role, "AmazonElasticFileSystemReadOnlyAccess")
    if "w" in permissions:
        grant_managed_policies(role, "AmazonElasticFileSystemClientReadWriteAccess")


def grant_grantable_file_system_access(
    file_system: efs.FileSystem, grantable: iam.IGrantable, permissions: Literal["r", "rw"] = "rw"
):
    actions = []
    if "w" in permissions:
        actions.append("elasticfilesystem:ClientWrite")
    file_system.grant(grantable, *actions)


def grant_file_system_access(file_system: efs.FileSystem, resource: lambda_.Function):
    grant_grantable_file_system_access(file_system, resource)
    grant_role_file_system_access(file_system, resource.role)
    grant_connectable_file_system_access(file_system, resource)


def repair_connectable_efs_dependency(file_system: efs.FileSystem, connectable: ec2.IConnectable):
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
