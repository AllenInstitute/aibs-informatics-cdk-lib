import logging
from typing import Any, List, Literal, Optional, TypeVar

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

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EFSFileSystem(EnvBaseConstruct):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        file_system_name: str,
        vpc: ec2.Vpc,
    ) -> None:
        super().__init__(scope, id, env_base)
        self.file_system = efs.FileSystem(
            self,
            f"{file_system_name}-fs",
            file_system_name=self.get_name_with_env(file_system_name),
            lifecycle_policy=efs.LifecyclePolicy.AFTER_7_DAYS,
            out_of_infrequent_access_policy=efs.OutOfInfrequentAccessPolicy.AFTER_1_ACCESS,
            enable_automatic_backups=False,
            throughput_mode=efs.ThroughputMode.BURSTING,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            vpc=vpc,
        )

        self.root_access_point = self.create_access_point(
            "root", EFS_ROOT_PATH, [EFS_ROOT_ACCESS_POINT_TAG]
        )
        self.shared_access_point = self.create_access_point(
            "shared", EFS_SHARED_PATH, [EFS_SHARED_ACCESS_POINT_TAG]
        )
        self.scratch_access_point = self.create_access_point(
            "scratch", EFS_SCRATCH_PATH, [EFS_SCRATCH_ACCESS_POINT_TAG]
        )
        self.tmp_access_point = self.create_access_point(
            "tmp", EFS_TMP_PATH, [EFS_TMP_ACCESS_POINT_TAG]
        )

    def create_access_point(self, name: str, path: str, tags: List[EFSTag]) -> efs.AccessPoint:
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
        cfn_access_point = efs.CfnAccessPoint(
            self,
            self.get_construct_id(name, "cfn-access-point"),
            file_system_id=self.file_system.file_system_id,
            access_point_tags=[
                efs.CfnAccessPoint.AccessPointTagProperty(key=tag.key, value=tag.value)
                for tag in tags
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
            file_system=self.file_system,
        )

    @property
    def as_lambda_file_system(self) -> lambda_.FileSystem:
        return lambda_.FileSystem.from_efs_access_point(
            ap=self.root_access_point,
            # Must start with `/mnt` per lambda regex requirements
            mount_path=f"/mnt/efs",
        )

    def grant_connectable_access(
        self, connectable: ec2.IConnectable, permissions: Literal["r", "rw"] = "rw"
    ):
        self.file_system.connections.allow_default_port_from(connectable)
        self.repair_connectable_efs_dependency(connectable)

    def grant_role_access(self, role: Optional[iam.IRole], permissions: Literal["r", "rw"] = "rw"):
        self.add_managed_policies(role, "AmazonElasticFileSystemReadOnlyAccess")
        if "w" in permissions:
            self.add_managed_policies(role, "AmazonElasticFileSystemClientReadWriteAccess")

    def grant_grantable_access(
        self, grantable: iam.IGrantable, permissions: Literal["r", "rw"] = "rw"
    ):
        actions = []
        if "w" in permissions:
            actions.append("elasticfilesystem:ClientWrite")
        self.file_system.grant(grantable, *actions)

    def grant_lambda_access(self, resource: lambda_.Function):
        resource.add_environment(
            EFS_MOUNT_PATH_VAR, self.as_lambda_file_system.config.local_mount_path
        )
        self.grant_grantable_access(resource)
        self.grant_role_access(resource.role)
        self.grant_connectable_access(resource)

    def repair_connectable_efs_dependency(self, connectable: ec2.IConnectable):
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
        for efs_sg in self.file_system.connections.security_groups:
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
        connections.allow_to_default_port(self.file_system)

    @staticmethod
    def _is_grantable(resource: Any) -> bool:
        # TODO: figure out better way
        return hasattr(resource, "grant_principal")

    @staticmethod
    def _is_connectable(resource: Any) -> bool:
        try:
            return isinstance(resource.connections, ec2.Connections)
        except AttributeError:
            return False
