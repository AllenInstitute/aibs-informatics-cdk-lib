from typing import Mapping, Optional, Sequence

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ec2 as ec2

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstructMixins


class EnvBaseVpc(ec2.Vpc, EnvBaseConstructMixins):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        max_azs: int = 6,
        ip_addresses: ec2.IpAddresses = ec2.IpAddresses.cidr("10.10.0.0/16"),
        subnet_configuration: Optional[Sequence[ec2.SubnetConfiguration]] = None,
        include_public_subnet: bool = True,
        include_private_subnet: bool = True,
        gateway_endpoints: Optional[Mapping[str, ec2.GatewayVpcEndpointOptions]] = None,
        include_default_endpoints: bool = True,
        include_default_interface_endpoints: bool = True,
        flow_logs: Optional[Mapping[str, ec2.FlowLogOptions]] = None,
        include_default_flow_logs: bool = True,
        nat_gateway_provider: Optional[ec2.NatProvider] = None,
        nat_gateways: Optional[int] = None,
        **kwargs,
    ):
        self.env_base = env_base

        subnet_configuration = list(subnet_configuration or [])
        if include_public_subnet and not any(
            [_ for _ in subnet_configuration if _.subnet_type == ec2.SubnetType.PUBLIC]
        ):
            subnet_configuration.append(
                ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PUBLIC, name="Public")
            )
        if include_private_subnet and not any(
            [
                _
                for _ in subnet_configuration
                if _.subnet_type == ec2.SubnetType.PRIVATE_WITH_EGRESS
            ]
        ):
            subnet_configuration.append(
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS, name="Private"
                )
            )

        gateway_endpoints = dict(gateway_endpoints or {})
        if include_default_endpoints:
            if "s3_endpoint" not in gateway_endpoints:
                gateway_endpoints["s3_endpoint"] = ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            if "dynamodb_endpoint" not in gateway_endpoints:
                gateway_endpoints["dynamodb_endpoint"] = ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
                )

        flow_logs = dict(flow_logs or {})
        if include_default_flow_logs:
            if "CW_flow_log" not in flow_logs:
                flow_logs["CW_flow_log"] = ec2.FlowLogOptions(
                    destination=ec2.FlowLogDestination.to_cloud_watch_logs(),
                    traffic_type=ec2.FlowLogTrafficType.ALL,
                )

        super().__init__(
            scope,
            id,
            max_azs=max_azs,
            ip_addresses=ip_addresses,
            subnet_configuration=subnet_configuration,
            gateway_endpoints=gateway_endpoints,
            flow_logs=flow_logs,
            nat_gateway_provider=nat_gateway_provider,
            nat_gateways=nat_gateways,
            **kwargs,
        )

        if include_default_interface_endpoints:
            self.add_interface_endpoint(
                "ecr",
                service=ec2.InterfaceVpcEndpointAwsService.ECR,
            )
            self.add_interface_endpoint(
                "ecr_docker",
                service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
            )

        cdk.CfnOutput(self, "VpcId", value=self.vpc_id)

    def as_reference(self, scope: constructs.Construct, id: str) -> ec2.IVpc:
        """Generates a VPC reference"""
        return ec2.Vpc.from_lookup(scope, id, vpc_id=self.vpc_id)
