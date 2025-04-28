import json
from typing import Optional, cast

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_route53, aws_route53_targets

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct


class LimsConnectionConstruct(EnvBaseConstruct):
    """This construct takes as input an AWS ec2.VPC and attaches a "VPC interface endpoint"
    that allows connections to another account/vpc with an on-prem LIMS2 connection.

    vpc_endpoint_service_name should be the DNS name of the service running the LIMS2 connection
    and should look something like: "com.amazonaws.vpce.{region}.vpce-svc-{service_id}"
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        target_vpc: ec2.Vpc,
        vpc_endpoint_service_name: str,
        **kwargs,
    ):
        super().__init__(scope, id, env_base, **kwargs)

        self.vpc_endpoint_service_name = vpc_endpoint_service_name
        self.target_vpc = target_vpc
        self.add_lims_vpc_endpoint()
        self.add_lims_vpc_endpoint_dns_alias()

    def add_lims_vpc_endpoint(self):
        """Add a VPC endpont to our target_vpc that connects to the LIMS2 endpoint
        service (located in another AWS account/VPC managed by the cloud infra team).

        Useful documentation:
        https://alleninstitute.atlassian.net/wiki/spaces/IT/pages/740360228/Accessing+LIMS2+from+AWS
        https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html#connect-to-endpoint-service
        https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_ec2-readme.html#vpc-endpoints

        Pricing:
        https://aws.amazon.com/privatelink/pricing/ (see: Interface Endpoint pricing section)
        """

        # NOTE: Currently this endpoint only gets associated with 1 subnet because the
        #       endpoint we deploy in our VPC can only be deployed in the same AZs as the
        #       source Service Endpoint. If we would like to have our InterfaceVpcEndpoint
        #       available in multiple AZs, we would need to request cloud infra team to
        #       increase the number of AZs that the source service is deployed to.
        #       See this related question: https://stackoverflow.com/questions/60081850/
        self.lims_vpc_endpoint = ec2.InterfaceVpcEndpoint(
            scope=self,
            id="External LIMS2 Network Load Balancer VPC Endpoint",
            # Obtained from cloud infra team, any changes on their end we also need to update here
            service=ec2.InterfaceVpcEndpointService(name=self.vpc_endpoint_service_name),
            vpc=self.target_vpc,
            open=True,
            lookup_supported_azs=True,
            subnets=ec2.SubnetSelection(one_per_az=True, subnets=self.target_vpc.private_subnets),
        )

        # Need to enable port 80 and 5432 (postgres) for this endpoint
        # https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_ec2-readme.html#allowing-connections
        self.lims_vpc_endpoint.connections.allow_from_any_ipv4(
            port_range=ec2.Port.POSTGRES, description="Postgres port 5432"
        )
        self.lims_vpc_endpoint.connections.allow_from_any_ipv4(
            port_range=ec2.Port.HTTP, description="HTTP port 80"
        )

    def add_lims_vpc_endpoint_dns_alias(self):
        """Add a route53 private hosted zone DNS resolver that will allow us to contact the
        LIMS2 VPC endpoint service using a less unwieldy DNS name.

        Useful documentation:
        https://stackoverflow.com/a/78258885

        Pricing:
        https://aws.amazon.com/route53/pricing/
        """

        # This costs money, but at our usage levels it shouldn't be an issue
        self.private_hosted_zone = aws_route53.PrivateHostedZone(
            scope=self,
            id="Lims2VpcEndpointPrivateHostedZone",
            zone_name="lims2.corp.alleninstitute.org",
            vpc=self.target_vpc,
            comment=(
                "A route53 private hosted zone that contains an Alias that can resolve DNS "
                "queries to `lims2.corp.alleninstitute.org` to the InterfaceVpcEndpoint hosting the LIMS2 "
                "VPC endpoint service"
            ),
        )

        # Use an ARecord (Alias Record) instead of a CRecord because those cost money per query
        alias_record_target = cast(
            aws_route53.IAliasRecordTarget,
            aws_route53_targets.InterfaceVpcEndpointTarget(self.lims_vpc_endpoint),
        )
        self.lims2_vpc_endpoint_arecord = aws_route53.ARecord(
            scope=self,
            id="Lims2VpcEndpointAliasRecord",
            target=aws_route53.RecordTarget.from_alias(alias_target=alias_record_target),
            zone=self.private_hosted_zone,
            delete_existing=True,
        )
