# flake8: noqa

import base64
import json
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, TypeVar

import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.constructs_.batch.types import IBatchEnvironmentDescriptor
from aibs_informatics_cdk_lib.constructs_.cw import (
    AlarmMetricConfig,
    GraphMetricConfig,
    GroupedGraphMetricConfig,
)
from aibs_informatics_cdk_lib.constructs_.s3 import EnvBaseBucket

T = TypeVar("T", bound="BatchLaunchTemplateUserData")


class IBatchLaunchTemplateBuilder(EnvBaseConstruct, Generic[T]):
    def grant_instance_role_permissions(self, instance_role: iam.Role):
        pass

    @abstractmethod
    def create_launch_template(
        self,
        descriptor: IBatchEnvironmentDescriptor,
        security_group: Optional[ec2.SecurityGroup] = None,
        **kwargs,
    ) -> ec2.LaunchTemplate:
        raise NotImplementedError()


class BatchLaunchTemplateBuilder(IBatchLaunchTemplateBuilder["BatchLaunchTemplateUserData"]):
    def grant_instance_role_permissions(self, instance_role: iam.Role):
        pass

    def create_launch_template(
        self,
        descriptor: IBatchEnvironmentDescriptor,
        security_group: Optional[ec2.SecurityGroup] = None,
        python_version: str = "python3.11",
        **kwargs,
    ) -> ec2.LaunchTemplate:
        user_data = ec2.UserData.custom(
            BatchLaunchTemplateUserData(
                env_base=self.env_base,
                batch_env_name=descriptor.get_name(),
                python_version=python_version,
            ).user_data_text
        )

        launch_template_name = f"{self.env_base}-{descriptor.get_name()}-launch-template"

        launch_template = ec2.LaunchTemplate(
            self,
            launch_template_name,
            # NOTE: unsetting because of complications when multiple batch environments are created
            # launch_template_name=launch_template_name,
            instance_initiated_shutdown_behavior=ec2.InstanceInitiatedShutdownBehavior.TERMINATE,
            security_group=security_group,
            user_data=user_data,
        )
        return launch_template


class EbsBatchLaunchTemplateBuilder(IBatchLaunchTemplateBuilder["EbsBatchLaunchTemplateUserData"]):
    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        env_base: EnvBase,
        assets_bucket: EnvBaseBucket,
    ) -> None:
        super().__init__(scope, id, env_base)

        self.assets_bucket = assets_bucket

        self.docker_volume_device_name = "/dev/xvdba"

    def grant_instance_role_permissions(self, instance_role: iam.Role):
        pass

    def create_launch_template(
        self,
        descriptor: IBatchEnvironmentDescriptor,
        security_group: Optional[ec2.SecurityGroup] = None,
        python_version: str = "python3.11",
        **kwargs,
    ) -> ec2.LaunchTemplate:
        user_data = ec2.UserData.custom(
            EbsBatchLaunchTemplateUserData(
                env_base=self.env_base,
                batch_env_name=descriptor.get_name(),
                docker_volume_device_name=self.docker_volume_device_name,
                python_version=python_version,
            ).user_data_text
        )

        launch_template_name = f"{self.env_base}-{descriptor}-launch-template"
        launch_template = ec2.LaunchTemplate(
            self,
            launch_template_name,
            launch_template_name=launch_template_name,
            block_devices=[
                ec2.BlockDevice(
                    device_name=self.docker_volume_device_name,
                    volume=ec2.BlockDeviceVolume.ebs(
                        30,
                        encrypted=True,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        iops=3000,
                    ),
                ),
            ],
            user_data=user_data,
            instance_initiated_shutdown_behavior=ec2.InstanceInitiatedShutdownBehavior.TERMINATE,
            security_group=security_group,
        )
        return launch_template


@dataclass
class BatchLaunchTemplateUserData:
    env_base: EnvBase
    batch_env_name: str
    python_version: str
    user_data_text: str = field(init=False)

    def __post_init__(self):
        self.user_data_text = DEFAULT_LAUNCH_TEMPLATE_USER_DATA.format(
            python_version=self.python_version,
            config_json=self.config_builder.to_string(),
        )

    @property
    def config_builder(self) -> "CloudWatchConfigBuilder":
        return CloudWatchConfigBuilder(self.env_base, self.batch_env_name)

    def get_base64_user_data(self) -> str:
        return base64.b64encode(self.user_data_text.encode("ascii")).decode("ascii")


@dataclass
class EbsBatchLaunchTemplateUserData(BatchLaunchTemplateUserData):
    docker_volume_device_name: str
    python_version: str

    def __post_init__(self):
        self.user_data_text = EBS_AUTOSCALE_LAUNCH_TEMPLATE_USER_DATA.format(
            device_name=self.docker_volume_device_name,
            config_json=self.config_builder.to_string(),
            python_version=self.python_version,
        )


@dataclass
class CloudWatchConfigBuilder:
    env_base: EnvBase
    batch_env_name: str

    @property
    def metric_namespace(self) -> str:
        return self.env_base.get_metric_namespace("CWAgent")

    def to_json(self) -> Dict[str, Any]:
        """Builds a CW Agent config

        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html

        Returns:
            Dict[str, Any]: config
        """
        return {
            "agent": {
                "metrics_collection_interval": 60,
                "logfile": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log",
            },
            "logs": self.get_logs_config(),
            "metrics": self.get_metrics_config(),
        }

    def to_string(self) -> str:
        config = self.to_json()
        return base64.b64encode(json.dumps(config).encode("ascii")).decode("ascii")

    def get_logs_config(self) -> Dict[str, Any]:
        """Generates CW Agent sub config for logs

        https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html#CloudWatch-Agent-Configuration-File-Logssection

        Returns:
            Dict[str, Any]: CW logs config
        """
        env_base = self.env_base
        return {
            "logs_collected": {
                "files": {
                    "collect_list": [
                        {
                            "file_path": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/amazon-cloudwatch-agent.log",
                        },
                        {
                            "file_path": "/var/log/cloud-init.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/cloud-init.log",
                        },
                        {
                            "file_path": "/var/log/cloud-init-output.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/cloud-init-output.log",
                        },
                        {
                            "file_path": "/var/log/ecs/ecs-init.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/ecs-init.log",
                        },
                        {
                            "file_path": "/var/log/ecs/ecs-agent.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/ecs-agent.log",
                        },
                        {
                            "file_path": "/var/log/ecs/ecs-volume-plugin.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/ecs-volume-plugin.log",
                        },
                        {
                            "file_path": "/var/log/ebs-autoscale-install.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/ebs-autoscale-install.log",
                        },
                        {
                            "file_path": "/var/log/ebs-autoscale.log",
                            "log_group_name": f"/aws/ecs/container-instance/{env_base}",
                            "log_stream_name": f"/aws/ecs/container-instance/{env_base}/{{instance_id}}/ebs-autoscale.log",
                        },
                    ]
                }
            }
        }

    def get_metrics_config(self) -> Dict[str, Any]:
        """Generates metrics config section of CW Agent config

        https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html#CloudWatch-Agent-Configuration-File-Metricssection

        Returns:
            Dict[str, Any]: metrics config
        """
        return {
            "namespace": self.metric_namespace,
            "metrics_collected": self._get_metrics_collected(),
            "append_dimensions": {
                "ImageId": "${aws:ImageId}",
                "InstanceId": "${aws:InstanceId}",
                "InstanceType": "${aws:InstanceType}",
                "AutoScalingGroupName": "${aws:AutoScalingGroupName}",
            },
            "aggregation_dimensions": [
                ["AutoScalingGroupName"],
                ["InstanceId", "InstanceType"],
                ["env_base", "batch_env_name"],
                [],
            ],
        }

    def _get_metrics_collected(self) -> Dict[str, Any]:
        metrics_collected: Dict[str, Any] = {}
        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/metrics-collected-by-CloudWatch-agent.html

        DEFAULT_COLLECTION_INTERVAL = 60 * 1
        DEFAULT_APPEND_DIMENSIONS = {
            "env_base": self.env_base,
            "batch_env_name": self.batch_env_name,
        }

        metrics_collected["cpu"] = {
            "measurement": ["cpu_usage_active"],
            "resources": ["*"],
            "metrics_collection_interval": DEFAULT_COLLECTION_INTERVAL,
            "append_dimensions": DEFAULT_APPEND_DIMENSIONS,
        }

        metrics_collected["disk"] = {
            "measurement": [
                "disk_used_percent",
            ],
            "resources": ["*"],
            "metrics_collection_interval": DEFAULT_COLLECTION_INTERVAL,
            "append_dimensions": DEFAULT_APPEND_DIMENSIONS,
        }

        metrics_collected["mem"] = {
            "measurement": [
                "mem_used_percent",
                "mem_used",
                "mem_free",
            ],
            "metrics_collection_interval": DEFAULT_COLLECTION_INTERVAL,
            "append_dimensions": DEFAULT_APPEND_DIMENSIONS,
        }
        metrics_collected["net"] = {
            "measurement": [
                "net_bytes_recv",
                "net_bytes_sent",
            ],
            "metrics_collection_interval": DEFAULT_COLLECTION_INTERVAL,
            "append_dimensions": DEFAULT_APPEND_DIMENSIONS,
        }

        return metrics_collected

    def get_grouped_graph_metric_configs(self) -> List[GroupedGraphMetricConfig]:
        PERC_ALARM_METRIC_FN = lambda name, threshold: AlarmMetricConfig(
            name=f"{self.batch_env_name}-{name}",
            threshold=threshold,
            evaluation_periods=3,
            datapoints_to_alarm=3,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        graph_metric_configs = [
            GroupedGraphMetricConfig(
                title="CPU Usage",
                namespace=self.metric_namespace,
                metrics=[
                    GraphMetricConfig(
                        metric="cpu_usage_active",
                        statistic="AVG",
                        alarm=PERC_ALARM_METRIC_FN("cpu_usage_active", 95),
                    )
                ],
            ),
            GroupedGraphMetricConfig(
                title="Memory Usage",
                namespace=self.metric_namespace,
                metrics=[
                    GraphMetricConfig(
                        metric="mem_used_percent",
                        statistic="AVG",
                        alarm=PERC_ALARM_METRIC_FN("mem_used_percent", 95),
                    )
                ],
            ),
            GroupedGraphMetricConfig(
                title="Network Traffic",
                namespace=self.metric_namespace,
                metrics=[
                    GraphMetricConfig(metric="net_bytes_recv", statistic="SUM"),
                    GraphMetricConfig(metric="net_bytes_sent", statistic="SUM"),
                ],
            ),
        ]
        return graph_metric_configs


DEFAULT_LAUNCH_TEMPLATE_USER_DATA = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==BOUNDARY=="

--==BOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

#cloud-config
repo_update: true
repo_upgrade: security
apt:
  sources:
    deadsnakes-ppa-source:
      source: "ppa:deadsnakes/ppa"

packages:
- jq
- btrfs-progs
- sed
- git
- {python_version}
- {python_version}-venv
- amazon-efs-utils
- amazon-ssm-agent
- unzip
- amazon-cloudwatch-agent

write_files:
- permissions: "0644"
  encoding: b64
  path: /opt/aws/amazon-cloudwatch-agent/etc/config.json
  content: {config_json}

runcmd:
- systemctl start amazon-ssm-agent

# Start aws cloudwatch agent
- /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json

# enable ecs spot instance draining
- echo ECS_ENABLE_SPOT_INSTANCE_DRAINING=true >> /etc/ecs/ecs.config

# pull docker images
- echo ECS_IMAGE_PULL_BEHAVIOR=default >> /etc/ecs/ecs.config

# Enable ECS Metadata file
- echo ECS_ENABLE_CONTAINER_METADATA=true >> /etc/ecs/ecs.config

# Install lustre
- amazon-linux-extras install lustre -y

--==BOUNDARY==--"""


EBS_AUTOSCALE_LAUNCH_TEMPLATE_USER_DATA = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==BOUNDARY=="

--==BOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

#cloud-config
repo_update: true
repo_upgrade: security
apt:
  sources:
    deadsnakes-ppa-source:
      source: "ppa:deadsnakes/ppa"

packages:
- jq
- btrfs-progs
- sed
- git
- {python_version}
- {python_version}-venv
- amazon-efs-utils
- amazon-ssm-agent
- unzip
- amazon-cloudwatch-agent

write_files:
- permissions: "0644"
  encoding: b64
  path: /opt/aws/amazon-cloudwatch-agent/etc/config.json
  content: {config_json}

runcmd:
- systemctl start amazon-ssm-agent

# Start aws cloudwatch agent
- /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json

# install aws-cli v2 and copy the static binary in an easy to find location for bind-mounts into containers
- mkdir -p /opt/aws-cli/bin
- curl -s --fail --retry 3 --retry-connrefused "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install -b /usr/bin && cp -a -f $(dirname $(find /usr/local/aws-cli -name 'aws' -type f))/. /opt/aws-cli/bin/
- command -v aws || sleep 5 && curl -s --fail --retry 3 --retry-connrefused "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install -b /usr/bin && cp -a -f $(dirname $(find /usr/local/aws-cli -name 'aws' -type f))/. /opt/aws-cli/bin/
- command -v aws || sleep 10 && curl -s --fail --retry 3 --retry-connrefused "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install -b /usr/bin && cp -a -f $(dirname $(find /usr/local/aws-cli -name 'aws' -type f))/. /opt/aws-cli/bin/
- command -v aws || echo "Unable to install AWS CLI v2"

## Enable EBS Autoscale
- EBS_AUTOSCALE_VERSION=$(curl --silent "https://api.github.com/repos/awslabs/amazon-ebs-autoscale/releases/latest" | jq -r .tag_name)
- cd /opt && git clone https://github.com/awslabs/amazon-ebs-autoscale.git
- cd /opt/amazon-ebs-autoscale && git checkout $EBS_AUTOSCALE_VERSION
- cp -au /var/lib/docker /var/lib/docker.bk
- rm -rf /var/lib/docker/*
- sh /opt/amazon-ebs-autoscale/install.sh -d {device_name} -f btrfs --volume-type gp3 --volume-iops 10000 --initial-utilization-threshold 65 --max-ebs-volume-size 500 -m /var/lib/docker 2>&1 > /var/log/ebs-autoscale-install.log
- docker_storage_options="DOCKER_STORAGE_OPTIONS=\"--storage-driver btrfs\""
- awk -v docker_storage_options="$docker_storage_options" \
'{{ sub(/DOCKER_STORAGE_OPTIONS=.*/, docker_storage_options); print }}' \
/etc/sysconfig/docker-storage \
> /opt/amazon-ebs-autoscale/docker-storage
- mv -f /opt/amazon-ebs-autoscale/docker-storage /etc/sysconfig/docker-storage
- cp -au /var/lib/docker.bk/* /var/lib/docker

# enable ecs spot instance draining
- echo ECS_ENABLE_SPOT_INSTANCE_DRAINING=true >> /etc/ecs/ecs.config

# pull docker images
- echo ECS_IMAGE_PULL_BEHAVIOR=default >> /etc/ecs/ecs.config

# Enable ECS Metadata file
- echo ECS_ENABLE_CONTAINER_METADATA=true >> /etc/ecs/ecs.config

--==BOUNDARY==--"""
