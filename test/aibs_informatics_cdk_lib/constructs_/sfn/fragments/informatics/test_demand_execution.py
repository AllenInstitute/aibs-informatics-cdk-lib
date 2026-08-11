from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_efs as efs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn

from aibs_informatics_cdk_lib.constructs_.efs.file_system import MountPointConfiguration
from aibs_informatics_cdk_lib.constructs_.sfn.fragments.informatics.demand_execution import (
    DemandExecutionFragment,
)
from test.aibs_informatics_cdk_lib.base import CdkBaseTest


class DemandExecutionFragmentTests(CdkBaseTest):
    def get_stack_with_fixtures(
        self,
    ) -> tuple[cdk.Stack, s3.Bucket, sfn.StateMachine, sfn.StateMachine]:
        stack = self.get_dummy_stack("DemandExecution")
        bucket = s3.Bucket(stack, "bucket", bucket_name="test-bucket")
        batch_invoked_lambda_state_machine = sfn.StateMachine(
            stack,
            "bisl-sm",
            definition_body=sfn.DefinitionBody.from_chainable(sfn.Pass(stack, "bisl-pass")),
        )
        data_sync_state_machine = sfn.StateMachine(
            stack,
            "data-sync-sm",
            definition_body=sfn.DefinitionBody.from_chainable(sfn.Pass(stack, "data-sync-pass")),
        )
        return stack, bucket, batch_invoked_lambda_state_machine, data_sync_state_machine

    def import_access_point(
        self, stack: cdk.Stack, name: str, access_point_id: str, file_system_id: str
    ) -> efs.IAccessPoint:
        file_system = efs.FileSystem.from_file_system_attributes(
            stack,
            f"{name}-fs",
            file_system_id=file_system_id,
            security_group=ec2.SecurityGroup.from_security_group_id(
                stack, f"{name}-sg", "sg-123456789012"
            ),
        )
        return efs.AccessPoint.from_access_point_attributes(
            stack, name, access_point_id=access_point_id, file_system=file_system
        )

    def create_fragment(
        self,
        stack: cdk.Stack,
        bucket: s3.Bucket,
        batch_invoked_lambda_state_machine: sfn.StateMachine,
        data_sync_state_machine: sfn.StateMachine,
        **kwargs,
    ) -> DemandExecutionFragment:
        return DemandExecutionFragment(
            stack,
            "demand-execution",
            env_base=self.env_base,
            aibs_informatics_docker_asset="123456789012.dkr.ecr.us-west-2.amazonaws.com/image:latest",
            scaffolding_bucket=bucket,
            scaffolding_job_queue="scaffolding-queue",
            batch_invoked_lambda_state_machine=batch_invoked_lambda_state_machine,
            data_sync_state_machine=data_sync_state_machine,
            **kwargs,
        )

    def get_request_file_system_configurations(self, fragment: DemandExecutionFragment) -> dict:
        start_state = cast(sfn.Pass, fragment.node.try_find_child("Start Demand Batch Task"))
        assert start_state is not None
        state_json = start_state.to_state_json()
        return state_json["Parameters"]["request"]["file_system_configurations"]

    def get_scaffolding_pass_state_json(self, fragment: DemandExecutionFragment) -> dict:
        pass_state = cast(
            sfn.Pass, fragment.node.try_find_child("Pass: Prepare Demand Scaffolding")
        )
        assert pass_state is not None
        return pass_state.to_state_json()

    def test__init__single_candidates__emits_singular_request(self):
        stack, bucket, bisl_sm, ds_sm = self.get_stack_with_fixtures()
        shared = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "shared-ap", "fsap-11111111", "fs-11111111"),
            "/opt/shared",
            read_only=True,
        )
        scratch = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "scratch-ap", "fsap-22222222", "fs-11111111"),
            "/opt/scratch",
        )

        fragment = self.create_fragment(
            stack,
            bucket,
            bisl_sm,
            ds_sm,
            shared_mount_point_config=shared,
            scratch_mount_point_config=scratch,
        )

        file_system_configurations = self.get_request_file_system_configurations(fragment)
        assert file_system_configurations == {
            "shared": {
                "file_system": "fs-11111111",
                "access_point": "fsap-11111111",
                "container_path": "/opt/shared",
            },
            "scratch": {
                "file_system": "fs-11111111",
                "access_point": "fsap-22222222",
                "container_path": "/opt/scratch",
            },
        }

        scaffolding_state_json = self.get_scaffolding_pass_state_json(fragment)
        mount_points = scaffolding_state_json["Parameters"]["mount_points"]
        assert [mp["SourceVolume"] for mp in mount_points] == ["shared", "scratch"]

    def test__init__multiple_scratch_candidates__emits_candidate_lists(self):
        stack, bucket, bisl_sm, ds_sm = self.get_stack_with_fixtures()
        shared = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "shared-ap", "fsap-11111111", "fs-11111111"),
            "/opt/fsap-11111111/shared",
            read_only=True,
        )
        scratch_configs = [
            MountPointConfiguration.from_access_point(
                self.import_access_point(
                    stack, f"scratch-ap-{i}", f"fsap-2222222{i}", f"fs-1111111{i}"
                ),
                f"/opt/fsap-2222222{i}/scratch",
            )
            for i in range(2)
        ]

        fragment = self.create_fragment(
            stack,
            bucket,
            bisl_sm,
            ds_sm,
            shared_mount_point_config=[shared],
            scratch_mount_point_config=scratch_configs,
        )

        file_system_configurations = self.get_request_file_system_configurations(fragment)
        assert file_system_configurations == {
            "selection_strategy": "RANDOM",
            "shared": [
                {
                    "file_system": "fs-11111111",
                    "access_point": "fsap-11111111",
                    "container_path": "/opt/fsap-11111111/shared",
                }
            ],
            "scratch": [
                {
                    "file_system": "fs-11111110",
                    "access_point": "fsap-22222220",
                    "container_path": "/opt/fsap-22222220/scratch",
                },
                {
                    "file_system": "fs-11111111",
                    "access_point": "fsap-22222221",
                    "container_path": "/opt/fsap-22222221/scratch",
                },
            ],
        }

        scaffolding_state_json = self.get_scaffolding_pass_state_json(fragment)
        mount_points = scaffolding_state_json["Parameters"]["mount_points"]
        volumes = scaffolding_state_json["Parameters"]["volumes"]
        assert [mp["SourceVolume"] for mp in mount_points] == ["shared0", "scratch0", "scratch1"]
        assert [v["Name"] for v in volumes] == ["shared0", "scratch0", "scratch1"]
        assert {
            v["EfsVolumeConfiguration"]["AuthorizationConfig"]["AccessPointId"] for v in volumes
        } == {"fsap-11111111", "fsap-22222220", "fsap-22222221"}

    def test__init__explicit_selection_strategy__emits_lists_even_for_single_candidates(self):
        stack, bucket, bisl_sm, ds_sm = self.get_stack_with_fixtures()
        shared = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "shared-ap", "fsap-11111111", "fs-11111111"),
            "/opt/shared",
            read_only=True,
        )
        scratch = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "scratch-ap", "fsap-22222222", "fs-11111111"),
            "/opt/scratch",
        )

        fragment = self.create_fragment(
            stack,
            bucket,
            bisl_sm,
            ds_sm,
            shared_mount_point_config=shared,
            scratch_mount_point_config=scratch,
            file_system_selection_strategy="RANDOM",
        )

        file_system_configurations = self.get_request_file_system_configurations(fragment)
        assert file_system_configurations["selection_strategy"] == "RANDOM"
        assert isinstance(file_system_configurations["shared"], list)
        assert isinstance(file_system_configurations["scratch"], list)

    def test__init__duplicate_mount_points__raises(self):
        stack, bucket, bisl_sm, ds_sm = self.get_stack_with_fixtures()
        shared = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "shared-ap", "fsap-11111111", "fs-11111111"),
            "/opt/efs",
            read_only=True,
        )
        scratch_configs = [
            MountPointConfiguration.from_access_point(
                self.import_access_point(
                    stack, f"scratch-ap-{i}", f"fsap-2222222{i}", f"fs-1111111{i}"
                ),
                "/opt/scratch",
            )
            for i in range(2)
        ]

        with self.assertRaisesRegex(ValueError, "unique"):
            self.create_fragment(
                stack,
                bucket,
                bisl_sm,
                ds_sm,
                shared_mount_point_config=shared,
                scratch_mount_point_config=scratch_configs,
            )

    def test__init__empty_scratch_candidates__raises(self):
        stack, bucket, bisl_sm, ds_sm = self.get_stack_with_fixtures()
        shared = MountPointConfiguration.from_access_point(
            self.import_access_point(stack, "shared-ap", "fsap-11111111", "fs-11111111"),
            "/opt/shared",
            read_only=True,
        )

        with self.assertRaisesRegex(ValueError, "scratch"):
            self.create_fragment(
                stack,
                bucket,
                bisl_sm,
                ds_sm,
                shared_mount_point_config=shared,
                scratch_mount_point_config=[],
            )
