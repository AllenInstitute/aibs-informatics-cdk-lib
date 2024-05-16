from aibs_informatics_test_resources import BaseTest

from aibs_informatics_cdk_lib.project.config import (
    BaseModel,
    BaseProjectConfig,
    CodePipelineBuildConfig,
    CodePipelineNotificationsConfig,
    CodePipelineSourceConfig,
    ConfigProvider,
    Env,
    EnvBase,
    EnvType,
    GlobalConfig,
    PipelineConfig,
    ProjectConfig,
    StageConfig,
)


class EnvTests(BaseTest):
    def test__model_validate__parses_simple_dict(self):
        env_dict = {
            "env_type": "prod",
            "label": "marmot",
            "account": "12345678910",
            "region": "us-west-2",
        }
        expected_env = Env(
            env_type=EnvType.PROD,
            label="marmot",
            account="12345678910",
            region="us-west-2",
        )
        parsed_env = Env.model_validate(env_dict)
        self.assertEqual(expected_env, parsed_env)

    def test__model_validate__parses_dict_with_env_fields(self):
        self.set_env_vars(
            *list(dict(CUSTOM_LABEL="marmot", CUSTOM_ACCOUNT_ID="123123123").items())
        )
        env_dict = {
            "env_type": "prod",
            "label": "${CUSTOM_LABEL}",
            "account": "$CUSTOM_ACCOUNT_ID",
            "region": "{CUSTOM_REGION}",
        }

        expected_env = Env(
            env_type=EnvType.PROD,
            label="marmot",
            account="123123123",
            region="{CUSTOM_REGION}",
        )
        parsed_env = Env.model_validate(env_dict)
        self.assertEqual(expected_env, parsed_env)

    def test__model_validate__invalid_dict__FAILS(self):
        with self.assertRaises(Exception):
            Env.model_validate({"env_type": "not_valid"})

    def test__env_base__includes_label_when_present_and_not_otherwise(self):
        env_with_label = Env(env_type=EnvType.PROD, label="marmot")
        self.assertEqual(env_with_label.env_base, EnvBase("prod-marmot"))

        env_no_label = Env(env_type=EnvType.PROD)
        self.assertEqual(env_no_label.env_base, EnvBase("prod"))

    def test__is_configured__works_as_intended(self):
        env_with_label = Env(env_type=EnvType.PROD, account="123123")
        self.assertTrue(env_with_label.is_configured)

        env_no_label = Env(env_type=EnvType.PROD)
        self.assertFalse(env_no_label.is_configured)


def create_global_config() -> GlobalConfig:
    return GlobalConfig(pipeline_name="pipeline_name", stage_promotions={})


def create_stage_config() -> StageConfig:
    return StageConfig(
        env=Env(
            env_type=EnvType.PROD,
            label="gcs",
            account="123123123",
            region="us-west-2",
        ),
        pipeline=PipelineConfig(
            enable=True,
            build=CodePipelineBuildConfig(
                ssh_key_secret_name="ssh-deploy-key",
                docker_hub_credentials_secret_name="docker-hub-credentials",
            ),
            source=CodePipelineSourceConfig(
                repository="repo",
                branch="",
                codestar_connection="2d4b8e0e-e3c0-4909-aaa1-50c935acd6ec",
                oauth_secret_name="asdf",
            ),
            notifications=CodePipelineNotificationsConfig(slack_channel_configuration_arn=None),
        ),
    )


class MyStageServiceConfig(BaseModel):
    service_name: str


class ExtendedStageConfig(StageConfig):
    service: MyStageServiceConfig


class ExtendedProjectConfig(BaseProjectConfig[GlobalConfig, ExtendedStageConfig]):
    pass


class ProjectConfigTests(BaseTest):
    def test__get_stage_config__simple_override(self):
        global_config = create_global_config()
        default_config = create_stage_config()

        default_config_overrides = {
            EnvType.DEV: {
                "env": {
                    "env_type": "dev",
                    "label": "marmot",
                    "account": "111222333",
                }
            }
        }
        proj_config = ProjectConfig(
            global_config=global_config,
            default_config=default_config,
            default_config_overrides=default_config_overrides,
        )

        expected_config = StageConfig.model_validate(
            {
                **default_config.model_copy().model_dump(exclude_unset=True),
            }
        )
        expected_config.env.env_type = EnvType.DEV
        expected_config.env.label = "marmot"
        expected_config.env.account = "111222333"

        resolved_config = proj_config.get_stage_config("dev")

        self.assertEqual(resolved_config, expected_config)

    def test__parse_file__test_loads_json_and_yml(self):
        # Load original file
        proj_config = ProjectConfig(
            global_config=create_global_config(),
            default_config=create_stage_config(),
            default_config_overrides={},
        )
        proj_config_json_path = self.tmp_path() / "project.json"
        proj_config_json_path.write_text(proj_config.model_dump_json())
        another_proj_config = ProjectConfig.load_config(proj_config_json_path)
        self.assertEqual(proj_config, another_proj_config)


class ConfigProviderTests(BaseTest):
    def test__get_stage_config__fails_with_invalid_env_type(self):
        proj_config = ProjectConfig(
            global_config=create_global_config(),
            default_config=create_stage_config(),
            default_config_overrides={EnvType.DEV: {}, EnvType.TEST: {}},
        )
        proj_config_json_path = self.tmp_path() / "project.json"
        proj_config_json_path.write_text(proj_config.model_dump_json())

        with self.assertRaises(ValueError):
            ConfigProvider.get_stage_config("invalid", proj_config_json_path)

    def test__get_stage_config__simple_check(self):
        proj_config = ProjectConfig(
            global_config=create_global_config(),
            default_config=create_stage_config(),
            default_config_overrides={EnvType.DEV: {}, EnvType.TEST: {}},
        )
        proj_config_json_path = self.tmp_path() / "project.json"
        proj_config_json_path.write_text(proj_config.model_dump_json())

        # no overrides means they should be equal
        self.assertEqual(
            ConfigProvider.get_stage_config("dev", path=proj_config_json_path),
            ConfigProvider.get_stage_config("test", path=proj_config_json_path),
        )

    def test__get_stage_config__override_default_project_config_cls(self):
        extended_stage_config = ExtendedStageConfig(
            **create_stage_config().model_dump(exclude_unset=True),
            service=MyStageServiceConfig(service_name="my-service"),
        )
        proj_config = ExtendedProjectConfig(
            global_config=create_global_config(),
            default_config=extended_stage_config,
            default_config_overrides={EnvType.DEV: {}, EnvType.TEST: {}},
        )
        proj_config_json_path = self.tmp_path() / "project.json"
        proj_config_json_path.write_text(proj_config.model_dump_json())

        stage_config = ConfigProvider.get_stage_config(
            "dev", path=proj_config_json_path, project_config_cls=ExtendedProjectConfig
        )
        self.assertEqual(stage_config.service.service_name, "my-service")
