from typing import Optional

import aws_cdk as cdk
from aibs_informatics_core.env import ENV_BASE_KEY, EnvBase, EnvType
from aibs_informatics_test_resources import BaseTest as _BaseTest
from aibs_informatics_test_resources import does_not_raise
from aibs_informatics_test_resources import reset_environ_after_test as reset_environ_after_test
from aws_cdk.assertions import Template

from aibs_informatics_cdk_lib.constructs_.base import EnvBaseConstruct
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class BaseTest(_BaseTest):
    @property
    def env_base(self) -> EnvBase:
        if not hasattr(self, "_env_base"):
            self._env_base = EnvBase.from_type_and_label(EnvType.DEV, "marmotdev")
        return self._env_base

    @env_base.setter
    def env_base(self, env_base: EnvBase):
        self._env_base = env_base

    def set_env_base_env_var(self, env_base: Optional[EnvBase] = None):
        self.set_env_vars((ENV_BASE_KEY, env_base or self.env_base))


class CdkBaseTest(BaseTest):
    @property
    def app(self) -> cdk.App:
        try:
            return self._app
        except AttributeError:
            self.app = cdk.App()
        return self.app

    @app.setter
    def app(self, app: cdk.App):
        self._app = app

    def get_template(self, stack: cdk.Stack) -> Template:
        return Template.from_stack(stack)

    def get_dummy_construct(
        self, name: str, stack: Optional[cdk.Stack] = None
    ) -> EnvBaseConstruct:
        stack = stack or self.get_dummy_stack(name)
        return EnvBaseConstruct(stack, name, self.env_base)

    def get_dummy_stack(self, name: str) -> EnvBaseStack:
        return EnvBaseStack(self.app, name, self.env_base)
