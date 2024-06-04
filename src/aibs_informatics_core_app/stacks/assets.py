from typing import Optional

import constructs
from aibs_informatics_core.env import EnvBase

from aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions import (
    AIBSInformaticsAssets,
    AIBSInformaticsCodeAssets,
    AIBSInformaticsDockerAssets,
)
from aibs_informatics_cdk_lib.stacks.base import EnvBaseStack


class AIBSInformaticsAssetsStack(EnvBaseStack):
    def __init__(
        self,
        scope: constructs.Construct,
        id: Optional[str],
        env_base: EnvBase,
        **kwargs,
    ):
        super().__init__(scope, id, env_base, **kwargs)

        self.assets = AIBSInformaticsAssets(
            self,
            "aibs-info-assets",
            self.env_base,
        )
        self.code_assets = self.assets.code_assets
        self.docker_assets = self.assets.docker_assets
