import warnings
from unittest.mock import MagicMock, patch

import pytest

from aibs_informatics_cdk_lib.constructs_.assets.code_asset import (
    GLOBAL_GLOB_EXCLUDES,
    PYTHON_GLOB_EXCLUDES,
)
from aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions import (
    AIBS_INFORMATICS_AWS_LAMBDA_REPO,
    AIBSInformaticsAssets,
    AIBSInformaticsCodeAssets,
    AIBSInformaticsDockerAssets,
    AssetsMixin,
)
from aibs_informatics_cdk_lib.constructs_.assets.docker_asset import DockerAsset
from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    GitSource,
    PackageSource,
)
from test.aibs_informatics_cdk_lib.base import CdkBaseTest

# ---------------------------------------------------------------------------
# AssetsMixin._normalize_source
# ---------------------------------------------------------------------------


class TestNormalizeSource:
    def test__none_returns_default_git_source(self):
        source = AssetsMixin._normalize_source(None, "git@github.com:org/repo.git")
        assert isinstance(source, GitSource)
        assert source.url == "git@github.com:org/repo.git"

    def test__str_git_url_returns_git_source(self):
        source = AssetsMixin._normalize_source(
            "git@github.com:org/repo.git#main", "git@github.com:org/default.git"
        )
        assert isinstance(source, GitSource)
        assert source.url == "git@github.com:org/repo.git"
        assert source.branch == "main"

    def test__str_container_image_returns_container_source(self):
        source = AssetsMixin._normalize_source(
            "ghcr.io/org/repo:v1.2.3", "git@github.com:org/default.git"
        )
        assert isinstance(source, ContainerImageSource)
        assert source.image == "ghcr.io/org/repo"
        assert source.tag == "v1.2.3"

    def test__package_source_passed_through(self):
        original = GitSource(url="git@github.com:org/repo.git", tag="v2.0")
        source = AssetsMixin._normalize_source(original, "git@github.com:org/default.git")
        assert source is original

    def test__container_image_source_passed_through(self):
        original = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        source = AssetsMixin._normalize_source(original, "git@github.com:org/default.git")
        assert source is original

    def test__unsupported_package_source_raises(self):
        """An unrecognized subclass must be rejected here, not deep in a cached_property."""

        class CustomSource(PackageSource):
            source_type: str = "custom"

        with pytest.raises(TypeError, match="Unsupported package source type: CustomSource"):
            AssetsMixin._normalize_source(CustomSource(), "git@github.com:org/default.git")


# ---------------------------------------------------------------------------
# AssetsMixin._resolve_deprecated_source
# ---------------------------------------------------------------------------


class TestResolveDeprecatedSource:
    def test__neither_supplied_returns_none(self):
        assert AssetsMixin._resolve_deprecated_source(None, {}) is None

    def test__current_param_returned_without_warning(self):
        source = GitSource(url="git@github.com:org/repo.git")
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert AssetsMixin._resolve_deprecated_source(source, {}) is source

    def test__deprecated_kwarg_is_popped_warned_and_returned(self):
        source = GitSource(url="git@github.com:org/repo.git")
        kwargs: dict = {"aibs_informatics_aws_lambda_repo": source}
        with pytest.warns(DeprecationWarning, match="aibs_informatics_aws_lambda_repo"):
            assert AssetsMixin._resolve_deprecated_source(None, kwargs) is source
        assert kwargs == {}

    def test__both_supplied_raises(self):
        with pytest.raises(ValueError, match="Cannot specify both"):
            AssetsMixin._resolve_deprecated_source("a", {"aibs_informatics_aws_lambda_repo": "b"})

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            pytest.param(
                {"aibs_informatics_aws_lambda_rep": "git@github.com:org/repo.git"},
                "unexpected keyword argument 'aibs_informatics_aws_lambda_rep'",
                id="near_miss_on_the_alias",
            ),
            pytest.param({"beta": None, "alpha": None}, "'alpha', 'beta'", id="all_reported"),
        ],
    )
    def test__unexpected_keyword_raises(self, kwargs: dict, match: str):
        """**kwargs must not silently swallow a near miss like a misspelled alias.

        That the message names the concrete construct is pinned by
        TestAIBSInformaticsDockerAssets.test__init__misspelled_kwarg_raises; asserting it
        here would only ever show `AssetsMixin`, which has no __init__.
        """
        with pytest.raises(TypeError, match=match):
            AssetsMixin._resolve_deprecated_source(None, kwargs)


# ---------------------------------------------------------------------------
# AIBSInformaticsDockerAssets
# ---------------------------------------------------------------------------


class TestAIBSInformaticsDockerAssets(CdkBaseTest):
    def test__init__default_source(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsDockerAssets(stack, "DockerAssets", self.env_base)
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__str_source(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=repo_url
        )
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == repo_url

    def test__init__deprecated_repo_kwarg_still_works(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        with pytest.warns(DeprecationWarning, match="aibs_informatics_aws_lambda_repo"):
            assets = AIBSInformaticsDockerAssets(
                stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=repo_url
            )
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == repo_url

    def test__init__both_source_kwargs_raises(self):
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="Cannot specify both"):
            AIBSInformaticsDockerAssets(
                stack,
                "DockerAssets",
                self.env_base,
                aibs_informatics_aws_lambda_source="git@github.com:org/a.git",
                aibs_informatics_aws_lambda_repo="git@github.com:org/b.git",
            )

    def test__init__misspelled_kwarg_raises(self):
        """A near miss on the deprecated alias must fail, not fall back to the default."""
        stack = self.get_dummy_stack("test")
        with pytest.raises(
            TypeError, match=r"AIBSInformaticsDockerAssets\.__init__\(\) got an unexpected keyword"
        ):
            AIBSInformaticsDockerAssets(
                stack,
                "DockerAssets",
                self.env_base,
                aibs_informatics_aws_lambda_rep="git@github.com:org/custom.git",
            )

    def test__init__git_source(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        assert assets.source is source

    def test__init__container_image_source(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        assert assets.source is source

    def test__AIBS_INFORMATICS_AWS_LAMBDA_REPO__git_source_warns_and_returns_url(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        with pytest.warns(DeprecationWarning, match="AIBS_INFORMATICS_AWS_LAMBDA_REPO"):
            assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == "git@github.com:org/repo.git"

    def test__AIBS_INFORMATICS_AWS_LAMBDA_REPO__container_image_source_is_none(self):
        """A Container Image Source has no repo URL, so the shim must not invent one."""
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        with pytest.warns(DeprecationWarning, match="AIBS_INFORMATICS_AWS_LAMBDA_REPO"):
            assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO is None

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_returns_uri(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        result = assets.AIBS_INFORMATICS_AWS_LAMBDA
        assert isinstance(result, DockerAsset)
        assert result.image_uri == "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:v1.2.3"
        assert result.source is source

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_with_digest(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
            digest="sha256:abcdef1234567890",
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        result = assets.AIBS_INFORMATICS_AWS_LAMBDA
        assert isinstance(result, DockerAsset)
        assert (
            result.image_uri
            == "ghcr.io/alleninstitute/aibs-informatics-aws-lambda@sha256:abcdef1234567890"
        )

    @patch.object(AssetsMixin, "resolve_repo_path")
    def test__AIBS_INFORMATICS_AWS_LAMBDA__git_source_calls_resolve(self, mock_resolve):
        """When using GitSource, the property should call resolve_repo_path with the ref URL."""
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        # Mock resolve_repo_path to avoid actual git clone
        mock_path = MagicMock()
        mock_path.as_posix.return_value = "/tmp/fake-repo"
        mock_path.resolve.return_value = "/tmp/fake-repo"
        mock_resolve.return_value = mock_path

        # Access the property - we expect it to call resolve_repo_path
        # but the DockerImageAsset constructor will fail without a real directory,
        # so we patch that too
        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.ecr_assets.DockerImageAsset"
        ):
            with patch(
                "aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.generate_path_hash",
                return_value="fakehash",
            ):
                assets.AIBS_INFORMATICS_AWS_LAMBDA

        mock_resolve.assert_called_once_with(
            "git@github.com:org/repo.git#v1.0.0",
            "AIBS_INFORMATICS_AWS_LAMBDA_REPO",
        )

    @patch.object(AssetsMixin, "resolve_repo_path")
    def test__AIBS_INFORMATICS_AWS_LAMBDA__git_source_preserves_build_props(self, mock_resolve):
        """The build props feed asset hashing and the ssh-backed build -- they must not drift."""
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsDockerAssets(stack, "DockerAssets", self.env_base)

        mock_path = MagicMock()
        mock_path.as_posix.return_value = "/tmp/fake-repo"
        mock_path.resolve.return_value = "/tmp/fake-repo"
        mock_resolve.return_value = mock_path

        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.ecr_assets.DockerImageAsset"
        ) as mock_image_asset:
            with patch(
                "aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.generate_path_hash",
                return_value="fakehash",
            ):
                result = assets.AIBS_INFORMATICS_AWS_LAMBDA

        assert isinstance(result, DockerAsset)
        _, kwargs = mock_image_asset.call_args
        assert kwargs["directory"] == "/tmp/fake-repo"
        assert kwargs["file"] == "docker/Dockerfile"
        assert kwargs["build_ssh"] == "default"
        assert kwargs["asset_name"] == "aibs-informatics-aws-lambda"
        assert kwargs["platform"].platform == "linux/amd64"
        assert kwargs["extra_hash"] == "fakehash"
        assert kwargs["exclude"] == [*PYTHON_GLOB_EXCLUDES, *GLOBAL_GLOB_EXCLUDES]


# ---------------------------------------------------------------------------
# AIBSInformaticsCodeAssets
# ---------------------------------------------------------------------------


class TestAIBSInformaticsCodeAssets(CdkBaseTest):
    def test__init__default_source(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsCodeAssets(stack, "CodeAssets", self.env_base)
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__str_source(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_source=repo_url
        )
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == repo_url

    def test__init__deprecated_repo_kwarg_still_works(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        with pytest.warns(DeprecationWarning, match="aibs_informatics_aws_lambda_repo"):
            assets = AIBSInformaticsCodeAssets(
                stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_repo=repo_url
            )
        assert isinstance(assets.source, GitSource)
        assert assets.source.url == repo_url

    def test__AIBS_INFORMATICS_AWS_LAMBDA_REPO__git_source_warns_and_returns_url(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_source=repo_url
        )
        with pytest.warns(DeprecationWarning, match="AIBS_INFORMATICS_AWS_LAMBDA_REPO"):
            assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == repo_url

    def test__init__container_image_source_accepted(self):
        """ContainerImageSource is accepted at init time (lazy error)."""
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        # Should not raise at construction time
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        assert isinstance(assets.source, ContainerImageSource)

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_raises_type_error(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )
        with pytest.raises(TypeError, match="requires a GitSource"):
            assets.AIBS_INFORMATICS_AWS_LAMBDA

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_str_raises_type_error(self):
        """A str that parses to an image ref is caught at property access, not init."""
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsCodeAssets(
            stack,
            "CodeAssets",
            self.env_base,
            aibs_informatics_aws_lambda_source="ghcr.io/org/repo:v1",
        )
        with pytest.raises(TypeError, match="requires a GitSource"):
            assets.AIBS_INFORMATICS_AWS_LAMBDA

    @patch.object(AssetsMixin, "resolve_repo_path")
    def test__AIBS_INFORMATICS_AWS_LAMBDA__git_source_calls_resolve(self, mock_resolve):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", branch="main")
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_source=source
        )

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(return_value="/tmp/fake-repo")
        mock_path.resolve.return_value = mock_path
        mock_path.as_posix.return_value = "/tmp/fake-repo"
        mock_resolve.return_value = mock_path

        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.generate_path_hash",
            return_value="fakehash",
        ):
            assets.AIBS_INFORMATICS_AWS_LAMBDA

        mock_resolve.assert_called_once_with(
            "git@github.com:org/repo.git#main",
            "AIBS_INFORMATICS_AWS_LAMBDA_REPO",
        )


# ---------------------------------------------------------------------------
# AIBSInformaticsAssets
# ---------------------------------------------------------------------------


class TestAIBSInformaticsAssets(CdkBaseTest):
    def test__init__default_passes_through(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsAssets(stack, "Assets", self.env_base)
        assert isinstance(assets.code_assets.source, GitSource)
        assert isinstance(assets.docker_assets.source, GitSource)
        assert assets.code_assets.source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO
        assert assets.docker_assets.source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__sources_are_independent(self):
        """A container image for docker must not leak into the code assets."""
        stack = self.get_dummy_stack("test")
        code_source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        docker_source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1.0.0")
        assets = AIBSInformaticsAssets(
            stack,
            "Assets",
            self.env_base,
            aibs_informatics_aws_lambda_code_source=code_source,
            aibs_informatics_aws_lambda_docker_source=docker_source,
        )
        assert assets.code_assets.source is code_source
        assert assets.docker_assets.source is docker_source

    def test__init__only_docker_source_leaves_code_source_at_default(self):
        stack = self.get_dummy_stack("test")
        docker_source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1.0.0")
        assets = AIBSInformaticsAssets(
            stack,
            "Assets",
            self.env_base,
            aibs_informatics_aws_lambda_docker_source=docker_source,
        )
        assert isinstance(assets.code_assets.source, GitSource)
        assert assets.code_assets.source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO
        assert assets.docker_assets.source is docker_source

    def test__init__str_sources(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsAssets(
            stack,
            "Assets",
            self.env_base,
            aibs_informatics_aws_lambda_code_source="git@github.com:org/custom.git",
            aibs_informatics_aws_lambda_docker_source="ghcr.io/org/repo:v1",
        )
        assert isinstance(assets.code_assets.source, GitSource)
        assert assets.code_assets.source.url == "git@github.com:org/custom.git"
        assert isinstance(assets.docker_assets.source, ContainerImageSource)
        assert assets.docker_assets.source.image_uri == "ghcr.io/org/repo:v1"

    def test__init__no_deprecated_repo_kwarg(self):
        """This construct never shipped an `aibs_informatics_aws_lambda_repo` parameter."""
        stack = self.get_dummy_stack("test")
        with pytest.raises(TypeError, match="aibs_informatics_aws_lambda_repo"):
            AIBSInformaticsAssets(
                stack,
                "Assets",
                self.env_base,
                aibs_informatics_aws_lambda_repo="git@github.com:org/custom.git",
            )
