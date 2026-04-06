from unittest.mock import MagicMock, patch

import pytest

from aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions import (
    AIBS_INFORMATICS_AWS_LAMBDA_REPO,
    AIBSInformaticsAssets,
    AIBSInformaticsCodeAssets,
    AIBSInformaticsDockerAssets,
    AssetsMixin,
)
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


# ---------------------------------------------------------------------------
# AIBSInformaticsDockerAssets
# ---------------------------------------------------------------------------


class TestAIBSInformaticsDockerAssets(CdkBaseTest):
    def test__init__default_source(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsDockerAssets(stack, "DockerAssets", self.env_base)
        assert isinstance(assets._source, GitSource)
        assert assets._source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__str_source_backward_compat(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=repo_url
        )
        assert isinstance(assets._source, GitSource)
        assert assets._source.url == repo_url
        # Backward compatibility attribute
        assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == repo_url

    def test__init__git_source(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        assert assets._source is source
        assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == "git@github.com:org/repo.git"

    def test__init__container_image_source(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        assert assets._source is source
        # Backward compat attribute falls back to default
        assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_returns_uri(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        result = assets.AIBS_INFORMATICS_AWS_LAMBDA
        assert isinstance(result, str)
        assert result == "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:v1.2.3"

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_with_digest(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
            digest="sha256:abcdef1234567890",
        )
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        result = assets.AIBS_INFORMATICS_AWS_LAMBDA
        assert isinstance(result, str)
        assert result == "ghcr.io/alleninstitute/aibs-informatics-aws-lambda@sha256:abcdef1234567890"

    @patch.object(AssetsMixin, "resolve_repo_path")
    def test__AIBS_INFORMATICS_AWS_LAMBDA__git_source_calls_resolve(self, mock_resolve):
        """When using GitSource, the property should call resolve_repo_path with the ref URL."""
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsDockerAssets(
            stack, "DockerAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        # Mock resolve_repo_path to avoid actual git clone
        mock_path = MagicMock()
        mock_path.as_posix.return_value = "/tmp/fake-repo"
        mock_path.resolve.return_value = "/tmp/fake-repo"
        mock_resolve.return_value = mock_path

        # Access the property - we expect it to call resolve_repo_path
        # but the DockerImageAsset constructor will fail without a real directory,
        # so we patch that too
        with patch("aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.aws_ecr_assets.DockerImageAsset"):
            with patch("aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.generate_path_hash", return_value="fakehash"):
                assets.AIBS_INFORMATICS_AWS_LAMBDA

        mock_resolve.assert_called_once_with(
            "git@github.com:org/repo.git#v1.0.0",
            "AIBS_INFORMATICS_AWS_LAMBDA_REPO",
        )


# ---------------------------------------------------------------------------
# AIBSInformaticsCodeAssets
# ---------------------------------------------------------------------------


class TestAIBSInformaticsCodeAssets(CdkBaseTest):
    def test__init__default_source(self):
        stack = self.get_dummy_stack("test")
        assets = AIBSInformaticsCodeAssets(stack, "CodeAssets", self.env_base)
        assert isinstance(assets._source, GitSource)
        assert assets._source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__str_source_backward_compat(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom-repo.git"
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_repo=repo_url
        )
        assert isinstance(assets._source, GitSource)
        assert assets.AIBS_INFORMATICS_AWS_LAMBDA_REPO == repo_url

    def test__init__container_image_source_accepted(self):
        """ContainerImageSource is accepted at init time (lazy error)."""
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        # Should not raise at construction time
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        assert isinstance(assets._source, ContainerImageSource)

    def test__AIBS_INFORMATICS_AWS_LAMBDA__container_image_raises_type_error(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        with pytest.raises(TypeError, match="requires a GitSource"):
            assets.AIBS_INFORMATICS_AWS_LAMBDA

    @patch.object(AssetsMixin, "resolve_repo_path")
    def test__AIBS_INFORMATICS_AWS_LAMBDA__git_source_calls_resolve(self, mock_resolve):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", branch="main")
        assets = AIBSInformaticsCodeAssets(
            stack, "CodeAssets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(return_value="/tmp/fake-repo")
        mock_path.resolve.return_value = mock_path
        mock_path.as_posix.return_value = "/tmp/fake-repo"
        mock_resolve.return_value = mock_path

        with patch("aibs_informatics_cdk_lib.constructs_.assets.code_asset_definitions.generate_path_hash", return_value="fakehash"):
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
        assert isinstance(assets.code_assets._source, GitSource)
        assert isinstance(assets.docker_assets._source, GitSource)
        assert assets.code_assets._source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO
        assert assets.docker_assets._source.url == AIBS_INFORMATICS_AWS_LAMBDA_REPO

    def test__init__git_source_passed_to_both(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assets = AIBSInformaticsAssets(
            stack, "Assets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        assert assets.code_assets._source is source
        assert assets.docker_assets._source is source

    def test__init__container_image_source_passed_to_both(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        assets = AIBSInformaticsAssets(
            stack, "Assets", self.env_base, aibs_informatics_aws_lambda_repo=source
        )
        # Both should receive the source (code_assets will fail lazily on property access)
        assert isinstance(assets.docker_assets._source, ContainerImageSource)
        assert isinstance(assets.code_assets._source, ContainerImageSource)

    def test__init__str_source_backward_compat(self):
        stack = self.get_dummy_stack("test")
        repo_url = "git@github.com:org/custom.git"
        assets = AIBSInformaticsAssets(
            stack, "Assets", self.env_base, aibs_informatics_aws_lambda_repo=repo_url
        )
        assert isinstance(assets.code_assets._source, GitSource)
        assert isinstance(assets.docker_assets._source, GitSource)
        assert assets.code_assets._source.url == "git@github.com:org/custom.git"
        assert assets.docker_assets._source.url == "git@github.com:org/custom.git"
