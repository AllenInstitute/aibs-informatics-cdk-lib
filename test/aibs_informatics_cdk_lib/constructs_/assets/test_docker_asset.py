from pathlib import Path
from unittest.mock import patch

import pytest
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.assets.docker_asset import (
    DockerAsset,
    resolve_image_uri,
)
from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    GitSource,
)
from test.aibs_informatics_cdk_lib.base import CdkBaseTest

IMAGE_URI = "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:v1.2.3"


class DockerAssetBaseTest(CdkBaseTest):
    """Shared scaffolding for DockerAsset tests.

    A local build needs a real build context on disk: ``DockerImageAsset`` stages and
    fingerprints the directory at synth time. It does not invoke docker, so no image is
    ever built and nothing leaves the machine.
    """

    def build_context(self) -> Path:
        path = self.tmp_path()
        (path / "docker").mkdir(parents=True, exist_ok=True)
        (path / "docker" / "Dockerfile").write_text("FROM scratch\n")
        return path

    def local_build_asset(self, id: str = "LocalImage") -> DockerAsset:
        stack = self.get_dummy_stack("test")
        return DockerAsset.from_local_build(
            stack,
            id,
            directory=self.build_context().as_posix(),
            file="docker/Dockerfile",
        )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestDockerAssetConstruction(DockerAssetBaseTest):
    def test__init__requires_a_directory_or_a_registry_uri(self):
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="exactly one of"):
            DockerAsset(stack, "Image")

    def test__init__rejects_both_a_directory_and_a_registry_uri(self):
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="exactly one of"):
            DockerAsset(
                stack,
                "Image",
                directory=self.build_context().as_posix(),
                registry_image_uri=IMAGE_URI,
            )

    def test__init__rejects_image_asset_props_without_a_directory(self):
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="only valid when building"):
            DockerAsset(stack, "Image", registry_image_uri=IMAGE_URI, file="docker/Dockerfile")

    def test__from_local_build__owns_the_image_asset_as_a_child(self):
        """PR 3 hangs an ECR repository and mirror deployment off this construct."""
        asset = self.local_build_asset()
        assert isinstance(asset, DockerAsset)
        assert asset.docker_image_asset is not None
        assert asset.docker_image_asset.node.scope is asset

    def test__from_registry__records_the_container_image_source(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        asset = DockerAsset.from_registry(stack, "Image", source)
        assert asset.source is source
        assert asset.docker_image_asset is None

    def test__from_registry__accepts_a_bare_image_uri(self):
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert asset.source is None
        assert asset.image_uri == IMAGE_URI


# ---------------------------------------------------------------------------
# DockerAsset.from_source
# ---------------------------------------------------------------------------


class TestDockerAssetFromSource(DockerAssetBaseTest):
    def test__from_source__container_image_source_resolves_to_the_registry(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(
            image="ghcr.io/alleninstitute/aibs-informatics-aws-lambda", tag="v1.2.3"
        )
        asset = DockerAsset.from_source(stack, "Image", source)
        assert asset.docker_image_asset is None
        assert asset.image_uri == IMAGE_URI

    def test__from_source__container_image_source_ignores_a_directory(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1")
        asset = DockerAsset.from_source(
            stack, "Image", source, directory=self.build_context().as_posix()
        )
        assert asset.docker_image_asset is None

    def test__from_source__git_source_builds_from_the_directory(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        asset = DockerAsset.from_source(
            stack,
            "Image",
            source,
            directory=self.build_context().as_posix(),
            file="docker/Dockerfile",
        )
        assert asset.docker_image_asset is not None
        assert asset.source is source

    def test__from_source__git_source_without_a_directory_raises(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git")
        with pytest.raises(ValueError, match="without a `directory`"):
            DockerAsset.from_source(stack, "Image", source)


# ---------------------------------------------------------------------------
# URI accessors
# ---------------------------------------------------------------------------


class TestDockerAssetUris(DockerAssetBaseTest):
    def test__local_build__all_uris_are_the_built_asset_uri(self):
        asset = self.local_build_asset()
        assert asset.docker_image_asset is not None
        built_uri = asset.docker_image_asset.image_uri
        assert asset.image_uri == built_uri
        assert asset.source_image_uri == built_uri
        assert asset.ecr_image_uri == built_uri

    def test__registry__source_uri_is_the_registry_uri_and_ecr_uri_is_none(self):
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert asset.source_image_uri == IMAGE_URI
        assert asset.ecr_image_uri is None

    def test__registry__image_uri_falls_back_to_the_origin_registry(self):
        """Until the image is mirrored, the origin registry is the most-local URI there is."""
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert asset.image_uri == asset.source_image_uri


# ---------------------------------------------------------------------------
# Per-service representations
# ---------------------------------------------------------------------------


class TestDockerAssetAsEcsImage(DockerAssetBaseTest):
    def test__as_ecs_image__local_build_uses_the_image_asset(self):
        asset = self.local_build_asset()
        with patch.object(ecs.ContainerImage, "from_docker_image_asset") as mock_from_asset:
            result = asset.as_ecs_image()
        mock_from_asset.assert_called_once_with(asset.docker_image_asset)
        assert result is mock_from_asset.return_value

    def test__as_ecs_image__registry_pulls_directly(self):
        """ECS can pull a public registry itself, so no mirror is required."""
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        with patch.object(ecs.ContainerImage, "from_registry") as mock_from_registry:
            result = asset.as_ecs_image()
        mock_from_registry.assert_called_once_with(IMAGE_URI)
        assert result is mock_from_registry.return_value

    def test__as_ecs_image__returns_a_container_image(self):
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert isinstance(asset.as_ecs_image(), ecs.ContainerImage)


class TestDockerAssetAsLambdaCode(DockerAssetBaseTest):
    def test__as_lambda_code__local_build_points_at_the_asset_repository(self):
        asset = self.local_build_asset()
        image_asset = asset.docker_image_asset
        assert image_asset is not None
        with patch.object(lambda_.DockerImageCode, "from_ecr") as mock_from_ecr:
            result = asset.as_lambda_code()
        # jsii hands back a fresh proxy on every property read, so compare by name.
        (repository,), kwargs = mock_from_ecr.call_args
        assert repository.repository_name == image_asset.repository.repository_name
        assert kwargs == {"tag_or_digest": image_asset.image_tag}
        assert result is mock_from_ecr.return_value

    def test__as_lambda_code__local_build_returns_docker_image_code(self):
        asset = self.local_build_asset()
        assert isinstance(asset.as_lambda_code(), lambda_.DockerImageCode)

    def test__as_lambda_code__unmirrored_registry_raises(self):
        """Lambda container images must live in ECR; mirroring is the fix, not a fallback."""
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        with pytest.raises(ValueError, match="mirror_to_ecr=True"):
            asset.as_lambda_code()

    def test__as_lambda_code__error_names_the_offending_image(self):
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        with pytest.raises(ValueError, match=IMAGE_URI):
            asset.as_lambda_code()

    def test__as_lambda_code__does_not_create_infrastructure_on_failure(self):
        """A property access must never provision an ECR repository as a side effect."""
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        with pytest.raises(ValueError):
            asset.as_lambda_code()
        assert asset.node.children == []


# ---------------------------------------------------------------------------
# resolve_image_uri
# ---------------------------------------------------------------------------


class TestResolveImageUri(DockerAssetBaseTest):
    def test__resolve_image_uri__str_passes_through(self):
        assert resolve_image_uri(IMAGE_URI) == IMAGE_URI

    def test__resolve_image_uri__docker_asset(self):
        stack = self.get_dummy_stack("test")
        asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert resolve_image_uri(asset) == IMAGE_URI

    def test__resolve_image_uri__raw_docker_image_asset(self):
        """Downstream Assets Constructs still expose raw CDK assets; they must keep working."""
        stack = self.get_dummy_stack("test")
        image_asset = ecr_assets.DockerImageAsset(
            stack,
            "RawImage",
            directory=self.build_context().as_posix(),
            file="docker/Dockerfile",
        )
        assert resolve_image_uri(image_asset) == image_asset.image_uri
