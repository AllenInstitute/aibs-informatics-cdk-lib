import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.assets.docker_asset import (
    DockerAsset,
    resolve_image_uri,
)
from aibs_informatics_cdk_lib.constructs_.assets.registry import DigestResolutionError
from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    GitSource,
)
from test.aibs_informatics_cdk_lib.base import CdkBaseTest

IMAGE_NAME = "ghcr.io/alleninstitute/aibs-informatics-aws-lambda"
IMAGE_URI = f"{IMAGE_NAME}:v1.2.3"
DIGEST = "sha256:" + "ab" * 32
DIGEST_TAG = "sha256-" + "ab" * 32
DIGEST_URI = f"{IMAGE_NAME}@{DIGEST}"

ECR_DEPLOYMENT_RESOURCE = "Custom::CDKECRDeployment"

requires_ecr_deployment = pytest.mark.skipif(
    importlib.util.find_spec("cdk_ecr_deployment") is None,
    reason="mirroring needs the optional `ecr-mirror` extra (cdk-ecr-deployment)",
)


def patched_digest(digest: str = DIGEST):
    """Patch the digest resolver so that no test ever reaches a registry.

    Args:
        digest: The digest the resolver should return.

    Returns:
        A patcher whose mock records how the resolver was called.
    """
    return patch(
        "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.resolve_image_digest",
        return_value=digest,
    )


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

    def test__init__rejects_mirror_options_for_a_local_build(self):
        """A built image is already in the CDK asset repository, which is ECR."""
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="apply to a `registry_image_uri`"):
            DockerAsset(
                stack,
                "Image",
                directory=self.build_context().as_posix(),
                mirror_to_ecr=True,
            )

    def test__init__rejects_pin_digest_for_a_local_build(self):
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="apply to a `registry_image_uri`"):
            DockerAsset(stack, "Image", directory=self.build_context().as_posix(), pin_digest=True)

    def test__init__rejects_mirror_tags_without_mirroring(self):
        """Silently ignoring a mirror option would leave the caller thinking it mirrored."""
        stack = self.get_dummy_stack("test")
        with pytest.raises(ValueError, match="unless `mirror_to_ecr=True`"):
            DockerAsset(stack, "Image", registry_image_uri=IMAGE_URI, mirror_tags=["v1"])

    def test__init__rejects_an_ecr_repository_without_mirroring(self):
        stack = self.get_dummy_stack("test")
        repository = ecr.Repository(stack, "Repo", repository_name="preexisting")
        with pytest.raises(ValueError, match="unless `mirror_to_ecr=True`"):
            DockerAsset(stack, "Image", registry_image_uri=IMAGE_URI, ecr_repository=repository)


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

    def test__from_source__git_source_rejects_mirror_options(self):
        stack = self.get_dummy_stack("test")
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        with pytest.raises(ValueError, match="not available for GitSource"):
            DockerAsset.from_source(
                stack,
                "Image",
                source,
                directory=self.build_context().as_posix(),
                mirror_to_ecr=True,
            )

    def test__from_source__container_image_source_forwards_pin_digest(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image=IMAGE_NAME, tag="v1.2.3")
        with patched_digest():
            asset = DockerAsset.from_source(stack, "Image", source, pin_digest=True)
        assert asset.source_image_uri == DIGEST_URI


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
# Digest Pinning in direct-pull mode
# ---------------------------------------------------------------------------


class TestDockerAssetDigestPinning(DockerAssetBaseTest):
    def test__direct_pull__does_not_resolve_a_digest_by_default(self):
        """Direct pull keeps following the mutable tag, and synth stays offline-capable."""
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        mock_resolve.assert_not_called()
        assert asset.source_image_uri == IMAGE_URI

    def test__pin_digest__references_the_image_by_its_resolved_digest(self):
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI, pin_digest=True)
        mock_resolve.assert_called_once_with(IMAGE_NAME, "v1.2.3")
        assert asset.source_image_uri == DIGEST_URI
        assert asset.image_uri == DIGEST_URI

    def test__pin_digest__does_not_put_the_image_in_ecr(self):
        """Pinning names the image more precisely; it does not move it."""
        stack = self.get_dummy_stack("test")
        with patched_digest():
            asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI, pin_digest=True)
        assert asset.ecr_image_uri is None
        assert asset.mirror_repository is None

    def test__pin_digest__an_explicit_digest_skips_the_network(self):
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            asset = DockerAsset.from_registry(stack, "Image", DIGEST_URI, pin_digest=True)
        mock_resolve.assert_not_called()
        assert asset.source_image_uri == DIGEST_URI

    def test__pin_digest__untagged_reference_resolves_latest(self):
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            DockerAsset.from_registry(stack, "Image", IMAGE_NAME, pin_digest=True)
        mock_resolve.assert_called_once_with(IMAGE_NAME, "latest")

    def test__pin_digest__resolution_failure_is_a_hard_error(self):
        stack = self.get_dummy_stack("test")
        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.resolve_image_digest",
            side_effect=DigestResolutionError("boom"),
        ):
            with pytest.raises(DigestResolutionError):
                DockerAsset.from_registry(stack, "Image", IMAGE_URI, pin_digest=True)


# ---------------------------------------------------------------------------
# Mirroring
# ---------------------------------------------------------------------------


@requires_ecr_deployment
class TestDockerAssetMirroring(DockerAssetBaseTest):
    def mirrored(self, image=IMAGE_URI, **kwargs) -> DockerAsset:
        stack = kwargs.pop("stack", None) or self.get_dummy_stack("test")
        with patched_digest():
            return DockerAsset.from_registry(stack, "Image", image, mirror_to_ecr=True, **kwargs)

    def test__mirror__creates_a_retained_repository_named_for_the_env_and_asset(self):
        stack = self.get_dummy_stack("test")
        self.mirrored(stack=stack)
        template = self.get_template(stack)
        template.resource_count_is("AWS::ECR::Repository", 1)
        template.has_resource_properties(
            "AWS::ECR::Repository",
            {"RepositoryName": f"{self.env_base}-aibs-informatics-aws-lambda"},
        )
        template.has_resource("AWS::ECR::Repository", {"DeletionPolicy": "Retain"})

    def test__mirror__deploys_one_ecr_deployment_per_destination_tag(self):
        stack = self.get_dummy_stack("test")
        self.mirrored(stack=stack)
        self.get_template(stack).resource_count_is(ECR_DEPLOYMENT_RESOURCE, 2)

    def test__mirror__defaults_to_a_digest_tag_plus_the_source_tag(self):
        asset = self.mirrored()
        assert asset.mirror_tags == (DIGEST_TAG, "v1.2.3")

    def test__mirror__digest_tag_sanitizes_the_illegal_colon(self):
        asset = self.mirrored()
        assert ":" not in asset.mirror_tags[0]

    def test__mirror__copies_from_the_digest_not_the_tag(self):
        """A tag-to-tag copy would run once and then never notice the tag moving."""
        stack = self.get_dummy_stack("test")
        self.mirrored(stack=stack)
        self.get_template(stack).has_resource_properties(
            ECR_DEPLOYMENT_RESOURCE, {"SrcImage": f"docker://{DIGEST_URI}"}
        )

    def test__mirror__resolves_a_mutable_tag_at_synth(self):
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            DockerAsset.from_registry(stack, "Image", IMAGE_URI, mirror_to_ecr=True)
        mock_resolve.assert_called_once_with(IMAGE_NAME, "v1.2.3")

    def test__mirror__an_explicit_digest_skips_the_network(self):
        stack = self.get_dummy_stack("test")
        with patched_digest() as mock_resolve:
            asset = DockerAsset.from_registry(stack, "Image", DIGEST_URI, mirror_to_ecr=True)
        mock_resolve.assert_not_called()
        # No source tag to alias, so the digest tag is the only destination.
        assert asset.mirror_tags == (DIGEST_TAG,)

    def test__mirror__resolution_failure_raises_naming_the_fix(self):
        stack = self.get_dummy_stack("test")
        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.resolve_image_digest",
            side_effect=DigestResolutionError("Pin the digest explicitly"),
        ):
            with pytest.raises(DigestResolutionError, match="Pin the digest explicitly"):
                DockerAsset.from_registry(stack, "Image", IMAGE_URI, mirror_to_ecr=True)

    def test__mirror__resolution_failure_never_falls_back_to_the_mutable_tag(self):
        stack = self.get_dummy_stack("test")
        with patch(
            "aibs_informatics_cdk_lib.constructs_.assets.docker_asset.resolve_image_digest",
            side_effect=DigestResolutionError("boom"),
        ):
            with pytest.raises(DigestResolutionError):
                DockerAsset.from_registry(stack, "Image", IMAGE_URI, mirror_to_ecr=True)
        self.get_template(stack).resource_count_is(ECR_DEPLOYMENT_RESOURCE, 0)

    def test__mirror__mirror_tags_overrides_the_default_pair(self):
        """The escape hatch when two deployments cost more than the alias is worth."""
        stack = self.get_dummy_stack("test")
        asset = self.mirrored(stack=stack, mirror_tags=["stable"])
        assert asset.mirror_tags == ("stable",)
        self.get_template(stack).resource_count_is(ECR_DEPLOYMENT_RESOURCE, 1)

    def test__mirror__empty_mirror_tags_raises(self):
        with pytest.raises(ValueError, match="at least one destination tag"):
            self.mirrored(mirror_tags=[])

    def test__mirror__uses_a_supplied_repository_instead_of_creating_one(self):
        stack = self.get_dummy_stack("test")
        repository = ecr.Repository(stack, "Existing", repository_name="already-mine")
        asset = self.mirrored(stack=stack, ecr_repository=repository)
        assert asset.mirror_repository is repository
        template = self.get_template(stack)
        template.resource_count_is("AWS::ECR::Repository", 1)
        template.has_resource_properties(
            "AWS::ECR::Repository", {"RepositoryName": "already-mine"}
        )

    def test__mirror__ecr_image_uri_points_at_the_immutable_tag(self):
        asset = self.mirrored()
        ecr_image_uri = asset.ecr_image_uri
        assert ecr_image_uri is not None
        assert ecr_image_uri.endswith(f":{DIGEST_TAG}")
        assert "dkr.ecr" in ecr_image_uri

    def test__mirror__image_uri_prefers_ecr_over_the_origin_registry(self):
        asset = self.mirrored()
        # jsii mints a fresh token on every property read, so compare shape, not identity.
        assert "dkr.ecr" in asset.image_uri
        assert asset.image_uri.endswith(f":{DIGEST_TAG}")
        assert asset.source_image_uri == IMAGE_URI

    def test__mirror__as_lambda_code_uses_the_mirror_repository(self):
        """The whole point for Lambda: AWS requires the container image to be in ECR."""
        asset = self.mirrored()
        with patch.object(lambda_.DockerImageCode, "from_ecr") as mock_from_ecr:
            result = asset.as_lambda_code()
        (repository,), kwargs = mock_from_ecr.call_args
        assert repository is asset.mirror_repository
        assert kwargs == {"tag_or_digest": DIGEST_TAG}
        assert result is mock_from_ecr.return_value

    def test__mirror__as_lambda_code_returns_docker_image_code(self):
        asset = self.mirrored()
        assert isinstance(asset.as_lambda_code(), lambda_.DockerImageCode)

    def test__mirror__as_ecs_image_grants_pull_on_the_repository(self):
        """from_registry would produce the same URI but grant the role no ECR permission."""
        asset = self.mirrored()
        with patch.object(ecs.ContainerImage, "from_ecr_repository") as mock_from_ecr:
            result = asset.as_ecs_image()
        mock_from_ecr.assert_called_once_with(asset.mirror_repository, tag=DIGEST_TAG)
        assert result is mock_from_ecr.return_value

    def test__mirror__from_source_forwards_the_mirror_options(self):
        stack = self.get_dummy_stack("test")
        source = ContainerImageSource(image=IMAGE_NAME, tag="v1.2.3")
        with patched_digest():
            asset = DockerAsset.from_source(stack, "Image", source, mirror_to_ecr=True)
        assert asset.mirror_repository is not None
        self.get_template(stack).resource_count_is(ECR_DEPLOYMENT_RESOURCE, 2)


class TestDockerAssetMirroringWithoutTheExtra(DockerAssetBaseTest):
    """Installing without the `ecr-mirror` extra must break nothing but mirroring."""

    def test__mirror__without_cdk_ecr_deployment_raises_naming_the_extra(self):
        stack = self.get_dummy_stack("test")
        with patch.dict(sys.modules, {"cdk_ecr_deployment": None}):
            with patched_digest():
                with pytest.raises(ImportError, match="ecr-mirror"):
                    DockerAsset.from_registry(stack, "Image", IMAGE_URI, mirror_to_ecr=True)

    def test__direct_pull__works_without_cdk_ecr_deployment(self):
        stack = self.get_dummy_stack("test")
        with patch.dict(sys.modules, {"cdk_ecr_deployment": None}):
            asset = DockerAsset.from_registry(stack, "Image", IMAGE_URI)
        assert asset.image_uri == IMAGE_URI

    def test__local_build__works_without_cdk_ecr_deployment(self):
        with patch.dict(sys.modules, {"cdk_ecr_deployment": None}):
            asset = self.local_build_asset()
        assert asset.docker_image_asset is not None


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
