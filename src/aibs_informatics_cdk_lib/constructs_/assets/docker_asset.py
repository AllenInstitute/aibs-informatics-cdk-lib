"""A Docker Asset -- one container image and its per-service representations.

Each AWS service consumes a container image differently: Batch wants a URI string, ECS
wants an ``ecs.ContainerImage``, Lambda wants a ``lambda_.DockerImageCode``. Those
representations belong to a single image, not to the Assets Construct that collects
several of them, so they hang off ``DockerAsset``.

``DockerAsset`` is a ``constructs.Construct`` rather than a plain value object so that it
can own child constructs -- today the ``DockerImageAsset`` it builds, later the ECR
repository and deployment used to mirror a Container Image Source in-region.
"""

import logging
from typing import Any

import constructs
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    PackageSource,
)

logger = logging.getLogger(__name__)

IMAGE_CONSTRUCT_ID = "Image"
"""Construct id of the ``DockerImageAsset`` a locally built ``DockerAsset`` owns."""


class DockerAsset(constructs.Construct):
    """A single container image, built locally or resolved from a registry.

    Use the factories rather than the constructor: ``from_source`` when you have a
    Source, ``from_local_build`` when you have a docker build context, and
    ``from_registry`` when you have a pre-built image reference.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        *,
        directory: str | None = None,
        registry_image_uri: str | None = None,
        source: PackageSource | None = None,
        **image_asset_props: Any,
    ) -> None:
        """Create a Docker Asset from exactly one of a build context or a registry URI.

        Args:
            scope: The parent construct.
            id: The construct id.
            directory: The docker build context to build the image from. Mutually
                exclusive with ``registry_image_uri``.
            registry_image_uri: The URI of a pre-built image in a registry. Mutually
                exclusive with ``directory``.
            source: The Source this image was produced from, recorded for provenance.
            **image_asset_props: Additional props forwarded to
                ``aws_ecr_assets.DockerImageAsset`` (e.g. ``file``, ``platform``,
                ``build_ssh``, ``asset_name``, ``extra_hash``, ``exclude``). Only valid
                alongside ``directory``.

        Raises:
            ValueError: If neither or both of ``directory`` and ``registry_image_uri``
                are given, or if image asset props are given without a ``directory``.
        """
        super().__init__(scope, id)

        if (directory is None) == (registry_image_uri is None):
            raise ValueError(
                "DockerAsset requires exactly one of `directory` (build the image) or "
                "`registry_image_uri` (use a pre-built image)."
            )
        if directory is None and image_asset_props:
            raise ValueError(
                f"Docker image asset props {sorted(image_asset_props)} are only valid when "
                "building from a `directory`."
            )

        self._source = source
        self._registry_image_uri = registry_image_uri
        self._image_asset = (
            ecr_assets.DockerImageAsset(
                self, IMAGE_CONSTRUCT_ID, directory=directory, **image_asset_props
            )
            if directory is not None
            else None
        )

    # -----------------------------------------------------------------------------
    # Factories
    # -----------------------------------------------------------------------------

    @classmethod
    def from_source(
        cls,
        scope: constructs.Construct,
        id: str,
        source: PackageSource,
        *,
        directory: str | None = None,
        **image_asset_props: Any,
    ) -> "DockerAsset":
        """Create a Docker Asset from a Source, building or resolving as the Source requires.

        A Container Image Source names an image that already exists, so it is used
        directly. A Git Source has to be built, which needs a checkout -- pass the
        directory it was checked out into.

        Args:
            scope: The parent construct.
            id: The construct id.
            source: The Source to produce the image from.
            directory: The docker build context. Required for a Git Source, ignored for
                a Container Image Source.
            **image_asset_props: Additional props forwarded to
                ``aws_ecr_assets.DockerImageAsset``.

        Returns:
            A Docker Asset for the given Source.

        Raises:
            ValueError: If the Source must be built but no ``directory`` was given.
        """
        if isinstance(source, ContainerImageSource):
            return cls.from_registry(scope, id, source)
        if directory is None:
            raise ValueError(
                f"Cannot build a Docker Asset from {type(source).__name__} without a "
                "`directory` to build from. Resolve the Source to a checkout first."
            )
        return cls.from_local_build(
            scope, id, directory=directory, source=source, **image_asset_props
        )

    @classmethod
    def from_local_build(
        cls,
        scope: constructs.Construct,
        id: str,
        *,
        directory: str,
        source: PackageSource | None = None,
        **image_asset_props: Any,
    ) -> "DockerAsset":
        """Create a Docker Asset by building an image from a local docker build context.

        Args:
            scope: The parent construct.
            id: The construct id.
            directory: The docker build context.
            source: The Source the build context came from, recorded for provenance.
            **image_asset_props: Additional props forwarded to
                ``aws_ecr_assets.DockerImageAsset`` (e.g. ``file``, ``platform``,
                ``build_ssh``, ``asset_name``, ``extra_hash``, ``exclude``).

        Returns:
            A Docker Asset wrapping the built image.
        """
        return cls(scope, id, directory=directory, source=source, **image_asset_props)

    @classmethod
    def from_registry(
        cls,
        scope: constructs.Construct,
        id: str,
        image: ContainerImageSource | str,
    ) -> "DockerAsset":
        """Create a Docker Asset from an image that is already published to a registry.

        Args:
            scope: The parent construct.
            id: The construct id.
            image: A Container Image Source, or an image URI string.

        Returns:
            A Docker Asset referencing the published image.
        """
        if isinstance(image, ContainerImageSource):
            return cls(scope, id, registry_image_uri=image.image_uri, source=image)
        return cls(scope, id, registry_image_uri=image)

    # -----------------------------------------------------------------------------
    # Accessors
    # -----------------------------------------------------------------------------

    @property
    def source(self) -> PackageSource | None:
        """The Source this image was produced from, if one was recorded."""
        return self._source

    @property
    def docker_image_asset(self) -> ecr_assets.DockerImageAsset | None:
        """The underlying CDK asset for a locally built image, or None for a registry image."""
        return self._image_asset

    @property
    def image_uri(self) -> str:
        """The most-local URI available for this image.

        Prefers a URI in an ECR repository we control over the image's origin registry,
        so that workloads pull in-region wherever that is possible.

        Returns:
            An image URI suitable for Batch, ECS, or anything else that takes a string.
        """
        return self.ecr_image_uri or self.source_image_uri

    @property
    def source_image_uri(self) -> str:
        """The URI of this image where its Source puts it.

        For a Container Image Source that is the origin registry. For a locally built
        image there is no upstream registry, so it is the URI CDK publishes the built
        asset to.

        Returns:
            The image URI at its origin.
        """
        if self._image_asset is not None:
            return self._image_asset.image_uri
        # The constructor guarantees one of the two is set.
        return str(self._registry_image_uri)

    @property
    def ecr_image_uri(self) -> str | None:
        """The URI of this image inside an ECR repository, or None if it is not in one.

        A locally built image is published to the CDK asset repository, which is ECR. An
        image resolved from a Container Image Source stays at its origin registry until
        it is mirrored into ECR, which this construct does not yet do.

        Returns:
            The in-ECR image URI, or None when the image is only at its origin registry.
        """
        # NOTE: mirroring a Container Image Source into ECR resolves the None case.
        if self._image_asset is not None:
            return self._image_asset.image_uri
        return None

    def as_ecs_image(self) -> ecs.ContainerImage:
        """Present this image to ECS.

        ECS can pull directly from a public registry, so a Container Image Source needs
        no mirroring to be usable here.

        Returns:
            The image as an ``ecs.ContainerImage``.
        """
        if self._image_asset is not None:
            return ecs.ContainerImage.from_docker_image_asset(self._image_asset)
        return ecs.ContainerImage.from_registry(self.image_uri)

    def as_lambda_code(self) -> lambda_.DockerImageCode:
        """Present this image to Lambda.

        Returns:
            The image as a ``lambda_.DockerImageCode``.

        Raises:
            ValueError: If the image is only available at its origin registry. Lambda
                container images must live in ECR -- ``DockerImageCode`` offers only
                ``from_ecr()`` and ``from_image_asset()`` -- so the image has to be
                mirrored first. Provisioning that ECR repository here would create
                infrastructure as a side effect of reading a property, so it has to be
                asked for explicitly.
        """
        # NOTE: mirroring a Container Image Source into ECR gives this the repository and
        # tag it needs, and the `from_ecr` call below serves that case unchanged.
        if self._image_asset is None:
            raise ValueError(
                f"Cannot use '{self.source_image_uri}' as Lambda code: Lambda container "
                "images must live in ECR, and this image is only available at its origin "
                "registry. Pass `mirror_to_ecr=True` for this asset to copy it into ECR."
            )
        return lambda_.DockerImageCode.from_ecr(
            self._image_asset.repository, tag_or_digest=self._image_asset.image_tag
        )


DockerAssetLike = ecr_assets.DockerImageAsset | DockerAsset | str
"""Anything that can stand in for a container image reference.

Downstream Assets Constructs expose raw ``DockerImageAsset`` members alongside our
``DockerAsset`` ones, and callers sometimes have nothing but a URI string, so everything
that consumes an image accepts all three.
"""


def resolve_image_uri(asset: DockerAssetLike) -> str:
    """Resolve anything image-like to a container image URI.

    Args:
        asset: A ``DockerAsset``, a raw CDK ``DockerImageAsset``, or an image URI string.

    Returns:
        The container image URI.
    """
    return asset if isinstance(asset, str) else asset.image_uri
