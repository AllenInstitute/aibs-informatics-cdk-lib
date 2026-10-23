"""A Docker Asset -- one container image and its per-service representations.

Each AWS service consumes a container image differently: Batch wants a URI string, ECS
wants an ``ecs.ContainerImage``, Lambda wants a ``lambda_.DockerImageCode``. Those
representations belong to a single image, not to the Assets Construct that collects
several of them, so they hang off ``DockerAsset``.

``DockerAsset`` is a ``constructs.Construct`` rather than a plain value object so that it
can own child constructs -- the ``DockerImageAsset`` it builds, and the ECR repository and
deployments used to mirror a Container Image Source in-region.
"""

import logging
import re
from collections.abc import Sequence
from typing import Any

import aws_cdk as cdk
import constructs
from aibs_informatics_core.env import EnvBase
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_lambda as lambda_

from aibs_informatics_cdk_lib.constructs_.assets.registry import (
    DEFAULT_TAG,
    digest_to_tag,
    parse_image_reference,
    resolve_image_digest,
)
from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    PackageSource,
)
from aibs_informatics_cdk_lib.project.utils import get_env_base

logger = logging.getLogger(__name__)

IMAGE_CONSTRUCT_ID = "Image"
"""Construct id of the ``DockerImageAsset`` a locally built ``DockerAsset`` owns."""

MIRROR_REPOSITORY_CONSTRUCT_ID = "MirrorRepository"
"""Construct id of the ECR repository a ``DockerAsset`` creates to mirror into."""

MIRROR_CONSTRUCT_ID_PREFIX = "Mirror"
"""Construct id prefix of the ``ECRDeployment`` copying an image to one destination tag."""


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
        mirror_to_ecr: bool = False,
        ecr_repository: ecr.IRepository | None = None,
        mirror_tags: Sequence[str] | None = None,
        pin_digest: bool = False,
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
            mirror_to_ecr: Copy the registry image into an ECR repository at deploy time so
                workloads pull in-region. Requires the ``ecr-mirror`` extra, and resolves
                the source tag to its digest at synth -- see ``pin_digest``.
            ecr_repository: The repository to mirror into. Defaults to one this construct
                creates, named ``{env_base}-{asset_name}`` and retained on stack deletion.
                Only valid alongside ``mirror_to_ecr``.
            mirror_tags: The destination tags to publish the mirrored image under, one
                ``ECRDeployment`` each. Defaults to an immutable ``sha256-<hex>`` tag plus
                the source tag as a moving alias. Only valid alongside ``mirror_to_ecr``.
            pin_digest: Resolve the source tag to its immutable digest and reference the
                image by that digest, for direct pull as well as mirroring. Off by default
                so that direct pull keeps following the mutable tag. Mirroring pins
                regardless, because a mirror of a mutable tag would freeze on first deploy.
            **image_asset_props: Additional props forwarded to
                ``aws_ecr_assets.DockerImageAsset`` (e.g. ``file``, ``platform``,
                ``build_ssh``, ``asset_name``, ``extra_hash``, ``exclude``). Only valid
                alongside ``directory``.

        Raises:
            ValueError: If neither or both of ``directory`` and ``registry_image_uri`` are
                given, if image asset props or registry-only options are given for the
                wrong one of the two, or if a mirror option is given without
                ``mirror_to_ecr``.
            ImportError: If ``mirror_to_ecr`` is set without the ``ecr-mirror`` extra.
            DigestResolutionError: If the source tag has to be resolved to a digest and the
                registry cannot be reached.
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
        if directory is not None and (
            mirror_to_ecr or pin_digest or ecr_repository is not None or mirror_tags is not None
        ):
            raise ValueError(
                "Mirroring and digest pinning apply to a `registry_image_uri`. An image "
                "built from a `directory` is already published to the CDK asset repository "
                "in ECR, and its digest is only known after the build."
            )
        if not mirror_to_ecr and (ecr_repository is not None or mirror_tags is not None):
            raise ValueError(
                "`ecr_repository` and `mirror_tags` configure a mirror, so they do nothing "
                "unless `mirror_to_ecr=True` is also passed."
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
        self._mirror_repository: ecr.IRepository | None = None
        self._mirror_tags: tuple[str, ...] = ()

        if registry_image_uri is not None and (mirror_to_ecr or pin_digest):
            image_name, tag, digest = parse_image_reference(registry_image_uri)
            # An explicitly pinned digest is already the answer, so skip the network and
            # keep synth offline-capable.
            digest = digest or resolve_image_digest(image_name, tag or DEFAULT_TAG)
            if pin_digest:
                self._registry_image_uri = f"{image_name}@{digest}"
            if mirror_to_ecr:
                self._mirror(
                    image_name=image_name,
                    tag=tag,
                    digest=digest,
                    ecr_repository=ecr_repository,
                    mirror_tags=mirror_tags,
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
        mirror_to_ecr: bool = False,
        ecr_repository: ecr.IRepository | None = None,
        mirror_tags: Sequence[str] | None = None,
        pin_digest: bool = False,
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
            mirror_to_ecr: Mirror the image into ECR. Only valid for a Container Image
                Source; see ``DockerAsset.__init__``.
            ecr_repository: The repository to mirror into. Only valid for a Container Image
                Source; see ``DockerAsset.__init__``.
            mirror_tags: The destination tags for the mirror. Only valid for a Container
                Image Source; see ``DockerAsset.__init__``.
            pin_digest: Reference the image by its resolved digest. Only valid for a
                Container Image Source; see ``DockerAsset.__init__``.
            **image_asset_props: Additional props forwarded to
                ``aws_ecr_assets.DockerImageAsset``.

        Returns:
            A Docker Asset for the given Source.

        Raises:
            ValueError: If the Source must be built but no ``directory`` was given, or if a
                registry-only option was given for a Source that has to be built.
        """
        if isinstance(source, ContainerImageSource):
            return cls.from_registry(
                scope,
                id,
                source,
                mirror_to_ecr=mirror_to_ecr,
                ecr_repository=ecr_repository,
                mirror_tags=mirror_tags,
                pin_digest=pin_digest,
            )
        if mirror_to_ecr or pin_digest or ecr_repository is not None or mirror_tags is not None:
            raise ValueError(
                f"Mirroring and digest pinning are not available for {type(source).__name__}: "
                "an image built from a checkout is already published to the CDK asset "
                "repository in ECR."
            )
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
        *,
        mirror_to_ecr: bool = False,
        ecr_repository: ecr.IRepository | None = None,
        mirror_tags: Sequence[str] | None = None,
        pin_digest: bool = False,
    ) -> "DockerAsset":
        """Create a Docker Asset from an image that is already published to a registry.

        Args:
            scope: The parent construct.
            id: The construct id.
            image: A Container Image Source, or an image URI string.
            mirror_to_ecr: Mirror the image into ECR; see ``DockerAsset.__init__``.
            ecr_repository: The repository to mirror into; see ``DockerAsset.__init__``.
            mirror_tags: The destination tags for the mirror; see ``DockerAsset.__init__``.
            pin_digest: Reference the image by its resolved digest; see
                ``DockerAsset.__init__``.

        Returns:
            A Docker Asset referencing the published image.
        """
        source: ContainerImageSource | None = None
        registry_image_uri: str
        if isinstance(image, ContainerImageSource):
            source, registry_image_uri = image, image.image_uri
        else:
            registry_image_uri = image
        return cls(
            scope,
            id,
            registry_image_uri=registry_image_uri,
            source=source,
            mirror_to_ecr=mirror_to_ecr,
            ecr_repository=ecr_repository,
            mirror_tags=mirror_tags,
            pin_digest=pin_digest,
        )

    # -----------------------------------------------------------------------------
    # Mirroring
    # -----------------------------------------------------------------------------

    def _mirror(
        self,
        *,
        image_name: str,
        tag: str | None,
        digest: str,
        ecr_repository: ecr.IRepository | None,
        mirror_tags: Sequence[str] | None,
    ) -> None:
        """Wire up the ECR repository and deployments that copy this image in-region.

        Args:
            image_name: The source image name, including its registry host.
            tag: The source tag, if the reference carried one.
            digest: The resolved content digest of the image to copy.
            ecr_repository: The repository to mirror into, or None to create one.
            mirror_tags: The destination tags, or None for the default pair.

        Raises:
            ValueError: If ``mirror_tags`` is empty.
            ImportError: If the ``ecr-mirror`` extra is not installed.
        """
        docker_image_name, ecr_deployment = _import_ecr_deployment()

        tags = tuple(mirror_tags) if mirror_tags is not None else _default_mirror_tags(digest, tag)
        if not tags:
            raise ValueError("`mirror_tags` must name at least one destination tag to copy to.")

        repository = ecr_repository or self._create_mirror_repository(image_name)
        # Copy from the digest, never the tag: the custom resource reruns only when its
        # properties change, and a tag's properties do not change when the tag moves.
        source_uri = f"{image_name}@{digest}"
        for mirror_tag in tags:
            ecr_deployment(
                self,
                f"{MIRROR_CONSTRUCT_ID_PREFIX}{_construct_id_fragment(mirror_tag)}",
                src=docker_image_name(source_uri),
                dest=docker_image_name(repository.repository_uri_for_tag(mirror_tag)),
            )

        self._mirror_repository = repository
        self._mirror_tags = tags

    def _create_mirror_repository(self, image_name: str) -> ecr.IRepository:
        """Create the ECR repository this image is mirrored into.

        Args:
            image_name: The source image name, whose last path segment names the asset.

        Returns:
            A repository named ``{env_base}-{asset_name}``, retained on stack deletion so
            that tearing down a stack never deletes images other stacks may still pull.
        """
        asset_name = image_name.rsplit("/", 1)[-1]
        return ecr.Repository(
            self,
            MIRROR_REPOSITORY_CONSTRUCT_ID,
            repository_name=f"{self._resolve_env_base()}-{asset_name}".lower(),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

    def _resolve_env_base(self) -> EnvBase:
        """Find the environment this asset is being deployed into.

        Returns:
            The nearest ``EnvBase`` in scope, falling back to CDK context or environment.

        Raises:
            ValueError: If no environment can be resolved, since the default repository
                name is built from it.
        """
        for scope in reversed(self.node.scopes):
            env_base = getattr(scope, "env_base", None)
            if isinstance(env_base, EnvBase):
                return env_base
        try:
            return get_env_base(self.node)
        except Exception as e:
            raise ValueError(
                f"Cannot name an ECR repository to mirror '{self.node.path}' into: no "
                "EnvBase is in scope. Place this asset under an EnvBaseStack or "
                "EnvBaseConstruct, set the env base in CDK context, or pass "
                "`ecr_repository` to mirror into a repository you name yourself."
            ) from e

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
    def mirror_repository(self) -> ecr.IRepository | None:
        """The ECR repository this image is mirrored into, or None if it is not mirrored."""
        return self._mirror_repository

    @property
    def mirror_tags(self) -> tuple[str, ...]:
        """The destination tags the mirror publishes, most specific first, or empty."""
        return self._mirror_tags

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

        For a Container Image Source that is the origin registry, digest-pinned when
        ``pin_digest`` asked for it. For a locally built image there is no upstream
        registry, so it is the URI CDK publishes the built asset to.

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
        image resolved from a Container Image Source stays at its origin registry until it
        is mirrored, and then it is in the mirror repository under the first mirror tag --
        the immutable digest tag, by default.

        Returns:
            The in-ECR image URI, or None when the image is only at its origin registry.
        """
        if self._image_asset is not None:
            return self._image_asset.image_uri
        if self._mirror_repository is not None:
            return self._mirror_repository.repository_uri_for_tag(self._mirror_tags[0])
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
        if self._mirror_repository is not None:
            # from_registry would produce the same URI but grant the execution role no
            # pull permission on the repository we just created.
            return ecs.ContainerImage.from_ecr_repository(
                self._mirror_repository, tag=self._mirror_tags[0]
            )
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
        if self._image_asset is not None:
            return lambda_.DockerImageCode.from_ecr(
                self._image_asset.repository, tag_or_digest=self._image_asset.image_tag
            )
        if self._mirror_repository is not None:
            return lambda_.DockerImageCode.from_ecr(
                self._mirror_repository, tag_or_digest=self._mirror_tags[0]
            )
        raise ValueError(
            f"Cannot use '{self.source_image_uri}' as Lambda code: Lambda container "
            "images must live in ECR, and this image is only available at its origin "
            "registry. Pass `mirror_to_ecr=True` for this asset to copy it into ECR."
        )


def _default_mirror_tags(digest: str, tag: str | None) -> tuple[str, ...]:
    """Build the destination tags a mirror publishes when the caller names none.

    An immutable digest tag so a workload can name one exact image forever, plus the source
    tag as a moving alias so ``:latest`` in ECR keeps meaning what it means upstream.

    Args:
        digest: The resolved content digest of the image being mirrored.
        tag: The source tag, if the reference carried one.

    Returns:
        The destination tags, immutable one first.
    """
    tags = [digest_to_tag(digest)]
    if tag is not None and tag not in tags:
        tags.append(tag)
    return tuple(tags)


def _construct_id_fragment(value: str) -> str:
    """Reduce a tag to something usable inside a construct id.

    Args:
        value: The tag to reduce.

    Returns:
        The tag with every non-alphanumeric character removed.
    """
    return re.sub(r"[^A-Za-z0-9]+", "", value)


def _import_ecr_deployment() -> tuple[Any, Any]:
    """Import the optional ``cdk-ecr-deployment`` package, lazily.

    Mirroring is opt-in per Docker Asset, so its dependency is opt-in too: ``gcs``, ``ocs``,
    and ``ocs-deploy`` all pin ``cdk-ecr-deployment~=3.1.13`` while upstream supports only
    v4, and a hard dependency on either major would make those repos unresolvable.

    Returns:
        The ``DockerImageName`` and ``ECRDeployment`` classes.

    Raises:
        ImportError: If ``cdk-ecr-deployment`` is not installed, naming the extra.
    """
    try:
        from cdk_ecr_deployment import DockerImageName, ECRDeployment
    except ImportError as e:
        raise ImportError(
            "Mirroring a Docker Asset into ECR needs `cdk-ecr-deployment`, which is an "
            "optional dependency of aibs-informatics-cdk-lib. Install it with the "
            "`ecr-mirror` extra (e.g. `pip install 'aibs-informatics-cdk-lib[ecr-mirror]'` "
            "or `uv add 'aibs-informatics-cdk-lib[ecr-mirror]'`), or drop `mirror_to_ecr` "
            "and pull the image directly from its origin registry."
        ) from e
    return DockerImageName, ECRDeployment


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
