"""Digest Pinning -- resolving a mutable container image tag to its content digest.

A Mirror is a CloudFormation custom resource, and CloudFormation invokes one only when its
properties change. Mirroring ``:latest`` to ``:latest`` would therefore copy the image once
at first deploy and then freeze the ECR copy forever while ``latest`` moved on upstream.
Resolving the tag to its immutable digest at synth time is what makes the mirror's
properties change whenever the upstream image does, so mirroring requires it.

The lookup is two anonymous requests against the OCI Distribution API -- a pull-scoped
token, then a manifest ``HEAD`` -- issued with stdlib ``urllib`` so that Digest Pinning
costs no runtime dependency. The ``Accept`` header lists the index media types as well as
the single-manifest one, so a multi-platform image resolves to its image index digest; that
is the correct thing to pin and to mirror, because the per-platform child is selected at
pull time.

Auth is pluggable via ``auth_header`` so a token can be supplied if the package ever goes
private, but no credential plumbing exists today -- see ADR 0002.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable

logger = logging.getLogger(__name__)

DEFAULT_TAG = "latest"
"""The tag an image reference means when it names none."""

DIGEST_HEADER = "Docker-Content-Digest"
"""The manifest response header carrying the content digest."""

MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
"""Manifest media types to accept, multi-platform indexes first."""

DEFAULT_TIMEOUT: float = 10.0
"""Seconds to wait on each registry request before giving up."""

AuthHeaderProvider = Callable[[str, str], str | None]
"""Builds the ``Authorization`` header value for a ``(registry, repository)`` pair.

Returning ``None`` sends the manifest request unauthenticated.
"""


class DigestResolutionError(RuntimeError):
    """Raised when a container image tag cannot be resolved to a content digest."""


def parse_image_reference(image_uri: str) -> tuple[str, str | None, str | None]:
    """Split a container image URI into its name, tag, and digest.

    A colon only introduces a tag when it comes after the last ``/``; before that it is a
    registry port.

    Args:
        image_uri: An image reference such as ``ghcr.io/org/repo:v1`` or
            ``ghcr.io/org/repo@sha256:abc123``.

    Returns:
        A ``(name, tag, digest)`` tuple, where ``name`` includes the registry host and
        ``tag`` and ``digest`` are None when the reference does not carry them.
    """
    name, _, digest = image_uri.partition("@")
    head, separator, candidate_tag = name.rpartition(":")
    tag = None
    if separator and "/" not in candidate_tag:
        name, tag = head, candidate_tag
    return name, tag or None, digest or None


def digest_to_tag(digest: str) -> str:
    """Convert a content digest into a tag that ECR will accept.

    ``:`` is illegal in an image tag, so ``sha256:abc123`` becomes ``sha256-abc123``.

    Args:
        digest: A content digest, e.g. ``sha256:abc123``.

    Returns:
        The digest rendered as a legal tag.
    """
    return digest.replace(":", "-")


def resolve_image_digest(
    image_name: str,
    tag: str = DEFAULT_TAG,
    *,
    auth_header: AuthHeaderProvider | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> str:
    """Resolve a container image tag to the content digest it currently points at.

    Args:
        image_name: The image name including its registry host, e.g.
            ``ghcr.io/alleninstitute/aibs-informatics-aws-lambda``.
        tag: The tag to resolve. Defaults to ``latest``.
        auth_header: Builds the ``Authorization`` header for the manifest request. Defaults
            to fetching an anonymous pull-scoped token from the registry.
        timeout: Seconds to wait on each registry request.

    Returns:
        The content digest, e.g. ``sha256:abc123``.

    Raises:
        DigestResolutionError: If the registry is unreachable, refuses the request, or
            answers without a digest header. Never falls back to the mutable tag.
    """
    registry, _, repository = image_name.partition("/")
    if not repository:
        raise _resolution_error(image_name, tag, "the reference names no registry host to ask")

    provider = auth_header or (lambda reg, repo: _anonymous_auth_header(reg, repo, timeout))
    try:
        header = provider(registry, repository)
        request = urllib.request.Request(
            f"https://{registry}/v2/{repository}/manifests/{tag}",
            method="HEAD",
            headers={"Accept": ", ".join(MANIFEST_MEDIA_TYPES)},
        )
        if header:
            request.add_header("Authorization", header)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            digest = response.headers.get(DIGEST_HEADER)
    except DigestResolutionError:
        raise
    except (OSError, ValueError) as e:
        raise _resolution_error(image_name, tag, f"{type(e).__name__}: {e}") from e

    if not digest:
        raise _resolution_error(
            image_name, tag, f"the registry answered without a {DIGEST_HEADER} header"
        )

    logger.debug("Resolved %s:%s to %s", image_name, tag, digest)
    return digest


def _anonymous_auth_header(registry: str, repository: str, timeout: float) -> str | None:
    """Fetch an anonymous pull-scoped bearer token for a repository.

    Args:
        registry: The registry host, e.g. ``ghcr.io``.
        repository: The repository path within the registry, e.g. ``org/repo``.
        timeout: Seconds to wait on the token request.

    Returns:
        The ``Authorization`` header value.

    Raises:
        DigestResolutionError: If the registry issues no token.
    """
    scope = urllib.parse.quote(f"repository:{repository}:pull", safe=":/")
    token_url = f"https://{registry}/token?scope={scope}&service={registry}"
    with urllib.request.urlopen(urllib.request.Request(token_url), timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    token = payload.get("token") or payload.get("access_token")
    if not token:
        raise DigestResolutionError(
            f"Registry '{registry}' issued no anonymous pull token for '{repository}'. "
            "The package is probably private; this library builds no credential plumbing, "
            "so pin the digest explicitly instead."
        )
    return f"Bearer {token}"


def _resolution_error(image_name: str, tag: str, reason: str) -> DigestResolutionError:
    """Build the hard error raised when a tag cannot be resolved.

    Args:
        image_name: The image name that failed to resolve.
        tag: The tag that failed to resolve.
        reason: What went wrong, phrased to follow a colon.

    Returns:
        The error to raise, naming the fix.
    """
    return DigestResolutionError(
        f"Could not resolve '{image_name}:{tag}' to a content digest: {reason}. "
        "A mirror must be digest-pinned -- CloudFormation would otherwise copy the tag "
        "once and never notice it moving -- so this cannot silently fall back to the "
        f"mutable tag. Pin the digest explicitly ('{image_name}@sha256:<hex>') to skip "
        "this lookup and keep synth offline-capable, or give synth network access to "
        f"'{image_name.partition('/')[0]}'."
    )
