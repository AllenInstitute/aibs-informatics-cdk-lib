"""Package source models for resolving code and container image assets.

This module provides Pydantic models for referencing specific versions of a package,
whether from a Git repository or a container image registry.
"""

import re
from typing import Annotated, Literal

from pydantic import BaseModel, Discriminator

from aibs_informatics_cdk_lib.common.git import GitUrl, is_local_repo, is_repo_url

# Matches container image references: registry.tld/path[:tag][@sha256:digest]
_CONTAINER_IMAGE_RE = re.compile(
    r"^(?P<image>[\w.\-]+\.[\w.\-]+(?:/[\w.\-]+)+)"
    r"(?::(?P<tag>[\w][\w.\-]*))?(?:@(?P<digest>sha256:[a-fA-F0-9]+))?$"
)


class PackageSource(BaseModel):
    """Base reference to a specific version of a package."""

    source_type: str

    def version_id(self) -> str:
        """Return a deterministic identifier for asset hashing and cache keys."""
        raise NotImplementedError

    @classmethod
    def from_str(cls, value: str) -> "PackageSource":
        """Parse a string into the appropriate source type.

        Handles:
          - Git SSH/HTTPS URLs:  git@github.com:org/repo.git
          - Git URLs with ref:   git@github.com:org/repo.git#v1.2.3
          - Local repo paths:    /home/user/workspace/my-repo
          - Container images:    ghcr.io/org/repo:tag
          - Container digests:   ghcr.io/org/repo@sha256:abc123

        Args:
            value: The string to parse.

        Returns:
            A GitSource or ContainerImageSource instance.

        Raises:
            ValueError: If the string cannot be parsed as any known source type.
        """
        if is_repo_url(value):
            git_url = GitUrl(value)
            return GitSource(url=git_url.repo_base_url, branch=git_url.ref)

        if is_local_repo(value):
            return GitSource(url=value)

        match = _CONTAINER_IMAGE_RE.match(value)
        if match:
            return ContainerImageSource(
                image=match.group("image"),
                tag=match.group("tag") or "latest",
                digest=match.group("digest"),
            )

        raise ValueError(
            f"Cannot parse '{value}' as a package source. "
            "Expected a git URL, local repo path, or container image reference."
        )


class GitSource(PackageSource):
    """Reference to a git repository, optionally pinned to a specific ref."""

    source_type: Literal["git"] = "git"
    url: str
    branch: str | None = None
    commit: str | None = None
    tag: str | None = None

    def version_id(self) -> str:
        return self.commit or self.tag or self.branch or "HEAD"

    @property
    def repo_url_with_ref(self) -> str:
        """Reconstruct the URL with ref appended (e.g. repo.git#v1.2.3).

        Compatible with the existing clone_repo / resolve_repo_path functions
        which parse the #ref fragment.
        """
        ref = self.commit or self.tag or self.branch
        if ref:
            return f"{self.url}#{ref}"
        return self.url


class ContainerImageSource(PackageSource):
    """Reference to a pre-built container image in a registry."""

    source_type: Literal["container"] = "container"
    image: str
    tag: str = "latest"
    digest: str | None = None

    def version_id(self) -> str:
        return self.digest or self.tag

    @property
    def image_uri(self) -> str:
        """Full image URI suitable for ECR/Batch/ECS consumption."""
        if self.digest:
            return f"{self.image}@{self.digest}"
        return f"{self.image}:{self.tag}"


PackageSourceType = Annotated[
    GitSource | ContainerImageSource,
    Discriminator("source_type"),
]
"""Discriminated union type for use in other Pydantic models."""
