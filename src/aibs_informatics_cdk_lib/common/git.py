"""Git utilities for repository operations.

This module provides functions for working with Git repositories,
including URL parsing, cloning, and commit hash retrieval.
"""

import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import ClassVar, Literal, cast

from aibs_informatics_core.collections import ValidatedStr
from aibs_informatics_core.utils.file_operations import remove_path

logger = logging.getLogger(__name__)

GitRefKind = Literal["branch", "tag", "commit"]
"""The kind of ref a git URL points at.

Branch and tag are interchangeable everywhere we resolve them; commit is not, and is the
only distinction that changes behaviour (see ``clone_repo`` / ``get_commit_hash_from_url``).
"""

# The repository portion of a git URL: scheme/host followed by <org>/<name>[.git]
_REPO_PATTERN = (
    r"(?P<repo>"
    r"(?:(?:git|ssh|http(?:s)?)(?::\/\/)(?:[\w\.]+)\/|(?:git@(?:[\w\.]+)):)"
    r"(?:[\w\.-]+)\/(?:[\w\.-]+)(?:\.git)?"
    r")"
)
# Separators that introduce a ref: `#ref`, `@ref`, and GitHub's `/tree/ref`
_REF_SEP_PATTERN = r"(?:\#|@|\/tree\/)"
# A ref name; may contain slashes (e.g. `release/1.0`) but never starts a new URL segment
_REF_NAME_PATTERN = r"[\w\./-]+"
# An abbreviated or full commit SHA
_SHA_PATTERN = r"[a-fA-F0-9]{7,40}"

# Ordered alternation of every ref form we can classify. Fully qualified refs come first so
# that `refs/heads/x` is not swallowed by the catch-all, and the catch-all comes last.
_REF_PATTERN = (
    r"(?:"
    rf"{_REF_SEP_PATTERN}refs\/heads\/(?P<branch>{_REF_NAME_PATTERN})"
    rf"|{_REF_SEP_PATTERN}refs\/tags\/(?P<tag>{_REF_NAME_PATTERN})"
    rf"|\/releases\/tag\/(?P<release_tag>{_REF_NAME_PATTERN})"
    rf"|\/commit\/(?P<commit_path>{_SHA_PATTERN})"
    rf"|{_REF_SEP_PATTERN}(?P<commit>{_SHA_PATTERN})"
    rf"|{_REF_SEP_PATTERN}(?P<ambiguous_ref>{_REF_NAME_PATTERN})"
    r")?"
)


class GitUrl(ValidatedStr):
    """Validated string representing a Git repository URL.

    Supports various Git URL formats including HTTPS, SSH, and git protocols.
    Can extract repository name and optional ref (branch/tag/commit).

    The ref is classified into a ``GitRefKind`` where the URL says so syntactically
    (``refs/heads/``, ``refs/tags/``, ``/releases/tag/``, ``/commit/``, or a bare SHA).
    Anything else -- ``#v1.2.3``, ``/tree/main`` -- is reported as a branch. This is
    deliberate: ``/tree/`` is GitHub's segment for branches, tags *and* SHAs, so no pattern
    can tell them apart, and branch vs tag makes no difference to anything downstream
    (``git ls-remote --branch v1.0.0`` resolves tags just fine). Only commit is special.

    Attributes:
        regex_pattern: Compiled regex pattern for URL validation.
        REF_GROUP_KINDS: Ref capture group name -> the kind of ref it represents.
    """

    regex_pattern: ClassVar[re.Pattern] = re.compile(_REPO_PATTERN + _REF_PATTERN)

    REF_GROUP_KINDS: ClassVar[dict[str, GitRefKind]] = {
        "branch": "branch",
        "tag": "tag",
        "release_tag": "tag",
        "commit_path": "commit",
        "commit": "commit",
        "ambiguous_ref": "branch",
    }

    @property
    def match_groups(self) -> Mapping[str, str | None]:
        """Named capture groups from matching this URL against ``regex_pattern``."""
        # Validation at construction time guarantees a full match.
        return cast(re.Match, self.regex_pattern.fullmatch(self)).groupdict()

    @property
    def repo_base_url(self) -> str:
        return f"{cast(str, self.match_groups['repo']).removesuffix('.git')}.git"

    @property
    def repo_name(self) -> str:
        return os.path.basename(self.repo_base_url.removesuffix(".git"))

    @property
    def ref(self) -> str | None:
        """The ref this URL pins, or None if it names no ref.

        Fully qualified refs are reported by their short name, so that ``#refs/tags/v1.0.0``
        and ``@v1.0.0`` both yield ``v1.0.0`` -- the form git's ``--branch`` accepts.
        """
        groups = self.match_groups
        for group_name in self.REF_GROUP_KINDS:
            if (value := groups[group_name]) is not None:
                return value
        return None

    @property
    def ref_kind(self) -> GitRefKind | None:
        """The kind of ref this URL pins, or None if it names no ref.

        Ambiguous refs are reported as ``"branch"``. See the class docstring for why.
        """
        groups = self.match_groups
        for group_name, ref_kind in self.REF_GROUP_KINDS.items():
            if groups[group_name] is not None:
                return ref_kind
        return None


def is_repo_url(url: str) -> bool:
    """Check if a URL is a valid Git repository URL.

    Args:
        url (str): The URL to validate.

    Returns:
        True if the URL is a valid Git repository URL, False otherwise.
    """
    return GitUrl.is_valid(url)


def is_local_repo(repo_path: str | Path) -> bool:
    """Check if a path is a local Git repository.

    Args:
        repo_path (Union[str, Path]): The file system path to check.

    Returns:
        True if the path is a local Git repository, False otherwise.
    """
    repo_path = Path(repo_path)
    try:
        subprocess.check_output(["git", "-C", repo_path, "rev-parse", "--is-inside-work-tree"])
        return True
    except subprocess.CalledProcessError:
        return False


def get_commit_hash(repo_url_or_path: str | Path) -> str | None:
    """Get the HEAD commit hash of a Git repository.

    Args:
        repo_url_or_path (Union[str, Path]): The repository URL or local path.

    Returns:
        The commit hash of the HEAD reference.

    Raises:
        ValueError: If the input is neither a valid URL nor a local repository.
    """
    if isinstance(repo_url_or_path, str) and is_repo_url(repo_url_or_path):
        return get_commit_hash_from_url(repo_url_or_path)
    elif is_local_repo(repo_url_or_path):
        repo_path = Path(repo_url_or_path)
        return get_commit_hash_from_local(repo_path)
    else:
        raise ValueError("The input must be a string or a Path object.")


def get_repo_url_components(repo_url: str) -> tuple[str, str | None]:
    """Extract base URL and ref from a Git repository URL.

    Args:
        repo_url (str): The full repository URL.

    Returns:
        Tuple of (base_url, ref) where ref may be None.
    """
    git_url = GitUrl(repo_url)
    return (git_url.repo_base_url, git_url.ref)


def get_commit_hash_from_url(repo_url: str) -> str:
    """Get the commit hash from a remote Git repository URL.

    Uses git ls-remote to fetch the commit hash without cloning.

    Args:
        repo_url (str): The repository URL.

    Returns:
        The commit hash of the HEAD or specified ref.

    Raises:
        ValueError: If the ref does not resolve to any commit on the remote.
        subprocess.CalledProcessError: If git ls-remote fails.
    """
    url = GitUrl(repo_url)
    ref = url.ref
    if ref is not None and url.ref_kind == "commit":
        # A commit SHA is already the commit hash. `git ls-remote --branch` matches refs
        # only, so it would exit 0 with no output and leave us with an empty hash.
        return ref
    ref = ref or "HEAD"
    try:
        # Use git ls-remote to get the commit hashes of remote heads
        output = (
            subprocess.check_output(["git", "ls-remote", url.repo_base_url, "--branch", ref])
            .decode("utf-8")
            .strip()
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred: {e}")
        raise e
    if not output:
        raise ValueError(
            f"Could not resolve ref '{ref}' to a commit hash on {url.repo_base_url}. "
            "The ref does not exist on the remote."
        )
    # The first part of the output is the commit hash of the matched reference
    return output.split("\t")[0]


def get_commit_hash_from_local(repo_path: str | Path) -> str:
    """Get the HEAD commit hash from a local Git repository.

    Args:
        repo_path (Union[str, Path]): Path to the local repository.

    Returns:
        The commit hash of HEAD.

    Raises:
        subprocess.CalledProcessError: If git rev-parse fails.
    """
    try:
        # Get the latest commit hash
        commit_hash = (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_path)
            .decode("utf-8")
            .strip()
        )
        return commit_hash
    except subprocess.CalledProcessError as e:
        logger.error(
            f"An error occurred while trying to get the commit hash from local path {repo_path}: {e}"  # noqa: E501
        )
        raise e
    except Exception as e:
        logger.error(
            "An unexpected error occurred while trying to get the commit hash from local path "
            f"{repo_path}: {e}"
        )
        raise e


def get_repo_name(repo_url_or_path: str | Path) -> str:
    """Get the repository name from a URL or local path.

    Args:
        repo_url_or_path (Union[str, Path]): The repository URL or local path.

    Returns:
        The repository name.

    Raises:
        ValueError: If the input is neither a valid URL nor a local repository.
        subprocess.CalledProcessError: If git commands fail.
    """
    if isinstance(repo_url_or_path, str) and is_repo_url(repo_url_or_path):
        return GitUrl(repo_url_or_path).repo_name
    elif is_local_repo(repo_url_or_path):
        repo_path = Path(repo_url_or_path)
        try:
            # Get the remote URL of the 'origin' remote (commonly used name for the default remote)
            remote_url = (
                subprocess.check_output(
                    ["git", "config", "--get", "remote.origin.url"], cwd=repo_path
                )
                .decode("utf-8")
                .strip()
            )

            # Strip trailing slashes or .git if present
            remote_url = remote_url.rstrip("/").rstrip(".git")

            # Extract the repository name
            repo_name = os.path.basename(remote_url)

            return repo_name
        except subprocess.CalledProcessError as e:
            logger.error(f"An error occurred: {e}")
            raise e

    else:
        raise ValueError("The input must be a string or a Path object.")


def construct_repo_path(repo_url: str, target_dir: str | Path | None = None) -> Path:
    """Construct a deterministic path for a cloned repository.

    The path includes the repository name and commit hash to ensure
    unique paths for different versions.

    Args:
        repo_url (str): The repository URL.
        target_dir (Optional[Union[str, Path]]): Base directory for the path.
            Defaults to system temp directory.

    Returns:
        Path where the repository should be cloned.
    """
    target_dir = Path(target_dir) if target_dir else Path(tempfile.gettempdir())

    repo_name = get_repo_name(repo_url)
    repo_commit_hash = get_commit_hash(repo_url)

    target_base_name = f"{repo_name}_{repo_commit_hash}"

    target_repo_path = target_dir / target_base_name

    return target_repo_path


def clone_repo(
    repo_url: str, target_dir: str | Path | None = None, skip_if_exists: bool = True
) -> Path:
    """Clone a Git repository into a target directory.

    Args:
        repo_url (str): The URL of the Git repository.
        target_dir (Optional[Union[str, Path]]): Target directory to store repo under.
            The repo will be written to a subdirectory. Defaults to temp directory.
        skip_if_exists (bool): Skip cloning if the target directory already exists
            and the commit hash matches. Defaults to True.

    Returns:
        Path to the cloned repository.

    Raises:
        subprocess.CalledProcessError: If any of the git commands fail.
    """
    target_path = construct_repo_path(repo_url, target_dir)

    if target_path.exists():
        if skip_if_exists:
            repo_url_commit_hash = get_commit_hash(repo_url)
            try:
                target_path_commit_hash = get_commit_hash(target_path)
            except Exception as e:
                logger.warning(
                    f"An error occurred while checking the commit hash of the existing repository: {e}"  # noqa: E501
                    "Removing the existing path and proceeding with cloning into the following path: "  # noqa: E501
                    f"{target_path}"
                )

            else:
                if target_path_commit_hash == repo_url_commit_hash:
                    # If the commit hashes match, return the existing path
                    logger.info(
                        f"Skipping cloning of repository as target path already exists: {target_path}"  # noqa: E501
                    )
                return target_path
        # If the target path exists but the commit hashes do not match, remove the existing path
        remove_path(target_path)
    git_url = GitUrl(repo_url)
    ref, ref_kind = git_url.ref, git_url.ref_kind
    try:
        # Clone the repository into the target directory
        cmd: list[str] = [
            "git",
            "clone",
            git_url.repo_base_url,
            target_path.as_posix(),
            "--single-branch",
        ]
        # `--branch` accepts branch and tag names only, so a commit is fetched and checked
        # out after the fact instead.
        if ref and ref_kind != "commit":
            cmd.extend(["--branch", ref])
        subprocess.check_call(cmd)
        if ref and ref_kind == "commit":
            subprocess.check_call(["git", "fetch", "origin", ref], cwd=target_path)
            subprocess.check_call(["git", "checkout", ref], cwd=target_path)
        return target_path
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred while trying to clone the repo: {e}")
        raise e
