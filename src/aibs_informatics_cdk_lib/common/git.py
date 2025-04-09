import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import ClassVar, Optional, Union

from aibs_informatics_core.collections import ValidatedStr
from aibs_informatics_core.utils.file_operations import remove_path

logger = logging.getLogger(__name__)


class GitUrl(ValidatedStr):
    regex_pattern: ClassVar[re.Pattern] = re.compile(
        r"((?:(?:git|ssh|http(?:s)?)(?::\/\/)(?:[\w\.]+)\/|(?:git@(?:[\w\.]+)):)(?:[\w\.-]+)\/(?:[\w\.-]+)(?:\.git)?)(?:(?:\#|@|\/tree\/)([\w\./-]+))?"
    )

    @property
    def repo_base_url(self) -> str:
        return f"{self.get_match_groups()[0].removesuffix('.git')}.git"

    @property
    def repo_name(self) -> str:
        return os.path.basename(self.repo_base_url.removesuffix(".git"))

    @property
    def ref(self) -> Optional[str]:
        return self.get_match_groups()[-1]


# REPO_URL_PATTERN = re.compile(r"(https:\/\/github\.com\/[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+(?:\/[a-zA-Z0-9_\/.-]*)?)|(git@github\.com:[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+\.git(?:\/[a-zA-Z0-9_\/.-]*)?)")


def is_repo_url(url: str) -> bool:
    """
    Checks if a given URL is a valid Git repository URL.

    Args:
        url (str): The URL to check.

    Returns:
    bool: True if the URL is a valid Git repository URL, False otherwise.
    """
    return GitUrl.is_valid(url)


def is_local_repo(repo_path: Union[str, Path]) -> bool:
    """
    Checks if a given path is a local Git repository.

    Args:
    repo_path (Path): The file system path to check.

    Returns:
    bool: True if the path is a local Git repository, False otherwise.
    """
    repo_path = Path(repo_path)
    try:
        subprocess.check_output(["git", "-C", repo_path, "rev-parse", "--is-inside-work-tree"])
        return True
    except subprocess.CalledProcessError:
        return False


def get_commit_hash(repo_url_or_path: Union[str, Path]) -> Optional[str]:
    """
    Get the commit hash of the HEAD reference of a Git repository.

    Args:
        repo_url_or_path (Union[str, Path]): The URL or local path of the Git repository.

    Returns:
    str: The commit hash of the HEAD reference.
    """
    if isinstance(repo_url_or_path, str) and is_repo_url(repo_url_or_path):
        return get_commit_hash_from_url(repo_url_or_path)
    elif is_local_repo(repo_url_or_path):
        repo_path = Path(repo_url_or_path)
        return get_commit_hash_from_local(repo_path)
    else:
        raise ValueError("The input must be a string or a Path object.")


def get_repo_url_components(repo_url: str) -> tuple[str, Optional[str]]:
    git_url = GitUrl(repo_url)
    return (git_url.repo_base_url, git_url.ref)


def get_commit_hash_from_url(repo_url: str) -> str:
    try:
        url = GitUrl(repo_url)
        branch = url.ref or "HEAD"
        # Use git ls-remote to get the commit hashes of remote heads
        output = (
            subprocess.check_output(["git", "ls-remote", url.repo_base_url, "--branch", branch])
            .decode("utf-8")
            .strip()
        )
        # The first part of the output is the commit hash of the HEAD reference
        commit_hash = output.split("\t")[0]
        return commit_hash
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred: {e}")
        raise e


def get_commit_hash_from_local(repo_path: Union[str, Path]) -> str:
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
            f"An error occurred while trying to get the commit hash from local path {repo_path}: {e}"
        )
        raise e
    except Exception as e:
        logger.error(
            "An unexpected error occurred while trying to get the commit hash from local path "
            f"{repo_path}: {e}"
        )
        raise e


def get_repo_name(repo_url_or_path: Union[str, Path]) -> str:
    """
    Get the name of a Git repository from its URL or local path.

    Args:
        repo_url_or_path (Union[str, Path]): The URL or local path of the Git repository.

    Returns:
    str: The name of the repository.
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


def construct_repo_path(repo_url: str, target_dir: Optional[Union[str, Path]] = None) -> Path:
    target_dir = Path(target_dir) if target_dir else Path(tempfile.gettempdir())

    repo_name = get_repo_name(repo_url)
    repo_commit_hash = get_commit_hash(repo_url)

    target_base_name = f"{repo_name}_{repo_commit_hash}"

    target_repo_path = target_dir / target_base_name

    return target_repo_path


def clone_repo(
    repo_url: str, target_dir: Optional[Union[str, Path]] = None, skip_if_exists: bool = True
) -> Path:
    """Clone a Git repository into a target directory.

    Args:
        repo_url (str): The URL of the Git repository.
        target_dir (Optional[Union[str, Path]], optional): Target prefix to store repo under.
            The repo will be written to a subdirectory. Defaults to None (defaut tmp dir used).
        skip_if_exists (bool, optional): Skip cloning if the target directory already exists.
            If the target directory exists and the commit hash matches, the function will return
            the existing path. Defaults to True.

    Returns:
        Path: The path to the cloned repository.
    """
    target_path = construct_repo_path(repo_url, target_dir)

    if target_path.exists():
        if skip_if_exists:
            repo_url_commit_hash = get_commit_hash(repo_url)
            try:
                target_path_commit_hash = get_commit_hash(target_path)
            except Exception as e:
                logger.warning(
                    f"An error occurred while checking the commit hash of the existing repository: {e}"
                    "Removing the existing path and proceeding with cloning into the following path: "
                    f"{target_path}"
                )

            else:
                if target_path_commit_hash == repo_url_commit_hash:
                    # If the commit hashes match, return the existing path
                    logger.info(
                        f"Skipping cloning of repository as target path already exists: {target_path}"
                    )
                return target_path
        # If the target path exists but the commit hashes do not match, remove the existing path
        remove_path(target_path)
    try:
        # Clone the repository into the target directory
        base_url, branch = get_repo_url_components(repo_url)
        cmd: list[str] = ["git", "clone", base_url, target_path.as_posix(), "--single-branch"]
        if branch:
            cmd.extend(["--branch", branch])
        subprocess.check_call(cmd)
        return target_path
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred while trying to clone the repo: {e}")
        raise e
