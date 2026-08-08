from unittest.mock import call, patch

import pytest
from aibs_informatics_test_resources import BaseTest
from pytest import mark, param

from aibs_informatics_cdk_lib.common.git import (
    GitUrl,
    clone_repo,
    construct_repo_path,
    get_commit_hash,
    get_commit_hash_from_url,
    get_repo_name,
    get_repo_url_components,
    is_local_repo,
    is_repo_url,
)

FULL_SHA = "a" * 40
SHORT_SHA = "abc1234"


@mark.parametrize(
    "repo_url, expected_components",
    [
        param(
            "https://github.com/org/package.git",
            ("https://github.com/org/package.git", None),
            id="https",
        ),
        param(
            "https://github.com/org/package.git@branch",
            ("https://github.com/org/package.git", "branch"),
            id="https @branch",
        ),
        param(
            "https://github.com/org/package@branch/name",
            ("https://github.com/org/package.git", "branch/name"),
            id="https @branch/name no .git",
        ),
        param(
            "https://github.com/org/package.git@branch/name",
            ("https://github.com/org/package.git", "branch/name"),
            id="https @branch/name",
        ),
        param(
            "https://github.com/org/package.git#branch",
            ("https://github.com/org/package.git", "branch"),
            id="https #branch",
        ),
        param(
            "https://github.com/org/package.git/tree/branch",
            ("https://github.com/org/package.git", "branch"),
            id="https tree/branch",
        ),
        param(
            "https://github.com/org/package/tree/branch",
            ("https://github.com/org/package.git", "branch"),
            id="https tree/branch no .git",
        ),
        param(
            "https://github.com/org/package.git/tree/branch/name",
            ("https://github.com/org/package.git", "branch/name"),
            id="https tree/branch/name",
        ),
        param(
            "ssh://github.com/org/package.git",
            ("ssh://github.com/org/package.git", None),
            id="ssh url",
        ),
        param(
            "ssh://github.com/org/package.git@branch",
            ("ssh://github.com/org/package.git", "branch"),
            id="ssh url branch",
        ),
        param(
            "git@github.com:org/package.git",
            ("git@github.com:org/package.git", None),
            id="ssh",
        ),
        param(
            "git@github.com:org/package.git@branch",
            ("git@github.com:org/package.git", "branch"),
            id="ssh branch",
        ),
        param(
            "git@github.com:org/package.git@branch/name",
            ("git@github.com:org/package.git", "branch/name"),
            id="ssh branch/name",
        ),
        param(
            "git@github.com:github/octoforce-actions.git@v1.0.0",
            ("git@github.com:github/octoforce-actions.git", "v1.0.0"),
            id="ssh branch2",
        ),
        param(
            "https://github.com/org/package/releases/tag/v1.0.0",
            ("https://github.com/org/package.git", "v1.0.0"),
            id="https releases/tag",
        ),
        param(
            f"https://github.com/org/package/commit/{SHORT_SHA}",
            ("https://github.com/org/package.git", SHORT_SHA),
            id="https commit",
        ),
        param(
            "git@github.com:org/package.git#refs/heads/branch/name",
            ("git@github.com:org/package.git", "branch/name"),
            id="ssh refs/heads is reported by short name",
        ),
        param(
            "git@github.com:org/package.git#refs/tags/v1.0.0",
            ("git@github.com:org/package.git", "v1.0.0"),
            id="ssh refs/tags is reported by short name",
        ),
    ],
)
def test__get_url_components(repo_url, expected_components):
    git_url_components = get_repo_url_components(repo_url)
    assert git_url_components == expected_components


@mark.parametrize(
    "repo_url, expected_ref, expected_ref_kind",
    [
        param("git@github.com:org/package.git", None, None, id="no ref"),
        param(
            "git@github.com:org/package.git#refs/heads/main",
            "main",
            "branch",
            id="refs/heads is a branch",
        ),
        param(
            "https://github.com/org/package/tree/refs/heads/release/1.0",
            "release/1.0",
            "branch",
            id="tree refs/heads is a branch",
        ),
        param(
            "git@github.com:org/package.git#refs/tags/v1.2.3",
            "v1.2.3",
            "tag",
            id="refs/tags is a tag",
        ),
        param(
            "https://github.com/org/package/releases/tag/v1.2.3",
            "v1.2.3",
            "tag",
            id="releases/tag is a tag",
        ),
        param(
            f"https://github.com/org/package/commit/{SHORT_SHA}",
            SHORT_SHA,
            "commit",
            id="commit path is a commit",
        ),
        param(
            f"https://github.com/org/package/commit/{FULL_SHA}",
            FULL_SHA,
            "commit",
            id="commit path with full sha is a commit",
        ),
        param(
            f"git@github.com:org/package.git#{SHORT_SHA}",
            SHORT_SHA,
            "commit",
            id="bare short sha is a commit",
        ),
        param(
            f"git@github.com:org/package.git@{FULL_SHA}",
            FULL_SHA,
            "commit",
            id="bare full sha is a commit",
        ),
        # Anything the URL does not classify is reported as a branch. `/tree/` is GitHub's
        # segment for branches, tags and SHAs alike, and branch vs tag changes nothing
        # downstream -- only commit does.
        param("git@github.com:org/package.git#main", "main", "branch", id="ambiguous name"),
        param("git@github.com:org/package.git#v1.2.3", "v1.2.3", "branch", id="ambiguous tag"),
        param("https://github.com/org/package/tree/main", "main", "branch", id="ambiguous tree"),
        param(
            "git@github.com:org/package.git#abc123",
            "abc123",
            "branch",
            id="six hex chars is too short to be a sha",
        ),
    ],
)
def test__git_url__ref_and_ref_kind(repo_url, expected_ref, expected_ref_kind):
    git_url = GitUrl(repo_url)
    assert git_url.ref == expected_ref
    assert git_url.ref_kind == expected_ref_kind


@patch("aibs_informatics_cdk_lib.common.git.subprocess.check_output")
def test__get_commit_hash_from_url__commit_ref_short_circuits(mock_check_output):
    """A commit SHA is already a commit hash -- ls-remote would report nothing for it."""
    assert get_commit_hash_from_url(f"git@github.com:org/package.git#{FULL_SHA}") == FULL_SHA
    mock_check_output.assert_not_called()


@patch("aibs_informatics_cdk_lib.common.git.subprocess.check_output", return_value=b"")
def test__get_commit_hash_from_url__unresolvable_ref_raises(mock_check_output):
    with pytest.raises(ValueError, match="Could not resolve ref 'no-such-branch'"):
        get_commit_hash_from_url("git@github.com:org/package.git#no-such-branch")


@patch("aibs_informatics_cdk_lib.common.git.subprocess.check_output")
def test__construct_repo_path__commit_ref_uses_the_sha(mock_check_output, tmp_path):
    target_path = construct_repo_path(f"git@github.com:org/package.git#{FULL_SHA}", tmp_path)
    assert target_path == tmp_path / f"package_{FULL_SHA}"
    mock_check_output.assert_not_called()


@patch("aibs_informatics_cdk_lib.common.git.subprocess.check_call")
def test__clone_repo__commit_ref_fetches_and_checks_out(mock_check_call, tmp_path):
    """`git clone --branch` takes branch and tag names only, so a commit needs a checkout."""
    target_path = clone_repo(f"git@github.com:org/package.git#{FULL_SHA}", tmp_path)

    assert target_path == tmp_path / f"package_{FULL_SHA}"
    assert mock_check_call.call_args_list == [
        call(
            [
                "git",
                "clone",
                "git@github.com:org/package.git",
                target_path.as_posix(),
                "--single-branch",
            ]
        ),
        call(["git", "fetch", "origin", FULL_SHA], cwd=target_path),
        call(["git", "checkout", FULL_SHA], cwd=target_path),
    ]


@patch("aibs_informatics_cdk_lib.common.git.get_commit_hash_from_url", return_value="b" * 40)
@patch("aibs_informatics_cdk_lib.common.git.subprocess.check_call")
def test__clone_repo__tag_ref_uses_branch_flag(mock_check_call, mock_get_commit_hash, tmp_path):
    target_path = clone_repo("https://github.com/org/package/releases/tag/v1.0.0", tmp_path)

    assert mock_check_call.call_args_list == [
        call(
            [
                "git",
                "clone",
                "https://github.com/org/package.git",
                target_path.as_posix(),
                "--single-branch",
                "--branch",
                "v1.0.0",
            ]
        )
    ]


class GitTests(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        # Why did I pick this repo? It has the following:
        # 1. a public repo
        # 2. It is maintained by GitHub (stable)
        # 3. It has a release tag
        # 4. It is minimal in size
        #
        # This is to ensure that the tests are stable and fast
        # If the repo is not available, the tests will fail
        self.GIT_URL_HTTPS = "https://github.com/github/octoforce-actions/tree/v1.0.0"
        self.GIT_URL_SSH = "git@github.com:github/octoforce-actions.git@v1.0.0"
        self.GIT_REPO_NAME = "octoforce-actions"

    def test__is_repo_url(self):
        assert is_repo_url(self.GIT_URL_HTTPS)
        assert not is_repo_url(self.tmp_path().as_posix())

    def test__is_local_repo(self):
        p = clone_repo(self.GIT_URL_HTTPS, self.tmp_path())
        assert is_local_repo(p.as_posix())
        assert not is_local_repo(p.parent.as_posix())
        assert not is_local_repo(self.GIT_URL_HTTPS)

    def test__get_repo_name__works_for_url(self):
        repo_name = get_repo_name(self.GIT_URL_HTTPS)
        assert repo_name == self.GIT_REPO_NAME

    def test__get_repo_name__works_for_path(self):
        repo_path = clone_repo(self.GIT_URL_HTTPS, self.tmp_path())
        repo_name = get_repo_name(repo_path.as_posix())
        assert repo_name == self.GIT_REPO_NAME

    def test__get_repo_name__fails_for_invalid_path(self):
        with self.assertRaises(ValueError):
            get_repo_name(self.tmp_path().as_posix())

    def test_construct_repo_path__creates_same_path_for_same_commit(self):
        root = self.tmp_path()

        p1a = construct_repo_path("https://github.com/github/check-all/tree/v0.4.0", root)
        p1b = construct_repo_path("git@github.com:github/check-all.git@v0.4.0", root)
        # NOTE: this used to read `0.3.0`, a tag that does not exist. It "passed" only
        # because an unresolvable ref used to yield an empty commit hash.
        p2 = construct_repo_path("https://github.com/github/check-all/tree/v0.3.0", root)
        assert p1a == p1b
        assert p1a != p2

    def test__get_commit_hash__handles_url_and_path(self):
        commit_hash1 = get_commit_hash(self.GIT_URL_HTTPS)
        repo_path = clone_repo(self.GIT_URL_HTTPS, self.tmp_path())
        commit_hash2 = get_commit_hash(repo_path)
        assert commit_hash1 == commit_hash2

    def test__clone_repo__should_understand_https_ssh_repos(self):
        # Arrange
        root = self.tmp_path()

        # Act
        path_https = clone_repo(self.GIT_URL_HTTPS, root)
        ct_time_1 = path_https.stat().st_ctime

        path_ssh = clone_repo(self.GIT_URL_SSH, root, skip_if_exists=True)
        ct_time_2 = path_ssh.stat().st_ctime

        path_ssh = clone_repo(self.GIT_URL_SSH, root, skip_if_exists=False)
        ct_time_3 = path_ssh.stat().st_ctime

        # Assert
        assert path_https.exists()
        assert path_ssh.exists()
        assert path_ssh == path_https
        assert ct_time_1 == ct_time_2
        assert ct_time_1 != ct_time_3
