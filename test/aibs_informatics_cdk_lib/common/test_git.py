from aibs_informatics_test_resources import BaseTest
from pytest import mark, param

from aibs_informatics_cdk_lib.common.git import (
    clone_repo,
    construct_repo_path,
    get_commit_hash,
    get_repo_name,
    get_repo_url_components,
    is_local_repo,
    is_repo_url,
)


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
            "git@github.com:github/octoforce-actions.git@v1.0.0",
            ("git@github.com:github/octoforce-actions.git", "v1.0.0"),
            id="ssh branch2",
        ),
    ],
)
def test__get_url_components(repo_url, expected_components):
    git_url_components = get_repo_url_components(repo_url)
    assert git_url_components == expected_components


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
        p2 = construct_repo_path("https://github.com/github/check-all/tree/0.3.0", root)
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
