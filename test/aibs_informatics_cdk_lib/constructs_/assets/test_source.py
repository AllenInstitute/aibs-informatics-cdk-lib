import pytest
from pytest import mark, param

from aibs_informatics_cdk_lib.constructs_.assets.source import (
    ContainerImageSource,
    GitSource,
    PackageSource,
    PackageSourceType,
)


# ---------------------------------------------------------------------------
# GitSource
# ---------------------------------------------------------------------------


class TestGitSource:
    def test__version_id__prefers_commit(self):
        source = GitSource(url="git@github.com:org/repo.git", commit="abc123", tag="v1", branch="main")
        assert source.version_id() == "abc123"

    def test__version_id__falls_back_to_tag(self):
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.2.3", branch="main")
        assert source.version_id() == "v1.2.3"

    def test__version_id__falls_back_to_branch(self):
        source = GitSource(url="git@github.com:org/repo.git", branch="feature-x")
        assert source.version_id() == "feature-x"

    def test__version_id__defaults_to_HEAD(self):
        source = GitSource(url="git@github.com:org/repo.git")
        assert source.version_id() == "HEAD"

    def test__repo_url_with_ref__no_ref(self):
        source = GitSource(url="git@github.com:org/repo.git")
        assert source.repo_url_with_ref == "git@github.com:org/repo.git"

    def test__repo_url_with_ref__with_branch(self):
        source = GitSource(url="git@github.com:org/repo.git", branch="main")
        assert source.repo_url_with_ref == "git@github.com:org/repo.git#main"

    def test__repo_url_with_ref__with_tag(self):
        source = GitSource(url="git@github.com:org/repo.git", tag="v1.0.0")
        assert source.repo_url_with_ref == "git@github.com:org/repo.git#v1.0.0"

    def test__repo_url_with_ref__with_commit(self):
        source = GitSource(url="git@github.com:org/repo.git", commit="abc123")
        assert source.repo_url_with_ref == "git@github.com:org/repo.git#abc123"

    def test__repo_url_with_ref__prefers_commit_over_tag_and_branch(self):
        source = GitSource(
            url="git@github.com:org/repo.git", commit="abc123", tag="v1", branch="main"
        )
        assert source.repo_url_with_ref == "git@github.com:org/repo.git#abc123"

    def test__source_type(self):
        source = GitSource(url="git@github.com:org/repo.git")
        assert source.source_type == "git"

    def test__serialization_roundtrip(self):
        source = GitSource(url="git@github.com:org/repo.git", branch="main", tag="v1")
        data = source.model_dump()
        restored = GitSource.model_validate(data)
        assert restored == source


# ---------------------------------------------------------------------------
# ContainerImageSource
# ---------------------------------------------------------------------------


class TestContainerImageSource:
    def test__version_id__prefers_digest(self):
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1", digest="sha256:abc")
        assert source.version_id() == "sha256:abc"

    def test__version_id__falls_back_to_tag(self):
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1.2.3")
        assert source.version_id() == "v1.2.3"

    def test__image_uri__with_tag(self):
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1.2.3")
        assert source.image_uri == "ghcr.io/org/repo:v1.2.3"

    def test__image_uri__with_digest(self):
        source = ContainerImageSource(image="ghcr.io/org/repo", digest="sha256:abc123")
        assert source.image_uri == "ghcr.io/org/repo@sha256:abc123"

    def test__image_uri__defaults_to_latest(self):
        source = ContainerImageSource(image="ghcr.io/org/repo")
        assert source.image_uri == "ghcr.io/org/repo:latest"
        assert source.tag == "latest"

    def test__source_type(self):
        source = ContainerImageSource(image="ghcr.io/org/repo")
        assert source.source_type == "container"

    def test__serialization_roundtrip(self):
        source = ContainerImageSource(image="ghcr.io/org/repo", tag="v1", digest="sha256:abc")
        data = source.model_dump()
        restored = ContainerImageSource.model_validate(data)
        assert restored == source


# ---------------------------------------------------------------------------
# PackageSource.from_str
# ---------------------------------------------------------------------------


class TestPackageSourceFromStr:
    @mark.parametrize(
        "value, expected_url, expected_branch",
        [
            param(
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git",
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git",
                None,
                id="ssh no ref",
            ),
            param(
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git#main",
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git",
                "main",
                id="ssh with branch",
            ),
            param(
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git#v1.2.3",
                "git@github.com:AllenInstitute/aibs-informatics-aws-lambda.git",
                "v1.2.3",
                id="ssh with tag ref",
            ),
            param(
                "https://github.com/AllenInstitute/aibs-informatics-aws-lambda.git",
                "https://github.com/AllenInstitute/aibs-informatics-aws-lambda.git",
                None,
                id="https no ref",
            ),
            param(
                "https://github.com/AllenInstitute/aibs-informatics-aws-lambda.git@main",
                "https://github.com/AllenInstitute/aibs-informatics-aws-lambda.git",
                "main",
                id="https with branch",
            ),
            param(
                "ssh://github.com/org/package.git",
                "ssh://github.com/org/package.git",
                None,
                id="ssh protocol url",
            ),
        ],
    )
    def test__from_str__git_urls(self, value, expected_url, expected_branch):
        source = PackageSource.from_str(value)
        assert isinstance(source, GitSource)
        assert source.url == expected_url
        assert source.branch == expected_branch

    @mark.parametrize(
        "value, expected_image, expected_tag, expected_digest",
        [
            param(
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:v1.2.3",
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "v1.2.3",
                None,
                id="ghcr with tag",
            ),
            param(
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:latest",
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "latest",
                None,
                id="ghcr with latest",
            ),
            param(
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "latest",
                None,
                id="ghcr no tag defaults to latest",
            ),
            param(
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda:sha-abc1234",
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "sha-abc1234",
                None,
                id="ghcr with sha tag",
            ),
            param(
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda@sha256:abcdef1234567890",
                "ghcr.io/alleninstitute/aibs-informatics-aws-lambda",
                "latest",
                "sha256:abcdef1234567890",
                id="ghcr with digest",
            ),
            param(
                "docker.io/library/python:3.11",
                "docker.io/library/python",
                "3.11",
                None,
                id="docker hub",
            ),
            param(
                "123456789.dkr.ecr.us-west-2.amazonaws.com/my-repo:v1",
                "123456789.dkr.ecr.us-west-2.amazonaws.com/my-repo",
                "v1",
                None,
                id="ecr",
            ),
        ],
    )
    def test__from_str__container_images(self, value, expected_image, expected_tag, expected_digest):
        source = PackageSource.from_str(value)
        assert isinstance(source, ContainerImageSource)
        assert source.image == expected_image
        assert source.tag == expected_tag
        assert source.digest == expected_digest

    def test__from_str__invalid_string_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            PackageSource.from_str("not-a-valid-source")

    def test__from_str__local_repo(self, tmp_path):
        """Local git repos should parse as GitSource."""
        # Initialize a git repo in the temp directory
        import subprocess

        subprocess.check_call(["git", "init", str(tmp_path)])
        subprocess.check_call(
            ["git", "commit", "--allow-empty", "-m", "init"],
            cwd=str(tmp_path),
        )

        source = PackageSource.from_str(str(tmp_path))
        assert isinstance(source, GitSource)
        assert source.url == str(tmp_path)


# ---------------------------------------------------------------------------
# Discriminated union (PackageSourceType)
# ---------------------------------------------------------------------------


class TestPackageSourceType:
    def test__discriminated_union__git_source(self):
        from pydantic import TypeAdapter

        adapter = TypeAdapter(PackageSourceType)
        result = adapter.validate_python({"source_type": "git", "url": "git@github.com:org/repo.git"})
        assert isinstance(result, GitSource)

    def test__discriminated_union__container_source(self):
        from pydantic import TypeAdapter

        adapter = TypeAdapter(PackageSourceType)
        result = adapter.validate_python(
            {"source_type": "container", "image": "ghcr.io/org/repo", "tag": "v1"}
        )
        assert isinstance(result, ContainerImageSource)
