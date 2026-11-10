"""Tests for synth-time Digest Pinning.

Every test mocks ``urlopen``. Nothing here touches the network -- the point of pinning is
that a mirror stays correct, not that a registry answers.
"""

import json
import urllib.error
import urllib.request
from unittest.mock import patch

import pytest
from pytest import mark, param

from aibs_informatics_cdk_lib.constructs_.assets.registry import (
    DIGEST_HEADER,
    MANIFEST_MEDIA_TYPES,
    DigestResolutionError,
    digest_to_tag,
    parse_image_reference,
    resolve_image_digest,
)

IMAGE_NAME = "ghcr.io/alleninstitute/aibs-informatics-aws-lambda"
DIGEST = "sha256:" + "ab" * 32
TOKEN_URL = (
    "https://ghcr.io/token"
    "?scope=repository:alleninstitute/aibs-informatics-aws-lambda:pull&service=ghcr.io"
)
MANIFEST_URL = "https://ghcr.io/v2/alleninstitute/aibs-informatics-aws-lambda/manifests/latest"


class FakeResponse:
    """The subset of an ``http.client.HTTPResponse`` the resolver uses."""

    def __init__(self, body: bytes = b"", headers: dict[str, str] | None = None):
        self._body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def registry_responses(
    token_payload: dict | None = None,
    manifest_headers: dict[str, str] | None = None,
):
    """Build a ``urlopen`` side effect answering the token and manifest requests.

    Args:
        token_payload: The JSON body the token endpoint returns.
        manifest_headers: The headers the manifest endpoint returns.

    Returns:
        A callable suitable as a ``urlopen`` mock side effect, and the list it records
        every request into.
    """
    requests: list[urllib.request.Request] = []

    def urlopen(request: urllib.request.Request, **kwargs: object) -> FakeResponse:
        requests.append(request)
        if "/token" in request.full_url:
            payload = token_payload if token_payload is not None else {"token": "t0ken"}
            return FakeResponse(body=json.dumps(payload).encode("utf-8"))
        headers = manifest_headers if manifest_headers is not None else {DIGEST_HEADER: DIGEST}
        return FakeResponse(headers=headers)

    return urlopen, requests


# ---------------------------------------------------------------------------
# parse_image_reference
# ---------------------------------------------------------------------------


class TestParseImageReference:
    @mark.parametrize(
        "image_uri, expected",
        [
            param("ghcr.io/org/repo:v1.2.3", ("ghcr.io/org/repo", "v1.2.3", None), id="tag"),
            param("ghcr.io/org/repo", ("ghcr.io/org/repo", None, None), id="no-tag"),
            param(
                f"ghcr.io/org/repo@{DIGEST}",
                ("ghcr.io/org/repo", None, DIGEST),
                id="digest",
            ),
            param(
                f"ghcr.io/org/repo:v1@{DIGEST}",
                ("ghcr.io/org/repo", "v1", DIGEST),
                id="tag-and-digest",
            ),
            param(
                "registry.local:5000/org/repo:v1",
                ("registry.local:5000/org/repo", "v1", None),
                id="registry-port-and-tag",
            ),
            param(
                "registry.local:5000/org/repo",
                ("registry.local:5000/org/repo", None, None),
                id="registry-port-is-not-a-tag",
            ),
        ],
    )
    def test__parse_image_reference__splits_name_tag_and_digest(self, image_uri, expected):
        assert parse_image_reference(image_uri) == expected


# ---------------------------------------------------------------------------
# digest_to_tag
# ---------------------------------------------------------------------------


class TestDigestToTag:
    def test__digest_to_tag__replaces_the_illegal_colon(self):
        """`:` is illegal in an ECR tag, so the digest tag has to be sanitized."""
        assert digest_to_tag(DIGEST) == "sha256-" + "ab" * 32

    def test__digest_to_tag__leaves_a_sanitized_digest_alone(self):
        assert digest_to_tag("sha256-abc") == "sha256-abc"


# ---------------------------------------------------------------------------
# resolve_image_digest
# ---------------------------------------------------------------------------


class TestResolveImageDigest:
    def test__resolve_image_digest__returns_the_digest_header(self):
        urlopen, _ = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            assert resolve_image_digest(IMAGE_NAME, "latest") == DIGEST

    def test__resolve_image_digest__requests_an_anonymous_pull_scoped_token(self):
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME, "latest")
        assert requests[0].full_url == TOKEN_URL

    def test__resolve_image_digest__heads_the_manifest_with_the_bearer_token(self):
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME, "latest")
        manifest_request = requests[1]
        assert manifest_request.full_url == MANIFEST_URL
        assert manifest_request.get_method() == "HEAD"
        assert manifest_request.get_header("Authorization") == "Bearer t0ken"

    def test__resolve_image_digest__accepts_multi_platform_indexes(self):
        """A multi-platform image must resolve to its index digest, not one platform's."""
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME, "latest")
        accept = requests[1].get_header("Accept")
        assert accept is not None
        for media_type in MANIFEST_MEDIA_TYPES:
            assert media_type in accept

    def test__resolve_image_digest__defaults_to_the_latest_tag(self):
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME)
        assert requests[1].full_url.endswith("/manifests/latest")

    def test__resolve_image_digest__accepts_an_access_token_field(self):
        urlopen, _ = registry_responses(token_payload={"access_token": "other"})
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            assert resolve_image_digest(IMAGE_NAME, "latest") == DIGEST

    def test__resolve_image_digest__uses_a_supplied_auth_header(self):
        """Auth is pluggable for a future private package; no token request is made."""
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME, "latest", auth_header=lambda reg, repo: "Bearer mine")
        assert len(requests) == 1
        assert requests[0].get_header("Authorization") == "Bearer mine"

    def test__resolve_image_digest__auth_header_provider_sees_registry_and_repository(self):
        urlopen, _ = registry_responses()
        seen: list[tuple[str, str]] = []
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(
                IMAGE_NAME,
                "latest",
                auth_header=lambda reg, repo: seen.append((reg, repo)) or None,  # type: ignore
            )
        assert seen == [("ghcr.io", "alleninstitute/aibs-informatics-aws-lambda")]

    def test__resolve_image_digest__omits_authorization_when_the_provider_returns_none(self):
        urlopen, requests = registry_responses()
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            resolve_image_digest(IMAGE_NAME, "latest", auth_header=lambda reg, repo: None)
        assert requests[0].get_header("Authorization") is None


class TestResolveImageDigestFailures:
    """Resolution failure is a hard error naming the fix, never a fallback to the tag."""

    def test__resolve_image_digest__unreachable_registry_raises(self):
        with patch.object(urllib.request, "urlopen", side_effect=urllib.error.URLError("no dns")):
            with pytest.raises(DigestResolutionError, match="Could not resolve"):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__failure_names_the_explicit_digest_fix(self):
        with patch.object(urllib.request, "urlopen", side_effect=urllib.error.URLError("no dns")):
            with pytest.raises(DigestResolutionError, match=r"@sha256:<hex>"):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__http_error_raises(self):
        error = urllib.error.HTTPError(MANIFEST_URL, 404, "Not Found", {}, None)  # type: ignore
        with patch.object(urllib.request, "urlopen", side_effect=error):
            with pytest.raises(DigestResolutionError, match="HTTPError"):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__unparseable_token_response_raises(self):
        def urlopen(request, **kwargs):
            return FakeResponse(body=b"<html>not json</html>")

        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            with pytest.raises(DigestResolutionError, match="Could not resolve"):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__token_denied_raises_naming_private_packages(self):
        urlopen, _ = registry_responses(token_payload={"errors": ["denied"]})
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            with pytest.raises(DigestResolutionError, match="no anonymous pull token"):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__missing_digest_header_raises(self):
        urlopen, _ = registry_responses(manifest_headers={})
        with patch.object(urllib.request, "urlopen", side_effect=urlopen):
            with pytest.raises(DigestResolutionError, match=DIGEST_HEADER):
                resolve_image_digest(IMAGE_NAME, "latest")

    def test__resolve_image_digest__image_without_a_registry_host_raises(self):
        with patch.object(urllib.request, "urlopen") as mock_urlopen:
            with pytest.raises(DigestResolutionError, match="no registry host"):
                resolve_image_digest("repo", "latest")
        mock_urlopen.assert_not_called()
