"""Tests for URL construction across all three API base-URL attributes and the
request() method's path-assembly logic.

URL shapes under test
─────────────────────
Attribute       US                                  EU                                  IN
raw_api         https://data.mixpanel.com/api       https://data-eu.mixpanel.com/api    https://data-in.mixpanel.com/api
import_api      https://api.mixpanel.com            https://api-eu.mixpanel.com         https://api-in.mixpanel.com
formatted_api   https://mixpanel.com/api            https://eu.mixpanel.com/api         https://in.mixpanel.com/api

request() path assembly
────────────────────────
• import_api  → <base>/<endpoint>          (no VERSION segment)
• raw_api     → <base>/<VERSION>/<endpoint>
• formatted_api → <base>/<VERSION>/<endpoint>
"""

import urllib.request
from unittest.mock import MagicMock, patch

import pytest

from mixpanel_utils import MixpanelUtils

VERSION = MixpanelUtils.VERSION  # "2.0"

_BASE_KWARGS = dict(
    service_account_username="test.a12345.mp-service-account",
    service_account_password="test123TEST123abc1234testing",
    project_id=1234567,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _client(**extra):
    return MixpanelUtils(**_BASE_KWARGS, **extra)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Base-URL attribute values per residency
# ──────────────────────────────────────────────────────────────────────────────

class TestBaseUrlAttributes:
    @pytest.mark.parametrize("residency,expected", [
        ("us", "https://data.mixpanel.com/api"),
        ("eu", "https://data-eu.mixpanel.com/api"),
        ("in", "https://data-in.mixpanel.com/api"),
    ])
    def test_raw_api(self, residency, expected):
        assert _client(residency=residency).raw_api == expected

    @pytest.mark.parametrize("residency,expected", [
        ("us", "https://api.mixpanel.com"),
        ("eu", "https://api-eu.mixpanel.com"),
        ("in", "https://api-in.mixpanel.com"),
    ])
    def test_import_api(self, residency, expected):
        assert _client(residency=residency).import_api == expected

    @pytest.mark.parametrize("residency,expected", [
        ("us", "https://mixpanel.com/api"),
        ("eu", "https://eu.mixpanel.com/api"),
        ("in", "https://in.mixpanel.com/api"),
    ])
    def test_formatted_api(self, residency, expected):
        assert _client(residency=residency).formatted_api == expected

    def test_default_residency_is_us(self):
        client = _client()
        assert client.raw_api == "https://data.mixpanel.com/api"
        assert client.import_api == "https://api.mixpanel.com"
        assert client.formatted_api == "https://mixpanel.com/api"

    def test_invalid_residency_raises_value_error(self):
        with pytest.raises(ValueError):
            _client(residency="uk")

    def test_invalid_residency_error_message(self):
        with pytest.raises(ValueError, match="residency"):
            _client(residency="ca")


# ──────────────────────────────────────────────────────────────────────────────
# 2. request() path assembly — VERSION segment presence
# ──────────────────────────────────────────────────────────────────────────────

def _captured_url(client, base_url, path_components, params=None, method="GET"):
    """Call client.request() with a mocked urlopen and return the URL that was
    passed to urllib.request.Request."""
    if params is None:
        params = {}
    captured = {}

    fake_response = MagicMock()
    fake_response.read.return_value = b'{"results": [], "status": "ok"}'

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        return fake_response

    with patch.object(urllib.request, "urlopen", side_effect=fake_urlopen):
        try:
            client.request(base_url, path_components, params, method=method)
        except Exception:
            pass  # we only care about the URL that was built

    return captured.get("url", "")


class TestRequestPathAssembly:
    def test_raw_api_includes_version_segment(self):
        client = _client()
        url = _captured_url(client, client.raw_api, ["export"])
        assert f"/api/{VERSION}/export" in url

    def test_formatted_api_includes_version_segment(self):
        client = _client()
        url = _captured_url(client, client.formatted_api, ["engage"])
        assert f"/api/{VERSION}/engage" in url

    def test_import_api_omits_version_segment(self):
        client = _client()
        url = _captured_url(
            client, client.import_api, ["import"], params={"data": b"[]"}, method="POST"
        )
        # Should go straight to /import, no /2.0/ in path
        assert f"/{VERSION}/" not in url
        assert url.startswith(client.import_api)
        assert "/import" in url

    def test_import_api_engage_endpoint_omits_version(self):
        client = _client()
        url = _captured_url(
            client, client.import_api, ["engage"], params={"data": b"[]"}, method="POST"
        )
        assert f"/{VERSION}/" not in url
        assert "/engage" in url

    def test_import_api_groups_endpoint_omits_version(self):
        client = _client()
        url = _captured_url(
            client, client.import_api, ["groups"], params={"data": b"[]"}, method="POST"
        )
        assert f"/{VERSION}/" not in url
        assert "/groups" in url

    def test_multiple_path_components_are_joined(self):
        client = _client()
        url = _captured_url(client, client.formatted_api, ["jql"])
        assert f"/api/{VERSION}/jql" in url


# ──────────────────────────────────────────────────────────────────────────────
# 3. Full assembled URLs per residency for each endpoint
# ──────────────────────────────────────────────────────────────────────────────

class TestFullUrlsPerResidency:
    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", f"https://data.mixpanel.com/api/{VERSION}/export"),
        ("eu", f"https://data-eu.mixpanel.com/api/{VERSION}/export"),
        ("in", f"https://data-in.mixpanel.com/api/{VERSION}/export"),
    ])
    def test_export_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(client, client.raw_api, ["export"])
        assert url.startswith(expected_prefix)

    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", f"https://mixpanel.com/api/{VERSION}/engage"),
        ("eu", f"https://eu.mixpanel.com/api/{VERSION}/engage"),
        ("in", f"https://in.mixpanel.com/api/{VERSION}/engage"),
    ])
    def test_engage_query_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(client, client.formatted_api, ["engage"])
        assert url.startswith(expected_prefix)

    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", f"https://mixpanel.com/api/{VERSION}/jql"),
        ("eu", f"https://eu.mixpanel.com/api/{VERSION}/jql"),
        ("in", f"https://in.mixpanel.com/api/{VERSION}/jql"),
    ])
    def test_jql_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(
            client, client.formatted_api, ["jql"], params={"script": "return []"}, method="POST"
        )
        assert url.startswith(expected_prefix)

    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", "https://api.mixpanel.com/import"),
        ("eu", "https://api-eu.mixpanel.com/import"),
        ("in", "https://api-in.mixpanel.com/import"),
    ])
    def test_import_events_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(
            client, client.import_api, ["import"], params={"data": b"[]"}, method="POST"
        )
        assert url.startswith(expected_prefix)

    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", "https://api.mixpanel.com/engage"),
        ("eu", "https://api-eu.mixpanel.com/engage"),
        ("in", "https://api-in.mixpanel.com/engage"),
    ])
    def test_import_people_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(
            client, client.import_api, ["engage"], params={"data": b"[]"}, method="POST"
        )
        assert url.startswith(expected_prefix)

    @pytest.mark.parametrize("residency,expected_prefix", [
        ("us", "https://api.mixpanel.com/groups"),
        ("eu", "https://api-eu.mixpanel.com/groups"),
        ("in", "https://api-in.mixpanel.com/groups"),
    ])
    def test_import_groups_url(self, residency, expected_prefix):
        client = _client(residency=residency)
        url = _captured_url(
            client, client.import_api, ["groups"], params={"data": b"[]"}, method="POST"
        )
        assert url.startswith(expected_prefix)


# ──────────────────────────────────────────────────────────────────────────────
# 4. project_id is always appended to the query string
# ──────────────────────────────────────────────────────────────────────────────

class TestProjectIdInQueryString:
    def test_project_id_appended_on_get(self):
        client = _client()
        url = _captured_url(client, client.raw_api, ["export"])
        assert f"project_id={client.project_id}" in url

    def test_project_id_in_body_and_verbose_flag_on_post_to_formatted_api_engage(self):
        # For POST /engage via formatted_api, project_id is encoded into the
        # request body (not the query string); the URL only gets ?verbose=1.
        client = _client()
        captured = {}
        fake_response = MagicMock()
        fake_response.read.return_value = b'{"results": [], "status": "ok"}'

        def fake_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            captured["data"] = req.data
            return fake_response

        with patch.object(urllib.request, "urlopen", side_effect=fake_urlopen):
            try:
                client.request(client.formatted_api, ["engage"], {}, method="POST")
            except Exception:
                pass

        assert "verbose=1" in captured.get("url", "")
        body = captured.get("data", b"").decode("utf-8")
        assert f"project_id={client.project_id}" in body

    def test_project_id_appended_on_import_post(self):
        client = _client()
        url = _captured_url(
            client, client.import_api, ["import"], params={"data": b"[]"}, method="POST"
        )
        assert f"project_id={client.project_id}" in url

    def test_correct_project_id_value_used(self):
        client = MixpanelUtils(**{**_BASE_KWARGS, "project_id": 9999999})
        url = _captured_url(client, client.raw_api, ["export"])
        assert "project_id=9999999" in url


# ──────────────────────────────────────────────────────────────────────────────
# 5. No cross-residency contamination
# ──────────────────────────────────────────────────────────────────────────────

class TestNoCrossResidencyContamination:
    def test_eu_raw_api_does_not_contain_us_domain(self):
        client = _client(residency="eu")
        assert "data.mixpanel.com" not in client.raw_api
        assert "data-eu.mixpanel.com" in client.raw_api

    def test_in_import_api_does_not_contain_eu_segment(self):
        client = _client(residency="in")
        assert "-eu" not in client.import_api
        assert "-in" in client.import_api

    def test_us_urls_contain_no_residency_infix(self):
        client = _client(residency="us")
        for attr in ("raw_api", "import_api", "formatted_api"):
            url = getattr(client, attr)
            assert "-eu" not in url, f"{attr} should not contain '-eu'"
            assert "-in" not in url, f"{attr} should not contain '-in'"
            assert "eu." not in url, f"{attr} should not contain 'eu.'"
            assert "in." not in url, f"{attr} should not contain 'in.'"
