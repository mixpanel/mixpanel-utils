"""Tests for query_engage / _get_engage_page error handling."""

import io
import urllib.error
from unittest.mock import patch

import pytest

from mixpanel_utils import MixpanelUtils


def _client():
    return MixpanelUtils(
        service_account_username="test.a12345.mp-service-account",
        service_account_password="test123TEST123abc1234testing",
        project_id=1234567,
    )


def test_query_engage_raises_clear_error_when_rate_limited():
    rate_limited = urllib.error.HTTPError(
        "https://mixpanel.com/api/2.0/engage", 429, "Too Many Requests", {}, io.BytesIO(b"")
    )
    with patch("urllib.request.urlopen", side_effect=rate_limited):
        with pytest.raises(RuntimeError, match="/engage"):
            _client().query_engage()


def test_query_engage_raises_clear_error_when_response_has_no_results():
    with patch.object(MixpanelUtils, "request", return_value='{"error": "bad request"}'):
        with pytest.raises(RuntimeError, match="Invalid response from /engage"):
            _client().query_engage()
