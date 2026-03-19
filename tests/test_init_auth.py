import pytest

from mixpanel_utils import MixpanelUtils


def test_init_succeeds_with_required_service_account_fields():
    client = MixpanelUtils(
        service_account_username="test.a12345.mp-service-account",
        service_account_password="test123TEST123abc1234testing",
        project_id=1234567,
    )

    assert client.service_account_username == "test.a12345.mp-service-account"
    assert client.service_account_password == "test123TEST123abc1234testing"
    assert client.project_id == 1234567


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "service_account_password": "test123TEST123abc1234testing",
            "project_id": 1234567,
        },
        {
            "service_account_username": "test.a12345.mp-service-account",
            "project_id": 1234567,
        },
        {
            "service_account_username": "test.a12345.mp-service-account",
            "service_account_password": "test123TEST123abc1234testing",
        },
    ],
)
def test_init_raises_value_error_when_required_field_missing(kwargs):
    with pytest.raises(ValueError) as exc_info:
        MixpanelUtils(**kwargs)

    message = str(exc_info.value)
    assert "API Secret authentication is deprecated" in message
    assert "service_account_username" in message
    assert "service_account_password" in message
    assert "project_id" in message


def test_init_rejects_legacy_api_secret_kwarg():
    with pytest.raises(TypeError) as exc_info:
        MixpanelUtils(
            service_account_username="test.a12345.mp-service-account",
            service_account_password="test123TEST123abc1234testing",
            project_id=1234567,
            api_secret="legacy-secret",
        )

    assert "api_secret" in str(exc_info.value)


def test_init_rejects_legacy_positional_api_secret_only():
    with pytest.raises(ValueError) as exc_info:
        MixpanelUtils("legacy-api-secret-only")

    message = str(exc_info.value)
    assert "API Secret authentication is deprecated" in message
    assert "service_account_username" in message
    assert "service_account_password" in message
    assert "project_id" in message
