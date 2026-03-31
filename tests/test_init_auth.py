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


def test_init_accepts_string_project_id():
    client = MixpanelUtils(
        service_account_username="test.a12345.mp-service-account",
        service_account_password="test123TEST123abc1234testing",
        project_id="1234567",
    )

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


_VALID = dict(
    service_account_username="test.a12345.mp-service-account",
    service_account_password="test123TEST123abc1234testing",
    project_id=1234567,
)


@pytest.mark.parametrize(
    "override,description",
    [
        ({"service_account_username": ""}, "empty username"),
        ({"service_account_username": 123}, "non-string username"),
        ({"service_account_password": ""}, "empty password"),
        ({"service_account_password": None}, "None password"),
        ({"project_id": 0}, "zero project_id"),
        ({"project_id": -1}, "negative project_id"),
        ({"project_id": 1.5}, "float project_id"),
        ({"project_id": "abc"}, "non-numeric string project_id"),
    ],
)
def test_init_raises_value_error_for_invalid_field_values(override, description):
    kwargs = {**_VALID, **override}
    with pytest.raises(ValueError) as exc_info:
        MixpanelUtils(**kwargs)

    message = str(exc_info.value)
    assert "API Secret authentication is deprecated" in message, description
    assert "service_account_username" in message, description
    assert "service_account_password" in message, description
    assert "project_id" in message, description


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
