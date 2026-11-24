from mixpanel_utils import MixpanelUtils


_BASE_KWARGS = {
    "service_account_username": "test.a12345.mp-service-account",
    "service_account_password": "test123TEST123abc1234testing",
    "project_id": 1234567,
}


def test_export_events_daily_output_preserves_periods_in_directory_and_filename(
    tmp_path, monkeypatch
):
    output_dir = tmp_path / "reports.v1"
    output_dir.mkdir()
    output_file = output_dir / "events.backup.json"
    client = MixpanelUtils(**_BASE_KWARGS)
    requested_params = []

    def fake_query_export(params, **kwargs):
        requested_params.append(params)
        return []

    monkeypatch.setattr(client, "query_export", fake_query_export)

    client.export_events(
        str(output_file),
        {"from_date": "2024-01-01", "to_date": "2024-01-02"},
        request_per_day=True,
    )

    expected_files = [
        output_dir / "events.backup_2024-01-01.json",
        output_dir / "events.backup_2024-01-02.json",
    ]
    assert all(path.is_file() for path in expected_files)
    assert {path.name for path in output_dir.iterdir()} == {
        path.name for path in expected_files
    }
    assert [params["from_date"] for params in requested_params] == [
        "2024-01-01",
        "2024-01-02",
    ]
    assert [params["to_date"] for params in requested_params] == [
        "2024-01-01",
        "2024-01-02",
    ]
