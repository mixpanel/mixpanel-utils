import builtins
import csv

import mixpanel_utils
from mixpanel_utils import MixpanelUtils


def test_write_items_to_csv_does_not_add_blank_rows_with_windows_line_endings(
    tmp_path, monkeypatch
):
    # Simulate Windows text mode, where newline=None translates "\n" to "\r\n".
    real_open = builtins.open

    def windows_open(file, mode="r", *args, newline=None, **kwargs):
        if "b" not in mode and newline is None:
            newline = "\r\n"
        return real_open(file, mode, *args, newline=newline, **kwargs)

    monkeypatch.setattr(mixpanel_utils, "open", windows_open, raising=False)

    output_file = tmp_path / "events.csv"
    items = [
        {"event": "Signup", "properties": {"distinct_id": "a", "time": 1}},
        {"event": "Login", "properties": {"distinct_id": "b", "time": 2}},
    ]

    MixpanelUtils._write_items_to_csv(items, str(output_file))

    assert b"\r\r\n" not in output_file.read_bytes()
    with open(output_file, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows == [
        ["event", "distinct_id", "time"],
        ["Signup", "a", "1"],
        ["Login", "b", "2"],
    ]
