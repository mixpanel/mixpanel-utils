"""Tests for the async pipeline and dry-run mode."""

import asyncio
import json
import os
import tempfile
import pytest
from pathlib import Path

from mixpanel_utils.streaming import mp_import

TEST_DATA = Path(__file__).parent.parent.parent / "SCRATCH_TEST_DATA"


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def run(coro):
    return asyncio.run(coro)


class TestDryRun:
    def test_jsonl_dry_run(self):
        path = TEST_DATA / "events.ndjson"
        if not path.exists():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {"dry_run": True}))
        assert result["total"] > 0
        assert result["success"] == result["total"]
        assert len(result["dry_run"]) == result["total"]

    def test_csv_dry_run(self):
        path = TEST_DATA / "eventAsTable.csv"
        if not path.exists():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {"dry_run": True, "stream_format": "csv"}))
        assert result["total"] > 0
        assert result["success"] > 0

    def test_list_input(self):
        events = [
            {"event": "Test", "properties": {"time": 1700000000, "$insert_id": "a"}},
            {"event": "Test2", "properties": {"time": 1700000001, "$insert_id": "b"}},
        ]
        result = run(mp_import(None, events, {"dry_run": True}))
        assert result["total"] == 2
        assert result["success"] == 2

    def test_inline_json(self):
        data = '[{"event":"A","properties":{"time":1700000000}},{"event":"B","properties":{"time":1700000001}}]'
        result = run(mp_import(None, data, {"dry_run": True}))
        assert result["total"] == 2

    def test_fix_data_mode(self):
        events = [
            {"event": "Test", "page": "/home", "time": "2024-01-15"},
        ]
        result = run(mp_import(None, events, {"dry_run": True, "fix_data": True}))
        assert result["total"] == 1
        assert result["success"] == 1
        # fixData should have moved props into properties and fixed time
        evt = result["dry_run"][0]
        assert "properties" in evt

    def test_user_profile_dry_run(self):
        profiles = [
            {"$distinct_id": "u1", "$set": {"name": "Alice"}},
            {"$distinct_id": "u2", "$set": {"name": "Bob"}},
        ]
        result = run(mp_import(None, profiles, {"record_type": "user", "dry_run": True}))
        assert result["total"] == 2
        assert result["success"] == 2

    def test_dedupe(self):
        events = [
            {"event": "Dup", "properties": {"key": "val", "time": 1}},
            {"event": "Dup", "properties": {"key": "val", "time": 1}},
            {"event": "Unique", "properties": {"key": "other", "time": 2}},
        ]
        result = run(mp_import(None, events, {"dry_run": True, "dedupe": True}))
        assert result["total"] == 3
        assert result["duplicates"] == 1
        assert result["success"] == 2

    def test_transform_func(self):
        events = [
            {"event": "Raw", "properties": {"time": 1700000000}},
        ]

        def my_transform(record):
            record["event"] = "Transformed"
            return record

        result = run(mp_import(None, events, {"dry_run": True, "transform_func": my_transform}))
        assert result["success"] == 1
        assert result["dry_run"][0]["event"] == "Transformed"

    def test_max_records(self):
        events = [{"event": f"E{i}", "properties": {"time": i}} for i in range(100)]
        result = run(mp_import(None, events, {"dry_run": True, "max_records": 10}))
        assert result["total"] == 10


class TestVendorDryRun:
    def test_amplitude_vendor(self):
        path = TEST_DATA / "amplitude" / "2023-04-10_1#0.json"
        if not path.exists():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {
            "vendor": "amplitude",
            "vendor_opts": {"user_id": "user_id", "v2_compat": True},
            "dry_run": True,
            "fix_data": True,
        }))
        assert result["total"] > 0
        assert result["success"] == result["total"]
        assert result["dry_run"][0]["properties"]["$source"] == "amplitude-to-mixpanel"

    def test_june_csv_vendor(self):
        path = TEST_DATA / "june" / "events-small.csv"
        if not path.exists():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {
            "vendor": "june",
            "vendor_opts": {"v2compat": True},
            "dry_run": True,
            "stream_format": "csv",
        }))
        assert result["total"] > 0
        assert result["success"] == result["total"]

    def test_mparticle_vendor(self):
        path = TEST_DATA / "mparticle" / "sample_data.txt"
        if not path.exists():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {
            "vendor": "mparticle",
            "dry_run": True,
            "stream_format": "jsonl",
        }))
        assert result["total"] > 0
        assert result["success"] > 0


class TestDirectoryImport:
    def test_directory_import(self):
        path = TEST_DATA / "formats" / "json"
        if not path.exists() or not path.is_dir():
            pytest.skip("Test data not available")
        result = run(mp_import(None, str(path), {"dry_run": True}))
        assert result["total"] > 0


class TestWriteToFile:
    def test_write_to_file_creates_ndjson(self):
        events = [
            {"event": "A", "properties": {"time": 1700000000}},
            {"event": "B", "properties": {"time": 1700000001}},
        ]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            out_path = f.name

        try:
            result = run(mp_import(None, events, {
                "write_to_file": True,
                "output_file_path": out_path,
            }))
            assert result["success"] == 2
            with open(out_path) as f:
                lines = [json.loads(line) for line in f if line.strip()]
            assert len(lines) == 2
            assert lines[0]["event"] == "A"
            assert lines[1]["event"] == "B"
        finally:
            os.unlink(out_path)

    def test_write_to_file_no_creds_required(self):
        events = [{"event": "Test", "properties": {"time": 1}}]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            out_path = f.name

        try:
            result = run(mp_import(None, events, {
                "write_to_file": True,
                "output_file_path": out_path,
            }))
            assert result["success"] == 1
        finally:
            os.unlink(out_path)

    def test_write_to_file_with_transforms(self):
        events = [{"event": "Raw", "page": "/home", "time": "2024-01-15"}]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            out_path = f.name

        try:
            result = run(mp_import(None, events, {
                "write_to_file": True,
                "output_file_path": out_path,
                "fix_data": True,
            }))
            assert result["success"] == 1
            with open(out_path) as f:
                lines = [json.loads(line) for line in f if line.strip()]
            assert len(lines) == 1
            assert "properties" in lines[0]
        finally:
            os.unlink(out_path)


class TestHeavyObjects:
    def test_dimension_map_loading(self):
        map_data = [
            {"posthog_id": "ph1", "mp_id": "mp1"},
            {"posthog_id": "ph2", "mp_id": "mp2"},
        ]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump(map_data, f)
            map_path = f.name

        try:
            events = [{"event": "Test", "properties": {"time": 1}}]
            result = run(mp_import(None, events, {
                "dry_run": True,
                "dimension_maps": [
                    {"filePath": map_path, "keyOne": "posthog_id", "keyTwo": "mp_id", "label": "people"}
                ],
            }))
            assert result["success"] == 1
        finally:
            os.unlink(map_path)
