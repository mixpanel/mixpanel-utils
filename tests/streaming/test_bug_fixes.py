"""Tests for bug fixes documented in improvements.md."""

import asyncio
import gzip
import json
import os
import tempfile
import pytest
from pathlib import Path

from mixpanel_utils.streaming import mp_import
from mixpanel_utils.streaming.transforms.builtin import ez_transforms
from mixpanel_utils.streaming.core.job import Job


def run(coro):
    return asyncio.run(coro)


class FakeJob:
    """Minimal job stub for testing transforms."""
    def __init__(self, **kwargs):
        self.record_type = kwargs.get("record_type", "event")
        self.token = kwargs.get("token", "test-token")
        self.tags = kwargs.get("tags", {})
        self.aliases = kwargs.get("aliases", {})
        self.directive = kwargs.get("directive", "$set")
        self.group_key = kwargs.get("group_key", "")


# ── BUG-3: Group $group_key preservation ────────────────────────────

class TestGroupTransformFix:
    def test_preserves_preshaped_group(self):
        """Pre-shaped records with $group_key/$group_id/$set should survive."""
        job = FakeJob(record_type="group", group_key="company")
        transform = ez_transforms(job)
        record = {
            "$group_key": "company",
            "$group_id": "Acme Corp",
            "$set": {"industry": "Tech", "employees": 500},
        }
        result = transform(record)
        assert result["$group_key"] == "company"
        assert result["$group_id"] == "Acme Corp"
        assert result["$set"]["industry"] == "Tech"
        assert result["$token"] == "test-token"

    def test_reshapes_flat_group(self):
        """Flat group records should still be correctly reshaped."""
        job = FakeJob(record_type="group", group_key="company")
        transform = ez_transforms(job)
        record = {
            "$group_id": "Acme Corp",
            "industry": "Tech",
            "employees": 500,
        }
        result = transform(record)
        assert result["$group_id"] == "Acme Corp"
        assert "$set" in result
        assert result["$set"]["industry"] == "Tech"
        assert result["$token"] == "test-token"

    def test_group_dry_run_pipeline(self):
        """Group records should pass through the full pipeline."""
        records = [
            {
                "$group_key": "company",
                "$group_id": "Acme",
                "$set": {"plan": "enterprise"},
            },
        ]
        result = run(mp_import(None, records, {
            "record_type": "group",
            "dry_run": True,
            "fix_data": True,
            "group_key": "company",
        }))
        assert result["success"] == 1
        rec = result["dry_run"][0]
        assert rec["$group_key"] == "company"
        assert rec["$group_id"] == "Acme"


# ── BUG-5: Failed records error messages ────────────────────────────

class TestStoreFailedRecords:
    def test_failed_records_show_real_errors(self):
        """job.store() with failed_records should show real error messages."""
        job = Job({"token": "test"}, {"dry_run": True})
        run(job.init())

        response = {
            "num_records_imported": 0,
            "num_failed": 2,
            "failed_records": [
                {"index": 0, "message": "event name is empty"},
                {"index": 1, "message": "invalid property type"},
            ],
            "error": None,
            "status": 0,
        }
        job.store(response, success=False)

        assert "event name is empty" in job.errors
        assert "invalid property type" in job.errors
        assert "unknown error" not in job.errors

    def test_error_field_preserved(self):
        """job.store() with error field should show the error."""
        job = Job({"token": "test"}, {"dry_run": True})
        run(job.init())

        response = {
            "error": "Unauthorized, invalid project secret",
            "status": 0,
        }
        job.store(response, success=False)

        assert "Unauthorized, invalid project secret" in job.errors


# ── BUG-6: Profile double-counting ──────────────────────────────────

class TestProfileCounting:
    def test_no_double_count_on_error_with_status(self):
        """User profiles should not double-count success+failure."""
        profiles = [
            {"$distinct_id": "u1", "$set": {"name": "Alice"}},
            {"$distinct_id": "u2", "$set": {"name": "Bob"}},
        ]
        result = run(mp_import(None, profiles, {
            "record_type": "user",
            "dry_run": True,
        }))
        # In dry_run, all should succeed with no double-counting
        assert result["success"] == 2
        assert result["failed"] == 0


# ── BUG-8: Parquet null stripping ───────────────────────────────────

class TestParquetNulls:
    def test_parquet_nulls_stripped(self):
        """Parquet reader should auto-strip None values."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed")

        # Create a parquet file with null values
        table = pa.table({
            "event": ["Click", "View", "Buy"],
            "user": ["u1", None, "u3"],
            "value": [1, 2, None],
        })
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            pq.write_table(table, f.name)
            parquet_path = f.name

        try:
            result = run(mp_import(None, parquet_path, {"dry_run": True}))
            assert result["total"] == 3
            for rec in result["dry_run"]:
                # No record should have None values in any field
                for k, v in rec.items():
                    if isinstance(v, dict):
                        for ik, iv in v.items():
                            assert iv is not None, f"Null found in {k}.{ik}"
                    else:
                        assert v is not None, f"Null found in {k}"
        finally:
            os.unlink(parquet_path)


# ── BUG-1: Cloud stream async generator ─────────────────────────────

class TestCloudStreamType:
    def test_gcs_stream_is_async_generator(self):
        """_gcs_stream should return an async generator, not a coroutine."""
        from mixpanel_utils.streaming.io.parsers import _gcs_stream
        import inspect
        assert inspect.isasyncgenfunction(_gcs_stream)

    def test_s3_stream_is_async_generator(self):
        """_s3_stream should return an async generator, not a coroutine."""
        from mixpanel_utils.streaming.io.parsers import _s3_stream
        import inspect
        assert inspect.isasyncgenfunction(_s3_stream)
