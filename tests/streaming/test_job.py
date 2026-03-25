"""Tests for the Job configuration class."""

import pytest
from mixpanel_utils.streaming.core.job import Job


class TestJobConfig:
    def test_default_config(self):
        job = Job({"token": "test"}, {})
        assert job.record_type == "event"
        assert job.region == "US"
        assert job.workers == 10
        assert job.records_per_batch == 2000
        assert job.compress is True
        assert job.strict is True
        assert job.verbose is False

    def test_write_to_file_allows_empty_creds(self):
        job = Job(None, {"write_to_file": True})
        assert job.write_to_file is True
        assert job.auth == ""

    def test_dry_run_allows_empty_creds(self):
        job = Job(None, {"dry_run": True})
        assert job.dry_run is True
        assert job.auth == ""

    def test_token_auth(self):
        job = Job({"token": "abc123"}, {})
        assert "Basic" in job.auth

    def test_service_account_auth(self):
        job = Job({"acct": "user", "pass": "pw", "project": "123"}, {})
        assert "Basic" in job.auth

    def test_secret_auth(self):
        job = Job({"secret": "mysecret"}, {})
        assert "Basic" in job.auth

    def test_no_creds_raises(self):
        with pytest.raises(ValueError, match="No credentials"):
            Job(None, {})

    def test_custom_options(self):
        job = Job({"token": "t"}, {
            "workers": 20,
            "records_per_batch": 500,
            "compress": False,
            "fix_data": True,
            "dedupe": True,
            "region": "EU",
        })
        assert job.workers == 20
        assert job.records_per_batch == 500
        assert job.compress is False
        assert job.fix_data is True
        assert job.dedupe is True
        assert job.region == "EU"

    def test_transform_ordering(self):
        job = Job({"token": "t"}, {
            "fix_data": True,
            "aliases": {"old": "new"},
            "tags": {"env": "test"},
            "remove_nulls": True,
        })
        # Should have at least aliases, ez_transforms, tags, remove_nulls
        assert len(job.active_transforms) >= 4

    def test_dimension_maps_config(self):
        job = Job({"token": "t"}, {
            "dimension_maps": [
                {"filePath": "test.json", "keyOne": "a", "keyTwo": "b", "label": "test"}
            ]
        })
        assert len(job.dimension_maps) == 1
        assert job.dimension_maps[0]["label"] == "test"

    def test_progress_callback_config(self):
        def my_callback(job):
            pass

        job = Job({"token": "t"}, {"progress_callback": my_callback})
        assert job.progress_callback is my_callback

    def test_camel_case_options(self):
        """Verify camelCase option names work (for JS compatibility)."""
        job = Job({"token": "t"}, {
            "recordType": "user",
            "fixData": True,
            "vendorOpts": {"key": "val"},
        })
        assert job.record_type == "user"
        assert job.fix_data is True
        assert job.vendor_opts == {"key": "val"}
