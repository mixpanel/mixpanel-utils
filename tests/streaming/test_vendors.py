"""Tests for vendor-specific transforms."""

import json
import pytest
from pathlib import Path

from mixpanel_utils.streaming.vendors.amplitude import amp_events_to_mp, amp_user_to_mp, amp_group_to_mp
from mixpanel_utils.streaming.vendors.heap import heap_events_to_mp, heap_user_to_mp
from mixpanel_utils.streaming.vendors.ga4 import ga_events_to_mp, ga_user_to_mp, ga_groups_to_mp
from mixpanel_utils.streaming.vendors.posthog import posthog_events_to_mp, posthog_person_to_mp_profile
from mixpanel_utils.streaming.vendors.mparticle import mparticle_events_to_mixpanel, mparticle_user_to_mixpanel
from mixpanel_utils.streaming.vendors.june import june_events_to_mp, june_user_to_mp, june_group_to_mp
from mixpanel_utils.streaming.vendors.mixpanel import mixpanel_events_to_mixpanel

TEST_DATA = Path(__file__).parent.parent.parent / "SCRATCH_TEST_DATA"


class TestAmplitude:
    def test_event_transform(self):
        transform = amp_events_to_mp({"user_id": "user_id", "v2_compat": True})
        record = {
            "event_type": "Button Click",
            "device_id": "dev123",
            "event_time": "2024-01-15T10:30:00Z",
            "user_id": "user456",
            "ip_address": "1.2.3.4",
            "event_properties": {"button": "signup"},
            "user_properties": {"plan": "pro"},
        }
        result = transform(record)
        assert result["event"] == "Button Click"
        assert result["properties"]["$user_id"] == "user456"
        assert result["properties"]["$source"] == "amplitude-to-mixpanel"
        assert result["properties"]["button"] == "signup"
        assert result["properties"]["distinct_id"] == "user456"

    def test_skips_experiment_events(self):
        transform = amp_events_to_mp({"includeExperimentEvents": False})
        record = {
            "event_type": "[Experiment] test variant",
            "device_id": "dev1",
            "event_time": "2024-01-15T10:00:00Z",
        }
        assert transform(record) is None

    def test_user_transform(self):
        transform = amp_user_to_mp({"user_id": "user_id"})
        record = {
            "user_id": "user789",
            "user_properties": {"name": "Alice", "plan": "enterprise"},
            "ip_address": "5.6.7.8",
        }
        result = transform(record)
        assert result["$distinct_id"] == "user789"
        assert result["$set"]["name"] == "Alice"

    def test_user_skip_empty(self):
        transform = amp_user_to_mp({"user_id": "user_id"})
        result = transform({"user_properties": {}})
        assert result == {}

    def test_real_data(self):
        path = TEST_DATA / "amplitude" / "2023-04-10_1#0.json"
        if not path.exists():
            pytest.skip("Test data not available")
        transform = amp_events_to_mp({"user_id": "user_id", "v2_compat": True})
        with open(path) as f:
            record = json.loads(f.readline())
        result = transform(record)
        assert result is not None
        assert result["event"]
        assert "$insert_id" in result["properties"]


class TestHeap:
    def test_event_transform(self):
        transform = heap_events_to_mp({})
        record = {
            "type": "pageview",
            "id": "(123,456789)",
            "time": "2024-01-15T10:00:00Z",
            "properties": {"page": "/home"},
        }
        result = transform(record)
        assert result["event"] == "pageview"
        assert result["properties"]["$device_id"] == "456789"
        assert result["properties"]["$source"] == "heap-to-mixpanel"

    def test_user_transform(self):
        transform = heap_user_to_mp({})
        record = {
            "id": "(123,456789)",
            "identity": "user@example.com",
            "properties": {"name": "Bob"},
        }
        result = transform(record)
        assert result["$distinct_id"] == "user@example.com"

    def test_real_data(self):
        path = TEST_DATA / "heap" / "heap-events-ex.json"
        if not path.exists():
            pytest.skip("Test data not available")
        transform = heap_events_to_mp({})
        data = _load_json_or_jsonl(path)
        result = transform(data[0])
        assert result is not None
        assert result.get("event")


class TestGA4:
    def test_event_transform(self):
        transform = ga_events_to_mp({"time_conversion": "seconds"})
        record = {
            "event_name": "page_view",
            "user_pseudo_id": "pseudo123",
            "event_timestamp": "1695676578000000",
            "event_params": [
                {"key": "page_title", "value": {"string_value": "Home"}},
                {"key": "engagement_time", "value": {"int_value": 5000}},
            ],
        }
        result = transform(record)
        assert result["event"] == "page_view"
        assert result["properties"]["time"] == 1695676578
        assert result["properties"]["$source"] == "ga4-to-mixpanel"
        assert result["properties"]["page_title"] == "Home"

    def test_insert_id_generation(self):
        transform = ga_events_to_mp({"set_insert_id": True})
        record = {
            "event_name": "click",
            "user_pseudo_id": "user1",
            "event_bundle_sequence_id": "123",
            "event_timestamp": "1695676578000000",
            "event_params": [],
        }
        result = transform(record)
        assert "$insert_id" in result["properties"]

    def test_real_data(self):
        path = TEST_DATA / "ga4" / "ga4_sample.json"
        if not path.exists():
            pytest.skip("Test data not available")
        transform = ga_events_to_mp({"time_conversion": "seconds"})
        data = _load_json_or_jsonl(path)
        result = transform(data[0])
        assert result is not None
        assert result.get("event")


class TestPostHog:
    def test_event_transform(self):
        transform = posthog_events_to_mp({})
        record = {
            "event": "page_view",
            "distinct_id": "user1",
            "timestamp": "2024-01-15T10:00:00Z",
            "uuid": "uuid-123",
            "properties": {
                "$device_id": "device1",
                "$user_id": "user1",
                "page": "/home",
            },
        }
        result = transform(record)
        assert result["event"] == "page_view"
        assert result["properties"]["$source"] == "posthog-to-mixpanel"

    def test_filters_system_events(self):
        transform = posthog_events_to_mp({})
        record = {"event": "$feature", "distinct_id": "u1", "timestamp": "2024-01-01", "properties": {}}
        assert transform(record) is None

        record2 = {"event": "$set", "distinct_id": "u1", "timestamp": "2024-01-01", "properties": {}}
        assert transform(record2) is None

    def test_person_profile(self):
        transform = posthog_person_to_mp_profile({})
        record = {
            "distinct_id": "user1",
            "created_at": "2024-01-01T00:00:00Z",
            "properties": {
                "email": "test@test.com",
                "name": "Test User",
                "$os": "Mac OS X",
            },
        }
        result = transform(record)
        assert result["$distinct_id"] == "user1"
        assert result["$set"]["$email"] == "test@test.com"


class TestMParticle:
    def test_batch_event_transform(self):
        transform = mparticle_events_to_mixpanel({})
        batch = {
            "events": [
                {
                    "event_type": "custom_event",
                    "data": {
                        "event_name": "Purchase",
                        "timestamp_unixtime_ms": 1700000000000,
                        "custom_attributes": {"product": "Widget"},
                    },
                }
            ],
            "user_identities": [
                {"identity_type": "customer_id", "identity": "cust123"}
            ],
            "mpid": "mp-123",
        }
        result = transform(batch)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["event"] == "Purchase"
        assert result[0]["properties"]["$user_id"] == "cust123"

    def test_real_data(self):
        path = TEST_DATA / "mparticle" / "sample_data.txt"
        if not path.exists():
            pytest.skip("Test data not available")
        transform = mparticle_events_to_mixpanel({})
        data = _load_json_or_jsonl(path)
        result = transform(data[0])
        assert isinstance(result, list)


class TestJune:
    def test_event_transform(self):
        transform = june_events_to_mp({"v2compat": True})
        record = {
            "name": "Signup",
            "user_id": "u1",
            "anonymous_id": "a1",
            "timestamp": "2024-01-15T10:30:00Z",
            "properties": '{"plan": "pro"}',
            "context": '{"ip": "1.2.3.4"}',
            "type": "track",
        }
        result = transform(record)
        assert result["event"] == "Signup"
        assert result["properties"]["$user_id"] == "u1"
        assert result["properties"]["$device_id"] == "a1"
        assert result["properties"]["distinct_id"] == "u1"
        assert result["properties"]["plan"] == "pro"

    def test_user_transform(self):
        transform = june_user_to_mp({})
        record = {
            "user_id": "u1",
            "traits": '{"email": "test@test.com", "name": "Test"}',
            "context": "{}",
        }
        result = transform(record)
        assert result["$distinct_id"] == "u1"
        assert result["$set"]["$email"] == "test@test.com"

    def test_group_transform(self):
        transform = june_group_to_mp({"group_key": "company"})
        record = {
            "group_id": "acme",
            "traits": '{"name": "Acme Corp"}',
            "context": "{}",
        }
        result = transform(record)
        assert result["$group_id"] == "acme"
        assert result["$group_key"] == "company"


class TestMixpanelReimport:
    def test_export_format_to_import_format(self):
        transform = mixpanel_events_to_mixpanel({})
        record = {
            "event_name": "Page View",
            "properties": {"page": "/home", "$browser": "Chrome"},
            "distinct_id": "user1",
            "time": 1700000000,
            "insert_id": "abc123",
        }
        result = transform(record)
        assert result["event"] == "Page View"
        assert result["properties"]["distinct_id"] == "user1"
        assert result["properties"]["$insert_id"] == "abc123"
        assert result["properties"]["time"] == 1700000000
        assert result["properties"]["page"] == "/home"


def _load_json_or_jsonl(path):
    with open(path) as f:
        content = f.read().strip()
    try:
        data = json.loads(content)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        return [json.loads(line) for line in content.split("\n") if line.strip()]
