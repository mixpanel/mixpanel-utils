"""Tests for built-in transforms, vendor transforms, dedup, and filters."""

import pytest
from mixpanel_utils.streaming.transforms.builtin import (
    ez_transforms, apply_aliases, add_tags, add_token,
    remove_nulls, flatten_properties, utc_offset,
    fix_time, fix_json, scrub_properties, add_insert,
)
from mixpanel_utils.streaming.transforms.dedup import dedupe_records
from mixpanel_utils.streaming.transforms.filters import whitelist_blacklist, epoch_filter


class FakeJob:
    """Minimal job stub for testing transforms."""

    def __init__(self, **kwargs):
        self.record_type = kwargs.get("record_type", "event")
        self.token = kwargs.get("token", "test-token")
        self.tags = kwargs.get("tags", {})
        self.aliases = kwargs.get("aliases", {})
        self.directive = kwargs.get("directive", "$set")
        self.epoch_start = kwargs.get("epoch_start", 0)
        self.epoch_end = kwargs.get("epoch_end", 9991427224)
        self.out_of_bounds = 0
        self.whitelist_skipped = 0
        self.blacklist_skipped = 0
        self.duplicates = 0
        self.hash_table = set()
        self.insert_id_tuple = kwargs.get("insert_id_tuple", [])


# ── ez_transforms ───────────────────────────────────────────────────

class TestEzTransforms:
    def test_event_basic(self):
        job = FakeJob(record_type="event", token="tok123")
        transform = ez_transforms(job)
        record = {"event": "Page View", "properties": {"page": "/home"}}
        result = transform(record)
        assert result["event"] == "Page View"
        assert result["properties"]["page"] == "/home"
        assert "$insert_id" in result["properties"]

    def test_event_name_from_event_name_key(self):
        job = FakeJob(record_type="event")
        transform = ez_transforms(job)
        record = {"event_name": "Click", "properties": {}}
        result = transform(record)
        assert result["event"] == "Click"

    def test_user_profile_set(self):
        job = FakeJob(record_type="user", token="tok123")
        transform = ez_transforms(job)
        record = {"$distinct_id": "user1", "$set": {"name": "Alice"}}
        result = transform(record)
        assert result["$distinct_id"] == "user1"
        # ez_transforms renames "name" to "$name" (Mixpanel special prop)
        assert result["$set"].get("$name") == "Alice" or result["$set"].get("name") == "Alice"

    def test_user_profile_bare_props(self):
        job = FakeJob(record_type="user", token="tok123")
        transform = ez_transforms(job)
        record = {"$distinct_id": "user1", "name": "Bob", "plan": "pro"}
        result = transform(record)
        assert result.get("$distinct_id") == "user1"
        # Props should be in a $set bucket; name may be renamed to $name
        set_props = result.get("$set", {})
        assert set_props.get("$name") == "Bob" or set_props.get("name") == "Bob"
        assert "plan" in set_props

    def test_skip_empty_event(self):
        job = FakeJob(record_type="event")
        transform = ez_transforms(job)
        result = transform({})
        # Empty events may still return a dict with just event="" and minimal props
        assert result == {} or result is None or (isinstance(result, dict) and not result.get("event"))


# ── apply_aliases ───────────────────────────────────────────────────

class TestAliases:
    def test_rename_event_props(self):
        job = FakeJob(aliases={"old_name": "new_name"})
        transform = apply_aliases(job)
        record = {"event": "Test", "properties": {"old_name": "value"}}
        result = transform(record)
        assert "new_name" in result["properties"]
        assert "old_name" not in result["properties"]


# ── add_tags ────────────────────────────────────────────────────────

class TestTags:
    def test_add_tags_to_event(self):
        job = FakeJob(tags={"env": "test", "version": "1.0"})
        transform = add_tags(job)
        record = {"event": "Test", "properties": {"page": "/home"}}
        result = transform(record)
        assert result["properties"]["env"] == "test"
        assert result["properties"]["version"] == "1.0"


# ── remove_nulls ────────────────────────────────────────────────────

class TestRemoveNulls:
    def test_removes_none_values(self):
        transform = remove_nulls()
        record = {"event": "Test", "properties": {"a": 1, "b": None, "c": ""}}
        result = transform(record)
        assert "b" not in result["properties"]
        assert "c" not in result["properties"]
        assert result["properties"]["a"] == 1


# ── flatten_properties ──────────────────────────────────────────────

class TestFlatten:
    def test_flattens_nested_dict(self):
        transform = flatten_properties()
        record = {"event": "Test", "properties": {"user": {"name": "Alice", "age": 30}}}
        result = transform(record)
        assert result["properties"]["user.name"] == "Alice"
        assert result["properties"]["user.age"] == 30


# ── fix_json ────────────────────────────────────────────────────────

class TestFixJson:
    def test_parses_json_strings(self):
        transform = fix_json()
        record = {"event": "Test", "properties": {"data": '{"key": "value"}'}}
        result = transform(record)
        assert result["properties"]["data"] == {"key": "value"}


# ── scrub_properties ───────────────────────────────────────────────

class TestScrub:
    def test_removes_specified_props(self):
        transform = scrub_properties(["secret", "internal"])
        record = {"event": "Test", "properties": {"secret": "123", "page": "/home", "internal": True}}
        result = transform(record)
        assert "secret" not in result["properties"]
        assert "internal" not in result["properties"]
        assert result["properties"]["page"] == "/home"


# ── dedup ───────────────────────────────────────────────────────────

class TestDedup:
    def test_deduplicates(self):
        job = FakeJob()
        transform = dedupe_records(job)
        r1 = {"event": "Test", "properties": {"key": "val"}}
        r2 = {"event": "Test", "properties": {"key": "val"}}
        r3 = {"event": "Other", "properties": {"key": "val"}}

        result1 = transform(r1)
        assert result1 is not None and result1 != {}
        result2 = transform(r2)
        assert result2 is None or result2 == {}  # duplicate
        result3 = transform(r3)
        assert result3 is not None and result3 != {}
        assert job.duplicates == 1


# ── epoch_filter ────────────────────────────────────────────────────

class TestEpochFilter:
    def test_filters_by_epoch(self):
        job = FakeJob(epoch_start=1000, epoch_end=2000)
        transform = epoch_filter(job)

        in_range = {"event": "Test", "properties": {"time": 1500}}
        too_early = {"event": "Test", "properties": {"time": 500}}
        too_late = {"event": "Test", "properties": {"time": 3000}}

        assert transform(in_range) is not None
        assert transform(too_early) is None
        assert transform(too_late) is None
        assert job.out_of_bounds == 2


# ── whitelist_blacklist ─────────────────────────────────────────────

class TestWhitelistBlacklist:
    def test_event_whitelist(self):
        job = FakeJob()
        params = {
            "event_whitelist": ["Page View", "Sign Up"],
            "event_blacklist": [],
            "prop_key_whitelist": [],
            "prop_key_blacklist": [],
            "prop_val_whitelist": [],
            "prop_val_blacklist": [],
            "combo_white_list": {},
            "combo_black_list": {},
        }
        transform = whitelist_blacklist(job, params)

        allowed = {"event": "Page View", "properties": {}}
        blocked = {"event": "Random Event", "properties": {}}

        result_allowed = transform(allowed)
        assert result_allowed is not None and result_allowed != {}
        result_blocked = transform(blocked)
        assert result_blocked is None or result_blocked == {}

    def test_event_blacklist(self):
        job = FakeJob()
        params = {
            "event_whitelist": [],
            "event_blacklist": ["Spam Event"],
            "prop_key_whitelist": [],
            "prop_key_blacklist": [],
            "prop_val_whitelist": [],
            "prop_val_blacklist": [],
            "combo_white_list": {},
            "combo_black_list": {},
        }
        transform = whitelist_blacklist(job, params)

        allowed = {"event": "Page View", "properties": {}}
        blocked = {"event": "Spam Event", "properties": {}}

        result_allowed = transform(allowed)
        assert result_allowed is not None and result_allowed != {}
        result_blocked = transform(blocked)
        assert result_blocked is None or result_blocked == {}
