"""Heap to Mixpanel vendor transforms."""

import hashlib
import json
from datetime import timezone
from pathlib import Path

# Heap → Mixpanel default property mappings
HEAP_MP_PAIRS = [
    ("joindate", "$created"),
    ("initial_utm_term", "$initial_utm_term"),
    ("initial_utm_source", "$initial_utm_source"),
    ("initial_utm_medium", "$initial_utm_medium"),
    ("initial_utm_content", "$initial_utm_content"),
    ("initial_utm_campaign", "$initial_utm_campaign"),
    ("initial_search_keyword", "$initial_search_keyword"),
    ("initial_region", "$region"),
    ("initial_referrer", "$initial_referrer"),
    ("initial_platform", "$os"),
    ("initial_browser", "$browser"),
    ("app_version", "$app_version_string"),
    ("device_brand", "$brand"),
    ("device_manufacturer", "$manufacturer"),
    ("device_model", "$model"),
    ("region", "$region"),
    ("initial_city", "$city"),
    ("initial_country", "$country_code"),
    ("email", "$email"),
    ("_email", "$email"),
    ("firstName", "$first_name"),
    ("lastName", "$last_name"),
    ("last_modified", "$last_seen"),
    ("Name", "$name"),
    ("city", "$city"),
    ("country", "$country_code"),
    ("ip", "$ip"),
]


def _build_device_id_map(file_path: str) -> dict:
    """Build a device_id → distinct_id mapping from a JSON file."""
    path = Path(file_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    return {item["id"]: item["distinct_id"] for item in data if "id" in item}


def _parse_heap_id(id_str: str) -> str:
    """Extract the second number from a heap tuple ID like '(2008543124,4810060720600030)'."""
    if not id_str:
        return ""
    try:
        return id_str.split(",")[1].replace(")", "").strip()
    except (IndexError, AttributeError):
        return ""


def _parse_time(time_str) -> int:
    """Parse a time value to milliseconds."""
    from dateutil import parser as dateparser

    if isinstance(time_str, (int, float)):
        val = int(time_str)
        if len(str(abs(val))) >= 13:
            return val
        return val * 1000
    try:
        ts = dateparser.parse(str(time_str))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return int(ts.timestamp() * 1000)
    except Exception:
        return 0


def heap_events_to_mp(options: dict):
    """Factory: returns transform for Heap events → Mixpanel events."""
    user_id_key = options.get("user_id", "")
    device_id_file = options.get("device_id_file", "")

    device_id_map = {}
    if device_id_file:
        device_id_map = _build_device_id_map(device_id_file)

    def transform(heap_event: dict) -> dict:
        event_id = heap_event.get("event_id")
        if event_id:
            insert_id = str(event_id)
        else:
            insert_id = hashlib.md5(json.dumps(heap_event, sort_keys=True).encode()).hexdigest()

        anon_id = _parse_heap_id(heap_event.get("id", ""))
        if heap_event.get("user_id"):
            device_id = str(heap_event["user_id"])
        else:
            device_id = str(anon_id)

        if not device_id:
            return {}

        event_name = heap_event.get("type") or heap_event.get("object") or "unknown action"
        time_ms = _parse_time(heap_event.get("time", ""))
        custom_props = dict(heap_event.get("properties", {}) or {})
        remaining = {k: v for k, v in heap_event.items() if k not in ("properties", "time")}

        mp_event = {
            "event": event_name,
            "properties": {
                "$device_id": device_id,
                "time": time_ms,
                "$insert_id": insert_id,
                "$source": "heap-to-mixpanel",
            }
        }

        merged = {**remaining, **custom_props, **mp_event["properties"]}

        for heap_key, mp_key in HEAP_MP_PAIRS:
            if heap_key in merged:
                merged[mp_key] = merged.pop(heap_key)

        if not user_id_key:
            identity = heap_event.get("identity")
            if identity:
                merged["$device_id"] = device_id
                merged["$user_id"] = str(identity)
                mp_event["event"] = "identity association"
            elif device_id_map:
                known_id = device_id_map.get(device_id)
                if known_id:
                    merged["$user_id"] = known_id

        if user_id_key and heap_event.get(user_id_key):
            val = heap_event[user_id_key]
            if isinstance(val, (str, int, float)):
                merged["$user_id"] = str(val)

        mp_event["properties"] = merged
        return mp_event

    return transform


def heap_user_to_mp(options: dict):
    """Factory: returns transform for Heap users → Mixpanel profiles."""
    user_id_key = options.get("user_id", "")

    def transform(heap_user: dict) -> dict:
        custom_id = None
        if user_id_key and heap_user.get(user_id_key):
            val = heap_user[user_id_key]
            if isinstance(val, (str, int, float)):
                custom_id = str(val)

        anon_id = _parse_heap_id(heap_user.get("id", ""))
        user_id = heap_user.get("identity")

        if not user_id and not custom_id:
            return {}

        from dateutil import parser as dateparser

        for field in ("last_modified", "joindate", "identity_time"):
            if heap_user.get(field):
                try:
                    ts = dateparser.parse(str(heap_user[field]))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    heap_user[field] = ts.isoformat()
                except Exception:
                    pass

        custom_props = dict(heap_user.get("properties", {}) or {})
        default_props = {k: v for k, v in heap_user.items() if k != "properties"}

        profile = {
            "$distinct_id": custom_id or user_id or anon_id,
            "$ip": heap_user.get("initial_ip"),
            "$set": {**default_props, **custom_props},
        }

        for heap_key, mp_key in HEAP_MP_PAIRS:
            if heap_key in profile["$set"]:
                profile["$set"][mp_key] = profile["$set"].pop(heap_key)

        return profile

    return transform


def heap_group_to_mp(options: dict):
    """Factory: returns transform for Heap groups → Mixpanel groups (stub)."""

    def transform(heap_group: dict) -> dict:
        return {}

    return transform
