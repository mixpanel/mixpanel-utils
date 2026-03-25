"""Amplitude to Mixpanel vendor transforms."""

from __future__ import annotations

import mmh3
from dateutil import parser as dateparser
from datetime import timezone

# Amplitude → Mixpanel default property mappings
AMP_MIX_PAIRS = [
    ("app_version", "$app_version_string"),
    ("os_name", "$os"),
    ("os_name", "$browser"),
    ("os_version", "$os_version"),
    ("device_brand", "$brand"),
    ("device_manufacturer", "$manufacturer"),
    ("device_model", "$model"),
    ("region", "$region"),
    ("city", "$city"),
]

# Keys to remove from amp event after extraction
_AMP_REMOVE_KEYS = {
    "device_id", "event_time", "$insert_id", "user_properties",
    "group_properties", "global_user_properties", "event_properties",
    "groups", "data",
}


def amp_events_to_mp(options: dict):
    """Factory: returns transform for Amplitude events → Mixpanel events."""
    user_id_key = options.get("user_id", "user_id")
    v2_compat = options.get("v2_compat", True)
    include_experiment_events = options.get("includeExperimentEvents", False)
    include_experiment_props = options.get("includeExperimentProps", False)

    def transform(amp_event: dict) -> dict | None:
        event_name = amp_event.get("event_type", "")

        # Skip experiment events
        if not include_experiment_events:
            if event_name and "[experiment]" in event_name.lower():
                return None

        # Parse time
        event_time = amp_event.get("event_time", "")
        try:
            ts = dateparser.parse(event_time)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            time_ms = int(ts.timestamp() * 1000)
        except Exception:
            time_ms = 0

        mp_event = {
            "event": event_name,
            "properties": {
                "$device_id": amp_event.get("device_id", ""),
                "time": time_ms,
                "ip": amp_event.get("ip_address"),
                "$city": amp_event.get("city"),
                "$region": amp_event.get("region"),
                "mp_country_code": amp_event.get("country"),
                "$source": "amplitude-to-mixpanel",
            }
        }

        # insert_id resolution
        insert_id = amp_event.get("$insert_id")
        if insert_id:
            mp_event["properties"]["$insert_id"] = insert_id
        else:
            tuple_str = "-".join([
                str(amp_event.get("device_id", "")),
                str(event_time),
                str(event_name),
            ])
            mp_event["properties"]["$insert_id"] = str(mmh3.hash(tuple_str) & 0xFFFFFFFF)

        # Canonical ID resolution
        user_props = amp_event.get("user_properties", {}) or {}
        if user_props.get(user_id_key):
            mp_event["properties"]["$user_id"] = user_props[user_id_key]
        if amp_event.get(user_id_key):
            mp_event["properties"]["$user_id"] = amp_event[user_id_key]

        # v2 compat
        if v2_compat:
            mp_event["properties"]["distinct_id"] = (
                mp_event["properties"].get("$user_id")
                or mp_event["properties"].get("$device_id")
            )

        # Merge custom props, group props, user props (lower priority than core props)
        merged = {}
        for source in [
            amp_event.get("event_properties", {}),
            amp_event.get("groups", {}),
            user_props,
        ]:
            if source:
                merged.update(source)

        # Build remaining amp props (everything not already extracted)
        remove_keys = _AMP_REMOVE_KEYS | {user_id_key}
        remaining = {k: v for k, v in amp_event.items() if k not in remove_keys}

        # Apply default property mappings
        for amp_key, mp_key in AMP_MIX_PAIRS:
            if amp_key in remaining:
                merged[mp_key] = remaining.pop(amp_key)

        # Gather everything
        final_props = {**remaining, **merged, **mp_event["properties"]}

        # Remove experiment props
        if not include_experiment_props:
            final_props = {
                k: v for k, v in final_props.items()
                if not (k and k.lower().startswith("[experiment]"))
            }

        mp_event["properties"] = final_props
        return mp_event

    return transform


def amp_user_to_mp(options: dict):
    """Factory: returns transform for Amplitude users → Mixpanel profiles."""
    user_id_key = options.get("user_id", "user_id")
    include_experiment_props = options.get("includeExperimentProps", False)

    def transform(amp_event: dict) -> dict:
        user_props = amp_event.get("user_properties", {}) or {}
        if not user_props:
            return {}

        distinct_id = None
        if user_props.get(user_id_key):
            distinct_id = user_props[user_id_key]
        if amp_event.get(user_id_key):
            distinct_id = amp_event[user_id_key]

        if not distinct_id:
            return {}

        profile = {
            "$distinct_id": distinct_id,
            "$ip": amp_event.get("ip_address"),
            "$set": dict(user_props),
        }

        # Include defaults
        for amp_key, mp_key in AMP_MIX_PAIRS:
            if amp_event.get(amp_key):
                profile["$set"][mp_key] = amp_event[amp_key]

        # Remove experiment props
        if not include_experiment_props:
            profile["$set"] = {
                k: v for k, v in profile["$set"].items()
                if not (k and k.lower().startswith("[experiment]"))
            }

        return profile

    return transform


def amp_group_to_mp(options: dict):
    """Factory: returns transform for Amplitude groups → Mixpanel groups."""

    def transform(amp_event: dict) -> dict:
        group_props = amp_event.get("group_properties", {}) or {}
        if not group_props:
            return {}
        if not amp_event.get("user_id"):
            return {}

        return {
            "$group_key": None,
            "$group_id": None,
            "$set": group_props,
        }

    return transform
