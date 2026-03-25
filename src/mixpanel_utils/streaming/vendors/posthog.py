"""PostHog to Mixpanel vendor transforms."""

import re
import json
from dateutil import parser as dateparser
from datetime import datetime, timezone


def _add_if_defined(target: dict, key: str, value):
    """Add key to target only if value is not None."""
    if value is not None:
        target[key] = value


# PostHog → Mixpanel profile property mappings
POSTHOG_MIX_PROFILE_PAIRS = [
    ("name", "$name"),
    ("first_name", "$first_name"),
    ("last_name", "$last_name"),
    ("email", "$email"),
    ("phone", "$phone"),
    ("avatar", "$avatar"),
    ("$geoip_city_name", "$city"),
    ("$geoip_subdivision_1_name", "$region"),
    ("$geoip_country_code", "$country_code"),
    (None, "$locale"),
    (None, "$geo_source"),
    ("$geoip_time_zone", "$timezone"),
    ("$os", "$os"),
    ("$browser", "$browser"),
    ("$browser_version", "$browser_version"),
    ("$initial_referrer", "$initial_referrer"),
    ("$initial_referring_domain", "$initial_referring_domain"),
    ("$initial_utm_source", "initial_utm_source"),
    ("$initial_utm_medium", "initial_utm_medium"),
    ("$initial_utm_campaign", "initial_utm_campaign"),
    ("$initial_utm_content", "initial_utm_content"),
    ("$initial_utm_term", "initial_utm_term"),
    (None, "$android_manufacturer"),
    (None, "$android_brand"),
    (None, "$android_model"),
    (None, "$ios_device_model"),
]


def _build_regex(patterns: list[str]) -> re.Pattern:
    """Build a compiled regex that matches any of the given prefixes."""
    escaped = [re.escape(p) for p in patterns]
    return re.compile(f"^({'|'.join(escaped)})")


def posthog_events_to_mp(options: dict, heavy_objects: dict | None = None):
    """Factory: returns transform for PostHog events → Mixpanel events."""
    v2_compat = options.get("v2_compat", False)
    ignore_events = options.get("ignore_events", [
        "$feature", "$set", "$webvitals", "$pageleave", "$groupidentify",
        "$autocapture", "$rageclick", "$screen", "$capture_pageview",
        "$merge_dangerously",
    ])
    identify_events = options.get("identify_events", ["$identify"])
    ignore_props = options.get("ignore_props", [
        "$feature/", "$feature_flag_", "$replay_", "$sdk_debug",
        "$session_recording", "$set", "$set_once",
    ])

    heavy_objects = heavy_objects or {}
    person_map = heavy_objects.get("people", {})

    # Pre-compile regex patterns
    delete_prefixes = ["token", *ignore_props]
    delete_prop_pattern = _build_regex(delete_prefixes)
    ignore_event_pattern = _build_regex(ignore_events)

    def transform(posthog_event: dict) -> dict | None:
        event_name = posthog_event.get("event", "")
        posthog_distinct_id = posthog_event.get("distinct_id")
        mp_ip = posthog_event.get("ip")
        mp_timestamp = posthog_event.get("timestamp")
        mp_insert_id = posthog_event.get("uuid")
        posthog_properties = posthog_event.get("properties", {})

        # Filter ignored events
        if ignore_event_pattern.search(event_name or ""):
            return None

        # Parse properties if string
        if isinstance(posthog_properties, str):
            try:
                posthog_properties = json.loads(posthog_properties)
            except json.JSONDecodeError:
                posthog_properties = {}
        posthog_properties = posthog_properties or {}

        # Extract known fields
        mp_city = posthog_properties.pop("$geoip_city_name", None)
        mp_country_code = posthog_properties.pop("$geoip_country_code", None)
        mp_latitude = posthog_properties.pop("$geoip_latitude", None)
        mp_longitude = posthog_properties.pop("$geoip_longitude", None)
        posthog_user_id = posthog_properties.pop("$user_id", None)
        posthog_device_id = posthog_properties.pop("$device_id", None)

        # Core Mixpanel props
        try:
            ts = dateparser.parse(str(mp_timestamp))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            time_ms = int(ts.timestamp() * 1000)
        except Exception:
            time_ms = 0

        mp_props = {
            "time": time_ms,
            "$source": "posthog-to-mixpanel",
        }
        _add_if_defined(mp_props, "ip", mp_ip)
        _add_if_defined(mp_props, "$city", mp_city)
        _add_if_defined(mp_props, "$region", mp_country_code)
        _add_if_defined(mp_props, "mp_country_code", posthog_event.get("country"))
        _add_if_defined(mp_props, "$insert_id", mp_insert_id)
        _add_if_defined(mp_props, "$latitude", mp_latitude)
        _add_if_defined(mp_props, "$longitude", mp_longitude)

        # Identity resolution
        user_id = posthog_user_id
        device_id = posthog_device_id
        distinct_id = posthog_distinct_id

        # Check person map
        if posthog_distinct_id and isinstance(person_map, dict):
            mapped = person_map.get(posthog_distinct_id)
            if mapped:
                user_id = mapped

        if user_id:
            mp_props["$user_id"] = user_id
        if device_id:
            mp_props["$device_id"] = device_id
        if distinct_id:
            mp_props["distinct_id"] = distinct_id

        # Remove ignored props
        remaining = {
            k: v for k, v in posthog_properties.items()
            if not delete_prop_pattern.search(k)
        }

        # Assemble
        mp_event = {"event": event_name, "properties": {**mp_props, **remaining}}
        mp_event["properties"].pop("token", None)

        # ID merge v2
        if v2_compat:
            if device_id:
                mp_event["properties"]["distinct_id"] = device_id
            if user_id:
                mp_event["properties"]["distinct_id"] = user_id

            if event_name and event_name.startswith("$identify"):
                mp_event["properties"]["$identified_id"] = user_id
                mp_event["properties"]["$anon_id"] = device_id

        # ID merge v3
        if not v2_compat:
            if any(event_name == evt for evt in identify_events):
                props = posthog_event.get("properties", {})
                if isinstance(props, str):
                    try:
                        props = json.loads(props)
                    except json.JSONDecodeError:
                        props = {}
                anon_distinct_id = props.get("$anon_distinct_id")
                ph_device_id = props.get("$device_id")
                session_id = props.get("$session_id")

                identify_props = {
                    "$user_id": distinct_id,
                    "$device_id": ph_device_id or anon_distinct_id,
                    "$insert_id": mp_insert_id,
                    **mp_props,
                }
                _add_if_defined(identify_props, "$session_id", session_id)

                return {
                    "event": "identity association",
                    "properties": identify_props,
                }

        return mp_event

    return transform


def posthog_person_to_mp_profile(options: dict, heavy_objects: dict | None = None):
    """Factory: returns transform for PostHog persons → Mixpanel profiles."""
    directive = options.get("directive", "$set")
    ignore_props = options.get("ignore_props", ["$creator_event_uuid"])

    delete_prefixes = ["token", *ignore_props]
    delete_prop_pattern = _build_regex(delete_prefixes)

    def transform(posthog_person: dict) -> dict | None:
        distinct_id = posthog_person.get("distinct_id")
        created_at = posthog_person.get("created_at")
        user_properties = posthog_person.get("properties", {}) or {}

        if not distinct_id:
            return {}

        mp_props = {}

        # Map known profile pairs
        mapped_keys = set()
        for ph_key, mp_key in POSTHOG_MIX_PROFILE_PAIRS:
            if ph_key is None:
                continue
            val = user_properties.get(ph_key)
            if val is not None:
                mp_props[mp_key] = val
                mapped_keys.add(ph_key)

        # Add remaining props
        for key, value in user_properties.items():
            if key in mapped_keys:
                continue
            if delete_prop_pattern.search(key):
                continue
            if value is None:
                continue
            mp_props[key] = value

        # Created timestamp
        created_str = None
        if created_at is not None:
            try:
                if isinstance(created_at, (int, float)):
                    created_str = datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat()
                else:
                    ts = dateparser.parse(str(created_at))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    created_str = ts.isoformat()
            except Exception:
                pass

        dir_props = {}
        if created_str:
            dir_props["$created"] = created_str
        dir_props.update(mp_props)

        profile = {
            "$distinct_id": distinct_id,
            directive: dir_props,
        }

        # Latitude/longitude at top level
        for init_key, final_key in [
            ("$initial_geoip_latitude", "$latitude"),
            ("$initial_geoip_longitude", "$longitude"),
            ("$geoip_latitude", "$latitude"),
            ("$geoip_longitude", "$longitude"),
        ]:
            val = user_properties.get(init_key)
            if val is not None:
                profile[final_key] = val

        # Don't send near-empty profiles
        if len(profile[directive]) <= 1:
            return None

        return profile

    return transform
