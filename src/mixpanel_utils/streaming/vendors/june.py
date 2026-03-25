"""June.so to Mixpanel vendor transforms."""

import json
from datetime import timezone

# June → Mixpanel field mappings
JUNE_MIX_PAIRS = [
    ("user_id", "$user_id"),
    ("anonymous_id", "anonymous_id"),
    ("timestamp", "time"),
    ("firstName", "$first_name"),
    ("lastName", "$last_name"),
    ("email", "$email"),
    ("phoneNumber", "$phone"),
    ("creationDate", "$created"),
    ("ip", "$ip"),
    ("name", "$name"),
]


def _safe_json_parse(val):
    """Try to parse a string as JSON, return original if it fails."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            pass
    return val


def june_events_to_mp(options: dict = None):
    """Factory: returns transform for June events → Mixpanel events."""
    options = options or {}
    v2_compat = options.get("v2compat", True)

    def transform(june_event: dict) -> dict:
        import mmh3
        from dateutil import parser as dateparser

        anonymous_id = june_event.get("anonymous_id", "")
        context = _safe_json_parse(june_event.get("context", "{}"))
        name = june_event.get("name", "")
        properties = _safe_json_parse(june_event.get("properties", "{}"))
        timestamp = june_event.get("timestamp", "")
        event_type = june_event.get("type", "")
        user_id = june_event.get("user_id", "")

        if not isinstance(properties, dict):
            properties = {}
        if not isinstance(context, dict):
            context = {}

        # Parse time
        try:
            ts = dateparser.parse(str(timestamp))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            time_ms = int(ts.timestamp() * 1000)
        except Exception:
            time_ms = 0

        mp_props = {
            "time": time_ms,
            "$source": "june-to-mixpanel",
            "type": event_type,
            **properties,
            **context,
        }

        # Insert ID
        delivery_id = properties.get("delivery_id") if isinstance(properties, dict) else None
        if delivery_id:
            mp_props["$insert_id"] = str(mmh3.hash(str(delivery_id)) & 0xFFFFFFFF)
        else:
            tuple_str = "-".join([str(anonymous_id), str(user_id), str(name), str(timestamp)])
            mp_props["$insert_id"] = str(mmh3.hash(tuple_str) & 0xFFFFFFFF)

        # Identity
        if anonymous_id:
            mp_props["$device_id"] = anonymous_id
        if user_id:
            mp_props["$user_id"] = user_id

        if v2_compat:
            if user_id:
                mp_props["distinct_id"] = user_id
            elif anonymous_id:
                mp_props["distinct_id"] = anonymous_id

        # Extract context properties
        if isinstance(context, dict):
            page = context.get("page")
            if isinstance(page, dict):
                if page.get("url"):
                    mp_props["$current_url"] = page["url"]
                if page.get("path"):
                    mp_props["$pathname"] = page["path"]
                if page.get("referrer"):
                    mp_props["$referrer"] = page["referrer"]
                if page.get("search"):
                    mp_props["$search"] = page["search"]
                if page.get("title"):
                    mp_props["$title"] = page["title"]

            if context.get("userAgent"):
                mp_props["$browser"] = context["userAgent"]
            if context.get("ip"):
                mp_props["ip"] = context["ip"]
            if context.get("locale"):
                mp_props["$locale"] = context["locale"]

            library = context.get("library")
            if isinstance(library, dict):
                if library.get("name"):
                    mp_props["$lib"] = library["name"]
                if library.get("version"):
                    mp_props["$lib_version"] = library["version"]

            integration = context.get("integration")
            if isinstance(integration, dict):
                if integration.get("name"):
                    mp_props["integration_name"] = integration["name"]
                if integration.get("version"):
                    mp_props["integration_version"] = integration["version"]

        return {
            "event": name or "unnamed june event",
            "properties": mp_props,
        }

    return transform


def june_user_to_mp(options: dict = None):
    """Factory: returns transform for June users → Mixpanel profiles."""
    options = options or {}

    june_mix_map = dict(JUNE_MIX_PAIRS)

    def transform(june_user: dict) -> dict:
        user_id = june_user.get("user_id", "")
        if not user_id:
            return {}

        traits = _safe_json_parse(june_user.get("traits", {}))
        context = _safe_json_parse(june_user.get("context", {}))

        if not isinstance(traits, dict):
            traits = {}
        if not isinstance(context, dict):
            context = {}

        props = {**context, **traits}

        # Remap known keys
        for key in list(props.keys()):
            mp_key = june_mix_map.get(key)
            if mp_key:
                props[mp_key] = props.pop(key)

        return {
            "$distinct_id": user_id,
            "$set": {
                "$source": "june-to-mixpanel",
                **props,
            }
        }

    return transform


def june_group_to_mp(options: dict = None):
    """Factory: returns transform for June groups → Mixpanel groups."""
    options = options or {}
    group_key = options.get("group_key", "group_id")

    june_mix_map = dict(JUNE_MIX_PAIRS)

    def transform(june_group: dict) -> dict:
        group_id = june_group.get("group_id", "")
        if not group_id:
            return {}

        traits = _safe_json_parse(june_group.get("traits", {}))
        context = _safe_json_parse(june_group.get("context", {}))

        if not isinstance(traits, dict):
            traits = {}
        if not isinstance(context, dict):
            context = {}

        props = {**traits, **context}

        for key in list(props.keys()):
            mp_key = june_mix_map.get(key)
            if mp_key:
                props[mp_key] = props.pop(key)

        return {
            "$group_id": group_id,
            "$group_key": group_key,
            "$set": {
                "$source": "june-to-mixpanel",
                **props,
            }
        }

    return transform
