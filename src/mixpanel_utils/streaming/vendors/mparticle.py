"""mParticle to Mixpanel vendor transforms."""

import mmh3

BAD_USER_IDS = {
    "-1", "0", "00000000-0000-0000-0000-000000000000", "<nil>", "]",
    "anon", "anonymous", "false", "lmy47d", "n/a", "na", "nil", "none",
    "null", "true", "undefined", "unknown", "{}",
}


def _flatten_nested(obj: dict, prefix: str = "") -> dict:
    """Flatten nested dicts into dot-notation keys."""
    result = {}
    if not isinstance(obj, dict):
        return result
    for key, value in obj.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            result.update(_flatten_nested(value, full_key))
        else:
            result[full_key] = value
    return result


def _flatten_props(record: dict) -> dict:
    """Flatten a properties dict (top-level only, like the Node transforms.flattenProperties)."""
    if not isinstance(record, dict):
        return {}
    result = {}
    for key, value in record.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                result[f"{key}.{sub_key}"] = sub_value
        else:
            result[key] = value
    return result


def mparticle_events_to_mixpanel(options: dict):
    """Factory: returns transform for mParticle batches → list of Mixpanel events.

    Note: mParticle sends batches containing multiple events. This transform
    returns a LIST of Mixpanel events from a single mParticle batch.
    """
    user_id_keys = options.get("user_id", ["customer_id"])
    device_id_keys = options.get("device_id", ["mp_deviceid", "mpid", "session_uuid"])
    insert_id_key = options.get("insert_id", "event_id")
    include_user_attributes = options.get("user_attributes", False)
    include_context = options.get("context", False)
    include_identities = options.get("identities", False)
    include_application_info = options.get("application_info", True)
    include_device_info = options.get("device_info", True)
    include_source_info = options.get("source_info", True)

    def transform(mp_batch: dict) -> list[dict]:
        events = mp_batch.get("events", [])
        user_identities = mp_batch.get("user_identities", []) or []

        # Resolve user_id
        known_id = ""
        for id_type in user_id_keys:
            for identity in user_identities:
                if identity.get("identity_type") == id_type:
                    val = identity.get("identity")
                    if val and str(val) not in BAD_USER_IDS:
                        known_id = str(val)
                        break
            if known_id:
                break

        # Resolve device_id
        anon_id = ""
        for id_type in device_id_keys:
            # Check user_identities
            for identity in user_identities:
                if identity.get("identity_type") == id_type and identity.get("identity"):
                    anon_id = str(identity["identity"])
                    break
            if anon_id:
                break

            # Check top level
            if mp_batch.get(id_type):
                anon_id = str(mp_batch[id_type])
                break

            # Check event data
            for event in events:
                data = event.get("data", {}) or {}
                if data.get(id_type):
                    anon_id = str(data[id_type])
                    break
            if anon_id:
                break

        # Inherited props from batch level
        inherited_props = {
            "batch_id": mp_batch.get("batch_id"),
            "message_id": mp_batch.get("message_id"),
            "message_type": mp_batch.get("message_type"),
            "unique_id": mp_batch.get("unique_id"),
            "source_request_id": mp_batch.get("source_request_id"),
            "schema_version": mp_batch.get("schema_version"),
        }

        # Additional properties based on options
        if include_user_attributes and mp_batch.get("user_attributes"):
            inherited_props.update(mp_batch["user_attributes"])
        if include_context and mp_batch.get("context"):
            inherited_props.update(_flatten_props(mp_batch["context"]))
        if include_identities:
            inherited_props["identities"] = user_identities
        if include_application_info and mp_batch.get("application_info"):
            inherited_props.update(mp_batch["application_info"])
        if include_device_info and mp_batch.get("device_info"):
            inherited_props.update(mp_batch["device_info"])
        if include_source_info and mp_batch.get("source_info"):
            inherited_props.update(mp_batch["source_info"])

        mixpanel_events = []

        for mp_event in events:
            data = mp_event.get("data", {}) or {}
            timestamp = data.get("timestamp_unixtime_ms", 0)
            event_type = mp_event.get("event_type", "")

            insert_id = data.get(insert_id_key)

            mp = {
                "event": event_type,
                "properties": {
                    "$device_id": anon_id,
                    "time": int(timestamp) if timestamp else 0,
                    "$source": "mparticle-to-mixpanel",
                }
            }

            # Insert ID
            if insert_id:
                mp["properties"]["$insert_id"] = insert_id
            else:
                tuple_str = "-".join([anon_id, str(timestamp), event_type])
                mp["properties"]["$insert_id"] = str(mmh3.hash(tuple_str) & 0xFFFFFFFF)

            # Custom event name
            if event_type == "custom_event":
                mp["event"] = data.get("event_name", event_type)

            # User ID
            if known_id:
                mp["properties"]["$user_id"] = known_id

            # Custom attributes (flattened)
            custom_attrs = data.get("custom_attributes", {}) or {}
            custom_props = _flatten_props(custom_attrs)

            # Standard data props (flattened, minus custom_attributes)
            standard_data = {k: v for k, v in data.items() if k != "custom_attributes"}
            standard_props = _flatten_props(standard_data)

            # Merge all
            mp["properties"] = {
                **inherited_props,
                **standard_props,
                **custom_props,
                **mp["properties"],
            }

            mixpanel_events.append(mp)

        return mixpanel_events

    return transform


def mparticle_user_to_mixpanel(options: dict):
    """Factory: returns transform for mParticle users → Mixpanel profiles."""
    user_id_keys = options.get("user_id", ["customer_id"])

    def transform(mp_batch: dict) -> dict:
        user_identities = mp_batch.get("user_identities", []) or []

        known_id = ""
        for id_type in user_id_keys:
            for identity in user_identities:
                if identity.get("identity_type") == id_type:
                    val = identity.get("identity")
                    if val and str(val) not in BAD_USER_IDS:
                        known_id = str(val)
                        break
            if known_id:
                break

        if not known_id:
            return {}

        user_props = mp_batch.get("user_attributes", {}) or {}
        if not user_props:
            return {}

        inherited = {
            **(mp_batch.get("application_info", {}) or {}),
            **(mp_batch.get("device_info", {}) or {}),
            "identities": user_identities,
            "mpid": mp_batch.get("mpid"),
        }

        profile = {
            "$distinct_id": known_id,
            "$set": {**inherited, **user_props},
        }

        if mp_batch.get("ip"):
            profile["$ip"] = mp_batch["ip"]

        return profile

    return transform


def mparticle_group_to_mixpanel(options: dict):
    """Factory: returns transform for mParticle groups → Mixpanel groups."""

    def transform(mp_batch: dict) -> dict:
        group_props = mp_batch.get("group_properties", {}) or {}
        if not group_props:
            return {}
        if not mp_batch.get("user_id"):
            return {}

        return {
            "$group_key": None,
            "$group_id": None,
            "$set": group_props,
        }

    return transform
