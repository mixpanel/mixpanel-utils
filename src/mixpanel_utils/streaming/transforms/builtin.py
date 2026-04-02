"""Built-in data transformation functions.

Each factory function returns a callable: (record: dict) -> dict | list | None
Returning {} or None skips the record. Returning a list explodes into multiple records.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ..constants import (
    SPECIAL_PROPS, OUTSIDE_PROPS, VALID_OPERATIONS,
    BAD_USER_IDS, MAX_STR_LEN, TIME_FIELD_ALIASES,
)
from ..utils import rename_keys


def _noop(record):
    return record


def _truncate(s: str) -> str:
    return s[:MAX_STR_LEN] if len(s) > MAX_STR_LEN else s


def _parse_time(val) -> int | None:
    """Try to parse a time value to unix epoch ms."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            pass
        try:
            from dateutil import parser as dateparser
            dt = dateparser.parse(val)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            pass
    return None


# ── ez_transforms ────────────────────────────────────────────────────

def ez_transforms(job):
    """Auto-fix records based on record type."""
    rt = job.record_type

    if rt.startswith("event") or rt == "export-import-event":
        return _transform_event(job)
    elif rt.startswith("user") or (rt == "export-import-profile" and not job.group_key):
        return _transform_user(job)
    elif rt.startswith("group") or (rt == "export-import-profile" and job.group_key):
        return _transform_group(job)
    return _noop


def _transform_event(job):
    def transform(record):
        # 0. Use event_name as event if present
        if not record.get("event") and isinstance(record.get("event_name"), str):
            record["event"] = record.pop("event_name")

        # 1. Ensure properties exists
        if "properties" not in record or not isinstance(record.get("properties"), dict):
            props = {k: v for k, v in record.items() if k not in ("properties", "event")}
            record["properties"] = props
            for k in props:
                if k != "properties" and k != "event":
                    del record[k]
            record = {"event": record.get("event", ""), "properties": record.get("properties", {})}

        # 1a. Use event_name from properties
        if not record.get("event") and isinstance(record["properties"].get("event_name"), str):
            record["event"] = record["properties"].pop("event_name")

        props = record["properties"]

        # 2a. Resolve time field aliases
        if not props.get("time"):
            for alias in TIME_FIELD_ALIASES:
                if props.get(alias) is not None:
                    props["time"] = props.pop(alias)
                    break

        # 2b. Normalize time to unix epoch
        if props.get("time") is not None:
            try:
                num = float(props["time"])
            except (ValueError, TypeError):
                parsed = _parse_time(props["time"])
                if parsed is not None:
                    props["time"] = parsed

        # 3. Add $insert_id if missing
        if not props.get("$insert_id"):
            try:
                import mmh3
                tuple_str = "-".join([
                    str(record.get("event", "")),
                    str(props.get("distinct_id", "")),
                    str(props.get("time", "")),
                ])
                props["$insert_id"] = str(mmh3.hash(tuple_str, signed=False))
            except Exception:
                props["$insert_id"] = str(props.get("distinct_id", ""))

        # 4. Rename well-known keys
        for orig in ("user_id", "device_id", "source"):
            if orig in props:
                props[f"${orig}"] = props.pop(orig)

        # 5. Promote special props
        for key in list(props.keys()):
            if key in SPECIAL_PROPS:
                if key == "country":
                    props["mp_country_code"] = props[key]
                else:
                    props[f"${key}"] = props[key]
                del props[key]

        # 6. Ensure IDs are strings
        for k in ("distinct_id", "$user_id", "$device_id", "$insert_id"):
            if props.get(k) is not None:
                props[k] = str(props[k])

        # 6a. Remove bad IDs
        for k in ("distinct_id", "$user_id", "$device_id"):
            if str(props.get(k, "")).lower() in BAD_USER_IDS or props.get(k) is None:
                props.pop(k, None)

        # 6b. Fallback distinct_id from $user_id or $device_id
        if not props.get("distinct_id"):
            if props.get("$user_id"):
                props["distinct_id"] = props["$user_id"]
            elif props.get("$device_id"):
                props["distinct_id"] = props["$device_id"]
            else:
                props["distinct_id"] = ""

        # 7. Truncate strings
        for k, v in list(props.items()):
            if isinstance(v, str):
                props[k] = _truncate(v)

        return record

    return transform


def _transform_user(job):
    def transform(user):
        directive = job.directive if job.directive in VALID_OPERATIONS else None

        # Check if record already has complete vendor structure
        has_operation = any(op in user for op in VALID_OPERATIONS)
        has_distinct_id = "$distinct_id" in user or "distinct_id" in user
        special_root_keys = {"$distinct_id", "distinct_id", "$token", "$ip",
                             "$ignore_time", "$ignore_alias"} | set(VALID_OPERATIONS)
        has_loose_props = any(k not in special_root_keys for k in user)
        has_complete_vendor_structure = has_operation and has_distinct_id and not has_loose_props

        if not has_complete_vendor_structure and (directive or not has_operation):
            # Find distinct_id key
            uuid_key = None
            if "$distinct_id" in user:
                uuid_key = "$distinct_id"
            elif "distinct_id" in user:
                uuid_key = "distinct_id"
            if not uuid_key:
                return {}

            uuid_value = str(user[uuid_key])
            base = dict(user)

            if directive:
                for op in VALID_OPERATIONS:
                    if op in base and isinstance(base[op], dict):
                        base.update(base[op])
                        del base[op]

            final_directive = directive or "$set"

            if final_directive == "$unset":
                props_to_unset = [
                    k for k in base
                    if k != uuid_key and k != "$token" and not k.startswith("$")
                ]
                user = {final_directive: props_to_unset}
            else:
                user = {final_directive: base}
                user[final_directive].pop(uuid_key, None)
                user[final_directive].pop("$token", None)

                # Handle Mixpanel-export shape
                if isinstance(user[final_directive].get("$properties"), dict):
                    user[final_directive] = dict(user[final_directive]["$properties"])

            user["$distinct_id"] = uuid_value

        # Ensure $token
        if not user.get("$token") and job.token:
            user["$token"] = job.token

        # Rename special props inside operation buckets
        for op in VALID_OPERATIONS:
            if isinstance(user.get(op), dict):
                for prop in list(user[op].keys()):
                    if prop in SPECIAL_PROPS:
                        if prop in ("country", "country_code"):
                            user[op]["$country_code"] = str(user[op][prop]).upper()
                        else:
                            user[op][f"${prop}"] = user[op][prop]
                        del user[op][prop]

        # Extract outside props from operation buckets
        for op in VALID_OPERATIONS:
            if isinstance(user.get(op), dict):
                for prop in OUTSIDE_PROPS:
                    if prop in user[op]:
                        user[f"${prop}"] = user[op].pop(prop)

        # Pull remaining outside props to root & truncate
        for key, val in list(user.items()):
            if key in OUTSIDE_PROPS:
                user[f"${key}"] = val
                del user[key]
            elif isinstance(val, str):
                user[key] = _truncate(val)

        return user

    return transform


def _transform_group(job):
    def transform(group):
        directive = job.directive if job.directive in VALID_OPERATIONS else None

        # Skip reshape if record already has valid group structure
        has_group_key = "$group_key" in group
        has_group_id = "$group_id" in group
        has_operation = any(op in group for op in VALID_OPERATIONS)
        if has_group_key and has_group_id and has_operation:
            if not group.get("$token") and job.token:
                group["$token"] = job.token
            return group

        if directive or not has_operation:
            # Fallback chain for uuid
            uuid_key = None
            for candidate_key in [job.group_key, "$group_id", "group_id", "$distinct_id", "distinct_id"]:
                if candidate_key and group.get(candidate_key):
                    uuid_key = candidate_key
                    break
            if not uuid_key:
                return {}

            uuid_value = str(group[uuid_key])
            base = dict(group)

            if directive:
                for op in VALID_OPERATIONS:
                    if op in base and isinstance(base[op], dict):
                        base.update(base[op])
                        del base[op]

            final_directive = directive or "$set"

            if final_directive == "$unset":
                props_to_unset = [
                    k for k in base
                    if k not in (uuid_key, "$group_id", "$token", "$group_key") and not k.startswith("$")
                ]
                group = {final_directive: props_to_unset}
            else:
                group = {final_directive: base}
                group[final_directive].pop(uuid_key, None)
                group[final_directive].pop("$group_id", None)
                group[final_directive].pop("$token", None)

            group["$group_id"] = uuid_value

        # Ensure $token and $group_key
        if not group.get("$token") and job.token:
            group["$token"] = job.token
        if not group.get("$group_key") and job.group_key:
            group["$group_key"] = job.group_key

        # Rename special props
        for op in VALID_OPERATIONS:
            if isinstance(group.get(op), dict):
                for prop in list(group[op].keys()):
                    if prop in SPECIAL_PROPS:
                        group[op][f"${prop}"] = group[op][prop]
                        del group[op][prop]

        # Extract outside props
        for op in VALID_OPERATIONS:
            if isinstance(group.get(op), dict):
                for prop in OUTSIDE_PROPS:
                    if prop in group[op]:
                        group[f"${prop}"] = group[op].pop(prop)

        # Pull remaining outside props to root & truncate
        for key, val in list(group.items()):
            if key in OUTSIDE_PROPS:
                group[f"${key}"] = val
                del group[key]
            elif isinstance(val, str):
                group[key] = _truncate(val)

        return group

    return transform


# ── Other Transforms ─────────────────────────────────────────────────

def apply_aliases(job):
    """Rename property keys based on aliases mapping."""
    aliases = job.aliases or {}
    rt = job.record_type

    def transform(record):
        if not aliases:
            return record
        if rt == "event":
            if record.get("properties"):
                record["properties"] = rename_keys(record["properties"], aliases)
            else:
                record = rename_keys(record, aliases)
        else:
            op = next((k for k in record if k in VALID_OPERATIONS), None)
            if op and isinstance(record[op], dict):
                record[op] = rename_keys(record[op], aliases)
            else:
                record = rename_keys(record, aliases)
        return record

    return transform


def add_tags(job):
    """Merge tags into every record."""
    tags = job.tags or {}
    rt = job.record_type

    def transform(record):
        if not tags:
            return record
        if rt == "event":
            if record.get("properties"):
                record["properties"] = {**record["properties"], **tags}
        elif rt in ("user", "group"):
            op = next((k for k in record if k in VALID_OPERATIONS), None)
            if op and isinstance(record[op], dict):
                record[op] = {**record[op], **tags}
        return record

    return transform


def add_token(job):
    """Add Mixpanel token to every record."""
    token = job.token
    rt = job.record_type

    def transform(record):
        if rt == "event":
            if record.get("properties"):
                record["properties"]["token"] = token
            else:
                record["properties"] = {"token": token}
        elif rt in ("user", "group"):
            record["$token"] = token
        return record

    return transform


def remove_nulls(values_to_remove=None):
    """Remove null/empty values from records."""
    if values_to_remove is None:
        values_to_remove = [None, ""]

    def transform(record):
        keys_to_check = ["properties"] + VALID_OPERATIONS
        for record_key in keys_to_check:
            bucket = record.get(record_key)
            if isinstance(bucket, dict):
                for k in list(bucket.keys()):
                    if bucket[k] in values_to_remove:
                        del bucket[k]
                    elif isinstance(bucket[k], dict) and len(bucket[k]) == 0:
                        del bucket[k]
        return record

    return transform


def flatten_properties(sep="."):
    """Flatten nested objects in properties."""
    def _flatten(obj, roots=None):
        if roots is None:
            roots = []
        result = {}
        for key, val in obj.items():
            if isinstance(val, dict) and not isinstance(val, list):
                result.update(_flatten(val, roots + [key]))
            else:
                result[sep.join(roots + [key])] = val
        return result

    def transform(record):
        if record.get("properties") and isinstance(record["properties"], dict):
            record["properties"] = _flatten(record["properties"])
            return record
        if record.get("$set") and isinstance(record["$set"], dict):
            record["$set"] = _flatten(record["$set"])
            return record
        return record

    return transform


def utc_offset(hours=0):
    """Offset event timestamps by N hours."""
    offset_ms = hours * 3600 * 1000

    def transform(record):
        if record.get("properties", {}).get("time") is not None:
            try:
                t = int(record["properties"]["time"])
                # If seconds, convert to ms
                if len(str(abs(t))) <= 10:
                    t *= 1000
                record["properties"]["time"] = t + offset_ms
            except (ValueError, TypeError):
                pass
        return record

    return transform


def fix_time():
    """Normalize timestamps to unix epoch ms."""
    def transform(record):
        if not record.get("properties"):
            return record
        if record["properties"].get("time") is not None:
            try:
                float(record["properties"]["time"])
            except (ValueError, TypeError):
                parsed = _parse_time(record["properties"]["time"])
                if parsed is not None:
                    record["properties"]["time"] = parsed
        else:
            record["properties"]["time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        return record

    return transform


def fix_json():
    """Try to parse string values that look like JSON."""
    def _might_be_json(val):
        if not isinstance(val, str):
            return False
        s = val.strip()
        return (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))

    def transform(record):
        if record.get("properties"):
            for key in list(record["properties"].keys()):
                val = record["properties"][key]
                if _might_be_json(val):
                    try:
                        record["properties"][key] = json.loads(val)
                    except json.JSONDecodeError:
                        pass
        return record

    return transform


def scrub_properties(props_to_scrub):
    """Remove specified properties from records."""
    def transform(record):
        if record.get("properties"):
            for prop in props_to_scrub:
                record["properties"].pop(prop, None)
        for op in VALID_OPERATIONS:
            if isinstance(record.get(op), dict):
                for prop in props_to_scrub:
                    record[op].pop(prop, None)
        return record

    return transform


def drop_columns(columns):
    """Drop specified top-level columns from records."""
    def transform(record):
        for col in columns:
            record.pop(col, None)
        return record

    return transform


def set_distinct_id_from_v2_props():
    """Set distinct_id from $user_id or $device_id for v2 compatibility."""
    def transform(record):
        if record.get("properties"):
            if not record["properties"].get("distinct_id"):
                if record["properties"].get("$user_id"):
                    record["properties"]["distinct_id"] = record["properties"]["$user_id"]
                elif record["properties"].get("$device_id"):
                    record["properties"]["distinct_id"] = record["properties"]["$device_id"]
        return record

    return transform


def add_insert(insert_tuple):
    """Add $insert_id based on a tuple of keys or hash the whole record."""
    def transform(record):
        import mmh3

        if not record:
            return {}
        if not insert_tuple:
            return record
        if record.get("properties", {}).get("$insert_id"):
            return record

        actual_tuple = []
        for key in insert_tuple:
            if record.get(key):
                actual_tuple.append(str(record[key]))
            elif record.get("properties", {}).get(key):
                actual_tuple.append(str(record["properties"][key]))

        if len(actual_tuple) == len(insert_tuple):
            insert_id = str(mmh3.hash("-".join(actual_tuple), signed=False))
        else:
            insert_id = str(mmh3.hash(json.dumps(record, sort_keys=True), signed=False))

        if "properties" not in record:
            record["properties"] = {}
        record["properties"]["$insert_id"] = insert_id
        return record

    return transform
