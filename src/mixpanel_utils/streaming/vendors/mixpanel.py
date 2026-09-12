"""Mixpanel re-import vendor transform.

Transforms Mixpanel export format back into Mixpanel import format.
Used for migrating data between projects or re-importing exported data.
"""


def mixpanel_events_to_mixpanel(options: dict = None):
    """Factory: returns transform for Mixpanel export format → Mixpanel import format."""
    options = options or {}

    def transform(mp_event: dict) -> dict:
        final_properties = dict(mp_event.get("properties", {}))

        if mp_event.get("device_id"):
            final_properties["$device_id"] = mp_event["device_id"]
        if mp_event.get("distinct_id"):
            final_properties["distinct_id"] = mp_event["distinct_id"]
        if mp_event.get("insert_id"):
            final_properties["$insert_id"] = mp_event["insert_id"]
        if mp_event.get("time"):
            final_properties["time"] = mp_event["time"]
        if mp_event.get("user_id"):
            final_properties["$user_id"] = mp_event["user_id"]

        return {
            "event": mp_event.get("event_name") or mp_event.get("event") or "unnamed",
            "properties": final_properties,
        }

    return transform
