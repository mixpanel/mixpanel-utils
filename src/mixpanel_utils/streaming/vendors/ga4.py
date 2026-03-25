"""Google Analytics 4 to Mixpanel vendor transforms."""

def _flatten_ga_params(ga_params) -> dict:
    """Flatten GA4 event_params array [{key, value: {string_value/int_value/...}}] to flat dict."""
    result = {}
    if not ga_params or not isinstance(ga_params, list):
        return result
    for param in ga_params:
        key = param.get("key")
        value = param.get("value")
        if key and value and isinstance(value, dict):
            # Find the *_value key that actually has data
            for vk in value:
                if "value" in vk and value[vk] is not None:
                    result[key] = value[vk]
    return result


def _ga_to_mixpanel_defaults(ga_event: dict) -> dict:
    """Extract GA4 nested properties and map to Mixpanel defaults."""
    result = {}
    geo = ga_event.get("geo") or {}
    device = ga_event.get("device") or {}
    web_info = device.get("web_info") or {}
    app_info = ga_event.get("app_info") or {}
    traffic = ga_event.get("collected_traffic_source") or {}

    # Geographic
    if geo.get("city"):
        result["$city"] = geo["city"]
    if geo.get("country"):
        result["mp_country_code"] = geo["country"]
    if geo.get("region"):
        result["$region"] = geo["region"]

    # OS
    if device.get("operating_system"):
        result["$os"] = device["operating_system"]
    if device.get("operating_system_version"):
        result["$os_version"] = device["operating_system_version"]

    # Browser
    if web_info.get("browser"):
        result["$browser"] = web_info["browser"]
    if web_info.get("browser_version"):
        result["$browser_version"] = web_info["browser_version"]

    # Mobile
    if device.get("mobile_brand_name"):
        result["$manufacturer"] = device["mobile_brand_name"]
    if device.get("mobile_marketing_name"):
        result["$brand"] = device["mobile_marketing_name"]
    if device.get("mobile_model_name"):
        result["$model"] = device["mobile_model_name"]

    # App
    if app_info.get("version"):
        result["$app_version_string"] = app_info["version"]

    # Device category
    if device.get("category"):
        result["$device"] = device["category"]

    # Current URL from event_params
    event_params = ga_event.get("event_params")
    if isinstance(event_params, list):
        for param in event_params:
            if param.get("key") == "page_location":
                val = (param.get("value") or {}).get("string_value")
                if val:
                    result["$current_url"] = val
                    break
    if "$current_url" not in result and web_info.get("hostname"):
        result["$current_url"] = web_info["hostname"]

    # Platform → mp_lib
    platform = (ga_event.get("platform") or "").lower()
    platform_map = {"web": "web", "android": "android", "ios": "iphone", "unity": "unity"}
    if platform in platform_map:
        result["mp_lib"] = platform_map[platform]

    result["$lib_version"] = "ga4-export"

    # UTM
    if traffic.get("manual_source"):
        result["utm_source"] = traffic["manual_source"]
    if traffic.get("manual_medium"):
        result["utm_medium"] = traffic["manual_medium"]
    if traffic.get("manual_campaign_name"):
        result["utm_campaign"] = traffic["manual_campaign_name"]
    if traffic.get("manual_term"):
        result["utm_term"] = traffic["manual_term"]
    if traffic.get("manual_content"):
        result["utm_content"] = traffic["manual_content"]

    return result


def ga_events_to_mp(options: dict):
    """Factory: returns transform for GA4 events → Mixpanel events."""
    user_id_key = options.get("user_id", "user_id")
    device_id_key = options.get("device_id", "user_pseudo_id")
    insert_id_col = options.get("insert_id_col", "")
    set_insert_id = options.get("set_insert_id", True)
    insert_id_tup = options.get("insert_id_tup", ["event_name", "user_pseudo_id", "event_bundle_sequence_id"])
    time_conversion = options.get("time_conversion", "seconds")

    if not isinstance(insert_id_tup, list):
        raise ValueError("insert_id_tup must be a list")

    def transform(ga_event: dict) -> dict:
        import mmh3

        mp_event = {
            "event": ga_event.get("event_name", ""),
            "properties": {
                "$device_id": ga_event.get(device_id_key, ""),
            }
        }

        # Time conversion (GA4 uses microseconds)
        event_ts = ga_event.get("event_timestamp", 0)
        try:
            event_ts = int(event_ts)
        except (ValueError, TypeError):
            event_ts = 0

        if time_conversion in ("seconds", "s"):
            mp_event["properties"]["time"] = event_ts // 1_000_000
        elif time_conversion in ("milliseconds", "ms"):
            mp_event["properties"]["time"] = event_ts // 1_000

        # Insert ID
        if set_insert_id:
            if insert_id_col and ga_event.get(insert_id_col):
                mp_event["properties"]["$insert_id"] = ga_event[insert_id_col]
            else:
                tuple_parts = [str(ga_event.get(k, "")) for k in insert_id_tup if ga_event.get(k)]
                event_id = "-".join(tuple_parts)
                if event_id:
                    mp_event["properties"]["$insert_id"] = str(mmh3.hash(event_id) & 0xFFFFFFFF)
                    mp_event["properties"]["event_id"] = event_id

        mp_event["properties"]["$source"] = "ga4-to-mixpanel"

        # User ID
        if ga_event.get(user_id_key):
            mp_event["properties"]["$user_id"] = ga_event[user_id_key]

        # Flatten event_params and get defaults
        ga_custom = _flatten_ga_params(ga_event.get("event_params", []))
        ga_defaults = _ga_to_mixpanel_defaults(ga_event)

        # Build remaining GA props (exclude already-processed nested fields)
        remaining = {k: v for k, v in ga_event.items() if k not in ("event_params", "user_properties")}

        # Merge: remaining + defaults + custom params + core props (highest priority)
        mp_event["properties"] = {**remaining, **ga_defaults, **ga_custom, **mp_event["properties"]}
        return mp_event

    return transform


def ga_user_to_mp(options: dict):
    """Factory: returns transform for GA4 users → Mixpanel profiles."""
    user_id_key = options.get("user_id", "user_id")

    def transform(ga_event: dict) -> dict:
        user_props = _flatten_ga_params(ga_event.get("user_properties", []))
        if not user_props:
            return {}

        distinct_id = None
        if isinstance(ga_event.get("user_properties"), dict) and ga_event["user_properties"].get(user_id_key):
            distinct_id = ga_event["user_properties"][user_id_key]
        if ga_event.get(user_id_key):
            distinct_id = ga_event[user_id_key]

        if not distinct_id:
            return {}

        defaults = _ga_to_mixpanel_defaults(ga_event)

        return {
            "$distinct_id": distinct_id,
            "$ip": 0,
            "$set": {**defaults, **user_props},
        }

    return transform


def ga_groups_to_mp(options: dict):
    """Factory: returns transform for GA4 groups → Mixpanel groups."""

    def transform(ga_event: dict) -> dict:
        group_props = ga_event.get("group_properties", {}) or {}
        if not group_props:
            return {}
        if not ga_event.get("user_id"):
            return {}

        return {
            "$group_key": None,
            "$group_id": None,
            "$set": group_props,
        }

    return transform
