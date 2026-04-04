"""Whitelist/blacklist and epoch-based filtering transforms."""



def whitelist_blacklist(job, params):
    """Filter records by event name, property keys, property values, and combos."""
    event_wl = params.get("event_whitelist", [])
    event_bl = params.get("event_blacklist", [])
    pk_wl = params.get("prop_key_whitelist", [])
    pk_bl = params.get("prop_key_blacklist", [])
    pv_wl = params.get("prop_val_whitelist", [])
    pv_bl = params.get("prop_val_blacklist", [])
    combo_wl = params.get("combo_white_list", {})
    combo_bl = params.get("combo_black_list", {})

    def transform(record):
        # Event whitelist
        if event_wl:
            if record.get("event") not in event_wl:
                job.whitelist_skipped += 1
                return {}

        # Event blacklist
        if event_bl:
            if record.get("event") in event_bl:
                job.blacklist_skipped += 1
                return {}

        props = record.get("properties", {})

        # Prop key whitelist
        if pk_wl:
            if not any(k in pk_wl for k in props):
                job.whitelist_skipped += 1
                return {}

        # Prop key blacklist
        if pk_bl:
            if any(k in pk_bl for k in props):
                job.blacklist_skipped += 1
                return {}

        # Prop val whitelist
        if pv_wl:
            if not any(v in pv_wl for v in props.values()):
                job.whitelist_skipped += 1
                return {}

        # Prop val blacklist
        if pv_bl:
            if any(v in pv_bl for v in props.values()):
                job.blacklist_skipped += 1
                return {}

        # Combo whitelist
        if combo_wl:
            found = False
            for key, vals in combo_wl.items():
                if not isinstance(vals, list):
                    vals = [vals]
                prop_val = props.get(key)
                if prop_val is not None and str(prop_val) in [str(v) for v in vals]:
                    found = True
                    break
            if not found:
                job.whitelist_skipped += 1
                return {}

        # Combo blacklist
        if combo_bl:
            for key, vals in combo_bl.items():
                if not isinstance(vals, list):
                    vals = [vals]
                prop_val = props.get(key)
                if prop_val is not None and str(prop_val) in [str(v) for v in vals]:
                    job.blacklist_skipped += 1
                    return {}

        return record

    return transform


def epoch_filter(job):
    """Filter events by time range (epoch_start..epoch_end)."""
    epoch_start = job.epoch_start
    epoch_end = job.epoch_end

    def transform(record):
        if job.record_type != "event":
            return record

        props = record.get("properties", {})
        event_time = props.get("time")
        if event_time is None:
            return record

        try:
            t = int(event_time)
        except (ValueError, TypeError):
            return record

        # Normalize to seconds
        if len(str(abs(t))) > 10:
            t = t // 1000

        if t < epoch_start:
            job.out_of_bounds += 1
            return None
        if t > epoch_end:
            job.out_of_bounds += 1
            return None

        return record

    return transform
