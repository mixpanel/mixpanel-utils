#!/usr/bin/env python3
"""
Export all events from the Mixpanel project specified by FROM_* in .env
for a given date range. Uses one API request per month to avoid connection timeouts.
Output: one JSON file per month in the events/ folder.
"""
import calendar
import os
import sys
import time
from datetime import datetime, timedelta

# Load .env from project root (parent of scripts/)
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_path = os.path.join(_root, ".env")
if os.path.isfile(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"'))
else:
    print("No .env found at", _env_path, file=sys.stderr)
    sys.exit(1)

from mixpanel_utils import MixpanelUtils


# Date range: August 10 2025 – October 31 2025
FROM_DATE = "2026-02-01"
TO_DATE = "2026-02-28"
EVENTS_DIR = "events"
EXCLUDE_EVENTS = ["session_completed"]


def main():
    api_secret = os.environ.get("FROM_PROJECT_API_SECRET")
    if not api_secret:
        print("FROM_PROJECT_API_SECRET is not set in .env", file=sys.stderr)
        sys.exit(1)

    from_id = os.environ.get("FROM_PROJECT_ID", "unknown")
    events_dir = os.path.join(_root, EVENTS_DIR)
    os.makedirs(events_dir, exist_ok=True)

    start = datetime.strptime(FROM_DATE, "%Y-%m-%d").date()
    end = datetime.strptime(TO_DATE, "%Y-%m-%d").date()

    print(
        "Exporting events from project",
        from_id,
        "for",
        FROM_DATE,
        "to",
        TO_DATE,
        "(one file per month in",
        EVENTS_DIR + "/)",
    )
    client = MixpanelUtils(api_secret)
    t0 = time.perf_counter()
    current = start
    month_num = 0
    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = min(current.replace(day=last_day), end)
        from_str = current.strftime("%Y-%m-%d")
        to_str = month_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            events_dir,
            f"events_export_from_{from_id}_{from_str}_{to_str}.json",
        )
        month_num += 1
        print("  Month", month_num, from_str, "->", to_str)
        events = client.query_export({"from_date": from_str, "to_date": to_str})
        filtered = [e for e in events if e.get("event") not in EXCLUDE_EVENTS]
        MixpanelUtils.export_data(filtered, out_file)
        current = month_end + timedelta(days=1)
    elapsed = time.perf_counter() - t0
    print("Done. Output:", events_dir)
    print("Time elapsed:    {:.1f}s".format(elapsed))


if __name__ == "__main__":
    main()
