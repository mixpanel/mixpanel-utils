#!/usr/bin/env python3
"""
Import events from JSON file(s) (e.g. from export_events_from_project.py) into
the Mixpanel project specified by TO_* in .env.

By default imports all events/events_export_from_<FROM_PROJECT_ID>_*.json files
in date order. Pass a single file path to import just that file.
"""
import glob
import json
import os
import sys
import time

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

EVENTS_DIR = "events"
# UTC offset in hours for the project that exported the data (0 = data is already UTC)
TIMEZONE_OFFSET = 0


def main():
    api_secret = os.environ.get("TO_PROJECT_API_SECRET")
    token = os.environ.get("TO_PROJECT_TOKEN")
    if not api_secret:
        print("TO_PROJECT_API_SECRET is not set in .env", file=sys.stderr)
        sys.exit(1)
    if not token:
        print("TO_PROJECT_TOKEN is not set in .env", file=sys.stderr)
        sys.exit(1)

    from_id = os.environ.get("FROM_PROJECT_ID")
    to_id = os.environ.get("TO_PROJECT_ID", "unknown")
    events_dir = os.path.join(_root, EVENTS_DIR)
    default_pattern = os.path.join(events_dir, f"events_export_from_{from_id}_*.json")

    if len(sys.argv) > 1:
        paths = [sys.argv[1]]
        if not os.path.isfile(paths[0]):
            print("File not found:", paths[0], file=sys.stderr)
            sys.exit(1)
    else:
        paths = sorted(glob.glob(default_pattern))
        if not paths:
            print("No event files found matching", default_pattern, file=sys.stderr)
            sys.exit(1)

    print("Importing events to project", to_id, "from", len(paths), "file(s)")
    client = MixpanelUtils(api_secret, token=token)
    t0 = time.perf_counter()
    total_events = 0
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        events = data if isinstance(data, list) else []
        n = len(events)
        total_events += n
        print("  ", os.path.basename(path), "->", n, "events")
        if events:
            client.import_events(events, timezone_offset=TIMEZONE_OFFSET)
    elapsed = time.perf_counter() - t0
    print("Done.")
    print("Records imported:", total_events)
    print("Time elapsed:    {:.1f}s".format(elapsed))
    if total_events and elapsed:
        print("Rate:            {:.0f} events/s".format(total_events / elapsed))


if __name__ == "__main__":
    main()
