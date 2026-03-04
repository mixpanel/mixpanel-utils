#!/usr/bin/env python3
"""
Import people from a JSON file (e.g. from export_people_from_project.py) into
the Mixpanel project specified by TO_* in .env.
"""
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
    default_file = os.path.join(_root, f"people_export_from_{from_id}.json")
    path = sys.argv[1] if len(sys.argv) > 1 else default_file

    if not os.path.isfile(path):
        print("File not found:", path, file=sys.stderr)
        sys.exit(1)

    print("Loading", path)
    with open(path) as f:
        data = json.load(f)
    count = len(data) if isinstance(data, list) else 0
    print("Importing", count, "people to project", to_id)
    client = MixpanelUtils(api_secret, token=token)
    t0 = time.perf_counter()
    client.import_people(data)
    elapsed = time.perf_counter() - t0
    print("Done.")
    print("Records imported:", count)
    print("Time elapsed:    {:.1f}s".format(elapsed))
    if count and elapsed:
        print("Rate:            {:.0f} records/s".format(count / elapsed))


if __name__ == "__main__":
    main()
