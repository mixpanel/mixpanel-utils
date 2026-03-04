#!/usr/bin/env python3
"""
Export all people from the Mixpanel project specified by FROM_* in .env.
Output is written to a JSON file in the project directory.
"""
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
    api_secret = os.environ.get("FROM_PROJECT_API_SECRET")
    if not api_secret:
        print("FROM_PROJECT_API_SECRET is not set in .env", file=sys.stderr)
        sys.exit(1)

    from_id = os.environ.get("FROM_PROJECT_ID", "unknown")
    out_file = os.path.join(_root, f"people_export_from_{from_id}.json")

    print("Exporting all people from project", from_id, "->", out_file)
    client = MixpanelUtils(api_secret)
    t0 = time.perf_counter()
    profiles = client.query_engage(params=None)
    count = len(profiles) if profiles else 0
    MixpanelUtils.export_data(profiles or [], out_file)
    elapsed = time.perf_counter() - t0
    print("Done. Output:", out_file)
    print("Records exported:", count)
    print("Time elapsed:    {:.1f}s".format(elapsed))
    if count and elapsed:
        print("Rate:           {:.0f} records/s".format(count / elapsed))


if __name__ == "__main__":
    main()
