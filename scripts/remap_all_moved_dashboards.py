#!/usr/bin/env python3
"""
Remap cohort IDs on all dashboards listed in data/from_ui_dashboard_mapping.json.

This reads the mapping of source dashboard ID -> target dashboard ID (created after
moving dashboards via the Mixpanel UI) and runs the cohort remapping on each target
dashboard.

Usage:
    python scripts/remap_all_moved_dashboards.py

Requires:
    - TO_SERVICE_ACCOUNT_USERNAME / TO_SERVICE_ACCOUNT_PASSWORD / TO_PROJECT_ID in .env
    - data/cohort_mapping.json (old cohort ID -> new cohort ID)
    - data/from_ui_dashboard_mapping.json (source dashboard ID -> target dashboard ID)
"""
import json
import os
import sys
import time
import requests

# Load .env from project root
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

# Import the processing logic from the single-dashboard script
from remap_moved_dashboard import process_dashboard, remap_filters, remap_params

COHORT_MAPPING = os.path.join(_root, "data", "cohort_mapping.json")
DASHBOARD_MAPPING = os.path.join(_root, "data", "from_ui_dashboard_mapping.json")


def main():
    sa_user = os.environ.get("TO_SERVICE_ACCOUNT_USERNAME")
    sa_password = os.environ.get("TO_SERVICE_ACCOUNT_PASSWORD")
    to_id = os.environ.get("TO_PROJECT_ID")

    if not sa_user or not sa_password:
        print("TO_SERVICE_ACCOUNT_USERNAME and TO_SERVICE_ACCOUNT_PASSWORD must be set in .env", file=sys.stderr)
        sys.exit(1)
    if not to_id:
        print("TO_PROJECT_ID is not set in .env", file=sys.stderr)
        sys.exit(1)

    for path, label in [(COHORT_MAPPING, "cohort_mapping.json"), (DASHBOARD_MAPPING, "from_ui_dashboard_mapping.json")]:
        if not os.path.isfile(path):
            print(f"{label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    with open(COHORT_MAPPING) as f:
        cohort_map = json.load(f)

    with open(DASHBOARD_MAPPING) as f:
        dashboard_map = json.load(f)

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    print(f"Remapping cohort IDs on {len(dashboard_map)} moved dashboard(s)\n")

    t0 = time.perf_counter()
    total_filters = 0
    total_reports = 0
    total_failed = 0

    for source_id, target_id in dashboard_map.items():
        print(f"[{source_id} -> {target_id}]", end="")
        fc, rf, ff = process_dashboard(target_id, auth, to_id, cohort_map, headers)
        if ff == -1:
            total_failed += 1
            continue
        total_filters += fc
        total_reports += rf
        total_failed += ff

    elapsed = time.perf_counter() - t0
    print(f"\nDone. Filters: {total_filters}, Reports: {total_reports}, Failed: {total_failed}, Time: {elapsed:.1f}s")
    if total_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
