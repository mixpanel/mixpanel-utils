#!/usr/bin/env python3
"""
Remap cohort IDs on a dashboard that was moved to the target project via the Mixpanel UI.

When you "move" a dashboard between projects in Mixpanel, the layout is preserved but
cohort references still point to the source project's IDs. This script updates the
dashboard filters and all report params with the new cohort IDs.

Usage:
    python scripts/remap_moved_dashboard.py <dashboard_id> [<dashboard_id> ...]

Requires:
    - TO_SERVICE_ACCOUNT_USERNAME / TO_SERVICE_ACCOUNT_PASSWORD / TO_PROJECT_ID in .env
    - data/cohort_mapping.json (old cohort ID -> new cohort ID)
    - The dashboard must be shared with the service account (Editor access)
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

COHORT_MAPPING = os.path.join(_root, "data", "cohort_mapping.json")


def remap_filters(filters, cohort_map):
    """Remap cohort IDs in dashboard-level filters. Returns count of changes."""
    changed = 0
    for filt in filters:
        for fv in filt.get("filterValue", []):
            cohort = fv.get("cohort", {})
            old_id = str(cohort.get("id", ""))
            if old_id in cohort_map:
                cohort["id"] = int(cohort_map[old_id])
                changed += 1
    return changed


def remap_params(params_obj, cohort_map):
    """Remap cohort IDs inside report params. Returns (new_params_str, changed)."""
    params_str = json.dumps(params_obj)
    changed = False
    for old_id, new_id in cohort_map.items():
        if f'"id": {old_id}' in params_str:
            params_str = params_str.replace(f'"id": {old_id}', f'"id": {new_id}')
            changed = True
    return params_str, changed


def process_dashboard(dashboard_id, auth, to_id, cohort_map, headers):
    """Remap cohort IDs on a single dashboard. Returns (filter_changes, reports_fixed, reports_failed)."""
    # Fetch dashboard
    resp = requests.get(
        f"https://mixpanel.com/api/app/projects/{to_id}/dashboards/{dashboard_id}",
        auth=auth
    )
    if resp.status_code != 200:
        print(f"  [Error] Could not fetch dashboard {dashboard_id}: {resp.status_code} {resp.text[:200]}")
        return 0, 0, -1

    dashboard = resp.json()["results"]
    title = dashboard.get("title", "Untitled")
    print(f"\n  Dashboard: '{title}' ({dashboard_id})")

    if not dashboard.get("can_update_basic"):
        print(f"  [Error] No edit permission. Share the dashboard with the service account as Editor.")
        return 0, 0, -1

    # 1. Remap dashboard filters
    filters = dashboard.get("filters", [])
    filter_changes = remap_filters(filters, cohort_map)

    if filter_changes:
        resp2 = requests.patch(
            f"https://mixpanel.com/api/app/projects/{to_id}/dashboards/{dashboard_id}",
            auth=auth, headers=headers,
            json={"filters": filters}
        )
        if resp2.status_code == 200:
            print(f"  Filters: {filter_changes} cohort ID(s) remapped")
        else:
            print(f"  [Warning] Filter PATCH failed: {resp2.status_code}")

    # 2. Remap report params
    reports = dashboard.get("contents", {}).get("report", {})
    fixed = 0
    failed = 0

    for bid, report in reports.items():
        params = report.get("params", {})
        new_params_str, changed = remap_params(params, cohort_map)

        if not changed:
            continue

        new_params = json.loads(new_params_str)
        for attempt in range(3):
            resp3 = requests.patch(
                f"https://mixpanel.com/api/app/projects/{to_id}/bookmarks/{bid}",
                auth=auth, headers=headers,
                json={"params": json.dumps(new_params)}
            )
            if resp3.status_code == 429:
                time.sleep(int(resp3.headers.get("Retry-After", 10)))
                continue
            break

        if resp3.status_code == 200:
            fixed += 1
        else:
            failed += 1
            print(f"    [Failed] '{report.get('name')}': {resp3.status_code}")

    print(f"  Reports: {fixed} updated, {len(reports) - fixed - failed} skipped, {failed} failed")
    return filter_changes, fixed, failed


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dashboard_id> [<dashboard_id> ...]", file=sys.stderr)
        sys.exit(1)

    sa_user = os.environ.get("TO_SERVICE_ACCOUNT_USERNAME")
    sa_password = os.environ.get("TO_SERVICE_ACCOUNT_PASSWORD")
    to_id = os.environ.get("TO_PROJECT_ID")

    if not sa_user or not sa_password:
        print("TO_SERVICE_ACCOUNT_USERNAME and TO_SERVICE_ACCOUNT_PASSWORD must be set in .env", file=sys.stderr)
        sys.exit(1)
    if not to_id:
        print("TO_PROJECT_ID is not set in .env", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(COHORT_MAPPING):
        print(f"Cohort mapping not found: {COHORT_MAPPING}", file=sys.stderr)
        sys.exit(1)

    with open(COHORT_MAPPING) as f:
        cohort_map = json.load(f)

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    dashboard_ids = sys.argv[1:]
    print(f"Remapping cohort IDs on {len(dashboard_ids)} dashboard(s)")

    t0 = time.perf_counter()
    total_filters = 0
    total_reports = 0
    total_failed = 0

    for did in dashboard_ids:
        fc, rf, ff = process_dashboard(did, auth, to_id, cohort_map, headers)
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
