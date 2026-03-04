#!/usr/bin/env python3
"""
Link imported bookmarks to their dashboards by PATCHing each bookmark's dashboard_id.
Uses reports_ready.json (which has the remapped dashboard IDs) and matches
bookmarks by name + type to assign the correct dashboard_id.
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

REPORTS_FILE = os.path.join(_root, "data", "reports_ready.json")


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

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # Load reports_ready.json to know which dashboard each report should be on
    with open(REPORTS_FILE) as f:
        data = json.load(f)
    source_reports = data.get('results', data) if isinstance(data, dict) else data

    # Fetch all bookmarks from the target project
    print("Fetching bookmarks from target project...")
    resp = requests.get(f"https://mixpanel.com/api/app/projects/{to_id}/bookmarks", auth=auth)
    if resp.status_code != 200:
        print(f"Failed to fetch bookmarks: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    all_bookmarks = resp.json().get('results', [])

    # Find bookmarks without a dashboard_id (the ones we created)
    unlinked = [b for b in all_bookmarks if b.get('dashboard_id') is None]
    print(f"Found {len(unlinked)} unlinked bookmarks")

    if not unlinked:
        print("Nothing to do.")
        return

    # Build a lookup from (name, type) -> dashboard_id from source reports
    # Use a list since multiple reports can have the same name+type (on different dashboards)
    report_queue = {}
    for r in source_reports:
        key = (r.get('name'), r.get('type'))
        report_queue.setdefault(key, []).append(r.get('dashboard_id'))

    t0 = time.perf_counter()
    linked = 0
    skipped = 0
    failed = 0

    for i, bookmark in enumerate(unlinked):
        key = (bookmark.get('name'), bookmark.get('type'))
        dashboard_ids = report_queue.get(key, [])

        if not dashboard_ids:
            skipped += 1
            print(f"    [Skip] '{bookmark['name']}': no matching source report")
            continue

        # Pop the first matching dashboard_id (handles duplicates in order)
        dashboard_id = dashboard_ids.pop(0)

        # Rate-limit retry
        for attempt in range(3):
            resp = requests.patch(
                f"https://mixpanel.com/api/app/projects/{to_id}/bookmarks/{bookmark['id']}",
                auth=auth,
                headers=headers,
                json={"dashboard_id": dashboard_id}
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"    [Rate limited] Waiting {wait}s...")
                time.sleep(wait)
                continue
            break

        if resp.status_code == 200:
            linked += 1
            if (i + 1) % 25 == 0:
                print(f"    Progress: {i + 1}/{len(unlinked)} ({linked} linked)")
        else:
            failed += 1
            print(f"    [Failed] '{bookmark['name']}': {resp.status_code} - {resp.text[:200]}")

    elapsed = time.perf_counter() - t0
    print(f"\nDone.")
    print(f"Linked:  {linked}")
    print(f"Skipped: {skipped}")
    print(f"Failed:  {failed}")
    print(f"Time:    {elapsed:.1f}s")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
