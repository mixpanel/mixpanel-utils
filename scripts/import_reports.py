#!/usr/bin/env python3
"""
Import reports from reports_ready.json into the Mixpanel project specified by TO_PROJECT_ID in .env.
Reports must already have remapped cohort IDs and dashboard IDs.
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

    path = sys.argv[1] if len(sys.argv) > 1 else REPORTS_FILE

    if not os.path.isfile(path):
        print("File not found:", path, file=sys.stderr)
        sys.exit(1)

    print(f"Importing reports to project {to_id} from {path}")

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    with open(path) as f:
        data = json.load(f)

    reports = data.get('results', data) if isinstance(data, dict) else data

    if not isinstance(reports, list):
        print("Invalid JSON structure. Expected a list.", file=sys.stderr)
        sys.exit(1)

    n = len(reports)
    print(f"-> {n} reports to process")

    t0 = time.perf_counter()
    created = 0
    failed = 0

    for i, report in enumerate(reports):
        dashboard_id = report.get('dashboard_id')
        name = report.get('name', 'Untitled')
        report_type = report.get('type', 'insights')

        # API endpoint: Mixpanel uses "bookmarks" internally for reports
        url = f"https://mixpanel.com/api/app/projects/{to_id}/bookmarks"

        # params must be sent as a JSON string, not a parsed object
        params = report.get('params', '{}')
        if not isinstance(params, str):
            params = json.dumps(params)

        payload = {
            "name": name,
            "description": report.get('description') or '',
            "type": report_type,
            "dashboard_id": dashboard_id,
            "params": params,
        }

        # Rate-limit retry
        for attempt in range(3):
            response = requests.post(url, auth=auth, headers=headers, json=payload)
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 10))
                print(f"    [Rate limited] Waiting {wait}s...")
                time.sleep(wait)
                continue
            break

        if response.status_code in (200, 201):
            created += 1
            if (i + 1) % 25 == 0 or (i + 1) == n:
                print(f"    Progress: {i + 1}/{n} ({created} created)")
        else:
            failed += 1
            print(f"    [Failed] '{name}' (dashboard {dashboard_id}): {response.status_code} - {response.text[:200]}")

    elapsed = time.perf_counter() - t0
    print(f"\nDone.")
    print(f"Reports imported: {created}")
    print(f"Reports failed:   {failed}")
    print(f"Time elapsed:     {elapsed:.1f}s")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
