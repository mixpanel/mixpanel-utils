#!/usr/bin/env python3
"""
Import dashboards from JSON file into the Mixpanel project specified by TO_PROJECT_ID in .env.
Generates a 'dashboard_mapping.json' file (old_id -> new_id) for importing reports later.
Uses dashboards_remapped.json (with cohort IDs already remapped).
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

DASHBOARDS_FILE = os.path.join(_root, "data", "dashboards_remapped.json")
MAPPING_FILE = os.path.join(_root, "data", "dashboard_mapping.json")


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

    path = sys.argv[1] if len(sys.argv) > 1 else DASHBOARDS_FILE

    if not os.path.isfile(path):
        print("File not found:", path, file=sys.stderr)
        sys.exit(1)

    print(f"Importing dashboards to project {to_id} from {path}")

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    base_url = f"https://mixpanel.com/api/app/projects/{to_id}/dashboards"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # Load existing mapping so re-runs don't lose previous entries
    if os.path.isfile(MAPPING_FILE):
        with open(MAPPING_FILE) as mf:
            mapping = json.load(mf)
    else:
        mapping = {}

    with open(path) as f:
        data = json.load(f)

    dashboards = data.get('results', data) if isinstance(data, dict) else data

    if not isinstance(dashboards, list):
        print("Invalid JSON structure. Expected a list.", file=sys.stderr)
        sys.exit(1)

    print(f"-> {len(dashboards)} dashboards to process")

    t0 = time.perf_counter()
    created = 0
    failed = 0

    for dashboard in dashboards:
        old_id = str(dashboard.get('id'))
        title = dashboard.get('title', 'Untitled')

        payload = {
            "title": title,
            "description": dashboard.get('description') or None,
        }

        # Include dashboard-level filters, breakdowns, time_filter if present
        for key in ('filters', 'breakdowns', 'time_filter'):
            val = dashboard.get(key)
            if val:
                payload[key] = val

        # Rate-limit retry
        for attempt in range(3):
            response = requests.post(base_url, auth=auth, headers=headers, json=payload)
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 10))
                print(f"    [Rate limited] Waiting {wait}s...")
                time.sleep(wait)
                continue
            break

        if response.status_code in (200, 201):
            try:
                resp_data = response.json()
                inner = resp_data.get('results', resp_data)
                new_id = str(inner['id']) if isinstance(inner, dict) and 'id' in inner else ''
            except (ValueError, KeyError, TypeError):
                new_id = ''
            if new_id:
                mapping[old_id] = new_id
                created += 1
                print(f"    [Success] '{title}': {old_id} -> {new_id}")
            else:
                failed += 1
                print(f"    [Failed] '{title}': could not parse dashboard ID from response")
        else:
            failed += 1
            print(f"    [Failed] '{title}': {response.status_code} - {response.text}")

    with open(MAPPING_FILE, 'w') as mf:
        json.dump(mapping, mf, indent=2)

    elapsed = time.perf_counter() - t0
    print(f"\nDone.")
    print(f"Dashboards imported: {created}")
    print(f"Dashboards failed:   {failed}")
    print(f"Mapping saved to:    {MAPPING_FILE}")
    print(f"Time elapsed:        {elapsed:.1f}s")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
