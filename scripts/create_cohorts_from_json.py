#!/usr/bin/env python3
"""
Import cohorts from JSON file into the Mixpanel project specified by TO_PROJECT_ID in .env.
Generates a 'cohort_mapping.json' file for updating dashboards later.
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

# Point directly to your specific JSON file
COHORTS_FILE = os.path.join(_root, "data", "cohorts.json")
MAPPING_FILE = os.path.join(_root, "data", "cohort_mapping.json")

def main():
    # Mixpanel App API requires Service Account credentials
    sa_user = os.environ.get("TO_SERVICE_ACCOUNT_USERNAME")
    sa_password = os.environ.get("TO_SERVICE_ACCOUNT_PASSWORD")
    to_id = os.environ.get("TO_PROJECT_ID")

    if not sa_user or not sa_password:
        print("TO_SERVICE_ACCOUNT_USERNAME and TO_SERVICE_ACCOUNT_PASSWORD must be set in .env", file=sys.stderr)
        sys.exit(1)
    if not to_id:
        print("TO_PROJECT_ID is not set in .env", file=sys.stderr)
        sys.exit(1)

    # Use command line argument if provided, otherwise default to data/cohorts.json
    path = sys.argv[1] if len(sys.argv) > 1 else COHORTS_FILE

    if not os.path.isfile(path):
        print("File not found:", path, file=sys.stderr)
        sys.exit(1)

    print(f"Importing cohorts to project {to_id} from {path}")

    auth = requests.auth.HTTPBasicAuth(sa_user, sa_password)
    base_url = f"https://mixpanel.com/api/app/projects/{to_id}/cohorts"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    t0 = time.perf_counter()
    total_cohorts = 0
    failed = 0

    # Load existing mapping so re-runs don't lose previous entries
    if os.path.isfile(MAPPING_FILE):
        with open(MAPPING_FILE) as mf:
            mapping = json.load(mf)
    else:
        mapping = {}

    with open(path) as f:
        data = json.load(f)

    # Handle both list formats and dict formats (depending on how it was exported)
    cohorts = data.get('results', data) if isinstance(data, dict) else data

    if not isinstance(cohorts, list):
        print(f"Skipping {os.path.basename(path)}: Invalid JSON structure. Expected a list.", file=sys.stderr)
        sys.exit(1)

    n = len(cohorts)
    print(f"-> {n} cohorts to process")

    for cohort in cohorts:
        old_id = str(cohort.get('id'))
        name = cohort.get('name')

        # Prepare payload for new project
        payload = {
            "name": name,
            "description": cohort.get('description', '')
        }

        # Include cohort definition fields expected by the API
        for key in ('groups', 'behaviors', 'selector'):
            if key in cohort and cohort[key]:
                payload[key] = cohort[key]

        # POST to Mixpanel (with rate-limit retry)
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
                new_cohort_data = response.json()
                inner = new_cohort_data.get('results', new_cohort_data)
                new_id = str(inner['id']) if isinstance(inner, dict) and 'id' in inner else ''
            except (ValueError, KeyError, TypeError):
                new_id = ''
            if new_id:
                mapping[old_id] = new_id
                total_cohorts += 1
                print(f"    [Success] '{name}': {old_id} -> {new_id}")
            else:
                failed += 1
                print(f"    [Failed] '{name}': could not parse cohort ID from response")
        else:
            failed += 1
            print(f"    [Failed] '{name}': {response.status_code} - {response.text}")

    # Save the critical mapping file
    with open(MAPPING_FILE, 'w') as mf:
        json.dump(mapping, mf, indent=2)

    elapsed = time.perf_counter() - t0
    print("\nDone.")
    print("Cohorts imported: ", total_cohorts)
    print("Cohorts failed:   ", failed)
    print("Mapping saved to: ", MAPPING_FILE)
    print("Time elapsed:     {:.1f}s".format(elapsed))
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()
