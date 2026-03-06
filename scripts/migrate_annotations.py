#!/usr/bin/env python3
"""
Export annotations from the source project and import them into the target project.

Usage:
    python scripts/migrate_annotations.py

Requires in .env:
    - FROM_SERVICE_ACCOUNT_USERNAME / FROM_SERVICE_ACCOUNT_PASSWORD / FROM_PROJECT_ID
    - TO_SERVICE_ACCOUNT_USERNAME / TO_SERVICE_ACCOUNT_PASSWORD / TO_PROJECT_ID
"""
import json
import os
import sys
import time
from datetime import datetime
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

EXPORT_FILE = os.path.join(_root, "data", "annotations.json")


def main():
    from_user = os.environ.get("FROM_SERVICE_ACCOUNT_USERNAME")
    from_pass = os.environ.get("FROM_SERVICE_ACCOUNT_PASSWORD")
    from_id = os.environ.get("FROM_PROJECT_ID")
    to_user = os.environ.get("TO_SERVICE_ACCOUNT_USERNAME")
    to_pass = os.environ.get("TO_SERVICE_ACCOUNT_PASSWORD")
    to_id = os.environ.get("TO_PROJECT_ID")

    if not from_user or not from_pass:
        print("FROM_SERVICE_ACCOUNT_USERNAME and FROM_SERVICE_ACCOUNT_PASSWORD must be set", file=sys.stderr)
        sys.exit(1)
    if not from_id:
        print("FROM_PROJECT_ID must be set", file=sys.stderr)
        sys.exit(1)
    if not to_user or not to_pass:
        print("TO_SERVICE_ACCOUNT_USERNAME and TO_SERVICE_ACCOUNT_PASSWORD must be set", file=sys.stderr)
        sys.exit(1)
    if not to_id:
        print("TO_PROJECT_ID must be set", file=sys.stderr)
        sys.exit(1)

    from_auth = requests.auth.HTTPBasicAuth(from_user, from_pass)
    to_auth = requests.auth.HTTPBasicAuth(to_user, to_pass)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # 1. Export annotations from source project
    print(f"Exporting annotations from project {from_id}...")
    resp = requests.get(
        f"https://mixpanel.com/api/app/projects/{from_id}/annotations",
        auth=from_auth
    )
    if resp.status_code != 200:
        print(f"Failed to fetch annotations: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    annotations = resp.json().get("results", [])
    print(f"Found {len(annotations)} annotation(s)")

    # Save export
    os.makedirs(os.path.dirname(EXPORT_FILE), exist_ok=True)
    with open(EXPORT_FILE, "w") as f:
        json.dump(annotations, f, indent=2)
    print(f"Saved to {EXPORT_FILE}")

    if not annotations:
        print("Nothing to import.")
        return

    # 2. Export tags from source and create in target
    print(f"\nFetching tags from source project...")
    resp_tags = requests.get(
        f"https://mixpanel.com/api/app/projects/{from_id}/annotations/tags",
        auth=from_auth
    )
    source_tags = resp_tags.json().get("results", []) if resp_tags.status_code == 200 else []

    tag_map = {}
    if source_tags:
        print(f"Found {len(source_tags)} source tag(s)")

        # Fetch existing tags in target to avoid duplicates
        resp_existing = requests.get(
            f"https://mixpanel.com/api/app/projects/{to_id}/annotations/tags",
            auth=to_auth
        )
        existing_tags = {}
        if resp_existing.status_code == 200:
            for t in resp_existing.json().get("results", []):
                existing_tags[t["name"]] = t["id"]

        for tag in source_tags:
            if tag["name"] in existing_tags:
                tag_map[tag["id"]] = existing_tags[tag["name"]]
                print(f"  Tag '{tag['name']}': {tag['id']} -> {tag_map[tag['id']]} (existing)")
                continue

            for attempt in range(3):
                r = requests.post(
                    f"https://mixpanel.com/api/app/projects/{to_id}/annotations/tags",
                    auth=to_auth, headers=headers,
                    json={"name": tag["name"]}
                )
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 10)))
                    continue
                break
            if r.status_code in (200, 201):
                new_tag = r.json().get("results", {})
                tag_map[tag["id"]] = new_tag.get("id", tag["id"])
                print(f"  Tag '{tag['name']}': {tag['id']} -> {tag_map[tag['id']]} (created)")
            else:
                print(f"  [Warning] Tag '{tag['name']}' failed: {r.status_code} {r.text[:100]}")

    # 3. Import annotations into target project
    print(f"\nImporting {len(annotations)} annotation(s) into project {to_id}...")
    created = 0
    failed = 0

    for ann in annotations:
        # Convert ISO 8601 (e.g. "2025-09-15T00:00:00-04:00") to "YYYY-MM-DD HH:mm:ss"
        dt = datetime.fromisoformat(ann["date"])
        date_str = dt.strftime("%Y-%m-%d %H:%M:%S")

        body = {
            "date": date_str,
            "description": ann["description"],
        }

        # Remap tag IDs if present
        if ann.get("tags") and tag_map:
            body["tags"] = [tag_map.get(t["id"], t["id"]) for t in ann["tags"]]
        elif ann.get("tags"):
            body["tags"] = [t["id"] for t in ann["tags"]]

        for attempt in range(3):
            r = requests.post(
                f"https://mixpanel.com/api/app/projects/{to_id}/annotations",
                auth=to_auth, headers=headers,
                json=body
            )
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 10)))
                continue
            break

        if r.status_code in (200, 201):
            created += 1
        else:
            failed += 1
            print(f"  [Failed] '{ann['description'][:50]}': {r.status_code} {r.text[:100]}")

    print(f"\nDone. Created: {created}, Failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
