#!/usr/bin/env python3
"""
Fetch full layouts from source dashboards, remap report (bookmark) IDs,
and PATCH them onto the target dashboards.

Requires:
  - data/dashboard_mapping.json (old dashboard ID -> new dashboard ID)
  - data/bookmark_mapping.json  (old bookmark ID -> new bookmark ID)
  - FROM_SERVICE_ACCOUNT_USERNAME/PASSWORD + FROM_PROJECT_ID in .env
  - TO_SERVICE_ACCOUNT_USERNAME/PASSWORD + TO_PROJECT_ID in .env
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

DASHBOARD_MAPPING = os.path.join(_root, "data", "dashboard_mapping.json")
BOOKMARK_MAPPING = os.path.join(_root, "data", "bookmark_mapping.json")


def remap_layout(layout, bm_map):
    """Replace old bookmark IDs with new ones in the layout rows."""
    new_rows = {}
    for row_id, row in layout.get("rows", {}).items():
        new_cells = []
        for cell in row.get("cells", []):
            new_cell = dict(cell)
            if cell.get("content_type") == "report":
                old_cid = str(cell.get("content_id", ""))
                if old_cid in bm_map:
                    new_cell["content_id"] = int(bm_map[old_cid])
            new_cells.append(new_cell)
        new_rows[row_id] = dict(row, cells=new_cells)
    return dict(layout, rows=new_rows)


def main():
    from_user = os.environ.get("FROM_SERVICE_ACCOUNT_USERNAME")
    from_pass = os.environ.get("FROM_SERVICE_ACCOUNT_PASSWORD")
    from_id = os.environ.get("FROM_PROJECT_ID")
    to_user = os.environ.get("TO_SERVICE_ACCOUNT_USERNAME")
    to_pass = os.environ.get("TO_SERVICE_ACCOUNT_PASSWORD")
    to_id = os.environ.get("TO_PROJECT_ID")

    for var in ("FROM_SERVICE_ACCOUNT_USERNAME", "FROM_SERVICE_ACCOUNT_PASSWORD", "FROM_PROJECT_ID",
                "TO_SERVICE_ACCOUNT_USERNAME", "TO_SERVICE_ACCOUNT_PASSWORD", "TO_PROJECT_ID"):
        if not os.environ.get(var):
            print(f"{var} is not set in .env", file=sys.stderr)
            sys.exit(1)

    with open(DASHBOARD_MAPPING) as f:
        dash_map = json.load(f)
    with open(BOOKMARK_MAPPING) as f:
        bm_map = json.load(f)

    auth_from = requests.auth.HTTPBasicAuth(from_user, from_pass)
    auth_to = requests.auth.HTTPBasicAuth(to_user, to_pass)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    print(f"Syncing layouts for {len(dash_map)} dashboards")

    t0 = time.perf_counter()
    synced = 0
    skipped = 0
    failed = 0

    for old_did, new_did in dash_map.items():
        # Fetch source dashboard full details
        for attempt in range(3):
            resp = requests.get(
                f"https://mixpanel.com/api/app/projects/{from_id}/dashboards/{old_did}",
                auth=auth_from
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 10)))
                continue
            break

        if resp.status_code != 200:
            failed += 1
            print(f"    [Failed] Fetch source {old_did}: {resp.status_code}")
            continue

        source = resp.json().get("results", {})
        layout = source.get("layout", {})

        if not layout.get("order"):
            skipped += 1
            continue

        # Remap bookmark IDs in layout
        new_layout = remap_layout(layout, bm_map)

        # PATCH target dashboard with remapped layout
        for attempt in range(3):
            resp2 = requests.patch(
                f"https://mixpanel.com/api/app/projects/{to_id}/dashboards/{new_did}",
                auth=auth_to,
                headers=headers,
                json={"layout": new_layout}
            )
            if resp2.status_code == 429:
                time.sleep(int(resp2.headers.get("Retry-After", 10)))
                continue
            break

        if resp2.status_code == 200:
            synced += 1
            title = source.get("title", old_did)
            n_reports = len(layout.get("order", []))
            print(f"    [OK] '{title}': {n_reports} rows")
        else:
            failed += 1
            print(f"    [Failed] PATCH {new_did}: {resp2.status_code} - {resp2.text[:200]}")

    elapsed = time.perf_counter() - t0
    print(f"\nDone.")
    print(f"Synced:  {synced}")
    print(f"Skipped: {skipped} (empty layouts)")
    print(f"Failed:  {failed}")
    print(f"Time:    {elapsed:.1f}s")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
