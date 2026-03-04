#!/usr/bin/env python3
"""
Replace old cohort IDs in dashboards.json with new IDs from cohort_mapping.json.
Writes the result to data/dashboards_remapped.json.
"""
import json
import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DASHBOARDS_FILE = os.path.join(_root, "data", "dashboards.json")
MAPPING_FILE = os.path.join(_root, "data", "cohort_mapping.json")
OUTPUT_FILE = os.path.join(_root, "data", "dashboards_remapped.json")


def remap_cohort_ids(obj, mapping):
    """Recursively walk JSON and replace cohort IDs found in the mapping."""
    if isinstance(obj, dict):
        # Direct cohort reference: {"id": 5976541, "name": "...", ...}
        if "cohort" in obj and isinstance(obj["cohort"], dict):
            cohort = obj["cohort"]
            old_id = str(cohort.get("id", ""))
            if old_id in mapping:
                cohort["id"] = int(mapping[old_id])
        # Also check top-level id in case of other structures
        for key, val in obj.items():
            obj[key] = remap_cohort_ids(val, mapping)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            obj[i] = remap_cohort_ids(item, mapping)
    return obj


def main():
    for path, label in [(DASHBOARDS_FILE, "dashboards.json"), (MAPPING_FILE, "cohort_mapping.json")]:
        if not os.path.isfile(path):
            print(f"{label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    with open(MAPPING_FILE) as f:
        mapping = json.load(f)

    with open(DASHBOARDS_FILE) as f:
        dashboards = json.load(f)

    replacements = 0
    original = json.dumps(dashboards)

    remap_cohort_ids(dashboards, mapping)

    remapped = json.dumps(dashboards)
    # Count how many IDs were actually swapped
    for old_id, new_id in mapping.items():
        replacements += original.count(f'"id": {old_id}') - remapped.count(f'"id": {old_id}')

    with open(OUTPUT_FILE, "w") as f:
        json.dump(dashboards, f, indent=4, ensure_ascii=False)

    print(f"Remapped {replacements} cohort ID(s)")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
