#!/usr/bin/env python3
"""
Replace old dashboard IDs in reports_remapped.json with new IDs from dashboard_mapping.json.
Writes the result to data/reports_ready.json.
"""
import json
import os
import re
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REPORTS_FILE = os.path.join(_root, "data", "reports_remapped.json")
MAPPING_FILE = os.path.join(_root, "data", "dashboard_mapping.json")
OUTPUT_FILE = os.path.join(_root, "data", "reports_ready.json")


def main():
    for path, label in [(REPORTS_FILE, "reports_remapped.json"), (MAPPING_FILE, "dashboard_mapping.json")]:
        if not os.path.isfile(path):
            print(f"{label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    with open(MAPPING_FILE) as f:
        mapping = json.load(f)

    with open(REPORTS_FILE) as f:
        data = json.load(f)

    reports = data.get('results', data) if isinstance(data, dict) else data

    if not isinstance(reports, list):
        print("Invalid JSON structure. Expected a list.", file=sys.stderr)
        sys.exit(1)

    remapped = 0
    unmapped = 0

    for report in reports:
        old_id = str(report.get('dashboard_id', ''))
        if old_id in mapping:
            report['dashboard_id'] = int(mapping[old_id])
            remapped += 1
        elif old_id:
            unmapped += 1

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Remapped {remapped} dashboard ID(s)")
    if unmapped:
        print(f"Warning: {unmapped} report(s) had dashboard IDs not found in mapping")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
