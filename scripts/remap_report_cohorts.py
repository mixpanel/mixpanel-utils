#!/usr/bin/env python3
"""
Replace old cohort IDs in reports.json with new IDs from cohort_mapping.json.
Writes the result to data/reports_remapped.json.

Cohort IDs in reports appear inside JSON-encoded strings (escaped JSON within
the 'params' field), so we do string-level replacement with careful patterns
to avoid false positives.
"""
import json
import os
import re
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REPORTS_FILE = os.path.join(_root, "data", "reports.json")
MAPPING_FILE = os.path.join(_root, "data", "cohort_mapping.json")
OUTPUT_FILE = os.path.join(_root, "data", "reports_remapped.json")


def main():
    for path, label in [(REPORTS_FILE, "reports.json"), (MAPPING_FILE, "cohort_mapping.json")]:
        if not os.path.isfile(path):
            print(f"{label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    with open(MAPPING_FILE) as f:
        mapping = json.load(f)

    with open(REPORTS_FILE) as f:
        text = f.read()

    total = 0
    # Cohort IDs appear as escaped JSON inside strings:
    #   \"id\": 5976541   (literal backslash-quote in the file)
    # and potentially as normal JSON:
    #   "id": 5976541
    for old_id, new_id in mapping.items():
        # Escaped JSON context: literal \"id\": OLD in the file
        pattern = re.compile(r'(\\?"id\\?": ?)' + re.escape(old_id) + r'\b')
        text, count = pattern.subn(r'\g<1>' + new_id, text)

        if count:
            print(f"  {old_id} -> {new_id}  ({count} replacements)")
            total += count

    # Validate the result is still valid JSON
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"ERROR: remapped JSON is invalid: {e}", file=sys.stderr)
        sys.exit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"\nRemapped {total} cohort ID(s) across {len(mapping)} mapping entries")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
