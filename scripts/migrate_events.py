#!/usr/bin/env python3
"""
Migrate Mixpanel events between projects and benchmark export/import throughput.

Uses the official mixpanel-utils SDK (pip install mixpanel-utils).

Requires a .env file (or exported env vars) with:
    FROM_SERVICE_ACCOUNT_USERNAME
    FROM_SERVICE_ACCOUNT_PASSWORD
    FROM_PROJECT_ID
    FROM_PROJECT_TOKEN
    TO_SERVICE_ACCOUNT_USERNAME
    TO_SERVICE_ACCOUNT_PASSWORD
    TO_PROJECT_ID
    TO_PROJECT_TOKEN

Usage:
    python scripts/migrate_events.py

    # Export only (no import):
    python scripts/migrate_events.py --export-only

    # Import only (from existing export file):
    python scripts/migrate_events.py --import-only
"""

import argparse
import json
import os
import sys
import time

from dotenv import load_dotenv
from mixpanel_utils import MixpanelUtils

load_dotenv()

DEFAULT_FROM = "2025-11-01"
DEFAULT_TO = "2026-01-31"
DEFAULT_EXPORT_FILE = "mixpanel_export.ndjson"


def env(name):
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: Missing required env var: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def count_lines(filepath):
    count = 0
    with open(filepath, "rb") as f:
        for _ in f:
            count += 1
    return count


def fmt_duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def run_export(args):
    sa_username = env("FROM_SERVICE_ACCOUNT_USERNAME")
    sa_password = env("FROM_SERVICE_ACCOUNT_PASSWORD")
    project_id = env("FROM_PROJECT_ID")
    project_token = env("FROM_PROJECT_TOKEN")

    print(f"\n{'='*60}")
    print(f"EXPORT PHASE")
    print(f"{'='*60}")
    print(f"Source project ID: {project_id}")
    print(f"Date range:        {args.from_date} to {args.to_date}")
    print(f"Output file:       {args.export_file}")
    print()

    source = MixpanelUtils(
        sa_password,
        service_account_username=sa_username,
        project_id=int(project_id),
        token=project_token,
        timeout=1200,
        debug=args.debug,
    )

    params = {"from_date": args.from_date, "to_date": args.to_date}

    print("Starting export...")
    t0 = time.monotonic()
    source.export_events(
        args.export_file,
        params,
        raw_stream=True,
        add_gzip_header=False,
    )
    export_time = time.monotonic() - t0

    event_count = count_lines(args.export_file)
    file_size_mb = os.path.getsize(args.export_file) / (1024 * 1024)

    print(f"Export complete.")
    print(f"  Events:    {event_count:,}")
    print(f"  File size: {file_size_mb:,.1f} MB")
    print(f"  Duration:  {fmt_duration(export_time)}")
    if export_time > 0:
        print(f"  Throughput: {event_count / export_time:,.0f} events/sec")

    return export_time, event_count


def run_import(args):
    sa_username = env("TO_SERVICE_ACCOUNT_USERNAME")
    sa_password = env("TO_SERVICE_ACCOUNT_PASSWORD")
    project_id = env("TO_PROJECT_ID")
    project_token = env("TO_PROJECT_TOKEN")

    print(f"\n{'='*60}")
    print(f"IMPORT PHASE")
    print(f"{'='*60}")
    print(f"Dest project ID: {project_id}")
    print(f"Input file:      {args.export_file}")
    print(f"Strict mode:     {not args.no_strict}")
    print()

    if not os.path.exists(args.export_file):
        print(f"ERROR: Export file not found: {args.export_file}", file=sys.stderr)
        print("Run with --export-only first, or without --import-only.", file=sys.stderr)
        sys.exit(1)

    print("Loading events from NDJSON...")
    events = []
    with open(args.export_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    event_count = len(events)
    print(f"Events to import: {event_count:,}")

    dest = MixpanelUtils(
        sa_password,
        service_account_username=sa_username,
        project_id=int(project_id),
        token=project_token,
        strict_import=not args.no_strict,
        timeout=1200,
        debug=args.debug,
    )

    print("Starting import...")
    t0 = time.monotonic()
    dest.import_events(events, timezone_offset=0)
    import_time = time.monotonic() - t0

    print(f"Import complete.")
    print(f"  Events:     {event_count:,}")
    print(f"  Duration:   {fmt_duration(import_time)}")
    if import_time > 0:
        print(f"  Throughput: {event_count / import_time:,.0f} events/sec")

    if os.path.exists("invalid_events.txt"):
        invalid_count = count_lines("invalid_events.txt")
        print(f"\n  WARNING: {invalid_count:,} invalid events dumped to invalid_events.txt")

    if os.path.exists("import_backup.txt"):
        failed_count = count_lines("import_backup.txt")
        print(f"\n  WARNING: {failed_count:,} failed batches dumped to import_backup.txt")

    return import_time, event_count


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Mixpanel events between projects with timing benchmarks."
    )

    parser.add_argument("--from-date", default=DEFAULT_FROM, help=f"Start date YYYY-MM-DD (default: {DEFAULT_FROM})")
    parser.add_argument("--to-date", default=DEFAULT_TO, help=f"End date YYYY-MM-DD (default: {DEFAULT_TO})")
    parser.add_argument("--export-file", default=DEFAULT_EXPORT_FILE, help=f"Intermediate NDJSON file (default: {DEFAULT_EXPORT_FILE})")

    phase = parser.add_mutually_exclusive_group()
    phase.add_argument("--export-only", action="store_true", help="Only run the export phase")
    phase.add_argument("--import-only", action="store_true", help="Only run the import phase")

    parser.add_argument("--no-strict", action="store_true", help="Disable strict validation on import (not recommended)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose SDK debug logging")

    args = parser.parse_args()

    total_t0 = time.monotonic()
    export_time = 0
    import_time = 0
    event_count = 0

    if not args.import_only:
        export_time, event_count = run_export(args)

    if not args.export_only:
        import_time, event_count = run_import(args)

    total_time = time.monotonic() - total_t0

    print(f"\n{'='*60}")
    print(f"MIGRATION BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"Date range:     {args.from_date} to {args.to_date}")
    print(f"Total events:   {event_count:,}")
    if not args.import_only:
        print(f"Export time:    {fmt_duration(export_time)}")
        if export_time > 0:
            print(f"Export rate:    {event_count / export_time:,.0f} events/sec")
    if not args.export_only:
        print(f"Import time:    {fmt_duration(import_time)}")
        if import_time > 0:
            print(f"Import rate:    {event_count / import_time:,.0f} events/sec")
    print(f"Total time:     {fmt_duration(total_time)}")
    print()

    if os.path.exists("invalid_events.txt"):
        print("WARNING: Some events failed validation. See invalid_events.txt")
    if os.path.exists("import_backup.txt"):
        print("WARNING: Some batches failed to import. See import_backup.txt")


if __name__ == "__main__":
    main()
