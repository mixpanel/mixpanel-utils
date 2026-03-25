"""CLI interface for mixpanel-utils streaming pipeline."""

import asyncio
import json
import os
import sys

import click

from . import mp_import
from .utils import comma, bytes_human


@click.command(context_settings={"max_content_width": 120})
@click.argument("data", required=False)
# ── Auth ─────────────────────────────────────────────────────────────
@click.option("--project", type=int, help="Mixpanel project ID", envvar="MP_PROJECT")
@click.option("--acct", help="Service account username", envvar="MP_ACCT")
@click.option("--pass", "pass_", help="Service account password", envvar="MP_PASS")
@click.option("--secret", help="Project API secret", envvar="MP_SECRET")
@click.option("--token", help="Project token", envvar="MP_TOKEN")
@click.option("--bearer", help="Bearer token for exports")
@click.option("--group", "group_key", help="Group analytics group key")
@click.option("--second-token", help="Second project token (export-import)")
# ── Core Config ──────────────────────────────────────────────────────
@click.option(
    "--type", "record_type", default="event",
    type=click.Choice([
        "event", "user", "group", "export",
        "profile-export", "profile-delete",
        "group-export", "group-delete",
        "export-import-event", "export-import-profile",
    ]),
    help="Record type to import/export",
)
@click.option(
    "--region", default="US",
    type=click.Choice(["US", "EU", "IN"]),
    help="Data residency region",
)
@click.option(
    "--format", "stream_format", default="",
    type=click.Choice(["", "json", "jsonl", "csv", "parquet"], case_sensitive=False),
    help="Data format (auto-detected from extension if omitted)",
)
@click.option(
    "--vendor", default="",
    type=click.Choice(["", "amplitude", "heap", "ga4", "posthog", "mparticle", "june", "mixpanel"]),
    help="Apply vendor-specific transform",
)
@click.option("--vendor-opts", default="{}", help="JSON options for vendor transform")
# ── Performance ──────────────────────────────────────────────────────
@click.option("--workers", default=10, type=int, help="Concurrent HTTP connections")
@click.option("--batch", "records_per_batch", default=2000, type=int, help="Records per batch")
@click.option("--retries", "max_retries", default=10, type=int, help="Max retry attempts")
@click.option("--compress/--no-compress", default=True, help="Gzip compress event requests")
# ── Transforms ───────────────────────────────────────────────────────
@click.option("--fix/--no-fix", "fix_data", default=True, help="Apply automatic data fixes (default: on)")
@click.option("--fix-time", is_flag=True, help="Normalize timestamps to UNIX epoch")
@click.option("--dedupe", is_flag=True, help="Remove duplicate records")
@click.option("--remove-nulls", is_flag=True, help="Remove null/empty values")
@click.option("--flatten", is_flag=True, help="Flatten nested properties")
@click.option("--fix-json", is_flag=True, help="Parse string values that look like JSON")
@click.option("--add-token", is_flag=True, help="Add token to each record")
@click.option("--offset", "time_offset", default=0, type=int, help="UTC hours offset for timestamps")
@click.option("--tags", default="{}", help="JSON tags to add to all records")
@click.option("--aliases", default="{}", help="JSON key renames for properties")
@click.option("--scrub-props", default="[]", help="JSON array of properties to remove")
@click.option("--directive", default="$set", help="Profile operation ($set, $set_once, etc.)")
@click.option("--v2-compat", is_flag=True, help="Set distinct_id from $user_id/$device_id")
# ── Write to File ─────────────────────────────────────────────────────
@click.option("--write-to-file", is_flag=True, help="Write transformed data to file instead of Mixpanel")
@click.option("--output-file", "output_file_path", default="./mixpanel-transform.json", help="Output file path for write-to-file mode")
@click.option("--dimension-maps", default="[]", help="JSON array of {filePath, keyOne, keyTwo, label} for lookup maps")
# ── Filtering ────────────────────────────────────────────────────────
@click.option("--epoch-start", default=0, type=int, help="Skip records before UNIX timestamp")
@click.option("--epoch-end", default=9991427224, type=int, help="Skip records after UNIX timestamp")
@click.option("--event-whitelist", default="[]", help="JSON array of allowed event names")
@click.option("--event-blacklist", default="[]", help="JSON array of blocked event names")
@click.option("--prop-key-whitelist", default="[]", help="JSON array of required property keys")
@click.option("--prop-key-blacklist", default="[]", help="JSON array of blocked property keys")
@click.option("--prop-val-whitelist", default="[]", help="JSON array of required property values")
@click.option("--prop-val-blacklist", default="[]", help="JSON array of blocked property values")
# ── Export ────────────────────────────────────────────────────────────
@click.option("--start", help="Export start date (YYYY-MM-DD)")
@click.option("--end", help="Export end date (YYYY-MM-DD)")
@click.option("--where", "where_clause", help="WHERE clause for export filtering")
@click.option("--limit", type=int, help="Maximum records to export")
# ── Debug ────────────────────────────────────────────────────────────
@click.option("--verbose/--quiet", default=True, help="Show progress output")
@click.option("--dry-run", is_flag=True, help="Transform without sending to Mixpanel")
@click.option("--strict/--no-strict", default=True, help="Validate data strictly")
@click.option("--logs", is_flag=True, help="Save import log to file")
# ── Cloud Storage ────────────────────────────────────────────────────
@click.option("--s3-key", envvar="S3_KEY", help="AWS S3 access key")
@click.option("--s3-secret", envvar="S3_SECRET", help="AWS S3 secret key")
@click.option("--s3-region", envvar="S3_REGION", help="AWS S3 region")
def main(data, **kwargs):
    """Stream data into Mixpanel's ingestion APIs.

    DATA is a file path, directory, glob pattern, or cloud URL (gs://, s3://).
    """
    # Build creds dict
    cred_keys = ["project", "acct", "pass_", "secret", "token", "bearer",
                 "group_key", "second_token",
                 "s3_key", "s3_secret", "s3_region"]
    creds = {}
    for k in cred_keys:
        val = kwargs.pop(k, None)
        if val:
            cred_key = "pass" if k == "pass_" else k
            creds[cred_key] = val

    # Map CLI names to options dict
    opts = {}
    for k, v in kwargs.items():
        if v is not None and v != "" and v != []:
            opts[k] = v

    # Parse JSON string options
    json_opts = ["tags", "aliases", "vendor_opts", "scrub_props",
                 "event_whitelist", "event_blacklist",
                 "prop_key_whitelist", "prop_key_blacklist",
                 "prop_val_whitelist", "prop_val_blacklist",
                 "dimension_maps"]
    for key in json_opts:
        if key in opts and isinstance(opts[key], str):
            try:
                opts[key] = json.loads(opts[key])
            except json.JSONDecodeError:
                pass

    rt = opts.get("record_type", "")
    no_data_types = ("export", "profile-export", "profile-delete", "group-export",
                     "group-delete", "export-import-event", "export-import-profile")
    if not data and rt not in no_data_types:
        click.echo("Error: DATA argument is required (file path, directory, or cloud URL)", err=True)
        sys.exit(1)

    # Set up verbose progress display
    verbose = opts.get("verbose", True)
    if verbose and not opts.get("dry_run"):
        _print_config_summary(data, rt, opts, creds)

    # Run the import
    try:
        result = asyncio.run(mp_import(creds or None, data, opts))
        _print_summary(result)

        if opts.get("logs"):
            _save_log(result)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _print_config_summary(data, record_type, opts, creds):
    """Print a config summary box at the start of an import."""
    click.echo("")
    click.echo("=" * 60)
    click.echo("  MIXPANEL STREAMING IMPORT")
    click.echo("=" * 60)
    click.echo(f"  Record Type:  {record_type or 'event'}")
    click.echo(f"  Region:       {opts.get('region', 'US')}")
    click.echo(f"  Data:         {data or '(none)'}")
    if opts.get("vendor"):
        click.echo(f"  Vendor:       {opts['vendor']}")
    click.echo(f"  Workers:      {opts.get('workers', 10)}")
    click.echo(f"  Batch Size:   {opts.get('records_per_batch', 2000)}")

    features = []
    if opts.get("fix_data"):
        features.append("fix_data")
    if opts.get("dedupe"):
        features.append("dedupe")
    if opts.get("compress", True):
        features.append("gzip")
    if opts.get("write_to_file"):
        features.append(f"write_to_file({opts.get('output_file_path', 'mixpanel-transform.json')})")
    if features:
        click.echo(f"  Features:     {', '.join(features)}")

    auth_type = "token" if creds.get("token") else "service_account" if creds.get("acct") else "secret" if creds.get("secret") else "none"
    click.echo(f"  Auth:         {auth_type}")
    click.echo("=" * 60)
    click.echo("")


def _print_summary(result: dict):
    """Print import results summary."""
    click.echo("")
    click.echo("=" * 60)
    click.echo("  IMPORT COMPLETE")
    click.echo("=" * 60)
    click.echo(f"  Record Type:  {result.get('record_type', 'unknown')}")
    click.echo(f"  Duration:     {result.get('duration_human', 'N/A')}")
    click.echo(f"  Total:        {comma(result.get('total', 0))}")
    click.echo(f"  Success:      {comma(result.get('success', 0))}")
    click.echo(f"  Failed:       {comma(result.get('failed', 0))}")

    if result.get("empty"):
        click.echo(f"  Empty:        {comma(result['empty'])}")
    if result.get("duplicates"):
        click.echo(f"  Duplicates:   {comma(result['duplicates'])}")
    if result.get("out_of_bounds"):
        click.echo(f"  Out of Bounds:{comma(result['out_of_bounds'])}")

    click.echo(f"  Bytes:        {result.get('bytes_human', '0 B')}")
    click.echo(f"  Requests:     {comma(result.get('requests', 0))}")
    click.echo(f"  Retries:      {comma(result.get('retries', 0))}")

    if result.get("eps"):
        click.echo(f"  Events/sec:   {comma(result['eps'])}")
    if result.get("rps"):
        click.echo(f"  Requests/sec: {result['rps']:.2f}")
    if result.get("mbps"):
        click.echo(f"  MB/sec:       {result['mbps']:.3f}")

    if result.get("errors"):
        click.echo("")
        click.echo("  Errors:")
        for msg, count in result["errors"].items():
            click.echo(f"    {msg}: {count}")

    click.echo("=" * 60)
    click.echo("")


def _save_log(result: dict):
    """Save import results to a JSON log file."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"mixpanel-import-{timestamp}.json"

    log_data = {k: v for k, v in result.items() if k != "dry_run" or v}
    with open(filename, "w") as f:
        json.dump(log_data, f, indent=2, default=str)
    click.echo(f"  Log saved: {filename}")


if __name__ == "__main__":
    main()
