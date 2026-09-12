"""Type definitions for the streaming pipeline.

These TypedDicts provide intellisense/autocomplete for the creds and options
dicts accepted by mp_import(), MpStream, and StreamInterface methods.
"""

from __future__ import annotations

from typing import TypedDict, Callable


class Creds(TypedDict, total=False):
    """Mixpanel authentication credentials."""
    acct: str
    pass_: str
    project: int | str
    token: str
    secret: str
    bearer: str
    group_key: str
    data_group_id: str
    workspace: int | str
    org: int | str
    second_token: str
    # Cloud storage (can also go in Options)
    gcp_project_id: str
    gcs_credentials: str
    s3_key: str
    s3_secret: str
    s3_region: str


class Options(TypedDict, total=False):
    """Import/export configuration options."""
    # Core
    record_type: str  # event, user, group, export, profile-export, profile-delete, group-export, group-delete, export-import-event, export-import-profile, export-import-group
    region: str  # US, EU, IN
    vendor: str  # amplitude, heap, ga4, posthog, mparticle, june, mixpanel
    vendor_opts: dict
    stream_format: str  # jsonl, json, csv, parquet

    # Performance
    workers: int
    records_per_batch: int
    bytes_per_batch: int
    max_retries: int
    compress: bool
    compression_level: int

    # Data transforms
    transform_func: Callable
    fix_data: bool
    fix_time: bool
    fix_json: bool
    remove_nulls: bool
    flatten: bool
    add_token: bool
    time_offset: int
    tags: dict
    aliases: dict
    directive: str  # $set, $set_once, $add, $union, $append, $remove, $unset
    dimension_maps: list
    insert_id_tuple: list[str]
    drop_columns: list[str]
    scrub_props: list[str]
    v2_compat: bool
    create_profiles: bool

    # Filtering
    dedupe: bool
    epoch_start: int
    epoch_end: int
    max_records: int | None
    event_whitelist: list[str]
    event_blacklist: list[str]
    prop_key_whitelist: list[str]
    prop_key_blacklist: list[str]
    prop_val_whitelist: list[str]
    prop_val_blacklist: list[str]
    combo_white_list: dict[str, list[str]]
    combo_black_list: dict[str, list[str]]

    # Export
    start: str
    end: str
    where: str
    limit: int
    cohort_id: int
    params: dict

    # Output
    strict: bool
    verbose: bool
    dry_run: bool
    write_to_file: bool
    logs: bool
    abridged: bool
    is_gzip: bool
    output_file_path: str
    progress_callback: Callable

    # Cloud storage (can also go in Creds)
    gcp_project_id: str
    gcs_credentials: str
    s3_key: str
    s3_secret: str
    s3_region: str

    # Group analytics
    group_key: str
    data_group_id: str

    # Advanced
    heavy_objects: dict


class ImportResults(TypedDict, total=False):
    """Results returned by mp_import() and streaming methods."""
    record_type: str
    total: int
    success: int
    failed: int
    empty: int
    out_of_bounds: int
    duplicates: int
    whitelist_skipped: int
    blacklist_skipped: int
    unparsable: int
    start_time: str
    end_time: str
    duration: int
    duration_human: str
    bytes: int
    bytes_human: str
    requests: int
    batches: int
    retries: int
    rate_limit: int
    server_errors: int
    client_errors: int
    eps: float
    rps: float
    mbps: float
    errors: dict[str, int]
    responses: list
    dry_run: list
    vendor: str
    vendor_opts: dict
