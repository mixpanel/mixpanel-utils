"""Type definitions for the streaming pipeline."""

from enum import Enum
from typing import TypedDict, Any, Callable, Optional


class RecordType(str, Enum):
    EVENT = "event"
    USER = "user"
    GROUP = "group"
    EXPORT = "export"
    PROFILE_EXPORT = "profile-export"
    PROFILE_DELETE = "profile-delete"
    GROUP_EXPORT = "group-export"
    GROUP_DELETE = "group-delete"
    EXPORT_IMPORT_EVENT = "export-import-event"
    EXPORT_IMPORT_PROFILE = "export-import-profile"
    EXPORT_IMPORT_GROUP = "export-import-group"


class Region(str, Enum):
    US = "US"
    EU = "EU"
    IN = "IN"


class Vendor(str, Enum):
    AMPLITUDE = "amplitude"
    HEAP = "heap"
    GA4 = "ga4"
    POSTHOG = "posthog"
    MPARTICLE = "mparticle"
    JUNE = "june"
    MIXPANEL = "mixpanel"


class Creds(TypedDict, total=False):
    acct: str
    pass_: str
    project: int | str
    token: str
    secret: str
    bearer: str
    group_key: str
    workspace: int | str
    org: int | str
    second_token: str
    gcp_project_id: str
    gcs_credentials: str
    s3_key: str
    s3_secret: str
    s3_region: str
    data_group_id: str


class Options(TypedDict, total=False):
    # Core
    record_type: str
    region: str
    stream_format: str

    # Performance
    workers: int
    concurrency: int
    records_per_batch: int
    bytes_per_batch: int
    max_retries: int
    high_water: int

    # Compression
    compress: bool
    compression_level: int
    is_gzip: bool

    # Transforms
    transform_func: Callable
    fix_data: bool
    fix_time: bool
    remove_nulls: bool
    time_offset: int
    tags: dict
    aliases: dict
    flatten: bool
    fix_json: bool
    add_token: bool
    directive: str

    # Vendor
    vendor: str
    vendor_opts: dict

    # Filtering
    dedupe: bool
    epoch_start: int
    epoch_end: int
    event_whitelist: list[str]
    event_blacklist: list[str]
    prop_key_whitelist: list[str]
    prop_key_blacklist: list[str]
    prop_val_whitelist: list[str]
    prop_val_blacklist: list[str]
    combo_white_list: dict[str, list[str]]
    combo_black_list: dict[str, list[str]]
    scrub_props: list[str]

    # Validation
    strict: bool

    # Export
    start: str
    end: str
    where: str
    limit: int
    cohort_id: int

    # Output
    verbose: bool
    dry_run: bool
    write_to_file: bool
    logs: bool
    abridged: bool

    # Insert ID
    insert_id_tuple: list[str]

    # Max records
    max_records: int | None

    # Heavy objects (dimension maps, etc.)
    heavy_objects: dict


class ImportResults(TypedDict, total=False):
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
    files: list[str]
