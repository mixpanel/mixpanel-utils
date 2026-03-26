"""Central state management class for import/export jobs."""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from ..constants import (
    BASE_URLS, EXPORT_BASE_URLS, ENGAGE_BASE_URLS,
    EXPORT_RECORD_TYPES, ENGAGE_RECORD_TYPES, GZIP_RECORD_TYPES,
    HTTP_METHODS, VALID_OPERATIONS, MAX_RECORDS_PER_BATCH,
    MAX_BYTES_PER_BATCH, DEFAULT_WORKERS, DEFAULT_MAX_RETRIES,
    DEFAULT_COMPRESSION_LEVEL, DEFAULT_EPOCH_START, DEFAULT_EPOCH_END,
)
from ..utils import bytes_human, comma, Timer


def _parse_json_option(val, default=None):
    """Parse a value that might be a JSON string, or return as-is."""
    if default is None:
        default = []
    if val is None:
        return default
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            try:
                return json.loads(val.replace("'", '"'))
            except json.JSONDecodeError:
                return default
    return val


async def _build_map_from_path(file_path: str, key_one: str, key_two: str) -> dict:
    """Load a JSON/JSONL file and build a key→value lookup dict."""
    import aiofiles
    from pathlib import Path

    records = []
    p = Path(file_path)
    if not p.exists():
        return {}

    async with aiofiles.open(file_path, "r") as f:
        content = await f.read()

    content = content.strip()
    if not content:
        return {}

    # Try JSON array first, then JSONL
    try:
        data = json.loads(content)
        if isinstance(data, list):
            records = data
        else:
            records = [data]
    except json.JSONDecodeError:
        for line in content.split("\n"):
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    return {r[key_one]: r[key_two] for r in records if key_one in r and key_two in r}


def _get_term_width() -> int:
    try:
        return os.get_terminal_size().columns
    except (ValueError, OSError, AttributeError):
        return 120


def _default_progress(job):
    """Overwrite current terminal line with live import stats."""
    elapsed = job.timer.elapsed()
    if elapsed <= 0:
        return
    job._progress_written = True
    hrs, rem = divmod(int(elapsed), 3600)
    mins, secs = divmod(rem, 60)
    time_str = f"{hrs:02d}:{mins:02d}:{secs:02d}"
    eps = int(job.success / elapsed) if elapsed > 0 else 0
    line = (
        f"  {job.records_processed:,} records | "
        f"{job.success:,} ok | "
        f"{job.failed:,} err | "
        f"{job.requests:,} req | "
        f"{eps:,} eps | "
        f"{time_str}"
    )
    if job.errors:
        top_err = max(job.errors, key=job.errors.get)
        line += f" | {top_err}"
    w = _get_term_width()
    print("\r" + line[:w].ljust(w), end="", flush=True)


class Job:
    """Central state holder for an import/export job.

    Holds credentials, configuration, runtime counters, and transform functions.
    """

    def __init__(self, creds: dict | None = None, opts: dict | None = None):
        opts = opts or {}
        allow_empty = opts.get("dry_run") or opts.get("write_to_file") or opts.get("fix_data")
        if not creds and not allow_empty:
            raise ValueError("No credentials provided")
        creds = creds or {}

        # ── Credentials ──────────────────────────────────────────────
        self.acct: str = creds.get("acct", "")
        self.pass_: str = creds.get("pass", creds.get("pass_", ""))
        self.project: str = str(creds["project"]) if creds.get("project") else ""
        self.workspace: str = str(creds["workspace"]) if creds.get("workspace") else ""
        self.org: str = str(creds["org"]) if creds.get("org") else ""
        self.secret: str = creds.get("secret", "")
        self.bearer: str = creds.get("bearer", "")
        self.token: str = creds.get("token", "")
        self.second_token: str = creds.get("second_token", creds.get("secondToken", ""))
        self.group_key: str = creds.get("group_key", creds.get("groupKey", "")) or (str(opts.get("group_key", "")) if opts.get("group_key") else "")
        self.data_group_id: str = creds.get("data_group_id", creds.get("dataGroupId", "")) or str(opts.get("data_group_id", opts.get("dataGroupId", "")))

        # Cloud storage credentials
        self.gcp_project_id: str = opts.get("gcp_project_id") or creds.get("gcp_project_id", creds.get("gcpProjectId", ""))
        self.gcs_credentials: str = opts.get("gcs_credentials") or creds.get("gcs_credentials", creds.get("gcsCredentials", ""))
        self.s3_key: str = opts.get("s3_key") or creds.get("s3_key", creds.get("s3Key", ""))
        self.s3_secret: str = opts.get("s3_secret") or creds.get("s3_secret", creds.get("s3Secret", ""))
        self.s3_region: str = opts.get("s3_region") or creds.get("s3_region", creds.get("s3Region", ""))

        # ── Core Config ──────────────────────────────────────────────
        self.record_type: str = opts.get("record_type", opts.get("recordType", "event"))
        self.stream_format: str = opts.get("stream_format", opts.get("streamFormat", ""))
        self.region: str = opts.get("region", "US").upper()
        self.vendor: str = opts.get("vendor", "")
        self.vendor_opts: dict = _parse_json_option(opts.get("vendor_opts", opts.get("vendorOpts")), {})

        # ── Performance ──────────────────────────────────────────────
        self.records_per_batch: int = opts.get("records_per_batch", opts.get("recordsPerBatch", MAX_RECORDS_PER_BATCH))
        self.bytes_per_batch: int = opts.get("bytes_per_batch", opts.get("bytesPerBatch", MAX_BYTES_PER_BATCH))
        self.max_retries: int = opts.get("max_retries", opts.get("maxRetries", DEFAULT_MAX_RETRIES))
        self.workers: int = opts.get("workers", opts.get("concurrency", DEFAULT_WORKERS))
        self.compression_level: int = opts.get("compression_level", opts.get("compressionLevel", DEFAULT_COMPRESSION_LEVEL))

        # Clamp batch sizes
        if self.records_per_batch > MAX_RECORDS_PER_BATCH:
            self.records_per_batch = MAX_RECORDS_PER_BATCH

        # ── Boolean Options ──────────────────────────────────────────
        self.compress: bool = opts.get("compress", True)
        self.strict: bool = opts.get("strict", True)
        self.verbose: bool = opts.get("verbose", False)
        self.fix_data: bool = opts.get("fix_data", opts.get("fixData", False))
        self.fix_time: bool = opts.get("fix_time", opts.get("fixTime", False))
        self.fix_json: bool = opts.get("fix_json", opts.get("fixJson", False))
        self.remove_nulls: bool = opts.get("remove_nulls", opts.get("removeNulls", False))
        self.flatten_data: bool = opts.get("flatten", opts.get("flattenData", False))
        self.dedupe: bool = opts.get("dedupe", False)
        self.dry_run: bool = opts.get("dry_run", opts.get("dryRun", False))
        self.logs: bool = opts.get("logs", False)
        self.abridged: bool = opts.get("abridged", False)
        self.add_token: bool = opts.get("add_token", opts.get("addToken", False))
        self.is_gzip: bool = opts.get("is_gzip", opts.get("isGzip", False))
        self.write_to_file: bool = opts.get("write_to_file", opts.get("writeToFile", False))
        self.v2_compat: bool = opts.get("v2_compat", False)
        self.create_profiles: bool = opts.get("create_profiles", opts.get("createProfiles", False))

        # ── Numeric Options ──────────────────────────────────────────
        self.time_offset: int = opts.get("time_offset", opts.get("timeOffset", 0))
        self.epoch_start: int = opts.get("epoch_start", opts.get("epochStart", DEFAULT_EPOCH_START))
        self.epoch_end: int = opts.get("epoch_end", opts.get("epochEnd", DEFAULT_EPOCH_END))
        self.max_records: int | None = opts.get("max_records", opts.get("maxRecords", None))

        # ── String / JSON Options ────────────────────────────────────
        self.directive: str = opts.get("directive", "$set")
        self.tags: dict = _parse_json_option(opts.get("tags"), {})
        self.aliases: dict = _parse_json_option(opts.get("aliases"), {})
        self.scrub_props: list = _parse_json_option(opts.get("scrub_props", opts.get("scrubProps")), [])
        self.drop_columns: list = _parse_json_option(opts.get("drop_columns", opts.get("dropColumns")), [])

        # ── Whitelist/Blacklist ───────────────────────────────────────
        self.event_whitelist: list = _parse_json_option(opts.get("event_whitelist", opts.get("eventWhitelist")), [])
        self.event_blacklist: list = _parse_json_option(opts.get("event_blacklist", opts.get("eventBlacklist")), [])
        self.prop_key_whitelist: list = _parse_json_option(opts.get("prop_key_whitelist", opts.get("propKeyWhitelist")), [])
        self.prop_key_blacklist: list = _parse_json_option(opts.get("prop_key_blacklist", opts.get("propKeyBlacklist")), [])
        self.prop_val_whitelist: list = _parse_json_option(opts.get("prop_val_whitelist", opts.get("propValWhitelist")), [])
        self.prop_val_blacklist: list = _parse_json_option(opts.get("prop_val_blacklist", opts.get("propValBlacklist")), [])
        self.combo_white_list: dict = _parse_json_option(opts.get("combo_white_list", opts.get("comboWhiteList")), {})
        self.combo_black_list: dict = _parse_json_option(opts.get("combo_black_list", opts.get("comboBlackList")), {})

        # ── Transform Functions ──────────────────────────────────────
        self.transform_func: Callable | None = opts.get("transform_func", opts.get("transformFunc"))
        self.vendor_transform: Callable | None = None
        self.heavy_objects: dict = opts.get("heavy_objects", opts.get("heavyObjects", {}))
        self.dimension_maps: list = _parse_json_option(opts.get("dimension_maps", opts.get("dimensionMaps")), [])
        self.insert_id_tuple: list = opts.get("insert_id_tuple", opts.get("insertIdTuple", []))

        # ── Export Options ───────────────────────────────────────────
        today = datetime.utcnow()
        start_default = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end_default = today.strftime("%Y-%m-%d")
        self.start: str = opts.get("start", start_default)
        self.end: str = opts.get("end", end_default)
        self.where_clause: str = opts.get("where", opts.get("whereClause", ""))
        self.limit: int | None = opts.get("limit")
        self.cohort_id: int | None = opts.get("cohort_id", opts.get("cohortId"))
        self.params: dict = opts.get("params", {})

        # ── Output Options ───────────────────────────────────────────
        self.output_file_path: str = opts.get("output_file_path", "./mixpanel-transform.json")
        self.where_dir: str | None = opts.get("where")
        self.progress_callback: Callable | None = opts.get("progress_callback")
        if self.verbose and not self.progress_callback and not self.dry_run and not self.write_to_file:
            self.progress_callback = _default_progress

        # ── Runtime State ────────────────────────────────────────────
        self.start_time: str = datetime.utcnow().isoformat() + "Z"
        self.end_time: str | None = None
        self.hash_table: set = set()
        self.dry_run_results: list = []

        # Counters
        self.records_processed: int = 0
        self.success: int = 0
        self.failed: int = 0
        self.retries: int = 0
        self.batches: int = 0
        self.requests: int = 0
        self.empty: int = 0
        self.rate_limited: int = 0
        self.server_errors: int = 0
        self.client_errors: int = 0
        self.bytes_processed: int = 0
        self.out_of_bounds: int = 0
        self.duplicates: int = 0
        self.whitelist_skipped: int = 0
        self.blacklist_skipped: int = 0
        self.unparsable: int = 0
        self.last_batch_length: int = 0

        self.responses: list = []
        self.errors: dict[str, int] = {}

        # Timer
        self.timer = Timer()
        self.timer.start()

        # ── Request Config ───────────────────────────────────────────
        self.req_method: str = HTTP_METHODS.get(self.record_type, "POST")
        self.content_type: str = "application/json"

        # Validate record type
        _validate_record_type(self.record_type)

        # Resolve auth
        self.auth: str = self._resolve_auth()

        # Build active transforms list (done after all options are set)
        self.active_transforms: list[Callable] = []
        self._build_transforms()

    # ── Properties ───────────────────────────────────────────────────

    @property
    def url(self) -> str:
        """Get the Mixpanel API URL for current record type and region."""
        region = self.region.upper()
        rt = self.record_type.lower()

        if rt in EXPORT_RECORD_TYPES:
            base = EXPORT_BASE_URLS.get(region, EXPORT_BASE_URLS["US"])
            return f"{base}/api/2.0/export"
        elif rt in ENGAGE_RECORD_TYPES:
            base = ENGAGE_BASE_URLS.get(region, ENGAGE_BASE_URLS["US"])
            return f"{base}/api/2.0/engage"
        else:
            base = BASE_URLS.get(region, BASE_URLS["US"])
            path_map = {
                "event": "/import",
                "user": "/engage",
                "group": "/groups",
            }
            path = path_map.get(rt, "/import")
            return f"{base}{path}"

    # ── Auth ─────────────────────────────────────────────────────────

    def _resolve_auth(self) -> str:
        """Resolve authentication header value."""
        is_import = self.record_type in ("event", "user", "group")

        # Service account auth (preferred)
        if self.acct and self.pass_ and self.project:
            return "Basic " + base64.b64encode(f"{self.acct}:{self.pass_}".encode()).decode()

        # Token auth for imports
        if is_import and self.token:
            return "Basic " + base64.b64encode(f"{self.token}:".encode()).decode()

        # Validate export with service account needs project
        export_types = {"export", "profile-export", "group-export", "profile-delete",
                        "export-import-event", "export-import-profile"}
        if self.record_type in export_types and self.acct and self.pass_ and not self.project:
            raise ValueError("Export with service account auth requires project_id")

        # API secret auth
        if self.secret:
            return "Basic " + base64.b64encode(f"{self.secret}:".encode()).decode()

        # Fallback token for non-imports
        if self.token and not is_import:
            return "Basic " + base64.b64encode(f"{self.token}:".encode()).decode()

        # Bearer token
        if self.bearer:
            return f"Bearer {self.bearer}"

        # Allow empty auth for dry runs, profiles without token, etc.
        if self.dry_run or self.write_to_file or self.record_type in ("user", "group"):
            return ""

        if "export" in self.record_type:
            raise ValueError("Export operations require API secret or service account (acct + pass + project)")

        raise ValueError("No valid authentication method found")

    # ── Transforms ───────────────────────────────────────────────────

    def _build_transforms(self):
        """Pre-compute the list of active helper transforms."""
        from ..transforms.builtin import (
            ez_transforms, apply_aliases, add_tags, add_token,
            remove_nulls, flatten_properties, utc_offset,
            fix_time, fix_json, scrub_properties, drop_columns,
            set_distinct_id_from_v2_props, add_insert,
        )
        from ..transforms.filters import whitelist_blacklist, epoch_filter
        from ..transforms.dedup import dedupe_records

        transforms = []

        if self.aliases:
            transforms.append(apply_aliases(self))
        if self.fix_data or "export-import" in self.record_type:
            transforms.append(ez_transforms(self))
        if self.v2_compat and self.record_type == "event":
            transforms.append(set_distinct_id_from_v2_props())
        if self.remove_nulls:
            transforms.append(remove_nulls())
        if self.time_offset:
            transforms.append(utc_offset(self.time_offset))
        if self.tags:
            transforms.append(add_tags(self))

        # Check for whitelist/blacklist
        wb_params = {
            "event_whitelist": self.event_whitelist,
            "event_blacklist": self.event_blacklist,
            "prop_key_whitelist": self.prop_key_whitelist,
            "prop_key_blacklist": self.prop_key_blacklist,
            "prop_val_whitelist": self.prop_val_whitelist,
            "prop_val_blacklist": self.prop_val_blacklist,
            "combo_white_list": self.combo_white_list,
            "combo_black_list": self.combo_black_list,
        }
        has_wb = any(
            (isinstance(v, list) and len(v) > 0) or (isinstance(v, dict) and len(v) > 0)
            for v in wb_params.values()
        )
        if has_wb:
            transforms.append(whitelist_blacklist(self, wb_params))

        if self.epoch_start or self.epoch_end != DEFAULT_EPOCH_END:
            transforms.append(epoch_filter(self))
        if self.scrub_props:
            transforms.append(scrub_properties(self.scrub_props))
        if self.drop_columns:
            transforms.append(drop_columns(self.drop_columns))
        if self.flatten_data:
            transforms.append(flatten_properties())
        if self.fix_json:
            transforms.append(fix_json())
        if self.insert_id_tuple and self.record_type == "event":
            transforms.append(add_insert(self.insert_id_tuple))
        if self.add_token:
            transforms.append(add_token(self))
        if self.fix_time and self.record_type == "event":
            transforms.append(fix_time())

        self.active_transforms = transforms

        # Dedup transform (separate, used in pipeline directly)
        if self.dedupe:
            self.deduper = dedupe_records(self)
        else:
            self.deduper = None

    async def insert_heavy_objects(self):
        """Load dimension maps from files into heavy_objects dict."""
        if self.heavy_objects or not self.dimension_maps:
            return
        for dim_map in self.dimension_maps:
            file_path = dim_map.get("filePath") or dim_map.get("file_path")
            key_one = dim_map.get("keyOne") or dim_map.get("key_one")
            key_two = dim_map.get("keyTwo") or dim_map.get("key_two")
            label = dim_map.get("label", f"map_{len(self.heavy_objects)}")

            if not file_path or not key_one or not key_two:
                continue

            id_map = await _build_map_from_path(file_path, key_one, key_two)
            self.heavy_objects[label] = id_map

    async def init(self):
        """Initialize vendor transforms and heavy objects."""
        await self.insert_heavy_objects()
        if self.vendor:
            self._init_vendor_transform()

    def _init_vendor_transform(self):
        """Set up vendor-specific transform function."""
        from ..transforms.dedup import dedupe_records

        vendor = self.vendor.lower()
        rt = self.record_type.lower()

        if vendor == "amplitude":
            from ..vendors.amplitude import amp_events_to_mp, amp_user_to_mp, amp_group_to_mp
            if rt == "event":
                self.vendor_transform = amp_events_to_mp(self.vendor_opts)
            elif rt == "user":
                self.dedupe = True
                self.deduper = dedupe_records(self)
                self.vendor_transform = amp_user_to_mp(self.vendor_opts)
            elif rt == "group":
                self.vendor_transform = amp_group_to_mp(self.vendor_opts)
            else:
                self.vendor_transform = amp_events_to_mp(self.vendor_opts)

        elif vendor == "heap":
            from ..vendors.heap import heap_events_to_mp, heap_user_to_mp, heap_group_to_mp
            if rt == "event":
                self.vendor_transform = heap_events_to_mp(self.vendor_opts)
            elif rt == "user":
                self.vendor_transform = heap_user_to_mp(self.vendor_opts)
            elif rt == "group":
                self.vendor_transform = heap_group_to_mp(self.vendor_opts)
            else:
                self.vendor_transform = heap_events_to_mp(self.vendor_opts)

        elif vendor == "ga4":
            from ..vendors.ga4 import ga_events_to_mp, ga_user_to_mp, ga_groups_to_mp
            if rt == "event":
                self.vendor_transform = ga_events_to_mp(self.vendor_opts)
            elif rt == "user":
                self.dedupe = True
                self.deduper = dedupe_records(self)
                self.vendor_transform = ga_user_to_mp(self.vendor_opts)
            elif rt == "group":
                self.vendor_transform = ga_groups_to_mp(self.vendor_opts)
            else:
                self.vendor_transform = ga_events_to_mp(self.vendor_opts)

        elif vendor == "mparticle":
            from ..vendors.mparticle import mparticle_events_to_mixpanel, mparticle_user_to_mixpanel, mparticle_group_to_mixpanel
            if rt == "event":
                self.vendor_transform = mparticle_events_to_mixpanel(self.vendor_opts)
            elif rt == "user":
                self.dedupe = True
                self.deduper = dedupe_records(self)
                self.vendor_transform = mparticle_user_to_mixpanel(self.vendor_opts)
            elif rt == "group":
                self.vendor_transform = mparticle_group_to_mixpanel(self.vendor_opts)
            else:
                self.vendor_transform = mparticle_events_to_mixpanel(self.vendor_opts)

        elif vendor == "posthog":
            from ..vendors.posthog import posthog_events_to_mp, posthog_person_to_mp_profile
            if rt == "event":
                self.vendor_transform = posthog_events_to_mp(self.vendor_opts, self.heavy_objects)
            elif rt == "user":
                self.dedupe = True
                self.deduper = dedupe_records(self)
                self.vendor_transform = posthog_person_to_mp_profile(self.vendor_opts)
            elif rt == "group":
                raise ValueError("PostHog does not support group transforms")
            else:
                self.vendor_transform = posthog_events_to_mp(self.vendor_opts, self.heavy_objects)

        elif vendor == "june":
            from ..vendors.june import june_events_to_mp, june_user_to_mp, june_group_to_mp
            if rt == "event":
                self.vendor_transform = june_events_to_mp(self.vendor_opts)
            elif rt == "user":
                self.dedupe = True
                self.deduper = dedupe_records(self)
                self.vendor_transform = june_user_to_mp(self.vendor_opts)
            elif rt == "group":
                self.vendor_transform = june_group_to_mp(self.vendor_opts)
            else:
                self.vendor_transform = june_events_to_mp(self.vendor_opts)

        elif vendor == "mixpanel":
            from ..vendors.mixpanel import mixpanel_events_to_mixpanel
            self.vendor_transform = mixpanel_events_to_mixpanel(self.vendor_opts)

    # ── Response Handling ────────────────────────────────────────────

    def store(self, response: Any, success: bool = True, batch: list | None = None):
        """Store API response and update error counts."""
        if not self.abridged and response is not None:
            self.responses.append(response)

        if not success:
            if response and isinstance(response, dict):
                if "failed_records" in response and isinstance(response["failed_records"], list):
                    for failure in response["failed_records"]:
                        msg = failure.get("message", "unknown error")
                        self.errors[msg] = self.errors.get(msg, 0) + 1
                elif response.get("error"):
                    msg = response["error"]
                    self.errors[msg] = self.errors.get(msg, 0) + 1
                elif response.get("message"):
                    msg = response["message"]
                    self.errors[msg] = self.errors.get(msg, 0) + 1
                else:
                    self.errors["unknown error"] = self.errors.get("unknown error", 0) + 1
            elif isinstance(response, str):
                self.errors[response] = self.errors.get(response, 0) + 1
            else:
                self.errors["unknown error"] = self.errors.get("unknown error", 0) + 1

    # ── Summary ──────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Generate final import/export results summary."""
        self.timer.stop()
        report = self.timer.report()
        delta = report["delta"]
        human = report["human"]

        result = {
            "record_type": self.record_type,
            "total": self.records_processed,
            "success": self.success,
            "failed": self.failed,
            "empty": self.empty,
            "out_of_bounds": self.out_of_bounds,
            "duplicates": self.duplicates,
            "whitelist_skipped": self.whitelist_skipped,
            "blacklist_skipped": self.blacklist_skipped,
            "unparsable": self.unparsable,
            "start_time": self.start_time,
            "end_time": datetime.utcnow().isoformat() + "Z",
            "duration": delta,
            "duration_human": human,
            "bytes": self.bytes_processed,
            "bytes_human": bytes_human(self.bytes_processed),
            "requests": self.requests,
            "batches": self.batches,
            "retries": self.retries,
            "rate_limit": self.rate_limited,
            "server_errors": self.server_errors,
            "client_errors": self.client_errors,
            "eps": 0,
            "rps": 0.0,
            "mbps": 0.0,
            "errors": self.errors,
            "responses": self.responses,
            "dry_run": self.dry_run_results,
            "vendor": self.vendor or "",
            "vendor_opts": self.vendor_opts,
        }

        # Calculate rates
        if result["total"] and delta and result["requests"] and result["bytes"]:
            duration_s = delta / 1000
            result["eps"] = int(result["total"] / duration_s)
            result["rps"] = round(result["requests"] / duration_s, 3)
            result["mbps"] = round((result["bytes"] / 1e6) / duration_s, 3)

        return result


def _validate_record_type(rt: str):
    """Validate record type, rejecting plural forms."""
    plural_map = {"events": "event", "users": "user", "groups": "group"}
    if rt in plural_map:
        raise ValueError(
            f'Invalid record_type: "{rt}" (plural form not allowed). '
            f'Use the singular form: "{plural_map[rt]}"'
        )
    valid_prefixes = ["event", "user", "group", "export",
                      "profile-", "export-import-"]
    if rt and not any(rt.startswith(p) or rt == p for p in valid_prefixes):
        raise ValueError(f'Invalid record_type: "{rt}"')
