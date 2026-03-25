"""Streaming pipeline for high-throughput Mixpanel data import/export.

Public API:
    mp_import(creds, data, options) — main async import function
    MpStream — push-based streaming interface
    validate_token(token) — validate a Mixpanel project token
    StreamInterface — async interface attached to MixpanelUtils instances
"""

from .core.job import Job
from .core.pipeline import core_pipeline
from .io.parsers import resolve_input
from .io.http_client import MixpanelHttpClient

__version__ = "1.0.0"


async def mp_import(
    creds: dict | None = None,
    data=None,
    options: dict | None = None,
) -> dict:
    """Stream events, users, and groups to Mixpanel.

    Args:
        creds: Mixpanel credentials (token, acct/pass/project, or secret).
        data: File path, list of dicts, async iterator, or cloud URL (gs://, s3://).
        options: Import configuration options.

    Returns:
        ImportResults dict with success/failure counts and statistics.
    """
    options = options or {}
    job = Job(creds, options)
    await job.init()

    # Handle export/delete record types
    rt = job.record_type
    if rt in ("export", "export-import-event"):
        from .io.exporters import export_events
        output_file = data or job.output_file_path
        if rt == "export-import-event":
            # Export then re-import: export to memory, then import
            export_result = await export_events("", job)
            if isinstance(export_result, list) and export_result:
                import_opts = dict(options)
                import_opts["record_type"] = "event"
                import_opts["vendor"] = "mixpanel"
                import_creds = dict(creds or {})
                if job.second_token:
                    import_creds["token"] = job.second_token
                return await mp_import(import_creds, export_result, import_opts)
            return job.summary()
        else:
            await export_events(output_file if isinstance(output_file, str) else "", job)
            return job.summary()

    if rt in ("profile-export", "group-export"):
        from .io.exporters import export_profiles
        output_dir = data or "./mixpanel-exports"
        await export_profiles(output_dir if isinstance(output_dir, str) else "", job)
        return job.summary()

    if rt in ("profile-delete", "group-delete"):
        from .io.exporters import delete_profiles
        return await delete_profiles(job)

    if rt == "export-import-profile":
        from .io.exporters import export_profiles
        export_result = await export_profiles("", job)
        if export_result:
            import_opts = dict(options)
            import_opts["record_type"] = "user"
            import_creds = dict(creds or {})
            if job.second_token:
                import_creds["token"] = job.second_token
            return await mp_import(import_creds, export_result, import_opts)
        return job.summary()

    http_client = MixpanelHttpClient()
    try:
        source = await resolve_input(data, job)
        result = await core_pipeline(source, job, http_client)
        return result
    finally:
        await http_client.close()


async def validate_token(token: str) -> dict:
    """Validate a Mixpanel project token and detect ID management version."""
    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.mixpanel.com/track",
            json=[{
                "event": "$mp_web_page_view",
                "properties": {"token": token, "distinct_id": "token_validation_test"}
            }],
        )
        try:
            data = resp.json()
            valid = data.get("status") == 1 or resp.status_code == 200
        except Exception:
            valid = False

        return {"token": token, "valid": valid}


class MpStream:
    """Push-based streaming interface for piping data into Mixpanel.

    Example:
        stream = await MpStream.create(creds, options)
        for record in my_data:
            await stream.push(record)
        result = await stream.flush()
    """

    def __init__(self, job: Job, http_client: MixpanelHttpClient):
        self._job = job
        self._http_client = http_client
        self._buffer: list[dict] = []

    @classmethod
    async def create(cls, creds: dict, options: dict | None = None) -> "MpStream":
        options = options or {}
        job = Job(creds, options)
        await job.init()
        http_client = MixpanelHttpClient()
        return cls(job, http_client)

    async def push(self, record: dict) -> None:
        self._buffer.append(record)

    async def flush(self) -> dict:
        try:
            source = _iter_list(self._buffer)
            result = await core_pipeline(source, self._job, self._http_client)
            return result
        finally:
            await self._http_client.close()


async def _iter_list(data: list):
    for item in data:
        yield item


class StreamInterface:
    """Async streaming interface attached to a MixpanelUtils instance.

    Provides import, export, and round-trip methods that inherit
    credentials from the parent MixpanelUtils object.
    """

    def __init__(self, parent):
        self._parent = parent

    def _build_creds(self) -> dict:
        """Build credentials dict from parent MixpanelUtils instance."""
        creds = {
            "acct": self._parent.service_account_username,
            "pass": self._parent.service_account_password,
            "project": self._parent.project_id,
        }
        if self._parent.token:
            creds["token"] = self._parent.token
        if getattr(self._parent, "data_group_id", None):
            creds["data_group_id"] = self._parent.data_group_id
        if getattr(self._parent, "group_key", None):
            creds["group_key"] = self._parent.group_key
        return creds

    def _region(self) -> str:
        """Get region string from parent, normalized to uppercase."""
        return (getattr(self._parent, "residency", "US") or "US").upper()

    def _merge_opts(self, options: dict | None, **defaults) -> dict:
        """Merge user options with defaults and region."""
        opts = {"region": self._region()}
        opts.update(defaults)
        if options:
            opts.update(options)
        return opts

    # ── Import Methods ───────────────────────────────────────────────

    async def import_events(self, data, options: dict | None = None) -> dict:
        """Import events to Mixpanel.

        Args:
            data: File path, list of dicts, async iterator, or cloud URL.
            options: Import options (fix_data, vendor, transforms, etc.)
        """
        opts = self._merge_opts(options, record_type="event")
        return await mp_import(self._build_creds(), data, opts)

    async def import_people(self, data, options: dict | None = None) -> dict:
        """Import user profiles to Mixpanel."""
        opts = self._merge_opts(options, record_type="user")
        return await mp_import(self._build_creds(), data, opts)

    async def import_groups(self, data, options: dict | None = None) -> dict:
        """Import group profiles to Mixpanel."""
        opts = self._merge_opts(options, record_type="group")
        return await mp_import(self._build_creds(), data, opts)

    # ── Export Methods ───────────────────────────────────────────────

    async def export_events(self, filename: str | None = None, options: dict | None = None) -> dict:
        """Export events from Mixpanel.

        Args:
            filename: Output file path. None to return records in memory.
            options: Export options (start, end, where, limit, etc.)
        """
        opts = self._merge_opts(options, record_type="export")
        return await mp_import(self._build_creds(), filename, opts)

    async def export_people(self, folder: str | None = None, options: dict | None = None) -> dict:
        """Export user profiles from Mixpanel."""
        opts = self._merge_opts(options, record_type="profile-export")
        return await mp_import(self._build_creds(), folder, opts)

    async def export_groups(self, folder: str | None = None, options: dict | None = None) -> dict:
        """Export group profiles from Mixpanel."""
        creds = self._build_creds()
        opts = self._merge_opts(options, record_type="group-export")
        if getattr(self._parent, "data_group_id", None):
            opts.setdefault("data_group_id", self._parent.data_group_id)
        return await mp_import(creds, folder, opts)

    # ── Export + Import (Round-Trip) Methods ──────────────────────────

    async def export_import_events(self, options: dict | None = None) -> dict:
        """Export events from this project and re-import them.

        Use 'second_token' in options to import into a different project.
        """
        opts = self._merge_opts(options, record_type="export-import-event")
        return await mp_import(self._build_creds(), None, opts)

    async def export_import_people(self, options: dict | None = None) -> dict:
        """Export user profiles and re-import them."""
        opts = self._merge_opts(options, record_type="export-import-profile")
        return await mp_import(self._build_creds(), None, opts)

    async def export_import_groups(self, options: dict | None = None) -> dict:
        """Export group profiles and re-import them."""
        creds = self._build_creds()
        opts = self._merge_opts(options, record_type="export-import-profile")
        if getattr(self._parent, "data_group_id", None):
            opts.setdefault("data_group_id", self._parent.data_group_id)
        return await mp_import(creds, None, opts)
