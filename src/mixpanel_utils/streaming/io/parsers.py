"""Input detection and format-specific readers.

All readers return AsyncIterator[dict] for consistent pipeline consumption.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import logging
from pathlib import Path
from typing import AsyncIterator, Any

from ..constants import (
    JSONL_EXTENSIONS, JSON_EXTENSIONS, CSV_EXTENSIONS,
    PARQUET_EXTENSIONS, GZIP_EXTENSIONS,
)

logger = logging.getLogger(__name__)


def detect_format(path: Path, job=None) -> str:
    """Detect file format from extension."""
    suffixes = path.suffixes
    # Handle compound extensions like .json.gz
    is_gzipped = suffixes and suffixes[-1].lower() in GZIP_EXTENSIONS
    base_ext = suffixes[-2].lower() if is_gzipped and len(suffixes) > 1 else (suffixes[-1].lower() if suffixes else "")

    if job and job.stream_format:
        fmt = job.stream_format.lower()
        if fmt in ("jsonl", "ndjson"):
            return "jsonl"
        if fmt in ("json", "strict_json"):
            return "json"
        if fmt in ("csv", "tsv"):
            return "csv"
        if fmt == "parquet":
            return "parquet"

    if base_ext in JSONL_EXTENSIONS:
        return "jsonl"
    if base_ext in JSON_EXTENSIONS:
        return "json"
    if base_ext in CSV_EXTENSIONS:
        return "csv"
    if base_ext in PARQUET_EXTENSIONS:
        return "parquet"

    # Default to jsonl
    return "jsonl"


def is_gzipped(path: Path, job=None) -> bool:
    """Check if file is gzipped."""
    if job and job.is_gzip:
        return True
    suffixes = path.suffixes
    return bool(suffixes and suffixes[-1].lower() in GZIP_EXTENSIONS)


async def resolve_input(data: Any, job) -> AsyncIterator[dict]:
    """Detect data type and return an async iterator of records."""
    # List or tuple of dicts
    if isinstance(data, (list, tuple)):
        return _iter_list(data)

    # Async iterable
    if hasattr(data, "__aiter__"):
        return data

    # Sync iterable (generator, etc.) but not string
    if hasattr(data, "__iter__") and not isinstance(data, (str, bytes)):
        return _wrap_sync_iter(data)

    # String: file path, directory, glob, or cloud URL
    if isinstance(data, str):
        if data.startswith("gs://"):
            return _gcs_stream(data, job)
        if data.startswith("s3://"):
            return _s3_stream(data, job)

        path = Path(data)
        if path.exists():
            if path.is_dir():
                return _multi_file_stream(sorted(path.iterdir()), job)
            return _file_stream(path, job)

        # Try glob
        import glob as glob_mod
        files = sorted(glob_mod.glob(data))
        if files:
            return _multi_file_stream([Path(f) for f in files], job)

        # Try parsing as inline JSON
        return _parse_string_data(data, job)

    raise ValueError(f"Cannot determine data type for {type(data)}")


async def _iter_list(data: list) -> AsyncIterator[dict]:
    for item in data:
        yield item


async def _wrap_sync_iter(data) -> AsyncIterator[dict]:
    for item in data:
        yield item


async def _file_stream(path: Path, job) -> AsyncIterator[dict]:
    """Stream records from a single file."""
    fmt = detect_format(path, job)
    gz = is_gzipped(path, job)

    if fmt == "jsonl":
        async for record in _jsonl_reader(path, gz, job):
            yield record
    elif fmt == "json":
        async for record in _json_reader(path, gz, job):
            yield record
    elif fmt == "csv":
        async for record in _csv_reader(path, gz, job):
            yield record
    elif fmt == "parquet":
        async for record in _parquet_reader(path, job):
            yield record


async def _multi_file_stream(paths: list[Path], job) -> AsyncIterator[dict]:
    """Stream records from multiple files sequentially."""
    for path in paths:
        if path.is_file():
            async for record in _file_stream(path, job):
                yield record


def _open_file(path: Path, gzipped: bool):
    """Open a file, with optional gzip decompression."""
    if gzipped:
        return gzip.open(str(path), "rt", encoding="utf-8")
    return open(str(path), "r", encoding="utf-8")


async def _jsonl_reader(path: Path, gzipped: bool, job) -> AsyncIterator[dict]:
    """Read JSONL/NDJSON file line by line."""
    loop = asyncio.get_event_loop()

    def _read_lines():
        results = []
        with _open_file(path, gzipped) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        job.unparsable += 1
        return results

    records = await loop.run_in_executor(None, _read_lines)
    for record in records:
        yield record


async def _json_reader(path: Path, gzipped: bool, job) -> AsyncIterator[dict]:
    """Read a JSON array file. Falls back to JSONL if JSON array parsing fails."""
    loop = asyncio.get_event_loop()

    def _read_json():
        with _open_file(path, gzipped) as f:
            content = f.read()
        # Try as JSON array/object first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except json.JSONDecodeError:
            pass
        # Fall back to JSONL (one JSON object per line)
        results = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    results.append(json.loads(line))
                except json.JSONDecodeError:
                    job.unparsable += 1
        return results

    records = await loop.run_in_executor(None, _read_json)
    for record in records:
        yield record


async def _csv_reader(path: Path, gzipped: bool, job) -> AsyncIterator[dict]:
    """Read CSV file, yielding dicts with headers as keys."""
    loop = asyncio.get_event_loop()

    def _read_csv():
        results = []
        with _open_file(path, gzipped) as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Strip whitespace from keys
                cleaned = {k.strip(): v for k, v in row.items() if k}
                results.append(cleaned)
        return results

    records = await loop.run_in_executor(None, _read_csv)
    for record in records:
        yield record


async def _parquet_reader(path: Path, job) -> AsyncIterator[dict]:
    """Read Parquet file using pyarrow."""
    loop = asyncio.get_event_loop()

    def _read_parquet():
        import pyarrow.parquet as pq
        from datetime import date, datetime
        table = pq.read_table(str(path))
        results = []
        for batch in table.to_batches(max_chunksize=1000):
            for row in batch.to_pylist():
                record = {}
                for k, v in row.items():
                    if v is None:
                        continue
                    if isinstance(v, datetime):
                        record[k] = v.isoformat()
                    elif isinstance(v, date):
                        record[k] = v.isoformat()
                    else:
                        record[k] = v
                results.append(record)
        return results

    records = await loop.run_in_executor(None, _read_parquet)
    for record in records:
        yield record


async def _parse_string_data(data: str, job) -> AsyncIterator[dict]:
    """Try to parse a string as JSON or JSONL."""
    data = data.strip()
    # Try JSON array
    if data.startswith("["):
        try:
            items = json.loads(data)
            if isinstance(items, list):
                for item in items:
                    yield item
                return
        except json.JSONDecodeError:
            pass

    # Try JSON object
    if data.startswith("{"):
        try:
            item = json.loads(data)
            yield item
            return
        except json.JSONDecodeError:
            pass

    # Try JSONL (multiple lines)
    for line in data.split("\n"):
        line = line.strip()
        if line:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                job.unparsable += 1


# ── Cloud Storage ────────────────────────────────────────────────────

async def _gcs_stream(uri: str, job) -> AsyncIterator[dict]:
    """Stream records from Google Cloud Storage."""
    try:
        import gcsfs
    except ImportError:
        raise ImportError('Install gcsfs for GCS support: pip install "mixpanel-utils[streaming-gcs]"')

    # Parse gs://bucket/path
    parts = uri.replace("gs://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    fs = gcsfs.GCSFileSystem(
        project=job.gcp_project_id or None,
        token=job.gcs_credentials or "google_default",
    )

    path = Path(key)
    fmt = detect_format(path, job)
    gz = is_gzipped(path, job)

    loop = asyncio.get_event_loop()

    def _read():
        results = []

        # Parquet: read as binary, don't decode to text
        if fmt == "parquet":
            import pyarrow.parquet as pq
            from datetime import date, datetime as dt_cls
            with fs.open(uri, "rb") as f:
                table = pq.read_table(f)
            for batch in table.to_batches(max_chunksize=1000):
                for row in batch.to_pylist():
                    record = {}
                    for k, v in row.items():
                        if v is None:
                            continue
                        if isinstance(v, dt_cls):
                            record[k] = v.isoformat()
                        elif isinstance(v, date):
                            record[k] = v.isoformat()
                        else:
                            record[k] = v
                    results.append(record)
            return results

        with fs.open(uri, "rb") as f:
            if gz:
                raw = gzip.decompress(f.read())
                text = raw.decode("utf-8")
            else:
                raw = f.read()
                text = raw.decode("utf-8") if isinstance(raw, bytes) else raw

        if fmt == "jsonl":
            for line in text.strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        job.unparsable += 1
        elif fmt == "json":
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    results = data
                else:
                    results = [data]
            except json.JSONDecodeError:
                # Fall back to JSONL
                for line in text.strip().split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            results.append(json.loads(line))
                        except json.JSONDecodeError:
                            job.unparsable += 1
        elif fmt == "csv":
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                results.append({k.strip(): v for k, v in row.items() if k})
        return results

    records = await loop.run_in_executor(None, _read)
    for record in records:
        yield record


async def _s3_stream(uri: str, job) -> AsyncIterator[dict]:
    """Stream records from Amazon S3."""
    try:
        import boto3
    except ImportError:
        raise ImportError('Install boto3 for S3 support: pip install "mixpanel-utils[streaming-s3]"')

    parts = uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    loop = asyncio.get_event_loop()

    def _read():
        kwargs = {}
        if job.s3_key and job.s3_secret:
            kwargs["aws_access_key_id"] = job.s3_key
            kwargs["aws_secret_access_key"] = job.s3_secret
        if job.s3_region:
            kwargs["region_name"] = job.s3_region

        s3 = boto3.client("s3", **kwargs)
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()

        path = Path(key)
        gz = is_gzipped(path, job)
        fmt = detect_format(path, job)

        if gz:
            body = gzip.decompress(body)

        results = []
        if fmt == "parquet":
            import pyarrow.parquet as pq
            from datetime import date, datetime as dt_cls
            table = pq.read_table(io.BytesIO(body))
            for batch in table.to_batches(max_chunksize=1000):
                for row in batch.to_pylist():
                    record = {}
                    for k, v in row.items():
                        if v is None:
                            continue
                        if isinstance(v, dt_cls):
                            record[k] = v.isoformat()
                        elif isinstance(v, date):
                            record[k] = v.isoformat()
                        else:
                            record[k] = v
                    results.append(record)
            return results

        text = body.decode("utf-8")

        if fmt == "jsonl":
            for line in text.strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        job.unparsable += 1
        elif fmt == "json":
            try:
                data = json.loads(text)
                results = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                # Fall back to JSONL
                for line in text.strip().split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            results.append(json.loads(line))
                        except json.JSONDecodeError:
                            job.unparsable += 1
        elif fmt == "csv":
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                results.append({k.strip(): v for k, v in row.items() if k})
        return results

    records = await loop.run_in_executor(None, _read)
    for record in records:
        yield record
