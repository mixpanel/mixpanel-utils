# Streaming event import

This script demonstrates high-volume event import using the async streaming interface (`mputils.stream.import_events`). Unlike the synchronous `import_events()` method, the streaming interface handles:

- **Multiple file formats** — JSONL, JSON arrays, CSV, Parquet, and gzip-compressed files (auto-detected from extension)
- **Cloud storage** — Import directly from `gs://` or `s3://` URLs
- **Concurrent connections** — Sends batches in parallel for maximum throughput
- **Built-in transforms** — Auto-fix data issues, deduplicate records, flatten nested properties, and more

This is the recommended approach for importing large datasets (100K+ events) or building production data pipelines.

## Configuration

Edit the `CREDENTIALS` section in `main.py` with your Mixpanel service account credentials and project token. Set the `DATA_FILE` variable to the path of your event data file.

## Installation

```
pip3 install mixpanel-utils[streaming]
```
