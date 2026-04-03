"""Async generator pipeline for streaming data through transforms to Mixpanel.

Each stage is an async generator: async def stage(source, job) -> AsyncIterator.
Stages are chained together and consume records lazily with natural backpressure.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator

logger = logging.getLogger(__name__)


def _is_not_empty(data) -> bool:
    if not isinstance(data, (dict, list)):
        return False
    return len(data) > 0


# ── Pipeline Stages ──────────────────────────────────────────────────

async def existence_filter(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Skip empty records and enforce max_records limit."""
    async for chunk in source:
        if job.max_records is not None and job.records_processed >= job.max_records:
            return
        if _is_not_empty(chunk):
            job.records_processed += 1
            yield chunk
        else:
            job.empty += 1


async def vendor_transform(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Apply vendor-specific transform (Amplitude, Heap, GA4, etc.)."""
    if not job.vendor_transform:
        async for record in source:
            yield record
        return

    async for record in source:
        try:
            result = job.vendor_transform(record)
        except Exception as e:
            logger.debug(f"Vendor transform error: {e}")
            msg = f"vendor transform: {e}"
            job.errors[msg] = job.errors.get(msg, 0) + 1
            job.empty += 1
            continue

        if result is None:
            job.empty += 1
        elif isinstance(result, list):
            yield result
        elif _is_not_empty(result):
            yield result
        else:
            job.empty += 1


async def flatten_stream(source: AsyncIterator, job) -> AsyncIterator[dict]:
    """Explode arrays into individual records."""
    async for item in source:
        if isinstance(item, list):
            for record in item:
                if _is_not_empty(record):
                    yield record
        else:
            yield item


async def user_transform(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Apply custom user-provided transform function."""
    if not job.transform_func:
        async for record in source:
            yield record
        return

    async for record in source:
        try:
            result = job.transform_func(record)
        except Exception as e:
            logger.debug(f"User transform error: {e}")
            job.empty += 1
            continue

        if result is None:
            job.empty += 1
        elif isinstance(result, list):
            yield result
        elif _is_not_empty(result):
            yield result
        else:
            job.empty += 1


async def dedupe_transform(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Deduplicate records by content hash."""
    if not job.deduper:
        async for record in source:
            yield record
        return

    async for record in source:
        result = job.deduper(record)
        if _is_not_empty(result):
            yield result


async def post_transform_filter(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Post-transform existence check (does NOT increment records_processed)."""
    async for chunk in source:
        if _is_not_empty(chunk):
            yield chunk
        else:
            job.empty += 1


async def helper_transforms(source: AsyncIterator[dict], job) -> AsyncIterator[dict]:
    """Apply all active helper transforms (aliases, tags, fixData, etc.)."""
    transforms = job.active_transforms

    if not transforms:
        async for record in source:
            yield record
        return

    async for record in source:
        skip = False
        for fn in transforms:
            result = fn(record)
            if result is None:
                skip = True
                break
            if isinstance(result, dict) and len(result) == 0:
                job.empty += 1
                skip = True
                break
            record = result
        if not skip:
            yield record


async def compute_sizes(source: AsyncIterator[dict], job) -> AsyncIterator[tuple[dict, int]]:
    """Compute JSON byte size for each record."""
    async for record in source:
        size = len(json.dumps(record).encode("utf-8"))
        job.bytes_processed += size
        yield (record, size)


async def smart_batcher(source: AsyncIterator[tuple[dict, int]], job) -> AsyncIterator[list[dict]]:
    """Batch records by count AND size, whichever limit is hit first."""
    buffer: list[dict] = []
    current_size = 0
    max_count = job.records_per_batch
    max_bytes = int(job.bytes_per_batch * 0.985)  # Safety margin

    async for record, size in source:
        if buffer and (len(buffer) >= max_count or current_size + size > max_bytes):
            job.batches += 1
            job.last_batch_length = len(buffer)
            yield buffer
            buffer = []
            current_size = 0
        buffer.append(record)
        current_size += size

    if buffer:
        job.batches += 1
        job.last_batch_length = len(buffer)
        yield buffer


async def send_batches(batches: AsyncIterator[list[dict]], job, http_client):
    """Send batches concurrently using asyncio.Semaphore for backpressure."""
    semaphore = asyncio.Semaphore(job.workers)
    tasks: set[asyncio.Task] = set()
    errors: list[Exception] = []

    async def _send_one(batch: list[dict]):
        try:
            await semaphore.acquire()
            job.requests += 1

            if job.dry_run:
                job.dry_run_results.extend(batch)
                job.success += len(batch)
                return

            if job.write_to_file:
                import aiofiles
                async with aiofiles.open(job.output_file_path, "a") as f:
                    for record in batch:
                        await f.write(json.dumps(record) + "\n")
                job.success += len(batch)
                return

            response, success = await http_client.send_batch(batch, job)

            # Update counters based on record type
            if job.record_type in ("event", "scd", "export-import-event"):
                job.success += response.get("num_records_imported", 0)
                failed_records = response.get("failed_records", [])
                job.failed += len(failed_records) if isinstance(failed_records, list) else 0
            elif job.record_type in ("user", "group"):
                if response.get("error") or not response.get("status", True):
                    job.failed += len(batch)
                else:
                    num_good = response.get("num_good_events", 0)
                    job.success += num_good if num_good else len(batch)

            # Store response (preserve failed_records for error tracking)
            abbreviated = {
                "num_records_imported": response.get("num_records_imported", 0),
                "num_failed": len(response.get("failed_records", [])) if isinstance(response.get("failed_records"), list) else 0,
                "failed_records": response.get("failed_records"),
                "error": response.get("error"),
                "status": response.get("status", success),
            }
            job.store(abbreviated, success)

        except Exception as e:
            logger.error(f"Batch send error: {e}")
            errors.append(e)
            job.failed += len(batch)
        finally:
            semaphore.release()
            if job.progress_callback:
                try:
                    job.progress_callback(job)
                except Exception:
                    pass

    async for batch in batches:
        task = asyncio.create_task(_send_one(batch))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

        if errors:
            logger.error(f"Stopping due to errors: {errors}")
            break

    # Wait for remaining tasks
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ── Pipeline Orchestrator ────────────────────────────────────────────

async def core_pipeline(source: AsyncIterator[dict], job, http_client) -> dict:
    """Chain all pipeline stages and execute the import.

    Pipeline:
        source → existence_filter → vendor_transform → flatten
        → user_transform → flatten → dedupe → existence_filter_2
        → helper_transforms → compute_sizes → smart_batcher → send_batches
    """
    # Chain stages
    s1 = existence_filter(source, job)
    s2 = vendor_transform(s1, job)
    s3 = flatten_stream(s2, job)
    s4 = user_transform(s3, job)
    s5 = flatten_stream(s4, job)
    s6 = dedupe_transform(s5, job)
    s7 = post_transform_filter(s6, job)  # Post-transform existence check
    s8 = helper_transforms(s7, job)
    s9 = compute_sizes(s8, job)
    s10 = smart_batcher(s9, job)

    # Send batches with concurrent workers
    await send_batches(s10, job, http_client)

    if getattr(job, "_progress_written", False):
        try:
            job.progress_callback(job)
        except Exception:
            pass
        print(flush=True)

    return job.summary()
