"""Export operations: events, profiles, groups, annotations, and delete profiles.

These functions call Mixpanel's export APIs and write results to local files or return them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import AsyncIterator

logger = logging.getLogger(__name__)


async def export_events(filename: str, job) -> list | str:
    """Export events from Mixpanel's raw export API.

    Streams NDJSON from /api/2.0/export and writes to local file or returns records.
    """
    skip_write = job.dry_run or not filename

    params = {
        "from_date": job.start,
        "to_date": job.end,
        **(job.params or {}),
    }
    if job.limit:
        params["limit"] = job.limit
    if job.where_clause:
        params["where"] = job.where_clause
    if job.project and job.acct and job.pass_:
        params["project_id"] = job.project

    headers = {}
    if job.auth:
        headers["Authorization"] = job.auth

    all_results = []
    max_retries = job.max_retries or 5
    retry_count = 0

    import httpx

    while retry_count <= max_retries:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
                async with client.stream(
                    "GET",
                    job.url,
                    params=params,
                    headers=headers,
                ) as response:
                    job.requests += 1

                    if response.status_code == 429:
                        job.rate_limited += 1
                        if retry_count < max_retries:
                            backoff = min(30 * (2 ** retry_count), 300)
                            logger.warning(f"Export rate limited (429). Retrying in {backoff}s...")
                            await asyncio.sleep(backoff)
                            retry_count += 1
                            continue
                        raise Exception(f"Export rate limited after {max_retries} retries")

                    response.raise_for_status()

                    if skip_write:
                        # Collect all results in memory
                        async for line in response.aiter_lines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                                if job.transform_func:
                                    try:
                                        transformed = job.transform_func(record)
                                        if isinstance(transformed, list):
                                            all_results.extend(transformed)
                                        elif transformed:
                                            all_results.append(transformed)
                                    except Exception:
                                        all_results.append(record)
                                else:
                                    all_results.append(record)
                            except json.JSONDecodeError:
                                pass
                    else:
                        # Write to file
                        path = Path(filename)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with open(path, "w", encoding="utf-8") as f:
                            record_count = 0
                            async for line in response.aiter_lines():
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    record = json.loads(line)
                                    if job.transform_func:
                                        try:
                                            transformed = job.transform_func(record)
                                            if isinstance(transformed, list):
                                                for item in transformed:
                                                    f.write(json.dumps(item) + "\n")
                                                    record_count += 1
                                            elif transformed:
                                                f.write(json.dumps(transformed) + "\n")
                                                record_count += 1
                                        except Exception:
                                            f.write(json.dumps(record) + "\n")
                                            record_count += 1
                                    else:
                                        f.write(line + "\n")
                                        record_count += 1
                                except json.JSONDecodeError:
                                    pass

            break  # Success
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                continue  # Already handled above
            logger.error(f"Export HTTP error: {e}")
            job.store({"error": str(e)}, False)
            break
        except Exception as e:
            logger.error(f"Export error: {e}")
            job.store({"error": str(e)}, False)
            break

    if skip_write:
        job.records_processed += len(all_results)
        job.success += len(all_results)
        job.dry_run_results.extend(all_results)
        return all_results
    else:
        job.records_processed += record_count
        job.success += record_count
        return filename


async def export_profiles(folder: str, job) -> list:
    """Export user/group profiles from Mixpanel's engage API.

    Paginates through all profiles and writes each page to a separate JSON file,
    or returns all profiles in memory if dry_run.
    """
    skip_write = job.dry_run or not folder

    headers = {}
    if job.auth:
        headers["Authorization"] = job.auth
        headers["content-type"] = "application/x-www-form-urlencoded"

    params = dict(job.params or {})
    if job.project and job.acct and job.pass_:
        params["project_id"] = job.project

    # Build form body
    form_parts = {}
    if job.cohort_id:
        form_parts["filter_by_cohort"] = json.dumps({"id": job.cohort_id})
        form_parts["include_all_users"] = "false"
    if job.where_clause:
        form_parts["where"] = job.where_clause
    if job.data_group_id:
        form_parts["data_group_id"] = job.data_group_id

    body = "&".join(f"{k}={v}" for k, v in form_parts.items()) if form_parts else None

    entity_name = "group" if job.data_group_id else "users"
    all_results = []
    iterations = 0

    import httpx

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        # First request
        response = await _profile_request(client, job.url, headers, params, body, job)
        data = response.json()

        page = data.get("page", 0)
        page_size = data.get("page_size", 1000)
        session_id = data.get("session_id")
        profiles = data.get("results", [])

        # Apply transforms
        profiles = _apply_transforms(profiles, job)

        if skip_write:
            all_results.extend(profiles)
        else:
            filepath = _profile_filepath(folder, entity_name, iterations)
            _write_jsonl(filepath, profiles)
            all_results.append(filepath)

        job.records_processed += len(profiles)
        job.success += len(profiles)
        job.requests += 1

        last_num_results = len(data.get("results", []))

        # Paginate
        while last_num_results >= page_size:
            page += 1
            iterations += 1
            params["page"] = page
            params["session_id"] = session_id

            response = await _profile_request(client, job.url, headers, params, body, job)
            data = response.json()

            job.requests += 1
            profiles = data.get("results", [])
            profiles = _apply_transforms(profiles, job)

            job.records_processed += len(profiles)
            job.success += len(profiles)

            if skip_write:
                all_results.extend(profiles)
            else:
                filepath = _profile_filepath(folder, entity_name, iterations)
                _write_jsonl(filepath, profiles)
                all_results.append(filepath)

            last_num_results = len(data.get("results", []))

    if skip_write:
        job.dry_run_results.extend(all_results)

    return all_results


async def delete_profiles(job) -> dict:
    """Delete all user or group profiles from a Mixpanel project.

    First exports all profiles, then sends delete requests via the import pipeline.
    """
    from .. import mp_import

    if not job.token:
        raise ValueError("Token required for profile deletion")

    entity_type = "user"
    delete_key = "$distinct_id"
    export_opts = {
        "record_type": "profile-export",
        "dry_run": True,
        "verbose": False,
    }

    if job.data_group_id:
        entity_type = "group"
        export_opts["data_group_id"] = job.data_group_id
        if job.group_key:
            delete_key = job.group_key
        else:
            raise ValueError("group_key required for group profile deletion")

    creds = {"acct": job.acct, "pass": job.pass_, "project": job.project, "secret": job.secret}
    creds = {k: v for k, v in creds.items() if v}

    # Export all profiles first
    export_result = await mp_import(creds, None, export_opts)
    exported_profiles = export_result.get("dry_run", [])

    # Build delete objects
    delete_objects = []
    for profile in exported_profiles:
        delete_obj = {
            "$token": job.token,
            "$delete": "null",
        }
        if entity_type == "user":
            delete_obj["$ignore_alias"] = False
            delete_obj["$distinct_id"] = profile.get("$distinct_id", "")
        elif entity_type == "group":
            delete_obj["$group_key"] = delete_key
            delete_obj["$group_id"] = profile.get("$distinct_id", "")
        delete_objects.append(delete_obj)

    # Send deletes
    delete_opts = {"record_type": entity_type}
    if job.group_key:
        delete_opts["group_key"] = job.group_key

    delete_result = await mp_import({"token": job.token}, delete_objects, delete_opts)
    return delete_result


async def stream_events(job) -> AsyncIterator[dict]:
    """Stream events from Mixpanel's export API as an async iterator of dicts."""
    params = {
        "from_date": job.start,
        "to_date": job.end,
    }
    if job.limit:
        params["limit"] = job.limit
    if job.where_clause:
        params["where"] = job.where_clause
    if job.project and job.acct and job.pass_:
        params["project_id"] = job.project

    headers = {}
    if job.auth:
        headers["Authorization"] = job.auth

    import httpx

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        async with client.stream("GET", job.url, params=params, headers=headers) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # Flatten properties to top level (like Node version)
                    if "properties" in record:
                        flat = {**record, **record["properties"]}
                        del flat["properties"]
                        yield flat
                    else:
                        yield record
                except json.JSONDecodeError:
                    pass


async def stream_profiles(job) -> AsyncIterator[dict]:
    """Stream profiles from Mixpanel's engage API as an async iterator of dicts."""
    headers = {}
    if job.auth:
        headers["Authorization"] = job.auth
        headers["content-type"] = "application/x-www-form-urlencoded"

    params = dict(job.params or {})
    if job.project and job.acct and job.pass_:
        params["project_id"] = job.project

    form_parts = {}
    if job.cohort_id:
        form_parts["filter_by_cohort"] = json.dumps({"id": job.cohort_id})
        form_parts["include_all_users"] = "true"
    if job.data_group_id:
        form_parts["data_group_id"] = job.data_group_id

    body = "&".join(f"{k}={v}" for k, v in form_parts.items()) if form_parts else None

    page = 0
    session_id = None

    import httpx

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        while True:
            params["page"] = page
            if session_id:
                params["session_id"] = session_id

            response = await client.post(
                job.url, headers=headers, params=params,
                content=body,
            )
            response.raise_for_status()
            data = response.json()

            session_id = data.get("session_id")
            page_size = data.get("page_size", 1000)
            results = data.get("results", [])

            if not results:
                break

            for profile in results:
                # Flatten $properties to top level
                flat = {**profile}
                if "$properties" in flat:
                    flat.update(flat["$properties"])
                    del flat["$properties"]
                yield flat

            if len(results) < page_size:
                break

            page += 1


# ── Helpers ─────────────────────────────────────────────────────────

async def _profile_request(
    client, url: str, headers: dict,
    params: dict, body: str | None, job,
    max_retries: int = 5,
):
    """Make a profile export request with retry logic."""
    import httpx

    for attempt in range(max_retries + 1):
        try:
            response = await client.post(url, headers=headers, params=params, content=body)
            if response.status_code == 429:
                job.rate_limited += 1
                if attempt < max_retries:
                    backoff = min(30 * (2 ** attempt), 300)
                    logger.warning(f"Profile export rate limited. Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    continue
                raise Exception(f"Profile export rate limited after {max_retries} retries")
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            if attempt < max_retries:
                backoff = min(30 * (2 ** attempt), 300)
                await asyncio.sleep(backoff)
                continue
            raise

    raise Exception("Profile request failed after all retries")


def _profile_filepath(folder: str, entity_name: str, iteration: int) -> str:
    """Build a profile export file path."""
    path = Path(folder)
    path.mkdir(parents=True, exist_ok=True)
    return str(path / f"{entity_name}-{iteration}.json")


def _write_jsonl(filepath: str, data: list):
    """Write a list of dicts as JSONL."""
    with open(filepath, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")


def _apply_transforms(records: list, job) -> list:
    """Apply user transform function to a list of records."""
    if not job.transform_func:
        return records
    result = []
    for record in records:
        try:
            transformed = job.transform_func(record)
            if isinstance(transformed, list):
                result.extend(transformed)
            elif transformed:
                result.append(transformed)
        except Exception:
            result.append(record)
    return result
