"""HTTP client for sending data to Mixpanel APIs."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
from urllib.parse import urlparse
from typing import Any

import httpx

from ..constants import RETRY_STATUS_CODES, GZIP_RECORD_TYPES

logger = logging.getLogger(__name__)


class MixpanelHttpClient:
    """Manages httpx connection pools and sends batches to Mixpanel."""

    def __init__(self):
        limits = httpx.Limits(
            max_connections=100,
            max_keepalive_connections=50,
            keepalive_expiry=30.0,
        )
        timeout = httpx.Timeout(60.0, connect=10.0)
        self._pools: dict[str, httpx.AsyncClient] = {}
        self._limits = limits
        self._timeout = timeout

    def _get_client(self, base_url: str) -> httpx.AsyncClient:
        if base_url not in self._pools:
            self._pools[base_url] = httpx.AsyncClient(
                base_url=base_url,
                limits=self._limits,
                timeout=self._timeout,
                http2=False,
            )
        return self._pools[base_url]

    async def send_batch(self, batch: list[dict], job) -> tuple[dict, bool]:
        """Send a batch of records to Mixpanel. Returns (response_dict, success_bool)."""
        body_bytes = json.dumps(batch).encode("utf-8")

        headers = {
            "Authorization": job.auth,
            "Content-Type": job.content_type,
            "Accept": "application/json",
            "Connection": "keep-alive",
        }

        if job.record_type in GZIP_RECORD_TYPES and job.compress:
            body_bytes = gzip.compress(body_bytes, compresslevel=job.compression_level)
            headers["Content-Encoding"] = "gzip"

        params: dict[str, Any] = {
            "ip": "0",
            "verbose": "1",
            "strict": str(int(job.strict)),
        }
        if job.project and not job.secret:
            params["project_id"] = job.project

        parsed = urlparse(job.url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path
        client = self._get_client(base_url)

        for attempt in range(job.max_retries + 1):
            try:
                resp = await client.request(
                    method=job.req_method,
                    url=path,
                    params=params,
                    content=body_bytes,
                    headers=headers,
                )

                if resp.status_code in RETRY_STATUS_CODES and attempt < job.max_retries:
                    job.retries += 1
                    if resp.status_code == 429:
                        job.rate_limited += 1
                    elif str(resp.status_code).startswith("5"):
                        job.server_errors += 1
                    else:
                        job.client_errors += 1
                    delay = min(2 ** attempt, 30)
                    if job.verbose:
                        logger.info(f"Retry #{attempt + 1} after {delay}s (status {resp.status_code})")
                    await asyncio.sleep(delay)
                    continue

                try:
                    data = resp.json()
                except Exception:
                    data = {"error": resp.text, "status": False, "status_code": resp.status_code}

                success = 200 <= resp.status_code < 300
                return data, success

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                    httpx.PoolTimeout, httpx.ConnectTimeout) as e:
                if attempt < job.max_retries:
                    job.retries += 1
                    job.client_errors += 1
                    delay = min(2 ** attempt, 30)
                    if job.verbose:
                        logger.info(f"Retry #{attempt + 1} after {delay}s ({type(e).__name__})")
                    await asyncio.sleep(delay)
                    continue
                return {"error": str(e), "status": False}, False

        return {"error": "max retries exceeded", "status": False}, False

    async def send_table(self, csv_data: str, job) -> tuple[dict, bool]:
        """Send lookup table data (CSV) to Mixpanel."""
        headers = {
            "Authorization": job.auth,
            "Content-Type": "text/csv",
            "Accept": "application/json",
        }

        parsed = urlparse(job.url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path
        client = self._get_client(base_url)

        try:
            resp = await client.request(
                method="PUT",
                url=path,
                content=csv_data.encode("utf-8"),
                headers=headers,
            )
            try:
                data = resp.json()
            except Exception:
                data = {"error": resp.text, "status": False}
            success = 200 <= resp.status_code < 300
            return data, success
        except Exception as e:
            return {"error": str(e), "status": False}, False

    async def close(self):
        """Close all connection pools."""
        for client in self._pools.values():
            await client.aclose()
        self._pools.clear()
