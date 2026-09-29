# mixpanel-utils: plan for a Rust rewrite

Status: **proposal, not started.** Nothing in this document has been built yet.
It covers what exists today, what is wrong with it, the design of the Rust
version, how we will prove it works, and the order we will build it in.

---

## 0. Summary

`mixpanel-utils` is a batch data-movement tool. It exports, imports and bulk-edits
Mixpanel events, user profiles and group profiles, and it migrates data out of
Amplitude. The current code is one 2,785-line Python class. Every job loads its
whole dataset into memory, gives up on data without saying so on common failures
(HTTP 429, validation errors, exhausted retries), and cannot resume after a crash.

For this workload, "maximum performance" does not mean a fast JSON parser. The
limit is Mixpanel's server-side budget: about 2 GB of uncompressed JSON per minute
per project for `/import`, 60 queries per hour for `/export` and `/engage`, and 5
concurrent queries. So the goals of the rewrite are:

1. **Use the whole server budget, and never go over it.** Adaptive concurrency,
   a byte-rate limiter, and backoff that honours `Retry-After`.
2. **Use very little CPU and a fixed amount of memory to do it.** Stream every
   stage, parse zero-copy, parse only the fields a job needs, keep batches bounded,
   and set one memory budget for the whole process. Memory use should not depend on
   input size.
3. **Never lose a record without reporting it.** Every input record ends up either
   accepted, rejected (written to a dead-letter file with the reason), or not yet
   sent (recorded in a resumable journal).
4. **Make it safe by construction.** No `unsafe` in our crates. Credentials are
   typed secrets. Invalid operations cannot be represented. Parsers are fuzzed and
   the pipeline is fault-injection tested.

What we ship: a Rust library (`mixpanel-utils` on crates.io), a single static CLI
binary (`mpu`), and a PyO3-based **v4 of the `mixpanel-utils` PyPI package** that
keeps the `MixpanelUtils` Python API and runs on the Rust core.

Rough effort: 16–20 engineer-weeks for one senior Rust engineer, or about 10
calendar weeks with two. Section 13 breaks this into phases, each with exit
criteria.

---

## 1. What the project does today

| Area | Python entry points (`src/mixpanel_utils/__init__.py`) | Mixpanel API |
|---|---|---|
| Client setup | `__init__` (service-account auth, residency us/eu/in, pool sizes, retries) | — |
| Raw HTTP | `request` | all |
| Event export | `export_events`, `query_export` | `GET data[-eu/-in].mixpanel.com/api/2.0/export` |
| Profile export | `export_people`, `export_groups`, `query_engage` (+ `ConcurrentPaginator`) | `/api/2.0/engage` (query) |
| Event import | `import_events` | `POST api[-eu/-in].mixpanel.com/import?strict=1` |
| Profile import | `import_people` (incl. `raw_record_import`), `import_groups` | `POST /engage`, `POST /groups` |
| Profile ops | `people_operation` + `people_{set,set_once,unset,add,append,union,remove,delete}` | `POST /engage` |
| Group ops | `group_operation`, `group_set`, `group_delete`, `define_group_context` | `POST /groups` |
| Derived ops | `people_change_property_name`, `people_revenue_property_from_transactions`, `deduplicate_people`, `event_counts_to_people` | engage + JQL |
| JQL | `query_jql`, `jql_operation`, `{query,export}_jql_{events,people}` | `POST /api/2.0/jql` |
| File I/O | `export_data` (JSON array / CSV, optional gzip), CSV/JSON/NDJSON sniffing on import | — |
| Migration | `import_from_amplitude`, `import_from_amplitude_id_mgmt_v3` | Amplitude `/api/2/export` |
| User transforms | Python lambdas passed as `value` (called once per profile) | — |

Other code: two sample scripts (export in date chunks while pacing requests;
stream-import gzipped NDJSON), an example tour (`tools/mixpanel_utils_example.py`),
and 58 pytest cases (after parametrization) covering constructor validation and URL building per residency.
Releases are tag-driven and use PyPI trusted publishing (`.github/workflows/`).

---

## 2. Audit findings: defects the rewrite must fix by design

We found these by reading the code. Finding 1 was reproduced locally. Line numbers
refer to `src/mixpanel_utils/__init__.py`.

**Data loss and hangs**

1. **When a batch exhausts its retries, the batch is lost and the process hangs.**
   `request()` raises a bare `BaseException` (L365). `_send_batch` catches only
   `Exception` (L2379), so it never writes the `import_backup.txt` fallback. The
   exception then kills the `ThreadPool` worker. That task's result is never set, so
   `pool.join()` (L2348) blocks forever. Reproduced with a 10-line script.
2. **HTTP 429 and all other 4xx responses are silently dropped.** Only status ≥ 500
   is retried (L297). Anything else falls through and `request()` returns `None`.
   The response callback ignores `None`, so a rate-limited or rejected batch
   disappears with nothing but an error log line. 429 is the *most common* error on
   `/import`.
3. **Strict-mode partial failures lose the whole batch.** With `strict=1`,
   Mixpanel returns HTTP 400 with `failed_records` and still ingests the valid
   records. The client treats this like finding 2: it does not parse
   `failed_records`, so it records no per-record reasons and keeps no count.
4. **Retries have no backoff and no jitter.** Retries are immediate recursive calls
   (L300–L360). They hammer an overloaded server and use up the retry budget in
   milliseconds.
5. **An Amplitude profile error drops all remaining files.**
   `amplitude_profile["user_properties"]["Name"]` (L2588) raises `KeyError` when a
   profile has no `Name`. A bare `except:` (L2742) catches it and **returns**, which
   skips that file and every file after it. The only trace is an error log line.

**Correctness**

6. **Timezone offsets compound when `request_per_day=True`.**
   `timezone_offset = timezone_offset * 3600` (L1571) runs inside the per-day loop.
   Day 2 is shifted by `offset·3600²` seconds.
7. **Amplitude timestamps depend on the machine's local timezone.** They are parsed
   into naive datetimes and then passed to `.timestamp()` (L2615), which uses the
   host's local zone. Amplitude times are UTC.
8. **Fixed hour offsets are wrong across DST changes** (every `timezone_offset`
   parameter). A project in `America/Los_Angeles` is −8 for part of the year and −7
   for the rest.
9. **The seconds-based offset is applied to millisecond timestamps.** This hits
   exports that use `time_in_ms` and events that already carry ms times (L2109).
10. **Shared mutable state is modified from worker threads.** A read timeout does
    `self.timeout += 30` on the shared client (L327). `export_events` changes and
    restores `self.timeout`. `define_group_context` mutates the client. None of this
    is thread-safe, and the timeout only ever grows.
11. **The selector is built by string concatenation.**
    `'(defined (properties["' + old_name + '"]))'` (L998) breaks, or targets the
    wrong profiles, when a property name contains `"`.
12. **CSV export opens the same path twice** (L177 and L1856). It also ignores
    `append_mode` and needs every row in memory to compute the column union.

**Scale**

13. **Memory grows with dataset size everywhere.** `_list_from_argument` loads whole
    files. `query_export` builds a list of dicts from one big string. `query_engage`
    collects every page. `export_data` writes one JSON array with `json.dump`. The
    Amplitude path reads the whole zip into memory (L2688), then writes and re-reads
    an intermediate JSON array per file. Python objects typically take 5–10× the
    JSON size.
14. **Nothing applies backpressure.** `_dispatch_batches` enqueues every batch into
    an unbounded `apply_async` queue.
15. **Every event is deep-copied** before import (L2107).
16. **Imports are uncompressed.** `/import` accepts `Content-Encoding: gzip`, and
    NDJSON bodies compress about 5–10×.
17. **Raw export streams through a 1 KB buffer** (`buffer_size=1024`).
18. **Nothing can be resumed or checkpointed.** A crash 90% of the way through a
    100 M-event import means starting over, which creates duplicates when events
    have no `$insert_id`.

**Security**

19. **Debug logging prints the `Authorization` header**, which contains the service
    account secret in Base64 (L285).
20. **Credentials are passed as constructor arguments and kept as plain strings.**
    The examples encourage hard-coding them.
21. **Side-effect files land in the working directory** (`invalid_events.txt`,
    `import_backup.txt`, `backup_<ts>.json`, `./amp_data/`). Several threads append
    to them with no lock, so writes can interleave. The Amplitude zip is extracted
    without a size cap, so a decompression bomb would not be stopped.

**API drift**

22. **JQL is in maintenance mode.** Mixpanel recommends migrating away from it.
    `event_counts_to_people` and the `*_jql_*` helpers depend on it.
23. **Profile queries use the legacy `/api/2.0/engage` path.** Current docs list
    `POST /api/query/engage`. Profile updates are sent as form-encoded base64
    `data=`, but the documented body is `application/json`.
24. **Amplitude EU** (`analytics.eu.amplitude.com`) is not supported.

---

## 3. External constraints

| Endpoint | Documented limits | What the design does about them |
|---|---|---|
| `/import` | ≤ 2,000 events and ≤ 10 MB uncompressed per request; ≤ 1 MB per event; ≤ 255 props. **~2 GB/min (~30k ev/s) per project.** JSON or NDJSON; gzip supported. `strict=1` returns 400 + `failed_records` + `num_records_imported`. Needs `time`, `distinct_id`, `$insert_id` (≤ 36 chars, `[A-Za-z0-9-]`). Recommended: 10–20 concurrent senders. Backoff on 429/502/503: start 2 s, cap 60 s, 1–5 s jitter. Never retry a 400. | Size-aware batcher, NDJSON + gzip bodies, byte-rate limiter, adaptive concurrency starting at 10, strict-mode response parsing, deterministic `$insert_id` synthesis. |
| `/engage`, `/groups` (updates) | `application/json`. Returns 200 even when validation fails, so the body must be checked (`verbose=1` / `strict=1`). Batch cap not documented; the current client uses 2,000. | Always send `verbose=1&strict=1` and parse the body. Batch cap is configurable (default 2,000). Test gzip support in Phase 0. |
| `/export` (raw) | **60 queries/hour, 3/s, 100 concurrent.** JSONL; `Accept-Encoding: gzip`; `limit` ≤ 100k; `time_in_ms`. | Token bucket (60/h + 3/s). Stream and decompress. Pick date windows adaptively. Write each window atomically. |
| `/query/engage` | **60 queries/hour, 5 concurrent.** Paged by `session_id` + `page`. | Stream pages with at most 5 in flight. Phase 0 checks whether page fetches count against 60/h. |
| JQL | Maintenance mode. | Feature-gated `legacy-jql`. Replace uses with export + local aggregation where possible. |
| Amplitude export | Zip of gzipped NDJSON, ≤ 4 GB per request, ≤ 365 days, UTC timestamps, US/EU hosts. | Download to a temp file, read zip members one at a time, stream through the transform into the import pipeline. |

**What this means for performance.** At the import cap of ~33 MB/s of uncompressed
JSON, a well-built Rust pipeline needs a fraction of one core. Raw parsing speed
has little value past that point. The value is in: (a) reaching the cap reliably,
(b) not tripping 429 storms, (c) fixed memory, (d) crash-safe resume, and (e)
finishing the CPU-heavy local work fast (dedupe, CSV, transforms, gzip). Every
optimization below must show a benchmark win on one of these before it lands.

---

## 4. Goals, non-goals, targets

**Goals.** Feature parity with v3 (§7). Streaming everywhere. A bounded memory
budget. Exactly-once accounting of every record. Crash-safe resume. Library, CLI and
Python bindings on one core. Linux, macOS and Windows on x86-64 and aarch64.

**Non-goals.** Server-side event tracking (that is the job of the official SDKs).
A GUI. Rebuilding JQL. HTTP/3. io_uring by default.

**Targets.** These are provisional. Phase 0 measures the Python baseline and
confirms or adjusts them.

| Metric | Target |
|---|---|
| Import throughput, real API | Hold the per-project cap (~2 GB/min) for the whole job, with ≤ 1% of requests getting 429 once steady |
| Import throughput, local (mock server, no cap) | ≥ 300k events/s per core for frame + batch + gzip-1 on ~1 KB events (headroom, not a goal in itself) |
| CPU at the real cap | ≤ 1.5 cores in total |
| Peak RSS | ≤ the configured budget (default 256 MiB) + ~20 MiB, **no matter how big the input is** |
| Export | Network line rate to disk; ≤ 64 MiB RSS |
| Profile query | Stays at the 5-concurrent cap; output streams |
| Reliability | 0 unaccounted records across the fault-injection suite (§9) |
| CLI | Cold start < 10 ms; static binary ≤ 15 MB |

---

## 5. Architecture

### 5.1 Workspace layout

```
Cargo.toml                    # workspace; edition 2024; resolver 3
rust-toolchain.toml           # pinned stable
crates/
  mpu-core/                   # types, config, errors, endpoints, secrets, time
  mpu-codec/                  # framing, sniffing, shallow parse/splice, CSV, gzip/zstd
  mpu-transport/              # hyper + rustls client, tower layers: auth, retry, limits
  mpu-pipeline/               # staged runtime, batcher, memory budget, journal, dead-letter
  mixpanel-utils/             # public facade: Client + jobs (import/export/ops/dedupe/migrate)
  mpu-cli/                    # `mpu` binary
  mpu-py/                     # PyO3 bindings -> PyPI `mixpanel-utils` 4.x
tools/
  mock-mixpanel/              # axum fake Mixpanel with fault injection (dev-only)
  xtask/                      # bench, pgo, release chores
fuzz/                         # cargo-fuzz targets
benches/                      # macro benchmarks
python-legacy/                # current v3 package, security fixes only until end of life
```

Only the facade, the CLI and the Python package are public products. The internal
crates are published under a `mixpanel-utils-*` prefix only because crates.io
requires it; their public API is not treated as stable. Splitting into crates keeps
incremental builds fast and forces clean dependency direction:
`core ← codec ← pipeline ← facade`, and `core ← transport ← facade`.

### 5.2 Layers

```
 ┌──────────── mpu (CLI) ────────────┐   ┌──── mixpanel_utils 4.x (Python / PyO3) ────┐
 └─────────────────┬─────────────────┘   └──────────────────────┬─────────────────────┘
                   ▼                                            ▼
 ┌──────────── mixpanel-utils (facade): Client, jobs, blocking wrapper ─────────────────┐
 │  import · export · profile/group ops · dedupe · migrate(amplitude, project→project) │
 └───────┬──────────────────────────┬──────────────────────────────┬───────────────────┘
         ▼                          ▼                              ▼
   mpu-pipeline               mpu-codec                     mpu-transport
   stages, batching,          NDJSON/array framing,         hyper 1 + rustls, pooling,
   backpressure, memory       format sniffing, shallow      tower: auth · timeout · retry ·
   budget, journal,           parse + splice, CSV,          rate limit · adaptive concurrency ·
   dead-letter, reorder       gzip / zstd                   metrics
         └──────────────────────────┴──────────────┬───────────────┘
                                                   ▼
                                               mpu-core
            ProjectId · Residency · Endpoints · SecretString creds · ProfileOp · errors · time
```

### 5.3 The import pipeline (the hot path)

```
 file(s) / stdin / HTTP body
   │  Bytes chunks (64–256 KiB), memory-budget permit taken per chunk
   ▼
 [Source] ─► [Decompress?] ─► [Frame + sniff] ─► [Shallow parse + transform]×N ─► [Batcher] ─► [Encode + gzip]×N ─► [Sender] ─► [Result handler]
   tokio        CPU pool        CPU pool           CPU pool (rayon)                 1 task      CPU pool             tokio       tokio
                                                                                                                      │  ▲          │
                                                                                     adaptive limit + byte-rate GCRA  │  └─ retry ──┤
                                                                                                                                    ├─► journal (ack watermark)
                                                                                                                                    ├─► dead-letter (record + reason)
                                                                                                                                    └─► metrics / progress / summary
```

- **Every hop is a bounded channel.** A slow stage stalls the ones upstream, so
  nothing queues without limit.
- **One memory budget covers the whole job.** It is a `Semaphore` whose permits
  count KiB. The permit is taken when a chunk is read and released when the batch
  holding those bytes is acknowledged or dead-lettered. That gives a hard ceiling
  on buffered data, and so on RSS.
- **CPU work never runs on tokio worker threads.** JSON, gzip and transforms run on
  a dedicated rayon pool sized to physical cores. If they blocked the reactor,
  latency would spike and the adaptive limiter would read the spike as server
  pressure.
- **Order is not preserved on import,** because Mixpanel orders by `time`. Exports
  that need order use sequence numbers and a bounded reorder buffer.
- **Cancellation is structured.** `CancellationToken` plus `JoinSet`. On Ctrl-C the
  job stops reading, drains in-flight requests up to a deadline, flushes the
  journal, and exits with code 130 in a resumable state.

---

## 6. Key design decisions

Each decision states the choice, the alternatives, and the reason.

### 6.1 Runtime: tokio (multi-threaded)
Alternatives: io_uring runtimes (monoio, glommio, compio). We use tokio because the
HTTP/TLS ecosystem, portability (macOS/Windows) and tooling are there. Disk I/O is
not the bottleneck at the server cap. We will look again at an io_uring file source
behind a feature flag only if profiling shows `read` syscalls matter.

### 6.2 HTTP: hyper 1.x, the `hyper-util` pooled client, and tower middleware
We need exact control over: replaying a retried body without copying it (`Bytes`
clones are refcount bumps), streaming request and response bodies, per-endpoint
concurrency and rate limits, and fully mockable layers in tests (`tower::Service`).
The layer stack, from the outside in:

```
Metrics → Auth(Basic, SecretString) → RateLimit(per endpoint family)
        → AdaptiveConcurrency → Retry(policy + budget) → Timeout(per attempt) → hyper
```

Alternative: reqwest 0.13, which already uses hyper and rustls. It is the fallback
if the Phase 0 spike shows the hand-built stack is not worth its complexity. Either
way the rest of the code sees only a `Transport` trait.

HTTP/2 is used when the server offers it through ALPN. We keep a small pool of h2
connections (2–4) instead of one, to avoid TCP head-of-line limits. Phase 0
benchmarks h1.1-pool against h2. The CLI exposes `--http2 auto|on|off`.

### 6.3 TLS: rustls with the aws-lc-rs provider
Memory-safe TLS 1.3 with session resumption. A FIPS build is possible if a customer
needs one. Trust comes from `rustls-platform-verifier`, falling back to
`webpki-roots`. There is no option to turn off certificate verification.

### 6.4 JSON: shallow parsing, verbatim copying, no DOM on the hot path
Most jobs touch 1–4 fields per record:
- event import: `properties.time`, `distinct_id`, `$insert_id`, and optionally `token`
- profile import: `$distinct_id`, `$properties`
- export: nothing, it is pass-through

So we skip building a DOM:
- **Framing** with `memchr` (SIMD) over `Bytes`. NDJSON lines become zero-copy
  `Bytes` slices, with carry-over of partial lines across chunk boundaries. JSON
  arrays use a small scanner that tracks depth and strings, and yields element
  slices without parsing them.
- **Shallow parse.** Deserialize with serde into a `Vec<(Cow<str>, &RawValue)>` for
  the one or two object levels a job needs. Values we do not touch are copied to
  the output byte-for-byte and never parsed. This also validates syntax, which
  matters because one malformed line can fail a whole strict-mode batch.
- **Splice.** Changes such as adding `token`, shifting `time`, or adding
  `$insert_id` are written by re-emitting only the edited object's keys around
  verbatim value slices.
- **Full DOM** only when a user transform needs the whole record (jaq, closures,
  Python lambdas).

The parser starts as `serde_json`: portable, audited, and fast enough for shallow
work. `sonic-rs` is an optional `simd-json` feature and is enabled only if the
Phase 0 benchmark shows a real gain on the DOM path. It needs `target-cpu` flags,
which clash with portable wheels.

Proptest must confirm that the splice output equals the result of the same edit
done through a DOM.

### 6.5 Input formats and compression
- **Formats are detected from magic bytes and the first non-whitespace byte**, not
  from parse errors: gzip `1f 8b`, zstd `28 b5 2f fd`, zip `PK\x03\x04`, `[` for a
  JSON array, `{` for NDJSON, anything else for CSV. Compressed inputs are read
  transparently, so the sample script's hand-written gunzip loop is no longer
  needed.
- **Request bodies:** `/import` sends NDJSON (`application/x-ndjson`), gzip level 1
  by default (configurable), via flate2 with the **zlib-rs** backend (pure Rust,
  on par with zlib-ng). `/engage` and `/groups` send a JSON array. Gzip for those
  two is turned on only after Phase 0 confirms the server accepts it.
- **Responses** are decompressed as a stream (async-compression).
- **Output files** are NDJSON by default, which is streamable, unlike v3's single
  JSON array. JSON-array output and CSV are options. Compression can be gzip or
  **zstd** (multi-threaded, level 3 by default; much faster than gzip at a similar
  ratio). Files are written to `*.partial` and renamed atomically when complete.

### 6.6 Size-aware batching
The batcher closes a batch at whichever comes first: `max_records` (default 2,000),
`max_bytes` (default 9.5 MB uncompressed, leaving room under 10 MB), or `linger`
(default 250 ms, only for slow streaming sources). Records are serialized straight
into a pooled `BytesMut`. Any single record over 1 MB is dead-lettered before it is
sent, with a reason. If the server still returns 413, the batch is split in half and
each half is retried.

### 6.7 Concurrency and rate control
- **Endpoint families get separate budgets:**
  - ingestion (`/import`, `/engage`, `/groups`)
  - raw export: GCRA at 3/s plus 60/h, ≤ 100 concurrent
  - query (`/query/engage`, JQL): 60/h, ≤ 5 concurrent

  All limits are configurable, because customers may share quota with other tools.
- **Ingestion concurrency adapts.** A gradient/AIMD limiter (the Netflix
  concurrency-limits approach) starts at 10, as Mixpanel recommends. It raises the
  limit while latency is flat, and cuts it multiplicatively on 429 or rising
  latency.
- **A byte-rate limiter** (GCRA via `governor`, where one cell is 1 KiB) holds
  uncompressed throughput just below 2 GB/min by default. It paces sending ahead of
  time rather than reacting to 429s.
- **`Retry-After` pauses the whole family.** One 429 pauses every sender in the
  family, not just the request that got it, which prevents thundering herds.

### 6.8 Retries, idempotency and error classes

| Class | Examples | Action |
|---|---|---|
| Transient | 429, 500, 502, 503, 504, connect or reset errors, per-attempt timeout, truncated or bad-CRC body | Retry with decorrelated-jitter backoff (2 s → 60 s, per Mixpanel guidance). A tower retry **budget** caps retries at about 10% of traffic, so retries cannot multiply load. |
| Partial | strict-mode 400 with `failed_records` | Accept the rest. Dead-letter only the failed indices, with the server's reason. |
| Too large | 413 | Split the batch and retry. |
| Fatal to the job | 401, 403, persistent 400 without `failed_records` | Stop the job fast with a clear error. The journal keeps it resumable. |

**Idempotency.** Retrying `/import` is safe only if events carry `$insert_id`. When
one is missing, we add a **deterministic** id: an xxh3-128 hash of the canonical
`(event, distinct_id, time, properties)` bytes, encoded as 32 hex characters. The
same input always produces the same id, so retries and resumes cannot create
duplicates. The option can be turned off.

Profile operations are split into two groups:
- **Idempotent:** `$set`, `$set_once`, `$unset`, `$delete`, `$union`, `$remove`.
- **Not idempotent:** `$add`, `$append`.

A timeout *after* a request was sent could double-apply a non-idempotent operation.
By default those operations are **not** retried after an ambiguous failure; the
record goes to dead-letter as `ambiguous`. `--retry-ambiguous` opts in to retrying.
This makes the at-least-once trade-off explicit instead of hidden.

### 6.9 Resuming and dead-lettering
- **Journal.** An append-only file (`<job>.mpu-journal`) records the job spec hash,
  each input's identity (path, size, mtime, content hash of the first MiB), and a
  **watermark**: the lowest byte offset not yet acknowledged. Batches complete out
  of order, so the watermark is the minimum over in-flight batches. It is fsync'd
  every N seconds and on shutdown. `--resume` seeks each input to its watermark.
  The overlap is re-sent safely thanks to `$insert_id`.
- **Exports** record each finished date window. On resume, finished windows are
  skipped.
- **Dead-letter** is NDJSON with one object per failed record:
  `{"record":…, "reason":…, "status":…, "attempts":…, "batch_id":…}`. It is written
  by a single writer task, so lines cannot interleave. It replaces
  `import_backup.txt` and `invalid_events.txt`. All side files go to a
  job-specific directory, not the working directory.
- **The final summary accounts for every record:**
  `read = accepted + rejected + skipped`, where `skipped` means invalid before
  sending. Any gap is a bug and makes the job exit non-zero.

### 6.10 Memory efficiency
- `Bytes`/`BytesMut` throughout: framing slices share the input buffer, and buffer
  pools recycle batch and gzip buffers.
- The memory budget from §5.3 caps buffered data.
- The CLI and Python wheel use **mimalloc** as the global allocator. musl's malloc
  would otherwise be very slow. The library crate never sets a global allocator.
- Jobs that need global state use compact structures:
  - **Dedupe:** per profile, keep `(match_key_hash: u64, last_seen: i64,
    distinct_id: interned, backup_offset: u64)`, about 32 bytes, instead of the
    whole profile. When properties must be merged, re-read them from the backup
    file by offset. Hash collisions are checked against the real key. Above the
    memory budget, fall back to hash partitioning into temp-file shards (external
    grouping).
  - **CSV column union:** interned keys from a first pass over a temp NDJSON spill,
    then a second pass writes rows. `--columns a,b,c` allows a single pass.
  - **Amplitude merge dedupe:** a `HashSet<u128>` of hashed id pairs.
- Hash maps are `hashbrown` with `foldhash`, and strings are interned with
  `lasso`.

### 6.11 User transforms (replacing Python lambdas)
The v3 API lets `value` be a Python callable. Three replacements:
1. **Rust API:** `impl Fn(&ProfileView) -> Option<ProfileOp> + Send + Sync`. It runs
   in parallel on the CPU pool.
2. **CLI:** `--jq '<filter>'` using **jaq** (a pure-Rust jq clone). The filter is
   compiled once and run per record in parallel. Data people already know jq
   syntax. For example:

   `mpu people set --where '...' --jq '{favorite_color: .["$properties"].color}'`
3. **Python bindings:** accept callables as before. They are called in chunks under
   the GIL to spread its cost, and in parallel on free-threaded CPython (3.14t).

Stretch goal: WASM plugins through the wasmtime component model, for sandboxed
custom transforms from any language.

### 6.12 Types that make misuse unrepresentable
```rust
pub struct ProjectId(NonZeroU64);                  // parsed from int or numeric string
pub enum Residency { Us, Eu, In }                  // endpoints derived by exhaustive match
pub struct ServiceAccount { username: String, secret: SecretString } // redacted Debug, zeroize on drop
#[non_exhaustive]
pub enum ProfileOp {
    Set(Props), SetOnce(Props), Add(NumericProps), Append(Props),
    Union(ListProps), Remove(Props), Unset(Vec<PropName>), Delete,
}
pub struct GroupContext { group_key: PropName, data_group_id: Option<DataGroupId> }
```
- The `Client` is **immutable** and cheap to clone (`Arc` inside), and it is safe to
  share across tasks. Group context, timeouts and so on are arguments, not mutable
  client state (fixes finding 10).
- The builder uses a typestate, so a missing service account or project id is a
  **compile error** in Rust. `trybuild` compile-fail tests take over from the
  Python constructor tests. The Python bindings keep the runtime `ValueError`
  messages word for word.
- Selectors are built with an escaping builder, and property names are quoted
  (fixes finding 11). A raw `where` string is still accepted.

### 6.13 Time
Use `jiff` for dates and time zones. Every offset parameter accepts either an **IANA
zone** (`America/Los_Angeles`), which handles DST correctly, or a fixed offset for
v3 compatibility. Before offsets are applied, the timestamp unit (seconds or
milliseconds) is detected. Amplitude times are parsed as UTC (fixes findings 6–9).

### 6.14 Errors, exit codes, observability
- Libraries use `thiserror` enums, grouped as Config / Auth / Http / RateLimited /
  Validation / Io / Cancelled. The CLI renders them with `miette` and suggests a
  fix.
- Exit codes:

  | Code | Meaning |
  |---|---|
  | 0 | Success |
  | 1 | Fatal error |
  | 2 | Finished with dead-lettered records |
  | 3 | Authentication error |
  | 4 | Configuration error |
  | 5 | Aborted, resumable |
  | 130 | Interrupted, resumable |

- **Tracing and metrics.** `tracing` spans for job, stage and batch. Counters and
  histograms via `metrics`: records and bytes in/out, 429s, retries, latency
  p50/p99, in-flight requests, and the current concurrency limit. `indicatif`
  progress bars on a TTY. `--summary-json` for scripting. An optional
  OpenTelemetry (OTLP) exporter sits behind a feature flag.
- **No request or response bodies or auth headers are ever logged.** A redaction
  layer guarantees this (fixes finding 19).

### 6.15 Deliberately not doing
- Hand-written `unsafe` SIMD.
- A global allocator in the library.
- Async inside CPU stages.
- io_uring by default.
- HTTP/3.
- A database for the journal (a plain file is enough).
- Any optimization that no benchmark justifies.

---

## 7. Feature parity map

| v3 Python | Rust library (`mixpanel-utils`) | CLI (`mpu`) | Notes |
|---|---|---|---|
| `__init__(...)` | `Client::builder()…build()` | global flags / env / `~/.config/mpu/config.toml` profiles | Credentials from env (`MP_SA_USERNAME`, `MP_SA_SECRET`), stdin, config file (0600 check), or OS keyring. Never from argv by default. |
| `request` | `Client::raw()` (escape hatch) | `mpu api <METHOD> <path>` | |
| `import_events` | `client.import_events(source)` | `mpu import events <paths…\|->` | gzip/zstd/zip input, NDJSON/array/CSV, `--tz`, `--resume`, `--dead-letter` |
| `import_people` (+`raw_record_import`) | `client.import_profiles(source)` / `.raw_updates()` | `mpu import people [--raw]` | |
| `import_groups` | `client.import_groups(ctx, source)` | `mpu import groups --group-key` | |
| `export_events`, `query_export` | `client.export_events(range)` → `.stream()` or `.to(sink)` | `mpu export events --from --to [--split auto\|day\|N] [-o dir\|-]` | Adaptive window sizing replaces `request_per_day` and the sample script's manual increments |
| `export_people`, `query_engage` | `client.query_profiles(q)` → stream / sink | `mpu export people [--where] [--cohort] [--props]` | `/api/query/engage`; 5 in flight |
| `export_groups` | `client.query_groups(data_group_id, q)` | `mpu export groups --data-group-id` | |
| `people_{set,set_once,unset,add,append,union,remove,delete}`, `people_operation` | `client.people().select(sel).apply(ProfileOp::…)` or `.apply_with(f)` | `mpu people <op> (--where\|--input) (--value JSON\|--jq F) [--backup]` | Backup is streamed NDJSON(.zst) |
| `group_*`, `group_operation`, `define_group_context` | `client.groups(ctx).…` | `mpu groups <op> --group-key [--data-group-id]` | No mutable context |
| `people_change_property_name` | `ops::rename_property` | `mpu people rename-prop OLD NEW [--keep-old]` | Escaped selector |
| `people_revenue_property_from_transactions`, `sum_transactions` | `ops::revenue_from_transactions` | `mpu people revenue-from-transactions` | |
| `deduplicate_people` | `ops::dedupe_profiles` (plan → apply) | `mpu people dedupe --by '$email' [--merge] [--case-sensitive] [--dry-run]` | Scales past RAM; dry-run prints the plan |
| `event_counts_to_people` | `ops::event_counts_to_people` (raw export + local aggregation) | `mpu people event-counts --from DATE --events a,b` | No longer needs JQL; semantics checked in Phase 0 (see risks) |
| `query_jql`, `jql_operation`, `*_jql_*` | `jql::*` behind the `legacy-jql` feature | `mpu jql run script.js` | Warns that JQL is in maintenance mode |
| `export_data` | `Sink::file(path).format(..).compress(..)` | `-o`, `--format`, `--gzip/--zstd` | |
| `import_from_amplitude[_id_mgmt_v3]` | `migrate::amplitude(..).id_mode(Original\|V3)` | `mpu migrate amplitude --start --end --region us\|eu --id-mode original\|v3` | Streams zip → import; UTC times; no crash on a missing `Name` |
| — (new) | `migrate::project_to_project` | `mpu migrate project --to-project … [--events/--people]` | Export → import with no disk, made possible by streaming |

The two sample scripts become one command each. Their READMEs will show the
commands:

```
mpu export events --from 2024-01-01 --to 2024-04-30 --split auto -o exported_files/ --zstd
mpu import events exported_files/
```

---

## 8. Safety and security

- **`#![forbid(unsafe_code)]` in every first-party crate.** Unsafe code in
  dependencies is tracked with `cargo-geiger` reports and reviewed through
  `cargo-vet`.
- **Lints.** `clippy::pedantic`. In library code, deny `unwrap_used`,
  `expect_used`, `panic`, `indexing_slicing`, `arithmetic_side_effects` (use
  checked, saturating or explicit wrapping ops) and `print_stdout`. Libraries never
  panic on input.
- **Secrets.**
  - `secrecy::SecretString` plus `zeroize`; `Debug` is redacted.
  - The auth header is built per request from the secret and is never logged.
  - The CLI refuses a secret passed as a flag unless `--insecure-secret-arg` is
    given.
  - Config files must be 0600.
- **Untrusted archives.**
  - Zip entries are checked for zip-slip (no absolute paths or `..`).
  - Per-entry and total decompressed-size caps, plus a ratio limit, stop
    decompression bombs.
  - Only temp files in a job directory are used.
- **Supply chain.**
  - `cargo-deny` checks advisories, licenses (Apache-2.0-compatible), duplicate
    and banned crates, and sources.
  - `cargo-audit` runs in CI.
  - `Cargo.lock` is committed.
  - Dependencies use minimal features.
  - Dependabot with the existing 30-day cooldown.
  - Actions stay pinned by SHA, matching repo convention.
  - GitHub artifact attestations (SLSA provenance) on release binaries.
  - PyPI and crates.io publish through trusted publishing (OIDC). No long-lived
    tokens.
- **Public API stability.** `cargo-semver-checks` runs on the facade crate.

---

## 9. Testing and verification

1. **Behaviour spec and differential tests.**
   - Phase 0 runs every v3 public method against a recording fake transport and
     saves the exact request stream as golden files: method, URL, query, headers
     minus secrets, and decoded body.
   - The Rust implementation must produce **semantically equal** requests: same
     records, fields and operations, ignoring batching boundaries and key order.
     Where §2 fixes a bug, the difference is intentional and each one is marked.
2. **Mock Mixpanel** (`tools/mock-mixpanel`, axum). It emulates `/import` (strict
   mode, `failed_records`), `/engage`, `/groups`, `/export` (streamed JSONL,
   gzip), `/query/engage` (`session_id` paging) and JQL. A scriptable fault plan can
   inject:
   - 429s with `Retry-After`
   - 5xx errors
   - slow responses
   - connection resets mid-body
   - truncated gzip
   - 413s
   - partial strict failures

   It records every accepted record, so tests can assert "each input record accepted
   exactly once, or dead-lettered with a reason."
3. **Deterministic simulation.** Run the full pipeline under `turmoil` (simulated
   network and clock) with seeded random faults, partitions and crashes. On crash,
   restart with `--resume`. Check the accounting invariant and "no duplicates
   without `$insert_id` synthesis turned off." Thousands of seeds run nightly; a
   failing seed is a reproducible test case.
4. **Property tests** (`proptest`):
   - splice output equals DOM editing
   - the framer handles any chunking of a byte stream
   - the batcher never exceeds its limits and emits each record exactly once
   - the journal watermark never passes an unacknowledged record
   - CSV round-trips
5. **Fuzzing** (`cargo-fuzz`). Targets: NDJSON framer, array scanner, format
   sniffer, CSV→record, shallow parser/splicer, strict-mode response parser,
   `/engage` page parser, zip entry validation. CI runs 60 s per target on each PR
   and 1 h per target nightly. Apply to OSS-Fuzz later.
6. **Concurrency.** Time-dependent tests (backoff, rate limits, linger) use tokio
   time-pausing. `loom` runs only if we hand-write a synchronization primitive; by
   default we use tokio's.
7. **Ported tests.** The 58 pytest cases are rewritten as table-driven `rstest`
   unit tests plus `trybuild` compile-fail tests. The Python package also re-runs
   the **existing pytest suite** against the v4 bindings. The URL tests change from
   patching `urllib` to asserting against the mock server's request log.
8. **Live tests.** A nightly job runs against a dedicated sandbox Mixpanel project
   (secrets in the `release` environment). It is small and kept under the rate
   limits.
9. **Performance gates.**
   - Micro benchmarks (`divan`): framing, splice, batch encode, gzip/zstd.
   - Instruction-count benchmarks (`gungraun`, formerly iai-callgrind) run on every
     PR; a regression over 3% fails CI.
   - A macro benchmark imports 10 M events into the mock server, and another
     exports 1 GB. They record throughput, CPU-seconds and peak RSS, compare
     against the Python baseline, and run nightly.
10. **Coverage.** `cargo-llvm-cov` uploads to Codecov, as today.

CI matrix: Linux, macOS and Windows × {stable, MSRV}. Every PR runs `cargo fmt`,
`clippy -D warnings`, `cargo nextest`, `cargo-deny`, the fuzz smoke tests and
instruction-count benchmarks. The existing PR-title check stays.

---

## 10. Build, performance tuning and distribution

- **Release profile.** `lto = "fat"`, `codegen-units = 1`, `opt-level = 3`,
  `strip = true`. `panic = "abort"` applies only to the CLI binary; the library and
  Python extension unwind so panics can cross the boundary as errors.
- **PGO + BOLT** via `cargo-pgo`, trained on the macro benchmark workloads. This
  usually gains 10–20% on parse-heavy code; we keep it only if the gain is measured.
- **CPU targets.** Portable baseline (x86-64-v2, aarch64). An x86-64-v3 CLI build is
  offered if benchmarks justify it; `memchr` already dispatches at runtime.
- **Artifacts.**
  - CLI: `dist` (cargo-dist) builds GitHub Releases for Linux gnu+musl (static),
    macOS arm64/x86-64 and Windows, plus shell/PowerShell installers, a Homebrew tap
    and a distroless container image.
  - Library: crates.io.
  - Python: `maturin` wheels: manylinux and musllinux (x86-64, aarch64), macOS
    universal2, Windows; `abi3-py39` plus free-threaded `cp314t` wheels.
- **Release process.** Extend the existing `.github/modules.json` model to three
  modules: `rust` (tag `rust-v*`), `cli` (`cli-v*`), `python` (`v*`, continuing
  PyPI numbering). The prepare-release and publish workflows keep their shape and
  read versions from `Cargo.toml` or `pyproject.toml` instead of `setup.py`.
- **Toolchain.** Edition 2024. MSRV is "stable minus 2" (1.94 is installed in this
  environment; 1.98 is current stable), pinned in `rust-toolchain.toml`, and
  checked in CI.

---

## 11. Python compatibility (package v4)

- `from mixpanel_utils import MixpanelUtils` keeps working with the same method
  names and keyword arguments, including the constructor's validation messages
  checked by `tests/test_init_auth.py`.
- **The GIL is released for all I/O and CPU work** (`Python::detach`, PyO3 ≥ 0.26).
  On free-threaded CPython, Python callbacks run in parallel.
- **Inputs:**
  - A file path is read entirely in Rust (fast path).
  - A Python list is converted in chunks through `pythonize`.
  - An iterable or generator is newly accepted and streamed.
- **Outputs:** `query_*` return lazy iterators that behave like lists (they support
  `len()` and indexing). `export_*` write from Rust.
- **Intentional behaviour changes**, documented in `CHANGELOG.md` under a v4
  migration guide:
  - typed exceptions (`MixpanelError` and subclasses) instead of `BaseException` or
    `None`
  - operations return a summary object that also works as an int count
  - side files go to a job directory
  - NDJSON is the default export format (`format="json"` still writes an array)
  - 429 and strict-mode failures are handled instead of dropped
- **v3 support.** v3 moves to `python-legacy/` and gets security fixes only, until
  the end-of-life date announced with v4.

---

## 12. Library API sketch

```rust
use mixpanel_utils::{Client, Residency, ProjectId, ServiceAccount, ProfileOp, Selector, Source, Tz};

let client = Client::builder()
    .service_account(ServiceAccount::from_env()?)      // MP_SA_USERNAME / MP_SA_SECRET
    .project_id(ProjectId::try_from("1234567")?)
    .residency(Residency::Eu)
    .build()?;

// Import: streaming, bounded memory, resumable
let report = client
    .import_events(Source::glob("exports/*.ndjson.zst")?)
    .source_tz(Tz::iana("America/Los_Angeles")?)
    .dead_letter("failed.ndjson")
    .journal("import.mpu-journal")
    .run()
    .await?;
assert_eq!(report.read, report.accepted + report.rejected + report.skipped);

// Profile operation with a computed value
client.people()
    .select(Selector::where_(r#"properties["$city"] == "Albany""#))
    .apply_with(|p| Some(ProfileOp::set([("favorite_color", p.prop("color")?.clone())])))
    .backup("albany-backup.ndjson.zst")
    .run()
    .await?;

// Export as a stream
let mut events = client.export_events("2026-01-01".parse()?..="2026-01-31".parse()?).stream().await?;
while let Some(ev) = events.try_next().await? { /* ev: RawRecord (zero-copy Bytes) */ }
```

A `mixpanel_utils::blocking` module wraps these calls for simple scripts, the same
way reqwest does.

---

## 13. Phased roadmap

Each phase ends with a demo and a merged PR series. Effort is in engineer-weeks
(ew).

| Phase | Scope | Exit criteria | Effort |
|---|---|---|---|
| **0. Spec and spikes** | Behaviour spec plus golden request corpus from v3; mock server v0; Python baseline benchmarks (throughput, CPU, RSS); spikes: hyper+tower vs reqwest, serde_json vs sonic-rs, h1 vs h2, gzip levels; **sandbox checks**: gzip and batch cap on `/engage` and `/groups`, whether engage paging counts against 60/h, `/api/query/engage` parity, JQL availability | Written decision record per spike; targets in §4 confirmed or revised | 1.5–2 |
| **1. Foundations** | Workspace, CI (lint, test, deny, coverage), `mpu-core` (types, config, errors, endpoints, secrets, time), `mpu-transport` (layers, retry, rate and adaptive limits); port the 58 pytest cases | URL, auth and validation tests pass; the transport survives the mock fault plan | 2 |
| **2. Codec and pipeline engine** | Framing, sniffing, shallow parse/splice, CSV, compression, batcher, memory budget, journal, dead-letter, cancellation | Proptest and fuzz targets green; framing ≥ 1 GB/s per core; RSS bounded by the budget | 2.5 |
| **3. Import** | Events, people and groups import; strict-mode parsing; `$insert_id` synthesis; resume | 10 M events at the mock-emulated cap with ≤ 1.5 cores and ≤ budget RSS; 0 unaccounted records across 1,000 simulation seeds | 2 |
| **4. Export and query** | Raw export (adaptive windows, atomic files, resume), profile and group query streaming, CSV/NDJSON/array sinks, `legacy-jql` | Byte-identical to v3 output on the golden corpus (NDJSON mode aside); rate budgets never exceeded in simulation | 2 |
| **5. Profile operations** | All `ProfileOp`s, closures and jaq transforms, rename, revenue, scalable dedupe with dry-run, export-based event counts | Differential tests match v3 on the corpus; dedupe of 50 M synthetic profiles within budget | 2 |
| **6. Migrations** | Amplitude (US/EU, original and v3 ID modes, streaming zip, bomb caps), project-to-project | Amplitude fixtures give the same output as v3 **after** the §2 fixes; no temp explosion | 1.5 |
| **7. CLI and Python bindings** | `mpu` UX, config profiles, progress, summaries, exit codes; PyO3 v4 package, existing pytest suite green | `tools/mixpanel_utils_example.py` runs unchanged on v4; CLI docs complete | 3 |
| **8. Hardening and release** | Nightly simulation and fuzz, PGO, docs (mdBook + rustdoc + migration guide), release pipelines, attestations; beta with 2–3 internal or customer pilot jobs | Pilots finish at least one production-size job each; no P0/P1 issues open; v4.0.0 / `mpu` 1.0 tagged | 2 |

Total: about 18.5 ew (range 16–20). Phases 4, 5 and 6 can run in parallel after
phase 3 lands the pipeline engine.

---

## 14. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Undocumented server behaviour (`/engage` gzip, batch caps, whether paging counts against 60/h) | Test in Phase 0 against a sandbox; every such behaviour is a config setting with a safe default |
| JQL removed outright | Already feature-gated; the main dependent (`event_counts_to_people`) moves to raw export |
| Export-based event counts differ from JQL (identity resolution; JQL did an inner join with `People()`) | Phase 0 compares both on a sandbox project; document the difference; optionally join with the profile set streamed from `/query/engage` |
| Python users rely on lambdas and list return values | PyO3 shim keeps both; the existing pytest suite runs against v4 |
| Over-engineering past the server cap | Benchmark gate in §3; spikes decide between the simple and complex options |
| `$insert_id` synthesis changes dedup semantics for customers who deliberately re-import | Documented; `--no-synthesize-insert-id` turns it off |
| Retrying non-idempotent `$add`/`$append` | Not retried after ambiguous failures by default; explicit opt-in |
| Customers share a rate budget with other tools | Adaptive limiter plus configurable caps; 429s pause only the affected family |
| Team Rust experience | Few crates, mainstream dependencies, and an architecture doc; unsafe is forbidden |
| Wheel and platform matrix cost | `maturin` + `dist` automation; abi3 keeps the wheel count down |

---

## 15. Decisions needed from the team

1. **Which ships first: the CLI, or the Python v4 package?** Recommendation: CLI
   first (fastest to validate at scale), Python v4 second, both on the same core.
2. **Python v4 compatibility level:** keep the API unchanged with the behaviour
   fixes in §11 (recommended), or also clean up the API?
3. **JQL:** keep `legacy-jql` in default builds for v4.0 (recommended), or drop it?
4. **Repository:** Rust at the root of this repo with v3 in `python-legacy/`
   (recommended), or a new repository?
5. **Default for `$insert_id` synthesis:** on (recommended, for safe retries and
   resume) or off.

---

## Sources

- Mixpanel Import Events API: https://docs.mixpanel.com/reference/import-events
- Mixpanel Raw Event Export API: https://docs.mixpanel.com/reference/raw-event-export
- Mixpanel Query Profiles (engage) API: https://docs.mixpanel.com/reference/engage-query
- Mixpanel Profile Batch Update: https://docs.mixpanel.com/reference/profile-batch-update
- Mixpanel Group Set Property: https://docs.mixpanel.com/reference/group-set-property
- Mixpanel JQL status: https://docs.mixpanel.com/docs/reports/apps/jql
- Amplitude Export API: https://amplitude.com/docs/apis/analytics/export
- reqwest 0.13 (rustls by default): https://seanmonstar.com/blog/reqwest-v013-rustls-default/
- PyO3 0.26 (`attach`/`detach`, free-threaded 3.14t): https://newreleases.io/project/cargo/pyo3/release/0.26.0
- sonic-rs: https://docs.rs/crate/sonic-rs/latest
- zlib-rs: https://trifectatech.org/projects/zlib-rs/
- Rust release cadence (1.98.1, Sep 2026): https://blog.rust-lang.org/inside-rust/2026/09/02/1.98.1-prerelease/
