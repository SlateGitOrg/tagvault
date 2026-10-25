# Decisions

Rulings made while building tagvault without access to the requester. Each entry gives
`what — why — cost if wrong`.

## Protocol and ingest

- Ruling: Sparkplug B protobuf types are hand-written `prost` derives (`src/sparkplug/proto.rs`) instead of `prost-build` + `protoc` — no `protoc` on the build machine or in CI, and the schema is small and stable — if the schema drifts or a field number is wrong, decoding silently ignores it. Mitigated by decode tests that use an independent wire encoder (`tests/common/mod.rs`).
- Ruling: only scalar metric values are historized. DataSet, Template, String/Text/UUID, Bytes/File and property sets are decoded or skipped and counted as `unsupported_values` — a historian of process values is numeric — users who need string tags (batch IDs, states as text) get nothing stored for them.
- Ruling: all values are stored as `f64`. Int64/UInt64 lose precision above 2^53 — one value codec keeps Gorilla XOR encoding simple — 64-bit counters beyond 2^53 are rounded.
- Ruling: signed Int8/16/32 are sign-extended from the declared width of the `uint32` field — accepts both zero- and sign-extended encoders, and the spec examples use small negative values — none known.
- Ruling: a sequence gap is held in a reorder buffer for `reorder_timeout_ms` (2 s). On timeout the buffered messages are discarded and a rebirth is requested — Sparkplug 3.0 asks hosts to wait a reorder timeout and then request a rebirth; buffered messages could depend on a missing DBIRTH or DDEATH — data between the gap and the rebirth is lost (counted in `discarded`; the demo shows 74 messages over 2 h with a lossy link).
- Ruling: one unknown alias or metric in a DATA message discards the whole message and requests a rebirth — the host's alias view is wrong, so the other aliases cannot be trusted either — a single stale alias costs the whole message.
- Ruling: rebirth requests are rate-limited to one per node per 5 s — avoids NCMD storms while a node is restarting — a node that ignores the first request waits 5 s for the next.
- Ruling: `bdSeq` and `Node Control/*` / `Device Control/*` metrics are not historized — they describe the session, not the process — anyone who wants them as tags must change `is_control_metric`.
- Ruling: NDEATH or DDEATH writes a `Stale` point (value NaN) for every affected tag at the death timestamp — mirrors the "I/O Timeout" digital state of PI-style historians, and queries can show the gap honestly — none.
- Ruling: the MQTT client is a minimal MQTT 3.1.1 implementation written for this crate (QoS 0/1, no TLS) rather than a crate such as rumqttc — keeps the dependency tree small and synchronous, and the tests use an in-process fake broker; `make integration` exercises a real Mosquitto broker — no TLS, QoS 2 or persistent sessions (listed in Limitations).
- Ruling: offline replay of recorded captures (`simulate` → `ingest`) is the primary demo and test path — `make check`/`make demo` must run without network or Docker, and replays are deterministic — the live MQTT path gets less exercise (one fake-broker test plus the optional Docker test).
- Ruling: the "spec example" decode tests are modelled on the example messages of Sparkplug 3.0 §6 (names, types and values of the Raspberry Pi / Pibrella examples), not copied byte for byte — no network fetch of the specification during the build; the protobuf bytes come from an independent encoder — a subtle disagreement with the official examples would not be caught.

## Compression

- Ruling: exception filtering runs at the collector and swinging door at the archive, as in PI — exception filtering saves link bandwidth before the spool, and compression needs the archive's state — none.
- Ruling: the default SDT mode is **lookback** (latest-feasible-point archiving with a ≤ 4 096-point open-segment buffer). **Strict** and **classic** are selectable — classic violated its bound on 16/16 held-out pressure series; strict archives 6.5x more points than lookback on tank level (57.7x vs 374.8x reduction). Lookback keeps measured values and a proven bound at near-classic compression — per-tag memory of up to 4 096 buffered points, and a worst-case re-run cost of O(cap) when a segment is cut.
- Ruling: a "fitted" mode (classic decisions, archiving the closest feasible value instead of the measurement) was prototyped and dropped before commit — it also guarantees the bound, but in an exploratory benchmark run it reached about 153x vs lookback's about 375x on tank level, because every fitted anchor sits on the edge of the corridor, and it stores values that were never measured — the prototype code was not kept, so that figure is not reproducible from this repository and is quoted only to explain the decision.
- Ruling: deviations are absolute engineering units, not percent of span — tags from Sparkplug carry no span metadata — users must compute deviations from spans themselves (the benchmark tunes them as fractions of span).
- Ruling: policy rules are full-name globs (`*` matches any characters including `/`), and the rule with the most literal characters wins — plant naming is hierarchical, so suffix patterns like `*/Tank01/Level` are natural — two equally specific rules resolve to the first one listed.
- Ruling: the policy file is the source of truth on open. Changed settings are applied to existing tags and checkpointed immediately; read-only tools never apply it — WAL replay must use the settings the records were written with — a mistyped policy applied to an archive takes effect at once (the old settings are not kept).
- Ruling: the benchmark sets `exc_dev = comp_dev / 2` — the long-standing PI rule of thumb — other ratios were not explored.
- Ruling: benchmark tuning objective is "largest grid deviation with mean RMSE ≤ 0.1 % of span" on tuning seeds 1–8, evaluated on held-out seeds 101–116 — a simple, explainable target, and the error bound is guaranteed anyway — noisy classes (flow) end up below their noise floor and barely compress (1.3x). The trade-off table shows what larger deviations would give.

## Storage

- Ruling: the WAL logs every accepted *raw* point plus checkpoints of the full compressor state, and recovery replays through the same code — gives bit-exact recovery (property-tested) instead of "recovers something plausible" — WAL volume follows the raw rate between flushes (bounded by the 32 MiB rotation).
- Ruling: a segment newer than the WAL checkpoint is deleted on open — its contents are still in the WAL, which is authoritative — none (re-flushed on the next flush).
- Ruling: a damaged segment that the checkpoint covers is a hard error, not silently skipped — silent data loss is worse than refusing to open — an operator must intervene; the error names the file.
- Ruling: per-tag timestamps must strictly increase; older points are rejected and counted — keeps chunks append-only and makes spool redelivery idempotent — Sparkplug `is_historical` data older than a tag's latest value (edge store-and-forward that arrives after live data) is dropped.
- Ruling: a torn or CRC-failing WAL frame ends the log, and everything after it is truncated — the standard torn-write rule; bit rot in the middle of a WAL is indistinguishable from a torn tail — mid-log corruption loses the records after it.
- Ruling: `SyncMode::Always` (fsync per commit) is the default for archive, spool and the live MQTT path. `ingest` of a capture defaults to `--sync os` — a replay can always be re-run, and fsync per one-second batch made a 2 h replay take 135 s instead of 1.5 s on macOS — none for replays; the live path pays about 5 ms per commit (measured p50 4.93 ms).
- Ruling: on macOS `sync_data` is a plain `fsync`, not `F_FULLFSYNC` — standard library behaviour — on macOS a power loss can lose data the drive cache had acknowledged (documented limitation).
- Ruling: archive and spool directories are locked with `File::try_lock` (stable since Rust 1.89, hence `rust-version = 1.89`) — prevents two writers corrupting one directory — `query` cannot run while `serve` owns the archive; use the HTTP API instead.
- Ruling: Gorilla timestamps add a per-chunk GCD unit and a 32-bit dod bucket — SDT output has irregular gaps measured in seconds to hours — slightly larger than the paper's format for perfectly regular series (one varint).

## Store-and-forward

- Ruling: the default overflow policy is `DropOldest` with a 256 MiB cap — freshest data matters most to operators, and losses are counted — long outages lose the start of the outage rather than the end.
- Ruling: permanently rejected batches (HTTP 4xx, undecodable) are dead-lettered (acknowledged and counted) — one poison batch must not block the queue forever — the data in that batch is lost.

## Queries and API

- Ruling: queries never extrapolate past the last point (the snapshot); values are undefined there — honest about what is known — dashboards that expect a held last value must ask for raw points.
- Ruling: the time-weighted average divides by covered time and reports `coverage` — avoids treating bad or unknown periods as zero — consumers must look at `coverage`.
- Ruling: the HTTP server (tiny_http) has no authentication and binds to `127.0.0.1:8080` by default — scope; this is a local tool — exposing it on a network exposes plant data and an ingest endpoint (listed in Limitations).

## Engineering

- Ruling: one crate (library + binary), synchronous code, no async runtime — the workloads are I/O-light, and it keeps the code readable — a high-fan-in deployment would need an async or multi-threaded collector.
- Ruling: deterministic SplitMix64 PRNG in `sim::rng` instead of the `rand` crate — generated signals must be identical across platforms and crate versions so benchmarks are reproducible — none.
- Ruling: `rustfmt.toml` sets `max_width = 120` — default formatting spread struct literals over many lines — none.
- Ruling: the non-test source is ~7 500 lines (≈ 5 900 excluding comments and blank lines), above the typical 2 500–5 000 — the spec covers a protocol stack, MQTT, HTTP, storage engine, spool, query engine, simulator and benchmark — more to review; no module is padding (each is exercised by tests and the demo).
