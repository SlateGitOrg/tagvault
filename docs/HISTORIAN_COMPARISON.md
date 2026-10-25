# tagvault compared with PI/AVEVA-style historians

tagvault is a small-scale implementation of the architecture that commercial process
historians popularised. The best-known of those is the OSIsoft PI System, now AVEVA PI. This
page maps tagvault's components onto that publicly documented architecture and says plainly
where tagvault is simpler.

Nothing here is a benchmark against PI or any other product. None was run, and the products are
not comparable in scope.

## Concept map

| PI System concept (public terminology) | tagvault equivalent | notes |
|---|---|---|
| PI point (tag) with point attributes | tag + policy rule (`fixtures/policy.json`) | tagvault attributes: `exc_dev`, `exc_max_ms`, `comp_dev`, `comp_max_ms`, `comp_mode`. No ExcMin/CompMin and no percent-of-span forms; deviations are absolute (see DECISIONS) |
| Interface / connector node | `Collector` (`src/collector.rs`) | decodes the field protocol, applies exception reporting, buffers |
| Exception reporting (ExcDev, ExcMax) | `compress::deadband` | same "send the previous value too" rule; guarantee: hold ≤ `e`, linear ≤ `2e` |
| PI Buffer Subsystem | `forward::spool` + `Forwarder` | disk FIFO, durable cursor, backoff, overflow policy, at-least-once |
| Snapshot subsystem / snapshot value | the open compression segment (`Sdt::pending`, `snapshot`) | lookback mode keeps *every* sample since the last archived point, so recent queries are exact |
| Compression (swinging door, CompDev/CompMax) | `compress::sdt` | three modes. The default (lookback) proves `≤ comp_dev` while archiving only measured values; see [SDT.md](SDT.md) |
| Archive files, archive subsystem | WAL + immutable segment files | segments are columnar, Gorilla-encoded and checksummed; the WAL makes the snapshot/compression state crash-safe |
| Digital states (e.g. I/O Timeout) | `Quality::{Bad, Stale}` | an NDEATH/DDEATH writes a Stale point; queries treat the signal as undefined until the next good value |
| Step attribute | `ValueKind::Discrete` (booleans) | discrete tags hold their value and archive only changes |
| Interpolated / plot / summary calls | `query::{interpolated, aggregate}` | aggregates: time-weighted average, min, max, count, coverage |
| PI Web API | `net::http` | a small read API plus binary ingest. No auth, no writes beyond ingest |

## Where the designs match on purpose

* **Two-stage reduction.** A cheap deadband at the edge saves link bandwidth, and a smarter
  compressor at the archive saves storage. tagvault keeps the split. It also states the bound
  the composition provides (`2·exc + comp`), which is usually left implicit, and property-tests
  it.
* **Store-and-forward at the collector.** Plants lose WAN links. Buffering has to happen before
  the network hop and survive a restart of the collector. tagvault's spool is on disk with an
  atomically replaced cursor.
* **Snapshot plus archive.** The newest value is always queryable, even though compression has
  not decided whether to keep it yet.

## Where tagvault is deliberately different

* **Bounded-error SDT by default.** Textbook swinging door archives the measured value of the
  held point when the doors cross. On real-looking noise that can violate the configured
  deviation at intermediate samples. The benchmark measured violations on 16/16 held-out
  pressure series and 2/16 temperature series.
  tagvault's default *lookback* rule archives the latest point whose straight line provably
  covers everything before it. The bound holds, and compression stays close to textbook
  (374.8x vs 610.6x on tank level).
* **Sparkplug B as the first-class protocol.** Births, aliases, sequence numbers, reorder
  window, rebirth requests and `bdSeq` death matching are built in, rather than added through a
  connector.
* **Exact crash recovery.** The WAL stores raw accepted points plus checkpointed compressor
  state. A crash at any byte recovers to exactly the committed prefix, which is property-tested.

## What a production historian has that tagvault does not

* High availability (collectives, replicated archives, failover) and backfill/edit/annotation
  workflows.
* Security: authentication, authorization, TLS on every hop, auditing.
* An asset framework, calculations and analytics, event frames, notifications.
* Retention management and archive file rollover by time. tagvault segments grow until you
  delete them.
* Out-of-order inserts into history. tagvault rejects points older than a tag's latest one; see
  Limitations in the README.
* Thousands of tested interfaces (OPC UA/DA, Modbus, vendor drivers). tagvault speaks Sparkplug B
  over MQTT 3.1.1 only.
* Scale engineering: tagvault was measured on one laptop with 64 tags and 1.4 M points, nothing
  more.
