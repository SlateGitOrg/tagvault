//! The collector (interface node): Sparkplug host → exception filter → store-and-forward spool.
//!
//! The collector owns no archive. It turns MQTT messages into exception-filtered samples,
//! groups them into batches, spools each batch durably, and drains the spool into whatever
//! [`Sink`] reaches the archive (in-process or HTTP). Rebirth commands produced by the host are
//! returned to the caller, which publishes them on MQTT.

use crate::compress::deadband::ExceptionFilter;
use crate::config::Policy;
use crate::error::Result;
use crate::forward::{Forwarder, PumpReport, Sink};
use crate::sparkplug::{Host, HostEvent, HostStats, Outbound};
use crate::types::{Point, Sample, Timestamp, ValueKind};
use serde::Serialize;
use std::collections::HashMap;

/// Collector counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CollectorStats {
    /// Samples decoded from Sparkplug.
    pub samples_in: u64,
    /// Samples that passed the exception filter.
    pub samples_reported: u64,
    /// Batches written to the spool.
    pub batches_spooled: u64,
    /// Batches refused by a full spool.
    pub batches_refused: u64,
}

struct TagFilter {
    kind: ValueKind,
    filter: ExceptionFilter,
}

/// The collector.
pub struct Collector {
    host: Host,
    policy: Policy,
    filters: HashMap<String, TagFilter>,
    forwarder: Forwarder,
    pending: Vec<Sample>,
    batch_max: usize,
    stats: CollectorStats,
    events: Vec<HostEvent>,
    keep_events: bool,
}

impl Collector {
    /// Build a collector. `batch_max` caps samples per spooled batch.
    pub fn new(host: Host, policy: Policy, forwarder: Forwarder, batch_max: usize) -> Self {
        Collector {
            host,
            policy,
            filters: HashMap::new(),
            forwarder,
            pending: Vec::new(),
            batch_max: batch_max.max(1),
            stats: CollectorStats::default(),
            events: Vec::new(),
            keep_events: false,
        }
    }

    /// Retain host events (for the demo's narrative output); off by default.
    pub fn keep_events(&mut self, on: bool) {
        self.keep_events = on;
    }

    /// Drain retained host events.
    pub fn take_events(&mut self) -> Vec<HostEvent> {
        std::mem::take(&mut self.events)
    }

    fn filter_sample(&mut self, s: Sample) {
        self.stats.samples_in += 1;
        let policy = &self.policy;
        let tf = self.filters.entry(s.tag.clone()).or_insert_with(|| TagFilter {
            kind: s.kind,
            filter: ExceptionFilter::new(policy.resolve(&s.tag).exception, s.kind),
        });
        let mut out: Vec<Point> = Vec::with_capacity(2);
        tf.filter.offer(s.point, &mut out);
        let kind = tf.kind;
        for p in out {
            self.stats.samples_reported += 1;
            self.pending.push(Sample { tag: s.tag.clone(), kind, point: p });
        }
    }

    /// Handle one MQTT message; returns commands (rebirth requests) to publish.
    pub fn on_message(&mut self, topic: &str, payload: &[u8], now: Timestamp) -> Result<Vec<Outbound>> {
        let out = self.host.handle(topic, payload, now);
        if self.keep_events {
            self.events.extend(out.events);
        }
        for s in out.samples {
            self.filter_sample(s);
        }
        if self.pending.len() >= self.batch_max {
            self.spool_pending()?;
        }
        Ok(out.commands)
    }

    /// Periodic housekeeping: reorder timeouts and spooling of buffered samples.
    pub fn tick(&mut self, now: Timestamp) -> Result<Vec<Outbound>> {
        let out = self.host.tick(now);
        if self.keep_events {
            self.events.extend(out.events);
        }
        self.spool_pending()?;
        Ok(out.commands)
    }

    /// Write buffered samples to the spool as one batch.
    pub fn spool_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.pending);
        if self.forwarder.enqueue(&batch)? {
            self.stats.batches_spooled += 1;
        } else {
            self.stats.batches_refused += 1;
        }
        Ok(())
    }

    /// Report every tag's held point (so the archive gets the latest values) and spool.
    pub fn flush_filters(&mut self) -> Result<()> {
        let mut held: Vec<Sample> = self
            .filters
            .iter_mut()
            .filter_map(|(name, tf)| tf.filter.flush().map(|p| Sample { tag: name.clone(), kind: tf.kind, point: p }))
            .collect();
        held.sort_by(|a, b| a.tag.cmp(&b.tag));
        self.stats.samples_reported += held.len() as u64;
        self.pending.extend(held);
        self.spool_pending()
    }

    /// Try to forward spooled batches.
    pub fn pump(&mut self, sink: &mut dyn Sink, now: Timestamp, max_batches: usize) -> Result<PumpReport> {
        self.forwarder.pump(sink, now, max_batches)
    }

    /// Collector counters.
    pub fn stats(&self) -> CollectorStats {
        self.stats
    }

    /// Host counters.
    pub fn host_stats(&self) -> &HostStats {
        self.host.stats()
    }

    /// The forwarder (for spool/forward counters).
    pub fn forwarder(&self) -> &Forwarder {
        &self.forwarder
    }
}

/// Outcome of [`replay`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ReplayReport {
    /// Frames replayed.
    pub frames: u64,
    /// Rebirth commands the host would have published.
    pub commands: u64,
    /// Pump attempts where the link was down.
    pub link_failures: u64,
    /// Largest spool backlog (batches) observed.
    pub max_backlog: u64,
    /// Batches still spooled at the end (0 unless the link never came back).
    pub left_in_spool: u64,
}

/// Replay captured MQTT frames through a collector into `sink`, using the frames' receive times
/// as the clock. Housekeeping (reorder timeouts, spooling, forwarding) runs every `tick_ms`.
pub fn replay(
    frames: &[crate::sim::CaptureFrame],
    collector: &mut Collector,
    sink: &mut dyn Sink,
    tick_ms: i64,
) -> Result<ReplayReport> {
    let mut report = ReplayReport::default();
    let mut next_tick: Option<Timestamp> = None;
    let mut now = 0;
    for f in frames {
        now = f.recv_ts;
        report.commands += collector.on_message(&f.topic, &f.payload, now)?.len() as u64;
        report.frames += 1;
        let due = *next_tick.get_or_insert(now + tick_ms);
        if now >= due {
            report.commands += collector.tick(now)?.len() as u64;
            let r = collector.pump(sink, now, 64)?;
            report.link_failures += u64::from(r.link_failed);
            report.max_backlog = report.max_backlog.max(collector.forwarder().spool().stats().pending_records);
            next_tick = Some(now + tick_ms);
        }
    }
    collector.tick(now)?;
    collector.flush_filters()?;
    // Drain: keep advancing the clock until the spool is empty or nothing moves for a while.
    let mut idle = 0;
    while !collector.forwarder().spool().is_empty() && idle < 2_000 {
        now += tick_ms;
        let r = collector.pump(sink, now, 256)?;
        report.link_failures += u64::from(r.link_failed);
        idle = if r.delivered + r.dead_lettered > 0 { 0 } else { idle + 1 };
    }
    report.left_in_spool = collector.forwarder().spool().stats().pending_records;
    Ok(report)
}
