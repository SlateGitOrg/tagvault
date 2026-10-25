//! Store-and-forward: buffer batches on disk while the link to the archive is down, and drain
//! them in order when it returns (the role of the PI Buffer Subsystem on an interface node).

pub mod batch;
pub mod spool;

use crate::error::Result;
use crate::storage::Archive;
use crate::types::{Sample, Timestamp};
use serde::Serialize;
use spool::Spool;
use std::fmt;

/// Why a delivery failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryError {
    /// Transient: keep the batch and retry later (link down, server error).
    Unavailable(String),
    /// Permanent: the archive refused the batch; retrying cannot help.
    Rejected(String),
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeliveryError::Unavailable(m) => write!(f, "unavailable: {m}"),
            DeliveryError::Rejected(m) => write!(f, "rejected: {m}"),
        }
    }
}

/// Destination of forwarded batches.
pub trait Sink {
    /// Deliver one encoded batch (see [`batch`]).
    fn deliver(&mut self, payload: &[u8], now: Timestamp)
        -> std::result::Result<(), DeliveryError>;
}

/// Delivers into an archive in the same process.
pub struct ArchiveSink<'a> {
    archive: &'a mut Archive,
    /// Points rejected by the archive as out-of-order/duplicates.
    pub rejected_points: u64,
}

impl<'a> ArchiveSink<'a> {
    /// Wrap an open archive.
    pub fn new(archive: &'a mut Archive) -> Self {
        ArchiveSink {
            archive,
            rejected_points: 0,
        }
    }
}

impl Sink for ArchiveSink<'_> {
    fn deliver(
        &mut self,
        payload: &[u8],
        _now: Timestamp,
    ) -> std::result::Result<(), DeliveryError> {
        let samples = batch::decode(payload).map_err(|e| DeliveryError::Rejected(e.to_string()))?;
        let r = self
            .archive
            .write(&samples)
            .map_err(|e| DeliveryError::Unavailable(e.to_string()))?;
        self.rejected_points += r.rejected_out_of_order;
        Ok(())
    }
}

/// Wraps a sink with scheduled link outages `[start, end)` (simulation time), for demos/tests.
pub struct OutageSink<S> {
    inner: S,
    outages: Vec<(Timestamp, Timestamp)>,
}

impl<S: Sink> OutageSink<S> {
    /// Wrap `inner`; deliveries during an outage fail as [`DeliveryError::Unavailable`].
    pub fn new(inner: S, outages: Vec<(Timestamp, Timestamp)>) -> Self {
        OutageSink { inner, outages }
    }

    /// Unwrap.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Sink> Sink for OutageSink<S> {
    fn deliver(
        &mut self,
        payload: &[u8],
        now: Timestamp,
    ) -> std::result::Result<(), DeliveryError> {
        if self.outages.iter().any(|&(a, b)| now >= a && now < b) {
            return Err(DeliveryError::Unavailable(
                "link down (scheduled outage)".into(),
            ));
        }
        self.inner.deliver(payload, now)
    }
}

/// Exponential retry backoff.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    /// First delay.
    pub initial_ms: i64,
    /// Ceiling.
    pub max_ms: i64,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            initial_ms: 500,
            max_ms: 30_000,
        }
    }
}

/// Result of one [`Forwarder::pump`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PumpReport {
    /// Batches delivered.
    pub delivered: u64,
    /// Batches dropped as permanently rejected.
    pub dead_lettered: u64,
    /// True if the link failed during this pump.
    pub link_failed: bool,
}

/// Cumulative forwarding counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ForwardStats {
    /// Batches enqueued.
    pub enqueued: u64,
    /// Batches delivered.
    pub delivered: u64,
    /// Failed delivery attempts.
    pub failed_attempts: u64,
    /// Batches dropped as permanently rejected.
    pub dead_lettered: u64,
    /// Largest number of batches waiting at once.
    pub max_backlog: u64,
}

/// Drains a [`Spool`] into a [`Sink`] with backoff.
#[derive(Debug)]
pub struct Forwarder {
    spool: Spool,
    backoff: Backoff,
    delay_ms: i64,
    next_attempt: Timestamp,
    stats: ForwardStats,
}

impl Forwarder {
    /// New forwarder over an open spool.
    pub fn new(spool: Spool, backoff: Backoff) -> Self {
        Forwarder {
            spool,
            backoff,
            delay_ms: 0,
            next_attempt: Timestamp::MIN,
            stats: ForwardStats::default(),
        }
    }

    /// Spool a batch of samples. Returns `false` if the spool refused it (full).
    pub fn enqueue(&mut self, samples: &[Sample]) -> Result<bool> {
        if samples.is_empty() {
            return Ok(true);
        }
        let ok = self.spool.push(&batch::encode(samples))?;
        if ok {
            self.stats.enqueued += 1;
            self.stats.max_backlog = self
                .stats
                .max_backlog
                .max(self.spool.stats().pending_records);
        }
        Ok(ok)
    }

    /// Deliver up to `max_batches` if the backoff allows an attempt at `now`.
    pub fn pump(
        &mut self,
        sink: &mut dyn Sink,
        now: Timestamp,
        max_batches: usize,
    ) -> Result<PumpReport> {
        let mut report = PumpReport::default();
        if now < self.next_attempt {
            return Ok(report);
        }
        let entries = self.spool.peek(max_batches)?;
        for e in entries {
            match sink.deliver(&e.payload, now) {
                Ok(()) => {
                    self.spool.ack(e.pos, 1)?;
                    report.delivered += 1;
                    self.delay_ms = 0;
                }
                Err(DeliveryError::Rejected(why)) => {
                    log::error!("dead-lettering batch: {why}");
                    self.spool.ack(e.pos, 1)?;
                    report.dead_lettered += 1;
                }
                Err(DeliveryError::Unavailable(why)) => {
                    self.delay_ms = if self.delay_ms == 0 {
                        self.backoff.initial_ms
                    } else {
                        (self.delay_ms * 2).min(self.backoff.max_ms)
                    };
                    self.next_attempt = now + self.delay_ms;
                    self.stats.failed_attempts += 1;
                    log::debug!("delivery failed ({why}); retry in {} ms", self.delay_ms);
                    report.link_failed = true;
                    break;
                }
            }
        }
        self.stats.delivered += report.delivered;
        self.stats.dead_lettered += report.dead_lettered;
        Ok(report)
    }

    /// Counters.
    pub fn stats(&self) -> ForwardStats {
        self.stats
    }

    /// The underlying spool.
    pub fn spool(&self) -> &Spool {
        &self.spool
    }
}

#[cfg(test)]
mod tests {
    use super::spool::SpoolOptions;
    use super::*;
    use crate::storage::SyncMode;
    use crate::types::{Point, ValueKind};

    struct Recorder {
        got: Vec<Vec<Sample>>,
        reject_next: bool,
    }

    impl Sink for Recorder {
        fn deliver(
            &mut self,
            payload: &[u8],
            _now: Timestamp,
        ) -> std::result::Result<(), DeliveryError> {
            if std::mem::take(&mut self.reject_next) {
                return Err(DeliveryError::Rejected("bad".into()));
            }
            self.got.push(batch::decode(payload).unwrap());
            Ok(())
        }
    }

    fn sample(i: i64) -> Sample {
        Sample {
            tag: "t".into(),
            kind: ValueKind::Analog,
            point: Point::good(i, i as f64),
        }
    }

    #[test]
    fn buffers_during_outage_then_drains_in_order_with_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(
            dir.path(),
            SpoolOptions {
                sync: SyncMode::Os,
                ..Default::default()
            },
        )
        .unwrap();
        let mut fwd = Forwarder::new(
            spool,
            Backoff {
                initial_ms: 100,
                max_ms: 400,
            },
        );
        let mut sink = OutageSink::new(
            Recorder {
                got: vec![],
                reject_next: false,
            },
            vec![(1000, 2000)],
        );
        for i in 0..5 {
            fwd.enqueue(&[sample(i)]).unwrap();
        }
        assert!(fwd.enqueue(&[]).unwrap());
        let r = fwd.pump(&mut sink, 1000, 10).unwrap();
        assert!(r.link_failed);
        assert_eq!(
            fwd.pump(&mut sink, 1050, 10).unwrap(),
            PumpReport::default(),
            "inside backoff window"
        );
        fwd.pump(&mut sink, 1100, 10).unwrap(); // fails again, delay doubles to 200
        fwd.pump(&mut sink, 1300, 10).unwrap(); // 400
        fwd.pump(&mut sink, 1700, 10).unwrap(); // capped at 400
        assert_eq!(fwd.stats().failed_attempts, 4);
        let r = fwd.pump(&mut sink, 2100, 3).unwrap();
        assert_eq!(r.delivered, 3);
        fwd.pump(&mut sink, 2101, 10).unwrap();
        let rec = sink.into_inner();
        let order: Vec<i64> = rec.got.iter().map(|b| b[0].point.ts).collect();
        assert_eq!(order, vec![0, 1, 2, 3, 4]);
        assert!(fwd.spool().is_empty());
        assert_eq!(fwd.stats().max_backlog, 5);
    }

    #[test]
    fn rejected_batches_are_dead_lettered_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(
            dir.path(),
            SpoolOptions {
                sync: SyncMode::Os,
                ..Default::default()
            },
        )
        .unwrap();
        let mut fwd = Forwarder::new(spool, Backoff::default());
        fwd.enqueue(&[sample(1)]).unwrap();
        fwd.enqueue(&[sample(2)]).unwrap();
        let mut sink = Recorder {
            got: vec![],
            reject_next: true,
        };
        let r = fwd.pump(&mut sink, 0, 10).unwrap();
        assert_eq!((r.delivered, r.dead_lettered), (1, 1));
        assert_eq!(sink.got.len(), 1);
        assert_eq!(fwd.stats().dead_lettered, 1);
    }

    #[test]
    fn archive_sink_writes_and_counts_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::open(dir.path(), Default::default()).unwrap();
        let mut sink = ArchiveSink::new(&mut archive);
        let payload = batch::encode(&[sample(1), sample(2)]);
        sink.deliver(&payload, 0).unwrap();
        sink.deliver(&payload, 0).unwrap();
        assert_eq!(sink.rejected_points, 2);
        assert!(matches!(
            sink.deliver(b"junk", 0),
            Err(DeliveryError::Rejected(_))
        ));
        assert_eq!(
            DeliveryError::Unavailable("x".into()).to_string(),
            "unavailable: x"
        );
        assert_eq!(
            DeliveryError::Rejected("y".into()).to_string(),
            "rejected: y"
        );
    }
}
