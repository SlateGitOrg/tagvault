//! Scripted plant scenario and the capture file format.
//!
//! A [`Scenario`] runs simulated edge nodes against a real [`Host`] over a lossy "broker" link:
//! some `DDATA` messages are lost, some arrive swapped, and one node restarts (NDEATH from its
//! MQTT will, then a new session with `bdSeq + 1`). Rebirth requests from the host are fed back
//! to the nodes, so the recorded stream contains the full gap → timeout → `NCMD` → rebirth
//! cycle. The result is a list of [`CaptureFrame`]s (what the host received, when), which the
//! `ingest` command replays offline.
//!
//! File format: `"TVCAP001"` followed by framed records
//! `ivarint(recv_ts) str(topic) uvarint(len) payload`.

use super::edge::{EdgeNode, SpMessage};
use super::rng::Rng;
use crate::encoding::varint::{put_ivarint, put_str, put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::sparkplug::{Host, HostConfig, Outbound};
use crate::storage::frame;
use crate::types::Timestamp;
use serde::Serialize;
use std::path::Path;

const MAGIC: &[u8; 8] = b"TVCAP001";

/// 2026-01-05T00:00:00Z — default scenario start.
pub const DEFAULT_START: Timestamp = 1_767_571_200_000;

/// One message as received by the host.
#[derive(Debug, Clone, PartialEq)]
pub struct CaptureFrame {
    /// Host receive time (ms since epoch).
    pub recv_ts: Timestamp,
    /// MQTT topic.
    pub topic: String,
    /// Sparkplug payload bytes.
    pub payload: Vec<u8>,
}

/// Scenario parameters.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Seed for every random choice.
    pub seed: u64,
    /// Start time.
    pub start: Timestamp,
    /// Length in seconds.
    pub duration_s: u64,
    /// Edge scan interval.
    pub scan_ms: i64,
    /// Number of edge nodes (`Line1`, `Line2`, …).
    pub nodes: usize,
    /// Sparkplug group id.
    pub group: String,
    /// Probability that a DDATA is lost on the way to the host.
    pub drop_prob: f64,
    /// Probability that two consecutive DDATA of a node arrive swapped.
    pub swap_prob: f64,
    /// `(at_s, offline_s)`: node 1 dies at `at_s` and is back after `offline_s`.
    pub node_restart: Option<(u64, u64)>,
    /// One-way broker latency.
    pub latency_ms: i64,
    /// Host settings used for the rebirth feedback loop.
    pub host: HostConfig,
}

impl Default for Scenario {
    fn default() -> Self {
        Scenario {
            seed: 7,
            start: DEFAULT_START,
            duration_s: 2 * 3600,
            scan_ms: 1000,
            nodes: 2,
            group: "Plant1".into(),
            drop_prob: 0.0003,
            swap_prob: 0.001,
            node_restart: Some((3600, 120)),
            latency_ms: 20,
            host: HostConfig::default(),
        }
    }
}

/// What happened while generating.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScenarioSummary {
    /// Messages published by the edge nodes.
    pub published: u64,
    /// Messages delivered to (and recorded for) the host.
    pub delivered: u64,
    /// DDATA lost in transit.
    pub dropped: u64,
    /// DDATA pairs delivered swapped.
    pub swapped: u64,
    /// Node restarts.
    pub restarts: u64,
    /// Rebirths requested by the host and honoured by a node.
    pub rebirths: u64,
}

fn is_ddata(m: &SpMessage) -> bool {
    m.topic.contains("/DDATA/")
}

impl Scenario {
    /// Run the scenario.
    pub fn generate(&self) -> (Vec<CaptureFrame>, ScenarioSummary) {
        let mut rng = Rng::new(self.seed ^ 0x00C0_FFEE);
        let mut nodes: Vec<EdgeNode> = (0..self.nodes)
            .map(|i| {
                EdgeNode::plant_cell(
                    &self.group,
                    &format!("Line{}", i + 1),
                    self.seed.wrapping_mul(1000).wrapping_add(i as u64),
                )
            })
            .collect();
        let mut host = Host::new(self.host.clone());
        let mut frames = Vec::new();
        let mut summary = ScenarioSummary::default();
        let mut pending: Vec<(Timestamp, Outbound)> = Vec::new();

        let deliver = |m: SpMessage,
                       at: Timestamp,
                       frames: &mut Vec<CaptureFrame>,
                       pending: &mut Vec<(Timestamp, Outbound)>,
                       host: &mut Host| {
            let out = host.handle(&m.topic, &m.payload, at);
            for c in out.commands {
                pending.push((at + self.latency_ms, c));
            }
            frames.push(CaptureFrame {
                recv_ts: at,
                topic: m.topic,
                payload: m.payload,
            });
        };

        for node in &mut nodes {
            for m in node.birth(self.start) {
                summary.published += 1;
                deliver(
                    m,
                    self.start + self.latency_ms,
                    &mut frames,
                    &mut pending,
                    &mut host,
                );
            }
        }
        let steps = self.duration_s as i64 * 1000 / self.scan_ms;
        let dt_s = self.scan_ms as f64 / 1000.0;
        for k in 1..=steps {
            let now = self.start + k * self.scan_ms;
            pending.sort_by_key(|(t, _)| *t);
            while pending.first().is_some_and(|(t, _)| *t <= now) {
                let (_, cmd) = pending.remove(0);
                for node in &mut nodes {
                    if node.command(&cmd.topic, &cmd.payload) {
                        summary.rebirths += 1;
                    }
                }
            }
            for (i, node) in nodes.iter_mut().enumerate() {
                if let (0, Some((at_s, off_s))) = (i, self.node_restart) {
                    let die = self.start + at_s as i64 * 1000;
                    let back = die + off_s as i64 * 1000;
                    if now == die {
                        summary.restarts += 1;
                        summary.published += 1;
                        let m = node.death(now);
                        deliver(
                            m,
                            now + self.latency_ms,
                            &mut frames,
                            &mut pending,
                            &mut host,
                        );
                    }
                    if now >= die && now < back {
                        node.scan(now, dt_s); // process keeps running; nothing is published
                        continue;
                    }
                    if now == back {
                        node.scan(now, dt_s);
                        for m in node.birth(now) {
                            summary.published += 1;
                            deliver(
                                m,
                                now + self.latency_ms,
                                &mut frames,
                                &mut pending,
                                &mut host,
                            );
                        }
                        continue;
                    }
                }
                let mut msgs = node.scan(now, dt_s);
                summary.published += msgs.len() as u64;
                msgs.retain(|m| {
                    let lose = is_ddata(m) && rng.chance(self.drop_prob);
                    summary.dropped += u64::from(lose);
                    !lose
                });
                if msgs.len() >= 2
                    && is_ddata(&msgs[0])
                    && is_ddata(&msgs[1])
                    && rng.chance(self.swap_prob)
                {
                    msgs.swap(0, 1);
                    summary.swapped += 1;
                }
                for m in msgs {
                    deliver(
                        m,
                        now + self.latency_ms,
                        &mut frames,
                        &mut pending,
                        &mut host,
                    );
                }
            }
            for c in host.tick(now).commands {
                pending.push((now + self.latency_ms, c));
            }
        }
        summary.delivered = frames.len() as u64;
        (frames, summary)
    }
}

/// Write frames to a capture file.
pub fn write_capture(path: &Path, frames: &[CaptureFrame]) -> Result<()> {
    let mut out = Vec::with_capacity(frames.len() * 128);
    out.extend_from_slice(MAGIC);
    let mut rec = Vec::new();
    for f in frames {
        rec.clear();
        put_ivarint(&mut rec, f.recv_ts);
        put_str(&mut rec, &f.topic);
        put_uvarint(&mut rec, f.payload.len() as u64);
        rec.extend_from_slice(&f.payload);
        frame::encode(&rec, &mut out);
    }
    std::fs::write(path, out)?;
    Ok(())
}

/// Read a capture file; any damage is an error (a capture is an input, not a log).
pub fn read_capture(path: &Path) -> Result<Vec<CaptureFrame>> {
    let buf = std::fs::read(path)?;
    if buf.len() < MAGIC.len() || &buf[..MAGIC.len()] != MAGIC {
        return Err(Error::Decode(format!(
            "{} is not a tagvault capture",
            path.display()
        )));
    }
    let body = &buf[MAGIC.len()..];
    let scan = frame::scan(body);
    if scan.valid_len != body.len() {
        return Err(Error::Corrupt(format!(
            "capture {} is truncated or damaged",
            path.display()
        )));
    }
    scan.frames
        .into_iter()
        .map(|r| {
            let mut c = Cursor::new(&body[r]);
            let recv_ts = c.ivarint()?;
            let topic = c.string()?;
            let len = c.uvarint()? as usize;
            let payload = c.bytes(len)?.to_vec();
            Ok(CaptureFrame {
                recv_ts,
                topic,
                payload,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_is_deterministic_and_exercises_the_protocol() {
        let sc = Scenario {
            duration_s: 1800,
            node_restart: Some((600, 30)),
            drop_prob: 0.002,
            swap_prob: 0.01,
            ..Default::default()
        };
        let (a, sa) = sc.generate();
        let (b, sb) = sc.generate();
        assert_eq!(a, b);
        assert_eq!(sa, sb);
        assert!(sa.dropped > 0 && sa.swapped > 0, "{sa:?}");
        assert_eq!(sa.restarts, 1);
        assert!(
            sa.rebirths >= 1,
            "lost messages must lead to honoured rebirths: {sa:?}"
        );
        assert_eq!(sa.delivered as usize, a.len());

        // Replaying the capture through a fresh host reproduces the same decisions.
        let mut host = Host::new(sc.host.clone());
        let mut cmds = 0;
        for f in &a {
            cmds += host.handle(&f.topic, &f.payload, f.recv_ts).commands.len();
        }
        assert!(cmds >= 1);
        assert!(host.stats().reordered >= 1);
        assert!(host.stats().samples > 10_000);
    }

    #[test]
    fn capture_file_roundtrip_and_damage() {
        let sc = Scenario {
            duration_s: 60,
            node_restart: None,
            ..Default::default()
        };
        let (frames, _) = sc.generate();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.tvcap");
        write_capture(&p, &frames).unwrap();
        assert_eq!(read_capture(&p).unwrap(), frames);
        let bytes = std::fs::read(&p).unwrap();
        std::fs::write(&p, &bytes[..bytes.len() - 2]).unwrap();
        assert!(read_capture(&p).is_err());
        std::fs::write(&p, b"nope").unwrap();
        assert!(read_capture(&p).is_err());
    }
}
