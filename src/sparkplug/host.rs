//! Sparkplug B Host Application state machine.
//!
//! Implements the host-side rules of the Sparkplug 3.0 specification that matter to a historian:
//!
//! * **Births** (`NBIRTH`/`DBIRTH`) define the metric set, datatypes and aliases of a session.
//!   An `NBIRTH` resets the node session (all devices offline until their `DBIRTH`).
//! * **Data** (`NDATA`/`DDATA`) is resolved by alias (or name) against the birth definitions.
//! * **Sequence numbers** (`seq`, 0–255 wrapping) are checked on every message after `NBIRTH`.
//!   An out-of-order message is held in a reorder buffer; if the gap is filled within
//!   `reorder_timeout_ms` the buffered messages are applied in order, otherwise the buffer is
//!   discarded and a rebirth is requested.
//! * **Rebirth** is requested by publishing `NCMD` with `Node Control/Rebirth = true` whenever
//!   the host cannot trust its session state: data before birth, sequence gap timeout, unknown
//!   alias, metric or device, or a missing sequence number. Requests are rate-limited per node.
//! * **Deaths** (`NDEATH`/`DDEATH`) mark the affected tags `Stale`. An `NDEATH` whose `bdSeq`
//!   does not match the current session's birth is a late death from an older session and is
//!   ignored.

use super::datatype::DataType;
use super::proto::{metric, Metric, Payload};
use super::topic::{MessageType, Topic};
use crate::types::{Point, Quality, Sample, Timestamp};
use prost::Message;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;

/// Host behaviour settings.
#[derive(Debug, Clone)]
pub struct HostConfig {
    /// Host application id (used for the `STATE` topic by the MQTT client).
    pub host_id: String,
    /// How long to wait for a missing sequence number before requesting a rebirth.
    pub reorder_timeout_ms: i64,
    /// Minimum interval between two rebirth requests to the same node.
    pub rebirth_cooldown_ms: i64,
}

impl Default for HostConfig {
    fn default() -> Self {
        HostConfig { host_id: "tagvault".into(), reorder_timeout_ms: 2_000, rebirth_cooldown_ms: 5_000 }
    }
}

/// A message the host wants published (rebirth commands).
#[derive(Debug, Clone, PartialEq)]
pub struct Outbound {
    /// MQTT topic.
    pub topic: String,
    /// Encoded Sparkplug payload.
    pub payload: Vec<u8>,
}

/// Why a rebirth was requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebirthReason {
    /// Data or device message arrived for a node without a live birth.
    NoSession,
    /// A sequence gap was not filled within the reorder timeout.
    ReorderTimeout,
    /// A data metric referenced an alias that was never born.
    UnknownAlias(u64),
    /// A data metric referenced a name that was never born.
    UnknownMetric(String),
    /// A device message referenced a device that is not online.
    UnknownDevice(String),
    /// A sequenced message had no (or an out-of-range) `seq`.
    BadSequence,
    /// A birth certificate was malformed (e.g. a metric without a name).
    MalformedBirth,
}

impl fmt::Display for RebirthReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RebirthReason::NoSession => write!(f, "message before birth"),
            RebirthReason::ReorderTimeout => {
                write!(f, "sequence gap not filled within reorder timeout")
            }
            RebirthReason::UnknownAlias(a) => write!(f, "unknown alias {a}"),
            RebirthReason::UnknownMetric(n) => write!(f, "unknown metric {n:?}"),
            RebirthReason::UnknownDevice(d) => write!(f, "device {d:?} not online"),
            RebirthReason::BadSequence => write!(f, "missing or invalid seq"),
            RebirthReason::MalformedBirth => write!(f, "malformed birth certificate"),
        }
    }
}

/// Notable things that happened while handling a message (for logs and tests).
#[derive(Debug, Clone, PartialEq)]
pub enum HostEvent {
    /// Edge node came online.
    NodeBirth {
        /// Node path `group/edge`.
        node: String,
        /// Birth/death sequence number from the `bdSeq` metric.
        bd_seq: Option<u64>,
        /// Number of metrics defined.
        metrics: usize,
    },
    /// Edge node went offline.
    NodeDeath {
        /// Node path.
        node: String,
    },
    /// Device came online.
    DeviceBirth {
        /// Node path.
        node: String,
        /// Device id.
        device: String,
        /// Number of metrics defined.
        metrics: usize,
    },
    /// Device went offline.
    DeviceDeath {
        /// Node path.
        node: String,
        /// Device id.
        device: String,
    },
    /// A sequence number arrived ahead of the expected one.
    SequenceGap {
        /// Node path.
        node: String,
        /// Expected sequence number.
        expected: u8,
        /// Received sequence number.
        got: u8,
    },
    /// Buffered out-of-order messages were applied after the gap filled.
    GapRecovered {
        /// Node path.
        node: String,
        /// Number of buffered messages applied.
        messages: usize,
    },
    /// An `NCMD` rebirth request was issued.
    RebirthRequested {
        /// Node path.
        node: String,
        /// Why.
        reason: RebirthReason,
    },
    /// A message was dropped.
    Discarded {
        /// Topic of the dropped message.
        topic: String,
        /// Why.
        reason: String,
    },
}

/// Result of handling one message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostOutput {
    /// Historizable samples, in message order.
    pub samples: Vec<Sample>,
    /// Commands to publish.
    pub commands: Vec<Outbound>,
    /// Diagnostic events.
    pub events: Vec<HostEvent>,
}

/// Running counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HostStats {
    /// Messages handled.
    pub messages: u64,
    /// NBIRTH + DBIRTH messages applied.
    pub births: u64,
    /// NDEATH + DDEATH messages applied.
    pub deaths: u64,
    /// NDATA + DDATA messages applied.
    pub data_messages: u64,
    /// Samples produced.
    pub samples: u64,
    /// Sequence gaps detected.
    pub seq_gaps: u64,
    /// Messages applied late from the reorder buffer.
    pub reordered: u64,
    /// Rebirth commands issued.
    pub rebirth_requests: u64,
    /// Messages dropped.
    pub discarded: u64,
    /// NDEATHs ignored because their bdSeq belonged to an older session.
    pub stale_deaths_ignored: u64,
    /// Metric values skipped because their type is not numeric.
    pub unsupported_values: u64,
    /// Metric values skipped because they were flagged transient.
    pub transient_skipped: u64,
    /// Payloads that failed protobuf decoding.
    pub decode_errors: u64,
    /// STATE messages seen.
    pub state_messages: u64,
}

#[derive(Debug, Clone)]
struct MetricDef {
    device: Option<String>,
    name: String,
    datatype: DataType,
}

#[derive(Debug, Default)]
struct NodeSession {
    online: bool,
    bd_seq: Option<u64>,
    expected: u8,
    by_alias: HashMap<u64, MetricDef>,
    by_name: HashMap<(Option<String>, String), MetricDef>,
    devices: HashMap<String, bool>,
    reorder: HashMap<u8, (Topic, Payload)>,
    gap_since: Option<Timestamp>,
    awaiting_rebirth: bool,
    last_rebirth_request: Option<Timestamp>,
}

impl NodeSession {
    fn register(&mut self, def: MetricDef, alias: Option<u64>) {
        if let Some(a) = alias {
            self.by_alias.insert(a, def.clone());
        }
        self.by_name.insert((def.device.clone(), def.name.clone()), def);
    }

    fn forget_device(&mut self, device: &str) {
        self.by_alias.retain(|_, d| d.device.as_deref() != Some(device));
        self.by_name.retain(|(d, _), _| d.as_deref() != Some(device));
    }
}

/// True for metrics that describe the session rather than the process.
fn is_control_metric(name: &str) -> bool {
    name == "bdSeq" || name.starts_with("Node Control/") || name.starts_with("Device Control/")
}

fn bd_seq_of(p: &Payload) -> Option<u64> {
    p.metrics.iter().find(|m| m.name.as_deref() == Some("bdSeq")).and_then(|m| match m.value {
        Some(metric::Value::LongValue(v)) => Some(v),
        Some(metric::Value::IntValue(v)) => Some(v as u64),
        _ => None,
    })
}

/// Build the `NCMD` payload asking an edge node to republish its births.
pub fn rebirth_command(group: &str, edge: &str, now: Timestamp) -> Outbound {
    let payload = Payload {
        timestamp: Some(now as u64),
        metrics: vec![Metric {
            name: Some("Node Control/Rebirth".into()),
            timestamp: Some(now as u64),
            datatype: Some(DataType::Boolean.code()),
            value: Some(metric::Value::BooleanValue(true)),
            ..Default::default()
        }],
        ..Default::default()
    };
    Outbound { topic: Topic::node(MessageType::NCmd, group, edge).to_string(), payload: payload.encode_to_vec() }
}

/// The host application.
#[derive(Debug, Default)]
pub struct Host {
    cfg: HostConfig,
    sessions: HashMap<(String, String), NodeSession>,
    stats: HostStats,
}

impl Host {
    /// New host with the given settings.
    pub fn new(cfg: HostConfig) -> Self {
        Host { cfg, sessions: HashMap::new(), stats: HostStats::default() }
    }

    /// Counters so far.
    pub fn stats(&self) -> &HostStats {
        &self.stats
    }

    /// Settings in force.
    pub fn config(&self) -> &HostConfig {
        &self.cfg
    }

    /// `Some(true)` if the node has a live session, `Some(false)` if known but offline.
    pub fn node_online(&self, group: &str, edge: &str) -> Option<bool> {
        self.sessions.get(&(group.to_string(), edge.to_string())).map(|s| s.online && !s.awaiting_rebirth)
    }

    /// Handle one MQTT message received at `now` (ms since epoch, host clock).
    pub fn handle(&mut self, topic: &str, payload: &[u8], now: Timestamp) -> HostOutput {
        let mut out = HostOutput::default();
        self.stats.messages += 1;
        let topic = match Topic::parse(topic) {
            Ok(t) => t,
            Err(e) => {
                self.discard(&mut out, topic, &e.to_string());
                return out;
            }
        };
        match topic.kind {
            MessageType::State => {
                self.stats.state_messages += 1;
                return out;
            }
            MessageType::NCmd | MessageType::DCmd => return out,
            _ => {}
        }
        let payload = match Payload::decode(payload) {
            Ok(p) => p,
            Err(e) => {
                self.stats.decode_errors += 1;
                self.discard(&mut out, &topic.to_string(), &format!("protobuf: {e}"));
                return out;
            }
        };
        match topic.kind {
            MessageType::NBirth => self.on_nbirth(topic, payload, now, &mut out),
            MessageType::NDeath => self.on_ndeath(topic, payload, now, &mut out),
            _ => self.on_sequenced(topic, payload, now, &mut out),
        }
        out
    }

    /// Evaluate reorder timeouts without a new message (call periodically).
    pub fn tick(&mut self, now: Timestamp) -> HostOutput {
        let mut out = HostOutput::default();
        let keys: Vec<(String, String)> = self.sessions.keys().cloned().collect();
        for key in keys {
            self.check_reorder_timeout(&key, now, &mut out);
        }
        out
    }

    fn discard(&mut self, out: &mut HostOutput, topic: &str, reason: &str) {
        self.stats.discarded += 1;
        log::debug!("discarding message on {topic}: {reason}");
        out.events.push(HostEvent::Discarded { topic: topic.to_string(), reason: reason.to_string() });
    }

    fn request_rebirth(&mut self, key: &(String, String), reason: RebirthReason, now: Timestamp, out: &mut HostOutput) {
        let cooldown = self.cfg.rebirth_cooldown_ms;
        let sess = self.sessions.entry(key.clone()).or_default();
        sess.awaiting_rebirth = true;
        sess.reorder.clear();
        sess.gap_since = None;
        if sess.last_rebirth_request.is_some_and(|t| now - t < cooldown) {
            return;
        }
        sess.last_rebirth_request = Some(now);
        self.stats.rebirth_requests += 1;
        let node = format!("{}/{}", key.0, key.1);
        log::info!("requesting rebirth from {node}: {reason}");
        out.commands.push(rebirth_command(&key.0, &key.1, now));
        out.events.push(HostEvent::RebirthRequested { node, reason });
    }

    fn on_nbirth(&mut self, topic: Topic, payload: Payload, now: Timestamp, out: &mut HostOutput) {
        let key = (topic.group.clone(), topic.edge.clone());
        let Some(seq) = payload.seq.filter(|s| *s <= 255) else {
            self.discard(out, &topic.to_string(), "NBIRTH without valid seq");
            self.request_rebirth(&key, RebirthReason::BadSequence, now, out);
            return;
        };
        if payload.metrics.iter().any(|m| m.name.is_none()) {
            self.discard(out, &topic.to_string(), "NBIRTH metric without name");
            self.request_rebirth(&key, RebirthReason::MalformedBirth, now, out);
            return;
        }
        let last_req = self.sessions.get(&key).and_then(|s| s.last_rebirth_request);
        let mut sess = NodeSession {
            online: true,
            bd_seq: bd_seq_of(&payload),
            expected: (seq as u8).wrapping_add(1),
            last_rebirth_request: last_req,
            ..Default::default()
        };
        let payload_ts = payload.timestamp;
        for m in &payload.metrics {
            let def = MetricDef {
                device: None,
                name: m.name.clone().unwrap_or_default(),
                datatype: DataType::from_code(m.datatype.unwrap_or(0)),
            };
            if let Some(s) = sample_for(&def, &topic, m, payload_ts, now, &mut self.stats) {
                out.samples.push(s);
            }
            sess.register(def, m.alias);
        }
        self.stats.births += 1;
        out.events.push(HostEvent::NodeBirth {
            node: format!("{}/{}", topic.group, topic.edge),
            bd_seq: sess.bd_seq,
            metrics: payload.metrics.len(),
        });
        self.sessions.insert(key, sess);
    }

    fn on_ndeath(&mut self, topic: Topic, payload: Payload, now: Timestamp, out: &mut HostOutput) {
        let key = (topic.group.clone(), topic.edge.clone());
        let death_bd = bd_seq_of(&payload);
        let Some(sess) = self.sessions.get_mut(&key).filter(|s| s.online) else {
            self.discard(out, &topic.to_string(), "NDEATH for offline node");
            return;
        };
        if let (Some(have), Some(got)) = (sess.bd_seq, death_bd) {
            if have != got {
                self.stats.stale_deaths_ignored += 1;
                self.discard(out, &topic.to_string(), &format!("NDEATH bdSeq {got} != session bdSeq {have}"));
                return;
            }
        }
        sess.online = false;
        sess.reorder.clear();
        sess.gap_since = None;
        for d in sess.devices.values_mut() {
            *d = false;
        }
        let ts = payload.timestamp.map(|t| t as i64).unwrap_or(now);
        let mut defs: Vec<&MetricDef> = sess.by_name.values().collect();
        defs.sort_by(|a, b| (&a.device, &a.name).cmp(&(&b.device, &b.name)));
        for def in defs {
            if let Some(s) = stale_sample(&topic, def, ts) {
                out.samples.push(s);
            }
        }
        self.stats.samples += out.samples.len() as u64;
        self.stats.deaths += 1;
        out.events.push(HostEvent::NodeDeath { node: format!("{}/{}", topic.group, topic.edge) });
    }

    fn on_sequenced(&mut self, topic: Topic, payload: Payload, now: Timestamp, out: &mut HostOutput) {
        let key = (topic.group.clone(), topic.edge.clone());
        let live = self.sessions.get(&key).is_some_and(|s| s.online && !s.awaiting_rebirth);
        if !live {
            self.discard(out, &topic.to_string(), "no live session");
            self.request_rebirth(&key, RebirthReason::NoSession, now, out);
            return;
        }
        let Some(seq) = payload.seq.filter(|s| *s <= 255).map(|s| s as u8) else {
            self.discard(out, &topic.to_string(), "missing seq");
            self.request_rebirth(&key, RebirthReason::BadSequence, now, out);
            return;
        };
        let sess = self.sessions.get_mut(&key).expect("checked above");
        let diff = seq.wrapping_sub(sess.expected);
        if diff == 0 {
            sess.expected = sess.expected.wrapping_add(1);
            if let Err(reason) = self.apply(&key, topic, payload, now, out) {
                self.request_rebirth(&key, reason, now, out);
                return;
            }
            let mut drained = 0usize;
            loop {
                let sess = self.sessions.get_mut(&key).expect("session exists");
                let Some((t, p)) = sess.reorder.remove(&sess.expected) else {
                    break;
                };
                sess.expected = sess.expected.wrapping_add(1);
                drained += 1;
                if let Err(reason) = self.apply(&key, t, p, now, out) {
                    self.request_rebirth(&key, reason, now, out);
                    return;
                }
            }
            let sess = self.sessions.get_mut(&key).expect("session exists");
            if drained > 0 {
                self.stats.reordered += drained as u64;
                out.events.push(HostEvent::GapRecovered { node: format!("{}/{}", key.0, key.1), messages: drained });
            }
            if sess.reorder.is_empty() {
                sess.gap_since = None;
            }
        } else if diff < 128 {
            if sess.reorder.is_empty() {
                self.stats.seq_gaps += 1;
                out.events.push(HostEvent::SequenceGap {
                    node: format!("{}/{}", key.0, key.1),
                    expected: sess.expected,
                    got: seq,
                });
            }
            sess.gap_since.get_or_insert(now);
            sess.reorder.insert(seq, (topic, payload));
        } else {
            self.discard(out, &topic.to_string(), &format!("duplicate or stale seq {seq}"));
        }
        self.check_reorder_timeout(&key, now, out);
    }

    fn check_reorder_timeout(&mut self, key: &(String, String), now: Timestamp, out: &mut HostOutput) {
        let timeout = self.cfg.reorder_timeout_ms;
        let Some(sess) = self.sessions.get_mut(key) else {
            return;
        };
        if sess.gap_since.is_some_and(|g| now - g >= timeout) {
            let dropped = sess.reorder.len() as u64;
            self.stats.discarded += dropped;
            self.request_rebirth(key, RebirthReason::ReorderTimeout, now, out);
        }
    }

    /// Apply an in-order DBIRTH/DDEATH/NDATA/DDATA message to the session.
    fn apply(
        &mut self,
        key: &(String, String),
        topic: Topic,
        payload: Payload,
        now: Timestamp,
        out: &mut HostOutput,
    ) -> Result<(), RebirthReason> {
        let sess = self.sessions.get_mut(key).expect("session exists");
        let node = format!("{}/{}", key.0, key.1);
        let payload_ts = payload.timestamp;
        match topic.kind {
            MessageType::DBirth => {
                let device = topic.device.clone().unwrap_or_default();
                if payload.metrics.iter().any(|m| m.name.is_none()) {
                    return Err(RebirthReason::MalformedBirth);
                }
                sess.forget_device(&device);
                for m in &payload.metrics {
                    let def = MetricDef {
                        device: Some(device.clone()),
                        name: m.name.clone().unwrap_or_default(),
                        datatype: DataType::from_code(m.datatype.unwrap_or(0)),
                    };
                    if let Some(s) = sample_for(&def, &topic, m, payload_ts, now, &mut self.stats) {
                        out.samples.push(s);
                    }
                    sess.register(def, m.alias);
                }
                sess.devices.insert(device.clone(), true);
                self.stats.births += 1;
                out.events.push(HostEvent::DeviceBirth { node, device, metrics: payload.metrics.len() });
            }
            MessageType::DDeath => {
                let device = topic.device.clone().unwrap_or_default();
                if sess.devices.get(&device) != Some(&true) {
                    return Err(RebirthReason::UnknownDevice(device));
                }
                sess.devices.insert(device.clone(), false);
                let ts = payload_ts.map(|t| t as i64).unwrap_or(now);
                let mut defs: Vec<&MetricDef> =
                    sess.by_name.values().filter(|d| d.device.as_deref() == Some(device.as_str())).collect();
                defs.sort_by(|a, b| a.name.cmp(&b.name));
                let before = out.samples.len();
                out.samples.extend(defs.into_iter().filter_map(|d| stale_sample(&topic, d, ts)));
                self.stats.samples += (out.samples.len() - before) as u64;
                self.stats.deaths += 1;
                out.events.push(HostEvent::DeviceDeath { node, device });
            }
            MessageType::NData | MessageType::DData => {
                if let Some(d) = &topic.device {
                    if sess.devices.get(d) != Some(&true) {
                        return Err(RebirthReason::UnknownDevice(d.clone()));
                    }
                }
                // Resolve every metric first: a single unknown alias means the host's view of
                // the session is wrong, so nothing from this message is trusted.
                let mut resolved = Vec::with_capacity(payload.metrics.len());
                for m in &payload.metrics {
                    let def = match (m.alias, &m.name) {
                        (Some(a), _) => sess.by_alias.get(&a).ok_or(RebirthReason::UnknownAlias(a))?,
                        (None, Some(n)) => sess
                            .by_name
                            .get(&(topic.device.clone(), n.clone()))
                            .ok_or_else(|| RebirthReason::UnknownMetric(n.clone()))?,
                        (None, None) => return Err(RebirthReason::UnknownMetric(String::new())),
                    };
                    if def.device != topic.device {
                        return Err(RebirthReason::UnknownMetric(def.name.clone()));
                    }
                    resolved.push((def.clone(), m));
                }
                for (def, m) in resolved {
                    if let Some(s) = sample_for(&def, &topic, m, payload_ts, now, &mut self.stats) {
                        out.samples.push(s);
                    }
                }
                self.stats.data_messages += 1;
            }
            _ => unreachable!("only sequenced message types are applied"),
        }
        Ok(())
    }
}

fn tag_name(topic: &Topic, def: &MetricDef) -> String {
    match &def.device {
        Some(d) => format!("{}/{}/{}/{}", topic.group, topic.edge, d, def.name),
        None => format!("{}/{}/{}", topic.group, topic.edge, def.name),
    }
}

fn stale_sample(topic: &Topic, def: &MetricDef, ts: Timestamp) -> Option<Sample> {
    if is_control_metric(&def.name) {
        return None;
    }
    let kind = def.datatype.value_kind()?;
    Some(Sample { tag: tag_name(topic, def), kind, point: Point::with_quality(ts, f64::NAN, Quality::Stale) })
}

/// Convert one metric into a historizable sample, or `None` if it should not be stored.
fn sample_for(
    def: &MetricDef,
    topic: &Topic,
    m: &Metric,
    payload_ts: Option<u64>,
    now: Timestamp,
    stats: &mut HostStats,
) -> Option<Sample> {
    if is_control_metric(&def.name) {
        return None;
    }
    if m.is_transient == Some(true) {
        stats.transient_skipped += 1;
        return None;
    }
    let Some(kind) = def.datatype.value_kind() else {
        stats.unsupported_values += 1;
        return None;
    };
    let ts = m.timestamp.or(payload_ts).map(|t| t as i64).unwrap_or(now);
    let point = if m.is_null == Some(true) {
        Point::with_quality(ts, f64::NAN, Quality::Bad)
    } else {
        let Some(v) = m.value.as_ref().and_then(|v| def.datatype.to_f64(v)) else {
            stats.unsupported_values += 1;
            return None;
        };
        // Sparkplug 3.0 reserves the `Quality` property: 192 = good, 500 = stale, else bad.
        let quality = match m.properties.as_ref().and_then(|p| p.int("Quality")) {
            None | Some(192) => Quality::Good,
            Some(500) => Quality::Stale,
            Some(_) => Quality::Bad,
        };
        Point::with_quality(ts, v, quality)
    };
    stats.samples += 1;
    Some(Sample { tag: tag_name(topic, def), kind, point })
}
