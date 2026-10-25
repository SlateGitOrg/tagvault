//! Simulated Sparkplug B edge node publishing plant devices.
//!
//! Behaviour follows the edge-node side of Sparkplug 3.0: `NBIRTH` (seq 0, `bdSeq`,
//! `Node Control/Rebirth`) followed by one `DBIRTH` per device with names, aliases and datatypes;
//! `DDATA` carries alias-only metrics; `seq` increments per message and wraps at 255; an
//! `NCMD` `Node Control/Rebirth = true` makes the node republish its births; `NDEATH` carries the
//! session's `bdSeq` and the next session increments it.

use super::signals::{FirstOrder, MeanReverting, Tank};
use crate::sparkplug::datatype::DataType;
use crate::sparkplug::proto::{metric, Metric, Payload};
use crate::sparkplug::topic::{MessageType, Topic};
use crate::types::Timestamp;
use prost::Message;

/// One published MQTT message.
#[derive(Debug, Clone, PartialEq)]
pub struct SpMessage {
    /// Topic.
    pub topic: String,
    /// Encoded payload.
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Model {
    Tank(Tank),
    Reactor { temp: FirstOrder, pressure: MeanReverting, coolant: FirstOrder },
}

#[derive(Debug, Clone)]
struct MetricSpec {
    name: &'static str,
    alias: u64,
    datatype: DataType,
}

/// A simulated device.
#[derive(Debug, Clone)]
pub struct SimDevice {
    id: String,
    model: Model,
    metrics: Vec<MetricSpec>,
    last: Vec<f64>,
}

impl SimDevice {
    /// A buffer tank: `Level` (Double, %), `InletFlow` (Float, m³/h), `PumpRunning` (Boolean).
    pub fn tank(id: &str, seed: u64, first_alias: u64) -> Self {
        let metrics = vec![
            MetricSpec { name: "Level", alias: first_alias, datatype: DataType::Double },
            MetricSpec { name: "InletFlow", alias: first_alias + 1, datatype: DataType::Float },
            MetricSpec { name: "PumpRunning", alias: first_alias + 2, datatype: DataType::Boolean },
        ];
        let mut d = SimDevice { id: id.into(), model: Model::Tank(Tank::new(seed)), metrics, last: Vec::new() };
        d.last = d.sample(1.0);
        d
    }

    /// A reactor: `Temperature` (Double, °C), `Pressure` (Float, bar), `CoolantFlow` (Float, m³/h).
    pub fn reactor(id: &str, seed: u64, first_alias: u64) -> Self {
        let metrics = vec![
            MetricSpec { name: "Temperature", alias: first_alias, datatype: DataType::Double },
            MetricSpec { name: "Pressure", alias: first_alias + 1, datatype: DataType::Float },
            MetricSpec { name: "CoolantFlow", alias: first_alias + 2, datatype: DataType::Float },
        ];
        let model = Model::Reactor {
            temp: FirstOrder::temperature(seed),
            pressure: MeanReverting::pressure(seed.wrapping_add(1)),
            coolant: FirstOrder::flow(seed.wrapping_add(2)),
        };
        let mut d = SimDevice { id: id.into(), model, metrics, last: Vec::new() };
        d.last = d.sample(1.0);
        d
    }

    fn sample(&mut self, dt_s: f64) -> Vec<f64> {
        match &mut self.model {
            Model::Tank(t) => {
                let r = t.step(dt_s);
                vec![r.level, r.inflow, f64::from(u8::from(r.pump_on))]
            }
            Model::Reactor { temp, pressure, coolant } => vec![temp.step(dt_s), pressure.step(dt_s), coolant.step(dt_s)],
        }
    }

    fn metric(&self, i: usize, ts: Timestamp, birth: bool) -> Metric {
        let spec = &self.metrics[i];
        let v = self.last[i];
        let value = match spec.datatype {
            DataType::Boolean => metric::Value::BooleanValue(v != 0.0),
            DataType::Float => metric::Value::FloatValue(v as f32),
            _ => metric::Value::DoubleValue(v),
        };
        Metric {
            name: birth.then(|| spec.name.to_string()),
            alias: Some(spec.alias),
            timestamp: Some(ts as u64),
            datatype: birth.then_some(spec.datatype.code()),
            value: Some(value),
            ..Default::default()
        }
    }

    /// Device id.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// A simulated edge node.
#[derive(Debug, Clone)]
pub struct EdgeNode {
    group: String,
    edge: String,
    devices: Vec<SimDevice>,
    bd_seq: u64,
    seq: u8,
    online: bool,
    rebirth_pending: bool,
    started: Option<Timestamp>,
}

fn encode(p: Payload) -> Vec<u8> {
    p.encode_to_vec()
}

impl EdgeNode {
    /// New offline node.
    pub fn new(group: &str, edge: &str, devices: Vec<SimDevice>) -> Self {
        EdgeNode {
            group: group.into(),
            edge: edge.into(),
            devices,
            bd_seq: 0,
            seq: 0,
            online: false,
            rebirth_pending: false,
            started: None,
        }
    }

    /// Standard plant cell: a tank and a reactor, aliases starting at 1 (node metric = alias 1).
    pub fn plant_cell(group: &str, edge: &str, seed: u64) -> Self {
        let devices = vec![SimDevice::tank("Tank01", seed, 10), SimDevice::reactor("Reactor01", seed.wrapping_add(100), 20)];
        EdgeNode::new(group, edge, devices)
    }

    fn next_seq(&mut self) -> u64 {
        let s = self.seq;
        self.seq = self.seq.wrapping_add(1);
        s as u64
    }

    /// Whether the node currently has a session.
    pub fn is_online(&self) -> bool {
        self.online
    }

    /// Group id.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Edge node id.
    pub fn edge(&self) -> &str {
        &self.edge
    }

    /// Publish NBIRTH and all DBIRTHs (starting a new session if offline).
    pub fn birth(&mut self, now: Timestamp) -> Vec<SpMessage> {
        self.online = true;
        self.rebirth_pending = false;
        self.seq = 0;
        let started = *self.started.get_or_insert(now);
        let uptime_s = ((now - started) / 1000) as u64;
        let mut out = Vec::with_capacity(1 + self.devices.len());
        let seq = self.next_seq();
        let nbirth = Payload {
            timestamp: Some(now as u64),
            metrics: vec![
                Metric {
                    name: Some("bdSeq".into()),
                    timestamp: Some(now as u64),
                    datatype: Some(DataType::UInt64.code()),
                    value: Some(metric::Value::LongValue(self.bd_seq)),
                    ..Default::default()
                },
                Metric {
                    name: Some("Node Control/Rebirth".into()),
                    timestamp: Some(now as u64),
                    datatype: Some(DataType::Boolean.code()),
                    value: Some(metric::Value::BooleanValue(false)),
                    ..Default::default()
                },
                Metric {
                    name: Some("Uptime".into()),
                    alias: Some(1),
                    timestamp: Some(now as u64),
                    datatype: Some(DataType::UInt64.code()),
                    value: Some(metric::Value::LongValue(uptime_s)),
                    ..Default::default()
                },
            ],
            seq: Some(seq),
            ..Default::default()
        };
        out.push(SpMessage { topic: Topic::node(MessageType::NBirth, &self.group, &self.edge).to_string(), payload: encode(nbirth) });
        for di in 0..self.devices.len() {
            let seq = self.next_seq();
            let d = &self.devices[di];
            let metrics = (0..d.metrics.len()).map(|i| d.metric(i, now, true)).collect();
            let p = Payload { timestamp: Some(now as u64), metrics, seq: Some(seq), ..Default::default() };
            out.push(SpMessage {
                topic: Topic::device(MessageType::DBirth, &self.group, &self.edge, &d.id).to_string(),
                payload: encode(p),
            });
        }
        out
    }

    /// Advance the process by `dt_s` and publish one scan: a DDATA per device (plus an NDATA
    /// uptime every 60 s). If a rebirth was requested, the births are published first.
    pub fn scan(&mut self, now: Timestamp, dt_s: f64) -> Vec<SpMessage> {
        for d in &mut self.devices {
            d.last = d.sample(dt_s);
        }
        if !self.online {
            return Vec::new();
        }
        if self.rebirth_pending {
            return self.birth(now);
        }
        let mut out = Vec::with_capacity(self.devices.len() + 1);
        for di in 0..self.devices.len() {
            let seq = self.next_seq();
            let d = &self.devices[di];
            let metrics = (0..d.metrics.len()).map(|i| d.metric(i, now, false)).collect();
            let p = Payload { timestamp: Some(now as u64), metrics, seq: Some(seq), ..Default::default() };
            out.push(SpMessage {
                topic: Topic::device(MessageType::DData, &self.group, &self.edge, &d.id).to_string(),
                payload: encode(p),
            });
        }
        let started = self.started.unwrap_or(now);
        if (now - started) % 60_000 == 0 {
            let seq = self.next_seq();
            let p = Payload {
                timestamp: Some(now as u64),
                metrics: vec![Metric {
                    alias: Some(1),
                    timestamp: Some(now as u64),
                    value: Some(metric::Value::LongValue(((now - started) / 1000) as u64)),
                    ..Default::default()
                }],
                seq: Some(seq),
                ..Default::default()
            };
            out.push(SpMessage { topic: Topic::node(MessageType::NData, &self.group, &self.edge).to_string(), payload: encode(p) });
        }
        out
    }

    /// The NDEATH the broker publishes from the node's MQTT will (no `seq`).
    pub fn death(&mut self, now: Timestamp) -> SpMessage {
        self.online = false;
        let p = Payload {
            timestamp: Some(now as u64),
            metrics: vec![Metric {
                name: Some("bdSeq".into()),
                datatype: Some(DataType::UInt64.code()),
                value: Some(metric::Value::LongValue(self.bd_seq)),
                ..Default::default()
            }],
            ..Default::default()
        };
        self.bd_seq += 1;
        SpMessage { topic: Topic::node(MessageType::NDeath, &self.group, &self.edge).to_string(), payload: encode(p) }
    }

    /// Handle a command addressed to this node. Returns true if it was a rebirth request.
    pub fn command(&mut self, topic: &str, payload: &[u8]) -> bool {
        let Ok(t) = Topic::parse(topic) else { return false };
        if t.kind != MessageType::NCmd || t.group != self.group || t.edge != self.edge {
            return false;
        }
        let Ok(p) = Payload::decode(payload) else { return false };
        let rebirth = p.metrics.iter().any(|m| {
            m.name.as_deref() == Some("Node Control/Rebirth") && m.value == Some(metric::Value::BooleanValue(true))
        });
        if rebirth && self.online {
            self.rebirth_pending = true;
        }
        rebirth
    }

    /// Device list.
    pub fn devices(&self) -> &[SimDevice] {
        &self.devices
    }
}
