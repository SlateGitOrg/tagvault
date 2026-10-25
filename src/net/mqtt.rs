//! Minimal MQTT 3.1.1 client — just what a Sparkplug host application needs:
//! CONNECT (clean session, keep-alive, last will), SUBSCRIBE, receiving PUBLISH at QoS 0/1
//! (PUBACK sent for QoS 1), PUBLISH at QoS 0/1, PINGREQ and DISCONNECT. No TLS, no QoS 2, no
//! persistent sessions (see Limitations in the README).
//!
//! [`run_host`] wires a client to a [`Collector`]: it publishes the Sparkplug 3.0 host `STATE`
//! birth (retained, QoS 1, with a matching will), subscribes to the group namespace, feeds every
//! message to the collector, publishes rebirth commands, and forwards spooled batches to a sink.

use crate::collector::Collector;
use crate::error::{Error, Result};
use crate::forward::Sink;
use crate::timefmt::now_ms;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const MAX_PACKET: usize = 16 << 20;

/// Last-will message.
#[derive(Debug, Clone)]
pub struct Will {
    /// Topic.
    pub topic: String,
    /// Payload.
    pub payload: Vec<u8>,
    /// QoS (0 or 1).
    pub qos: u8,
    /// Retain flag.
    pub retain: bool,
}

/// Connection settings.
#[derive(Debug, Clone)]
pub struct MqttOptions {
    /// Client identifier.
    pub client_id: String,
    /// Keep-alive in seconds.
    pub keep_alive_s: u16,
    /// Optional last will.
    pub will: Option<Will>,
    /// Optional credentials.
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<String>,
}

/// A connected client.
#[derive(Debug)]
pub struct MqttClient {
    stream: TcpStream,
    next_pid: u16,
    keep_alive: Duration,
    last_sent: Instant,
}

fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s);
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    let mut len = body.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        out.push(b);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

/// Read one packet; `Ok(None)` if no byte arrived before the socket's read timeout.
fn read_packet(stream: &mut TcpStream) -> Result<Option<(u8, Vec<u8>)>> {
    let mut first = [0u8; 1];
    match stream.read(&mut first) {
        Ok(0) => return Err(Error::Io(std::io::Error::new(ErrorKind::UnexpectedEof, "broker closed the connection"))),
        Ok(_) => {}
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    // Once a packet has started, finish it with a generous timeout so we never lose framing.
    let prev = stream.read_timeout()?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut len = 0usize;
    let mut mult = 1usize;
    for i in 0..4 {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b)?;
        len += (b[0] & 0x7f) as usize * mult;
        mult *= 128;
        if b[0] & 0x80 == 0 {
            break;
        }
        if i == 3 {
            return Err(Error::Decode("MQTT remaining length too long".into()));
        }
    }
    if len > MAX_PACKET {
        return Err(Error::Decode(format!("MQTT packet of {len} bytes exceeds limit")));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    stream.set_read_timeout(prev)?;
    Ok(Some((first[0], body)))
}

/// An application message received from the broker.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Topic.
    pub topic: String,
    /// Payload.
    pub payload: Vec<u8>,
}

fn parse_publish(header: u8, body: &[u8]) -> Result<(Message, u8, Option<u16>)> {
    let qos = (header >> 1) & 0x03;
    if body.len() < 2 {
        return Err(Error::Decode("short PUBLISH".into()));
    }
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2 + tlen;
    if body.len() < pos {
        return Err(Error::Decode("short PUBLISH topic".into()));
    }
    let topic = String::from_utf8(body[2..pos].to_vec()).map_err(|_| Error::Decode("non-UTF-8 topic".into()))?;
    let pid = if qos > 0 {
        if body.len() < pos + 2 {
            return Err(Error::Decode("short PUBLISH packet id".into()));
        }
        let p = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        Some(p)
    } else {
        None
    };
    Ok((Message { topic, payload: body[pos..].to_vec() }, qos, pid))
}

impl MqttClient {
    /// Connect and wait for CONNACK.
    pub fn connect(addr: &str, opts: &MqttOptions) -> Result<MqttClient> {
        let sock =
            addr.to_socket_addrs()?.next().ok_or_else(|| Error::InvalidInput(format!("cannot resolve {addr}")))?;
        let stream = TcpStream::connect_timeout(&sock, Duration::from_secs(10))?;
        stream.set_nodelay(true)?;
        let mut c = MqttClient {
            stream,
            next_pid: 1,
            keep_alive: Duration::from_secs(opts.keep_alive_s as u64),
            last_sent: Instant::now(),
        };
        let mut body = Vec::new();
        put_str(&mut body, b"MQTT");
        body.push(4);
        let mut flags = 0x02u8;
        if let Some(w) = &opts.will {
            flags |= 0x04 | ((w.qos & 1) << 3) | if w.retain { 0x20 } else { 0 };
        }
        if opts.username.is_some() {
            flags |= 0x80;
        }
        if opts.password.is_some() {
            flags |= 0x40;
        }
        body.push(flags);
        body.extend_from_slice(&opts.keep_alive_s.to_be_bytes());
        put_str(&mut body, opts.client_id.as_bytes());
        if let Some(w) = &opts.will {
            put_str(&mut body, w.topic.as_bytes());
            put_str(&mut body, &w.payload);
        }
        if let Some(u) = &opts.username {
            put_str(&mut body, u.as_bytes());
        }
        if let Some(p) = &opts.password {
            put_str(&mut body, p.as_bytes());
        }
        c.send(&packet(0x10, &body))?;
        c.stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        match read_packet(&mut c.stream)? {
            Some((0x20, b)) if b.len() == 2 && b[1] == 0 => Ok(c),
            Some((0x20, b)) => Err(Error::InvalidInput(format!(
                "broker refused connection (code {})",
                b.get(1).copied().unwrap_or(255)
            ))),
            _ => Err(Error::Decode("expected CONNACK".into())),
        }
    }

    fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes)?;
        self.last_sent = Instant::now();
        Ok(())
    }

    fn pid(&mut self) -> u16 {
        let p = self.next_pid;
        self.next_pid = self.next_pid.checked_add(1).unwrap_or(1);
        p
    }

    /// Subscribe to one filter at QoS 1 and wait for SUBACK.
    pub fn subscribe(&mut self, filter: &str) -> Result<()> {
        let pid = self.pid();
        let mut body = pid.to_be_bytes().to_vec();
        put_str(&mut body, filter.as_bytes());
        body.push(1);
        self.send(&packet(0x82, &body))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some((h, b)) = read_packet(&mut self.stream)? {
                if h >> 4 == 9 {
                    if b.get(2) == Some(&0x80) {
                        return Err(Error::InvalidInput(format!("subscription to {filter} refused")));
                    }
                    return Ok(());
                }
            }
        }
        Err(Error::Decode("no SUBACK".into()))
    }

    /// Publish at QoS 0 or 1 (QoS 1 PUBACKs are consumed by [`MqttClient::poll`]).
    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Result<()> {
        let mut body = Vec::with_capacity(topic.len() + payload.len() + 4);
        put_str(&mut body, topic.as_bytes());
        if qos > 0 {
            let pid = self.pid();
            body.extend_from_slice(&pid.to_be_bytes());
        }
        body.extend_from_slice(payload);
        let header = 0x30 | ((qos.min(1)) << 1) | u8::from(retain);
        self.send(&packet(header, &body))
    }

    /// Wait up to `timeout` for the next application message, answering protocol traffic and
    /// sending keep-alive pings as needed.
    pub fn poll(&mut self, timeout: Duration) -> Result<Option<Message>> {
        if !self.keep_alive.is_zero() && self.last_sent.elapsed() >= self.keep_alive / 2 {
            self.send(&[0xC0, 0x00])?;
        }
        self.stream.set_read_timeout(Some(timeout.max(Duration::from_millis(1))))?;
        let Some((h, body)) = read_packet(&mut self.stream)? else {
            return Ok(None);
        };
        match h >> 4 {
            3 => {
                let (msg, qos, pid) = parse_publish(h, &body)?;
                if qos >= 1 {
                    let pid = pid.expect("qos>0 has pid");
                    let mut ack = vec![0x40, 0x02];
                    ack.extend_from_slice(&pid.to_be_bytes());
                    self.send(&ack)?;
                }
                Ok(Some(msg))
            }
            _ => Ok(None), // PUBACK, PINGRESP, SUBACK duplicates
        }
    }

    /// Send DISCONNECT (the broker then discards the will).
    pub fn disconnect(mut self) -> Result<()> {
        self.send(&[0xE0, 0x00])
    }
}

/// `spBv1.0/STATE/<host_id>` JSON payload (Sparkplug 3.0).
pub fn state_payload(online: bool, ts: i64) -> Vec<u8> {
    format!(r#"{{"online":{online},"timestamp":{ts}}}"#).into_bytes()
}

/// Options for [`run_host`].
#[derive(Debug, Clone)]
pub struct HostLoopOptions {
    /// Topic filter, e.g. `spBv1.0/Plant1/#`.
    pub filter: String,
    /// Host id for the STATE topic.
    pub host_id: String,
    /// Housekeeping interval.
    pub tick: Duration,
    /// Stop after this many messages (for tests and bounded runs).
    pub max_messages: Option<u64>,
}

/// Run a Sparkplug host on an MQTT connection until `stop` is set, the broker disconnects, or
/// `max_messages` is reached. Returns the number of messages handled.
pub fn run_host(
    client: &mut MqttClient,
    collector: &mut Collector,
    sink: &mut dyn Sink,
    opts: &HostLoopOptions,
    stop: &AtomicBool,
) -> Result<u64> {
    let state_topic = format!("spBv1.0/STATE/{}", opts.host_id);
    client.publish(&state_topic, &state_payload(true, now_ms()), 1, true)?;
    client.subscribe(&opts.filter)?;
    let mut handled = 0u64;
    let mut last_tick = Instant::now();
    while !stop.load(Ordering::Relaxed) && opts.max_messages.is_none_or(|m| handled < m) {
        match client.poll(opts.tick) {
            Ok(Some(msg)) => {
                handled += 1;
                for cmd in collector.on_message(&msg.topic, &msg.payload, now_ms())? {
                    client.publish(&cmd.topic, &cmd.payload, 0, false)?;
                }
            }
            Ok(None) => {}
            Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                log::warn!("broker closed the connection");
                break;
            }
            Err(e) => return Err(e),
        }
        if last_tick.elapsed() >= opts.tick {
            last_tick = Instant::now();
            let now = now_ms();
            for cmd in collector.tick(now)? {
                client.publish(&cmd.topic, &cmd.payload, 0, false)?;
            }
            collector.pump(sink, now, 64)?;
        }
    }
    let now = now_ms();
    collector.tick(now)?;
    collector.spool_pending()?;
    collector.pump(sink, now, 1024)?;
    Ok(handled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_length_encoding() {
        assert_eq!(packet(0xC0, &[]), vec![0xC0, 0]);
        let p = packet(0x30, &[0u8; 321]);
        assert_eq!(&p[..3], &[0x30, 0xC1, 0x02]);
        assert_eq!(p.len(), 324);
    }

    #[test]
    fn publish_parsing() {
        let mut body = Vec::new();
        put_str(&mut body, b"a/b");
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(b"xyz");
        let (m, qos, pid) = parse_publish(0x32, &body).unwrap();
        assert_eq!((m.topic.as_str(), m.payload.as_slice(), qos, pid), ("a/b", &b"xyz"[..], 1, Some(7)));
        assert!(parse_publish(0x30, &[0]).is_err());
        assert!(parse_publish(0x30, &[0, 9, b'a']).is_err());
        assert!(parse_publish(0x32, &[0, 1, b'a', 0]).is_err());
        assert_eq!(state_payload(true, 5), br#"{"online":true,"timestamp":5}"#.to_vec());
    }
}
