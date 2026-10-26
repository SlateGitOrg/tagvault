//! HTTP API and MQTT host tests against in-process servers (no Docker, no network).

mod common;

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tagvault::collector::Collector;
use tagvault::config::Policy;
use tagvault::forward::spool::{Spool, SpoolOptions};
use tagvault::forward::{batch, ArchiveSink, Backoff, Forwarder, Sink};
use tagvault::net::http::{route, ApiServer, HttpSink};
use tagvault::net::mqtt::{run_host, HostLoopOptions, MqttClient, MqttOptions, Will};
use tagvault::sim::EdgeNode;
use tagvault::sparkplug::{Host, HostConfig};
use tagvault::storage::{Archive, ArchiveOptions, SyncMode};
use tagvault::{Point, Sample, ValueKind};

fn http_get(addr: &str, path: &str) -> (u16, Value) {
    let mut s = TcpStream::connect(addr).unwrap();
    write!(s, "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let status = resp.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn samples(n: i64) -> Vec<Sample> {
    (0..n)
        .map(|i| Sample {
            tag: "Plant1/Line1/Tank01/Level".into(),
            kind: ValueKind::Analog,
            point: Point::good(1_767_571_200_000 + i * 1000, (i as f64 / 30.0).sin() * 10.0 + 50.0),
        })
        .collect()
}

#[test]
fn http_api_ingest_and_queries() {
    let dir = common::tempdir();
    let archive = Archive::open(
        dir.path(),
        ArchiveOptions { sync: SyncMode::Os, policy: Policy::uniform(0.0, 0.05), ..Default::default() },
    )
    .unwrap();
    let archive = Arc::new(RwLock::new(archive));
    let server = ApiServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let handle = {
        let (a, s) = (Arc::clone(&archive), Arc::clone(&stop));
        std::thread::spawn(move || server.run(a, 2, s))
    };

    let mut sink = HttpSink::new(&format!("http://{addr}")).unwrap();
    sink.deliver(&batch::encode(&samples(600)), 0).unwrap();
    assert!(sink.deliver(b"garbage", 0).is_err(), "400 → rejected");

    let (st, tags) = http_get(&addr, "/api/v1/tags");
    assert_eq!(st, 200);
    assert_eq!(tags[0]["name"], "Plant1/Line1/Tank01/Level");
    let tag = "Plant1%2FLine1%2FTank01%2FLevel";
    let (st, raw) =
        http_get(&addr, &format!("/api/v1/raw?tag={tag}&start=2026-01-05T00:00:00Z&end=2026-01-05T00:10:00Z&limit=5"));
    assert_eq!(st, 200);
    assert_eq!(raw["points"].as_array().unwrap().len(), 5);
    assert_eq!(raw["points"][0]["time"], "2026-01-05T00:00:00.000Z");
    let (st, interp) =
        http_get(&addr, &format!("/api/v1/interpolated?tag={tag}&start=1767571200000&end=1767571800000&step=1m"));
    assert_eq!(st, 200);
    assert_eq!(interp["rows"].as_array().unwrap().len(), 11);
    let (st, agg) = http_get(
        &addr,
        &format!("/api/v1/aggregate?tag={tag}&start=2026-01-05T00:00:00Z&end=2026-01-05T00:10:00Z&interval=5m"),
    );
    assert_eq!(st, 200);
    let rows = agg["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0]["avg"].as_f64().unwrap() > 40.0);
    let (st, stats) = http_get(&addr, "/api/v1/stats");
    assert_eq!((st, stats["tags"].as_u64()), (200, Some(1)));
    assert_eq!(http_get(&addr, "/api/v1/health").0, 200);
    assert_eq!(http_get(&addr, "/api/v1/raw?tag=nope&start=0&end=1").0, 404);
    assert_eq!(http_get(&addr, "/api/v1/raw?tag=x").0, 400);
    assert_eq!(http_get(&addr, &format!("/api/v1/interpolated?tag={tag}&start=0&end=1&step=0")).0, 400);
    // Spans wider than i64 used to wrap past the row cap and loop for ever.
    let (min, max) = (i64::MIN, i64::MAX);
    assert_eq!(http_get(&addr, &format!("/api/v1/interpolated?tag={tag}&start={min}&end={max}&step=1ms")).0, 400);
    assert_eq!(http_get(&addr, &format!("/api/v1/aggregate?tag={tag}&start={min}&end={max}&interval=1ms")).0, 400);
    assert_eq!(
        http_get(&addr, &format!("/api/v1/interpolated?tag={tag}&start=0&end=*%2B9223372036854775807&step=1s")).0,
        400
    );
    assert_eq!(http_get(&addr, "/nope").0, 404);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    handle.join().unwrap();
    // Method checks via the pure router.
    assert_eq!(route(&archive, "DELETE", "/api/v1/tags", b"", 0).status, 405);
}

/// Read one MQTT packet (header byte, body) from the fake broker's socket.
fn read_pkt(s: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut h = [0u8; 1];
    if s.read_exact(&mut h).is_err() {
        return None;
    }
    let (mut len, mut mult) = (0usize, 1usize);
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).ok()?;
        len += (b[0] & 0x7f) as usize * mult;
        mult *= 128;
        if b[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).ok()?;
    Some((h[0], body))
}

fn publish_pkt(topic: &str, payload: &[u8], qos1_pid: Option<u16>) -> Vec<u8> {
    let mut body = (topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(topic.as_bytes());
    if let Some(pid) = qos1_pid {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut out = vec![if qos1_pid.is_some() { 0x32 } else { 0x30 }];
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
    out.extend_from_slice(&body);
    out
}

#[test]
fn mqtt_host_ingests_and_requests_rebirth_on_gap() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut log: Vec<String> = Vec::new();
        // CONNECT with a STATE will.
        let (h, body) = read_pkt(&mut s).unwrap();
        assert_eq!(h, 0x10);
        assert_eq!(&body[2..6], b"MQTT");
        assert_ne!(body[7] & 0x04, 0, "will flag set");
        s.write_all(&[0x20, 0x02, 0x00, 0x00]).unwrap();
        // STATE birth at QoS 1, retained.
        let (h, body) = read_pkt(&mut s).unwrap();
        assert_eq!(h, 0x33);
        let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
        log.push(String::from_utf8(body[2..2 + tlen].to_vec()).unwrap());
        s.write_all(&[0x40, 0x02, body[2 + tlen], body[3 + tlen]]).unwrap();
        // SUBSCRIBE → SUBACK granting QoS 1.
        let (h, body) = read_pkt(&mut s).unwrap();
        assert_eq!(h, 0x82);
        s.write_all(&[0x90, 0x03, body[0], body[1], 0x01]).unwrap();

        let mut edge = EdgeNode::plant_cell("Plant1", "Line1", 3);
        let t0 = 1_767_571_200_000;
        let mut msgs = edge.birth(t0);
        for k in 1..=5 {
            msgs.extend(edge.scan(t0 + k * 1000, 1.0));
        }
        let lost = msgs.remove(5); // a DDATA goes missing → sequence gap
        assert!(lost.topic.contains("DDATA"));
        for (i, m) in msgs.iter().enumerate() {
            let pid = (i % 2 == 0).then_some(i as u16 + 1);
            s.write_all(&publish_pkt(&m.topic, &m.payload, pid)).unwrap();
        }
        // Collect what the host publishes (PUBACKs, pings, the NCMD) for a moment.
        s.set_read_timeout(Some(Duration::from_millis(1500))).unwrap();
        while let Some((h, body)) = read_pkt(&mut s) {
            if h >> 4 == 3 {
                let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
                log.push(String::from_utf8(body[2..2 + tlen].to_vec()).unwrap());
            }
        }
        log // dropping the socket closes the connection
    });

    let dir = common::tempdir();
    let policy = Policy::uniform(0.0, 0.0);
    let spool =
        Spool::open(&dir.path().join("spool"), SpoolOptions { sync: SyncMode::Os, ..Default::default() }).unwrap();
    let host = Host::new(HostConfig { reorder_timeout_ms: 0, ..Default::default() });
    let mut collector = Collector::new(host, policy.clone(), Forwarder::new(spool, Backoff::default()), 100);
    let mut archive =
        Archive::open(&dir.path().join("archive"), ArchiveOptions { sync: SyncMode::Os, policy, ..Default::default() })
            .unwrap();
    let opts = MqttOptions {
        client_id: "tagvault-test".into(),
        keep_alive_s: 30,
        will: Some(Will {
            topic: "spBv1.0/STATE/tv".into(),
            payload: b"{\"online\":false}".to_vec(),
            qos: 1,
            retain: true,
        }),
        username: None,
        password: None,
    };
    let mut client = MqttClient::connect(&addr, &opts).unwrap();
    let loop_opts = HostLoopOptions {
        filter: "spBv1.0/Plant1/#".into(),
        host_id: "tv".into(),
        tick: Duration::from_millis(50),
        max_messages: None,
    };
    let handled = {
        let mut sink = ArchiveSink::new(&mut archive);
        run_host(&mut client, &mut collector, &mut sink, &loop_opts, &AtomicBool::new(false)).unwrap()
    };
    let published = broker.join().unwrap();
    assert_eq!(published[0], "spBv1.0/STATE/tv");
    assert!(published.iter().any(|t| t == "spBv1.0/Plant1/NCMD/Line1"), "rebirth requested: {published:?}");
    assert!(handled >= 10);
    assert_eq!(collector.host_stats().seq_gaps, 1);
    assert_eq!(archive.tags().len(), 7, "uptime + 3 tank + 3 reactor metrics");
}
