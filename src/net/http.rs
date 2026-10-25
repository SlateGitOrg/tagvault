//! Small HTTP/JSON API over an archive, plus the matching ingest client used as a
//! store-and-forward sink.
//!
//! | Method | Path | Parameters |
//! |---|---|---|
//! | GET | `/api/v1/health` | – |
//! | GET | `/api/v1/tags` | – |
//! | GET | `/api/v1/stats` | – |
//! | GET | `/api/v1/raw` | `tag`, `start`, `end`, `limit` (default 10 000) |
//! | GET | `/api/v1/interpolated` | `tag`, `start`, `end`, `step` |
//! | GET | `/api/v1/aggregate` | `tag`, `start`, `end`, `interval` |
//! | POST | `/api/v1/ingest` | body: a `TVB1` batch (see `forward::batch`) |
//!
//! Times accept epoch ms, RFC 3339 UTC or `*-1h`; durations accept `ms|s|m|h|d` suffixes.
//! The server binds to whatever address it is given — it has no authentication, so bind it to
//! localhost or put it behind a reverse proxy (see Limitations).

use crate::error::{Error, Result};
use crate::forward::{batch, DeliveryError, Sink};
use crate::query;
use crate::storage::Archive;
use crate::timefmt::{format_time, now_ms, parse_duration, parse_time};
use crate::types::{Point, Timestamp};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Largest accepted request body.
pub const MAX_BODY: usize = 16 << 20;

/// A routed response.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    /// HTTP status.
    pub status: u16,
    /// JSON body.
    pub body: Value,
}

fn ok(body: Value) -> Response {
    Response { status: 200, body }
}

fn err(status: u16, msg: impl Into<String>) -> Response {
    Response { status, body: json!({ "error": msg.into() }) }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = |c: u8| (c as char).to_digit(16);
                match (hex(b[i + 1]), hex(b[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 2;
                    }
                    _ => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn point_json(p: &Point) -> Value {
    json!({
        "ts": p.ts,
        "time": format_time(p.ts),
        "value": if p.value.is_finite() { json!(p.value) } else { Value::Null },
        "quality": p.quality,
    })
}

struct Params(HashMap<String, String>);

impl Params {
    fn get(&self, k: &str) -> Result<&str> {
        self.0.get(k).map(|s| s.as_str()).ok_or_else(|| Error::InvalidInput(format!("missing parameter {k:?}")))
    }
    fn time(&self, k: &str, now: Timestamp) -> Result<Timestamp> {
        parse_time(self.get(k)?, now)
    }
    fn duration(&self, k: &str) -> Result<i64> {
        parse_duration(self.get(k)?)
    }
}

fn to_response(r: Result<Value>) -> Response {
    match r {
        Ok(v) => ok(v),
        Err(e @ (Error::InvalidInput(_) | Error::Decode(_))) => err(400, e.to_string()),
        Err(e @ Error::UnknownTag(_)) => err(404, e.to_string()),
        Err(e) => err(500, e.to_string()),
    }
}

/// Route one request. `now` resolves relative times.
pub fn route(archive: &RwLock<Archive>, method: &str, url: &str, body: &[u8], now: Timestamp) -> Response {
    let (path, q) = url.split_once('?').unwrap_or((url, ""));
    let p = Params(parse_query(q));
    let read = || archive.read().map_err(|_| Error::Corrupt("archive lock poisoned".into()));
    match (method, path) {
        ("GET", "/api/v1/health") => ok(json!({ "status": "ok" })),
        ("GET", "/api/v1/tags") => to_response(read().map(|a| json!(a.tags()))),
        ("GET", "/api/v1/stats") => to_response(read().map(|a| json!(a.stats()))),
        ("GET", "/api/v1/raw") => to_response((|| {
            let (tag, start, end) = (p.get("tag")?, p.time("start", now)?, p.time("end", now)?);
            let limit = p.0.get("limit").map(|l| l.parse::<usize>()).transpose().map_err(|_| Error::InvalidInput("bad limit".into()))?;
            let series = read()?.series(tag, start, end)?;
            let pts = query::raw(&series, start, end, limit.unwrap_or(10_000).min(query::MAX_ROWS as usize));
            Ok(json!({ "tag": tag, "points": pts.iter().map(point_json).collect::<Vec<_>>() }))
        })()),
        ("GET", "/api/v1/interpolated") => to_response((|| {
            let (tag, start, end, step) = (p.get("tag")?, p.time("start", now)?, p.time("end", now)?, p.duration("step")?);
            let series = read()?.series(tag, start, end)?;
            let rows = query::interpolated(&series, start, end, step)?;
            Ok(json!({ "tag": tag, "rows": rows }))
        })()),
        ("GET", "/api/v1/aggregate") => to_response((|| {
            let (tag, start, end, interval) = (p.get("tag")?, p.time("start", now)?, p.time("end", now)?, p.duration("interval")?);
            let series = read()?.series(tag, start, end)?;
            let rows = query::aggregate(&series, start, end, interval)?;
            Ok(json!({ "tag": tag, "rows": rows }))
        })()),
        ("POST", "/api/v1/ingest") => to_response((|| {
            let samples = batch::decode(body)?;
            let mut a = archive.write().map_err(|_| Error::Corrupt("archive lock poisoned".into()))?;
            Ok(json!(a.write(&samples)?))
        })()),
        (_, "/api/v1/health" | "/api/v1/tags" | "/api/v1/stats" | "/api/v1/raw" | "/api/v1/interpolated" | "/api/v1/aggregate" | "/api/v1/ingest") => {
            err(405, "method not allowed")
        }
        _ => err(404, "not found"),
    }
}

/// A bound (not yet running) API server.
pub struct ApiServer {
    server: Arc<tiny_http::Server>,
}

impl ApiServer {
    /// Bind to `addr` (e.g. `127.0.0.1:8080`; port 0 picks a free port).
    pub fn bind(addr: &str) -> Result<ApiServer> {
        let server = tiny_http::Server::http(addr).map_err(|e| Error::InvalidInput(format!("cannot bind {addr}: {e}")))?;
        Ok(ApiServer { server: Arc::new(server) })
    }

    /// The bound socket address.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.server.server_addr().to_ip()
    }

    /// Serve with `threads` workers until `stop` is set.
    pub fn run(self, archive: Arc<RwLock<Archive>>, threads: usize, stop: Arc<AtomicBool>) {
        let workers: Vec<_> = (0..threads.max(1))
            .map(|_| {
                let server = Arc::clone(&self.server);
                let archive = Arc::clone(&archive);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let Ok(Some(mut req)) = server.recv_timeout(Duration::from_millis(200)) else { continue };
                        let method = req.method().as_str().to_string();
                        let url = req.url().to_string();
                        let mut body = Vec::new();
                        let too_big = req.body_length().is_some_and(|l| l > MAX_BODY);
                        let read_ok = !too_big && req.as_reader().take(MAX_BODY as u64 + 1).read_to_end(&mut body).is_ok();
                        let resp = if too_big || body.len() > MAX_BODY {
                            err(413, "body too large")
                        } else if !read_ok {
                            err(400, "unreadable body")
                        } else {
                            route(&archive, &method, &url, &body, now_ms())
                        };
                        log::debug!("{method} {url} -> {}", resp.status);
                        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).expect("static header");
                        let r = tiny_http::Response::from_string(resp.body.to_string()).with_status_code(resp.status).with_header(header);
                        if let Err(e) = req.respond(r) {
                            log::debug!("client went away: {e}");
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            let _ = w.join();
        }
    }
}

/// Forwards spooled batches to a remote `tagvault serve` over HTTP.
#[derive(Debug, Clone)]
pub struct HttpSink {
    addr: String,
    timeout: Duration,
}

impl HttpSink {
    /// `target` is `http://host:port` or `host:port`.
    pub fn new(target: &str) -> Result<HttpSink> {
        let addr = target.strip_prefix("http://").unwrap_or(target).trim_end_matches('/').to_string();
        if addr.contains('/') || !addr.contains(':') {
            return Err(Error::InvalidInput(format!("expected http://host:port, got {target:?}")));
        }
        Ok(HttpSink { addr, timeout: Duration::from_secs(10) })
    }

    fn post(&self, body: &[u8]) -> std::io::Result<u16> {
        let sock = self
            .addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "unresolvable"))?;
        let mut s = TcpStream::connect_timeout(&sock, self.timeout)?;
        s.set_read_timeout(Some(self.timeout))?;
        s.set_write_timeout(Some(self.timeout))?;
        let head = format!(
            "POST /api/v1/ingest HTTP/1.1\r\nHost: {}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.addr,
            body.len()
        );
        s.write_all(head.as_bytes())?;
        s.write_all(body)?;
        let mut resp = Vec::new();
        s.take(64 * 1024).read_to_end(&mut resp)?;
        let line = String::from_utf8_lossy(&resp);
        line.split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad HTTP status line"))
    }
}

impl Sink for HttpSink {
    fn deliver(&mut self, payload: &[u8], _now: Timestamp) -> std::result::Result<(), DeliveryError> {
        match self.post(payload) {
            Ok(200..=299) => Ok(()),
            Ok(code @ 400..=499) => Err(DeliveryError::Rejected(format!("HTTP {code}"))),
            Ok(code) => Err(DeliveryError::Unavailable(format!("HTTP {code}"))),
            Err(e) => Err(DeliveryError::Unavailable(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_string_decoding() {
        let q = parse_query("tag=Plant1%2FLine1%2FTank01%2FLevel&start=*-1h&x=a+b&bad=%zz&flag");
        assert_eq!(q["tag"], "Plant1/Line1/Tank01/Level");
        assert_eq!(q["x"], "a b");
        assert_eq!(q["bad"], "%zz");
        assert_eq!(q["flag"], "");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%4"), "%4");
        assert_eq!(percent_decode("%a\u{e9}x"), "%a\u{e9}x");
    }

    #[test]
    fn sink_target_parsing() {
        assert!(HttpSink::new("http://127.0.0.1:8080").is_ok());
        assert!(HttpSink::new("127.0.0.1:8080/").is_ok());
        assert!(HttpSink::new("http://host/path").is_err());
        assert!(HttpSink::new("justahost").is_err());
    }
}
