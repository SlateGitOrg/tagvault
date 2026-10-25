//! End-to-end tests of the `tagvault` binary.

mod common;

use std::path::Path;
use std::process::{Command, Output};

fn tv(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tagvault")).args(args).output().expect("run tagvault")
}

fn ok(args: &[&str]) -> String {
    let out = tv(args);
    assert!(out.status.success(), "tagvault {args:?} failed:\n{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap()
}

fn p(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn simulate_ingest_query_verify() {
    let dir = common::tempdir();
    let cap = dir.path().join("plant.tvcap");
    let data = dir.path().join("archive");
    let policy = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/policy.json");
    let out = ok(&["simulate", "--out", p(&cap), "--hours", "0.5", "--restart", "10m:1m", "--drop-prob", "0.002"]);
    assert!(out.contains("frames"), "{out}");
    let out =
        ok(&["ingest", "--capture", p(&cap), "--data", p(&data), "--policy", policy, "--outage", "5m:5m", "--events"]);
    assert!(out.contains("left_in_spool=0"), "{out}");
    assert!(out.contains("[birth]"), "{out}");
    assert!(out.contains("duplicates_rejected=0"), "{out}");

    let tags = ok(&["tags", "--data", p(&data)]);
    assert!(tags.contains("Plant1/Line1/Tank01/Level"));
    let json = ok(&["tags", "--data", p(&data), "--json"]);
    assert!(json.trim_start().starts_with('['));

    let tag = "Plant1/Line1/Tank01/Level";
    let (s, e) = ("2026-01-05T00:00:00Z", "2026-01-05T00:30:00Z");
    for fmt in ["table", "json", "csv"] {
        ok(&["query", "raw", "--data", p(&data), "--tag", tag, "--start", s, "--end", e, "--format", fmt]);
        ok(&[
            "query",
            "interp",
            "--data",
            p(&data),
            "--tag",
            tag,
            "--start",
            s,
            "--end",
            e,
            "--step",
            "5m",
            "--format",
            fmt,
        ]);
        ok(&[
            "query",
            "agg",
            "--data",
            p(&data),
            "--tag",
            tag,
            "--start",
            s,
            "--end",
            e,
            "--interval",
            "10m",
            "--format",
            fmt,
        ]);
    }
    let agg = ok(&[
        "query",
        "agg",
        "--data",
        p(&data),
        "--tag",
        tag,
        "--start",
        s,
        "--end",
        e,
        "--interval",
        "10m",
        "--format",
        "csv",
    ]);
    assert_eq!(agg.lines().count(), 4, "header + 3 intervals:\n{agg}");

    // Torn write at the end of the WAL is repaired on the next open.
    let wal = data.join("wal.log");
    let mut bytes = std::fs::read(&wal).unwrap();
    bytes.extend_from_slice(&[0x10, 0, 0, 0, 0xde, 0xad]);
    std::fs::write(&wal, bytes).unwrap();
    let out = ok(&["verify", "--data", p(&data)]);
    assert!(out.contains("truncated_bytes=6"), "{out}");
    let out = ok(&["verify", "--data", p(&data), "--json"]);
    assert!(out.contains("\"points_verified\""));

    // Errors are reported, not panicked.
    let bad = tv(&["query", "raw", "--data", p(&data), "--tag", "nope", "--start", s, "--end", e]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("unknown tag"));
    let bad = tv(&["query", "raw", "--data", p(&data), "--tag", tag, "--start", "yesterday", "--end", e]);
    assert!(!bad.status.success());
    let bad = tv(&["ingest", "--capture", p(&cap), "--data", p(&data), "--outage", "nonsense"]);
    assert!(!bad.status.success());
    let bad = tv(&["simulate", "--out", p(&cap), "--restart", "bad"]);
    assert!(!bad.status.success());
    // A span wider than i64 is rejected at once instead of wrapping past the row cap (fix round 1).
    let started = std::time::Instant::now();
    let bad = tv(&[
        "query",
        "interp",
        "--data",
        p(&data),
        "--tag",
        tag,
        "--start=-9223372036854775808",
        "--end",
        "9223372036854775807",
        "--step",
        "1ms",
    ]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("rows (max"), "{}", String::from_utf8_lossy(&bad.stderr));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    let bad = tv(&["ingest", "--capture", p(&cap), "--data", p(&data), "--outage", "9223372036854775807:1s"]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("out of range"), "{}", String::from_utf8_lossy(&bad.stderr));
}

#[test]
fn bench_quick_writes_reports() {
    let dir = common::tempdir();
    let out = ok(&["bench", "--quick", "--out", p(dir.path())]);
    assert!(out.contains("| tank_level |"), "{out}");
    assert!(dir.path().join("bench.md").exists());
    assert!(dir.path().join("bench.json").exists());
    assert!(!dir.path().join("scratch").exists(), "scratch archives are cleaned up");
}

#[test]
fn serve_answers_http_and_ingest_can_forward_to_it() {
    let dir = common::tempdir();
    let data = dir.path().join("server-archive");
    let addr_file = dir.path().join("addr");
    let mut server = Command::new(env!("CARGO_BIN_EXE_tagvault"))
        .args(["serve", "--data", p(&data), "--listen", "127.0.0.1:0", "--addr-file", p(&addr_file), "--threads", "2"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let t0 = std::time::Instant::now();
    while !addr_file.exists() && t0.elapsed().as_secs() < 30 {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let addr = std::fs::read_to_string(&addr_file).unwrap();

    let cap = dir.path().join("c.tvcap");
    ok(&["simulate", "--out", p(&cap), "--hours", "0.1", "--restart", "none"]);
    let spool = dir.path().join("spool");
    let out = ok(&[
        "ingest",
        "--capture",
        p(&cap),
        "--data",
        p(&dir.path().join("unused")),
        "--spool",
        p(&spool),
        "--comp-dev",
        "0.1",
        "--forward-to",
        &format!("http://{addr}"),
    ]);
    assert!(out.contains("left_in_spool=0"), "{out}");

    let mut s = std::net::TcpStream::connect(&addr).unwrap();
    use std::io::{Read, Write};
    write!(s, "GET /api/v1/tags HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(resp.contains("Plant1/Line2/Reactor01/Temperature"));
    server.kill().unwrap();
    server.wait().unwrap();
}
