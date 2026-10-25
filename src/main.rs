//! `tagvault` command-line interface.

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tagvault::bench::{self, BenchConfig};
use tagvault::collector::{replay, Collector};
use tagvault::config::Policy;
use tagvault::forward::spool::{Spool, SpoolOptions};
use tagvault::forward::{ArchiveSink, Backoff, Forwarder, OutageSink, Sink};
use tagvault::net::http::{ApiServer, HttpSink};
use tagvault::net::mqtt::{run_host, HostLoopOptions, MqttClient, MqttOptions, Will};
use tagvault::query;
use tagvault::sim::capture::{read_capture, write_capture};
use tagvault::sim::Scenario;
use tagvault::sparkplug::{Host, HostConfig, HostEvent};
use tagvault::storage::{Archive, ArchiveOptions, SyncMode};
use tagvault::timefmt::{format_time, now_ms, parse_duration, parse_time};
use tagvault::{Error, Result};

#[derive(Parser)]
#[command(name = "tagvault", version, about = "Industrial time-series historian for Sparkplug B plant telemetry")]
struct Cli {
    /// Increase log verbosity (-v info, -vv debug).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a simulated plant scenario and record what a Sparkplug host receives.
    Simulate(SimulateArgs),
    /// Replay a capture through host → exception → spool → archive.
    Ingest(IngestArgs),
    /// Run a live Sparkplug host against an MQTT broker.
    Mqtt(MqttArgs),
    /// Publish simulated Sparkplug edge nodes to an MQTT broker (honours rebirth commands).
    EdgeSim(EdgeSimArgs),
    /// Serve the HTTP query/ingest API.
    Serve(ServeArgs),
    /// List tags.
    Tags(DataArgs),
    /// Query one tag.
    Query(QueryArgs),
    /// Open (recovering if needed), verify every chunk, and print statistics.
    Verify(DataArgs),
    /// Run compression and query benchmarks on generated signals.
    Bench(BenchArgs),
}

#[derive(Args)]
struct SimulateArgs {
    /// Output capture file.
    #[arg(long)]
    out: PathBuf,
    /// Scenario length in hours.
    #[arg(long, default_value_t = 2.0)]
    hours: f64,
    /// Random seed.
    #[arg(long, default_value_t = 7)]
    seed: u64,
    /// Number of edge nodes.
    #[arg(long, default_value_t = 2)]
    nodes: usize,
    /// Probability a DDATA is lost in transit.
    #[arg(long, default_value_t = 0.0003)]
    drop_prob: f64,
    /// Probability two DDATA arrive swapped.
    #[arg(long, default_value_t = 0.001)]
    swap_prob: f64,
    /// Node restart as AT:OFFLINE (durations, e.g. 1h:2m); "none" disables.
    #[arg(long, default_value = "1h:2m")]
    restart: String,
}

#[derive(Args)]
struct PolicyArgs {
    /// JSON policy file (per-tag exception/compression deviations).
    #[arg(long)]
    policy: Option<PathBuf>,
    /// Uniform exception deviation when no policy file is given.
    #[arg(long, default_value_t = 0.0)]
    exc_dev: f64,
    /// Uniform compression deviation when no policy file is given.
    #[arg(long, default_value_t = 0.0)]
    comp_dev: f64,
}

impl PolicyArgs {
    fn load(&self) -> Result<Policy> {
        match &self.policy {
            Some(p) => Policy::load(p),
            None => {
                let p = Policy::uniform(self.exc_dev, self.comp_dev);
                p.validate()?;
                Ok(p)
            }
        }
    }
}

#[derive(Args)]
struct IngestArgs {
    /// Capture file from `simulate`.
    #[arg(long)]
    capture: PathBuf,
    /// Archive directory (ignored with --forward-to).
    #[arg(long)]
    data: PathBuf,
    /// Spool directory (default: <data>/../spool).
    #[arg(long)]
    spool: Option<PathBuf>,
    #[command(flatten)]
    policy: PolicyArgs,
    /// Simulated link outage START:DURATION relative to the capture start (repeatable).
    #[arg(long)]
    outage: Vec<String>,
    /// Forward to a running `tagvault serve` instead of a local archive.
    #[arg(long)]
    forward_to: Option<String>,
    /// Print Sparkplug session events (gaps, rebirths, births, deaths).
    #[arg(long)]
    events: bool,
    /// fsync policy for spool and WAL. An offline replay can always be re-run from the
    /// capture, so the default skips fsync; use `always` to measure the durable path.
    #[arg(long, value_enum, default_value = "os")]
    sync: SyncArg,
}

#[derive(Clone, Copy, ValueEnum)]
enum SyncArg {
    /// fsync every commit (power-loss safe).
    Always,
    /// Let the OS flush (process-crash safe).
    Os,
}

impl SyncArg {
    fn mode(self) -> SyncMode {
        match self {
            SyncArg::Always => SyncMode::Always,
            SyncArg::Os => SyncMode::Os,
        }
    }
}

#[derive(Args)]
struct MqttArgs {
    /// Broker address host:port.
    #[arg(long)]
    broker: String,
    /// Archive directory (ignored with --forward-to).
    #[arg(long)]
    data: PathBuf,
    /// Sparkplug group to subscribe to ("#" for all).
    #[arg(long, default_value = "#")]
    group: String,
    /// Host application id.
    #[arg(long, default_value = "tagvault")]
    host_id: String,
    /// Spool directory (default: <data>/../spool).
    #[arg(long)]
    spool: Option<PathBuf>,
    #[command(flatten)]
    policy: PolicyArgs,
    /// Forward to a running `tagvault serve` instead of a local archive.
    #[arg(long)]
    forward_to: Option<String>,
    /// MQTT username.
    #[arg(long)]
    username: Option<String>,
    /// Name of an environment variable holding the MQTT password.
    #[arg(long)]
    password_env: Option<String>,
    /// Stop after this many messages.
    #[arg(long)]
    max_messages: Option<u64>,
}

#[derive(Args)]
struct EdgeSimArgs {
    /// Broker address host:port.
    #[arg(long)]
    broker: String,
    /// Sparkplug group id.
    #[arg(long, default_value = "Plant1")]
    group: String,
    /// Number of edge nodes.
    #[arg(long, default_value_t = 1)]
    nodes: usize,
    /// Simulated seconds to publish.
    #[arg(long, default_value_t = 60)]
    seconds: u64,
    /// Time compression: simulated seconds per wall-clock second.
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Random seed.
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

#[derive(Args)]
struct ServeArgs {
    /// Archive directory.
    #[arg(long)]
    data: PathBuf,
    /// Listen address (port 0 = any free port).
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,
    /// Worker threads.
    #[arg(long, default_value_t = 4)]
    threads: usize,
    /// Write the bound address to this file once listening.
    #[arg(long)]
    addr_file: Option<PathBuf>,
    #[command(flatten)]
    policy: PolicyArgs,
}

#[derive(Args)]
struct DataArgs {
    /// Archive directory.
    #[arg(long)]
    data: PathBuf,
    /// Emit JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum QueryKind {
    /// Stored points.
    Raw,
    /// Values on a regular grid.
    Interp,
    /// Per-interval time-weighted average, min, max, count.
    Agg,
}

#[derive(Clone, Copy, ValueEnum, PartialEq)]
enum Format {
    Table,
    Json,
    Csv,
}

#[derive(Args)]
struct QueryArgs {
    /// Query type.
    kind: QueryKind,
    /// Archive directory.
    #[arg(long)]
    data: PathBuf,
    /// Tag name.
    #[arg(long)]
    tag: String,
    /// Start time (ms, RFC 3339 UTC, or *-1h).
    #[arg(long)]
    start: String,
    /// End time.
    #[arg(long)]
    end: String,
    /// Grid step for interp (e.g. 1m).
    #[arg(long, default_value = "1m")]
    step: String,
    /// Interval for agg (e.g. 15m).
    #[arg(long, default_value = "15m")]
    interval: String,
    /// Row limit for raw.
    #[arg(long, default_value_t = 1000)]
    limit: usize,
    /// Output format.
    #[arg(long, value_enum, default_value = "table")]
    format: Format,
}

#[derive(Args)]
struct BenchArgs {
    /// Directory for scratch archives and the report files.
    #[arg(long, default_value = "out/bench")]
    out: PathBuf,
    /// Small, fast configuration.
    #[arg(long)]
    quick: bool,
}

fn opt_num(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.3}")).unwrap_or_else(|| "-".into())
}

fn archive_opts(policy: Policy) -> ArchiveOptions {
    ArchiveOptions { policy, ..Default::default() }
}

fn default_spool(data: &Path, explicit: &Option<PathBuf>) -> PathBuf {
    explicit.clone().unwrap_or_else(|| data.parent().unwrap_or(Path::new(".")).join("spool"))
}

/// Print up to `per_kind` events of each kind, then a count of the rest.
fn print_events(events: &[HostEvent], per_kind: usize) {
    let mut shown: std::collections::HashMap<&'static str, usize> = std::collections::HashMap::new();
    let mut hidden = 0usize;
    for e in events {
        let (kind, line) = match e {
            HostEvent::Discarded { .. } | HostEvent::DeviceBirth { .. } | HostEvent::DeviceDeath { .. } => continue,
            HostEvent::NodeBirth { node, bd_seq, metrics } => {
                ("birth", format!("[birth]   {node} bdSeq={} metrics={metrics}", bd_seq.unwrap_or(0)))
            }
            HostEvent::NodeDeath { node } => ("death", format!("[death]   {node} → all its tags marked stale")),
            HostEvent::SequenceGap { node, expected, got } => {
                ("gap", format!("[gap]     {node} expected seq {expected}, got {got} → buffering"))
            }
            HostEvent::GapRecovered { node, messages } => {
                ("reorder", format!("[reorder] {node} gap filled, {messages} buffered message(s) applied in order"))
            }
            HostEvent::RebirthRequested { node, reason } => {
                ("rebirth", format!("[rebirth] NCMD Node Control/Rebirth → {node} ({reason})"))
            }
        };
        let n = shown.entry(kind).or_default();
        if *n < per_kind {
            println!("  {line}");
        } else {
            hidden += 1;
        }
        *n += 1;
    }
    if hidden > 0 {
        println!("  … {hidden} more session events (see the counters below)");
    }
}

fn cmd_simulate(a: SimulateArgs) -> Result<()> {
    let node_restart = if a.restart == "none" {
        None
    } else {
        let (at, off) =
            a.restart.split_once(':').ok_or_else(|| Error::InvalidInput("--restart expects AT:OFFLINE".into()))?;
        Some(((parse_duration(at)? / 1000) as u64, (parse_duration(off)? / 1000) as u64))
    };
    let sc = Scenario {
        seed: a.seed,
        duration_s: (a.hours * 3600.0) as u64,
        nodes: a.nodes,
        drop_prob: a.drop_prob,
        swap_prob: a.swap_prob,
        node_restart,
        ..Default::default()
    };
    let (frames, summary) = sc.generate();
    if let Some(parent) = a.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_capture(&a.out, &frames)?;
    let size = std::fs::metadata(&a.out)?.len();
    println!("capture   {} ({} frames, {:.1} MB)", a.out.display(), frames.len(), size as f64 / 1e6);
    println!("window    {} → {}", format_time(sc.start), format_time(sc.start + sc.duration_s as i64 * 1000));
    println!(
        "scenario  {} nodes × (Tank01, Reactor01); published {} msgs, lost {}, swapped {}, node restarts {}, rebirths honoured {}",
        sc.nodes, summary.published, summary.dropped, summary.swapped, summary.restarts, summary.rebirths
    );
    Ok(())
}

fn parse_outages(specs: &[String], origin: i64) -> Result<Vec<(i64, i64)>> {
    specs
        .iter()
        .map(|s| {
            let (a, d) = s
                .split_once(':')
                .ok_or_else(|| Error::InvalidInput(format!("--outage expects START:DURATION, got {s}")))?;
            let overflow = || Error::InvalidInput(format!("--outage {s} is out of range"));
            let start = origin.checked_add(parse_duration(a)?).ok_or_else(overflow)?;
            Ok((start, start.checked_add(parse_duration(d)?).ok_or_else(overflow)?))
        })
        .collect()
}

fn make_collector(policy: &Policy, spool_dir: &Path, host: HostConfig, sync: SyncMode) -> Result<Collector> {
    let spool = Spool::open(spool_dir, SpoolOptions { sync, ..Default::default() })?;
    Ok(Collector::new(Host::new(host), policy.clone(), Forwarder::new(spool, Backoff::default()), 2000))
}

fn cmd_ingest(a: IngestArgs) -> Result<()> {
    let policy = a.policy.load()?;
    let frames = read_capture(&a.capture)?;
    let origin = frames.first().map(|f| f.recv_ts).unwrap_or(0);
    let outages = parse_outages(&a.outage, origin)?;
    let spool_dir = default_spool(&a.data, &a.spool);
    let mut collector = make_collector(&policy, &spool_dir, HostConfig::default(), a.sync.mode())?;
    collector.keep_events(a.events);
    println!("replaying {} frames from {}", frames.len(), a.capture.display());
    for (s, e) in &outages {
        println!("  link outage {} → {}", format_time(*s), format_time(*e));
    }
    let t0 = std::time::Instant::now();
    let (report, rejected) = match &a.forward_to {
        Some(url) => {
            let mut sink = OutageSink::new(HttpSink::new(url)?, outages);
            (replay(&frames, &mut collector, &mut sink, 1000)?, None)
        }
        None => {
            let mut archive =
                Archive::open(&a.data, ArchiveOptions { sync: a.sync.mode(), ..archive_opts(policy.clone()) })?;
            let (r, rej) = {
                let mut sink = OutageSink::new(ArchiveSink::new(&mut archive), outages);
                let r = replay(&frames, &mut collector, &mut sink, 1000)?;
                (r, sink.into_inner().rejected_points)
            };
            archive.close()?;
            (r, Some(rej))
        }
    };
    let elapsed = t0.elapsed().as_secs_f64();
    if a.events {
        print_events(&collector.take_events(), 3);
    }
    let hs = collector.host_stats();
    let cs = collector.stats();
    let fs = collector.forwarder().stats();
    println!(
        "sparkplug  messages={} births={} deaths={} data={} seq_gaps={} reordered={} rebirth_requests={} discarded={}",
        hs.messages,
        hs.births,
        hs.deaths,
        hs.data_messages,
        hs.seq_gaps,
        hs.reordered,
        hs.rebirth_requests,
        hs.discarded
    );
    println!(
        "exception  samples_in={} reported={} ({:.1}% kept)",
        cs.samples_in,
        cs.samples_reported,
        100.0 * cs.samples_reported as f64 / cs.samples_in.max(1) as f64
    );
    println!(
        "forward    batches={} delivered={} failed_attempts={} max_backlog={} left_in_spool={}",
        fs.enqueued, fs.delivered, fs.failed_attempts, report.max_backlog, report.left_in_spool
    );
    if let Some(rej) = rejected {
        let archive = Archive::open(&a.data, read_only())?;
        let st = archive.stats();
        let archived = st.segment_points + st.memtable_points;
        println!(
            "archive    tags={} archived_points={} segment_bytes={} duplicates_rejected={} ({:.1}x fewer points than decoded)",
            st.tags,
            archived,
            st.segment_bytes,
            rej,
            hs.samples as f64 / archived.max(1) as f64
        );
    }
    println!("done in {elapsed:.2}s");
    Ok(())
}

fn cmd_mqtt(a: MqttArgs) -> Result<()> {
    let policy = a.policy.load()?;
    let spool_dir = default_spool(&a.data, &a.spool);
    let host_cfg = HostConfig { host_id: a.host_id.clone(), ..Default::default() };
    let mut collector = make_collector(&policy, &spool_dir, host_cfg, SyncMode::Always)?;
    let password = match &a.password_env {
        Some(var) => Some(
            std::env::var(var).map_err(|_| Error::InvalidInput(format!("environment variable {var} is not set")))?,
        ),
        None => None,
    };
    let opts = MqttOptions {
        client_id: format!("tagvault-{}", a.host_id),
        keep_alive_s: 30,
        will: Some(Will {
            topic: format!("spBv1.0/STATE/{}", a.host_id),
            payload: tagvault::net::mqtt::state_payload(false, now_ms()),
            qos: 1,
            retain: true,
        }),
        username: a.username.clone(),
        password,
    };
    let mut client = MqttClient::connect(&a.broker, &opts)?;
    let filter = if a.group == "#" { "spBv1.0/#".to_string() } else { format!("spBv1.0/{}/#", a.group) };
    println!("connected to {}; subscribed to {filter}", a.broker);
    let loop_opts =
        HostLoopOptions { filter, host_id: a.host_id, tick: Duration::from_millis(500), max_messages: a.max_messages };
    let stop = AtomicBool::new(false);
    let handled = match &a.forward_to {
        Some(url) => {
            let mut sink = HttpSink::new(url)?;
            run_host(&mut client, &mut collector, &mut sink, &loop_opts, &stop)?
        }
        None => {
            let mut archive = Archive::open(&a.data, archive_opts(policy))?;
            let n = {
                let mut sink: Box<dyn Sink + '_> = Box::new(ArchiveSink::new(&mut archive));
                run_host(&mut client, &mut collector, sink.as_mut(), &loop_opts, &stop)?
            };
            archive.close()?;
            n
        }
    };
    client.disconnect()?;
    println!("handled {handled} messages; host stats: {:?}", collector.host_stats());
    Ok(())
}

fn cmd_edge_sim(a: EdgeSimArgs) -> Result<()> {
    use tagvault::sim::EdgeNode;
    if a.speed <= 0.0 {
        return Err(Error::InvalidInput("--speed must be positive".into()));
    }
    let mut nodes: Vec<EdgeNode> = (0..a.nodes)
        .map(|i| EdgeNode::plant_cell(&a.group, &format!("Line{}", i + 1), a.seed * 1000 + i as u64))
        .collect();
    // One MQTT session per edge node, each with its NDEATH as the will.
    let mut clients = Vec::with_capacity(nodes.len());
    let start = now_ms();
    for node in &nodes {
        let death = node.death_certificate(start);
        let opts = MqttOptions {
            client_id: format!("tagvault-edge-{}", node.edge()),
            keep_alive_s: 30,
            will: Some(Will { topic: death.topic, payload: death.payload, qos: 1, retain: false }),
            username: None,
            password: None,
        };
        let mut c = MqttClient::connect(&a.broker, &opts)?;
        c.subscribe(&format!("spBv1.0/{}/NCMD/{}", node.group(), node.edge()))?;
        clients.push(c);
    }
    let mut published = 0u64;
    let mut rebirths = 0u64;
    for (node, c) in nodes.iter_mut().zip(&mut clients) {
        for m in node.birth(start) {
            c.publish(&m.topic, &m.payload, 0, false)?;
            published += 1;
        }
    }
    let tick = Duration::from_secs_f64(1.0 / a.speed);
    for k in 1..=a.seconds as i64 {
        let now = start + k * 1000;
        for (node, c) in nodes.iter_mut().zip(&mut clients) {
            while let Some(msg) = c.poll(Duration::from_millis(1))? {
                if node.command(&msg.topic, &msg.payload) {
                    rebirths += 1;
                }
            }
            for m in node.scan(now, 1.0) {
                c.publish(&m.topic, &m.payload, 0, false)?;
                published += 1;
            }
        }
        std::thread::sleep(tick);
    }
    for c in clients {
        c.disconnect()?;
    }
    println!("published {published} Sparkplug messages from {} node(s); rebirths honoured: {rebirths}", a.nodes);
    Ok(())
}

fn cmd_serve(a: ServeArgs) -> Result<()> {
    let archive = Arc::new(RwLock::new(Archive::open(&a.data, archive_opts(a.policy.load()?))?));
    let server = ApiServer::bind(&a.listen)?;
    let addr = server.local_addr().map(|s| s.to_string()).unwrap_or_else(|| a.listen.clone());
    if let Some(f) = &a.addr_file {
        std::fs::write(f, &addr)?;
    }
    println!("serving {} on http://{addr}/api/v1/ (Ctrl-C to stop)", a.data.display());
    server.run(archive, a.threads, Arc::new(AtomicBool::new(false)));
    Ok(())
}

/// Options for tools that only read: never rewrite stored settings.
fn read_only() -> ArchiveOptions {
    ArchiveOptions { reconfigure_existing: false, ..Default::default() }
}

fn cmd_tags(a: DataArgs) -> Result<()> {
    let archive = Archive::open(&a.data, read_only())?;
    let tags = archive.tags();
    if a.json {
        println!("{}", serde_json::to_string_pretty(&tags).map_err(|e| Error::InvalidInput(e.to_string()))?);
        return Ok(());
    }
    println!("{:<40} {:>8} {:>10} {:>9}  snapshot", "tag", "kind", "archived", "comp_dev");
    for t in tags {
        let snap = t.snapshot.map(|p| format!("{:.3} @ {}", p.value, format_time(p.ts))).unwrap_or_else(|| "-".into());
        println!(
            "{:<40} {:>8} {:>10} {:>9}  {snap}",
            t.name,
            format!("{:?}", t.kind).to_lowercase(),
            t.archived_points,
            t.comp_dev
        );
    }
    Ok(())
}

fn cmd_query(a: QueryArgs) -> Result<()> {
    let archive = Archive::open(&a.data, read_only())?;
    let now = now_ms();
    let (start, end) = (parse_time(&a.start, now)?, parse_time(&a.end, now)?);
    let series = archive.series(&a.tag, start, end)?;
    let json = |v: serde_json::Value| println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
    match a.kind {
        QueryKind::Raw => {
            let pts = query::raw(&series, start, end, a.limit);
            match a.format {
                Format::Json => json(serde_json::json!(pts)),
                Format::Csv => {
                    println!("time,value,quality");
                    pts.iter().for_each(|p| println!("{},{},{:?}", format_time(p.ts), p.value, p.quality));
                }
                Format::Table => {
                    println!("{:<26} {:>12}  quality", "time", "value");
                    pts.iter().for_each(|p| println!("{:<26} {:>12.4}  {:?}", format_time(p.ts), p.value, p.quality));
                }
            }
        }
        QueryKind::Interp => {
            let rows = query::interpolated(&series, start, end, parse_duration(&a.step)?)?;
            match a.format {
                Format::Json => json(serde_json::json!(rows)),
                Format::Csv => {
                    println!("time,value");
                    rows.iter().for_each(|r| {
                        println!("{},{}", format_time(r.ts), r.value.map(|v| v.to_string()).unwrap_or_default())
                    });
                }
                Format::Table => {
                    println!("{:<26} {:>12}", "time", "value");
                    rows.iter().for_each(|r| println!("{:<26} {:>12}", format_time(r.ts), opt_num(r.value)));
                }
            }
        }
        QueryKind::Agg => {
            let rows = query::aggregate(&series, start, end, parse_duration(&a.interval)?)?;
            match a.format {
                Format::Json => json(serde_json::json!(rows)),
                Format::Csv => {
                    println!("start,end,avg,min,max,count,coverage");
                    for r in &rows {
                        let f = |v: Option<f64>| v.map(|x| x.to_string()).unwrap_or_default();
                        println!(
                            "{},{},{},{},{},{},{}",
                            format_time(r.start),
                            format_time(r.end),
                            f(r.avg),
                            f(r.min),
                            f(r.max),
                            r.count,
                            r.coverage
                        );
                    }
                }
                Format::Table => {
                    println!(
                        "{:<26} {:>10} {:>10} {:>10} {:>6} {:>8}",
                        "interval start", "twa", "min", "max", "count", "coverage"
                    );
                    for r in &rows {
                        println!(
                            "{:<26} {:>10} {:>10} {:>10} {:>6} {:>7.1}%",
                            format_time(r.start),
                            opt_num(r.avg),
                            opt_num(r.min),
                            opt_num(r.max),
                            r.count,
                            r.coverage * 100.0
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn cmd_verify(a: DataArgs) -> Result<()> {
    let archive = Archive::open(&a.data, read_only())?;
    let checked = archive.verify()?;
    let rec = archive.recovery().clone();
    let st = archive.stats();
    if a.json {
        let v = serde_json::json!({ "recovery": rec, "stats": st, "points_verified": checked });
        println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
        return Ok(());
    }
    println!(
        "recovery  replayed_records={} replayed_points={} truncated_bytes={} orphan_segments_removed={} temp_files_removed={}",
        rec.replayed_records, rec.replayed_points, rec.truncated_bytes, rec.orphan_segments_removed, rec.temp_files_removed
    );
    println!(
        "archive   tags={} segments={} segment_bytes={} wal_bytes={} segment_points={} memtable_points={}",
        st.tags, st.segments, st.segment_bytes, st.wal_bytes, st.segment_points, st.memtable_points
    );
    println!("verified  {checked} points, all chunk checksums OK");
    Ok(())
}

fn cmd_bench(a: BenchArgs) -> Result<()> {
    let cfg = if a.quick { BenchConfig::quick() } else { BenchConfig::full() };
    std::fs::create_dir_all(&a.out)?;
    let scratch = a.out.join("scratch");
    if scratch.exists() {
        std::fs::remove_dir_all(&scratch)?;
    }
    let report = bench::run(&cfg, &scratch)?;
    std::fs::remove_dir_all(&scratch)?;
    let md = bench::to_markdown(&report);
    std::fs::write(a.out.join("bench.md"), &md)?;
    std::fs::write(
        a.out.join("bench.json"),
        serde_json::to_string_pretty(&report).map_err(|e| Error::InvalidInput(e.to_string()))?,
    )?;
    print!("{md}");
    println!("\nwrote {} and bench.json", a.out.join("bench.md").display());
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let level = match cli.verbose {
        0 => "warn",
        1 => "info",
        _ => "debug",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level)).init();
    let result = match cli.cmd {
        Cmd::Simulate(a) => cmd_simulate(a),
        Cmd::Ingest(a) => cmd_ingest(a),
        Cmd::Mqtt(a) => cmd_mqtt(a),
        Cmd::EdgeSim(a) => cmd_edge_sim(a),
        Cmd::Serve(a) => cmd_serve(a),
        Cmd::Tags(a) => cmd_tags(a),
        Cmd::Query(a) => cmd_query(a),
        Cmd::Verify(a) => cmd_verify(a),
        Cmd::Bench(a) => cmd_bench(a),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
