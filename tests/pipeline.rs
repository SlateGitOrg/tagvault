//! End-to-end: simulated plant → Sparkplug host → exception filter → spool (with a link
//! outage) → archive (SDT) → query. Checks the combined error bound on every decoded sample.

mod common;

use std::collections::HashMap;
use tagvault::collector::{replay, Collector};
use tagvault::config::Policy;
use tagvault::forward::spool::{Spool, SpoolOptions};
use tagvault::forward::{ArchiveSink, Backoff, Forwarder, OutageSink};
use tagvault::sim::Scenario;
use tagvault::sparkplug::Host;
use tagvault::storage::{Archive, ArchiveOptions, SyncMode};
use tagvault::{query, Point};

const EXC: f64 = 0.02;
const COMP: f64 = 0.1;

#[test]
fn plant_to_query_respects_combined_error_bound_through_an_outage() {
    let scenario = Scenario { duration_s: 5400, node_restart: Some((1800, 60)), drop_prob: 0.001, swap_prob: 0.005, ..Default::default() };
    let (frames, summary) = scenario.generate();
    assert!(summary.rebirths >= 1 && summary.swapped >= 1);

    let dir = common::tempdir();
    let policy = Policy::uniform(EXC, COMP);
    let spool = Spool::open(&dir.path().join("spool"), SpoolOptions { sync: SyncMode::Os, ..Default::default() }).unwrap();
    let mut collector = Collector::new(Host::new(scenario.host.clone()), policy.clone(), Forwarder::new(spool, Backoff::default()), 2000);
    let mut archive = Archive::open(
        &dir.path().join("archive"),
        ArchiveOptions { sync: SyncMode::Os, flush_points: 5_000, policy: policy.clone(), ..Default::default() },
    )
    .unwrap();
    let outage = (scenario.start + 2400_000, scenario.start + 3600_000);
    let report = {
        let mut sink = OutageSink::new(ArchiveSink::new(&mut archive), vec![outage]);
        let r = replay(&frames, &mut collector, &mut sink, 1000).unwrap();
        assert_eq!(sink.into_inner().rejected_points, 0, "no duplicates/out-of-order in a clean run");
        r
    };
    assert!(report.link_failures > 0);
    assert!(report.max_backlog > 50, "a 20-minute outage must build a backlog: {report:?}");
    assert_eq!(report.left_in_spool, 0, "everything drains after the link returns");
    assert!(report.commands >= 1);
    let cs = collector.stats();
    assert!(cs.samples_reported < cs.samples_in, "exception filter must drop something");

    // Reference: every sample the host decoded, per tag.
    let mut host = Host::new(scenario.host.clone());
    let mut raw: HashMap<String, Vec<Point>> = HashMap::new();
    for f in &frames {
        for s in host.handle(&f.topic, &f.payload, f.recv_ts).samples {
            raw.entry(s.tag).or_default().push(s.point);
        }
    }
    let tags = archive.tags();
    assert_eq!(tags.len(), raw.len());
    let bound = 2.0 * EXC + COMP + 1e-6;
    let mut checked = 0;
    let mut archived = 0;
    for t in &tags {
        let pts = &raw[&t.name];
        let series = archive.series(&t.name, pts[0].ts, pts.last().unwrap().ts).unwrap();
        archived += t.archived_points;
        for p in pts.iter().filter(|p| p.quality.is_good()) {
            let v = query::value_at(&series, p.ts).unwrap_or_else(|| panic!("{} undefined at {}", t.name, p.ts));
            assert!((v - p.value).abs() <= bound, "{} at {}: archive {v} vs raw {} (bound {bound})", t.name, p.ts, p.value);
            checked += 1;
        }
    }
    assert!(checked > 50_000, "checked {checked}");
    assert!(archived * 3 < checked as u64, "end-to-end reduction should exceed 3x: {archived} / {checked}");
}
