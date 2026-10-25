//! Sparkplug B decode and host state-machine tests.
//!
//! The payloads below are modelled on the example messages in the Sparkplug 3.0 specification
//! (an edge node "Raspberry Pi" in group "Sparkplug B Devices" with a "Pibrella" device): the
//! metric names, datatypes and values follow the spec's examples, and the protobuf bytes are
//! produced by the independent encoder in `tests/common` rather than by prost.

mod common;

use common::wire::{payload, M, V};
use prost::Message;
use tagvault::sparkplug::host::RebirthReason;
use tagvault::sparkplug::proto::{metric, Payload};
use tagvault::sparkplug::{Host, HostConfig, HostEvent};
use tagvault::{Quality, ValueKind};

const G: &str = "Sparkplug B Devices";
const E: &str = "Raspberry Pi";
const T0: u64 = 1_486_144_502_122;

fn nbirth_topic() -> String {
    format!("spBv1.0/{G}/NBIRTH/{E}")
}
fn topic(kind: &str, device: Option<&str>) -> String {
    match device {
        Some(d) => format!("spBv1.0/{G}/{kind}/{E}/{d}"),
        None => format!("spBv1.0/{G}/{kind}/{E}"),
    }
}

/// Spec-style NBIRTH: bdSeq, node controls, a string property and two process metrics.
fn nbirth(seq: u64, bd_seq: u64) -> Vec<u8> {
    payload(
        Some(T0),
        &[
            M {
                name: Some("bdSeq"),
                ts: Some(T0),
                datatype: Some(8),
                value: Some(V::Long(bd_seq)),
                ..Default::default()
            },
            M {
                name: Some("Node Control/Reboot"),
                ts: Some(T0),
                datatype: Some(11),
                value: Some(V::Bool(false)),
                ..Default::default()
            },
            M {
                name: Some("Node Control/Rebirth"),
                ts: Some(T0),
                datatype: Some(11),
                value: Some(V::Bool(false)),
                ..Default::default()
            },
            M {
                name: Some("Node Control/Scan Rate"),
                ts: Some(T0),
                datatype: Some(4),
                value: Some(V::Long(3000)),
                ..Default::default()
            },
            M {
                name: Some("Properties/Hardware Make"),
                ts: Some(T0),
                datatype: Some(12),
                value: Some(V::Str("Raspberry Pi")),
                ..Default::default()
            },
            M {
                name: Some("Supply Voltage"),
                alias: Some(1),
                ts: Some(T0),
                datatype: Some(9),
                value: Some(V::Float(12.1)),
                ..Default::default()
            },
            M {
                name: Some("Uptime"),
                alias: Some(2),
                ts: Some(T0),
                datatype: Some(8),
                value: Some(V::Long(3600)),
                ..Default::default()
            },
        ],
        Some(seq),
    )
}

/// Spec-style DBIRTH for the Pibrella device.
fn dbirth(seq: u64) -> Vec<u8> {
    payload(
        Some(T0 + 10),
        &[
            M {
                name: Some("Inputs/A"),
                alias: Some(10),
                datatype: Some(11),
                value: Some(V::Bool(false)),
                ..Default::default()
            },
            M {
                name: Some("Inputs/B"),
                alias: Some(11),
                datatype: Some(11),
                value: Some(V::Bool(true)),
                ..Default::default()
            },
            M {
                name: Some("Outputs/LEDs/Green"),
                alias: Some(12),
                datatype: Some(11),
                value: Some(V::Bool(true)),
                ..Default::default()
            },
            M {
                name: Some("Temperature"),
                alias: Some(13),
                datatype: Some(2),
                value: Some(V::IntSignExtended(-12)),
                ..Default::default()
            },
            M {
                name: Some("Level"),
                alias: Some(14),
                datatype: Some(10),
                value: Some(V::Double(42.5)),
                ..Default::default()
            },
        ],
        Some(seq),
    )
}

fn ddata(seq: u64, ts: u64, level: f64) -> Vec<u8> {
    payload(
        Some(ts),
        &[
            M { alias: Some(10), ts: Some(ts), value: Some(V::Bool(true)), ..Default::default() },
            M { alias: Some(14), ts: Some(ts), value: Some(V::Double(level)), ..Default::default() },
        ],
        Some(seq),
    )
}

fn born_host(cfg: HostConfig) -> Host {
    let mut h = Host::new(cfg);
    h.handle(&nbirth_topic(), &nbirth(0, 0), 0);
    h.handle(&topic("DBIRTH", Some("Pibrella")), &dbirth(1), 0);
    h
}

#[test]
fn decodes_spec_style_births_and_data() {
    let mut host = Host::new(HostConfig::default());
    let out = host.handle(&nbirth_topic(), &nbirth(0, 0), 5);
    // bdSeq and Node Control/* are session metadata; the string property is not numeric.
    let tags: Vec<&str> = out.samples.iter().map(|s| s.tag.as_str()).collect();
    assert_eq!(
        tags,
        vec!["Sparkplug B Devices/Raspberry Pi/Supply Voltage", "Sparkplug B Devices/Raspberry Pi/Uptime"]
    );
    assert!((out.samples[0].point.value - 12.1).abs() < 1e-6);
    assert_eq!(out.samples[0].point.ts, T0 as i64);
    assert_eq!(host.stats().unsupported_values, 1);
    assert!(matches!(out.events[0], HostEvent::NodeBirth { bd_seq: Some(0), metrics: 7, .. }));

    let out = host.handle(&topic("DBIRTH", Some("Pibrella")), &dbirth(1), 6);
    assert_eq!(out.samples.len(), 5);
    let temp = out.samples.iter().find(|s| s.tag.ends_with("/Temperature")).unwrap();
    assert_eq!(temp.point.value, -12.0, "Int16 must be sign-extended");
    assert_eq!(temp.kind, ValueKind::Analog);
    let input_b = out.samples.iter().find(|s| s.tag.ends_with("Inputs/B")).unwrap();
    assert_eq!((input_b.point.value, input_b.kind), (1.0, ValueKind::Discrete));
    // Metric without its own timestamp inherits the payload timestamp.
    assert_eq!(temp.point.ts, (T0 + 10) as i64);

    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(2, T0 + 1000, 43.0), 7);
    let tags: Vec<&str> = out.samples.iter().map(|s| s.tag.as_str()).collect();
    assert_eq!(
        tags,
        vec!["Sparkplug B Devices/Raspberry Pi/Pibrella/Inputs/A", "Sparkplug B Devices/Raspberry Pi/Pibrella/Level"]
    );
    assert_eq!(out.samples[1].point.value, 43.0);
    assert!(out.commands.is_empty());
    assert_eq!(host.node_online(G, E), Some(true));
    assert_eq!(host.stats().data_messages, 1);
}

#[test]
fn ndata_resolves_by_name_without_alias() {
    let mut host = born_host(HostConfig::default());
    let p = payload(
        Some(T0 + 5),
        &[M { name: Some("Supply Voltage"), value: Some(V::Float(11.9)), ..Default::default() }],
        Some(2),
    );
    let out = host.handle(&topic("NDATA", None), &p, 1);
    assert_eq!(out.samples.len(), 1);
    assert!((out.samples[0].point.value - 11.9).abs() < 1e-6);
}

#[test]
fn sequence_gap_is_reordered_within_timeout() {
    let mut host = born_host(HostConfig { reorder_timeout_ms: 1000, ..Default::default() });
    // seq 3 arrives before seq 2.
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(3, T0 + 2000, 2.0), 100);
    assert!(out.samples.is_empty());
    assert!(matches!(out.events[0], HostEvent::SequenceGap { expected: 2, got: 3, .. }));
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(2, T0 + 1000, 1.0), 500);
    let levels: Vec<f64> = out.samples.iter().filter(|s| s.tag.ends_with("Level")).map(|s| s.point.value).collect();
    assert_eq!(levels, vec![1.0, 2.0], "buffered message applied after the gap filled, in order");
    assert!(out.events.iter().any(|e| matches!(e, HostEvent::GapRecovered { messages: 1, .. })));
    assert!(out.commands.is_empty());
    assert_eq!(host.stats().reordered, 1);
}

#[test]
fn sequence_gap_timeout_requests_rebirth_and_rebirth_restores_session() {
    let mut host =
        born_host(HostConfig { reorder_timeout_ms: 1000, rebirth_cooldown_ms: 10_000, ..Default::default() });
    host.handle(&topic("DDATA", Some("Pibrella")), &ddata(3, T0 + 2000, 2.0), 100);
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(4, T0 + 3000, 3.0), 1200);
    assert_eq!(out.commands.len(), 1);
    let cmd = &out.commands[0];
    assert_eq!(cmd.topic, format!("spBv1.0/{G}/NCMD/{E}"));
    let decoded = Payload::decode(cmd.payload.as_slice()).unwrap();
    assert_eq!(decoded.metrics[0].name.as_deref(), Some("Node Control/Rebirth"));
    assert_eq!(decoded.metrics[0].value, Some(metric::Value::BooleanValue(true)));
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::ReorderTimeout, .. })));
    assert_eq!(host.node_online(G, E), Some(false));

    // Until the new NBIRTH, data is discarded (and the request is not repeated inside the cooldown).
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(5, T0 + 4000, 4.0), 1300);
    assert!(out.samples.is_empty() && out.commands.is_empty());

    // Edge answers with a fresh birth sequence (Sparkplug 3.0 allows any starting seq).
    host.handle(&nbirth_topic(), &nbirth(17, 1), 1400);
    host.handle(&topic("DBIRTH", Some("Pibrella")), &dbirth(18), 1400);
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(19, T0 + 5000, 5.0), 1500);
    assert_eq!(out.samples.len(), 2);
    assert_eq!(host.stats().rebirth_requests, 1);
    assert_eq!(host.stats().seq_gaps, 1);
}

#[test]
fn tick_fires_reorder_timeout_without_new_messages() {
    let mut host = born_host(HostConfig { reorder_timeout_ms: 500, ..Default::default() });
    host.handle(&topic("DDATA", Some("Pibrella")), &ddata(5, T0, 1.0), 0);
    assert!(host.tick(100).commands.is_empty());
    let out = host.tick(600);
    assert_eq!(out.commands.len(), 1);
    assert_eq!(host.stats().discarded, 1);
}

#[test]
fn sequence_wraps_from_255_to_0_and_duplicates_are_dropped() {
    let mut host = Host::new(HostConfig::default());
    host.handle(&nbirth_topic(), &nbirth(254, 0), 0);
    host.handle(&topic("DBIRTH", Some("Pibrella")), &dbirth(255), 0);
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(0, T0 + 1, 1.0), 1);
    assert_eq!(out.samples.len(), 2);
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(0, T0 + 1, 1.0), 2);
    assert!(out.samples.is_empty());
    assert!(matches!(&out.events[0], HostEvent::Discarded { reason, .. } if reason.contains("duplicate")));
    assert!(out.commands.is_empty());
}

#[test]
fn unknown_alias_discards_message_and_requests_rebirth() {
    let mut host = born_host(HostConfig::default());
    let p = payload(
        Some(T0),
        &[
            M { alias: Some(14), value: Some(V::Double(1.0)), ..Default::default() },
            M { alias: Some(99), value: Some(V::Double(1.0)), ..Default::default() },
        ],
        Some(2),
    );
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &p, 10);
    assert!(out.samples.is_empty(), "nothing from an untrusted message is archived");
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::UnknownAlias(99), .. })));
}

#[test]
fn node_alias_used_on_device_topic_is_rejected() {
    let mut host = born_host(HostConfig::default());
    let p = payload(Some(T0), &[M { alias: Some(1), value: Some(V::Float(1.0)), ..Default::default() }], Some(2));
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &p, 10);
    assert_eq!(out.commands.len(), 1);
    assert!(out.samples.is_empty());
}

#[test]
fn data_before_birth_requests_rebirth() {
    let mut host = Host::new(HostConfig::default());
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(0, T0, 1.0), 0);
    assert_eq!(out.commands.len(), 1);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::NoSession, .. })));

    // Node born, but DDATA for a device without DBIRTH.
    host.handle(&nbirth_topic(), &nbirth(0, 0), 10_000);
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(1, T0, 1.0), 10_001);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::UnknownDevice(_), .. })));
}

#[test]
fn ndeath_with_matching_bdseq_marks_tags_stale_and_old_bdseq_is_ignored() {
    let mut host = born_host(HostConfig::default());
    let death_old = payload(
        Some(T0 + 9),
        &[M { name: Some("bdSeq"), datatype: Some(8), value: Some(V::Long(7)), ..Default::default() }],
        None,
    );
    let out = host.handle(&topic("NDEATH", None), &death_old, 50);
    assert!(out.samples.is_empty());
    assert_eq!(host.stats().stale_deaths_ignored, 1);

    let death = payload(
        None,
        &[M { name: Some("bdSeq"), datatype: Some(8), value: Some(V::Long(0)), ..Default::default() }],
        None,
    );
    let out = host.handle(&topic("NDEATH", None), &death, 60);
    // 2 node process metrics + 5 device metrics go stale at the receive time.
    assert_eq!(out.samples.len(), 7);
    assert!(out.samples.iter().all(|s| s.point.quality == Quality::Stale && s.point.ts == 60));
    assert_eq!(host.node_online(G, E), Some(false));
    // A second death for an offline node is discarded.
    let out = host.handle(&topic("NDEATH", None), &death, 61);
    assert!(out.samples.is_empty());
}

#[test]
fn ddeath_marks_device_tags_stale() {
    let mut host = born_host(HostConfig::default());
    let out = host.handle(&topic("DDEATH", Some("Pibrella")), &payload(Some(T0 + 50), &[], Some(2)), 70);
    assert_eq!(out.samples.len(), 5);
    assert!(out.samples.iter().all(|s| s.point.quality == Quality::Stale && s.point.ts == (T0 + 50) as i64));
    // Data for the dead device now triggers a rebirth.
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &ddata(3, T0 + 60, 1.0), 80);
    assert_eq!(out.commands.len(), 1);
}

#[test]
fn null_quality_transient_and_historical_flags() {
    let mut host = born_host(HostConfig::default());
    let p = payload(
        Some(T0 + 100),
        &[
            M { alias: Some(14), value: Some(V::Null), ..Default::default() },
            M { alias: Some(13), value: Some(V::Int(5)), quality: Some(500), ..Default::default() },
            M { alias: Some(11), value: Some(V::Bool(false)), quality: Some(0), ..Default::default() },
            M { alias: Some(12), value: Some(V::Bool(false)), transient: true, ..Default::default() },
            M {
                alias: Some(10),
                ts: Some(T0 - 5000),
                value: Some(V::Bool(true)),
                historical: true,
                ..Default::default()
            },
        ],
        Some(2),
    );
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &p, 1);
    assert_eq!(out.samples.len(), 4);
    assert_eq!(out.samples[0].point.quality, Quality::Bad);
    assert!(out.samples[0].point.value.is_nan());
    assert_eq!(out.samples[1].point.quality, Quality::Stale);
    assert_eq!(out.samples[2].point.quality, Quality::Bad);
    assert_eq!(out.samples[3].point.ts, (T0 - 5000) as i64, "historical values keep their own timestamp");
    assert_eq!(host.stats().transient_skipped, 1);
}

#[test]
fn mismatched_value_type_is_counted_not_archived() {
    let mut host = born_host(HostConfig::default());
    let p = payload(Some(T0), &[M { alias: Some(14), value: Some(V::Str("oops")), ..Default::default() }], Some(2));
    let out = host.handle(&topic("DDATA", Some("Pibrella")), &p, 1);
    assert!(out.samples.is_empty());
    assert_eq!(host.stats().unsupported_values, 2, "one from the NBIRTH string property, one here");
}

#[test]
fn rebirth_requests_respect_cooldown() {
    let mut host = Host::new(HostConfig { rebirth_cooldown_ms: 1000, ..Default::default() });
    let a = host.handle(&topic("NDATA", None), &payload(None, &[], Some(0)), 0);
    let b = host.handle(&topic("NDATA", None), &payload(None, &[], Some(1)), 500);
    let c = host.handle(&topic("NDATA", None), &payload(None, &[], Some(2)), 1500);
    assert_eq!((a.commands.len(), b.commands.len(), c.commands.len()), (1, 0, 1));
}

#[test]
fn malformed_inputs_are_counted() {
    let mut host = born_host(HostConfig::default());
    let out = host.handle("not/a/sparkplug/topic", b"", 0);
    assert!(matches!(out.events[0], HostEvent::Discarded { .. }));
    host.handle(&topic("NDATA", None), &[0xff, 0xff, 0xff], 0);
    assert_eq!(host.stats().decode_errors, 1);
    host.handle("spBv1.0/STATE/other-host", br#"{"online":true,"timestamp":1}"#, 0);
    assert_eq!(host.stats().state_messages, 1);
    let out = host.handle(&topic("NCMD", None), &payload(None, &[], None), 0);
    assert!(out.samples.is_empty() && out.events.is_empty());
    // NBIRTH without seq and with an unnamed metric.
    let out = host.handle(&nbirth_topic(), &payload(Some(T0), &[], None), 20_000);
    assert_eq!(out.commands.len(), 1);
    let unnamed = payload(
        Some(T0),
        &[M { alias: Some(1), datatype: Some(9), value: Some(V::Float(1.0)), ..Default::default() }],
        Some(0),
    );
    let out = host.handle(&nbirth_topic(), &unnamed, 40_000);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::MalformedBirth, .. })));
    // A sequenced message with an out-of-range seq.
    host.handle(&nbirth_topic(), &nbirth(0, 3), 60_000);
    let out = host.handle(&topic("NDATA", None), &payload(None, &[], Some(300)), 60_001);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, HostEvent::RebirthRequested { reason: RebirthReason::BadSequence, .. })));
    assert_eq!(host.config().host_id, "tagvault");
}
