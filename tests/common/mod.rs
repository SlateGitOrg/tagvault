//! Test helpers shared by integration tests.
#![allow(dead_code)]

/// A protobuf wire encoder written independently of prost, so decoder tests do not round-trip
/// through the same library that decodes them.
pub mod wire {
    pub fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    pub fn key(field: u32, wt: u8, out: &mut Vec<u8>) {
        varint(((field as u64) << 3) | wt as u64, out);
    }
    pub fn uint(field: u32, v: u64, out: &mut Vec<u8>) {
        key(field, 0, out);
        varint(v, out);
    }
    pub fn bytes(field: u32, b: &[u8], out: &mut Vec<u8>) {
        key(field, 2, out);
        varint(b.len() as u64, out);
        out.extend_from_slice(b);
    }
    pub fn float(field: u32, v: f32, out: &mut Vec<u8>) {
        key(field, 5, out);
        out.extend_from_slice(&v.to_le_bytes());
    }
    pub fn double(field: u32, v: f64, out: &mut Vec<u8>) {
        key(field, 1, out);
        out.extend_from_slice(&v.to_le_bytes());
    }

    /// Metric value as it appears on the wire.
    #[derive(Clone, Debug)]
    pub enum V {
        Int(u32),
        /// Signed value written sign-extended to 64 bits, like proto `int32` encoders do.
        IntSignExtended(i32),
        Long(u64),
        Float(f32),
        Double(f64),
        Bool(bool),
        Str(&'static str),
        Null,
    }

    #[derive(Clone, Debug, Default)]
    pub struct M {
        pub name: Option<&'static str>,
        pub alias: Option<u64>,
        pub ts: Option<u64>,
        pub datatype: Option<u32>,
        pub value: Option<V>,
        pub historical: bool,
        pub transient: bool,
        pub quality: Option<u32>,
    }

    pub fn metric(m: &M) -> Vec<u8> {
        let mut o = Vec::new();
        if let Some(n) = m.name {
            bytes(1, n.as_bytes(), &mut o);
        }
        if let Some(a) = m.alias {
            uint(2, a, &mut o);
        }
        if let Some(t) = m.ts {
            uint(3, t, &mut o);
        }
        if let Some(d) = m.datatype {
            uint(4, d as u64, &mut o);
        }
        if m.historical {
            uint(5, 1, &mut o);
        }
        if m.transient {
            uint(6, 1, &mut o);
        }
        if let Some(q) = m.quality {
            // PropertySet { keys: ["Quality"], values: [PropertyValue { type: Int32, int_value: q }] }
            let mut pv = Vec::new();
            uint(1, 3, &mut pv);
            uint(3, q as u64, &mut pv);
            let mut ps = Vec::new();
            bytes(1, b"Quality", &mut ps);
            bytes(2, &pv, &mut ps);
            bytes(9, &ps, &mut o);
        }
        match m.value.clone() {
            None => {}
            Some(V::Null) => uint(7, 1, &mut o),
            Some(V::Int(v)) => uint(10, v as u64, &mut o),
            Some(V::IntSignExtended(v)) => uint(10, v as i64 as u64, &mut o),
            Some(V::Long(v)) => uint(11, v, &mut o),
            Some(V::Float(v)) => float(12, v, &mut o),
            Some(V::Double(v)) => double(13, v, &mut o),
            Some(V::Bool(v)) => uint(14, v as u64, &mut o),
            Some(V::Str(s)) => bytes(15, s.as_bytes(), &mut o),
        }
        o
    }

    pub fn payload(ts: Option<u64>, metrics: &[M], seq: Option<u64>) -> Vec<u8> {
        let mut o = Vec::new();
        if let Some(t) = ts {
            uint(1, t, &mut o);
        }
        for m in metrics {
            bytes(2, &metric(m), &mut o);
        }
        if let Some(s) = seq {
            uint(3, s, &mut o);
        }
        o
    }
}

/// Unique scratch directory under the system temp dir, removed on drop.
pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("tagvault-test-").tempdir().expect("tempdir")
}

/// Spawns short-lived child processes (`true`) on a background thread until dropped, the way a
/// concurrently running test that drives a child process does.
pub struct ChildChurn {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    spawned: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ChildChurn {
    pub fn start() -> ChildChurn {
        use std::sync::atomic::Ordering;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let spawned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (s, n) = (stop.clone(), spawned.clone());
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                std::process::Command::new("true").status().expect("spawn `true`");
                n.fetch_add(1, Ordering::Relaxed);
            }
        });
        ChildChurn { stop, spawned, thread: Some(thread) }
    }

    /// Children spawned so far.
    pub fn spawned(&self) -> usize {
        self.spawned.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for ChildChurn {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Proptest settings for integration tests. Failing seeds are saved next to the test source as
/// `tests/<file>.proptest-regressions`. Proptest's default, `SourceParallel`, looks for a
/// `lib.rs`/`main.rs` beside the test file, which integration tests do not have, so on a failure
/// it only printed "FileFailurePersistence::SourceParallel set, but failed to find lib.rs or main.rs".
pub fn proptest_config(cases: u32) -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases,
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..proptest::test_runner::Config::default()
    }
}
