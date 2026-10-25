//! Per-tag data-reduction policy, resolved by longest tag-name prefix.
//!
//! ```json
//! {
//!   "defaults": { "exc_dev": 0.05, "comp_dev": 0.1, "exc_max_ms": 600000, "comp_max_ms": 3600000 },
//!   "rules": [ { "prefix": "Plant1/Line1/Tank01/", "comp_dev": 0.5 } ]
//! }
//! ```
//!
//! Deviations are absolute, in the tag's engineering units (PI expresses them as % of span; an
//! absolute value avoids needing a span for every tag). A `*_max_ms` of `0` disables the forced
//! report/archive.

use crate::compress::deadband::ExceptionConfig;
use crate::compress::sdt::{SdtConfig, SdtMode};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Optional overrides; unset fields inherit from the enclosing level.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagSettings {
    /// Exception deviation (collector).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exc_dev: Option<f64>,
    /// Forced exception report interval in ms (0 = off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exc_max_ms: Option<i64>,
    /// Compression deviation (archive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comp_dev: Option<f64>,
    /// Forced archive interval in ms (0 = off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comp_max_ms: Option<i64>,
}

/// One prefix rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Tag-name prefix this rule applies to.
    pub prefix: String,
    /// Overrides.
    #[serde(flatten)]
    pub settings: TagSettings,
}

/// The whole policy file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Settings applied to every tag.
    #[serde(default)]
    pub defaults: TagSettings,
    /// Prefix rules; the longest matching prefix wins.
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// Fully resolved settings for one tag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Resolved {
    /// Collector-side exception filter.
    pub exception: ExceptionConfig,
    /// Archive-side compression.
    pub compression: SdtConfig,
}

fn max_ms(v: i64) -> Option<i64> {
    (v > 0).then_some(v)
}

impl Policy {
    /// Load from a JSON file.
    pub fn load(path: &Path) -> Result<Policy> {
        let text = std::fs::read_to_string(path)?;
        let p: Policy = serde_json::from_str(&text)?;
        p.validate()?;
        Ok(p)
    }

    /// Policy with uniform deviations for every tag.
    pub fn uniform(exc_dev: f64, comp_dev: f64) -> Policy {
        Policy {
            defaults: TagSettings { exc_dev: Some(exc_dev), comp_dev: Some(comp_dev), ..Default::default() },
            rules: Vec::new(),
        }
    }

    /// Reject negative or non-finite deviations.
    pub fn validate(&self) -> Result<()> {
        let all = std::iter::once(&self.defaults).chain(self.rules.iter().map(|r| &r.settings));
        for s in all {
            for d in [s.exc_dev, s.comp_dev].into_iter().flatten() {
                if !d.is_finite() || d < 0.0 {
                    return Err(Error::Config(format!("deviation must be finite and >= 0, got {d}")));
                }
            }
            for m in [s.exc_max_ms, s.comp_max_ms].into_iter().flatten() {
                if m < 0 {
                    return Err(Error::Config(format!("max interval must be >= 0, got {m}")));
                }
            }
        }
        Ok(())
    }

    /// Resolve the settings for `tag`.
    pub fn resolve(&self, tag: &str) -> Resolved {
        let mut s = self.defaults.clone();
        if let Some(rule) = self.rules.iter().filter(|r| tag.starts_with(&r.prefix)).max_by_key(|r| r.prefix.len()) {
            let o = &rule.settings;
            s.exc_dev = o.exc_dev.or(s.exc_dev);
            s.exc_max_ms = o.exc_max_ms.or(s.exc_max_ms);
            s.comp_dev = o.comp_dev.or(s.comp_dev);
            s.comp_max_ms = o.comp_max_ms.or(s.comp_max_ms);
        }
        Resolved {
            exception: ExceptionConfig { dev: s.exc_dev.unwrap_or(0.0), max_ms: max_ms(s.exc_max_ms.unwrap_or(600_000)) },
            compression: SdtConfig {
                dev: s.comp_dev.unwrap_or(0.0),
                max_ms: max_ms(s.comp_max_ms.unwrap_or(3_600_000)),
                mode: SdtMode::Strict,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_prefix_wins_and_inherits() {
        let p: Policy = serde_json::from_str(
            r#"{"defaults":{"exc_dev":0.1,"comp_dev":0.2},
                "rules":[{"prefix":"P/","comp_dev":1.0},{"prefix":"P/Tank","exc_dev":0.5,"comp_max_ms":0}]}"#,
        )
        .unwrap();
        p.validate().unwrap();
        let r = p.resolve("P/Tank01/Level");
        assert_eq!(r.exception.dev, 0.5);
        assert_eq!(r.compression.dev, 0.2, "only the longest rule applies; others fall back to defaults");
        assert_eq!(r.compression.max_ms, None);
        assert_eq!(p.resolve("P/Flow").compression.dev, 1.0);
        let d = p.resolve("Q/x");
        assert_eq!((d.exception.dev, d.compression.dev), (0.1, 0.2));
        assert_eq!(d.exception.max_ms, Some(600_000));
    }

    #[test]
    fn rejects_bad_values_and_unknown_fields() {
        assert!(Policy::uniform(-1.0, 0.0).validate().is_err());
        assert!(Policy::uniform(0.0, f64::NAN).validate().is_err());
        let p = Policy { defaults: TagSettings { exc_max_ms: Some(-5), ..Default::default() }, rules: vec![] };
        assert!(p.validate().is_err());
        assert!(serde_json::from_str::<Policy>(r#"{"defaults":{"typo":1}}"#).is_err());
    }

    #[test]
    fn loads_from_file() {
        let dir = std::env::temp_dir().join(format!("tagvault-policy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("p.json");
        std::fs::write(&f, r#"{"defaults":{"comp_dev":0.3}}"#).unwrap();
        assert_eq!(Policy::load(&f).unwrap().resolve("x").compression.dev, 0.3);
        std::fs::write(&f, r#"{"defaults":{"comp_dev":-0.3}}"#).unwrap();
        assert!(Policy::load(&f).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
