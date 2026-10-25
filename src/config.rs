//! Per-tag data-reduction policy, resolved by the most specific matching glob pattern.
//!
//! ```json
//! {
//!   "defaults": { "exc_dev": 0.05, "comp_dev": 0.1, "exc_max_ms": 600000, "comp_max_ms": 3600000 },
//!   "rules": [ { "pattern": "Plant1/*/Tank01/Level", "comp_dev": 0.5 } ]
//! }
//! ```
//!
//! A pattern must match the whole tag name; `*` matches any run of characters (including `/`).
//! When several rules match, the one with the most literal characters wins.
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
    /// SDT mode: `"lookback"` (default) or `"strict"` (`"classic"` is accepted for experiments).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comp_mode: Option<SdtMode>,
}

/// One pattern rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Glob over the full tag name (`*` = any characters).
    pub pattern: String,
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
    /// Pattern rules; the most specific match wins.
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

/// Glob match where `*` matches any (possibly empty) run of characters.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let (p, n) = (pattern.as_bytes(), name.as_bytes());
    let (mut pi, mut ni) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ni < n.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some((pi, ni));
            pi += 1;
        } else if pi < p.len() && p[pi] == n[ni] {
            pi += 1;
            ni += 1;
        } else if let Some((sp, sn)) = star {
            pi = sp + 1;
            ni = sn + 1;
            star = Some((sp, sn + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == b'*')
}

/// SDT mode used when a policy does not say otherwise.
pub const DEFAULT_MODE: SdtMode = SdtMode::Lookback;

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
        let specificity = |r: &&Rule| r.pattern.bytes().filter(|&c| c != b'*').count();
        if let Some(rule) = self.rules.iter().filter(|r| glob_match(&r.pattern, tag)).max_by_key(specificity) {
            let o = &rule.settings;
            s.exc_dev = o.exc_dev.or(s.exc_dev);
            s.exc_max_ms = o.exc_max_ms.or(s.exc_max_ms);
            s.comp_dev = o.comp_dev.or(s.comp_dev);
            s.comp_max_ms = o.comp_max_ms.or(s.comp_max_ms);
            s.comp_mode = o.comp_mode.or(s.comp_mode);
        }
        Resolved {
            exception: ExceptionConfig {
                dev: s.exc_dev.unwrap_or(0.0),
                max_ms: max_ms(s.exc_max_ms.unwrap_or(600_000)),
            },
            compression: SdtConfig {
                dev: s.comp_dev.unwrap_or(0.0),
                max_ms: max_ms(s.comp_max_ms.unwrap_or(3_600_000)),
                mode: s.comp_mode.unwrap_or(DEFAULT_MODE),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn most_specific_rule_wins_and_inherits() {
        let p: Policy = serde_json::from_str(
            r#"{"defaults":{"exc_dev":0.1,"comp_dev":0.2},
                "rules":[{"pattern":"P/*","comp_dev":1.0},{"pattern":"P/Tank*","exc_dev":0.5,"comp_max_ms":0,"comp_mode":"strict"}]}"#,
        )
        .unwrap();
        p.validate().unwrap();
        let r = p.resolve("P/Tank01/Level");
        assert_eq!(r.exception.dev, 0.5);
        assert_eq!(r.compression.dev, 0.2, "only the most specific rule applies; others fall back to defaults");
        assert_eq!(r.compression.max_ms, None);
        assert_eq!(r.compression.mode, SdtMode::Strict);
        assert_eq!(p.resolve("P/Flow").compression.mode, DEFAULT_MODE);
        assert_eq!(p.resolve("P/Flow").compression.dev, 1.0);
        let d = p.resolve("Q/x");
        assert_eq!((d.exception.dev, d.compression.dev), (0.1, 0.2));
        assert_eq!(d.exception.max_ms, Some(600_000));
    }

    #[test]
    fn glob_semantics() {
        assert!(glob_match("Plant1/*/Tank01/Level", "Plant1/Line2/Tank01/Level"));
        assert!(glob_match("*", ""));
        assert!(glob_match("a*b*c", "a/x/b/y/c"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
        assert!(!glob_match("*/Level", "Plant1/Line1/Tank01/LevelX"));
        assert!(glob_match("*Level", "LevelLevel"));
        assert!(!glob_match("a*c", "ab"));
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
