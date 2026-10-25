//! Sparkplug topic namespace: `spBv1.0/<group_id>/<message_type>/<edge_node_id>[/<device_id>]`
//! and the host `spBv1.0/STATE/<host_id>` topic.

use crate::error::{Error, Result};
use std::fmt;

/// Namespace prefix for Sparkplug B.
pub const NAMESPACE: &str = "spBv1.0";

/// Sparkplug message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Edge node birth certificate.
    NBirth,
    /// Edge node death certificate.
    NDeath,
    /// Device birth certificate.
    DBirth,
    /// Device death certificate.
    DDeath,
    /// Edge node data.
    NData,
    /// Device data.
    DData,
    /// Command to an edge node.
    NCmd,
    /// Command to a device.
    DCmd,
    /// Host application state (`spBv1.0/STATE/<host_id>`).
    State,
}

impl MessageType {
    fn parse(s: &str) -> Option<MessageType> {
        Some(match s {
            "NBIRTH" => MessageType::NBirth,
            "NDEATH" => MessageType::NDeath,
            "DBIRTH" => MessageType::DBirth,
            "DDEATH" => MessageType::DDeath,
            "NDATA" => MessageType::NData,
            "DDATA" => MessageType::DData,
            "NCMD" => MessageType::NCmd,
            "DCMD" => MessageType::DCmd,
            _ => return None,
        })
    }

    /// Wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            MessageType::NBirth => "NBIRTH",
            MessageType::NDeath => "NDEATH",
            MessageType::DBirth => "DBIRTH",
            MessageType::DDeath => "DDEATH",
            MessageType::NData => "NDATA",
            MessageType::DData => "DDATA",
            MessageType::NCmd => "NCMD",
            MessageType::DCmd => "DCMD",
            MessageType::State => "STATE",
        }
    }

    /// True for the device-scoped types, which require a device id.
    pub fn is_device(self) -> bool {
        matches!(
            self,
            MessageType::DBirth | MessageType::DDeath | MessageType::DData | MessageType::DCmd
        )
    }
}

/// A parsed Sparkplug topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    /// Message type.
    pub kind: MessageType,
    /// Group id (for STATE: empty).
    pub group: String,
    /// Edge node id (for STATE: the host id).
    pub edge: String,
    /// Device id for device-scoped messages.
    pub device: Option<String>,
}

fn valid_id(s: &str) -> bool {
    !s.is_empty() && !s.contains(['+', '#', '/'])
}

impl Topic {
    /// Parse and validate a topic string.
    pub fn parse(s: &str) -> Result<Topic> {
        let parts: Vec<&str> = s.split('/').collect();
        let bad = || Error::Decode(format!("not a Sparkplug B topic: {s:?}"));
        if parts.first() != Some(&NAMESPACE) {
            return Err(bad());
        }
        if parts.len() == 3 && parts[1] == "STATE" {
            if !valid_id(parts[2]) {
                return Err(bad());
            }
            return Ok(Topic {
                kind: MessageType::State,
                group: String::new(),
                edge: parts[2].to_string(),
                device: None,
            });
        }
        if !(4..=5).contains(&parts.len()) || !parts[1..].iter().all(|p| valid_id(p)) {
            return Err(bad());
        }
        let kind = MessageType::parse(parts[2]).ok_or_else(bad)?;
        let device = parts.get(4).map(|d| d.to_string());
        if kind.is_device() != device.is_some() {
            return Err(bad());
        }
        Ok(Topic {
            kind,
            group: parts[1].to_string(),
            edge: parts[3].to_string(),
            device,
        })
    }

    /// Build a node-scoped topic.
    pub fn node(kind: MessageType, group: &str, edge: &str) -> Topic {
        Topic {
            kind,
            group: group.into(),
            edge: edge.into(),
            device: None,
        }
    }

    /// Build a device-scoped topic.
    pub fn device(kind: MessageType, group: &str, edge: &str, device: &str) -> Topic {
        Topic {
            kind,
            group: group.into(),
            edge: edge.into(),
            device: Some(device.into()),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kind == MessageType::State {
            return write!(f, "{NAMESPACE}/STATE/{}", self.edge);
        }
        write!(
            f,
            "{NAMESPACE}/{}/{}/{}",
            self.group,
            self.kind.as_str(),
            self.edge
        )?;
        if let Some(d) = &self.device {
            write!(f, "/{d}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spec_topics() {
        let t = Topic::parse("spBv1.0/Sparkplug B Devices/DDATA/Raspberry Pi/Pibrella").unwrap();
        assert_eq!(t.kind, MessageType::DData);
        assert_eq!(t.group, "Sparkplug B Devices");
        assert_eq!(t.edge, "Raspberry Pi");
        assert_eq!(t.device.as_deref(), Some("Pibrella"));
        assert_eq!(
            t.to_string(),
            "spBv1.0/Sparkplug B Devices/DDATA/Raspberry Pi/Pibrella"
        );

        let n = Topic::parse("spBv1.0/G1/NBIRTH/E1").unwrap();
        assert_eq!(n, Topic::node(MessageType::NBirth, "G1", "E1"));
        let s = Topic::parse("spBv1.0/STATE/scada-host").unwrap();
        assert_eq!(s.kind, MessageType::State);
        assert_eq!(s.to_string(), "spBv1.0/STATE/scada-host");
        assert_eq!(
            Topic::device(MessageType::DCmd, "G", "E", "D").to_string(),
            "spBv1.0/G/DCMD/E/D"
        );
        for k in ["NDEATH", "DBIRTH", "DDEATH", "NDATA", "NCMD"] {
            let dev = if k.starts_with('D') { "/D" } else { "" };
            let t = Topic::parse(&format!("spBv1.0/G/{k}/E{dev}")).unwrap();
            assert_eq!(t.kind.as_str(), k);
        }
    }

    #[test]
    fn rejects_malformed_topics() {
        for bad in [
            "spAv1.0/G/NDATA/E",
            "spBv1.0/G/NDATA",
            "spBv1.0/G/NDATA/E/D",
            "spBv1.0/G/DDATA/E",
            "spBv1.0/G/XDATA/E",
            "spBv1.0/G+/NDATA/E",
            "spBv1.0//NDATA/E",
            "spBv1.0/STATE/",
            "spBv1.0/G/DDATA/E/D/extra",
        ] {
            assert!(Topic::parse(bad).is_err(), "{bad} should be rejected");
        }
    }
}
