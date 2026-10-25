//! Sparkplug B protobuf schema (`org.eclipse.tahu.protobuf.Payload`, proto2 syntax).
//!
//! Field numbers follow `sparkplug_b.proto` from the Sparkplug 3.0 specification. DataSet,
//! Template and extension values (fields 17–19 of `Metric`) and `MetaData` (field 8) are not
//! modelled: prost skips unknown fields, and the historian stores numeric values only.

/// Top-level Sparkplug B payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Payload {
    /// Milliseconds since epoch (UTC) at which the message was published.
    #[prost(uint64, optional, tag = "1")]
    pub timestamp: Option<u64>,
    /// The metrics carried by the message.
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
    /// Message sequence number, 0–255, wrapping. Absent in NDEATH.
    #[prost(uint64, optional, tag = "3")]
    pub seq: Option<u64>,
    /// Optional UUID describing the `body` contents.
    #[prost(string, optional, tag = "4")]
    pub uuid: Option<String>,
    /// Optional opaque body.
    #[prost(bytes = "vec", optional, tag = "5")]
    pub body: Option<Vec<u8>>,
}

/// One metric (tag) value.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Metric {
    /// Metric name; required in BIRTH messages, optional in DATA when an alias is used.
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    /// Numeric alias assigned in the BIRTH.
    #[prost(uint64, optional, tag = "2")]
    pub alias: Option<u64>,
    /// Sample timestamp (ms since epoch).
    #[prost(uint64, optional, tag = "3")]
    pub timestamp: Option<u64>,
    /// [`super::datatype::DataType`] code.
    #[prost(uint32, optional, tag = "4")]
    pub datatype: Option<u32>,
    /// True for values replayed from an edge store-and-forward buffer.
    #[prost(bool, optional, tag = "5")]
    pub is_historical: Option<bool>,
    /// True for values that must not be historized.
    #[prost(bool, optional, tag = "6")]
    pub is_transient: Option<bool>,
    /// True when the value is null.
    #[prost(bool, optional, tag = "7")]
    pub is_null: Option<bool>,
    /// Metric properties (e.g. the reserved `Quality` property).
    #[prost(message, optional, tag = "9")]
    pub properties: Option<PropertySet>,
    /// The value.
    #[prost(oneof = "metric::Value", tags = "10, 11, 12, 13, 14, 15, 16")]
    pub value: Option<metric::Value>,
}

/// Nested types of [`Metric`].
pub mod metric {
    /// Metric value variants (scalar subset).
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        /// Int8/16/32 and UInt8/16/32.
        #[prost(uint32, tag = "10")]
        IntValue(u32),
        /// Int64, UInt64 and DateTime.
        #[prost(uint64, tag = "11")]
        LongValue(u64),
        /// Float.
        #[prost(float, tag = "12")]
        FloatValue(f32),
        /// Double.
        #[prost(double, tag = "13")]
        DoubleValue(f64),
        /// Boolean.
        #[prost(bool, tag = "14")]
        BooleanValue(bool),
        /// String, Text and UUID.
        #[prost(string, tag = "15")]
        StringValue(String),
        /// Bytes and File.
        #[prost(bytes, tag = "16")]
        BytesValue(Vec<u8>),
    }
}

/// A set of key/value properties attached to a metric.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PropertySet {
    /// Property names, parallel to `values`.
    #[prost(string, repeated, tag = "1")]
    pub keys: Vec<String>,
    /// Property values, parallel to `keys`.
    #[prost(message, repeated, tag = "2")]
    pub values: Vec<PropertyValue>,
}

/// One property value (scalar subset; nested property sets are skipped).
#[derive(Clone, PartialEq, prost::Message)]
pub struct PropertyValue {
    /// Datatype code.
    #[prost(uint32, optional, tag = "1")]
    pub r#type: Option<u32>,
    /// Null flag.
    #[prost(bool, optional, tag = "2")]
    pub is_null: Option<bool>,
    /// The value.
    #[prost(oneof = "property_value::Value", tags = "3, 4, 5, 6, 7, 8")]
    pub value: Option<property_value::Value>,
}

/// Nested types of [`PropertyValue`].
pub mod property_value {
    /// Property value variants.
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        /// 8–32-bit integers.
        #[prost(uint32, tag = "3")]
        IntValue(u32),
        /// 64-bit integers.
        #[prost(uint64, tag = "4")]
        LongValue(u64),
        /// Float.
        #[prost(float, tag = "5")]
        FloatValue(f32),
        /// Double.
        #[prost(double, tag = "6")]
        DoubleValue(f64),
        /// Boolean.
        #[prost(bool, tag = "7")]
        BooleanValue(bool),
        /// String.
        #[prost(string, tag = "8")]
        StringValue(String),
    }
}

impl PropertySet {
    /// Look up an integer property by key.
    pub fn int(&self, key: &str) -> Option<i64> {
        let idx = self.keys.iter().position(|k| k == key)?;
        match self.values.get(idx)?.value.as_ref()? {
            property_value::Value::IntValue(v) => Some(*v as i32 as i64),
            property_value::Value::LongValue(v) => Some(*v as i64),
            _ => None,
        }
    }
}
