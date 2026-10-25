//! Sparkplug B datatype codes and numeric conversion.

use super::proto::metric::Value;
use crate::types::ValueKind;

/// Sparkplug B metric datatypes (spec v3.0, `DataType` enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    Boolean,
    String,
    DateTime,
    Text,
    Uuid,
    DataSet,
    Bytes,
    File,
    Template,
    /// Any other code (property sets, arrays, unknown).
    Other(u32),
}

impl DataType {
    /// Map a wire code.
    pub fn from_code(c: u32) -> DataType {
        use DataType::*;
        match c {
            1 => Int8,
            2 => Int16,
            3 => Int32,
            4 => Int64,
            5 => UInt8,
            6 => UInt16,
            7 => UInt32,
            8 => UInt64,
            9 => Float,
            10 => Double,
            11 => Boolean,
            12 => String,
            13 => DateTime,
            14 => Text,
            15 => Uuid,
            16 => DataSet,
            17 => Bytes,
            18 => File,
            19 => Template,
            other => Other(other),
        }
    }

    /// Wire code.
    pub fn code(self) -> u32 {
        use DataType::*;
        match self {
            Int8 => 1,
            Int16 => 2,
            Int32 => 3,
            Int64 => 4,
            UInt8 => 5,
            UInt16 => 6,
            UInt32 => 7,
            UInt64 => 8,
            Float => 9,
            Double => 10,
            Boolean => 11,
            String => 12,
            DateTime => 13,
            Text => 14,
            Uuid => 15,
            DataSet => 16,
            Bytes => 17,
            File => 18,
            Template => 19,
            Other(c) => c,
        }
    }

    /// Interpolation behaviour of a numeric datatype, or `None` if the historian does not store it.
    pub fn value_kind(self) -> Option<ValueKind> {
        use DataType::*;
        match self {
            Float | Double => Some(ValueKind::Analog),
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | DateTime => {
                Some(ValueKind::Analog)
            }
            Boolean => Some(ValueKind::Discrete),
            _ => None,
        }
    }

    /// Convert a metric value to `f64` according to this datatype.
    ///
    /// Signed 8/16/32-bit integers travel in the `uint32` field; the low bits are sign-extended
    /// from the declared width, which accepts both zero- and sign-extended encoders. 64-bit
    /// integers beyond ±2^53 lose precision (documented limitation).
    pub fn to_f64(self, v: &Value) -> Option<f64> {
        use DataType::*;
        Some(match (self, v) {
            (Int8, Value::IntValue(x)) => (*x as u8 as i8) as f64,
            (Int16, Value::IntValue(x)) => (*x as u16 as i16) as f64,
            (Int32, Value::IntValue(x)) => (*x as i32) as f64,
            (UInt8 | UInt16 | UInt32, Value::IntValue(x)) => *x as f64,
            (Int64, Value::LongValue(x)) => (*x as i64) as f64,
            (UInt64 | DateTime, Value::LongValue(x)) => *x as f64,
            (Float, Value::FloatValue(x)) => *x as f64,
            (Double, Value::DoubleValue(x)) => *x,
            (Boolean, Value::BooleanValue(b)) => f64::from(u8::from(*b)),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_roundtrip() {
        for c in 0..=40u32 {
            assert_eq!(DataType::from_code(c).code(), c);
        }
    }

    #[test]
    fn signed_ints_are_sign_extended_from_declared_width() {
        // -2 as Int8, zero-extended (0xFE) and sign-extended (0xFFFFFFFE) encodings.
        assert_eq!(DataType::Int8.to_f64(&Value::IntValue(0xFE)), Some(-2.0));
        assert_eq!(
            DataType::Int8.to_f64(&Value::IntValue(0xFFFF_FFFE)),
            Some(-2.0)
        );
        assert_eq!(
            DataType::Int16.to_f64(&Value::IntValue(0xFFFF_8000)),
            Some(-32768.0)
        );
        assert_eq!(
            DataType::Int32.to_f64(&Value::IntValue(u32::MAX)),
            Some(-1.0)
        );
        assert_eq!(
            DataType::UInt32.to_f64(&Value::IntValue(u32::MAX)),
            Some(4294967295.0)
        );
        assert_eq!(
            DataType::Int64.to_f64(&Value::LongValue(u64::MAX)),
            Some(-1.0)
        );
        assert_eq!(DataType::UInt64.to_f64(&Value::LongValue(7)), Some(7.0));
    }

    #[test]
    fn floats_bools_and_mismatches() {
        assert_eq!(DataType::Float.to_f64(&Value::FloatValue(1.5)), Some(1.5));
        assert_eq!(
            DataType::Double.to_f64(&Value::DoubleValue(-2.25)),
            Some(-2.25)
        );
        assert_eq!(
            DataType::Boolean.to_f64(&Value::BooleanValue(true)),
            Some(1.0)
        );
        assert_eq!(DataType::Float.to_f64(&Value::DoubleValue(1.0)), None);
        assert_eq!(
            DataType::String.to_f64(&Value::StringValue("x".into())),
            None
        );
        assert_eq!(DataType::Boolean.value_kind(), Some(ValueKind::Discrete));
        assert_eq!(DataType::Template.value_kind(), None);
        assert_eq!(DataType::DateTime.value_kind(), Some(ValueKind::Analog));
    }
}
