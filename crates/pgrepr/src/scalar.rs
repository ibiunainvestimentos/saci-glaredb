use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use chrono_tz::{Tz, TZ_VARIANTS};
use datafusion::arrow::array::{Array, Float16Array};
use datafusion::arrow::datatypes::{DataType as ArrowType, TimeUnit};
use datafusion::scalar::ScalarValue as DfScalar;
use decimal::Decimal128;
use once_cell::sync::Lazy;
use tokio_postgres::types::Type as PgType;

use crate::error::{PgReprError, Result};
use crate::format::Format;
use crate::reader::TextReader;
use crate::writer::{BinaryWriter, TextWriter};

static AVAILABLE_TIMEZONES: Lazy<HashMap<String, Tz>> = Lazy::new(|| {
    TZ_VARIANTS
        .iter()
        .map(|tz| (tz.name().to_owned(), *tz))
        .collect()
});

/// Scalasentation of Postgres value. This can be used as interface
/// between datafusion and postgres scalar values. All the scalar values
/// correspond to a postgres type.
///
/// An important thing to note is that a scalar value, even though corresponds
/// to a postgres type, it doesn't infer the type of in PG. We need extra
/// information to infer the type.
#[derive(Debug, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Tz>),
    Time(NaiveTime),
    Date(NaiveDate),
    Decimal(Decimal128),
    // A datafusion value that isn't yet supported by us. Ultimately we want to
    // remove this and error in case we don't support something explicitly.
    Other(DfScalar),
}

impl Scalar {
    /// Returns the most suitable scalar value for the array value.
    pub fn try_from_array(
        array: &Arc<dyn Array>,
        row_idx: usize,
        as_type: &PgType, // TODO: Type hints
    ) -> Result<Scalar> {
        match DfScalar::try_from_array(array, row_idx) {
            Ok(scalar) => Ok(Self::from_datafusion(scalar, as_type)),
            Err(_) => {
                // This data-type is not supported by arrow. Try to find a suitable
                // conversion if possible, else error!
                match array.data_type() {
                    &ArrowType::Float16 => {
                        // To ScalarValue::Float32
                        let array = array.as_any().downcast_ref::<Float16Array>().unwrap();
                        Ok(match array.is_null(row_idx) {
                            true => Scalar::Null,
                            false => Scalar::Float4(array.value(row_idx).to_f32()),
                        })
                    }
                    _ => Err(PgReprError::UnsupportedArrowType(
                        array.data_type().to_owned(),
                    )),
                }
            }
        }
    }

    /// Returns true if the underlaying value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, &Self::Null)
    }

    pub fn encode_with_format(&self, format: Format, buf: &mut BytesMut) -> Result<()> {
        match format {
            Format::Text => self.encode::<TextWriter>(buf),
            Format::Binary => self.encode::<BinaryWriter>(buf),
        }
    }

    /// Encodes the scalar using the specified writer.
    pub fn encode<W>(&self, buf: &mut BytesMut) -> Result<()>
    where
        W: crate::writer::Writer,
    {
        match self {
            Self::Null => Ok(()),
            Self::Bool(v) => W::write_bool(buf, *v),
            Self::Int2(v) => W::write_int2(buf, *v),
            Self::Int4(v) => W::write_int4(buf, *v),
            Self::Int8(v) => W::write_int8(buf, *v),
            Self::Float4(v) => W::write_float4(buf, *v),
            Self::Float8(v) => W::write_float8(buf, *v),
            Self::Text(v) => W::write_text(buf, v),
            Self::Bytea(v) => W::write_bytea(buf, v),
            Self::Timestamp(v) => W::write_timestamp(buf, v),
            Self::TimestampTz(v) => W::write_timestamptz(buf, v),
            Self::Time(v) => W::write_time(buf, v),
            Self::Date(v) => W::write_date(buf, v),
            Self::Decimal(v) => W::write_decimal(buf, v),
            // If a type is not supported, we try to encode it as text.
            Self::Other(other) => W::write_any(buf, other),
        }
    }

    pub fn decode_with_format(format: Format, buf: &[u8], as_type: &PgType) -> Result<Self> {
        match format {
            Format::Text => Self::decode::<TextReader>(buf, as_type),
            Format::Binary => Err(PgReprError::UnsupportedPgTypeForDecode(as_type.to_owned())),
        }
    }

    pub fn decode<R>(buf: &[u8], as_type: &PgType) -> Result<Self>
    where
        R: crate::reader::Reader,
    {
        let scalar = match *as_type {
            PgType::BOOL => Self::Bool(R::read_bool(buf)?),
            PgType::INT2 => Self::Int2(R::read_int2(buf)?),
            PgType::INT4 => Self::Int4(R::read_int4(buf)?),
            PgType::INT8 => Self::Int8(R::read_int8(buf)?),
            PgType::FLOAT4 => Self::Float4(R::read_float4(buf)?),
            PgType::FLOAT8 => Self::Float8(R::read_float8(buf)?),
            PgType::TEXT => Self::Text(R::read_text(buf)?),
            _ => return Err(PgReprError::UnsupportedPgTypeForDecode(as_type.clone())),
        };
        Ok(scalar)
    }

    pub fn from_datafusion(
        value: DfScalar,
        _as_type: &PgType, // TODO: type hints
    ) -> Self {
        if value.is_null() {
            return Self::Null;
        }

        match value {
            DfScalar::Boolean(Some(v)) => Self::Bool(v),
            DfScalar::Int8(Some(v)) => Self::Int2(v as i16),
            DfScalar::Int16(Some(v)) => Self::Int2(v),
            DfScalar::Int32(Some(v)) => Self::Int4(v),
            DfScalar::Int64(Some(v)) => Self::Int8(v),
            DfScalar::Float32(Some(v)) => Self::Float4(v),
            DfScalar::Float64(Some(v)) => Self::Float8(v),
            DfScalar::Utf8(Some(v)) => Self::Text(v),
            DfScalar::Binary(Some(v)) => Self::Bytea(v),
            DfScalar::TimestampSecond(Some(v), None) => {
                Self::Timestamp(
                    DateTime::from_timestamp(v, /* nsecs = */ 0)
                        .unwrap()
                        .naive_utc(),
                )
            }
            DfScalar::TimestampMillisecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_millis(v).unwrap().naive_utc())
            }
            DfScalar::TimestampMicrosecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_micros(v).unwrap().naive_utc())
            }
            DfScalar::TimestampNanosecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_nanos(v).naive_utc())
            }
            DfScalar::TimestampSecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_opt(v, /* nsecs = */ 0).unwrap())
            }
            DfScalar::TimestampMillisecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_millis_opt(v).unwrap())
            }
            DfScalar::TimestampMicrosecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_micros(v).unwrap())
            }
            DfScalar::TimestampNanosecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_nanos(v))
            }
            DfScalar::Time32Second(Some(v)) => Self::Time(
                DateTime::from_timestamp(v as i64, /* nsecs = */ 0)
                    .unwrap()
                    .time(),
            ),
            DfScalar::Time32Millisecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_millis(v as i64).unwrap().time())
            }
            DfScalar::Time64Microsecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_micros(v).unwrap().time())
            }
            DfScalar::Time64Nanosecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_nanos(v).time())
            }
            DfScalar::Date32(Some(v)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let naive_date = epoch
                    .checked_add_signed(Duration::try_days(v as i64).unwrap())
                    .expect("scalar value should be a valid date");
                Self::Date(naive_date)
            }
            DfScalar::Decimal128(Some(v), _precision, scale) => {
                let decimal =
                    Decimal128::new(v, scale).expect("value should be a valid decimal128");
                Self::Decimal(decimal)
            }

            // Nested types (Struct, List, LargeList, FixedSizeList) are serialised
            // as JSON text instead of falling through to DataFusion's struct-display
            // format, which is not parseable by any standard JSON consumer. The
            // pgwire type announcement (`crate::types::arrow_to_pg_type`) already
            // returns `PgType::TEXT` for these, so the wire-protocol contract is
            // unchanged — we just make the text content valid JSON.
            v @ (DfScalar::Struct(_)
                | DfScalar::List(_)
                | DfScalar::LargeList(_)
                | DfScalar::FixedSizeList(_)) => {
                Self::Text(dfscalar_to_json(&v).to_string())
            }

            other => {
                debug_assert!(!other.is_null());
                Scalar::Other(other)
            }
        }
    }

    pub fn into_datafusion(self, as_type: &ArrowType) -> Result<DfScalar> {
        let scalar = match (self, as_type) {
            (Self::Null, ty) => ty
                .try_into()
                .map_err(|_| PgReprError::UnsupportedArrowType(ty.clone()))?,
            (Self::Bool(v), ArrowType::Boolean) => DfScalar::Boolean(Some(v)),
            (Self::Int2(v), ArrowType::Int8) => DfScalar::Int8(Some(v as i8)),
            (Self::Int2(v), ArrowType::Int16) => DfScalar::Int16(Some(v)),
            (Self::Int4(v), ArrowType::Int32) => DfScalar::Int32(Some(v)),
            (Self::Int8(v), ArrowType::Int64) => DfScalar::Int64(Some(v)),
            // TODO: f16
            (Self::Float4(v), ArrowType::Float32) => DfScalar::Float32(Some(v)),
            (Self::Float8(v), ArrowType::Float64) => DfScalar::Float64(Some(v)),
            (Self::Text(v), ArrowType::Utf8) => DfScalar::Utf8(Some(v)),
            (Self::Bytea(v), ArrowType::Binary) => DfScalar::Binary(Some(v)),
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Second, None)) => {
                DfScalar::TimestampSecond(Some(v.second() as i64), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Millisecond, None)) => {
                DfScalar::TimestampMillisecond(Some(v.and_utc().timestamp_millis()), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Microsecond, None)) => {
                DfScalar::TimestampMicrosecond(Some(v.and_utc().timestamp_micros()), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Nanosecond, None)) => {
                DfScalar::TimestampNanosecond(v.and_utc().timestamp_nanos_opt(), None)
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Second, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampSecond(Some(v.timestamp()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Millisecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampMillisecond(Some(v.timestamp_millis()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Microsecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampMicrosecond(Some(v.timestamp_micros()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                let nanos = v.timestamp_nanos_opt().unwrap();
                DfScalar::TimestampNanosecond(Some(nanos), Some(tz.clone()))
            }
            (Self::Time(v), ArrowType::Time32(TimeUnit::Second)) => {
                DfScalar::Time32Second(Some(v.num_seconds_from_midnight() as i32))
            }
            (Self::Time(v), ArrowType::Time32(TimeUnit::Millisecond)) => {
                let secs = v.num_seconds_from_midnight() as i32;
                let sub_millis = (v.nanosecond() / 1_000_000) as i32;
                let millis = (secs * 1_000) + sub_millis;
                DfScalar::Time32Millisecond(Some(millis))
            }
            (Self::Time(v), ArrowType::Time64(TimeUnit::Microsecond)) => {
                let secs = v.num_seconds_from_midnight() as i64;
                let sub_micros = (v.nanosecond() / 1_000) as i64;
                let micros = (secs * 1_000_000) + sub_micros;
                DfScalar::Time64Microsecond(Some(micros))
            }
            (Self::Time(v), ArrowType::Time64(TimeUnit::Nanosecond)) => {
                let secs = v.num_seconds_from_midnight() as i64;
                let sub_nanos = (v.nanosecond()) as i64;
                let nanos = (secs * 1_000_000_000) + sub_nanos;
                DfScalar::Time64Nanosecond(Some(nanos))
            }
            (Self::Date(v), ArrowType::Date32) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days_since_epoch = v.signed_duration_since(epoch).num_days();
                DfScalar::Date32(Some(days_since_epoch as i32))
            }
            (Self::Decimal(v), arrow_type @ ArrowType::Decimal128(precision, scale)) => {
                if v.scale() != *scale {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::Decimal128(Some(v.mantissa()), *precision, *scale)
            }
            (scalar, arrow_type) => {
                return Err(PgReprError::InternalError(format!(
                    "cannot convert from scalar {:?} to arrow type {:?}",
                    scalar, arrow_type
                )))
            }
        };
        Ok(scalar)
    }
}

// TODO: Figure out if this should be parsing time zone names like
// 'Australia/Melbourne' or offsets like '+03:00'.
fn get_timezone(tz: &str) -> Tz {
    *AVAILABLE_TIMEZONES.get(tz).unwrap_or(&chrono_tz::UTC)
}

/// Recursively serialise a DataFusion `ScalarValue` into a `serde_json::Value`.
///
/// Used by `Scalar::from_datafusion` to convert nested types (Struct, List,
/// LargeList, FixedSizeList) into JSON text on the pgwire path. Timestamps are
/// emitted in `%Y-%m-%d %H:%M:%S` format (no `T`, no sub-second) to match the
/// first entry in the C# `DatabricksTimestampConverter`'s accepted formats —
/// see providers-datalake `DatabricksTimestampConverter.cs`.
fn dfscalar_to_json(value: &DfScalar) -> serde_json::Value {
    use serde_json::{Map, Value};

    if value.is_null() {
        return Value::Null;
    }

    match value {
        DfScalar::Boolean(Some(b)) => Value::Bool(*b),
        DfScalar::Int8(Some(v)) => Value::Number((*v as i64).into()),
        DfScalar::Int16(Some(v)) => Value::Number((*v as i64).into()),
        DfScalar::Int32(Some(v)) => Value::Number((*v as i64).into()),
        DfScalar::Int64(Some(v)) => Value::Number((*v).into()),
        DfScalar::UInt8(Some(v)) => Value::Number((*v as u64).into()),
        DfScalar::UInt16(Some(v)) => Value::Number((*v as u64).into()),
        DfScalar::UInt32(Some(v)) => Value::Number((*v as u64).into()),
        DfScalar::UInt64(Some(v)) => Value::Number((*v).into()),
        DfScalar::Float32(Some(v)) => f64_or_null(*v as f64),
        DfScalar::Float64(Some(v)) => f64_or_null(*v),

        // Decimal -> JSON Number via f64. The downstream C# JsonTypeHandler's
        // JsonSerializerOptions does NOT enable JsonNumberHandling.AllowReadingFromString,
        // so a string-form decimal would fail to deserialize into `decimal?`. The
        // saciflow gold registry stores all of these fields as float64, so going
        // through f64 here loses no precision relative to the upstream source.
        DfScalar::Decimal128(Some(mantissa), _precision, scale) => {
            let v = (*mantissa as f64) / 10f64.powi(*scale as i32);
            f64_or_null(v)
        }
        DfScalar::Decimal256(Some(_), _, _) => {
            tracing::warn!("pgrepr: Decimal256 in pgwire response — emitting null");
            Value::Null
        }

        DfScalar::Utf8(Some(s)) | DfScalar::LargeUtf8(Some(s)) => Value::String(s.clone()),

        // security_master never carries Binary; emit a warn-and-null so any
        // future use surfaces loudly. Adding base64 just for this path would
        // pull a new dep we don't need.
        DfScalar::Binary(Some(_))
        | DfScalar::LargeBinary(Some(_))
        | DfScalar::FixedSizeBinary(_, Some(_)) => {
            tracing::warn!("pgrepr: binary value in nested JSON — emitting null");
            Value::Null
        }

        DfScalar::Date32(Some(d)) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch
                .checked_add_signed(Duration::try_days(*d as i64).unwrap())
                .expect("scalar value should be a valid date");
            Value::String(date.format("%Y-%m-%d").to_string())
        }
        DfScalar::Date64(Some(ms)) => {
            let dt = DateTime::from_timestamp_millis(*ms).unwrap().naive_utc();
            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }

        // Timestamp* — emit `%Y-%m-%d %H:%M:%S` (no T, no millis). First entry of
        // DatabricksTimestampConverter.DateTimeFormats[]. Mirror the unit→NaiveDateTime
        // conversion already used by `Scalar::from_datafusion` for top-level timestamps.
        DfScalar::TimestampSecond(Some(v), _) => {
            let dt = DateTime::from_timestamp(*v, 0).unwrap().naive_utc();
            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DfScalar::TimestampMillisecond(Some(v), _) => {
            let dt = DateTime::from_timestamp_millis(*v).unwrap().naive_utc();
            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DfScalar::TimestampMicrosecond(Some(v), _) => {
            let dt = DateTime::from_timestamp_micros(*v).unwrap().naive_utc();
            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DfScalar::TimestampNanosecond(Some(v), _) => {
            let dt = DateTime::from_timestamp_nanos(*v).naive_utc();
            Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }

        // Time* — formatted as %H:%M:%S. Not used by security_master but
        // included for completeness.
        DfScalar::Time32Second(Some(v)) => {
            let t = DateTime::from_timestamp(*v as i64, 0).unwrap().time();
            Value::String(t.format("%H:%M:%S").to_string())
        }
        DfScalar::Time32Millisecond(Some(v)) => {
            let t = DateTime::from_timestamp_millis(*v as i64).unwrap().time();
            Value::String(t.format("%H:%M:%S").to_string())
        }
        DfScalar::Time64Microsecond(Some(v)) => {
            let t = DateTime::from_timestamp_micros(*v).unwrap().time();
            Value::String(t.format("%H:%M:%S").to_string())
        }
        DfScalar::Time64Nanosecond(Some(v)) => {
            let t = DateTime::from_timestamp_nanos(*v).time();
            Value::String(t.format("%H:%M:%S").to_string())
        }

        // List variants: DF36 wraps a 1-row List/LargeList/FixedSizeList array.
        // The actual element-array lives at index 0; iterate that.
        DfScalar::List(arr) => list_to_json(arr.value(0).as_ref()),
        DfScalar::LargeList(arr) => list_to_json(arr.value(0).as_ref()),
        DfScalar::FixedSizeList(arr) => list_to_json(arr.value(0).as_ref()),

        // Struct: 1-row StructArray with named fields.
        DfScalar::Struct(arr) => {
            let mut map = Map::with_capacity(arr.num_columns());
            for (i, field) in arr.fields().iter().enumerate() {
                let col = arr.column(i);
                let inner = DfScalar::try_from_array(col.as_ref(), 0).unwrap_or_else(|e| {
                    tracing::warn!(
                        "pgrepr: try_from_array failed for struct field {} ({:?}) — emitting null",
                        field.name(),
                        e
                    );
                    DfScalar::Null
                });
                map.insert(field.name().clone(), dfscalar_to_json(&inner));
            }
            Value::Object(map)
        }

        // Map / Dictionary / Union / RunEndEncoded / Utf8View / BinaryView /
        // intervals / durations etc. The saciflow gold registry doesn't use
        // them. Emit null + warn so any future use surfaces loudly instead of
        // silently producing un-deserialisable inner JSON.
        other => {
            tracing::warn!(
                "pgrepr: unsupported nested ScalarValue variant ({:?}) — emitting null",
                other.data_type()
            );
            Value::Null
        }
    }
}

fn list_to_json(inner: &dyn Array) -> serde_json::Value {
    let mut out = Vec::with_capacity(inner.len());
    for i in 0..inner.len() {
        let elem = DfScalar::try_from_array(inner, i).unwrap_or_else(|e| {
            tracing::warn!(
                "pgrepr: try_from_array failed for list element {} ({:?}) — emitting null",
                i,
                e
            );
            DfScalar::Null
        });
        out.push(dfscalar_to_json(&elem));
    }
    serde_json::Value::Array(out)
}

fn f64_or_null(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Float64Array,
        Int32Array,
        ListArray,
        StringArray,
        StructArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_get_timezone() {
        let tz = get_timezone("+00:00");
        assert_eq!(chrono_tz::UTC, tz);
    }

    // ---- dfscalar_to_json -------------------------------------------------

    #[test]
    fn json_primitive_scalars() {
        assert_eq!(
            dfscalar_to_json(&DfScalar::Boolean(Some(true))),
            serde_json::json!(true)
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Int32(Some(42))),
            serde_json::json!(42)
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Float64(Some(3.14))),
            serde_json::json!(3.14)
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Utf8(Some("hello".to_string()))),
            serde_json::json!("hello")
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Boolean(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn json_nan_and_infinity_become_null() {
        assert_eq!(
            dfscalar_to_json(&DfScalar::Float64(Some(f64::NAN))),
            serde_json::Value::Null
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Float64(Some(f64::INFINITY))),
            serde_json::Value::Null
        );
    }

    #[test]
    fn json_decimal128_via_f64() {
        // 12345 with scale 2 = 123.45
        let v = dfscalar_to_json(&DfScalar::Decimal128(Some(12345), 5, 2));
        assert_eq!(v, serde_json::json!(123.45));
    }

    #[test]
    fn json_date_and_timestamp_format() {
        // Date32 = days since 1970-01-01. 2024-05-01 is day 19844.
        let v = dfscalar_to_json(&DfScalar::Date32(Some(19844)));
        assert_eq!(v, serde_json::json!("2024-05-01"));

        // Timestamp microseconds. 2024-05-01 12:34:56 UTC.
        let micros = 1_714_566_896_000_000_i64;
        let v = dfscalar_to_json(&DfScalar::TimestampMicrosecond(Some(micros), None));
        assert_eq!(v, serde_json::json!("2024-05-01 12:34:56"));
    }

    #[test]
    fn json_string_escapes_quotes_and_backslashes() {
        let v = dfscalar_to_json(&DfScalar::Utf8(Some("a\"b\\c\n".to_string())));
        assert_eq!(v.to_string(), r#""a\"b\\c\n""#);
    }

    #[test]
    fn json_struct_emits_object_with_field_order_preserved() {
        // Build a 1-row StructArray with fields in declaration order: type, strike, ccy.
        let type_arr = Arc::new(StringArray::from(vec![Some("Call")])) as _;
        let strike_arr = Arc::new(Float64Array::from(vec![Some(91.0)])) as _;
        let ccy_arr = Arc::new(StringArray::from(vec![None::<&str>])) as _;
        let fields = vec![
            (
                Arc::new(Field::new("type", DataType::Utf8, true)),
                Arc::clone(&type_arr) as _,
            ),
            (
                Arc::new(Field::new("strike", DataType::Float64, true)),
                Arc::clone(&strike_arr),
            ),
            (
                Arc::new(Field::new("call_currency", DataType::Utf8, true)),
                Arc::clone(&ccy_arr),
            ),
        ];
        let s = StructArray::from(fields);
        let json = dfscalar_to_json(&DfScalar::Struct(Arc::new(s))).to_string();
        // serde_json with `preserve_order` keeps insertion order, which matches
        // struct-field declaration order. Null currency emits as JSON null.
        assert_eq!(json, r#"{"type":"Call","strike":91.0,"call_currency":null}"#);
    }

    #[test]
    fn json_list_of_int_emits_json_array() {
        // 1-row ListArray containing [1, 2, 3] at index 0.
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let list = ListArray::new(field, offsets, values, None);
        let json = dfscalar_to_json(&DfScalar::List(Arc::new(list))).to_string();
        assert_eq!(json, "[1,2,3]");
    }

    #[test]
    fn json_list_of_timestamp_uses_yyyy_mm_dd_hh_mm_ss() {
        // Two timestamps in microseconds: 2024-05-01T00:00:00 and 2024-06-01T00:00:00 UTC.
        let micros: Vec<i64> = vec![1_714_521_600_000_000, 1_717_200_000_000_000];
        let values = Arc::new(TimestampMicrosecondArray::from(micros));
        let offsets = OffsetBuffer::new(vec![0, 2].into());
        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let list = ListArray::new(field, offsets, values, None);
        let json = dfscalar_to_json(&DfScalar::List(Arc::new(list))).to_string();
        assert_eq!(json, r#"["2024-05-01 00:00:00","2024-06-01 00:00:00"]"#);
    }

    #[test]
    fn json_list_of_struct_emits_array_of_objects() {
        // Build a list of two amortization-schedule-like structs.
        let dates = Arc::new(TimestampMicrosecondArray::from(vec![
            1_714_521_600_000_000_i64,
            1_717_200_000_000_000_i64,
        ])) as _;
        let pcts = Arc::new(Float64Array::from(vec![0.5, 1.0])) as _;
        let fields = vec![
            (
                Arc::new(Field::new(
                    "schedule_date",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::clone(&dates) as _,
            ),
            (
                Arc::new(Field::new("amort_pct", DataType::Float64, true)),
                Arc::clone(&pcts),
            ),
        ];
        let inner = StructArray::from(fields);
        let inner_field = Arc::new(Field::new(
            "item",
            inner.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0, 2].into());
        let list = ListArray::new(inner_field, offsets, Arc::new(inner), None);
        let json = dfscalar_to_json(&DfScalar::List(Arc::new(list))).to_string();
        assert_eq!(
            json,
            r#"[{"schedule_date":"2024-05-01 00:00:00","amort_pct":0.5},{"schedule_date":"2024-06-01 00:00:00","amort_pct":1.0}]"#
        );
    }

    #[test]
    fn json_struct_with_null_row_emits_json_null() {
        // A 1-row StructArray whose row is null at the row level.
        use datafusion::arrow::buffer::NullBuffer;
        let type_arr = Arc::new(StringArray::from(vec![None::<&str>])) as _;
        let strike_arr = Arc::new(Float64Array::from(vec![None::<f64>])) as _;
        let fields_vec: Vec<(Arc<Field>, _)> = vec![
            (
                Arc::new(Field::new("type", DataType::Utf8, true)),
                Arc::clone(&type_arr) as _,
            ),
            (
                Arc::new(Field::new("strike", DataType::Float64, true)),
                Arc::clone(&strike_arr),
            ),
        ];
        let nulls = NullBuffer::from(vec![false]); // row 0 is null
        let s = StructArray::new(
            fields_vec
                .iter()
                .map(|(f, _)| Arc::clone(f))
                .collect::<Vec<_>>()
                .into(),
            fields_vec.iter().map(|(_, a)| Arc::clone(a)).collect(),
            Some(nulls),
        );
        assert_eq!(
            dfscalar_to_json(&DfScalar::Struct(Arc::new(s))),
            serde_json::Value::Null
        );
    }

    #[test]
    fn json_list_with_null_elements() {
        // [1, null, 3]
        let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let list = ListArray::new(field, offsets, values, None);
        assert_eq!(
            dfscalar_to_json(&DfScalar::List(Arc::new(list))).to_string(),
            "[1,null,3]"
        );
    }

    #[test]
    fn json_decimal128_negative_and_zero_scale() {
        assert_eq!(
            dfscalar_to_json(&DfScalar::Decimal128(Some(-12345), 5, 2)),
            serde_json::json!(-123.45)
        );
        // scale=0 keeps the integer value as a JSON number; ryu may emit "123.0"
        // (which decimal? still parses fine on the C# side).
        assert_eq!(
            dfscalar_to_json(&DfScalar::Decimal128(Some(123), 3, 0)),
            serde_json::json!(123.0)
        );
    }

    // ---- end-to-end: from_datafusion routes nested types via Self::Text ---

    #[test]
    fn from_datafusion_struct_returns_text_json() {
        let type_arr = Arc::new(StringArray::from(vec![Some("Put")])) as _;
        let strike_arr = Arc::new(Float64Array::from(vec![Some(50.0)])) as _;
        let fields = vec![
            (
                Arc::new(Field::new("type", DataType::Utf8, true)),
                Arc::clone(&type_arr) as _,
            ),
            (
                Arc::new(Field::new("strike", DataType::Float64, true)),
                Arc::clone(&strike_arr),
            ),
        ];
        let s = StructArray::from(fields);
        let scalar = Scalar::from_datafusion(DfScalar::Struct(Arc::new(s)), &PgType::TEXT);
        match scalar {
            Scalar::Text(s) => assert_eq!(s, r#"{"type":"Put","strike":50.0}"#),
            other => panic!("expected Scalar::Text, got {other:?}"),
        }
    }
}
