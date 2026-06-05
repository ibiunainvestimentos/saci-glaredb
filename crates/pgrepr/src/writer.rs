use std::fmt::Display;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use decimal::Decimal128;
use repr::str::encode::{
    encode_binary,
    encode_bool,
    encode_date,
    encode_decimal,
    encode_float,
    encode_int,
    encode_string,
    encode_time,
    encode_utc_timestamp,
};
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};

use crate::error::{PgReprError, Result};

/// Writer defines the interface for the different kinds of values that can be
/// encoded as a postgres type.
pub trait Writer {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()>;

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()>;
    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()>;
    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()>;

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()>;
    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()>;

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()>;
    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()>;

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()>;
    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()>;
    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()>;
    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()>;

    fn write_decimal(buf: &mut BytesMut, v: &Decimal128) -> Result<()>;

    /// Write a PostgreSQL `interval` value. Arrow's three interval encodings
    /// and `duration` are all normalised to the PG triple
    /// `(months, days, microseconds)` by the caller.
    fn write_interval(buf: &mut BytesMut, months: i32, days: i32, micros: i64) -> Result<()>;

    fn write_any<T: Display>(buf: &mut BytesMut, v: &T) -> Result<()> {
        encode_string(buf, v)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct TextWriter;

impl Writer for TextWriter {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()> {
        encode_bool(buf, v)?;
        Ok(())
    }

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()> {
        encode_float(buf, v)?;
        Ok(())
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        encode_float(buf, v)?;
        Ok(())
    }

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()> {
        encode_string(buf, v)?;
        Ok(())
    }

    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()> {
        encode_binary(buf, v)?;
        Ok(())
    }

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()> {
        encode_utc_timestamp(buf, v, false)?;
        Ok(())
    }

    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()> {
        encode_utc_timestamp(buf, v, true)?;
        Ok(())
    }

    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()> {
        encode_time(buf, v, false)?;
        Ok(())
    }

    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()> {
        encode_date(buf, v)?;
        Ok(())
    }

    fn write_decimal(buf: &mut BytesMut, v: &Decimal128) -> Result<()> {
        encode_decimal(buf, v)?;
        Ok(())
    }

    fn write_interval(buf: &mut BytesMut, months: i32, days: i32, micros: i64) -> Result<()> {
        encode_string(buf, &format_interval_text(months, days, micros))?;
        Ok(())
    }
}

/// Render an interval triple in PostgreSQL's default (`postgres`) interval
/// style, e.g. `1 year 2 mons 3 days 04:05:06.5`. A zero interval renders
/// `00:00:00`. Only used by the text writer — binary clients (asyncpg, JDBC)
/// take the 16-byte path in [`BinaryWriter::write_interval`].
fn format_interval_text(months: i32, days: i32, micros: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let years = months / 12;
    let mons = months % 12;
    let plural = |n: i32| if n.abs() == 1 { "" } else { "s" };
    if years != 0 {
        parts.push(format!("{years} year{}", plural(years)));
    }
    if mons != 0 {
        parts.push(format!("{mons} mon{}", plural(mons)));
    }
    if days != 0 {
        parts.push(format!("{days} day{}", plural(days)));
    }

    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 { "-" } else { "" };
        let abs = micros.unsigned_abs();
        let usecs = abs % 1_000_000;
        let total_secs = abs / 1_000_000;
        let secs = total_secs % 60;
        let mins = (total_secs / 60) % 60;
        let hours = total_secs / 3600;
        let mut time = format!("{sign}{hours:02}:{mins:02}:{secs:02}");
        if usecs != 0 {
            // Trim trailing zeros in the fractional seconds, like PG.
            let frac = format!("{usecs:06}");
            time.push('.');
            time.push_str(frac.trim_end_matches('0'));
        }
        parts.push(time);
    }

    parts.join(" ")
}

#[derive(Debug)]
pub struct BinaryWriter;

macro_rules! put_to_sql {
    ($buf:ident, $pgtype:ident, $v:ident) => {
        match $v.to_sql(&PgType::$pgtype, $buf).map_err(|e| {
            PgReprError::InternalError(format!(
                "cannot encode value={:?} as {}: {e}",
                $v,
                &PgType::$pgtype,
            ))
        })? {
            IsNull::Yes => unreachable!("nulls should not be encoded here"),
            _ => Ok(()),
        }
    };
}

impl Writer for BinaryWriter {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()> {
        put_to_sql!(buf, BOOL, v)
    }

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()> {
        put_to_sql!(buf, INT2, v)
    }

    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()> {
        put_to_sql!(buf, INT4, v)
    }

    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()> {
        put_to_sql!(buf, INT8, v)
    }

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()> {
        put_to_sql!(buf, FLOAT4, v)
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        put_to_sql!(buf, FLOAT8, v)
    }

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()> {
        put_to_sql!(buf, TEXT, v)
    }

    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()> {
        put_to_sql!(buf, BYTEA, v)
    }

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()> {
        put_to_sql!(buf, TIMESTAMP, v)
    }

    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()> {
        let utc_date_time = DateTime::<Utc>::from_naive_utc_and_offset(v.naive_utc(), Utc);
        put_to_sql!(buf, TIMESTAMPTZ, utc_date_time)
    }

    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()> {
        put_to_sql!(buf, TIME, v)
    }

    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()> {
        put_to_sql!(buf, DATE, v)
    }

    /// Encode a `Decimal128` into the PostgreSQL `numeric` binary wire format.
    ///
    /// Layout (all big-endian; the `i32` length prefix is written by the
    /// caller in `pgsrv::codec::server`):
    /// ```text
    /// i16 ndigits   number of base-10000 digit groups that follow
    /// i16 weight    base-10000 exponent of the most-significant group (0 = units)
    /// i16 sign      0x0000 positive, 0x4000 negative
    /// i16 dscale    display scale (fractional decimal digits)
    /// i16 * ndigits each group 0..=9999, most-significant first
    /// ```
    ///
    /// `Decimal128` carries `value = mantissa / 10^scale`. We pad the
    /// fractional part up to a multiple of 4 decimal digits so the decimal
    /// point lands on a base-10000 group boundary, split into groups, and
    /// trim trailing zero groups (PG does the same).
    fn write_decimal(buf: &mut BytesMut, v: &Decimal128) -> Result<()> {
        const NBASE: u128 = 10_000;

        let mantissa = v.mantissa();
        let scale = v.scale();

        let sign: i16 = if mantissa < 0 { 0x4000 } else { 0x0000 };
        // dscale (display scale) is the number of fractional decimal digits.
        // Negative Arrow scales mean the value is scaled *up*, so dscale = 0.
        let dscale: i16 = scale.max(0) as i16;

        let mut unscaled: u128 = mantissa.unsigned_abs();

        // Align the value onto base-10000 group boundaries.
        let (frac_groups, mul_pow): (u32, u32) = if scale >= 0 {
            let frac = scale as u32;
            let pad = (4 - (frac % 4)) % 4;
            ((frac + pad) / 4, pad)
        } else {
            // value = unscaled * 10^(-scale); no fractional groups.
            (0, (-scale) as u32)
        };
        if mul_pow > 0 {
            let factor = 10u128
                .checked_pow(mul_pow)
                .and_then(|f| unscaled.checked_mul(f));
            unscaled = factor.ok_or_else(|| {
                PgReprError::InternalError(format!(
                    "decimal too large to encode as PG numeric: mantissa={mantissa}, scale={scale}"
                ))
            })?;
        }

        // Split into base-10000 groups, least-significant first.
        let mut groups: Vec<i16> = Vec::new();
        if unscaled != 0 {
            while unscaled > 0 {
                groups.push((unscaled % NBASE) as i16);
                unscaled /= NBASE;
            }
        }
        let total = groups.len() as i32;
        // weight is the base-10000 exponent of the most-significant group.
        // Zero stays at weight 0.
        let weight: i16 = if total == 0 {
            0
        } else {
            (total - frac_groups as i32 - 1) as i16
        };

        // Wire order is most-significant first; then drop trailing zero groups.
        groups.reverse();
        while matches!(groups.last(), Some(0)) {
            groups.pop();
        }

        buf.put_i16(groups.len() as i16);
        buf.put_i16(weight);
        buf.put_i16(sign);
        buf.put_i16(dscale);
        for g in groups {
            buf.put_i16(g);
        }
        Ok(())
    }

    /// Encode an interval into PG's 16-byte binary `interval` representation:
    /// `i64` microseconds, then `i32` days, then `i32` months (all BE).
    fn write_interval(buf: &mut BytesMut, months: i32, days: i32, micros: i64) -> Result<()> {
        buf.put_i64(micros);
        buf.put_i32(days);
        buf.put_i32(months);
        Ok(())
    }

    /// Override the trait default for binary format only. The default
    /// implementation calls `encode_string`, which silently emits ASCII
    /// digits — wrong for binary clients (e.g. asyncpg announcing INT4
    /// for an `oid` column receives `b"16401"` instead of 4 BE bytes
    /// and fails with "unexpected trailing N bytes").
    ///
    /// Fail closed: if a `Scalar` variant reaches this path under the
    /// binary writer, the missing match arm in `Scalar::from_datafusion`
    /// is the bug. Caller will surface the error and the encoder gap
    /// can be fixed deterministically.
    fn write_any<T: Display>(_buf: &mut BytesMut, v: &T) -> Result<()> {
        Err(PgReprError::InternalError(format!(
            "binary writer: no encoder for value `{v}` — \
             add an explicit arm in `Scalar::from_datafusion`"
        )))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono_tz::UTC;

    use super::*;

    fn assert_buf(buf: &BytesMut, val: &[u8]) {
        let slice = buf.as_ref();
        assert_eq!(
            val,
            &slice[0..buf.len()],
            "\nExpected: {}\nGot: {}",
            &String::from_utf8_lossy(val),
            &String::from_utf8_lossy(slice)
        );
        assert_eq!(buf.len(), val.len());
    }

    #[test]
    fn test_text_writer() {
        type Writer = TextWriter;

        let mut buf = BytesMut::new();
        let buf = &mut buf;

        buf.clear();
        Writer::write_bool(buf, true).unwrap();
        assert_buf(buf, b"t");

        buf.clear();
        Writer::write_bool(buf, false).unwrap();
        assert_buf(buf, b"f");

        buf.clear();
        Writer::write_int2(buf, 1234).unwrap();
        assert_buf(buf, b"1234");

        buf.clear();
        Writer::write_int2(buf, -1234).unwrap();
        assert_buf(buf, b"-1234");

        buf.clear();
        Writer::write_int4(buf, 654321).unwrap();
        assert_buf(buf, b"654321");

        buf.clear();
        Writer::write_int4(buf, -654321).unwrap();
        assert_buf(buf, b"-654321");

        buf.clear();
        Writer::write_int8(buf, 1234567890).unwrap();
        assert_buf(buf, b"1234567890");

        buf.clear();
        Writer::write_int8(buf, -1234567890).unwrap();
        assert_buf(buf, b"-1234567890");

        buf.clear();
        Writer::write_float4(buf, 123.456).unwrap();
        assert_buf(buf, b"123.456");

        buf.clear();
        Writer::write_float4(buf, -123.456).unwrap();
        assert_buf(buf, b"-123.456");

        buf.clear();
        Writer::write_float4(buf, 0.000000001).unwrap();
        assert_buf(buf, b"1e-9");

        buf.clear();
        Writer::write_float4(buf, 1234000000000000000000.0).unwrap();
        assert_buf(buf, b"1.234e+21");

        buf.clear();
        Writer::write_float4(buf, f32::NAN).unwrap();
        assert_buf(buf, b"NaN");

        buf.clear();
        Writer::write_float4(buf, f32::INFINITY).unwrap();
        assert_buf(buf, b"Infinity");

        buf.clear();
        Writer::write_float4(buf, f32::NEG_INFINITY).unwrap();
        assert_buf(buf, b"-Infinity");

        buf.clear();
        Writer::write_float4(buf, -0.0).unwrap();
        assert_buf(buf, b"-0");

        buf.clear();
        Writer::write_float4(buf, 0.0).unwrap();
        assert_buf(buf, b"0");

        buf.clear();
        Writer::write_float8(buf, 123.0456789).unwrap();
        assert_buf(buf, b"123.0456789");

        buf.clear();
        Writer::write_float8(buf, 0.000000001).unwrap();
        assert_buf(buf, b"1e-9");

        buf.clear();
        Writer::write_float8(buf, 1234000000000000000000.0).unwrap();
        assert_buf(buf, b"1.234e+21");

        buf.clear();
        Writer::write_float8(buf, -123.0456789).unwrap();
        assert_buf(buf, b"-123.0456789");

        buf.clear();
        Writer::write_float8(buf, f64::NAN).unwrap();
        assert_buf(buf, b"NaN");

        buf.clear();
        Writer::write_float8(buf, f64::INFINITY).unwrap();
        assert_buf(buf, b"Infinity");

        buf.clear();
        Writer::write_float8(buf, f64::NEG_INFINITY).unwrap();
        assert_buf(buf, b"-Infinity");

        buf.clear();
        Writer::write_float8(buf, -0.0).unwrap();
        assert_buf(buf, b"-0");

        buf.clear();
        Writer::write_float8(buf, 0.0).unwrap();
        assert_buf(buf, b"0");

        buf.clear();
        Writer::write_text(buf, "abcdefghij").unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, &[23, 13, 255, 0, 130]).unwrap();
        assert_buf(buf, b"\\x170dff0082");

        buf.clear();
        let nt = DateTime::from_timestamp(938689324, 0).unwrap().naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = DateTime::from_timestamp(938689324, 123567)
            .unwrap()
            .naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = DateTime::from_timestamp(938689324, 123_400_000)
            .unwrap()
            .naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = DateTime::from_timestamp(-197199051, 0).unwrap().naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = DateTime::from_timestamp(-62143593684, 0)
            .unwrap()
            .naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("1-{} BC", nt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123_400_000).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(-197199051, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(-62143593684, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("1-{}+00 BC", dt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 0).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123567).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04.000124");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123_400_000).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04.1234");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1999-09-30");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1963, 10, 2).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1963-10-02");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(0, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1-09-30 BC");

        buf.clear();
        let decimal = Decimal128::new(3950123456, 6).unwrap();
        Writer::write_decimal(buf, &decimal).unwrap();
        assert_buf(buf, b"3950.123456");
    }

    #[test]
    fn test_binary_writer() {
        type Writer = BinaryWriter;

        let mut buf = BytesMut::new();
        let buf = &mut buf;

        buf.clear();
        Writer::write_bool(buf, true).unwrap();
        assert_buf(buf, &[1]);

        buf.clear();
        Writer::write_bool(buf, false).unwrap();
        assert_buf(buf, &[0]);

        buf.clear();
        Writer::write_int2(buf, 1234).unwrap();
        assert_buf(buf, 1234_i16.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_int4(buf, 654321).unwrap();
        assert_buf(buf, 654321_i32.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_int8(buf, 1234567890).unwrap();
        assert_buf(buf, 1234567890_i64.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_float4(buf, 123.456).unwrap();
        assert_buf(buf, 123.456_f32.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_float8(buf, 123.0456789).unwrap();
        assert_buf(buf, 123.0456789_f64.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_text(buf, "abcdefghij").unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, &[23, 13, 255, 0, 130]).unwrap();
        assert_buf(buf, &[23, 13, 255, 0, 130]);

        buf.clear();
        let nt = DateTime::from_timestamp(938689324, 123567)
            .unwrap()
            .naive_utc();
        Writer::write_timestamp(buf, &nt).unwrap();
        // Microseconds since Jan 1, 2000
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let nt = NaiveTime::from_hms_micro_opt(16, 32, 4, 1234).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        // Microseconds since mid-night
        assert_buf(buf, 59_524_001_234_i64.to_be_bytes().as_ref());

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        // Days since Jan 1, 2000
        assert_buf(buf, (-93_i32).to_be_bytes().as_ref());

        buf.clear();
        let decimal = Decimal128::new(3950123456, 6).unwrap();
        Writer::write_decimal(buf, &decimal).unwrap();
        // 3950.123456 → ndigits=3, weight=0, sign=+, dscale=6, [3950,1234,5600]
        assert_buf(buf, &[0, 3, 0, 0, 0, 0, 0, 6, 15, 110, 4, 210, 21, 224]);
    }

    #[test]
    fn test_binary_writer_decimal_edges() {
        type Writer = BinaryWriter;
        let mut buf = BytesMut::new();
        let buf = &mut buf;

        // Zero: ndigits=0, weight=0, sign=+, dscale=2, no groups.
        buf.clear();
        Writer::write_decimal(buf, &Decimal128::new(0, 2).unwrap()).unwrap();
        assert_buf(buf, &[0, 0, 0, 0, 0, 0, 0, 2]);

        // Plain integer (scale 0): 12345 = 1*10000 + 2345 → [1, 2345], weight=1.
        buf.clear();
        Writer::write_decimal(buf, &Decimal128::new(12345, 0).unwrap()).unwrap();
        assert_buf(buf, &[0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 9, 41]);

        // Negative value: -3950.123456 → same digits, sign=0x4000.
        buf.clear();
        Writer::write_decimal(buf, &Decimal128::new(-3950123456, 6).unwrap()).unwrap();
        assert_buf(buf, &[0, 3, 0, 0, 0x40, 0, 0, 6, 15, 110, 4, 210, 21, 224]);

        // Pure fraction needing a pad: 0.5 (scale 1) → pad 3 → 5000 in the
        // first fractional group. ndigits=1, weight=-1, dscale=1.
        buf.clear();
        Writer::write_decimal(buf, &Decimal128::new(5, 1).unwrap()).unwrap();
        assert_buf(buf, &[0, 1, 255, 255, 0, 0, 0, 1, 19, 136]);

        // Trailing zero group trimmed: 1.0000 (mantissa 10000, scale 4) →
        // unscaled 10000 = [1, 0]; trailing 0 dropped → [1], weight 0, dscale 4.
        buf.clear();
        Writer::write_decimal(buf, &Decimal128::new(10000, 4).unwrap()).unwrap();
        assert_buf(buf, &[0, 1, 0, 0, 0, 0, 0, 4, 0, 1]);
    }

    #[test]
    fn test_binary_writer_interval() {
        type Writer = BinaryWriter;
        let mut buf = BytesMut::new();
        let buf = &mut buf;

        // 1 month, 2 days, 3 seconds → micros=3_000_000, days=2, months=1.
        buf.clear();
        Writer::write_interval(buf, 1, 2, 3_000_000).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(3_000_000_i64.to_be_bytes().as_ref());
        expected.extend_from_slice(2_i32.to_be_bytes().as_ref());
        expected.extend_from_slice(1_i32.to_be_bytes().as_ref());
        assert_buf(buf, &expected);
    }
}
