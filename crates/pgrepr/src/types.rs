use datafusion::arrow::datatypes::DataType as ArrowType;
use tokio_postgres::types::Type as PgType;

use crate::pg_type_oid::arrow_to_pg_oid;

/// Returns a compatible postgres type for the arrow datatype. If the type hint
/// is not-none, it returns the type inside the option.
///
/// The Arrow → OID mapping is centralised in [`crate::pg_type_oid`] so the
/// `pg_catalog.pg_type` view, `pg_attribute.atttypid`, `pg_proc` argument
/// resolution and the wire-level `RowDescription` all agree.
///
/// Notes on a few non-obvious mappings:
///
/// * **Decimal** (`Decimal128`, `Decimal256`) → `NUMERIC` (oid 1700). Encoding
///   on the wire still flows through the existing `Scalar::Decimal` path in
///   `pgrepr::scalar`, which serialises decimals as text.
/// * **Nested types** (`Struct`, `Map`, `List`, `LargeList`, `FixedSizeList`,
///   `Dictionary`, `Union`, `RunEndEncoded`) → either the matching `_X` array
///   OID (when the element has one) or `JSON` (oid 114). The
///   JSON-over-pgwire serialisation in `pgrepr::scalar` already produces valid
///   JSON for these; advertising oid 114 lets DBeaver and other clients
///   format / parse the column correctly instead of treating it as opaque
///   text.
pub fn arrow_to_pg_type(df_type: &ArrowType, type_hint: Option<PgType>) -> PgType {
    if let Some(t) = type_hint {
        return t;
    }
    let oid = arrow_to_pg_oid(df_type);
    // tokio-postgres exposes a `from_oid(u32) -> Option<Type>` for all the
    // types it ships constants for. Anything outside that universe (eg the
    // `reg*` family) falls back to TEXT on the wire — the catalog views still
    // see the correct OID through `pg_type_oid`, only the wire-format path
    // degrades.
    PgType::from_oid(oid).unwrap_or(PgType::TEXT)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, TimeUnit};

    use super::*;

    #[test]
    fn type_hint_wins() {
        let t = arrow_to_pg_type(&ArrowType::Int32, Some(PgType::OID));
        assert_eq!(t, PgType::OID);
    }

    #[test]
    fn primitives_round_trip() {
        assert_eq!(arrow_to_pg_type(&ArrowType::Boolean, None), PgType::BOOL);
        assert_eq!(arrow_to_pg_type(&ArrowType::Int32, None), PgType::INT4);
        assert_eq!(arrow_to_pg_type(&ArrowType::Int64, None), PgType::INT8);
        assert_eq!(arrow_to_pg_type(&ArrowType::Float64, None), PgType::FLOAT8);
        assert_eq!(arrow_to_pg_type(&ArrowType::Utf8, None), PgType::TEXT);
        assert_eq!(arrow_to_pg_type(&ArrowType::Binary, None), PgType::BYTEA);
        assert_eq!(arrow_to_pg_type(&ArrowType::Date32, None), PgType::DATE);
    }

    #[test]
    fn decimal_maps_to_numeric() {
        assert_eq!(
            arrow_to_pg_type(&ArrowType::Decimal128(10, 2), None),
            PgType::NUMERIC
        );
        assert_eq!(
            arrow_to_pg_type(&ArrowType::Decimal256(38, 9), None),
            PgType::NUMERIC
        );
    }

    #[test]
    fn timestamps_distinguish_tz() {
        assert_eq!(
            arrow_to_pg_type(&ArrowType::Timestamp(TimeUnit::Microsecond, None), None),
            PgType::TIMESTAMP
        );
        assert_eq!(
            arrow_to_pg_type(
                &ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                None
            ),
            PgType::TIMESTAMPTZ
        );
    }

    #[test]
    fn lists_announce_as_json_to_match_wire() {
        // The JSON-over-pgwire commit emits `[1,2,3]` (JSON) for List values,
        // *not* `{1,2,3}` (PG-array text). Announcing INT4_ARRAY here would
        // make every PG-wire client mis-parse the bytes. Until scalar.rs
        // emits real PG-array text, we announce JSON for every nested type.
        let int_list = ArrowType::List(Arc::new(Field::new("item", ArrowType::Int32, true)));
        assert_eq!(arrow_to_pg_type(&int_list, None), PgType::JSON);
    }

    #[test]
    fn nested_announce_as_json_for_dbeaver() {
        // Struct / Map serialise as JSON-text on the wire (see pgrepr::scalar);
        // make sure we *announce* JSON so clients format the column
        // appropriately — fixes DBeaver showing ugly braces and gives
        // downstream JSON deserialisers a chance.
        let s = ArrowType::Struct(vec![Field::new("a", ArrowType::Int32, true)].into());
        assert_eq!(arrow_to_pg_type(&s, None), PgType::JSON);
    }
}
