//! Static reference table of PostgreSQL types we expose, plus an
//! Arrow `DataType` → Postgres OID resolver.
//!
//! Source of truth for:
//!   * the `pg_catalog.pg_type` view (one row per [`PgTypeInfo`]),
//!   * `pg_attribute.atttypid` (column type OIDs),
//!   * `pg_proc.proargtypes` / `pg_proc.prorettype` (function signature OIDs),
//!   * the wire-level `RowDescription` `typeoid` field (used by
//!     [`crate::types::arrow_to_pg_type`]).
//!
//! All OIDs match upstream PostgreSQL exactly so `'pg_class'::regclass`,
//! `INT4OID = 23`, and similar invariants hold across drivers.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowType;
use once_cell::sync::Lazy;

/// PostgreSQL type metadata. Every column maps 1:1 to the relevant
/// `pg_catalog.pg_type` column.
#[derive(Debug, Clone, Copy)]
pub struct PgTypeInfo {
    /// Stable Postgres OID. Matches upstream.
    pub oid: u32,
    /// `typname` — short type name (e.g. `"int4"`, `"_int4"`).
    pub name: &'static str,
    /// `typlen` — fixed length in bytes, or -1 for variable length, -2 for
    /// C-string-style null-terminated.
    pub typlen: i16,
    /// `typbyval` — true if the value is passed by value.
    pub typbyval: bool,
    /// `typtype` — kind: `'b'` base, `'c'` composite, `'d'` domain, `'e'` enum,
    /// `'p'` pseudo, `'r'` range, `'m'` multirange.
    pub typtype: char,
    /// `typcategory` — broad category. See
    /// <https://www.postgresql.org/docs/current/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE>.
    pub typcategory: char,
    /// `typispreferred` — true if this is the preferred type in its category.
    pub typispreferred: bool,
    /// `typisdefined` — true if the type is fully defined (always true here).
    pub typisdefined: bool,
    /// `typdelim` — array-element delimiter (typically `','`).
    pub typdelim: char,
    /// `typelem` — element type OID if this is an array (else 0).
    pub typelem: u32,
    /// `typarray` — OID of the array type that has this as element (else 0).
    pub typarray: u32,
    /// `typalign` — `'c'` char / `'s'` short / `'i'` int / `'d'` double.
    pub typalign: char,
    /// `typstorage` — `'p'` plain / `'e'` external / `'m'` main / `'x'` extended.
    pub typstorage: char,
}

impl PgTypeInfo {
    /// True if this type represents a Postgres array.
    pub const fn is_array(&self) -> bool {
        self.typelem != 0
    }
}

// ---------------------------------------------------------------------------
// The big static table. Keep ordered by OID for readability.
// Standard Postgres OIDs — DO NOT renumber.
// ---------------------------------------------------------------------------

macro_rules! t {
    ($oid:expr, $name:expr, $typlen:expr, $typbyval:expr, $typtype:expr,
     $typcategory:expr, $typispreferred:expr, $typdelim:expr,
     $typelem:expr, $typarray:expr, $typalign:expr, $typstorage:expr) => {
        PgTypeInfo {
            oid: $oid,
            name: $name,
            typlen: $typlen,
            typbyval: $typbyval,
            typtype: $typtype,
            typcategory: $typcategory,
            typispreferred: $typispreferred,
            typisdefined: true,
            typdelim: $typdelim,
            typelem: $typelem,
            typarray: $typarray,
            typalign: $typalign,
            typstorage: $typstorage,
        }
    };
}

/// All built-in PostgreSQL types we surface in `pg_catalog.pg_type`.
/// Numbers match upstream PostgreSQL `pg_type.dat` exactly.
pub static PG_TYPES: &[PgTypeInfo] = &[
    // --- core scalars ---
    t!(  16, "bool",         1, true , 'b', 'B', true , ',',   0, 1000, 'c', 'p'),
    t!(  17, "bytea",       -1, false, 'b', 'U', false, ',',   0, 1001, 'i', 'x'),
    t!(  18, "char",         1, true , 'b', 'S', false, ',',   0, 1002, 'c', 'p'),
    t!(  19, "name",        64, false, 'b', 'S', false, ',',  18, 1003, 'c', 'p'),
    t!(  20, "int8",         8, true , 'b', 'N', false, ',',   0, 1016, 'd', 'p'),
    t!(  21, "int2",         2, true , 'b', 'N', false, ',',   0, 1005, 's', 'p'),
    t!(  22, "int2vector",  -1, false, 'b', 'A', false, ',',  21, 1006, 'i', 'p'),
    t!(  23, "int4",         4, true , 'b', 'N', false, ',',   0, 1007, 'i', 'p'),
    t!(  24, "regproc",      4, true , 'b', 'N', false, ',',   0, 1008, 'i', 'p'),
    t!(  25, "text",        -1, false, 'b', 'S', true , ',',   0, 1009, 'i', 'x'),
    t!(  26, "oid",          4, true , 'b', 'N', true , ',',   0, 1028, 'i', 'p'),
    t!(  27, "tid",          6, false, 'b', 'U', false, ',',   0, 1010, 's', 'p'),
    t!(  28, "xid",          4, true , 'b', 'U', false, ',',   0, 1011, 'i', 'p'),
    t!(  29, "cid",          4, true , 'b', 'U', false, ',',   0, 1012, 'i', 'p'),
    t!(  30, "oidvector",   -1, false, 'b', 'A', false, ',',  26, 1013, 'i', 'p'),
    // --- pg_node_tree (used in pg_attrdef.adbin etc.) ---
    t!( 194, "pg_node_tree",-1, false, 'b', 'S', false, ',',   0,    0, 'i', 'x'),
    // --- json / xml ---
    t!( 114, "json",        -1, false, 'b', 'U', false, ',',   0,  199, 'i', 'x'),
    t!( 142, "xml",         -1, false, 'b', 'U', false, ',',   0,  143, 'i', 'x'),
    // --- floating point ---
    t!( 700, "float4",       4, true , 'b', 'N', false, ',',   0, 1021, 'i', 'p'),
    t!( 701, "float8",       8, true , 'b', 'N', true , ',',   0, 1022, 'd', 'p'),
    t!( 705, "unknown",     -2, false, 'p', 'X', false, ',',   0,    0, 'c', 'p'),
    // --- money ---
    t!( 790, "money",        8, true , 'b', 'N', false, ',',   0,  791, 'd', 'p'),
    // --- character ---
    t!(1042, "bpchar",      -1, false, 'b', 'S', false, ',',   0, 1014, 'i', 'x'),
    t!(1043, "varchar",     -1, false, 'b', 'S', false, ',',   0, 1015, 'i', 'x'),
    // --- date/time ---
    t!(1082, "date",         4, true , 'b', 'D', false, ',',   0, 1182, 'i', 'p'),
    t!(1083, "time",         8, true , 'b', 'D', false, ',',   0, 1183, 'd', 'p'),
    t!(1114, "timestamp",    8, true , 'b', 'D', false, ',',   0, 1115, 'd', 'p'),
    t!(1184, "timestamptz",  8, true , 'b', 'D', true , ',',   0, 1185, 'd', 'p'),
    t!(1186, "interval",    16, false, 'b', 'T', true , ',',   0, 1187, 'd', 'p'),
    t!(1266, "timetz",      12, false, 'b', 'D', false, ',',   0, 1270, 'd', 'p'),
    // --- bit ---
    t!(1560, "bit",         -1, false, 'b', 'V', false, ',',   0, 1561, 'i', 'x'),
    t!(1562, "varbit",      -1, false, 'b', 'V', true , ',',   0, 1563, 'i', 'x'),
    // --- numeric ---
    t!(1700, "numeric",     -1, false, 'b', 'N', false, ',',   0, 1231, 'i', 'm'),
    // --- reg* ---
    t!(2202, "regprocedure", 4, true , 'b', 'N', false, ',',   0, 2207, 'i', 'p'),
    t!(2203, "regoper",      4, true , 'b', 'N', false, ',',   0, 2208, 'i', 'p'),
    t!(2204, "regoperator",  4, true , 'b', 'N', false, ',',   0, 2209, 'i', 'p'),
    t!(2205, "regclass",     4, true , 'b', 'N', false, ',',   0, 2210, 'i', 'p'),
    t!(2206, "regtype",      4, true , 'b', 'N', false, ',',   0, 2211, 'i', 'p'),
    t!(4096, "regrole",      4, true , 'b', 'N', false, ',',   0, 4097, 'i', 'p'),
    t!(4089, "regnamespace", 4, true , 'b', 'N', false, ',',   0, 4090, 'i', 'p'),
    t!(3734, "regconfig",    4, true , 'b', 'N', false, ',',   0, 3735, 'i', 'p'),
    t!(3769, "regdictionary",4, true , 'b', 'N', false, ',',   0, 3770, 'i', 'p'),
    // --- pseudo ---
    t!(2249, "record",      -1, false, 'p', 'P', false, ',',   0, 2287, 'd', 'x'),
    t!(2275, "cstring",     -2, false, 'p', 'P', false, ',',   0, 1263, 'c', 'p'),
    t!(2276, "any",          4, true , 'p', 'P', false, ',',   0,    0, 'i', 'p'),
    t!(2277, "anyarray",    -1, false, 'p', 'P', false, ',',   0,    0, 'd', 'x'),
    t!(2278, "void",         4, true , 'p', 'P', false, ',',   0,    0, 'i', 'p'),
    t!(2279, "trigger",      4, true , 'p', 'P', false, ',',   0,    0, 'i', 'p'),
    t!(2281, "internal",     8, true , 'p', 'P', false, ',',   0,    0, 'd', 'p'),
    t!(2283, "anyelement",   4, true , 'p', 'P', false, ',',   0,    0, 'i', 'p'),
    // --- uuid ---
    t!(2950, "uuid",        16, false, 'b', 'U', false, ',',   0, 2951, 'c', 'p'),
    // --- jsonb / jsonpath ---
    t!(3802, "jsonb",       -1, false, 'b', 'U', true , ',',   0, 3807, 'i', 'x'),
    t!(4072, "jsonpath",    -1, false, 'b', 'U', false, ',',   0, 4073, 'i', 'x'),
    // --- arrays (typelem points back to base) ---
    // bool[]
    t!(1000, "_bool",       -1, false, 'b', 'A', false, ',',  16,    0, 'i', 'x'),
    t!(1001, "_bytea",      -1, false, 'b', 'A', false, ',',  17,    0, 'i', 'x'),
    t!(1002, "_char",       -1, false, 'b', 'A', false, ',',  18,    0, 'i', 'x'),
    t!(1003, "_name",       -1, false, 'b', 'A', false, ',',  19,    0, 'i', 'x'),
    t!(1005, "_int2",       -1, false, 'b', 'A', false, ',',  21,    0, 'i', 'x'),
    t!(1006, "_int2vector", -1, false, 'b', 'A', false, ',',  22,    0, 'i', 'x'),
    t!(1007, "_int4",       -1, false, 'b', 'A', false, ',',  23,    0, 'i', 'x'),
    t!(1008, "_regproc",    -1, false, 'b', 'A', false, ',',  24,    0, 'i', 'x'),
    t!(1009, "_text",       -1, false, 'b', 'A', false, ',',  25,    0, 'i', 'x'),
    t!(1010, "_tid",        -1, false, 'b', 'A', false, ',',  27,    0, 'i', 'x'),
    t!(1011, "_xid",        -1, false, 'b', 'A', false, ',',  28,    0, 'i', 'x'),
    t!(1012, "_cid",        -1, false, 'b', 'A', false, ',',  29,    0, 'i', 'x'),
    t!(1013, "_oidvector",  -1, false, 'b', 'A', false, ',',  30,    0, 'i', 'x'),
    t!(1014, "_bpchar",     -1, false, 'b', 'A', false, ',',1042,    0, 'i', 'x'),
    t!(1015, "_varchar",    -1, false, 'b', 'A', false, ',',1043,    0, 'i', 'x'),
    t!(1016, "_int8",       -1, false, 'b', 'A', false, ',',  20,    0, 'd', 'x'),
    t!(1021, "_float4",     -1, false, 'b', 'A', false, ',', 700,    0, 'i', 'x'),
    t!(1022, "_float8",     -1, false, 'b', 'A', false, ',', 701,    0, 'd', 'x'),
    t!(1028, "_oid",        -1, false, 'b', 'A', false, ',',  26,    0, 'i', 'x'),
    t!(1115, "_timestamp",  -1, false, 'b', 'A', false, ',',1114,    0, 'd', 'x'),
    t!(1182, "_date",       -1, false, 'b', 'A', false, ',',1082,    0, 'i', 'x'),
    t!(1183, "_time",       -1, false, 'b', 'A', false, ',',1083,    0, 'd', 'x'),
    t!(1185, "_timestamptz",-1, false, 'b', 'A', false, ',',1184,    0, 'd', 'x'),
    t!(1187, "_interval",   -1, false, 'b', 'A', false, ',',1186,    0, 'd', 'x'),
    t!(1231, "_numeric",    -1, false, 'b', 'A', false, ',',1700,    0, 'i', 'x'),
    t!(1263, "_cstring",    -1, false, 'b', 'A', false, ',',2275,    0, 'i', 'x'),
    t!(1270, "_timetz",     -1, false, 'b', 'A', false, ',',1266,    0, 'd', 'x'),
    t!(1561, "_bit",        -1, false, 'b', 'A', false, ',',1560,    0, 'i', 'x'),
    t!(1563, "_varbit",     -1, false, 'b', 'A', false, ',',1562,    0, 'i', 'x'),
    t!( 199, "_json",       -1, false, 'b', 'A', false, ',', 114,    0, 'i', 'x'),
    t!( 143, "_xml",        -1, false, 'b', 'A', false, ',', 142,    0, 'i', 'x'),
    t!( 791, "_money",      -1, false, 'b', 'A', false, ',', 790,    0, 'd', 'x'),
    t!(2207, "_regprocedure",-1,false, 'b', 'A', false, ',',2202,    0, 'i', 'x'),
    t!(2208, "_regoper",    -1, false, 'b', 'A', false, ',',2203,    0, 'i', 'x'),
    t!(2209, "_regoperator",-1, false, 'b', 'A', false, ',',2204,    0, 'i', 'x'),
    t!(2210, "_regclass",   -1, false, 'b', 'A', false, ',',2205,    0, 'i', 'x'),
    t!(2211, "_regtype",    -1, false, 'b', 'A', false, ',',2206,    0, 'i', 'x'),
    t!(2287, "_record",     -1, false, 'p', 'P', false, ',',2249,    0, 'd', 'x'),
    t!(2951, "_uuid",       -1, false, 'b', 'A', false, ',',2950,    0, 'i', 'x'),
    t!(3735, "_regconfig",  -1, false, 'b', 'A', false, ',',3734,    0, 'i', 'x'),
    t!(3770, "_regdictionary",-1,false,'b', 'A', false, ',',3769,    0, 'i', 'x'),
    t!(3807, "_jsonb",      -1, false, 'b', 'A', false, ',',3802,    0, 'i', 'x'),
    t!(4073, "_jsonpath",   -1, false, 'b', 'A', false, ',',4072,    0, 'i', 'x'),
    t!(4090, "_regnamespace",-1,false, 'b', 'A', false, ',',4089,    0, 'i', 'x'),
    t!(4097, "_regrole",    -1, false, 'b', 'A', false, ',',4096,    0, 'i', 'x'),
];

// ---------------------------------------------------------------------------
// Lookup helpers — built once at startup.
// ---------------------------------------------------------------------------

static BY_OID: Lazy<Arc<std::collections::HashMap<u32, &'static PgTypeInfo>>> =
    Lazy::new(|| {
        Arc::new(
            PG_TYPES
                .iter()
                .map(|t| (t.oid, t))
                .collect::<std::collections::HashMap<_, _>>(),
        )
    });

static BY_NAME: Lazy<Arc<std::collections::HashMap<&'static str, &'static PgTypeInfo>>> =
    Lazy::new(|| {
        Arc::new(
            PG_TYPES
                .iter()
                .map(|t| (t.name, t))
                .collect::<std::collections::HashMap<_, _>>(),
        )
    });

/// Lookup PG type metadata by stable OID.
pub fn pg_type_by_oid(oid: u32) -> Option<&'static PgTypeInfo> {
    BY_OID.get(&oid).copied()
}

/// Lookup PG type metadata by Postgres `typname` (e.g. `"int4"`, `"_int4"`).
pub fn pg_type_by_name(name: &str) -> Option<&'static PgTypeInfo> {
    BY_NAME.get(name).copied()
}

// ---------------------------------------------------------------------------
// Arrow → PG mapping
// ---------------------------------------------------------------------------

static TEXT: &PgTypeInfo = &PG_TYPES[9]; // OID 25 — fallback for unknown

/// Map an Arrow `DataType` to PG type metadata. List-like containers recurse
/// into the element type and resolve to that element's array companion when
/// available, falling back to `text` for elements with no array OID.
///
/// `Struct`, `Map`, `Union` are serialised as JSON over the wire (see
/// `pgrepr::scalar`), so they map to `json` (OID 114).
///
/// `Dictionary<K, V>` and `RunEndEncoded<RE, V>` are *physical encodings* of
/// `V`, not nested logical types. We recurse into the value type so they
/// announce as whatever PG type `V` would — e.g. `Dictionary<UInt16, Utf8>`
/// resolves to OID 25 (`text`). Without this recursion, partition columns
/// from Hive-partitioned Delta tables (which delta-rs emits as
/// `Dictionary<UInt16, Utf8>` at scan time) get announced as `json` (OID 114),
/// then `Scalar::from_datafusion` has no Dictionary arm, falls through to
/// `Scalar::Other`, and `BinaryWriter::write_any` panics. This is the
/// regression that took out every dashboard on `rates_report_*` in 2026-05.
pub fn arrow_to_pg_type_info(t: &ArrowType) -> &'static PgTypeInfo {
    match t {
        ArrowType::Null => pg_type_by_oid(25).unwrap_or(TEXT),
        ArrowType::Boolean => pg_type_by_oid(16).unwrap_or(TEXT),
        ArrowType::Int8 | ArrowType::Int16 => pg_type_by_oid(21).unwrap_or(TEXT),
        ArrowType::UInt8 | ArrowType::UInt16 => pg_type_by_oid(21).unwrap_or(TEXT),
        ArrowType::Int32 => pg_type_by_oid(23).unwrap_or(TEXT),
        ArrowType::UInt32 => pg_type_by_oid(23).unwrap_or(TEXT),
        ArrowType::Int64 => pg_type_by_oid(20).unwrap_or(TEXT),
        ArrowType::UInt64 => pg_type_by_oid(20).unwrap_or(TEXT),
        ArrowType::Float16 | ArrowType::Float32 => pg_type_by_oid(700).unwrap_or(TEXT),
        ArrowType::Float64 => pg_type_by_oid(701).unwrap_or(TEXT),
        ArrowType::Decimal128(_, _) | ArrowType::Decimal256(_, _) => {
            pg_type_by_oid(1700).unwrap_or(TEXT)
        }
        ArrowType::Utf8 | ArrowType::LargeUtf8 => pg_type_by_oid(25).unwrap_or(TEXT),
        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::FixedSizeBinary(_) => {
            pg_type_by_oid(17).unwrap_or(TEXT)
        }
        ArrowType::Date32 | ArrowType::Date64 => pg_type_by_oid(1082).unwrap_or(TEXT),
        ArrowType::Time32(_) | ArrowType::Time64(_) => pg_type_by_oid(1083).unwrap_or(TEXT),
        ArrowType::Timestamp(_, None) => pg_type_by_oid(1114).unwrap_or(TEXT),
        ArrowType::Timestamp(_, Some(_)) => pg_type_by_oid(1184).unwrap_or(TEXT),
        ArrowType::Duration(_) | ArrowType::Interval(_) => pg_type_by_oid(1186).unwrap_or(TEXT),
        // Physical encodings — recurse on the value type so the wire OID
        // matches what the executor actually delivers.
        ArrowType::Dictionary(_, value) => arrow_to_pg_type_info(value),
        ArrowType::RunEndEncoded(_, value_field) => {
            arrow_to_pg_type_info(value_field.data_type())
        }
        // All nested Arrow types serialise as JSON text on the wire (see the
        // `Scalar::from_datafusion` Struct/List branch in `pgrepr::scalar`).
        // Postgres arrays use `{1,2,3}` text format — *not* JSON — so we
        // cannot announce `_int4` for a `List<Int32>` column without
        // breaking every Postgres-wire client that tries to parse
        // `[1,2,3]` as a PG array. Until we emit real PG-array text, every
        // nested type is announced as JSON (oid 114), matching the wire
        // bytes byte-for-byte and letting DBeaver / drivers format the
        // column as JSON instead of opaque text.
        ArrowType::List(_)
        | ArrowType::LargeList(_)
        | ArrowType::FixedSizeList(_, _)
        | ArrowType::Struct(_)
        | ArrowType::Map(_, _)
        | ArrowType::Union(_, _) => pg_type_by_oid(114).unwrap_or(TEXT),
    }
}

/// Convenience: return just the OID for an Arrow `DataType`.
pub fn arrow_to_pg_oid(t: &ArrowType) -> u32 {
    arrow_to_pg_type_info(t).oid
}

/// Map an Arrow type *name* (the Debug-style identifier we store in
/// `glare_catalog.columns.data_type` and `glare_catalog.functions.argument_types`)
/// to a Postgres OID. Unknown names resolve to OID 25 (`text`).
///
/// Accepts the leading identifier of a parametric type — `"Decimal128(10, 2)"`
/// matches as `"Decimal128"`, `"Timestamp(Microsecond, None)"` matches as
/// `"Timestamp"` etc.
///
/// `Dictionary(K, V)` recurses into V so the OID matches the underlying
/// logical type. Mirrors `arrow_to_pg_type_info`'s treatment — without this,
/// a Hive-partition `Dictionary(UInt16, Utf8)` row in `glare_catalog.columns`
/// resolves to `json` (114) and pgwire announces JSON for a text column.
pub fn arrow_name_to_pg_oid(name: &str) -> u32 {
    let head = name.split(['(', '<']).next().unwrap_or(name).trim();
    match head {
        "Boolean" | "Bool" => 16,
        "Int8" | "Int16" | "UInt8" | "UInt16" => 21,
        "Int32" | "UInt32" => 23,
        "Int64" | "UInt64" => 20,
        "Float16" | "Float32" => 700,
        "Float64" => 701,
        "Decimal128" | "Decimal256" => 1700,
        "Utf8" | "LargeUtf8" => 25,
        "Binary" | "LargeBinary" | "FixedSizeBinary" => 17,
        "Date32" | "Date64" => 1082,
        "Time32" | "Time64" => 1083,
        "Timestamp" => {
            // A bare "Timestamp" without tz info → assume timestamp w/o tz.
            // Distinguish only by inspecting the raw string.
            if name.contains("Some(") {
                1184
            } else {
                1114
            }
        }
        "Duration" | "Interval" => 1186,
        // Dictionary<K, V> is a physical wrapper around V — recurse so e.g.
        // "Dictionary(UInt16, Utf8)" → OID 25 (text), matching what
        // `arrow_to_pg_type_info` does for the runtime ArrowType. Walks paren
        // depth because V can itself be parametric — `Dictionary(Int32,
        // Decimal128(10, 2))` must split on the comma after Int32, not on
        // the comma inside Decimal128.
        "Dictionary" => {
            if let Some(start) = name.find('(') {
                if let Some(end) = name.rfind(')') {
                    if end > start {
                        let inner = &name[start + 1..end];
                        if let Some(comma) = split_top_level_comma(inner) {
                            let value = inner[comma + 1..].trim();
                            return arrow_name_to_pg_oid(value);
                        }
                    }
                }
            }
            114
        }
        // All nested Arrow types are JSON-on-wire — see arrow_to_pg_type_info
        // for the rationale (PG arrays use `{}` text, not JSON).
        "List" | "LargeList" | "FixedSizeList" => 114,
        "Struct" | "Map" | "Union" | "RunEndEncoded" => 114,
        "Null" => 25,
        _ => 25,
    }
}

/// Find the index of the first top-level comma in `s` — i.e. one that lives
/// at paren depth zero. Returns `None` if no such comma exists.
///
/// Used by `arrow_name_to_pg_oid`'s `Dictionary` arm to split
/// `"K, V"` correctly even when `V` is itself parametric and contains its
/// own commas inside its parens.
fn split_top_level_comma(s: &str) -> Option<usize> {
    let mut depth: i32 = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' | '<' => depth += 1,
            ')' | '>' => depth -= 1,
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, TimeUnit};

    use super::*;

    #[test]
    fn no_duplicate_oids() {
        let mut seen = HashSet::new();
        for t in PG_TYPES {
            assert!(seen.insert(t.oid), "duplicate oid: {} ({})", t.oid, t.name);
        }
    }

    #[test]
    fn no_duplicate_names() {
        let mut seen = HashSet::new();
        for t in PG_TYPES {
            assert!(seen.insert(t.name), "duplicate name: {}", t.name);
        }
    }

    #[test]
    fn array_back_references_resolve() {
        // Every `_X` row's typelem must point at an existing scalar row.
        for t in PG_TYPES.iter().filter(|t| t.name.starts_with('_')) {
            assert!(
                pg_type_by_oid(t.typelem).is_some(),
                "{} has typelem {} which is missing from PG_TYPES",
                t.name,
                t.typelem
            );
        }
    }

    #[test]
    fn scalar_array_pairs_match() {
        // Every scalar that advertises typarray must point at a real array row
        // whose typelem points back.
        for t in PG_TYPES.iter().filter(|t| t.typarray != 0) {
            let arr = pg_type_by_oid(t.typarray).expect("typarray missing");
            assert_eq!(
                arr.typelem, t.oid,
                "{} ↔ {} mismatch (typarray {} vs typelem {})",
                t.name, arr.name, t.typarray, arr.typelem
            );
        }
    }

    #[test]
    fn well_known_oids() {
        assert_eq!(pg_type_by_name("bool").unwrap().oid, 16);
        assert_eq!(pg_type_by_name("int4").unwrap().oid, 23);
        assert_eq!(pg_type_by_name("int8").unwrap().oid, 20);
        assert_eq!(pg_type_by_name("text").unwrap().oid, 25);
        assert_eq!(pg_type_by_name("numeric").unwrap().oid, 1700);
        assert_eq!(pg_type_by_name("json").unwrap().oid, 114);
        assert_eq!(pg_type_by_name("jsonb").unwrap().oid, 3802);
        assert_eq!(pg_type_by_name("uuid").unwrap().oid, 2950);
        assert_eq!(pg_type_by_name("regclass").unwrap().oid, 2205);
    }

    #[test]
    fn arrow_primitive_mappings() {
        assert_eq!(arrow_to_pg_oid(&ArrowType::Boolean), 16);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Int32), 23);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Int64), 20);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Float64), 701);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Utf8), 25);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Binary), 17);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Date32), 1082);
        assert_eq!(
            arrow_to_pg_oid(&ArrowType::Timestamp(TimeUnit::Microsecond, None)),
            1114
        );
        assert_eq!(
            arrow_to_pg_oid(&ArrowType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            1184
        );
        assert_eq!(arrow_to_pg_oid(&ArrowType::Decimal128(10, 2)), 1700);
        assert_eq!(arrow_to_pg_oid(&ArrowType::Decimal256(10, 2)), 1700);
    }

    #[test]
    fn arrow_list_announces_json_to_match_wire() {
        // Every nested type — including List — is announced as JSON because
        // the wire content is JSON, not Postgres-array text. See
        // arrow_to_pg_type_info for the rationale. If we ever switch the
        // wire format to PG-array `{1,2,3}` syntax, this test should change
        // along with the mapping.
        let int_list = ArrowType::List(Arc::new(Field::new("item", ArrowType::Int32, true)));
        assert_eq!(arrow_to_pg_oid(&int_list), 114);

        let ts_list = ArrowType::LargeList(Arc::new(Field::new(
            "item",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )));
        assert_eq!(arrow_to_pg_oid(&ts_list), 114);
    }

    #[test]
    fn arrow_struct_maps_to_json() {
        let s = ArrowType::Struct(
            vec![Field::new("a", ArrowType::Int32, true)]
                .into(),
        );
        assert_eq!(arrow_to_pg_oid(&s), 114); // json
    }

    #[test]
    fn arrow_name_to_pg_oid_basic() {
        assert_eq!(arrow_name_to_pg_oid("Int32"), 23);
        assert_eq!(arrow_name_to_pg_oid("Utf8"), 25);
        assert_eq!(arrow_name_to_pg_oid("Decimal128(10, 2)"), 1700);
        assert_eq!(arrow_name_to_pg_oid("Timestamp(Microsecond, None)"), 1114);
        assert_eq!(
            arrow_name_to_pg_oid("Timestamp(Microsecond, Some(\"UTC\"))"),
            1184
        );
        assert_eq!(arrow_name_to_pg_oid("List<Int32>"), 114);
        assert_eq!(arrow_name_to_pg_oid("Struct"), 114);
        assert_eq!(arrow_name_to_pg_oid("definitely-not-a-type"), 25);
    }

    /// Regression: `silver_securities.rates_report_series` had its
    /// partition column `countrycode` persisted as `Dictionary(UInt16, Utf8)`.
    /// `arrow_name_to_pg_oid` previously announced this as OID 114 (json),
    /// which broke every dashboard reading the column. Dictionary should
    /// announce as the value type's OID.
    #[test]
    fn arrow_to_pg_oid_dictionary_recurses_into_value_type() {
        let dict_utf8 = ArrowType::Dictionary(
            Box::new(ArrowType::UInt16),
            Box::new(ArrowType::Utf8),
        );
        assert_eq!(arrow_to_pg_oid(&dict_utf8), 25);

        let dict_int = ArrowType::Dictionary(
            Box::new(ArrowType::Int32),
            Box::new(ArrowType::Int64),
        );
        assert_eq!(arrow_to_pg_oid(&dict_int), 20);

        let dict_decimal = ArrowType::Dictionary(
            Box::new(ArrowType::Int32),
            Box::new(ArrowType::Decimal128(10, 2)),
        );
        assert_eq!(arrow_to_pg_oid(&dict_decimal), 1700);

        // Doubly-wrapped dictionaries are unusual but well-defined; the
        // recursion should peel both layers.
        let dict_dict = ArrowType::Dictionary(
            Box::new(ArrowType::UInt8),
            Box::new(ArrowType::Dictionary(
                Box::new(ArrowType::UInt16),
                Box::new(ArrowType::Utf8),
            )),
        );
        assert_eq!(arrow_to_pg_oid(&dict_dict), 25);
    }

    #[test]
    fn arrow_to_pg_oid_run_end_encoded_recurses_into_value_type() {
        // RunEndEncoded<run_ends_field, values_field> wraps the values
        // field's logical type. Recurse so the OID matches what the wire
        // delivers, not what the run-length encoding looks like.
        let ree_utf8 = ArrowType::RunEndEncoded(
            Arc::new(Field::new("run_ends", ArrowType::Int32, false)),
            Arc::new(Field::new("values", ArrowType::Utf8, true)),
        );
        assert_eq!(arrow_to_pg_oid(&ree_utf8), 25);
    }

    /// `arrow_name_to_pg_oid` operates on the Debug-rendered Arrow string
    /// stored in `glare_catalog.columns`. For `Dictionary(K, V)` the value
    /// can itself be parametric, so the split must walk paren depth — a
    /// naive first-comma split on `Dictionary(Int32, Decimal128(10, 2))`
    /// would extract the wrong value name.
    #[test]
    fn arrow_name_to_pg_oid_dictionary_recurses_into_value_type() {
        assert_eq!(arrow_name_to_pg_oid("Dictionary(UInt16, Utf8)"), 25);
        assert_eq!(arrow_name_to_pg_oid("Dictionary(Int32, Int64)"), 20);
        assert_eq!(
            arrow_name_to_pg_oid("Dictionary(Int32, Decimal128(10, 2))"),
            1700
        );
        assert_eq!(
            arrow_name_to_pg_oid("Dictionary(Int32, Timestamp(Microsecond, None))"),
            1114
        );
        // Doubly-wrapped — recursion peels both layers.
        assert_eq!(
            arrow_name_to_pg_oid("Dictionary(UInt8, Dictionary(UInt16, Utf8))"),
            25
        );
        // Malformed input falls back to OID 114 (json), matching the
        // prior nested-type catch-all behaviour.
        assert_eq!(arrow_name_to_pg_oid("Dictionary"), 114);
        assert_eq!(arrow_name_to_pg_oid("Dictionary(NoCommaInside)"), 114);
    }

    #[test]
    fn split_top_level_comma_walks_paren_depth() {
        assert_eq!(split_top_level_comma("Int32, Utf8"), Some(5));
        assert_eq!(
            split_top_level_comma("Int32, Decimal128(10, 2)"),
            Some(5)
        );
        // Comma inside parens does NOT split.
        assert_eq!(split_top_level_comma("Decimal128(10, 2)"), None);
        // No comma at all.
        assert_eq!(split_top_level_comma("Utf8"), None);
    }
}
