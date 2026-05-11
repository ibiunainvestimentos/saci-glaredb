//! Builtins as determined by Metastore.
//!
//! On catalog initialization, either by loading in a catalog from storage, or
//! creating a new one, a set of builtins will be inserted into the catalog
//! (see Storage initialize).
//!
//! Two main takeaways:
//!
//! - Builtins are not persisted.
//! - Changing builtins just requires redeploying Metastore.
//!
//! However, there is one drawback to Metastore being the source-of-truth for
//! builtins. If we add or change a builtin table and redeploy Metastore, a
//! database node will be able to see it, but will not be able to execute
//! appropriately. We can revisit this if this isn't acceptable long-term.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use once_cell::sync::Lazy;
use pgrepr::oid::FIRST_GLAREDB_BUILTIN_ID;
use protogen::metastore::types::options::InternalColumnDefinition;

/// The default catalog that exists in all GlareDB databases.
pub const DEFAULT_CATALOG: &str = "default";

/// Default schema that's created on every startup.
pub const DEFAULT_SCHEMA: &str = "public";

/// Internal schema for system tables.
pub const INTERNAL_SCHEMA: &str = "glare_catalog";

pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// Schema to store temporary objects (only valid for current session).
pub const CURRENT_SESSION_SCHEMA: &str = "current_session";

/// First oid available for other builtin objects that don't have a stable OID.
///
/// Builtin schemas have stable OIDs since everything (builtin and user objects)
/// depends on a schema.
///
/// Builtin tables have stable OIDs since some depend on data written to disk.
///
/// First glaredb builtin OID: 16384
/// First user object OID: 20000
///
/// This means we have ~3600 OIDs to play with for builtin objects. Note that
/// once a builtin object is given a stable OID, it **must not** be changed ever
/// (unless you're the person willing to write a migration system).
///
/// Stable OIDs should also not be reused. E.g. if we end up removing a table in
/// the future, we should default to not using that OID in the future (there are
/// cases where an OID is safe to reuse, but that should be determined
/// case-by-case).
///
/// General OID ranges:
/// Builtin schemas: 16385 - 16400 (16 OIDs)
/// Builtin tables: 16401 - 16500 (100 OIDs)
///
/// Constructing the builtin catalog happens in metastore, and errors on
/// encountering a duplicated OID. A test exists to ensure it's able to be
/// built.
pub const FIRST_NON_STATIC_OID: u32 = FIRST_GLAREDB_BUILTIN_ID + 116;

#[derive(Debug, Clone)]
pub struct BuiltinDatabase {
    pub name: &'static str,
    pub oid: u32,
}

pub static DATABASE_DEFAULT: Lazy<BuiltinDatabase> = Lazy::new(|| BuiltinDatabase {
    name: DEFAULT_CATALOG,
    oid: FIRST_GLAREDB_BUILTIN_ID,
});

impl BuiltinDatabase {
    pub fn builtins() -> Vec<&'static BuiltinDatabase> {
        vec![&DATABASE_DEFAULT]
    }
}

/// A builtin table.
// TODO: Do we want something to indicate if a table is persisted in delta?
#[derive(Debug, Clone)]
pub struct BuiltinTable {
    pub schema: &'static str,
    pub name: &'static str,
    pub columns: Vec<InternalColumnDefinition>,
    pub oid: u32,
}

pub static GLARE_DATABASES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "databases",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("external", DataType::Boolean, false),
        ("datasource", DataType::Utf8, false),
        ("access_mode", DataType::Utf8, false), // `SourceAccessMode::as_str()`
    ]),
    oid: 16401,
});

pub static GLARE_TUNNELS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tunnels",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("tunnel_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("tunnel_type", DataType::Utf8, false),
    ]),
    oid: 16402,
});

pub static GLARE_CREDENTIALS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "credentials",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("credentials_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("provider", DataType::Utf8, false),
        ("comment", DataType::Utf8, false),
    ]),
    oid: 16403,
});

pub static GLARE_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "schemas",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("database_name", DataType::Utf8, false),
        ("schema_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
    ]),
    oid: 16404,
});

pub static GLARE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tables",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("table_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("external", DataType::Boolean, false),
        ("datasource", DataType::Utf8, false),
        ("access_mode", DataType::Utf8, false), // `SourceAccessMode::as_str()`
        // User comment (`COMMENT ON TABLE`). Surfaces as
        // `pg_description.description`.
        ("comment", DataType::Utf8, true),
        // Approximate row count from Delta statistics. Populated by
        // `build_glare_tables` for native (writeable Delta) tables;
        // NULL for builtin / external / temp / never-loaded tables.
        // Surfaced as `pg_class.reltuples`.
        ("reltuples", DataType::Float64, true),
    ]),
    oid: 16405,
});

pub static GLARE_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "views",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("view_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("sql", DataType::Utf8, false),
        ("comment", DataType::Utf8, true),
    ]),
    oid: 16406,
});

pub static GLARE_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "columns",
    columns: InternalColumnDefinition::from_tuples([
        ("schema_oid", DataType::UInt32, false),
        ("table_oid", DataType::UInt32, false),
        ("table_name", DataType::Utf8, false),
        ("column_name", DataType::Utf8, false),
        ("column_ordinal", DataType::UInt32, false),
        ("data_type", DataType::Utf8, false),
        ("is_nullable", DataType::Boolean, false),
    ]),
    oid: 16407,
});

pub static GLARE_FUNCTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "functions",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("function_name", DataType::Utf8, false),
        ("function_type", DataType::Utf8, false), // table, scalar, aggregate
        // Human-readable signature strings — one entry per `OneOf` branch.
        // Pre-existing column; kept for backward compatibility with the
        // `glare_catalog.functions` table that users / tools already query.
        (
            "parameters",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
            false,
        ),
        ("builtin", DataType::Boolean, false),
        ("example", DataType::Utf8, true),
        ("description", DataType::Utf8, true),
        // Arrow type name of each argument of the function's *first* (or
        // preferred) signature, in positional order. Drives
        // `pg_proc.proargtypes` and `pg_get_function_arguments(oid)`.
        // Empty list = nullary or signature is non-`Exact`.
        (
            "argument_types",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
            false,
        ),
        // Pre-formatted oidvector wire shape for `pg_proc.proargtypes` —
        // space-separated decimal OIDs (`"23 25 1043"`). Real Postgres'
        // `proargtypes` is `oidvector`, parsed by PgJDBC's
        // `getFunctionColumns` via `StringTokenizer`. NULL when the
        // signature is non-`Exact` or arrow→OID resolution fails.
        ("argument_oids_text", DataType::Utf8, true),
        // Arrow type name of the return value. NULL when the function does
        // not declare a fixed return type at registration time (most
        // DataFusion `ScalarUDF`s defer return-type inference to planning).
        ("return_type", DataType::Utf8, true),
        // True for table-returning (i.e. set-returning) functions. Maps to
        // `pg_proc.proretset`.
        ("is_set_returning", DataType::Boolean, false),
    ]),
    oid: 16408,
});

pub static GLARE_SSH_KEYS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "ssh_keys",
    columns: InternalColumnDefinition::from_tuples([
        ("ssh_tunnel_oid", DataType::UInt32, false),
        ("ssh_tunnel_name", DataType::Utf8, false),
        ("public_key", DataType::Utf8, false),
    ]),
    oid: 16409,
});

pub static GLARE_DEPLOYMENT_METADATA: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "deployment_metadata",
    columns: InternalColumnDefinition::from_tuples([
        ("key", DataType::Utf8, false),
        ("value", DataType::Utf8, false),
    ]),
    oid: 16410,
});

/// Cached table metadata for external databases.
///
/// This stores information for all tables, and all columns for each table.
///
/// The cached data lives in an on-disk (delta) table alongside user table data.
// TODO: Do we want to store columns in a separate table?
pub static GLARE_CACHED_EXTERNAL_DATABASE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "cached_external_database_tables",
    columns: InternalColumnDefinition::from_tuples([
        // External database this entry is for.
        ("database_oid", DataType::UInt32, false),
        // Schema name (in external database).
        ("schema_name", DataType::Utf8, false),
        // Table name (in external database).
        ("table_name", DataType::Utf8, false),
        // Column name (in external database).
        ("column_name", DataType::Utf8, false),
        ("data_type", DataType::Utf8, false),
    ]),
    oid: 16411,
});

/// Index metadata for relations. Empty today (Delta tables have no
/// secondary indexes), but the column shape is the source-of-truth for the
/// `pg_catalog.pg_index` view — DBeaver and friends issue
/// `SELECT … FROM pg_index WHERE 1<>1` to probe columns at startup, so the
/// schema must exist even when no rows do.
pub static GLARE_INDEXES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "indexes",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("table_oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("index_name", DataType::Utf8, false),
        // Whether this is the primary key for the table.
        ("is_primary", DataType::Boolean, false),
        // Whether this index enforces uniqueness.
        ("is_unique", DataType::Boolean, false),
        // 1-based ordinal positions of the indexed columns. Stored as a
        // text-encoded space-separated list so it round-trips through
        // `pg_index.indkey` (an `int2vector` on the wire).
        ("column_positions", DataType::Utf8, true),
        // Optional partial-index predicate (raw SQL). NULL = full index.
        ("predicate", DataType::Utf8, true),
        // Optional expression list for expression indexes.
        ("expression", DataType::Utf8, true),
        ("access_method", DataType::Utf8, false), // btree / hash / gin / ...
    ]),
    oid: 16412,
});

/// Constraint metadata for relations. Empty today (Delta tables have no
/// PRIMARY KEY / FOREIGN KEY enforcement), but DBeaver probes
/// `pg_catalog.pg_constraint` columns at connect — keep the column shape so
/// that probe succeeds even with zero rows.
pub static GLARE_CONSTRAINTS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "constraints",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("conname", DataType::Utf8, false),
        ("schema_oid", DataType::UInt32, false),
        // 'c' check, 'u' unique, 'p' primary, 'f' foreign, 'x' exclusion.
        ("contype", DataType::Utf8, false),
        ("table_oid", DataType::UInt32, false),
        ("ref_table_oid", DataType::UInt32, true),
        // Constrained columns (1-based ordinals, space-joined). Maps to
        // `pg_constraint.conkey` (`int2[]`).
        ("column_positions", DataType::Utf8, true),
        // Referenced columns for FKs (`confkey`).
        ("ref_column_positions", DataType::Utf8, true),
        ("update_action", DataType::Utf8, true), // a / r / c / n / d
        ("delete_action", DataType::Utf8, true),
        ("match_type", DataType::Utf8, true), // f full / p partial / s simple
        ("is_deferrable", DataType::Boolean, false),
        ("is_deferred", DataType::Boolean, false),
        ("is_validated", DataType::Boolean, false),
        // Raw SQL expression for CHECK / partial constraints. Renders to
        // `pg_get_constraintdef` output.
        ("definition", DataType::Utf8, true),
    ]),
    oid: 16413,
});

/// Per-session variable inventory. Populated at dispatch time from
/// `SessionVarsInner::entries()` in datafusion_ext. Backs
/// `pg_catalog.pg_settings`, which downstream tools (DBeaver,
/// `SHOW ALL`, JDBC's `VariableInfo`) rely on for runtime
/// configuration introspection.
pub static GLARE_SESSION_VARS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "session_vars",
    columns: InternalColumnDefinition::from_tuples([
        ("name", DataType::Utf8, false),
        ("setting", DataType::Utf8, true),
        ("short_desc", DataType::Utf8, true),
    ]),
    oid: 16414,
});

impl BuiltinTable {
    /// Check if this table matches the provided schema and name.
    pub fn matches(&self, schema: &str, name: &str) -> bool {
        self.schema == schema && self.name == name
    }

    /// Get the arrow schema for the builtin table.
    pub fn arrow_schema(&self) -> ArrowSchema {
        ArrowSchema::new(
            self.columns
                .iter()
                .map(|col| ArrowField::new(&col.name, col.arrow_type.clone(), col.nullable))
                .collect::<Vec<_>>(),
        )
    }

    /// Return a vector of all builtin tables.
    pub fn builtins() -> Vec<&'static BuiltinTable> {
        vec![
            &GLARE_DATABASES,
            &GLARE_TUNNELS,
            &GLARE_CREDENTIALS,
            &GLARE_SCHEMAS,
            &GLARE_VIEWS,
            &GLARE_TABLES,
            &GLARE_COLUMNS,
            &GLARE_FUNCTIONS,
            &GLARE_SSH_KEYS,
            &GLARE_DEPLOYMENT_METADATA,
            &GLARE_CACHED_EXTERNAL_DATABASE_TABLES,
            &GLARE_INDEXES,
            &GLARE_CONSTRAINTS,
            &GLARE_SESSION_VARS,
        ]
    }
}

/// A builtin schema.
#[derive(Debug, Clone)]
pub struct BuiltinSchema {
    pub name: &'static str,
    pub oid: u32,
}

pub static SCHEMA_INTERNAL: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INTERNAL_SCHEMA,
    oid: 16385,
});

pub static SCHEMA_DEFAULT: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: DEFAULT_SCHEMA,
    oid: 16386,
});

pub static SCHEMA_INFORMATION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INFORMATION_SCHEMA,
    oid: 16387,
});

pub static SCHEMA_POSTGRES: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: POSTGRES_SCHEMA,
    oid: 16388,
});

pub static SCHEMA_CURRENT_SESSION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: CURRENT_SESSION_SCHEMA,
    oid: 16389,
});

impl BuiltinSchema {
    pub fn builtins() -> Vec<&'static BuiltinSchema> {
        vec![
            &SCHEMA_INTERNAL,
            &SCHEMA_DEFAULT,
            &SCHEMA_INFORMATION,
            &SCHEMA_POSTGRES,
            &SCHEMA_CURRENT_SESSION,
        ]
    }
}

/// A builtin view.
#[derive(Debug, Clone)]
pub struct BuiltinView {
    pub schema: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
}

pub static GLARE_EXTERNAL_DATASOURCES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INTERNAL_SCHEMA,
    name: "external_datasources",
    sql: "
WITH datasources(oid, name, datasource, object_type, external, access_mode) AS (
    SELECT oid,
           database_name,
           datasource,
           'database',
           external,
           access_mode
    FROM glare_catalog.databases
    UNION
    SELECT oid,
           table_name,
           datasource,
           'table',
           external,
           access_mode
    FROM glare_catalog.tables
)
SELECT * FROM datasources WHERE external = true",
});

// Information schema tables.
//
// See <https://www.postgresql.org/docs/current/information-schema.html>

pub static INFORMATION_SCHEMA_SCHEMATA: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "schemata",
    sql: "
SELECT
    database_name AS catalog_name,
    schema_name AS schema_name,
    null AS schema_owner,
    null AS default_character_set_catalog,
    null AS default_character_set_schema,
    null AS default_character_set_name,
    null AS sql_path
FROM glare_catalog.schemas",
});

pub static INFORMATION_SCHEMA_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "tables",
    sql: "
SELECT *
FROM (
    SELECT
        d.database_name AS table_catalog,
        t.schema_name AS table_schema,
        t.table_name AS table_name,
        'BASE TABLE' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.tables t INNER JOIN glare_catalog.databases d ON t.database_oid = d.oid
    UNION ALL
    SELECT
        d.database_name AS table_catalog,
        v.schema_name AS table_schema,
        v.view_name AS table_name,
        'VIEW' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.views v INNER JOIN glare_catalog.databases d ON v.database_oid = d.oid
)",
});

pub static INFORMATION_SCHEMA_COLUMNS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "columns",
    sql: "
SELECT
    d.database_name AS table_catalog,
    s.schema_name AS table_schema,
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.column_ordinal + 1 AS ordinal_position,
    null AS column_default,
    c.is_nullable AS is_nullable,
    c.data_type AS data_type,
    null AS character_maximum_length,
    null AS numeric_precision,
    null AS numeric_precision_radix,
    null AS numeric_scale,
    null AS datetime_precision,
    null AS interval_type,
    null AS interval_precision,
    null AS character_set_catalog,
    null AS character_set_schema,
    null AS character_set_name,
    null AS collation_catalog,
    null AS collation_schema,
    null AS collation_name,
    null AS domain_catalog,
    null AS domain_schema,
    null AS domain_name,
    null AS udt_catalog,
    null AS udt_schema,
    null AS udt_name,
    null AS scope_catalog,
    null AS scope_schema,
    null AS scope_name,
    null AS maximum_cardinality,
    null AS dtd_identifier,
    null AS is_self_referencing,
    null AS is_identity,
    null AS identity_generation,
    null AS identity_start,
    null AS identity_increment,
    null AS identity_maximum,
    null AS identity_minimum,
    null AS identity_cycle,
    null AS is_generated,
    null AS generation_expression,
    'NO' AS is_updatable
FROM glare_catalog.columns c
INNER JOIN glare_catalog.schemas s ON c.schema_oid = s.oid
INNER JOIN glare_catalog.databases d ON s.database_oid = d.oid
",
});

// ---------------------------------------------------------------------------
// Postgres catalog tables.
//
// Each `pg_catalog.X` relation is exposed as a `BuiltinView` whose body is a
// `SELECT` against the canonical `glare_catalog.X` builtin tables. This
// follows the DuckDB `default_views.cpp` shape and means we never duplicate
// catalog state — `glare_catalog` is the single source of truth, `pg_catalog`
// is a thin Postgres-shaped projection on top.
//
// The full column shape matters for tools like DBeaver / DataGrip / pgcli /
// dbt / ibis: they probe each column at startup with
// `SELECT col FROM pg_catalog.X WHERE 1<>1`, branch on success, and silently
// disable features when a column is missing. Empty rowsets are fine; missing
// *columns* are not.
//
// See <https://www.postgresql.org/docs/current/catalogs.html>.
//
// IMPORTANT: every NULL inside `WHERE false` views is wrapped in `CAST(NULL
// AS …)` so DataFusion infers a concrete type for the column instead of
// `Null` — otherwise the wire-level `RowDescription` reports `text` for a
// numeric column and DBeaver mis-formats the probe result.

pub static PG_AM: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_am",
    // The classic Postgres access methods so DBeaver's `AccessMethodCache`
    // populates with sensible names. `amhandler=0` is acceptable — DBeaver
    // does not require it to point at a real `pg_proc` row.
    sql: "
SELECT * FROM (VALUES
    (CAST(403 AS INT), 'btree', CAST(0 AS INT), 'i'),
    (CAST(405 AS INT), 'hash',  CAST(0 AS INT), 'i'),
    (CAST(783 AS INT), 'gist',  CAST(0 AS INT), 'i'),
    (CAST(2742 AS INT),'gin',   CAST(0 AS INT), 'i'),
    (CAST(3580 AS INT),'brin',  CAST(0 AS INT), 'i')
) AS am(oid, amname, amhandler, amtype)",
});

pub static PG_ATTRIBUTE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_attribute",
    // One row per (table, column). DBeaver issues this view *per result-set
    // column* of every query (see PgJDBC `PgResultSetMetaData`), so this is
    // the hottest catalog view — keep it cheap.
    //
    // `atttypid` is resolved via the helper UDF `pg_type_oid_by_name`
    // against the Arrow type name stored in `glare_catalog.columns.data_type`
    // — see `pgrepr::pg_type_oid::arrow_name_to_pg_oid` for the bijection.
    //
    // `glare_catalog.columns` carries rows for both tables and views (the
    // dispatcher emits view rows from `ViewEntry.column_types`, captured
    // at `CREATE VIEW` time from the planned SELECT body), so a single
    // projection covers both `relkind='r'` and `relkind='v'` consumers.
    sql: "
SELECT
    c.table_oid                            AS attrelid,
    c.column_name                          AS attname,
    pg_catalog.pg_type_oid_by_name(c.data_type) AS atttypid,
    CAST(0 AS INT)                         AS attstattarget,
    CAST(-1 AS SMALLINT)                   AS attlen,
    CAST(c.column_ordinal AS SMALLINT) + 1 AS attnum,
    CAST(0 AS SMALLINT)                    AS attndims,
    CAST(-1 AS INT)                        AS attcacheoff,
    CAST(-1 AS INT)                        AS atttypmod,
    false                                  AS attbyval,
    CAST(NULL AS TEXT)                     AS attalign,
    CAST(NULL AS TEXT)                     AS attstorage,
    CAST(NULL AS TEXT)                     AS attcompression,
    NOT c.is_nullable                      AS attnotnull,
    false                                  AS atthasdef,
    false                                  AS atthasmissing,
    CAST('' AS TEXT)                       AS attidentity,
    CAST('' AS TEXT)                       AS attgenerated,
    false                                  AS attisdropped,
    true                                   AS attislocal,
    CAST(0 AS SMALLINT)                    AS attinhcount,
    CAST(0 AS INT)                         AS attcollation,
    CAST(NULL AS TEXT)                     AS attacl,
    CAST(NULL AS TEXT)                     AS attoptions,
    CAST(NULL AS TEXT)                     AS attfdwoptions,
    CAST(NULL AS TEXT)                     AS attmissingval
FROM glare_catalog.columns c",
});

pub static PG_CLASS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_class",
    // Tables (`relkind='r'`) and views (`relkind='v'`) projected from
    // `glare_catalog`. `relnatts` comes from a correlated count of matching
    // `glare_catalog.columns` rows. The `reltype` join into `pg_type`
    // composite-row entries is left at 0 for now — DBeaver doesn't follow
    // that pointer for the tree expansion path.
    sql: "
SELECT
    t.oid                                                    AS oid,
    t.table_name                                             AS relname,
    CASE t.schema_oid WHEN 16388 THEN 11 WHEN 16386 THEN 2200
         ELSE t.schema_oid END                               AS relnamespace,
    CAST(0 AS INT)                                           AS reltype,
    CAST(0 AS INT)                                           AS reloftype,
    CAST(10 AS INT)                                          AS relowner,
    CAST(0 AS INT)                                           AS relam,
    CAST(0 AS INT)                                           AS relfilenode,
    CAST(0 AS INT)                                           AS reltablespace,
    CAST(0 AS INT)                                           AS relpages,
    CAST(COALESCE(t.reltuples, 0.0) AS REAL)                 AS reltuples,
    CAST(0 AS INT)                                           AS relallvisible,
    CAST(0 AS INT)                                           AS reltoastrelid,
    false                                                    AS relhasindex,
    false                                                    AS relisshared,
    'p'                                                      AS relpersistence,
    'r'                                                      AS relkind,
    CAST((SELECT COUNT(*) FROM glare_catalog.columns cc
          WHERE cc.table_oid = t.oid) AS SMALLINT)           AS relnatts,
    CAST(0 AS SMALLINT)                                      AS relchecks,
    false                                                    AS relhasrules,
    false                                                    AS relhastriggers,
    false                                                    AS relhassubclass,
    false                                                    AS relrowsecurity,
    false                                                    AS relforcerowsecurity,
    true                                                     AS relispopulated,
    'd'                                                      AS relreplident,
    false                                                    AS relispartition,
    CAST(0 AS INT)                                           AS relrewrite,
    CAST(0 AS BIGINT)                                        AS relfrozenxid,
    CAST(0 AS BIGINT)                                        AS relminmxid,
    CAST(NULL AS TEXT)                                       AS relacl,
    CAST(NULL AS TEXT)                                       AS reloptions,
    CAST(NULL AS TEXT)                                       AS relpartbound
FROM glare_catalog.tables t
UNION ALL
SELECT
    v.oid                                                    AS oid,
    v.view_name                                              AS relname,
    CASE v.schema_oid WHEN 16388 THEN 11 WHEN 16386 THEN 2200
         ELSE v.schema_oid END                               AS relnamespace,
    CAST(0 AS INT)                                           AS reltype,
    CAST(0 AS INT)                                           AS reloftype,
    CAST(10 AS INT)                                          AS relowner,
    CAST(0 AS INT)                                           AS relam,
    CAST(0 AS INT)                                           AS relfilenode,
    CAST(0 AS INT)                                           AS reltablespace,
    CAST(0 AS INT)                                           AS relpages,
    CAST(0.0 AS REAL)                                        AS reltuples,
    CAST(0 AS INT)                                           AS relallvisible,
    CAST(0 AS INT)                                           AS reltoastrelid,
    false                                                    AS relhasindex,
    false                                                    AS relisshared,
    'p'                                                      AS relpersistence,
    'v'                                                      AS relkind,
    CAST(0 AS SMALLINT)                                      AS relnatts,
    CAST(0 AS SMALLINT)                                      AS relchecks,
    false                                                    AS relhasrules,
    false                                                    AS relhastriggers,
    false                                                    AS relhassubclass,
    false                                                    AS relrowsecurity,
    false                                                    AS relforcerowsecurity,
    true                                                     AS relispopulated,
    'd'                                                      AS relreplident,
    false                                                    AS relispartition,
    CAST(0 AS INT)                                           AS relrewrite,
    CAST(0 AS BIGINT)                                        AS relfrozenxid,
    CAST(0 AS BIGINT)                                        AS relminmxid,
    CAST(NULL AS TEXT)                                       AS relacl,
    CAST(NULL AS TEXT)                                       AS reloptions,
    CAST(NULL AS TEXT)                                       AS relpartbound
FROM glare_catalog.views v",
});

pub static PG_NAMESPACE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_namespace",
    // Translate well-known schemas to their upstream-canonical OIDs so
    // that any client code hardcoding `pg_catalog.oid = 11` /
    // `public.oid = 2200` (psycopg3 TypeInfo, asyncpg `_TYPEINFO`,
    // PgJDBC `getMaxNameLength`, every DBeaver introspection probe)
    // can JOIN against pg_namespace by OID and find a match.
    // `pg_type.typnamespace` is already hardcoded to 11 for built-in
    // types — without this translation that FK breaks. Internal
    // storage in `glare_catalog.schemas.oid` keeps the 16385-16389
    // range; only the public-facing view does the renumber.
    sql: "
SELECT
    CASE s.schema_name
        WHEN 'pg_catalog' THEN 11
        WHEN 'public' THEN 2200
        ELSE s.oid
    END                    AS oid,
    s.schema_name          AS nspname,
    CAST(10 AS INT)        AS nspowner,
    CAST(NULL AS TEXT)     AS nspacl
FROM glare_catalog.schemas s",
});

pub static PG_DESCRIPTION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_description",
    // Sourced from `glare_catalog.tables.comment` and
    // `glare_catalog.views.comment`. `classoid` is the OID of the catalog
    // class that owns the row — `pg_class` for both tables and views;
    // 1259 is the well-known upstream OID for `pg_class`. `objsubid=0`
    // means "the relation itself, not a column" (column comments would
    // use the column ordinal here once column-level comments land).
    sql: "
SELECT
    t.oid                  AS objoid,
    CAST(1259 AS INT)      AS classoid,
    CAST(0 AS INT)         AS objsubid,
    t.comment              AS description
FROM glare_catalog.tables t
WHERE t.comment IS NOT NULL
UNION ALL
SELECT
    v.oid                  AS objoid,
    CAST(1259 AS INT)      AS classoid,
    CAST(0 AS INT)         AS objsubid,
    v.comment              AS description
FROM glare_catalog.views v
WHERE v.comment IS NOT NULL",
});

pub static PG_SHDESCRIPTION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_shdescription",
    sql: "
SELECT
    CAST(NULL AS INT)  AS objoid,
    CAST(NULL AS INT)  AS classoid,
    CAST(NULL AS TEXT) AS description
FROM (VALUES (1)) WHERE false",
});

pub static PG_DATABASE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_database",
    // First arm enumerates `glare_catalog.databases` (the builtin
    // 'default' database plus any `CREATE EXTERNAL DATABASE` rows).
    // Second arm synthesizes a row for the connection-time database
    // name returned by `current_database()` so that
    // `SELECT * FROM pg_database WHERE datname = current_database()`
    // is non-empty — JDBC, asyncpg's introspection, and DBeaver's
    // navigator all rely on this. The `WHERE … NOT IN` predicate
    // dedupes when current_database() = 'default'.
    sql: "
SELECT
    oid                              AS oid,
    database_name                    AS datname,
    CAST(10 AS INT)                  AS datdba,
    CAST(6 AS INT)                   AS encoding,
    'c'                              AS datlocprovider,
    false                            AS datistemplate,
    true                             AS datallowconn,
    CAST(-1 AS INT)                  AS datconnlimit,
    CAST(0 AS BIGINT)                AS datfrozenxid,
    CAST(0 AS BIGINT)                AS datminmxid,
    CAST(1663 AS INT)                AS dattablespace,
    'en_US.UTF-8'                    AS datcollate,
    'en_US.UTF-8'                    AS datctype,
    CAST(NULL AS TEXT)               AS daticulocal,
    -- `daticurules` (PG 16+) — ICU locale rules. NULL when the
    -- collation provider is not ICU (we use libc).
    CAST(NULL AS TEXT)               AS daticurules,
    CAST(NULL AS TEXT)               AS datcollversion,
    CAST(NULL AS TEXT)               AS datacl
FROM glare_catalog.databases
UNION ALL
SELECT
    CAST(0 AS INT)                   AS oid,
    current_database()               AS datname,
    CAST(10 AS INT)                  AS datdba,
    CAST(6 AS INT)                   AS encoding,
    'c'                              AS datlocprovider,
    false                            AS datistemplate,
    true                             AS datallowconn,
    CAST(-1 AS INT)                  AS datconnlimit,
    CAST(0 AS BIGINT)                AS datfrozenxid,
    CAST(0 AS BIGINT)                AS datminmxid,
    CAST(1663 AS INT)                AS dattablespace,
    'en_US.UTF-8'                    AS datcollate,
    'en_US.UTF-8'                    AS datctype,
    CAST(NULL AS TEXT)               AS daticulocal,
    -- `daticurules` (PG 16+) — ICU locale rules. NULL when the
    -- collation provider is not ICU (we use libc).
    CAST(NULL AS TEXT)               AS daticurules,
    CAST(NULL AS TEXT)               AS datcollversion,
    CAST(NULL AS TEXT)               AS datacl
WHERE current_database() NOT IN
      (SELECT database_name FROM glare_catalog.databases)",
});

pub static PG_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_tables",
    sql: "
SELECT
    schema_name        AS schemaname,
    table_name         AS tablename,
    CAST('' AS TEXT)   AS tableowner,
    CAST('' AS TEXT)   AS tablespace,
    false              AS hasindexes,
    false              AS hasrules,
    false              AS hastriggers,
    false              AS rowsecurity
FROM glare_catalog.tables",
});

pub static PG_VIEWS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_views",
    sql: "
SELECT
    schema_name        AS schemaname,
    view_name          AS viewname,
    CAST('' AS TEXT)   AS viewowner,
    sql                AS definition
FROM glare_catalog.views",
});

pub static PG_TYPE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_type",
    // Hand-curated subset of upstream PostgreSQL types covering every Arrow
    // datatype we surface plus the OID/regclass/array helpers DBeaver
    // requires. The OID values match upstream Postgres exactly so
    // `'pg_class'::regclass` (oid 1259, looked up via the regclass cast UDF
    // in W2-funcs) and `INT4OID = 23` invariants hold.
    //
    // The full lookup table lives in `pgrepr::pg_type_oid::PG_TYPES`; this
    // SQL view is a hand-mirror suitable for SQL queries. Keep them aligned.
    //
    // Full PG 16 column shape — the IO/subscript/handler OIDs (typinput,
    // typoutput, typreceive, typsend, typmodin, typmodout, typanalyze,
    // typsubscript) point at C-language handler procs in real PG. We
    // emit `0` / NULL for them since GlareDB has no per-type C handlers;
    // DBeaver checks "is the OID nonzero?" rather than calling them.
    sql: "
SELECT t.*,
    CAST(0 AS INT)     AS typsubscript,
    CAST(0 AS INT)     AS typinput,
    CAST(0 AS INT)     AS typoutput,
    CAST(0 AS INT)     AS typreceive,
    CAST(0 AS INT)     AS typsend,
    CAST(0 AS INT)     AS typmodin,
    CAST(0 AS INT)     AS typmodout,
    CAST(0 AS INT)     AS typanalyze,
    CAST(0 AS INT)     AS typndims,
    CAST(NULL AS TEXT) AS typdefaultbin,
    CAST(NULL AS TEXT) AS typacl
FROM (
SELECT * FROM (VALUES
    -- (oid, typname, typnamespace, typlen, typbyval, typtype, typcategory, typispreferred, typisdefined, typdelim, typrelid, typelem, typarray, typalign, typstorage, typnotnull, typbasetype, typtypmod, typowner, typdefault, typcollation)
    (CAST(16 AS INT),   'bool',         CAST(11 AS INT), CAST(1 AS SMALLINT),  true,  'b', 'B', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1000 AS INT), 'c', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(17 AS INT),   'bytea',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'U', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1001 AS INT), 'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(18 AS INT),   'char',         CAST(11 AS INT), CAST(1 AS SMALLINT),  true,  'b', 'S', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1002 AS INT), 'c', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(19 AS INT),   'name',         CAST(11 AS INT), CAST(64 AS SMALLINT), false, 'b', 'S', false, true, ',', CAST(0 AS INT), CAST(18 AS INT),   CAST(1003 AS INT), 'c', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(950 AS INT)),
    (CAST(20 AS INT),   'int8',         CAST(11 AS INT), CAST(8 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1016 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(21 AS INT),   'int2',         CAST(11 AS INT), CAST(2 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1005 AS INT), 's', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(23 AS INT),   'int4',         CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1007 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(25 AS INT),   'text',         CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'S', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1009 AS INT), 'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(26 AS INT),   'oid',          CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'N', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1028 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(114 AS INT),  'json',         CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'U', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(199 AS INT),  'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(700 AS INT),  'float4',       CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1021 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(701 AS INT),  'float8',       CAST(11 AS INT), CAST(8 AS SMALLINT),  true,  'b', 'N', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1022 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1042 AS INT), 'bpchar',       CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'S', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1014 AS INT), 'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(1043 AS INT), 'varchar',      CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'S', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1015 AS INT), 'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(1082 AS INT), 'date',         CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'D', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1182 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1083 AS INT), 'time',         CAST(11 AS INT), CAST(8 AS SMALLINT),  true,  'b', 'D', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1183 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1114 AS INT), 'timestamp',    CAST(11 AS INT), CAST(8 AS SMALLINT),  true,  'b', 'D', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1115 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1184 AS INT), 'timestamptz',  CAST(11 AS INT), CAST(8 AS SMALLINT),  true,  'b', 'D', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1185 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1186 AS INT), 'interval',     CAST(11 AS INT), CAST(16 AS SMALLINT), false, 'b', 'T', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1187 AS INT), 'd', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1700 AS INT), 'numeric',      CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(1231 AS INT), 'i', 'm', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2205 AS INT), 'regclass',     CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(2210 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2206 AS INT), 'regtype',      CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'b', 'N', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(2211 AS INT), 'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2249 AS INT), 'record',       CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'p', 'P', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(2287 AS INT), 'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2278 AS INT), 'void',         CAST(11 AS INT), CAST(4 AS SMALLINT),  true,  'p', 'P', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(0 AS INT),    'i', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2950 AS INT), 'uuid',         CAST(11 AS INT), CAST(16 AS SMALLINT), false, 'b', 'U', false, true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(2951 AS INT), 'c', 'p', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(3802 AS INT), 'jsonb',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'U', true,  true, ',', CAST(0 AS INT), CAST(0 AS INT),    CAST(3807 AS INT), 'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    -- arrays
    (CAST(1000 AS INT), '_bool',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(16 AS INT),   CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1005 AS INT), '_int2',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(21 AS INT),   CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1007 AS INT), '_int4',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(23 AS INT),   CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1009 AS INT), '_text',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(25 AS INT),   CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(1014 AS INT), '_bpchar',      CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1042 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(1015 AS INT), '_varchar',     CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1043 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(100 AS INT)),
    (CAST(1016 AS INT), '_int8',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(20 AS INT),   CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1021 AS INT), '_float4',      CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(700 AS INT),  CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1022 AS INT), '_float8',      CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(701 AS INT),  CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1115 AS INT), '_timestamp',   CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1114 AS INT), CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1185 AS INT), '_timestamptz', CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1184 AS INT), CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1182 AS INT), '_date',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1082 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1183 AS INT), '_time',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1083 AS INT), CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1187 AS INT), '_interval',    CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1186 AS INT), CAST(0 AS INT),    'd', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1231 AS INT), '_numeric',     CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(1700 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(1028 AS INT), '_oid',         CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(26 AS INT),   CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(199 AS INT),  '_json',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(114 AS INT),  CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(2951 AS INT), '_uuid',        CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(2950 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT)),
    (CAST(3807 AS INT), '_jsonb',       CAST(11 AS INT), CAST(-1 AS SMALLINT), false, 'b', 'A', false, true, ',', CAST(0 AS INT), CAST(3802 AS INT), CAST(0 AS INT),    'i', 'x', false, CAST(0 AS INT), CAST(-1 AS INT), CAST(10 AS INT), CAST(NULL AS TEXT), CAST(0 AS INT))
) AS t(oid, typname, typnamespace, typlen, typbyval, typtype, typcategory, typispreferred, typisdefined, typdelim, typrelid, typelem, typarray, typalign, typstorage, typnotnull, typbasetype, typtypmod, typowner, typdefault, typcollation)
) AS t",
});

pub static PG_PROC: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_proc",
    // Functions tree node + Dependencies tab in DBeaver. Backed by
    // `glare_catalog.functions`. `prorettype` and `proargtypes` will become
    // structured OIDs once W2-funcs ships `pg_type_oid_by_name` /
    // `pg_type_oid_vector` UDFs; until then they remain 0 / empty and
    // DBeaver renders the row with a `<unknown>` return type — still better
    // than not showing the function at all.
    sql: "
SELECT
    f.oid                                      AS oid,
    f.function_name                            AS proname,
    CASE f.schema_oid WHEN 16388 THEN 11 WHEN 16386 THEN 2200
         ELSE f.schema_oid END                  AS pronamespace,
    CAST(10 AS INT)                            AS proowner,
    CAST(12 AS INT)                            AS prolang,
    CAST(0.0 AS REAL)                          AS procost,
    CAST(0.0 AS REAL)                          AS prorows,
    CAST(0 AS INT)                             AS provariadic,
    CAST(0 AS INT)                             AS prosupport,
    CASE f.function_type
        WHEN 'aggregate' THEN 'a'
        ELSE                  'f'
    END                                        AS prokind,
    f.function_type = 'aggregate'              AS proisagg,
    false                                      AS proiswindow,
    false                                      AS prosecdef,
    false                                      AS proleakproof,
    false                                      AS proisstrict,
    f.is_set_returning                         AS proretset,
    'v'                                        AS provolatile,
    's'                                        AS proparallel,
    CAST(COALESCE(cardinality(f.argument_types), 0) AS SMALLINT) AS pronargs,
    CAST(0 AS SMALLINT)                        AS pronargdefaults,
    -- `prorettype` is NULL when the function does not declare a return
    -- type at registration (DataFusion `ScalarUDF`s defer to planning) —
    -- fall back to OID 0 (`InvalidOid`) which DBeaver renders as
    -- `<unknown>`. When a return type is recorded, resolve via
    -- `pg_type_oid_by_name`.
    COALESCE(
        pg_catalog.pg_type_oid_by_name(f.return_type),
        CAST(0 AS INT)
    )                                          AS prorettype,
    -- Pre-formatted oidvector wire string from
    -- `glare_catalog.functions.argument_oids_text` (populated at
    -- dispatch time via `arrow_name_to_pg_oid`). PgJDBC's
    -- `getFunctionColumns` runs `StringTokenizer` over this column
    -- and yields one parameter row per token.
    f.argument_oids_text                       AS proargtypes,
    -- `proallargtypes` mirrors `proargtypes` since we don't
    -- distinguish OUT/INOUT parameters today.
    f.argument_oids_text                       AS proallargtypes,
    -- `prosqlbody` (PG 16+) — SQL function body for SQL-LANGUAGE
    -- functions; NULL for the C/internal-language functions we ship.
    CAST(NULL AS TEXT)                         AS prosqlbody,
    CAST(NULL AS TEXT)                         AS proargmodes,
    -- proargnames is `text[]` in real Postgres — argument *names*
    -- (NULL when the function uses positional-only args). The source
    -- column `glare_catalog.functions.parameters` actually stores
    -- type-signature strings (`Int8/Int16/...`), not parameter names,
    -- so emitting them here would be a lie that confuses introspection
    -- tools. Real Postgres returns NULL for builtins without recorded
    -- argument names; we do the same. (When DataFusion eventually
    -- exposes real argument names on `ScalarUDF`, swap this for the
    -- proper text-array projection.)
    CAST(NULL AS TEXT)                         AS proargnames,
    CAST(NULL AS TEXT)                         AS proargdefaults,
    CAST(NULL AS TEXT)                         AS protrftypes,
    COALESCE(f.example, '')                    AS prosrc,
    CAST(NULL AS TEXT)                         AS probin,
    CAST(NULL AS TEXT)                         AS proconfig,
    CAST(NULL AS TEXT)                         AS proacl
FROM glare_catalog.functions f",
});

pub static PG_AGGREGATE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_aggregate",
    // Slim view over `pg_proc` for aggregate functions only. Transition-
    // function metadata is intentionally NULL/0 — DBeaver renders the
    // function but skips internal aggregate details.
    sql: "
SELECT
    f.oid                          AS aggfnoid,
    'n'                            AS aggkind,
    CAST(0 AS SMALLINT)            AS aggnumdirectargs,
    CAST(0 AS INT)                 AS aggtransfn,
    CAST(0 AS INT)                 AS aggfinalfn,
    CAST(0 AS INT)                 AS aggcombinefn,
    CAST(0 AS INT)                 AS aggserialfn,
    CAST(0 AS INT)                 AS aggdeserialfn,
    CAST(0 AS INT)                 AS aggmtransfn,
    CAST(0 AS INT)                 AS aggminvtransfn,
    CAST(0 AS INT)                 AS aggmfinalfn,
    false                          AS aggfinalextra,
    false                          AS aggmfinalextra,
    'r'                            AS aggfinalmodify,
    'r'                            AS aggmfinalmodify,
    CAST(0 AS INT)                 AS aggsortop,
    CAST(0 AS INT)                 AS aggtranstype,
    CAST(0 AS INT)                 AS aggtransspace,
    CAST(0 AS INT)                 AS aggmtranstype,
    CAST(0 AS INT)                 AS aggmtransspace,
    CAST(NULL AS TEXT)             AS agginitval,
    CAST(NULL AS TEXT)             AS aggminitval
FROM glare_catalog.functions f
WHERE f.function_type = 'aggregate'",
});

pub static PG_ATTRDEF: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_attrdef",
    sql: "
SELECT
    CAST(NULL AS INT)      AS oid,
    CAST(NULL AS INT)      AS adrelid,
    CAST(NULL AS SMALLINT) AS adnum,
    CAST(NULL AS TEXT)     AS adbin,
    CAST(NULL AS TEXT)     AS adsrc
FROM (VALUES (1)) WHERE false",
});

pub static PG_INDEX: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_index",
    // `indkey`, `indoption`, `indclass` are `int2vector` / `oidvector`
    // upstream. Until we have a native int2vector Arrow type, project
    // them as space-separated text:
    //   - `indkey` carries the real per-column ordinals from
    //     `glare_catalog.indexes.column_positions`.
    //   - `indoption` is the per-key option bitmap; default `0` =
    //     `ASC NULLS LAST`. We don't carry per-key options today, so
    //     synthesize a `0` for each ordinal. Same idea for `indclass`
    //     (per-key op-class oid; we don't have op-class metadata).
    //
    // Real PG drivers (PgJDBC `getIndexInfo`) wrap
    // `information_schema._pg_expandarray(i.indkey)` around `indkey`
    // and bitwise-AND `indoption[ord-1] & 1::smallint` for the ASC/
    // DESC bit. The text form satisfies the SRF contract; the
    // bitwise step is cosmetic and we report ASC NULLS LAST.
    sql: "
SELECT
    i.oid               AS indexrelid,
    i.table_oid         AS indrelid,
    CAST(0 AS SMALLINT) AS indnatts,
    CAST(0 AS SMALLINT) AS indnkeyatts,
    i.is_unique         AS indisunique,
    -- `indnullsnotdistinct` (PG 15+) — column position 6 in upstream
    -- pg_index.h. Always false; we don't model UNIQUE NULL semantics.
    false               AS indnullsnotdistinct,
    i.is_primary        AS indisprimary,
    false               AS indisexclusion,
    false               AS indimmediate,
    false               AS indisclustered,
    true                AS indisvalid,
    false               AS indcheckxmin,
    true                AS indisready,
    true                AS indislive,
    false               AS indisreplident,
    COALESCE(i.column_positions, '')                            AS indkey,
    CAST(NULL AS TEXT)                                          AS indcollation,
    -- Per-key op-class oid stub; one '0' per indkey ordinal. The
    -- 'g' flag is critical — the 3-arg form replaces only the first
    -- match, which would corrupt multi-key vectors (`'1 17 42'` →
    -- `'0 17 42'` instead of `'0 0 0'`).
    regexp_replace(COALESCE(i.column_positions, ''), '[0-9]+', '0', 'g') AS indclass,
    -- Per-key option bitmap stub; '0' = ASC NULLS LAST.
    regexp_replace(COALESCE(i.column_positions, ''), '[0-9]+', '0', 'g') AS indoption,
    i.expression                                                AS indexprs,
    i.predicate                                                 AS indpred
FROM glare_catalog.indexes i",
});

pub static PG_CONSTRAINT: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_constraint",
    // Pass through raw `glare_catalog.constraints` columns. Today the table
    // is always empty (Delta tables have no PK/FK enforcement), so `NULL`
    // values are fine — DBeaver's `ConstraintCache` only inspects the
    // column shape via a `WHERE 1<>1` probe at startup.
    sql: "
SELECT
    c.oid                       AS oid,
    c.conname                   AS conname,
    CASE c.schema_oid WHEN 16388 THEN 11 WHEN 16386 THEN 2200
         ELSE c.schema_oid END   AS connamespace,
    c.contype                   AS contype,
    c.is_deferrable             AS condeferrable,
    c.is_deferred               AS condeferred,
    c.is_validated              AS convalidated,
    c.table_oid                 AS conrelid,
    CAST(0 AS INT)              AS contypid,
    CAST(0 AS INT)              AS conindid,
    CAST(0 AS INT)              AS conparentid,
    c.ref_table_oid             AS confrelid,
    c.update_action             AS confupdtype,
    c.delete_action             AS confdeltype,
    c.match_type                AS confmatchtype,
    true                        AS conislocal,
    CAST(0 AS INT)              AS coninhcount,
    true                        AS connoinherit,
    -- `conkey` / `confkey` are `int2[]` upstream, formatted as the
    -- Postgres array literal `{1,2,3}` on the wire. Convert from our
    -- internal space-separated `column_positions` ('1 2 3') by
    -- swapping spaces for commas and wrapping in braces. NULL-safe.
    CASE WHEN c.column_positions IS NULL THEN NULL
         ELSE '{' || replace(c.column_positions, ' ', ',') || '}'
    END                         AS conkey,
    CASE WHEN c.ref_column_positions IS NULL THEN NULL
         ELSE '{' || replace(c.ref_column_positions, ' ', ',') || '}'
    END                         AS confkey,
    CAST(NULL AS TEXT)          AS conpfeqop,
    CAST(NULL AS TEXT)          AS conppeqop,
    CAST(NULL AS TEXT)          AS conffeqop,
    CAST(NULL AS TEXT)          AS confdelsetcols,
    CAST(NULL AS TEXT)          AS conexclop,
    c.definition                AS conbin
FROM glare_catalog.constraints c",
});

pub static PG_INHERITS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_inherits",
    sql: "
SELECT
    CAST(NULL AS INT)      AS inhrelid,
    CAST(NULL AS INT)      AS inhparent,
    CAST(NULL AS INT)      AS inhseqno,
    CAST(NULL AS BOOLEAN)  AS inhdetachpending
FROM (VALUES (1)) WHERE false",
});

pub static PG_DEPEND: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_depend",
    sql: "
SELECT
    CAST(NULL AS INT)  AS classid,
    CAST(NULL AS INT)  AS objid,
    CAST(NULL AS INT)  AS objsubid,
    CAST(NULL AS INT)  AS refclassid,
    CAST(NULL AS INT)  AS refobjid,
    CAST(NULL AS INT)  AS refobjsubid,
    CAST(NULL AS TEXT) AS deptype
FROM (VALUES (1)) WHERE false",
});

pub static PG_REWRITE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_rewrite",
    sql: "
SELECT
    CAST(NULL AS INT)      AS oid,
    CAST(NULL AS TEXT)     AS rulename,
    CAST(NULL AS INT)      AS ev_class,
    CAST(NULL AS TEXT)     AS ev_type,
    CAST(NULL AS TEXT)     AS ev_enabled,
    CAST(NULL AS BOOLEAN)  AS is_instead,
    CAST(NULL AS TEXT)     AS ev_qual,
    CAST(NULL AS TEXT)     AS ev_action
FROM (VALUES (1)) WHERE false",
});

pub static PG_TRIGGER: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_trigger",
    sql: "
SELECT
    CAST(NULL AS INT)      AS oid,
    CAST(NULL AS INT)      AS tgrelid,
    CAST(NULL AS INT)      AS tgparentid,
    CAST(NULL AS TEXT)     AS tgname,
    CAST(NULL AS INT)      AS tgfoid,
    CAST(NULL AS SMALLINT) AS tgtype,
    CAST(NULL AS TEXT)     AS tgenabled,
    CAST(NULL AS BOOLEAN)  AS tgisinternal,
    CAST(NULL AS INT)      AS tgconstrrelid,
    CAST(NULL AS INT)      AS tgconstrindid,
    CAST(NULL AS INT)      AS tgconstraint,
    CAST(NULL AS BOOLEAN)  AS tgdeferrable,
    CAST(NULL AS BOOLEAN)  AS tginitdeferred,
    CAST(NULL AS SMALLINT) AS tgnargs,
    CAST(NULL AS TEXT)     AS tgattr,
    CAST(NULL AS TEXT)     AS tgargs,
    CAST(NULL AS TEXT)     AS tgqual,
    CAST(NULL AS TEXT)     AS tgoldtable,
    CAST(NULL AS TEXT)     AS tgnewtable
FROM (VALUES (1)) WHERE false",
});

pub static PG_ENUM: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_enum",
    sql: "
SELECT
    CAST(NULL AS INT)   AS oid,
    CAST(NULL AS INT)   AS enumtypid,
    CAST(NULL AS REAL)  AS enumsortorder,
    CAST(NULL AS TEXT)  AS enumlabel
FROM (VALUES (1)) WHERE false",
});

pub static PG_ROLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_roles",
    // Single hardcoded admin role — GlareDB does not have multi-user RBAC.
    // DBeaver populates the Roles tree from this view; one row keeps the
    // tree from breaking.
    sql: "
SELECT * FROM (VALUES
    (CAST(10 AS INT), 'glaredb', true, true, true, true, true, true, false, CAST(-1 AS INT), CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT))
) AS r(oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, rolvaliduntil, rolconfig)",
});

pub static PG_AUTHID: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_authid",
    sql: "
SELECT * FROM (VALUES
    (CAST(10 AS INT), 'glaredb', true, true, true, true, true, true, false, CAST(-1 AS INT), CAST(NULL AS TEXT), CAST(NULL AS TEXT))
) AS a(oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, rolvaliduntil)",
});

pub static PG_COLLATION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_collation",
    // `collicurules` added in PG 16 — ICU locale-rule overrides. NULL
    // for our libc-collations.
    sql: "
SELECT * FROM (VALUES
    (CAST(100 AS INT), 'default', CAST(11 AS INT), CAST(10 AS INT), 'd', true, CAST(-1 AS INT), CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT)),
    (CAST(950 AS INT), 'C',       CAST(11 AS INT), CAST(10 AS INT), 'c', true, CAST(-1 AS INT), 'C', 'C', CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT)),
    (CAST(951 AS INT), 'POSIX',   CAST(11 AS INT), CAST(10 AS INT), 'c', true, CAST(-1 AS INT), 'POSIX', 'POSIX', CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT))
) AS c(oid, collname, collnamespace, collowner, collprovider, collisdeterministic, collencoding, collcollate, collctype, colliculocale, collicurules, collversion)",
});

pub static PG_LANGUAGE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_language",
    sql: "
SELECT * FROM (VALUES
    (CAST(12 AS INT), 'internal', CAST(10 AS INT), false, false, CAST(0 AS INT), CAST(0 AS INT), CAST(2246 AS INT), CAST(NULL AS TEXT)),
    (CAST(13 AS INT), 'c',        CAST(10 AS INT), false, false, CAST(0 AS INT), CAST(0 AS INT), CAST(2247 AS INT), CAST(NULL AS TEXT)),
    (CAST(14 AS INT), 'sql',      CAST(10 AS INT), false, true,  CAST(0 AS INT), CAST(0 AS INT), CAST(2248 AS INT), CAST(NULL AS TEXT))
) AS l(oid, lanname, lanowner, lanispl, lanpltrusted, lanplcallfoid, laninline, lanvalidator, lanacl)",
});

pub static PG_TABLESPACE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_tablespace",
    sql: "
SELECT * FROM (VALUES
    (CAST(1663 AS INT), 'pg_default', CAST(10 AS INT), CAST(NULL AS TEXT), CAST(NULL AS TEXT)),
    (CAST(1664 AS INT), 'pg_global',  CAST(10 AS INT), CAST(NULL AS TEXT), CAST(NULL AS TEXT))
) AS t(oid, spcname, spcowner, spcacl, spcoptions)",
});

pub static PG_SETTINGS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_settings",
    // Backed by `glare_catalog.session_vars`, which the dispatcher
    // populates from `SessionVarsInner::entries()` at request time.
    // DBeaver / pgcli / `SHOW ALL` all read this; the prior empty-stub
    // body left them blank.
    sql: "
SELECT
    name                   AS name,
    setting                AS setting,
    CAST(NULL AS TEXT)     AS unit,
    CAST(NULL AS TEXT)     AS category,
    short_desc             AS short_desc,
    CAST(NULL AS TEXT)     AS extra_desc,
    'user'                 AS context,
    'string'               AS vartype,
    'session'              AS source,
    CAST(NULL AS TEXT)     AS min_val,
    CAST(NULL AS TEXT)     AS max_val,
    CAST(NULL AS TEXT)     AS enumvals,
    setting                AS boot_val,
    setting                AS reset_val,
    CAST(NULL AS TEXT)     AS sourcefile,
    CAST(NULL AS INT)      AS sourceline,
    false                  AS pending_restart
FROM glare_catalog.session_vars
UNION ALL
-- `max_index_keys` — read by PgJDBC `getMaxIndexKeys` on every
-- `Connection.getMetaData()` call. Without this row the call logs a
-- SQLException at WARN. Real PG hardcodes this to 32 in pg_settings.
SELECT
    'max_index_keys'       AS name,
    '32'                   AS setting,
    CAST(NULL AS TEXT)     AS unit,
    'Preset Options'       AS category,
    'Shows the maximum number of index keys.' AS short_desc,
    CAST(NULL AS TEXT)     AS extra_desc,
    'internal'             AS context,
    'integer'              AS vartype,
    'default'              AS source,
    '32'                   AS min_val,
    '32'                   AS max_val,
    CAST(NULL AS TEXT)     AS enumvals,
    '32'                   AS boot_val,
    '32'                   AS reset_val,
    CAST(NULL AS TEXT)     AS sourcefile,
    CAST(NULL AS INT)      AS sourceline,
    false                  AS pending_restart",
});

pub static PG_EXTENSION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_extension",
    sql: "
SELECT
    CAST(NULL AS INT)     AS oid,
    CAST(NULL AS TEXT)    AS extname,
    CAST(NULL AS INT)     AS extowner,
    CAST(NULL AS INT)     AS extnamespace,
    CAST(NULL AS BOOLEAN) AS extrelocatable,
    CAST(NULL AS TEXT)    AS extversion,
    CAST(NULL AS TEXT)    AS extconfig,
    CAST(NULL AS TEXT)    AS extcondition
FROM (VALUES (1)) WHERE false",
});

pub static PG_AVAILABLE_EXTENSIONS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_available_extensions",
    sql: "
SELECT
    CAST(NULL AS TEXT) AS name,
    CAST(NULL AS TEXT) AS default_version,
    CAST(NULL AS TEXT) AS installed_version,
    CAST(NULL AS TEXT) AS comment
FROM (VALUES (1)) WHERE false",
});

pub static PG_EVENT_TRIGGER: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_event_trigger",
    sql: "
SELECT
    CAST(NULL AS INT)  AS oid,
    CAST(NULL AS TEXT) AS evtname,
    CAST(NULL AS TEXT) AS evtevent,
    CAST(NULL AS INT)  AS evtowner,
    CAST(NULL AS INT)  AS evtfoid,
    CAST(NULL AS TEXT) AS evtenabled,
    CAST(NULL AS TEXT) AS evttags
FROM (VALUES (1)) WHERE false",
});

pub static PG_DEFAULT_ACL: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_default_acl",
    sql: "
SELECT
    CAST(NULL AS INT)  AS oid,
    CAST(NULL AS INT)  AS defaclrole,
    CAST(NULL AS INT)  AS defaclnamespace,
    CAST(NULL AS TEXT) AS defaclobjtype,
    CAST(NULL AS TEXT) AS defaclacl
FROM (VALUES (1)) WHERE false",
});

pub static PG_FOREIGN_DATA_WRAPPER: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_foreign_data_wrapper",
    sql: "
SELECT
    CAST(NULL AS INT)  AS oid,
    CAST(NULL AS TEXT) AS fdwname,
    CAST(NULL AS INT)  AS fdwowner,
    CAST(NULL AS INT)  AS fdwhandler,
    CAST(NULL AS INT)  AS fdwvalidator,
    CAST(NULL AS TEXT) AS fdwacl,
    CAST(NULL AS TEXT) AS fdwoptions
FROM (VALUES (1)) WHERE false",
});

pub static PG_FOREIGN_SERVER: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_foreign_server",
    sql: "
SELECT
    CAST(NULL AS INT)  AS oid,
    CAST(NULL AS TEXT) AS srvname,
    CAST(NULL AS INT)  AS srvowner,
    CAST(NULL AS INT)  AS srvfdw,
    CAST(NULL AS TEXT) AS srvtype,
    CAST(NULL AS TEXT) AS srvversion,
    CAST(NULL AS TEXT) AS srvacl,
    CAST(NULL AS TEXT) AS srvoptions
FROM (VALUES (1)) WHERE false",
});

pub static PG_CONVERSION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_conversion",
    // Single UTF8↔UTF8 conversion entry — keeps DBeaver's
    // `EncodingCache` populated. Encoding 6 is `UTF8`.
    sql: "
SELECT * FROM (VALUES
    (CAST(0 AS INT), 'utf8_to_utf8', CAST(11 AS INT), CAST(10 AS INT), CAST(6 AS INT), CAST(6 AS INT), CAST(0 AS INT), true)
) AS c(oid, conname, connamespace, conowner, conforencoding, contoencoding, conproc, condefault)",
});

pub static PG_MATVIEWS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_matviews",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS matviewname,
    CAST(NULL AS TEXT)    AS matviewowner,
    CAST(NULL AS TEXT)    AS tablespace,
    CAST(NULL AS BOOLEAN) AS hasindexes,
    CAST(NULL AS BOOLEAN) AS ispopulated,
    CAST(NULL AS TEXT)    AS definition
FROM (VALUES (1)) WHERE false",
});

pub static PG_REPLICATION_SLOTS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_replication_slots",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS slot_name,
    CAST(NULL AS TEXT)    AS plugin,
    CAST(NULL AS TEXT)    AS slot_type,
    CAST(NULL AS INT)     AS datoid,
    CAST(NULL AS TEXT)    AS database,
    CAST(NULL AS BOOLEAN) AS temporary,
    CAST(NULL AS BOOLEAN) AS active,
    CAST(NULL AS INT)     AS active_pid,
    CAST(NULL AS BIGINT)  AS xmin,
    CAST(NULL AS BIGINT)  AS catalog_xmin,
    CAST(NULL AS TEXT)    AS restart_lsn,
    CAST(NULL AS TEXT)    AS confirmed_flush_lsn,
    CAST(NULL AS TEXT)    AS wal_status,
    CAST(NULL AS BIGINT)  AS safe_wal_size
FROM (VALUES (1)) WHERE false",
});

pub static PG_PUBLICATION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_publication",
    sql: "
SELECT
    CAST(NULL AS INT)     AS oid,
    CAST(NULL AS TEXT)    AS pubname,
    CAST(NULL AS INT)     AS pubowner,
    CAST(NULL AS BOOLEAN) AS puballtables,
    CAST(NULL AS BOOLEAN) AS pubinsert,
    CAST(NULL AS BOOLEAN) AS pubupdate,
    CAST(NULL AS BOOLEAN) AS pubdelete,
    CAST(NULL AS BOOLEAN) AS pubtruncate,
    CAST(NULL AS BOOLEAN) AS pubviaroot
FROM (VALUES (1)) WHERE false",
});

pub static PG_RANGE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_range",
    // Empty stub. asyncpg's `_TYPEINFO` LEFT JOINs `pg_range` on every
    // `Connection.fetch()` against an unfamiliar OID; without this view
    // the JOIN aborts with `relation "pg_range" does not exist`,
    // cascading into "could not introspect type" errors on any
    // composite/domain column. We have no real range types, so an
    // empty 7-col rowset is the spec-correct shape.
    //
    // `rngmultitypid` is the PG 14+ multirange-type pointer; the 7-col
    // shape here matches PG 14/15/16 catalog/pg_range.h.
    sql: "
SELECT
    CAST(NULL AS INT)  AS rngtypid,
    CAST(NULL AS INT)  AS rngsubtype,
    CAST(NULL AS INT)  AS rngmultitypid,
    CAST(NULL AS INT)  AS rngcollation,
    CAST(NULL AS INT)  AS rngsubopc,
    CAST(NULL AS TEXT) AS rngcanonical,
    CAST(NULL AS TEXT) AS rngsubdiff
FROM (VALUES (1)) WHERE false",
});

// =============================================================================
// New views shipped in the Postgres-spec compliance pass (Phase 4 / A8).
//
// HIGH PRIORITY — drivers fail or log SQLException without these:
//   pg_indexes        — DBeaver Index tab
//   pg_locks          — pgAdmin connect-time
//   pg_stat_activity  — pgAdmin Dashboard
//   pg_user / pg_shadow / pg_group — PgJDBC `getUserName`
//   pg_stats          — DBeaver Statistics tab
//
// MEDIUM PRIORITY — empty-stub forms; psql `\d+`, pgAdmin Dashboard,
// ad-hoc tooling probe these and silently disable features when missing:
//   pg_stat_user_tables, pg_stat_user_indexes, pg_statio_user_tables,
//   pg_publication_tables, pg_partitioned_table, pg_policy,
//   pg_seclabel, pg_shseclabel, pg_init_privs, pg_largeobject,
//   pg_largeobject_metadata, pg_timezone_names, pg_timezone_abbrevs,
//   pg_prepared_statements, pg_cursors, pg_user_mappings, pg_rules,
//   pg_policies, pg_sequences.
// =============================================================================

/// `pg_indexes` — joins pg_index ↔ pg_class ↔ pg_namespace ↔
/// pg_tablespace. DBeaver's Index tab issues `SELECT *` against this.
/// Today `glare_catalog.indexes` is empty, so the join is empty too,
/// but the column shape is the contract.
pub static PG_INDEXES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_indexes",
    sql: "
SELECT
    n.nspname                       AS schemaname,
    c.relname                       AS tablename,
    ic.relname                      AS indexname,
    CAST(NULL AS TEXT)              AS tablespace,
    pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c  ON c.oid  = i.indrelid
JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace",
});

/// `pg_locks` — empty stub. pgAdmin / DataGrip issue `SELECT *` at
/// connect; column shape per PG 16 catalog/lock.c output.
pub static PG_LOCKS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_locks",
    sql: "
SELECT
    CAST(NULL AS TEXT)        AS locktype,
    CAST(NULL AS INT)         AS database,
    CAST(NULL AS INT)         AS relation,
    CAST(NULL AS INT)         AS page,
    CAST(NULL AS SMALLINT)    AS tuple,
    CAST(NULL AS TEXT)        AS virtualxid,
    CAST(NULL AS BIGINT)      AS transactionid,
    CAST(NULL AS INT)         AS classid,
    CAST(NULL AS INT)         AS objid,
    CAST(NULL AS SMALLINT)    AS objsubid,
    CAST(NULL AS TEXT)        AS virtualtransaction,
    CAST(NULL AS INT)         AS pid,
    CAST(NULL AS TEXT)        AS mode,
    CAST(NULL AS BOOLEAN)     AS granted,
    CAST(NULL AS BOOLEAN)     AS fastpath,
    CAST(NULL AS TIMESTAMP)   AS waitstart
FROM (VALUES (1)) WHERE false",
});

/// `pg_stat_activity` — single-row stub for the current session.
/// pgAdmin's Dashboard breaks if this is empty.
pub static PG_STAT_ACTIVITY: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_stat_activity",
    sql: "
SELECT
    CAST(0 AS INT)            AS datid,
    current_database()        AS datname,
    CAST(0 AS INT)            AS pid,
    CAST(NULL AS INT)         AS leader_pid,
    CAST(10 AS INT)           AS usesysid,
    'glaredb'                 AS usename,
    CAST(NULL AS TEXT)        AS application_name,
    CAST(NULL AS TEXT)        AS client_addr,
    CAST(NULL AS TEXT)        AS client_hostname,
    CAST(NULL AS INT)         AS client_port,
    CAST(NULL AS TIMESTAMP)   AS backend_start,
    CAST(NULL AS TIMESTAMP)   AS xact_start,
    CAST(NULL AS TIMESTAMP)   AS query_start,
    CAST(NULL AS TIMESTAMP)   AS state_change,
    CAST(NULL AS TEXT)        AS wait_event_type,
    CAST(NULL AS TEXT)        AS wait_event,
    'active'                  AS state,
    CAST(NULL AS BIGINT)      AS backend_xid,
    CAST(NULL AS BIGINT)      AS backend_xmin,
    CAST(NULL AS BIGINT)      AS query_id,
    CAST('' AS TEXT)          AS query,
    'client backend'          AS backend_type",
});

/// `pg_user` — view over `pg_authid` (per upstream system_views.sql).
/// PgJDBC's `DatabaseMetaData.getUserName()` issues
/// `SELECT usename FROM pg_user WHERE usesysid = …`.
pub static PG_USER: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_user",
    sql: "
SELECT
    rolname                AS usename,
    oid                    AS usesysid,
    rolcreatedb            AS usecreatedb,
    rolsuper               AS usesuper,
    rolreplication         AS userepl,
    rolbypassrls           AS usebypassrls,
    CAST('********' AS TEXT) AS passwd,
    CAST(NULL AS TIMESTAMP) AS valuntil,
    CAST(NULL AS TEXT)     AS useconfig
FROM pg_catalog.pg_authid
WHERE rolcanlogin = true",
});

/// `pg_shadow` — same shape as pg_user, restricted to login roles.
/// Mirrors upstream system_views.sql.
pub static PG_SHADOW: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_shadow",
    sql: "
SELECT
    rolname                AS usename,
    oid                    AS usesysid,
    rolcreatedb            AS usecreatedb,
    rolsuper               AS usesuper,
    rolreplication         AS userepl,
    rolbypassrls           AS usebypassrls,
    CAST('********' AS TEXT) AS passwd,
    CAST(NULL AS TIMESTAMP) AS valuntil,
    CAST(NULL AS TEXT)     AS useconfig
FROM pg_catalog.pg_authid
WHERE rolcanlogin = true",
});

/// `pg_group` — view over `pg_authid` for non-login roles. Empty in
/// practice (we only ship one role).
pub static PG_GROUP: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_group",
    sql: "
SELECT
    rolname                AS groname,
    oid                    AS grosysid,
    CAST(NULL AS TEXT)     AS grolist
FROM pg_catalog.pg_authid
WHERE rolcanlogin = false",
});

/// `pg_stats` — empty stub. DBeaver's Statistics tab issues
/// `SELECT *` here when expanding a column-level view.
pub static PG_STATS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_stats",
    sql: "
SELECT
    CAST(NULL AS TEXT)     AS schemaname,
    CAST(NULL AS TEXT)     AS tablename,
    CAST(NULL AS TEXT)     AS attname,
    CAST(NULL AS BOOLEAN)  AS inherited,
    CAST(NULL AS REAL)     AS null_frac,
    CAST(NULL AS INT)      AS avg_width,
    CAST(NULL AS REAL)     AS n_distinct,
    CAST(NULL AS TEXT)     AS most_common_vals,
    CAST(NULL AS TEXT)     AS most_common_freqs,
    CAST(NULL AS TEXT)     AS histogram_bounds,
    CAST(NULL AS REAL)     AS correlation,
    CAST(NULL AS TEXT)     AS most_common_elems,
    CAST(NULL AS TEXT)     AS most_common_elem_freqs,
    CAST(NULL AS TEXT)     AS elem_count_histogram
FROM (VALUES (1)) WHERE false",
});

// ---------- Medium priority — empty stubs ----------

pub static PG_STAT_USER_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_stat_user_tables",
    sql: "
SELECT
    CAST(NULL AS INT)     AS relid,
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS relname,
    CAST(NULL AS BIGINT)  AS seq_scan,
    CAST(NULL AS BIGINT)  AS seq_tup_read,
    CAST(NULL AS BIGINT)  AS idx_scan,
    CAST(NULL AS BIGINT)  AS idx_tup_fetch,
    CAST(NULL AS BIGINT)  AS n_tup_ins,
    CAST(NULL AS BIGINT)  AS n_tup_upd,
    CAST(NULL AS BIGINT)  AS n_tup_del,
    CAST(NULL AS BIGINT)  AS n_tup_hot_upd,
    CAST(NULL AS BIGINT)  AS n_live_tup,
    CAST(NULL AS BIGINT)  AS n_dead_tup,
    CAST(NULL AS BIGINT)  AS n_mod_since_analyze,
    CAST(NULL AS BIGINT)  AS n_ins_since_vacuum,
    CAST(NULL AS TIMESTAMP) AS last_vacuum,
    CAST(NULL AS TIMESTAMP) AS last_autovacuum,
    CAST(NULL AS TIMESTAMP) AS last_analyze,
    CAST(NULL AS TIMESTAMP) AS last_autoanalyze,
    CAST(NULL AS BIGINT)  AS vacuum_count,
    CAST(NULL AS BIGINT)  AS autovacuum_count,
    CAST(NULL AS BIGINT)  AS analyze_count,
    CAST(NULL AS BIGINT)  AS autoanalyze_count
FROM (VALUES (1)) WHERE false",
});

pub static PG_STAT_USER_INDEXES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_stat_user_indexes",
    sql: "
SELECT
    CAST(NULL AS INT)     AS relid,
    CAST(NULL AS INT)     AS indexrelid,
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS relname,
    CAST(NULL AS TEXT)    AS indexrelname,
    CAST(NULL AS BIGINT)  AS idx_scan,
    CAST(NULL AS BIGINT)  AS idx_tup_read,
    CAST(NULL AS BIGINT)  AS idx_tup_fetch
FROM (VALUES (1)) WHERE false",
});

pub static PG_STATIO_USER_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_statio_user_tables",
    sql: "
SELECT
    CAST(NULL AS INT)     AS relid,
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS relname,
    CAST(NULL AS BIGINT)  AS heap_blks_read,
    CAST(NULL AS BIGINT)  AS heap_blks_hit,
    CAST(NULL AS BIGINT)  AS idx_blks_read,
    CAST(NULL AS BIGINT)  AS idx_blks_hit,
    CAST(NULL AS BIGINT)  AS toast_blks_read,
    CAST(NULL AS BIGINT)  AS toast_blks_hit,
    CAST(NULL AS BIGINT)  AS tidx_blks_read,
    CAST(NULL AS BIGINT)  AS tidx_blks_hit
FROM (VALUES (1)) WHERE false",
});

pub static PG_PUBLICATION_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_publication_tables",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS pubname,
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS tablename,
    CAST(NULL AS TEXT)    AS attnames,
    CAST(NULL AS TEXT)    AS rowfilter
FROM (VALUES (1)) WHERE false",
});

pub static PG_PARTITIONED_TABLE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_partitioned_table",
    sql: "
SELECT
    CAST(NULL AS INT)     AS partrelid,
    CAST(NULL AS TEXT)    AS partstrat,
    CAST(NULL AS SMALLINT) AS partnatts,
    CAST(NULL AS INT)     AS partdefid,
    CAST(NULL AS TEXT)    AS partattrs,
    CAST(NULL AS TEXT)    AS partclass,
    CAST(NULL AS TEXT)    AS partcollation,
    CAST(NULL AS TEXT)    AS partexprs
FROM (VALUES (1)) WHERE false",
});

pub static PG_POLICY: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_policy",
    sql: "
SELECT
    CAST(NULL AS INT)     AS oid,
    CAST(NULL AS TEXT)    AS polname,
    CAST(NULL AS INT)     AS polrelid,
    CAST(NULL AS TEXT)    AS polcmd,
    CAST(NULL AS BOOLEAN) AS polpermissive,
    CAST(NULL AS TEXT)    AS polroles,
    CAST(NULL AS TEXT)    AS polqual,
    CAST(NULL AS TEXT)    AS polwithcheck
FROM (VALUES (1)) WHERE false",
});

pub static PG_POLICIES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_policies",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS tablename,
    CAST(NULL AS TEXT)    AS policyname,
    CAST(NULL AS TEXT)    AS permissive,
    CAST(NULL AS TEXT)    AS roles,
    CAST(NULL AS TEXT)    AS cmd,
    CAST(NULL AS TEXT)    AS qual,
    CAST(NULL AS TEXT)    AS with_check
FROM (VALUES (1)) WHERE false",
});

pub static PG_SECLABEL: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_seclabel",
    sql: "
SELECT
    CAST(NULL AS INT)     AS objoid,
    CAST(NULL AS INT)     AS classoid,
    CAST(NULL AS INT)     AS objsubid,
    CAST(NULL AS TEXT)    AS provider,
    CAST(NULL AS TEXT)    AS label
FROM (VALUES (1)) WHERE false",
});

pub static PG_SHSECLABEL: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_shseclabel",
    sql: "
SELECT
    CAST(NULL AS INT)     AS objoid,
    CAST(NULL AS INT)     AS classoid,
    CAST(NULL AS TEXT)    AS provider,
    CAST(NULL AS TEXT)    AS label
FROM (VALUES (1)) WHERE false",
});

pub static PG_INIT_PRIVS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_init_privs",
    sql: "
SELECT
    CAST(NULL AS INT)     AS objoid,
    CAST(NULL AS INT)     AS classoid,
    CAST(NULL AS INT)     AS objsubid,
    CAST(NULL AS TEXT)    AS privtype,
    CAST(NULL AS TEXT)    AS initprivs
FROM (VALUES (1)) WHERE false",
});

pub static PG_LARGEOBJECT: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_largeobject",
    sql: "
SELECT
    CAST(NULL AS INT)     AS loid,
    CAST(NULL AS INT)     AS pageno,
    CAST(NULL AS TEXT)    AS data
FROM (VALUES (1)) WHERE false",
});

pub static PG_LARGEOBJECT_METADATA: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_largeobject_metadata",
    sql: "
SELECT
    CAST(NULL AS INT)     AS oid,
    CAST(NULL AS INT)     AS lomowner,
    CAST(NULL AS TEXT)    AS lomacl
FROM (VALUES (1)) WHERE false",
});

pub static PG_TIMEZONE_NAMES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_timezone_names",
    // DBeaver Settings → Time Zone picker. Empty stub fine.
    sql: "
SELECT
    CAST(NULL AS TEXT)     AS name,
    CAST(NULL AS TEXT)     AS abbrev,
    CAST(NULL AS INTERVAL) AS utc_offset,
    CAST(NULL AS BOOLEAN)  AS is_dst
FROM (VALUES (1)) WHERE false",
});

pub static PG_TIMEZONE_ABBREVS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_timezone_abbrevs",
    sql: "
SELECT
    CAST(NULL AS TEXT)     AS abbrev,
    CAST(NULL AS INTERVAL) AS utc_offset,
    CAST(NULL AS BOOLEAN)  AS is_dst
FROM (VALUES (1)) WHERE false",
});

pub static PG_PREPARED_STATEMENTS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_prepared_statements",
    sql: "
SELECT
    CAST(NULL AS TEXT)      AS name,
    CAST(NULL AS TEXT)      AS statement,
    CAST(NULL AS TIMESTAMP) AS prepare_time,
    CAST(NULL AS TEXT)      AS parameter_types,
    CAST(NULL AS BOOLEAN)   AS from_sql,
    CAST(NULL AS BIGINT)    AS generic_plans,
    CAST(NULL AS BIGINT)    AS custom_plans
FROM (VALUES (1)) WHERE false",
});

pub static PG_CURSORS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_cursors",
    sql: "
SELECT
    CAST(NULL AS TEXT)      AS name,
    CAST(NULL AS TEXT)      AS statement,
    CAST(NULL AS BOOLEAN)   AS is_holdable,
    CAST(NULL AS BOOLEAN)   AS is_binary,
    CAST(NULL AS BOOLEAN)   AS is_scrollable,
    CAST(NULL AS TIMESTAMP) AS creation_time
FROM (VALUES (1)) WHERE false",
});

pub static PG_USER_MAPPINGS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_user_mappings",
    sql: "
SELECT
    CAST(NULL AS INT)     AS umid,
    CAST(NULL AS INT)     AS srvid,
    CAST(NULL AS TEXT)    AS srvname,
    CAST(NULL AS INT)     AS umuser,
    CAST(NULL AS TEXT)    AS usename,
    CAST(NULL AS TEXT)    AS umoptions
FROM (VALUES (1)) WHERE false",
});

pub static PG_RULES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_rules",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS tablename,
    CAST(NULL AS TEXT)    AS rulename,
    CAST(NULL AS TEXT)    AS definition
FROM (VALUES (1)) WHERE false",
});

pub static PG_SEQUENCES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_sequences",
    sql: "
SELECT
    CAST(NULL AS TEXT)    AS schemaname,
    CAST(NULL AS TEXT)    AS sequencename,
    CAST(NULL AS TEXT)    AS sequenceowner,
    CAST(NULL AS TEXT)    AS data_type,
    CAST(NULL AS BIGINT)  AS start_value,
    CAST(NULL AS BIGINT)  AS min_value,
    CAST(NULL AS BIGINT)  AS max_value,
    CAST(NULL AS BIGINT)  AS increment_by,
    CAST(NULL AS BOOLEAN) AS cycle,
    CAST(NULL AS BIGINT)  AS cache_size,
    CAST(NULL AS BIGINT)  AS last_value
FROM (VALUES (1)) WHERE false",
});

pub static PG_SUBSCRIPTION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_subscription",
    sql: "
SELECT
    CAST(NULL AS INT)     AS oid,
    CAST(NULL AS INT)     AS subdbid,
    CAST(NULL AS TEXT)    AS subname,
    CAST(NULL AS INT)     AS subowner,
    CAST(NULL AS BOOLEAN) AS subenabled,
    CAST(NULL AS TEXT)    AS subconninfo,
    CAST(NULL AS TEXT)    AS subslotname,
    CAST(NULL AS TEXT)    AS subsynccommit,
    CAST(NULL AS INT)     AS subpublications
FROM (VALUES (1)) WHERE false",
});

impl BuiltinView {
    pub fn builtins() -> Vec<&'static BuiltinView> {
        vec![
            &GLARE_EXTERNAL_DATASOURCES,
            &INFORMATION_SCHEMA_SCHEMATA,
            &INFORMATION_SCHEMA_TABLES,
            &INFORMATION_SCHEMA_COLUMNS,
            // pg_catalog views — see DBeaver corpus comment above each one.
            &PG_AM,
            &PG_ATTRIBUTE,
            &PG_ATTRDEF,
            &PG_AUTHID,
            &PG_AVAILABLE_EXTENSIONS,
            &PG_AGGREGATE,
            &PG_CLASS,
            &PG_COLLATION,
            &PG_CONSTRAINT,
            &PG_CONVERSION,
            &PG_DATABASE,
            &PG_DEFAULT_ACL,
            &PG_DEPEND,
            &PG_DESCRIPTION,
            &PG_ENUM,
            &PG_EVENT_TRIGGER,
            &PG_EXTENSION,
            &PG_FOREIGN_DATA_WRAPPER,
            &PG_FOREIGN_SERVER,
            &PG_INDEX,
            &PG_INHERITS,
            &PG_LANGUAGE,
            &PG_MATVIEWS,
            &PG_NAMESPACE,
            &PG_PARTITIONED_TABLE,
            &PG_PROC,
            &PG_CURSORS,
            &PG_GROUP,
            &PG_INDEXES,
            &PG_INIT_PRIVS,
            &PG_LARGEOBJECT,
            &PG_LARGEOBJECT_METADATA,
            &PG_LOCKS,
            &PG_POLICIES,
            &PG_POLICY,
            &PG_PREPARED_STATEMENTS,
            &PG_PUBLICATION,
            &PG_PUBLICATION_TABLES,
            &PG_RANGE,
            &PG_REPLICATION_SLOTS,
            &PG_REWRITE,
            &PG_ROLES,
            &PG_RULES,
            &PG_SECLABEL,
            &PG_SEQUENCES,
            &PG_SETTINGS,
            &PG_SHADOW,
            &PG_SHDESCRIPTION,
            &PG_SHSECLABEL,
            &PG_STAT_ACTIVITY,
            &PG_STAT_USER_INDEXES,
            &PG_STAT_USER_TABLES,
            &PG_STATIO_USER_TABLES,
            &PG_STATS,
            &PG_SUBSCRIPTION,
            &PG_TABLES,
            &PG_TABLESPACE,
            &PG_TIMEZONE_ABBREVS,
            &PG_TIMEZONE_NAMES,
            &PG_TRIGGER,
            &PG_TYPE,
            &PG_USER,
            &PG_USER_MAPPINGS,
            &PG_VIEWS,
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn builtin_schema_oid_range() {
        let mut oids = HashSet::new();
        for schema in BuiltinSchema::builtins() {
            assert!(schema.oid < FIRST_NON_STATIC_OID);
            assert!(schema.oid > FIRST_GLAREDB_BUILTIN_ID);
            assert!(schema.oid >= 16385);
            assert!(schema.oid <= 16400);
            assert!(oids.insert(schema.oid), "duplicate oid: {}", schema.oid);
        }
    }

    #[test]
    fn builtin_table_oid_range() {
        let mut oids = HashSet::new();
        for schema in BuiltinTable::builtins() {
            assert!(schema.oid < FIRST_NON_STATIC_OID);
            assert!(schema.oid >= FIRST_GLAREDB_BUILTIN_ID);
            assert!(oids.insert(schema.oid), "duplicate oid: {}", schema.oid);
        }
    }

    #[test]
    fn builtin_unique_schema_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinSchema::builtins() {
            assert!(names.insert(builtin.name.to_string()))
        }
    }

    #[test]
    fn builtin_unique_view_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinView::builtins() {
            let name = format!("{}.{}", builtin.schema, builtin.name);
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }

    #[test]
    fn builtin_unique_table_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinTable::builtins() {
            let name = format!("{}.{}", builtin.schema, builtin.name);
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }
}
