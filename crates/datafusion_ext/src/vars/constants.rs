use pgrepr::compatible::server_version;
use pgrepr::notice::NoticeSeverity;

use super::{Dialect, Lazy, ServerVar, ToOwned, Uuid};

pub(super) const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: "server_version",
    value: server_version(),
    group: "postgres",
    user_configurable: false,
    description: "Version of the server",
};

/// Numeric form of `server_version`. JDBC / DBeaver branch on this
/// (`server_version_num >= 90100`, `>= 140000`, etc.) to enable
/// version-specific code paths (partitioning, generated columns, etc.).
/// Reported alongside `server_version` in the startup ParameterStatus.
pub(super) const SERVER_VERSION_NUM: ServerVar<str> = ServerVar {
    name: "server_version_num",
    // Mirror `pgrepr::compatible::server_version()` (currently 15.1).
    // Encoded as MMmm00 — 15.1 → 150001 — matching upstream PostgreSQL's
    // `server_version_num` GUC format.
    value: "150001",
    group: "postgres",
    user_configurable: false,
    description: "Numeric form of server_version (MMmm00).",
};

pub(super) const SERVER_ENCODING: ServerVar<str> = ServerVar {
    name: "server_encoding",
    value: "UTF8",
    group: "postgres",
    user_configurable: false,
    description: "Server-side text encoding",
};

pub(super) const IS_SUPERUSER: ServerVar<str> = ServerVar {
    name: "is_superuser",
    value: "off",
    group: "postgres",
    user_configurable: false,
    description: "Whether the current session user is a superuser (always off — GlareDB has no privilege model).",
};

pub(super) const SESSION_AUTHORIZATION: ServerVar<str> = ServerVar {
    name: "session_authorization",
    value: "glaredb",
    group: "postgres",
    user_configurable: true,
    description: "Session authorization role name (no enforcement)",
};

pub(super) const INTERVAL_STYLE: ServerVar<str> = ServerVar {
    name: "IntervalStyle",
    value: "postgres",
    group: "postgres",
    user_configurable: true,
    description: "Interval display style",
};

pub(super) const INTEGER_DATETIMES: ServerVar<str> = ServerVar {
    name: "integer_datetimes",
    value: "on",
    group: "postgres",
    user_configurable: false,
    description: "Whether timestamps use 64-bit integer representation (always on).",
};

pub(super) const APPLICATION_NAME: ServerVar<str> = ServerVar {
    name: "application_name",
    value: "",
    group: "postgres",
    user_configurable: true,
    description: "Name of the application",
};

pub(super) const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: "client_encoding",
    value: "UTF8",
    group: "postgres",
    user_configurable: true,
    description: "Encoding of the client",
};

pub(super) const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: "extra_float_digits",
    value: &1,
    group: "postgres",
    user_configurable: true,
    description: "Extra precision in float values",
};

pub(super) const STATEMENT_TIMEOUT: ServerVar<i32> = ServerVar {
    name: "statement_timeout",
    value: &0,
    group: "postgres",
    user_configurable: true,
    description: "Statement timeout in milliseconds",
};

pub(super) const TIMEZONE: ServerVar<str> = ServerVar {
    name: "TimeZone",
    value: "UTC",
    group: "postgres",
    user_configurable: true,
    description: "Timezone of the client, default UTC",
};

pub(super) const DATESTYLE: ServerVar<str> = ServerVar {
    name: "DateStyle",
    // Postgres default — `<output style>, <day-month order>`. DBeaver and
    // PgJDBC parse this string at startup; bare `ISO` confused some
    // versions, the full `ISO, MDY` form is what real Postgres reports.
    value: "ISO, MDY",
    group: "postgres",
    user_configurable: true,
    description: "Date style of the client, default 'ISO, MDY'",
};

pub(super) const TRANSACTION_ISOLATION: ServerVar<str> = ServerVar {
    name: "transaction_isolation",
    value: "read uncommitted",
    group: "postgres",
    user_configurable: false,
    description: "Transaction isolation level, defaults to 'read uncommitted'",
};

pub(super) static DEFAULT_SEARCH_PATH: Lazy<[String; 1]> = Lazy::new(|| ["public".to_owned()]);
pub(super) static SEARCH_PATH: Lazy<ServerVar<[String]>> = Lazy::new(|| ServerVar {
    name: "search_path",
    value: &*DEFAULT_SEARCH_PATH,
    group: "postgres",
    user_configurable: true,
    description: "Search path for schemas",
});

pub(super) const CLIENT_MIN_MESSAGES: ServerVar<NoticeSeverity> = ServerVar {
    name: "client_min_messages",
    value: &NoticeSeverity::Notice,
    group: "postgres",
    user_configurable: true,
    description: "Controls which messages are sent to the client, defaults NOTICE",
};

pub(super) const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: "standard_conforming_strings",
    value: &true,
    group: "postgres",
    user_configurable: false,
    description: "Treat backslashes literally in string literals",
};

pub(super) static GLAREDB_VERSION_OWNED: Lazy<String> =
    Lazy::new(|| format!("v{}", env!("CARGO_PKG_VERSION")));
pub(super) static GLAREDB_VERSION: Lazy<ServerVar<str>> = Lazy::new(|| ServerVar {
    name: "glaredb_version",
    value: &GLAREDB_VERSION_OWNED,
    group: "glaredb",
    user_configurable: false,
    description: "Version of glaredb",
});

pub(super) const ENABLE_DEBUG_DATASOURCES: ServerVar<bool> = ServerVar {
    name: "enable_debug_datasources",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "Enable debug datasources",
};

pub(super) const FORCE_CATALOG_REFRESH: ServerVar<bool> = ServerVar {
    name: "force_catalog_refresh",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "Force catalog refresh",
};

pub(super) const DATABASE_ID: ServerVar<Uuid> = ServerVar {
    name: "database_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "Database ID",
};

pub(super) const CONNECTION_ID: ServerVar<Uuid> = ServerVar {
    name: "connection_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "Connection ID",
};

pub(super) const REMOTE_SESSION_ID: ServerVar<Option<Uuid>> = ServerVar {
    name: "remote_session_id",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Session ID on remote service.",
};

pub(super) const USER_ID: ServerVar<Uuid> = ServerVar {
    name: "user_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "User ID",
};

pub(super) const USER_NAME: ServerVar<str> = ServerVar {
    name: "user_name",
    value: "",
    group: "glaredb",
    user_configurable: false,
    description: "User name",
};

pub(super) const DATABASE_NAME: ServerVar<str> = ServerVar {
    name: "database_name",
    value: "",
    group: "glaredb",
    user_configurable: false,
    description: "Database name",
};

pub(super) const MAX_DATASOURCE_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_datasource_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max datasource count",
};

pub(super) const MEMORY_LIMIT_BYTES: ServerVar<Option<usize>> = ServerVar {
    name: "memory_limit_bytes",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Memory limit in bytes",
};

pub(super) const MAX_TUNNEL_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_tunnel_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max tunnel count",
};

pub(super) const MAX_CREDENTIALS_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_credentials_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max credentials allowed",
};

pub(super) const IS_CLOUD_INSTANCE: ServerVar<bool> = ServerVar {
    name: "is_cloud_instance",
    value: &false,
    group: "glaredb",
    user_configurable: false,
    description: "Determines if the server is local or cloud",
};

pub(super) const DIALECT: ServerVar<Dialect> = ServerVar {
    name: "dialect",
    value: &Dialect::Sql,
    group: "glaredb",
    user_configurable: true,
    description: "Dialect of the sql engine",
};

pub(super) const ENABLE_EXPERIMENTAL_SCHEDULER: ServerVar<bool> = ServerVar {
    name: "enable_experimental_scheduler",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "If the experimental query scheduler should be enabled",
};

/// Note that these are not normally shown in the search path.
pub(super) const IMPLICIT_SCHEMAS: [&str; 2] = [
    POSTGRES_SCHEMA,
    // Objects stored in current session will always have a priority over the
    // schemas in search path.
    CURRENT_SESSION_SCHEMA,
];

pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// Schema to store temporary objects (only valid for current session).
pub const CURRENT_SESSION_SCHEMA: &str = "current_session";
