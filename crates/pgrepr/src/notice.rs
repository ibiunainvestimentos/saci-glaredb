use std::fmt;
use std::str::FromStr;

use crate::error::PgReprError;

/// 'SQLSTATE' error codes.
///
/// Drivers and tools (DBeaver, JDBC, asyncpg, libpq) branch on these to
/// distinguish "table not found" from "auth failure" from "operator timeout"
/// — coarse `XX000` everywhere makes them treat every error as a fatal
/// internal one and abort retries / reconnect strategies.
///
/// See the complete list at
/// <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlState {
    // Class 00 — Successful Completion
    Successful,

    // Class 01 — Warning
    Warning,

    // Class 0A — Feature Not Supported
    FeatureNotSupported,

    // Class 22 — Data Exception
    /// 22P02 — invalid text representation (e.g. malformed timestamp string).
    InvalidTextRepresentation,
    /// 22008 — datetime field overflow.
    DatetimeFieldOverflow,
    /// 22023 — invalid parameter value.
    InvalidParameterValue,

    // Class 25 — Invalid Transaction State
    /// 25P02 — current transaction is aborted, commands ignored until end.
    InFailedTransaction,

    // Class 28 — Invalid Authorization Specification
    /// 28P01 — invalid password.
    InvalidPassword,
    /// 28000 — invalid authorization specification.
    InvalidAuth,

    // Class 3D — Invalid Catalog Name
    /// 3D000 — database does not exist.
    DatabaseDoesNotExist,

    // Class 3F — Invalid Schema Name
    /// 3F000 — schema does not exist.
    SchemaDoesNotExist,

    // Class 42 — Syntax Error or Access Rule Violation
    /// 42601 — syntax error.
    SyntaxError,
    /// 42P01 — undefined table / view / matview / index.
    UndefinedTable,
    /// 42703 — undefined column.
    UndefinedColumn,
    /// 42883 — undefined function (also covers undefined operator).
    UndefinedFunction,
    /// 42704 — undefined object (catch-all for missing schema-qualified
    /// references).
    UndefinedObject,
    /// 42P02 — undefined parameter.
    UndefinedParameter,
    /// 42501 — insufficient privilege.
    InsufficientPrivilege,
    /// 42P07 — duplicate table.
    DuplicateTable,

    // Class 53 — Insufficient Resources
    /// 53300 — too many connections.
    TooManyConnections,
    /// 53400 — configuration limit exceeded.
    ConfigurationLimitExceeded,

    // Class 57 — Operator Intervention
    /// 57014 — query canceled (used by `pg_cancel_backend` / statement timeout).
    QueryCanceled,
    /// 57P01 — admin shutdown.
    AdminShutdown,

    // Class XX — Internal Error
    InternalError,
}

impl SqlState {
    pub fn as_code_str(&self) -> &'static str {
        match self {
            SqlState::Successful => "00000",
            SqlState::Warning => "01000",
            SqlState::FeatureNotSupported => "0A000",
            SqlState::InvalidTextRepresentation => "22P02",
            SqlState::DatetimeFieldOverflow => "22008",
            SqlState::InvalidParameterValue => "22023",
            SqlState::InFailedTransaction => "25P02",
            SqlState::InvalidPassword => "28P01",
            SqlState::InvalidAuth => "28000",
            SqlState::DatabaseDoesNotExist => "3D000",
            SqlState::SchemaDoesNotExist => "3F000",
            SqlState::SyntaxError => "42601",
            SqlState::UndefinedTable => "42P01",
            SqlState::UndefinedColumn => "42703",
            SqlState::UndefinedFunction => "42883",
            SqlState::UndefinedObject => "42704",
            SqlState::UndefinedParameter => "42P02",
            SqlState::InsufficientPrivilege => "42501",
            SqlState::DuplicateTable => "42P07",
            SqlState::TooManyConnections => "53300",
            SqlState::ConfigurationLimitExceeded => "53400",
            SqlState::QueryCanceled => "57014",
            SqlState::AdminShutdown => "57P01",
            SqlState::InternalError => "XX000",
        }
    }
}

/// Indicates severity of notice.
///
/// These must remain in order to allow us to easily test if a message should be
/// sent back to the client based on a configured session var. For example, if
/// someone sets that var to WARNING, we can just check severity >= WARNING.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NoticeSeverity {
    Debug5,
    Debug4,
    Debug3,
    Debug2,
    Debug1,
    Log,
    Notice,
    Warning,
    Error,
    Info,
}

impl NoticeSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            NoticeSeverity::Debug5 => "DEBUG5",
            NoticeSeverity::Debug4 => "DEBUG4",
            NoticeSeverity::Debug3 => "DEBUG3",
            NoticeSeverity::Debug2 => "DEBUG2",
            NoticeSeverity::Debug1 => "DEBUG1",
            NoticeSeverity::Log => "LOG",
            NoticeSeverity::Notice => "NOTICE",
            NoticeSeverity::Warning => "WARNING",
            NoticeSeverity::Error => "ERROR",
            NoticeSeverity::Info => "INFO",
        }
    }
}

impl FromStr for NoticeSeverity {
    type Err = PgReprError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "DEBUG5" => Self::Debug5,
            "DEBUG4" => Self::Debug4,
            "DEBUG3" => Self::Debug3,
            "DEBUG2" => Self::Debug2,
            "DEBUG1" => Self::Debug1,
            "LOG" => Self::Log,
            "NOTICE" => Self::Notice,
            "WARNING" => Self::Warning,
            "ERROR" => Self::Error,
            "INFO" => Self::Info,
            other => {
                return Err(PgReprError::String(format!(
                    "unknown notice severity: {other}"
                )))
            }
        })
    }
}

impl fmt::Display for NoticeSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A notice that should be displayed to the user.
#[derive(Debug, Clone)]
pub struct Notice {
    pub severity: NoticeSeverity,
    pub code: SqlState,
    pub message: String,
}

impl Notice {
    pub fn info(msg: impl Into<String>) -> Notice {
        Notice {
            severity: NoticeSeverity::Info,
            code: SqlState::Successful,
            message: msg.into(),
        }
    }

    pub fn warning(msg: impl Into<String>) -> Notice {
        Notice {
            severity: NoticeSeverity::Warning,
            code: SqlState::Warning,
            message: msg.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notice_severity_to_from_string() {
        fn assert_to_from(sev: NoticeSeverity) {
            assert_eq!(sev, NoticeSeverity::from_str(sev.as_str()).unwrap())
        }

        assert_to_from(NoticeSeverity::Debug5);
        assert_to_from(NoticeSeverity::Debug4);
        assert_to_from(NoticeSeverity::Debug3);
        assert_to_from(NoticeSeverity::Debug2);
        assert_to_from(NoticeSeverity::Debug1);
        assert_to_from(NoticeSeverity::Log);
        assert_to_from(NoticeSeverity::Notice);
        assert_to_from(NoticeSeverity::Warning);
        assert_to_from(NoticeSeverity::Error);
        assert_to_from(NoticeSeverity::Info);
    }
}
