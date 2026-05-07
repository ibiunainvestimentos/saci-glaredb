use std::collections::HashMap;

use datafusion::arrow::record_batch::RecordBatch;
use pgrepr::error::PgReprError;
use pgrepr::format::Format;
use sqlexec::errors::ExecError;
use tokio_postgres::types::Type as PgType;

use crate::errors::{PgSrvError, Result};

/// Version number (v3.0) used during normal frontend startup.
pub const VERSION_V3: i32 = 0x30000;
/// Version number used to request a cancellation.
pub const VERSION_CANCEL: i32 = (1234 << 16) ^ 5678;
/// Version number used to request an SSL connection.
pub const VERSION_SSL: i32 = (1234 << 16) ^ 5679;

/// Messages sent by the frontend during connection startup.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum StartupMessage {
    SSLRequest {
        version: i32,
    },
    /// Postgres cancel request — sent over a *separate* TCP connection to
    /// kill an in-flight query on another connection. The payload is the
    /// `(process_id, secret_key)` pair that the server sent in the
    /// `BackendKeyData` message at connect time.
    CancelRequest {
        version: i32,
        process_id: i32,
        secret_key: i32,
    },
    StartupRequest {
        version: i32,
        params: HashMap<String, String>,
    },
}

/// Messages sent by the frontend.
#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// A query (or queries) to execute.
    Query { sql: String },
    /// An encrypted or unencrypted password.
    PasswordMessage { password: String },
    /// An extended query parse message.
    Parse {
        /// The name of the prepared statement. An empty string denotes the
        /// unnamed prepared statement.
        name: String,
        /// The query string to be parsed.
        sql: String,
        /// The object IDs of the parameter data types. Placing a zero here is
        /// equivalent to leaving the type unspecified.
        param_types: Vec<i32>,
    },
    Bind {
        /// The name of the destination portal (an empty string selects the
        /// unnamed portal).
        portal: String,
        /// The name of the source prepared statement (an empty string selects
        /// the unnamed prepared statement).
        statement: String,
        /// The parameter format codes. Each must presently be zero (text) or
        /// one (binary).
        ///
        /// Valid lengths may be:
        /// 0 -> Use default format for all inputs (text)
        /// 1 -> Use this one format for all inputs
        /// n -> Individually specified formats for each input.
        param_formats: Vec<Format>,
        /// The parameter values, in the format indicated by the associated
        /// format code. n is the above length.
        param_values: Vec<Option<Vec<u8>>>,
        /// The result-column format codes. Each must presently be zero (text)
        /// or one (binary).
        ///
        /// Valid lengths may be:
        /// 0 -> Use default format for all outputs (text)
        /// 1 -> Use this one format for all outputs
        /// n -> Individually specified formats for each output.
        result_formats: Vec<Format>,
    },
    Describe {
        /// The kind of item to describe: 'S' to describe a prepared statement;
        /// or 'P' to describe a portal.
        object_type: DescribeObjectType,
        /// The name of the item to describe (an empty string selects the
        /// unnamed prepared statement or portal).
        name: String,
    },
    Execute {
        /// The name of the portal to execute (an empty string selects the
        /// unnamed portal).
        portal: String,
        /// The maximum number of rows to return, if portal contains a query
        /// that returns rows (ignored otherwise). Zero denotes "no limit".
        max_rows: i32,
    },
    Close {
        /// The kind of item to close (portal or statement).
        object_type: DescribeObjectType,
        /// Name of the object to close.
        name: String,
    },
    /// Synchronize after running through the extended query protocol.
    Sync,
    /// Flush the connection.
    Flush,
    /// Close the connection.
    Terminate,
}

impl FrontendMessage {
    pub const fn name(&self) -> &'static str {
        match self {
            FrontendMessage::Query { .. } => "query",
            FrontendMessage::PasswordMessage { .. } => "password",
            FrontendMessage::Parse { .. } => "parse",
            FrontendMessage::Bind { .. } => "bind",
            FrontendMessage::Describe { .. } => "describe",
            FrontendMessage::Execute { .. } => "execute",
            FrontendMessage::Close { .. } => "close",
            FrontendMessage::Flush => "flush",
            FrontendMessage::Sync => "sync",
            FrontendMessage::Terminate => "terminate",
        }
    }

    pub(crate) fn is_auth_message(&self) -> bool {
        matches!(self, FrontendMessage::PasswordMessage { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InBlock,
    Failed,
}

#[derive(Debug)]
pub enum BackendMessage {
    ErrorResponse(ErrorResponse),
    NoticeResponse(pgrepr::notice::Notice),
    AuthenticationOk,
    AuthenticationCleartextPassword,
    ParameterStatus { key: String, val: String },
    EmptyQueryResponse,
    ReadyForQuery(TransactionStatus),
    CommandComplete { tag: String },
    RowDescription(Vec<FieldDescription>),
    DataRow(RecordBatch, usize),
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription(Vec<i32>),
    /// `BackendKeyData` ('K') — sent during startup so the client can later
    /// issue a `CancelRequest` on a sideband TCP connection. The two ints
    /// must travel back unchanged in the cancel request and the server's
    /// session-registry must be able to look the pair up.
    BackendKeyData { process_id: i32, secret_key: i32 },
}

impl From<ErrorResponse> for BackendMessage {
    fn from(error: ErrorResponse) -> Self {
        BackendMessage::ErrorResponse(error)
    }
}

impl From<pgrepr::notice::Notice> for BackendMessage {
    fn from(notice: pgrepr::notice::Notice) -> Self {
        BackendMessage::NoticeResponse(notice)
    }
}

#[derive(Debug)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

impl ErrorSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorSeverity::Error => "ERROR",
            ErrorSeverity::Fatal => "FATAL",
            ErrorSeverity::Panic => "PANIC",
        }
    }
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub severity: ErrorSeverity,
    pub code: pgrepr::notice::SqlState,
    pub message: String,
}

impl ErrorResponse {
    pub fn error(code: pgrepr::notice::SqlState, msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Error,
            code,
            message: msg.into(),
        }
    }

    pub fn feature_not_supported(msg: impl Into<String>) -> ErrorResponse {
        Self::error(pgrepr::notice::SqlState::FeatureNotSupported, msg)
    }

    pub fn error_internal(msg: impl Into<String>) -> ErrorResponse {
        Self::error(pgrepr::notice::SqlState::InternalError, msg)
    }

    pub fn fatal_internal(msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Fatal,
            code: pgrepr::notice::SqlState::InternalError,
            message: msg.into(),
        }
    }
}

impl From<ExecError> for ErrorResponse {
    fn from(e: ExecError) -> Self {
        let msg = e.to_string();
        ErrorResponse::error(classify_sqlstate(&msg), msg)
    }
}

impl From<&PgSrvError> for ErrorResponse {
    fn from(e: &PgSrvError) -> Self {
        let msg = e.to_string();
        ErrorResponse::error(classify_sqlstate(&msg), msg)
    }
}

impl From<PgReprError> for ErrorResponse {
    fn from(e: PgReprError) -> Self {
        let msg = e.to_string();
        ErrorResponse::error(classify_sqlstate(&msg), msg)
    }
}

/// Inspect an error message and pick the best-matching Postgres SQLSTATE
/// from `pgrepr::notice::SqlState`. Drivers (DBeaver / JDBC / asyncpg /
/// libpq) branch heavily on these codes — for example, JDBC retries on
/// `57014` (query canceled) but not on `XX000` (internal). Sending the
/// right code matters more than the message text.
///
/// The matcher errs on the side of preserving `InternalError` when no
/// signal is present — a wrong specific code is worse than a generic one.
pub(crate) fn classify_sqlstate(msg: &str) -> pgrepr::notice::SqlState {
    use pgrepr::notice::SqlState;
    let lower = msg.to_ascii_lowercase();

    // 22P02 (cast / Arrow conversion) — checked FIRST so a "cast not
    // supported" wording (which Arrow occasionally emits) hits the
    // narrower invalid-input class instead of falling through to the
    // generic 0A000 block below. DataFusion/Arrow framings:
    //   "Cast error: Cannot cast string '...' to value of Date32"
    //   "Cannot cast value ... to type ..."
    if lower.contains("cast error")
        || (lower.contains("cannot cast") && lower.contains(" to "))
    {
        return SqlState::InvalidTextRepresentation;
    }

    // 42P01 — table / view / matview / index not found.
    if lower.contains("table not found")
        || lower.contains("relation not found")
        || lower.contains("table or view not found")
        || lower.contains("does not exist")
            && (lower.contains("table") || lower.contains("relation") || lower.contains("view"))
        || lower.contains("no table named")
        || lower.contains("missing builtin table")
        || lower.contains("unable to fetch table provider")
    {
        return SqlState::UndefinedTable;
    }

    // 42703 — column not found.
    if lower.contains("column not found")
        || lower.contains("no field named")
        || lower.contains("no column")
        || (lower.contains("does not exist") && lower.contains("column"))
    {
        return SqlState::UndefinedColumn;
    }

    // 42883 — function not found.
    if lower.contains("function not found")
        || (lower.contains("does not exist") && lower.contains("function"))
        || lower.contains("invalid function")
    {
        return SqlState::UndefinedFunction;
    }

    // 3D000 — database not found.
    if lower.contains("database not found")
        || (lower.contains("does not exist") && lower.contains("database"))
    {
        return SqlState::DatabaseDoesNotExist;
    }

    // 3F000 — schema not found.
    if lower.contains("schema not found")
        || (lower.contains("does not exist") && lower.contains("schema"))
        || lower.contains("missing schema")
    {
        return SqlState::SchemaDoesNotExist;
    }

    // 28P01 / 28000 — auth.
    if lower.contains("invalid password") || lower.contains("authentication failed") {
        return SqlState::InvalidPassword;
    }
    if lower.contains("not authorized") || lower.contains("authorization") {
        return SqlState::InvalidAuth;
    }

    // 42501 — insufficient privilege.
    if lower.contains("permission denied") || lower.contains("insufficient privilege") {
        return SqlState::InsufficientPrivilege;
    }

    // 22P02 — invalid text representation.
    if lower.contains("invalid input syntax")
        || lower.contains("could not parse")
        || lower.contains("parse error")
        || lower.contains("invalid value for")
    {
        return SqlState::InvalidTextRepresentation;
    }

    // 22023 — invalid parameter value.
    if lower.contains("invalid parameter value") {
        return SqlState::InvalidParameterValue;
    }

    // 57014 — query canceled (statement_timeout, pg_cancel_backend).
    if lower.contains("query canceled")
        || lower.contains("statement timeout")
        || lower.contains("execution canceled")
    {
        return SqlState::QueryCanceled;
    }

    // 53300 — too many connections.
    if lower.contains("too many connections") || lower.contains("connection limit") {
        return SqlState::TooManyConnections;
    }

    // 0A000 — feature not supported. The "does not support" branch
    // catches DataFusion errors like "Execution error: LIKE does not
    // support escape_char" that the bare "not supported" pattern
    // misses (the actual message uses the present-tense "support").
    if lower.contains("not supported")
        || lower.contains("does not support")
        || lower.contains("unsupported")
        || lower.contains("not yet implemented")
    {
        return SqlState::FeatureNotSupported;
    }

    // 42601 — syntax. (Parser-level errors fire SyntaxError directly upstream;
    // catch a few common shapes that surface as ExecError via DataFusion.)
    if lower.contains("syntax error") || lower.contains("expected") && lower.contains("found") {
        return SqlState::SyntaxError;
    }

    // 42P07 — duplicate object. Catches both the canonical "already
    // exists" wording and glaredb's catalog-side framing for duplicate
    // CREATE statements which surfaces as "Catalog error: failed to
    // create schema/table".
    if lower.contains("already exists")
        || (lower.contains("catalog error")
            && lower.contains("failed to create")
            && (lower.contains("schema") || lower.contains("table")))
    {
        return SqlState::DuplicateTable;
    }

    SqlState::InternalError
}

#[derive(Debug)]
pub struct FieldDescriptionBuilder<'a> {
    name: String,
    pg_type: Option<&'a PgType>,
    format: Format,
}

impl<'a> FieldDescriptionBuilder<'a> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pg_type: None,
            format: Format::Text,
        }
    }

    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    pub fn with_type<'b: 'a>(mut self, pg_type: &'b PgType) -> Self {
        self.pg_type = Some(pg_type);
        self
    }

    pub fn build(self) -> Result<FieldDescription> {
        let pg_type = self.pg_type.ok_or(PgSrvError::Internal(
            "type cannot be `None` in field description".to_string(),
        ))?;

        Ok(FieldDescription {
            name: self.name,
            table_id: 0, // TODO
            col_id: 0,   // TODO
            type_oid: pg_type.oid() as i32,
            type_size: 0, // TODO
            type_mod: 0,  // TODO
            format: self.format.into(),
        })
    }
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: String,
    pub table_id: i32,
    pub col_id: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_mod: i32,
    pub format: i16,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum DescribeObjectType {
    Statement = b'S',
    Portal = b'P',
}

impl std::fmt::Display for DescribeObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DescribeObjectType::Statement => write!(f, "Statement"),
            DescribeObjectType::Portal => write!(f, "Portal"),
        }
    }
}

impl TryFrom<u8> for DescribeObjectType {
    type Error = PgSrvError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'S' => Ok(DescribeObjectType::Statement),
            b'P' => Ok(DescribeObjectType::Portal),
            _ => Err(PgSrvError::UnexpectedDescribeObjectType(value)),
        }
    }
}
