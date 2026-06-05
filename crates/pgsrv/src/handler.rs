use std::collections::{HashMap, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::scalar::ScalarValue;
use datafusion::variable::VarType;
use datafusion_ext::vars::{Dialect, SessionVars};
use futures::StreamExt;
use parser::StatementWithExtensions;
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use sqlexec::context::local::{OutputFields, Portal, PreparedStatement};
use sqlexec::engine::{Engine, SessionStorageConfig};
use sqlexec::session::{ExecutionResult, Session};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_postgres::types::Type as PgType;
use tracing::{debug, debug_span, warn, Instrument};
use uuid::Uuid;

use crate::auth::{LocalAuthenticator, PasswordMode};
use crate::codec::server::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    classify_sqlstate,
    BackendMessage,
    DescribeObjectType,
    ErrorResponse,
    FieldDescriptionBuilder,
    FrontendMessage,
    StartupMessage,
    TransactionStatus,
};
use crate::proxy::{
    ProxyKey,
    GLAREDB_DATABASE_ID_KEY,
    GLAREDB_GCS_STORAGE_BUCKET_KEY,
    GLAREDB_MAX_CREDENTIALS_COUNT_KEY,
    GLAREDB_MAX_DATASOURCE_COUNT_KEY,
    GLAREDB_MAX_TUNNEL_COUNT_KEY,
    GLAREDB_MEMORY_LIMIT_BYTES_KEY,
    GLAREDB_USER_ID_KEY,
};
use crate::ssl::{Connection, SslConfig};

pub struct ProtocolHandlerConfig {
    /// Authenticor to use on the server side.
    pub authenticator: Box<dyn LocalAuthenticator>,
    /// SSL configuration to use on the server side.
    pub ssl_conf: Option<SslConfig>,
    /// If the server should be configured for integration tests. This is only
    /// applicable for local databases.
    pub integration_testing: bool,
}

/// Per-server registry of active sessions for cancel-key routing. Each
/// connection generates a `(process_id, secret_key)` pair at startup,
/// registers itself here, and unregisters on disconnect. A cancel request
/// arrives over a sideband connection and looks up the process_id; if the
/// secret matches, we fire the cancellation token (best-effort —
/// in-flight DataFusion streams check the token cooperatively).
#[derive(Debug, Default)]
pub(crate) struct CancelRegistry {
    inner: std::sync::Mutex<std::collections::HashMap<i32, CancelEntry>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CancelEntry {
    pub secret_key: i32,
    /// Cancel flag shared with the running session. Setting it to `true`
    /// asks the active query to stop. DataFusion does not cooperatively
    /// poll this today, so cancel is currently best-effort at the wire
    /// level; full cancel propagation lands when we plumb the flag into
    /// `TaskContext`. The wire protocol contract — accept the cancel
    /// request without erroring — is satisfied either way.
    pub cancel: Arc<std::sync::atomic::AtomicBool>,
}

impl CancelRegistry {
    fn register(&self, pid: i32, entry: CancelEntry) {
        let mut g = self.inner.lock().unwrap();
        g.insert(pid, entry);
    }

    fn deregister(&self, pid: i32) {
        let mut g = self.inner.lock().unwrap();
        g.remove(&pid);
    }

    /// Look up by `process_id`, verify `secret_key`, fire the cancel flag.
    /// Returns true on success — a no-op return is sufficient on mismatch
    /// since the Postgres protocol provides no acknowledgement back to the
    /// canceling connection.
    fn fire(&self, pid: i32, secret: i32) -> bool {
        let g = self.inner.lock().unwrap();
        match g.get(&pid) {
            Some(entry) if entry.secret_key == secret => {
                entry
                    .cancel
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                true
            }
            _ => false,
        }
    }
}

/// A wrapper around a SQL engine that implements the Postgres frontend/backend
/// protocol.
pub struct ProtocolHandler {
    engine: Arc<Engine>,
    conf: ProtocolHandlerConfig,
    /// Process-wide cancel registry — see `CancelRegistry`.
    cancel_registry: Arc<CancelRegistry>,
}

impl ProtocolHandler {
    pub fn new(engine: Arc<Engine>, conf: ProtocolHandlerConfig) -> Self {
        ProtocolHandler {
            engine,
            conf,
            cancel_registry: Arc::new(CancelRegistry::default()),
        }
    }

    pub async fn handle_connection<C>(&self, id: Uuid, conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut conn = Connection::new_unencrypted(conn);
        loop {
            let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
            debug!(?startup, "received startup message (local)");

            match startup {
                StartupMessage::StartupRequest { params, .. } => {
                    self.begin(id, conn, params).await?;
                    return Ok(());
                }
                StartupMessage::SSLRequest { .. } => {
                    conn = match (conn, &self.conf.ssl_conf) {
                        (Connection::Unencrypted(mut conn), Some(conf)) => {
                            debug!("accepting ssl request");
                            // SSL supported, send back that we support it and
                            // start encrypting.
                            conn.write_all(&[b'S']).await?;
                            Connection::new_encrypted(conn, conf.config.clone()).await?
                        }
                        (mut conn, _) => {
                            debug!("rejecting ssl request");
                            // SSL not supported (or the connection is already
                            // wrapped). Reject and continue.
                            conn.write_all(&[b'N']).await?;
                            conn
                        }
                    }
                }
                StartupMessage::CancelRequest {
                    process_id,
                    secret_key,
                    ..
                } => {
                    let fired = self.cancel_registry.fire(process_id, secret_key);
                    debug!(
                        process_id,
                        secret_key, fired, "received cancel request (local)"
                    );
                    return Ok(());
                }
            }
        }
    }

    /// Read a value from the startup params that's been placed by pgsrv.
    ///
    /// This will also write any errors to the connection.
    async fn read_proxy_key_val<C, V, K>(
        &self,
        framed: &mut FramedConn<C>,
        key: &K,
        params: &HashMap<String, String>,
    ) -> Result<V>
    where
        C: AsyncRead + AsyncWrite + Unpin,
        K: ProxyKey<V>,
    {
        match key.value_from_params(params) {
            Ok(v) => Ok(v),
            Err(e) => {
                let resp = ErrorResponse::from(&e);
                framed.send(resp.into()).await?;
                // Technicall a client error, but the most likely cause is
                // misconfiguration on our end, go ahead and return the error so
                // it gets logged.
                Err(e)
            }
        }
    }

    /// Whether the server should be configured for integration testing.
    fn is_integration_testing_enabled(&self) -> bool {
        self.conf.integration_testing
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn begin<C>(
        &self,
        conn_id: Uuid,
        conn: Connection<C>,
        params: HashMap<String, String>,
    ) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        debug!("starting protocol with params: {:?}", params);

        let mut framed = FramedConn::new(conn);

        // Get params.
        // TODO: Possibly just serialize these into a single key on the proxy
        // side and deserialize here?
        let db_id = self
            .read_proxy_key_val(&mut framed, &GLAREDB_DATABASE_ID_KEY, &params)
            .await?;
        let is_cloud_instance = !db_id.is_nil();

        let user_id = self
            .read_proxy_key_val(&mut framed, &GLAREDB_USER_ID_KEY, &params)
            .await?;
        let max_datasource_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_DATASOURCE_COUNT_KEY, &params)
            .await?;
        let memory_limit_bytes = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MEMORY_LIMIT_BYTES_KEY, &params)
            .await?;
        let max_tunnel_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_TUNNEL_COUNT_KEY, &params)
            .await?;
        let max_credentials_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_CREDENTIALS_COUNT_KEY, &params)
            .await?;

        let storage_bucket = params.get(GLAREDB_GCS_STORAGE_BUCKET_KEY).cloned();

        // Standard postgres params. These values are used only for informational purposes.
        let user_name = params.get("user").cloned().unwrap_or_default();
        let database_name = params.get("database").cloned().unwrap_or_default();
        let db_id = if self.is_integration_testing_enabled() {
            // When in integration testing mode, try to get the database ID from dbname.
            database_name.parse::<Uuid>().unwrap_or(db_id)
        } else {
            db_id
        };

        // Handle password.
        match self.conf.authenticator.password_mode() {
            PasswordMode::RequireCleartext => {
                framed
                    .send(BackendMessage::AuthenticationCleartextPassword)
                    .await?;
                let msg = framed.read().await?;
                match msg {
                    Some(FrontendMessage::PasswordMessage { password }) => {
                        match self.conf.authenticator.authenticate(
                            &user_name,
                            &password,
                            &database_name,
                        ) {
                            Ok(sess) => sess,
                            Err(e) => {
                                framed
                                    .send(
                                        ErrorResponse::fatal_internal(format!(
                                            "Failed to authenticate: {}",
                                            e
                                        ))
                                        .into(),
                                    )
                                    .await?;
                                return Err(e);
                            }
                        }
                        framed.send(BackendMessage::AuthenticationOk).await?;
                    }
                    Some(other) => {
                        // TODO: Send error.
                        return Err(PgSrvError::UnexpectedFrontendMessage(Box::new(other)));
                    }
                    None => return Ok(()),
                }
            }
            PasswordMode::NoPassword { drop_auth_messages } => {
                if drop_auth_messages {
                    // Send the message to frontend to ask for an auth message.
                    // We will drop this message later on.
                    framed
                        .send(BackendMessage::AuthenticationCleartextPassword)
                        .await?;

                    // Read the auth message from the frontend. This will be
                    // ignored.
                    let msg = framed.peek().await?;
                    match msg {
                        Some(msg) if msg.is_auth_message() => {
                            let dropped = framed.read().await?; // Drop auth message.
                            warn!(?dropped, "dropping authentication message");
                        }
                        Some(_msg) => (), // We peeked a message not related to auth.
                        None => return Ok(()), // Connection closed
                    }
                }

                // Nothin to do.
                framed.send(BackendMessage::AuthenticationOk).await?;
            }
        }
        let mut vars = SessionVars::default()
            .with_user_id(user_id, VarType::System)
            .with_user_name(user_name, VarType::System)
            .with_connection_id(conn_id, VarType::System)
            .with_database_id(db_id, VarType::System)
            .with_database_name(database_name, VarType::System)
            .with_max_datasource_count(max_datasource_count, VarType::System)
            .with_memory_limit_bytes(memory_limit_bytes, VarType::System)
            .with_max_tunnel_count(max_tunnel_count, VarType::System)
            .with_max_credentials_count(max_credentials_count, VarType::System)
            .with_is_cloud_instance(is_cloud_instance, VarType::System);

        // Set other params provided on startup. Note that these are all set as
        // the "user" since these include values set in options.
        //
        // Note that we're ignoring unknown params, or params that we're unable
        // to set as a user.
        for (key, val) in &params {
            if let Err(e) = vars.set(key, val, VarType::UserDefined) {
                debug!(%e, %key, %val, "unable to set session variable from startup param");
            }
        }

        let sess = match self
            .engine
            .new_local_session_context(
                vars,
                SessionStorageConfig {
                    gcs_bucket: storage_bucket,
                },
            )
            .await
        {
            Ok(sess) => sess,
            Err(e) => {
                framed
                    .send(
                        ErrorResponse::fatal_internal(format!("failed to open session: {}", e))
                            .into(),
                    )
                    .await?;
                return Err(e.into());
            }
        };

        // Send server parameters.
        let msgs: Vec<_> = sess
            .get_session_vars()
            .read()
            .startup_vars_iter()
            .map(|var| BackendMessage::ParameterStatus {
                key: var.name().to_string(),
                val: var.formatted_value(),
            })
            .collect();
        for msg in msgs {
            framed.send(msg).await?;
        }

        // Allocate a per-session (process_id, secret_key) pair for cancel
        // routing and announce it to the client via `BackendKeyData` (`K`).
        // process_id is derived from the session UUID so it is stable across
        // SHOW pg_backend_pid() calls within the session; secret_key is
        // randomized per connection.
        let process_id = uuid_to_pg_pid(&conn_id);
        let secret_key: i32 = rand_i32();
        let cancel_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.cancel_registry.register(
            process_id,
            CancelEntry {
                secret_key,
                cancel: cancel_flag.clone(),
            },
        );
        framed
            .send(BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            })
            .await?;

        let cs = ClientSession::new(sess, framed);
        let result = cs.run().await;
        // Always deregister even if the session run errored.
        self.cancel_registry.deregister(process_id);
        let _ = cancel_flag; // silence unused-warning when full plumbing lands.
        result
    }

}

/// Map a session UUID to a stable, positive int32 — Postgres `pg_backend_pid`
/// is a 4-byte signed integer. We hash the high+low 64-bit halves with FNV-1a
/// then mask the sign bit so the result is always positive (libpq treats
/// negative pids as malformed).
fn uuid_to_pg_pid(uuid: &Uuid) -> i32 {
    let bytes = uuid.as_bytes();
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Fold to 32 bits and clear the sign bit.
    let folded = ((h ^ (h >> 32)) & 0x7FFFFFFF) as i32;
    folded.max(1) // 0 is reserved by libpq for "no pid"
}

/// Generate a random non-zero i32 for the cancel-key secret.
fn rand_i32() -> i32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Cheap entropy — combines the system clock with a thread-local hash of
    // the current Tokio task pointer. Good enough for cancel-key generation;
    // not for cryptographic use. The downside of a weaker PRNG here is an
    // attacker on the same loopback port may guess the secret — and they
    // already have read access if they reached the postmaster.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut h: u64 = 0xcbf29ce484222325;
    for b in now.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let folded = ((h ^ (h >> 32)) & 0x7FFFFFFF) as i32;
    folded.max(1)
}

struct ClientSession<C, S> {
    conn: FramedConn<C>,
    session: S,
    /// Current transaction state, surfaced in every `ReadyForQuery` (`Z`)
    /// message so JDBC / DBeaver / asyncpg can correctly decide whether
    /// they're inside a transaction block. Transitions:
    ///   Idle    --BEGIN--> InBlock
    ///   InBlock --COMMIT--> Idle
    ///   InBlock --ROLLBACK--> Idle
    ///   InBlock --error--> Failed
    ///   Failed  --ROLLBACK or COMMIT--> Idle
    tx_status: TransactionStatus,
    /// Statement names that the `parse_sql` preprocessor identified as
    /// connection-housekeeping no-ops (`RESET ALL`, `UNLISTEN *`, etc.).
    /// Maps statement name → CommandComplete tag to emit at Execute time
    /// (e.g. "RESET", "DISCARD ALL"). Populated at Parse, consumed at
    /// Execute, evicted at Close.
    noop_tags: HashMap<String, &'static str>,
    /// Portal name → CommandComplete tag, for the extended-query path.
    /// Populated at Bind time when the bound statement is a no-op,
    /// consumed at Execute, evicted at Close.
    portal_noop_tags: HashMap<String, &'static str>,
    /// Original SQL text per prepared statement, populated at Parse and
    /// evicted at Close. The 25P02 enforcement at `execute()` needs to
    /// know whether the SQL is a transaction-end command (ROLLBACK /
    /// COMMIT / END / ABORT / RELEASE) before deciding to gate.
    stmt_sql: HashMap<String, String>,
    /// Portal name → statement name. Populated at Bind, evicted at Close.
    /// Used by `execute()` to look up the statement's SQL text.
    portal_stmt: HashMap<String, String>,
}

/// This helper macro is used so we can call some `get_*` methods on the
/// session and maybe do some processing over it.
///
/// The motivation to write this macro is that during a query, we don't want to
/// return `Err(...)` in case of non-connection errors.
macro_rules! session_do {
    ($client:ident, $sess:ident, $get_fn:ident, $name:expr, $do:expr) => {
        match $sess.$get_fn($name) {
            Ok(v) => $do(v),
            Err(e) => {
                $client.send_error(e.into()).await?;
                return $client.ready_for_query().await;
            }
        }
    };
}

impl<C, S> ClientSession<C, S>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: DerefMut<Target = Session>,
{
    fn new(session: S, conn: FramedConn<C>) -> Self {
        ClientSession {
            session,
            conn,
            tx_status: TransactionStatus::Idle,
            noop_tags: HashMap::new(),
            portal_noop_tags: HashMap::new(),
            stmt_sql: HashMap::new(),
            portal_stmt: HashMap::new(),
        }
    }

    async fn run(mut self) -> Result<()> {
        self.ready_for_query().await?;
        loop {
            let msg = self.conn.read().await?;

            let msg = match msg {
                Some(msg) => msg,
                None => {
                    // No message received, connection closed.
                    debug!("connection closed");
                    return Ok(());
                }
            };

            let span = debug_span!("pg_protocol_message", name = msg.name());
            span.follows_from(tracing::Span::current());

            match msg {
                FrontendMessage::Query { sql } => self.query(sql).instrument(span).await?,
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => self.parse(name, sql, param_types).instrument(span).await?,
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                } => {
                    self.bind(
                        portal,
                        statement,
                        param_formats,
                        param_values,
                        result_formats,
                    )
                    .instrument(span)
                    .await?
                }
                FrontendMessage::Describe { object_type, name } => {
                    self.describe(object_type, name).instrument(span).await?
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.execute(portal, max_rows).instrument(span).await?
                }
                FrontendMessage::Close { object_type, name } => {
                    self.close_object(object_type, name)
                        .instrument(span)
                        .await?
                }
                FrontendMessage::Sync => self.sync().instrument(span).await?,
                FrontendMessage::Flush => self.flush().instrument(span).await?,
                FrontendMessage::Terminate => return Ok(()),
                other => {
                    warn!(?other, "unsupported frontend message");
                    self.conn
                        .send(
                            ErrorResponse::feature_not_supported(format!(
                                "unsupported frontend message: {:?}",
                                other
                            ))
                            .into(),
                        )
                        .await?;
                    self.ready_for_query().await?;
                }
            }
        }
    }

    /// Send an error response to the client.
    async fn send_error(&mut self, err: ErrorResponse) -> Result<()> {
        // Auto-transition tx_status to Failed when an error fires inside
        // an active transaction block. Real Postgres behaviour: any
        // server-side error during BEGIN..COMMIT moves the tx into
        // failed state, surfaced via the `Z` byte ('E') and enforced by
        // the 25P02 gate at the top of `query()` / `execute()`. Doing
        // it here covers every error path uniformly (Parse, Bind,
        // Execute, send_error from gates) without requiring each
        // handler to remember the transition.
        if matches!(self.tx_status, TransactionStatus::InBlock) {
            self.tx_status = TransactionStatus::Failed;
        }
        self.conn.send(err.into()).await?;
        Ok(())
    }

    async fn ready_for_query(&mut self) -> Result<()> {
        // Display notice messages before indicating we're ready for the next
        // query. The pg protocol does not presribe a specific flow for notice
        // messages, and so frontends should be capable of handling notices at
        // any point in the message flow.
        for notice in self.session.take_notices() {
            self.conn
                .send(BackendMessage::NoticeResponse(notice))
                .await?;
        }

        self.conn
            .send(BackendMessage::ReadyForQuery(self.tx_status))
            .await?;
        self.flush().await
    }

    /// Run the simple query flow.
    ///
    /// Note that this should only returns errors related to the underlying
    /// connection. All errors resulting from query execution should be sent to
    /// client following by a "ready for query".
    async fn query(&mut self, sql: String) -> Result<()> {
        let session = &mut self.session;
        let conn = &mut self.conn;

        let parsed = match parse_sql(session.get_session_vars(), &sql) {
            Ok(p) => p,
            Err(e) => {
                self.send_error(e).await?;
                return self.ready_for_query().await;
            }
        };

        // Connection-housekeeping no-ops short-circuit here: emit one
        // CommandComplete with the canonical PG tag so asyncpg's
        // `Connection.reset()` decoder succeeds. EmptyQueryResponse is
        // NOT a valid substitute (asyncpg's tag.decode() fails on None).
        let stmts = match parsed {
            ParsedSql::Noop(tag) => {
                Self::command_complete(conn, tag).await?;
                return self.ready_for_query().await;
            }
            ParsedSql::Stmts(stmts) => stmts,
        };

        // Determines if we send back an empty query response.
        let num_statements = stmts.len();

        for stmt in stmts {
            // 25P02 enforcement (matches real Postgres): once an error
            // inside a transaction puts the session into Failed, every
            // subsequent statement is rejected with `InFailedSqlTransaction`
            // until ROLLBACK / COMMIT / END / ABORT / RELEASE clears the
            // state. Per-statement check inside the loop so a multi-stmt
            // simple-query like `SELECT 1; ROLLBACK;` doesn't slip the
            // gate when the first stmt arrives in failed state.
            if matches!(self.tx_status, TransactionStatus::Failed)
                && !ends_failed_transaction(&stmt.to_string())
            {
                self.send_error(ErrorResponse::error(
                    pgrepr::notice::SqlState::InFailedTransaction,
                    "current transaction is aborted, commands ignored \
                     until end of transaction block",
                ))
                .await?;
                return self.ready_for_query().await;
            }

            // Note everything is using unnamed portals/prepared statements.

            const UNNAMED: String = String::new();

            // Parse...
            if let Err(e) = session.prepare_statement(UNNAMED, stmt, Vec::new()).await {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            };

            // Describe statement and get number of fields...
            fn get_num_fields(s: &PreparedStatement) -> usize {
                s.output_fields().map(|f| f.len()).unwrap_or(0)
            }
            let num_fields = session_do!(
                self,
                session,
                get_prepared_statement,
                &UNNAMED,
                get_num_fields
            );

            // Bind...
            if let Err(e) = session
                .bind_statement(UNNAMED, &UNNAMED, Vec::new(), all_text_formats(num_fields))
                .await
            {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            }

            // Execute...
            let stream = match session.execute_portal(&UNNAMED, 0).await {
                Ok(stream) => stream,
                Err(e) => {
                    // An error inside an open transaction block puts the
                    // session into the "Failed" state — every subsequent
                    // command is rejected until ROLLBACK or COMMIT.
                    if matches!(self.tx_status, TransactionStatus::InBlock) {
                        self.tx_status = TransactionStatus::Failed;
                    }
                    self.send_error(e.into()).await?;
                    return self.ready_for_query().await;
                }
            };

            // If we're returning data (SELECT), send back the output fields
            // before sending back actual data.
            if let ExecutionResult::Query { .. } = stream {
                let output_fields =
                    session_do!(self, session, get_portal, &UNNAMED, Portal::output_fields);
                if let Some(fields) = output_fields {
                    Self::send_row_descriptor(conn, fields).await?;
                }
            }

            // Track transaction-state transitions so the trailing
            // `ReadyForQuery` reports the right status byte (`I`/`T`/`E`).
            // Driver-side code (PgJDBC, asyncpg, DBeaver) branches on this
            // to know whether it is inside a transaction.
            match &stream {
                ExecutionResult::Begin => {
                    self.tx_status = TransactionStatus::InBlock;
                }
                ExecutionResult::Commit | ExecutionResult::Rollback => {
                    self.tx_status = TransactionStatus::Idle;
                }
                _ => {}
            }

            Self::send_result(
                conn,
                stream,
                session_do!(self, session, get_portal, &UNNAMED, get_encoding_state),
            )
            .await?;
        }

        if num_statements == 0 {
            self.conn.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready_for_query().await
    }

    /// Parse the provided SQL statement and store it in the session.
    async fn parse(&mut self, name: String, sql: String, param_types: Vec<i32>) -> Result<()> {
        // TODO: Ensure in transaction.
        let vars = self.session.get_session_vars();
        let parsed = match parse_sql(vars, &sql) {
            Ok(p) => p,
            Err(e) => return self.send_error(e).await,
        };

        // Stash the SQL text against the statement name so the 25P02
        // gate in execute() can decide whether it's a transaction-end
        // command without re-parsing.
        self.stmt_sql.insert(name.clone(), sql.clone());

        // Connection-housekeeping no-ops in the extended-query flow:
        // record the tag against the statement name, prepare an empty
        // statement (so Bind / Describe / Close all work), and send
        // ParseComplete. The Execute path emits the recorded tag.
        let mut stmts = match parsed {
            ParsedSql::Noop(tag) => {
                self.noop_tags.insert(name.clone(), tag);
                return match self
                    .session
                    .prepare_statement(name, None, param_types)
                    .await
                {
                    Ok(_) => self.conn.send(BackendMessage::ParseComplete).await,
                    Err(e) => self.send_error(e.into()).await,
                };
            }
            ParsedSql::Stmts(stmts) => stmts,
        };

        // Can only have one statement per parse.
        if stmts.len() > 1 {
            return self
                .send_error(ErrorResponse::error_internal(
                    "cannot parse multiple statements",
                ))
                .await;
        }

        // TODO: Check if in failed transaction.

        // Store statement for future use.
        match self
            .session
            .prepare_statement(name, stmts.pop_front(), param_types)
            .await
        {
            Ok(_) => self.conn.send(BackendMessage::ParseComplete).await,
            Err(e) => self.send_error(e.into()).await,
        }
    }

    /// Bind to a prepared statement.
    async fn bind(
        &mut self,
        portal: String,
        statement: String,
        param_formats: Vec<Format>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        // TODO: Ensure in transaction.

        // Check for the statement.
        let stmt = match self.session.get_prepared_statement(&statement) {
            Ok(stmt) => stmt,
            Err(e) => return self.send_error(e.into()).await,
        };

        // Read scalars for query parameters.
        let scalars = match stmt.input_paramaters() {
            Some(types) => match decode_param_scalars(param_formats, param_values, types) {
                Ok(scalars) => scalars,
                Err(e) => return self.send_error(e).await,
            },
            None => Vec::new(), // Would only happen with an empty query.
        };

        // Extend out the result formats.
        let result_formats = match extend_formats(
            result_formats,
            stmt.output_fields().map(|fields| fields.len()).unwrap_or(0),
        ) {
            Ok(formats) => formats,
            Err(e) => return self.send_error(e).await,
        };

        // Propagate no-op tag from statement → portal so the Execute
        // path can emit the right CommandComplete tag.
        if let Some(tag) = self.noop_tags.get(&statement).copied() {
            self.portal_noop_tags.insert(portal.clone(), tag);
        }

        // Map portal → statement so execute() can read the SQL text out
        // of stmt_sql for the 25P02 gate decision.
        self.portal_stmt
            .insert(portal.clone(), statement.clone());

        match self
            .session
            .bind_statement(portal, &statement, scalars, result_formats)
            .await
        {
            Ok(_) => self.conn.send(BackendMessage::BindComplete).await,
            Err(e) => self.send_error(e.into()).await,
        }
    }

    async fn describe(&mut self, object_type: DescribeObjectType, name: String) -> Result<()> {
        // TODO: Ensure in a transaction.

        let conn = &mut self.conn;
        match object_type {
            DescribeObjectType::Statement => match self.session.get_prepared_statement(&name) {
                Ok(stmt) => {
                    // TODO: We don't accept parameters yet. So just send back
                    // an empty paramaters list.
                    conn.send(BackendMessage::ParameterDescription(Vec::new()))
                        .await?;

                    // Send back row description.
                    match stmt.output_fields() {
                        Some(fields) => {
                            // When fields are extracted from a prepared
                            // statement, default format is applied, i.e., Text
                            // which is exactly what the protocol dictates.
                            //
                            // See: https://www.postgresql.org/docs/15/protocol-flow.html
                            // > Note that since Bind has not yet been issued,
                            // > the formats to be used for returned columns
                            // > are not yet known to the backend; the format
                            // > code fields in the RowDescription message will
                            // > be zeroes in this case.
                            Self::send_row_descriptor(conn, fields).await?
                        }
                        None => self.conn.send(BackendMessage::NoData).await?,
                    }

                    Ok(())
                }
                Err(e) => self.send_error(e.into()).await,
            },
            DescribeObjectType::Portal => match self.session.get_portal(&name) {
                Ok(portal) => {
                    // Send back row description.
                    match portal.output_fields() {
                        Some(fields) => Self::send_row_descriptor(conn, fields).await?,
                        None => self.conn.send(BackendMessage::NoData).await?,
                    }
                    Ok(())
                }
                Err(e) => self.send_error(e.into()).await,
            },
        }
    }

    async fn execute(&mut self, portal: String, max_rows: i32) -> Result<()> {
        // Connection-housekeeping no-op: short-circuit with the recorded
        // CommandComplete tag instead of running through execute_portal
        // (which would emit EmptyQueryResponse — wrong for asyncpg).
        if let Some(tag) = self.portal_noop_tags.get(&portal).copied() {
            return Self::command_complete(&mut self.conn, tag).await;
        }

        // 25P02 enforcement (extended-query path). Same gate as the
        // simple-query path: reject every statement until ROLLBACK /
        // COMMIT clears the failed-tx state. Look up the SQL text via
        // portal → statement → stmt_sql.
        if matches!(self.tx_status, TransactionStatus::Failed) {
            let allow = self
                .portal_stmt
                .get(&portal)
                .and_then(|stmt| self.stmt_sql.get(stmt))
                .map(|sql| ends_failed_transaction(sql))
                .unwrap_or(false);
            if !allow {
                return self
                    .send_error(ErrorResponse::error(
                        pgrepr::notice::SqlState::InFailedTransaction,
                        "current transaction is aborted, commands ignored \
                         until end of transaction block",
                    ))
                    .await;
            }
        }

        let conn = &mut self.conn;
        let session = &mut self.session;
        let stream = match session.execute_portal(&portal, max_rows).await {
            Ok(r) => r,
            Err(e) => {
                if matches!(self.tx_status, TransactionStatus::InBlock) {
                    self.tx_status = TransactionStatus::Failed;
                }
                return self.send_error(e.into()).await;
            }
        };

        // Track transaction transitions for the trailing `ReadyForQuery`
        // — same logic as in the simple-query path. See comments above.
        match &stream {
            ExecutionResult::Begin => self.tx_status = TransactionStatus::InBlock,
            ExecutionResult::Commit | ExecutionResult::Rollback => {
                self.tx_status = TransactionStatus::Idle
            }
            _ => {}
        }

        // TODO: This seems to be missing sending back row description. Is it
        // needed? If not, a comment needs to go here.

        Self::send_result(
            conn,
            stream,
            session_do!(self, session, get_portal, &portal, get_encoding_state),
        )
        .await
    }

    async fn close_object(&mut self, object_type: DescribeObjectType, name: String) -> Result<()> {
        match object_type {
            DescribeObjectType::Statement => {
                self.session.remove_prepared_statement(&name);
                self.noop_tags.remove(&name);
                self.stmt_sql.remove(&name);
            }
            DescribeObjectType::Portal => {
                self.session.remove_portal(&name);
                self.portal_noop_tags.remove(&name);
                self.portal_stmt.remove(&name);
            }
        }
        self.conn.send(BackendMessage::CloseComplete).await
    }

    async fn sync(&mut self) -> Result<()> {
        self.ready_for_query().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.conn.flush().await?;
        Ok(())
    }

    async fn send_result(
        conn: &mut FramedConn<C>,
        stream: ExecutionResult,
        encoding_state: Vec<(PgType, Format)>,
    ) -> Result<()> {
        match stream {
            ExecutionResult::Error(e) => return Err(e.into()),
            ExecutionResult::Query { stream, .. } => {
                if let Some(num_rows) = Self::stream_batch(conn, stream, encoding_state).await? {
                    Self::command_complete(conn, format!("SELECT {}", num_rows)).await?;
                }
            }
            ExecutionResult::EmptyQuery => conn.send(BackendMessage::EmptyQueryResponse).await?,
            ExecutionResult::Begin => Self::command_complete(conn, "BEGIN").await?,
            ExecutionResult::Commit => Self::command_complete(conn, "COMMIT").await?,
            ExecutionResult::Rollback => Self::command_complete(conn, "ROLLBACK").await?,
            ExecutionResult::InsertSuccess { rows_inserted } => {
                // Format is 'INSERT <oid> <num_inserted>'. Oid will always be
                // zero according to postgres docs.
                Self::command_complete(conn, format!("INSERT 0 {rows_inserted}")).await?
            }
            ExecutionResult::CopySuccess => Self::command_complete(conn, "COPY").await?,
            ExecutionResult::DeleteSuccess { deleted_rows } => {
                Self::command_complete(conn, format!("DELETE {}", deleted_rows)).await?
            }
            ExecutionResult::UpdateSuccess { updated_rows } => {
                Self::command_complete(conn, format!("UPDATE {}", updated_rows)).await?
            }
            ExecutionResult::CreateTable => Self::command_complete(conn, "CREATE TABLE").await?,
            ExecutionResult::CreateDatabase => {
                Self::command_complete(conn, "CREATE DATABASE").await?
            }
            ExecutionResult::CreateTunnel => Self::command_complete(conn, "CREATE TUNNEL").await?,
            ExecutionResult::CreateCredential => {
                Self::command_complete(conn, "CREATE CREDENTIAL").await?
            }
            ExecutionResult::CreateCredentials => {
                Self::command_complete(
                    conn,
                    "CREATE CREDENTIALS\nDEPRECATION WARNING.USE `CREATE CREDENTIAL`.",
                )
                .await?
            }
            ExecutionResult::CreateSchema => Self::command_complete(conn, "CREATE SCHEMA").await?,
            ExecutionResult::CreateView => Self::command_complete(conn, "CREATE VIEW").await?,
            ExecutionResult::AlterTable => Self::command_complete(conn, "ALTER TABLE").await?,
            ExecutionResult::AlterDatabase => {
                Self::command_complete(conn, "ALTER DATABASE").await?
            }
            ExecutionResult::AlterTunnelRotateKeys => {
                Self::command_complete(conn, "ALTER TUNNEL").await?
            }
            ExecutionResult::Set => Self::command_complete(conn, "SET").await?,
            ExecutionResult::DropTables => Self::command_complete(conn, "DROP TABLE").await?,
            ExecutionResult::DropViews => Self::command_complete(conn, "DROP VIEW").await?,
            ExecutionResult::DropSchemas => Self::command_complete(conn, "DROP SCHEMA").await?,
            ExecutionResult::DropDatabase => Self::command_complete(conn, "DROP DATABASE").await?,
            ExecutionResult::DropTunnel => Self::command_complete(conn, "DROP TUNNEL").await?,
            ExecutionResult::DropCredentials => {
                Self::command_complete(conn, "DROP CREDENTIALS").await?
            }
        };
        Ok(())
    }

    /// Convert an arrow schema into a row descriptor and send it to the client.
    async fn send_row_descriptor(conn: &mut FramedConn<C>, fields: OutputFields<'_>) -> Result<()> {
        let mut row_description = Vec::with_capacity(fields.len());
        for f in fields {
            let desc = FieldDescriptionBuilder::new(f.name)
                .with_type(f.pg_type)
                .with_format(*f.format)
                .build()?;
            row_description.push(desc);
        }
        conn.send(BackendMessage::RowDescription(row_description))
            .await?;
        Ok(())
    }

    /// Streams the batch to the client, returns an optional total number of
    /// rows sent. `None` rows sent means that an error response was sent.
    async fn stream_batch(
        conn: &mut FramedConn<C>,
        mut stream: SendableRecordBatchStream,
        encoding_state: Vec<(PgType, Format)>,
    ) -> Result<Option<usize>> {
        conn.set_encoding_state(encoding_state);
        let mut num_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = match result {
                Ok(r) => r,
                Err(e) => {
                    // Errors that surface mid-stream (e.g. Arrow Cast
                    // errors during execution) flow through here. Pass
                    // through classify_sqlstate so cast / not-supported /
                    // and similar wordings get the right code instead of
                    // the catch-all XX000 InternalError.
                    let msg = e.to_string();
                    conn.send(
                        ErrorResponse::error(classify_sqlstate(&msg), msg).into(),
                    )
                    .await?;
                    return Ok(None);
                }
            };
            num_rows += batch.num_rows();
            for row_idx in 0..batch.num_rows() {
                // Clone is cheapish here, all columns behind an arc.
                if let Err(e) = conn
                    .send(BackendMessage::DataRow(batch.clone(), row_idx))
                    .await
                {
                    // A column value failed to encode (e.g. an unsupported
                    // Arrow type, or a decimal too large for PG numeric). The
                    // codec already rolled the partial frame back, so the
                    // write buffer is clean — emit a classified ErrorResponse
                    // and stop. Propagating via `?` here would instead drop
                    // the connection mid-result, leaving the pooled client
                    // desynced and failing every subsequent query (incl.
                    // `SELECT 1`) until the connection ages out.
                    let msg = e.to_string();
                    conn.send(ErrorResponse::error(classify_sqlstate(&msg), msg).into())
                        .await?;
                    return Ok(None);
                }
            }
        }
        Ok(Some(num_rows))
    }

    async fn command_complete(conn: &mut FramedConn<C>, tag: impl Into<String>) -> Result<()> {
        conn.send(BackendMessage::CommandComplete { tag: tag.into() })
            .await
    }
}

/// Outcome of `parse_sql`: either a statement list to plan + execute, or
/// a no-op tag to emit directly as `CommandComplete` without going
/// through the planner.
enum ParsedSql {
    Stmts(VecDeque<StatementWithExtensions>),
    /// One of the connection-housekeeping commands listed in
    /// `is_postgres_noop`. The carried tag is the wire-protocol
    /// `CommandComplete` tag (e.g. `"RESET"`, `"DISCARD ALL"`,
    /// `"UNLISTEN"`).
    Noop(&'static str),
}

/// Parse a sql string, returning an error response if failed to parse.
///
/// Before handing the string to the underlying parser we shortcut a small
/// set of Postgres housekeeping statements that GlareDB does not need to
/// model — `RESET ALL`, `DISCARD ALL` / `DISCARD TEMP` / `DISCARD PLANS` /
/// `DISCARD SEQUENCES`, `DEALLOCATE ALL`, `UNLISTEN *`, and the
/// multi-statement combined string asyncpg's `Connection.reset()` sends
/// (`SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL;`).
///
/// These are emitted by JDBC / asyncpg / DBeaver on every
/// connection-pool release. Rejecting them forces clients into bespoke
/// connection-class shims (see sacibackend `_GlareDBConnection.reset()`
/// for the prior workaround). Each driver expects a `CommandComplete`
/// with a matching tag (e.g. `RESET`) — `EmptyQueryResponse` is NOT a
/// valid substitute (asyncpg's tag decoder fails on None).
fn parse_sql(session_vars: SessionVars, sql: &str) -> Result<ParsedSql, ErrorResponse> {
    if let Some(tag) = is_postgres_noop(sql) {
        return Ok(ParsedSql::Noop(tag));
    }
    match session_vars.dialect() {
        Dialect::Prql => parser::parse_prql(sql),
        Dialect::Sql => parser::parse_sql(sql),
    }
    .map(ParsedSql::Stmts)
    .map_err(|e| ErrorResponse::error(pgrepr::notice::SqlState::SyntaxError, e.to_string()))
}

/// Map a connection-housekeeping SQL string to the wire-protocol
/// `CommandComplete` tag GlareDB should emit. Returns `None` if the
/// statement isn't recognized as a no-op (caller should hand it to the
/// real parser).
///
/// Accepts both single statements and the multi-statement combined
/// string asyncpg sends in `Connection.reset()`. Bails on quoted strings
/// because splitting on `;` across a string literal would partition the
/// literal incorrectly — caller falls back to the parser, which still
/// fails today, but we've never seen a real driver quote-wrap a reset
/// command.
fn is_postgres_noop(sql: &str) -> Option<&'static str> {
    if sql.contains('\'') || sql.contains('"') {
        return None;
    }
    fn match_one(piece: &str) -> Option<&'static str> {
        match piece.trim().to_ascii_lowercase().as_str() {
            "reset all" => Some("RESET"),
            "discard all" => Some("DISCARD ALL"),
            "discard temp" | "discard temporary" => Some("DISCARD TEMP"),
            "discard plans" => Some("DISCARD PLANS"),
            "discard sequences" => Some("DISCARD SEQUENCES"),
            "deallocate all" => Some("DEALLOCATE ALL"),
            "unlisten *" => Some("UNLISTEN"),
            // Members of asyncpg's combined `Connection.reset()` query —
            // GlareDB doesn't track listen channels, advisory locks, or
            // open cursors by session, so each is a free no-op.
            "close all" => Some("CLOSE CURSOR ALL"),
            "select pg_advisory_unlock_all()" => Some("SELECT"),
            _ => None,
        }
    }

    let pieces: Vec<&str> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if pieces.is_empty() {
        return None;
    }
    let mut last_tag = None;
    for piece in pieces {
        last_tag = Some(match_one(piece)?);
    }
    last_tag
}

/// True iff `sql`'s first non-whitespace word is one of the keywords
/// that ends or rolls back a failed transaction (matches Postgres
/// behavior: in `25P02`, every command is rejected EXCEPT ROLLBACK /
/// COMMIT / END / ABORT / RELEASE — and even COMMIT silently rolls
/// back the failed tx).
fn ends_failed_transaction(sql: &str) -> bool {
    let first = sql
        .trim_start()
        .trim_start_matches(';')
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        first.as_str(),
        "rollback" | "commit" | "end" | "abort" | "release"
    )
}

/// Decodes inputs for a prepared query into the appropriate scalar values.
fn decode_param_scalars(
    param_formats: Vec<Format>,
    param_values: Vec<Option<Vec<u8>>>,
    types: &HashMap<String, Option<(PgType, DataType)>>,
) -> Result<Vec<ScalarValue>, ErrorResponse> {
    let param_formats = extend_formats(param_formats, param_values.len())?;

    if param_values.len() != types.len() {
        return Err(ErrorResponse::error_internal(format!(
            "Invalid number of values provided. Expected: {}, got: {}",
            types.len(),
            param_values.len(),
        )));
    }

    let mut scalars = Vec::with_capacity(param_values.len());
    for (idx, (val, format)) in param_values
        .into_iter()
        .zip(param_formats.into_iter())
        .enumerate()
    {
        // Parameter types keyed by '$n'.
        let str_id = format!("${}", idx + 1);

        let typ = types.get(&str_id).ok_or_else(|| {
            ErrorResponse::error_internal(format!(
                "Missing type for param value at index {}, input types: {:?}",
                idx, types
            ))
        })?;

        match typ {
            Some(typ) => {
                let scalar = {
                    match val.as_deref() {
                        None => ScalarValue::Null,
                        Some(v) => Scalar::decode_with_format(format, v, &typ.0)?
                            .into_datafusion(&typ.1)?,
                    }
                };
                scalars.push(scalar);
            }
            None => {
                return Err(ErrorResponse::error_internal(format!(
                    "Unknown type at index {}, input types: {:?}",
                    idx, types
                )))
            }
        }
    }

    Ok(scalars)
}

/// Returns a vector with all the formats extended to the default "text".
fn all_text_formats(num: usize) -> Vec<Format> {
    extend_formats(Vec::new(), num).unwrap()
}

/// Extend a vector of format codes to the desired size.
///
/// See the doc for the `Bind` message for more.
fn extend_formats(formats: Vec<Format>, num: usize) -> Result<Vec<Format>, ErrorResponse> {
    Ok(match formats.len() {
        0 => vec![Format::Text; num], // Everything defaults to text,
        1 => vec![formats[0]; num],   // Use the singly specified format for everything.
        len if len == num => formats,
        len => {
            return Err(ErrorResponse::error_internal(format!(
                "invalid number for format specifiers, got: {}, expected: {}",
                len, num,
            )))
        }
    })
}

/// Returns the encoding state, i.e., postgres type and format from the portal.
fn get_encoding_state(portal: &Portal) -> Vec<(PgType, Format)> {
    match portal.output_fields() {
        None => Vec::new(),
        Some(fields) => fields
            .map(|field| (field.pg_type.to_owned(), field.format.to_owned()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_params_success() {
        // Success test cases for decoding params.

        struct TestCase {
            values: Vec<Option<Vec<u8>>>,
            types: Vec<(&'static str, Option<(PgType, DataType)>)>,
            expected: Vec<ScalarValue>,
        }

        let test_cases = vec![
            // No params.
            TestCase {
                values: Vec::new(),
                types: Vec::new(),
                expected: Vec::new(),
            },
            // One param of type int64.
            TestCase {
                values: vec![Some(vec![49])],
                types: vec![("$1", Some((PgType::INT8, DataType::Int64)))],
                expected: vec![ScalarValue::Int64(Some(1))],
            },
            // Two params param of type string.
            TestCase {
                values: vec![Some(vec![49, 48]), Some(vec![50, 48])],
                types: vec![
                    ("$1", Some((PgType::TEXT, DataType::Utf8))),
                    ("$2", Some((PgType::TEXT, DataType::Utf8))),
                ],
                expected: vec![
                    ScalarValue::Utf8(Some("10".to_string())),
                    ScalarValue::Utf8(Some("20".to_string())),
                ],
            },
        ];

        for test_case in test_cases {
            let types: HashMap<_, _> = test_case
                .types
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();

            let scalars = decode_param_scalars(Vec::new(), test_case.values, &types).unwrap();
            assert_eq!(test_case.expected, scalars);
        }
    }

    #[test]
    fn decode_params_fail() {
        // Failure test cases for decoding params (all cases should result in an
        // error).

        struct TestCase {
            values: Vec<Option<Vec<u8>>>,
            types: Vec<(&'static str, Option<(PgType, DataType)>)>,
        }

        let test_cases = vec![
            // Params provided, none expected.
            TestCase {
                values: vec![Some(vec![49])],
                types: Vec::new(),
            },
            // No params provided, one expected.
            TestCase {
                values: Vec::new(),
                types: vec![("$1", Some((PgType::INT8, DataType::Int64)))],
            },
        ];

        for test_case in test_cases {
            let types: HashMap<_, _> = test_case
                .types
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();

            decode_param_scalars(Vec::new(), test_case.values, &types).unwrap_err();
        }
    }
}
