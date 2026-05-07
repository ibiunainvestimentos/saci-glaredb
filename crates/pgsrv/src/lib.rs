//! Postgres protocol compatibility for GlareDB.
//!
//! Working with this crates requires knowledge about the Postgres
//! frontend/backend protocol.
//!
//! - <https://www.postgresql.org/docs/current/protocol-flow.html>
//! - <https://www.postgresql.org/docs/current/protocol-message-formats.html>
//!
//! We currently implement most of the Simple Query Flow and the Extended Query
//! Flow. We do not implement the copy protocol (yet), or the functional call
//! protocol (never).
// Async query chains got deep enough that rustc's default 128-step query
// limit overflowed during `query()` layout computation after the W1c
// COMMENT ON path was added. Bump for headroom — compile-time only.
#![recursion_limit = "256"]

pub mod auth;
pub mod errors;
pub mod handler;
pub mod proxy;
pub mod ssl;

mod codec;
mod messages;
