//! SQL execution.
// The async statement-execution chains get deep enough that rustc's default
// 128-step query limit overflows when computing layout of the root `async
// fn` body. Bump for headroom.
#![recursion_limit = "256"]

pub mod context;
pub mod engine;
pub mod environment;
pub mod errors;
pub mod extension_codec;
mod optimizer;
pub mod remote;
pub mod session;

mod dispatch;
mod planner;
mod resolve;

pub use planner::logical_plan::{LogicalPlan, OperationInfo};
