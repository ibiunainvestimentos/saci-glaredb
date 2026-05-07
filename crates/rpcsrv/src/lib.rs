// Async chains in `flight::handler` got long enough that rustc's default
// 128-step query limit overflowed during `prepare_statement` layout
// computation after the W1c (`COMMENT ON`) parser hook landed. Bump for
// headroom — this is purely a compile-time knob.
#![recursion_limit = "256"]

pub mod errors;
pub mod flight;
pub mod handler;
pub mod proxy;
pub mod simple;

mod session;
mod util;
