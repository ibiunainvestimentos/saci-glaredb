//! `information_schema._pg_expandarray(int2vector)` — set-returning
//! function that yields one row `(x int2, n int4)` per array element,
//! where `n` is the 1-based ordinal position. Real Postgres ships this
//! in `information_schema.sql`; PgJDBC's `getPrimaryKeys`,
//! `getBestRowIdentifier`, and `getIndexInfo` all wrap it around
//! `pg_index.indkey` to project the per-key ordinal.
//!
//! Until we have a native `int2vector` Arrow type, our `pg_index.indkey`
//! is TEXT (space-separated decimals — the upstream wire form for
//! `int2vector`). This function consumes that exact text shape, so
//! PgJDBC's `(information_schema._pg_expandarray(i.indkey)).n` works
//! end-to-end without driver-side coercion.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int16Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

/// Hard cap on input length to avoid pathological allocations from
/// adversarial inputs. Real PG `pg_index.indkey` tops out at
/// `INDEX_MAX_KEYS` (32 by default), so 4 KiB of decimals leaves a
/// generous margin while still bounding the allocation.
const MAX_INPUT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy)]
pub struct PgExpandArray;

impl ConstBuiltinFunction for PgExpandArray {
    const NAME: &'static str = "_pg_expandarray";
    const DESCRIPTION: &'static str =
        "Expand a space-separated int2vector text into (x int2, n int4) rows.";
    const EXAMPLE: &'static str =
        "SELECT * FROM information_schema._pg_expandarray('23 25 1043')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    /// One required argument — `int2vector` in real PG, TEXT here. The
    /// value comes straight from `pg_index.indkey` whose Arrow type is
    /// `Utf8` in our catalog.
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            1,
            vec![DataType::Utf8],
            Volatility::Immutable,
        ))
    }
}

#[async_trait]
impl TableFunc for PgExpandArray {
    fn detect_runtime(
        &self,
        _: &[FuncParamValue],
        parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(match parent {
            RuntimePreference::Unspecified => RuntimePreference::Local,
            other => other,
        })
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let raw: String = args.next().unwrap().try_into()?;

        if raw.len() > MAX_INPUT_BYTES {
            return Err(ExtensionError::String(format!(
                "_pg_expandarray input exceeds {MAX_INPUT_BYTES} bytes"
            )));
        }

        // Parse space-separated decimals. Empty input → zero rows
        // (matches PG behaviour: `_pg_expandarray('')` is empty).
        // Whitespace-only inputs and Postgres `NULL`-formatted oidvectors
        // both end up empty for the same reason.
        let mut xs: Vec<i16> = Vec::new();
        for tok in raw.split_ascii_whitespace() {
            let parsed: i16 = tok.parse().map_err(|_| ExtensionError::InvalidParamValue {
                param: tok.to_string(),
                expected: "int2",
            })?;
            xs.push(parsed);
        }

        let ns: Vec<i32> = (1..=xs.len() as i32).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int16, false),
            Field::new("n", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int16Array::from(xs)),
                Arc::new(Int32Array::from(ns)),
            ],
        )
        .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        // Rows ≤ INDEX_MAX_KEYS (32 upstream) and further bounded by
        // MAX_INPUT_BYTES, so a single in-memory batch is the right
        // shape — `StreamingTable` would be overkill for this size.
        let provider = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(provider))
    }
}
