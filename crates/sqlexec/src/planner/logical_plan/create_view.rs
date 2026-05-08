use protogen::metastore::types::options::InternalColumnDefinition;

use super::{
    DfLogicalPlan,
    ExtensionNode,
    OwnedFullObjectReference,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_LOGICAL_SCHEMA,
};
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateView {
    pub view_reference: OwnedFullObjectReference,
    pub sql: String,
    pub columns: Vec<String>,
    pub or_replace: bool,
    /// Column types resolved from the planned SELECT body (one per
    /// output field, ordered). Captured at plan time so the metastore
    /// can record real column rows for the view in
    /// `glare_catalog.columns` (and therefore `pg_catalog.pg_attribute`).
    pub column_types: Vec<InternalColumnDefinition>,
}

impl UserDefinedLogicalNodeCore for CreateView {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::EXTENSION_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateView {
    const EXTENSION_NAME: &'static str = "CreateView";
}
