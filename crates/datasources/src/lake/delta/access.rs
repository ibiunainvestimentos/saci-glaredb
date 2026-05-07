use std::collections::HashMap;
use std::sync::Arc;

use deltalake::DeltaTable;
use protogen::metastore::types::options::{
    DeltaLakeCatalog,
    DeltaLakeUnityCatalog,
    StorageOptions,
};
use tracing::debug;

use crate::lake::delta::catalog::{DataCatalog, UnityCatalog};
use crate::lake::delta::errors::Result;

/// Access a delta lake using a catalog.
pub struct DeltaLakeAccessor {
    catalog: Arc<dyn DataCatalog>,
    storage_options: StorageOptions,
}

impl DeltaLakeAccessor {
    /// Connect to a deltalake using the provided catalog information.
    // TODO: Allow accessing delta tables without a catalog?
    pub async fn connect(
        catalog: &DeltaLakeCatalog,
        storage_options: StorageOptions,
    ) -> Result<DeltaLakeAccessor> {
        let catalog: Arc<dyn DataCatalog> = match catalog {
            DeltaLakeCatalog::Unity(DeltaLakeUnityCatalog {
                catalog_id,
                databricks_access_token,
                workspace_url,
            }) => {
                let catalog =
                    UnityCatalog::connect(databricks_access_token, workspace_url, catalog_id)
                        .await?;
                Arc::new(catalog)
            }
        };

        Ok(DeltaLakeAccessor {
            catalog,
            storage_options,
        })
    }

    pub async fn load_table(self, database: &str, table: &str) -> Result<DeltaTable> {
        let loc = self
            .catalog
            .get_table_storage_location(database, table)
            .await?;

        debug!(%loc, %database, %table, "deltalake location");

        let table = load_table_direct(&loc, self.storage_options).await?;
        Ok(table)
    }
}

/// Loads the table at the given location.
pub async fn load_table_direct(location: &str, opts: StorageOptions) -> Result<DeltaTable> {
    // Convert to delta-rs compatible options
    let opts = HashMap::from_iter(opts.inner.into_iter());
    let table = deltalake::open_table_with_storage_options(location, opts).await?;

    // Note that the deltalake crate does the appropriate jank for
    // registering the object store in the datafusion session's runtime env
    // during execution.
    Ok(table)
}

/// Open a Delta table and return its Arrow schema. Used by the planner at
/// `CREATE EXTERNAL TABLE` time to populate the catalog so
/// `pg_catalog.pg_attribute` can enumerate columns of the registered table
/// — without this, DBeaver / dbt / ibis show zero columns for any
/// externally-registered Delta table.
pub async fn load_table_arrow_schema(
    location: &str,
    opts: StorageOptions,
) -> Result<datafusion::arrow::datatypes::Schema> {
    use datafusion::arrow::datatypes::{DataType as Arrow, Field, TimeUnit};
    use deltalake::kernel::{DataType as D, PrimitiveType as P};

    let table = load_table_direct(location, opts).await?;
    let state = table
        .state
        .as_ref()
        .ok_or_else(|| crate::lake::delta::errors::DeltaError::Static(
            "delta table loaded without a state — schema unavailable",
        ))?;
    let delta_schema = state.schema();

    fn to_arrow(t: &D) -> Arrow {
        match t {
            D::Primitive(p) => match p {
                P::String => Arrow::Utf8,
                P::Long => Arrow::Int64,
                P::Integer => Arrow::Int32,
                P::Short => Arrow::Int16,
                P::Byte => Arrow::Int8,
                P::Float => Arrow::Float32,
                P::Double => Arrow::Float64,
                P::Boolean => Arrow::Boolean,
                P::Binary => Arrow::Binary,
                P::Date => Arrow::Date32,
                P::Timestamp => Arrow::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                P::TimestampNtz => Arrow::Timestamp(TimeUnit::Microsecond, None),
                P::Decimal(precision, scale) => {
                    Arrow::Decimal128(*precision, *scale as i8)
                }
            },
            // Nested Arrow types are JSON-serialised on the wire (see
            // `pgrepr::scalar`); a Utf8 placeholder is sufficient for the
            // catalog.
            D::Struct(_) | D::Array(_) | D::Map(_) => Arrow::Utf8,
        }
    }

    let fields: Vec<Field> = delta_schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), to_arrow(f.data_type()), f.is_nullable()))
        .collect();
    Ok(datafusion::arrow::datatypes::Schema::new(fields))
}
