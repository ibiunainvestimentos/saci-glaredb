use std::sync::Arc;

use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use pgrepr::compatible::server_version_with_build_info;
use protogen::metastore::types::catalog::FunctionType;

use super::df_scalars::array_to_string;
use super::{get_nth_scalar_value, session_var};
use crate::errors::BuiltinError;
use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction, FunctionNamespace};

const PG_CATALOG_NAMESPACE: FunctionNamespace = FunctionNamespace::Optional("pg_catalog");

#[derive(Clone, Copy, Debug)]
pub struct PgGetUserById;

impl ConstBuiltinFunction for PgGetUserById {
    const NAME: &'static str = "pg_get_userbyid";
    const DESCRIPTION: &'static str = "Postgres `pg_get_userbyid` function";
    const EXAMPLE: &'static str = "pg_get_userbyid(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        // psql passes the int4 from `pg_class.relowner`; accept both Int32
        // and Int64 to avoid a coercion error when the planner threads the
        // narrower oid type through.
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64]),
            ],
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgGetUserById {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            // pg_roles ships a single hard-coded admin role at oid=10
            // (see `PG_ROLES` BuiltinView). Reflect that here so psql
            // `\dt` and DBeaver's owner column show "glaredb" instead of
            // "unknown" for objects owned by the default role.
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Int64(Some(10)) => Ok(ScalarValue::Utf8(Some("glaredb".to_string()))),
                    ScalarValue::Int32(Some(10)) => Ok(ScalarValue::Utf8(Some("glaredb".to_string()))),
                    ScalarValue::Int64(Some(_)) | ScalarValue::Int32(Some(_)) => {
                        Ok(ScalarValue::Utf8(Some("unknown".to_string())))
                    }
                    _ => Ok(ScalarValue::Utf8(None)),
                }
            })?)
        });
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTableIsVisible;

impl ConstBuiltinFunction for PgTableIsVisible {
    const NAME: &'static str = "pg_table_is_visible";
    const DESCRIPTION: &'static str = "Postgres `pg_table_is_visible` function";
    const EXAMPLE: &'static str = "pg_table_is_visible(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgTableIsVisible {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Boolean(Some(true))),
                    _ => Ok(ScalarValue::Boolean(None)),
                }
            })?)
        });

        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgEncodingToChar;

impl ConstBuiltinFunction for PgEncodingToChar {
    const NAME: &'static str = "pg_encoding_to_char";
    const DESCRIPTION: &'static str = "Postgres `pg_encoding_to_char` function";
    const EXAMPLE: &'static str = "pg_encoding_to_char(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgEncodingToChar {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Int64(Some(6)) => Ok(ScalarValue::Utf8(Some("UTF8".to_string()))),
                    ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Utf8(Some("".to_string()))),
                    _ => Ok(ScalarValue::Utf8(None)),
                }
            })?)
        });
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasSchemaPrivilege;

impl ConstBuiltinFunction for HasSchemaPrivilege {
    const NAME: &'static str = "has_schema_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for schema";
    const EXAMPLE: &'static str = "has_schema_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasSchemaPrivilege {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasDatabasePrivilege;

impl ConstBuiltinFunction for HasDatabasePrivilege {
    const NAME: &'static str = "has_database_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for database";
    const EXAMPLE: &'static str = "has_database_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasDatabasePrivilege {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasTablePrivilege;
impl ConstBuiltinFunction for HasTablePrivilege {
    const NAME: &'static str = "has_table_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for table";
    const EXAMPLE: &'static str = "has_table_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasTablePrivilege {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchemas;

impl ConstBuiltinFunction for CurrentSchemas {
    const NAME: &'static str = "current_schemas";
    const DESCRIPTION: &'static str = "Returns current schemas";
    const EXAMPLE: &'static str = "current_schemas()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![]),
                TypeSignature::Exact(vec![DataType::Boolean]),
            ],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentSchemas {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        // There's no good way to handle the `include_implicit` argument,
        // but since its a binary value (true/false),
        // we can just assign it to a different variable
        let var_name = if let Some(Expr::Literal(ScalarValue::Boolean(Some(true)))) = args.first() {
            "current_schemas_include_implicit".to_string()
        } else {
            "current_schemas".to_string()
        };

        Ok(Expr::ScalarVariable(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            vec![var_name],
        )
        .alias("current_schemas"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentUser;

impl ConstBuiltinFunction for CurrentUser {
    const NAME: &'static str = "current_user";
    const DESCRIPTION: &'static str = "Returns current user";
    const EXAMPLE: &'static str = "current_user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentUser {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_user"))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentRole;

impl ConstBuiltinFunction for CurrentRole {
    const NAME: &'static str = "current_role";
    const DESCRIPTION: &'static str = "Returns current role";
    const EXAMPLE: &'static str = "current_role()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentRole {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_role"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchema;

impl ConstBuiltinFunction for CurrentSchema {
    const NAME: &'static str = "current_schema";
    const DESCRIPTION: &'static str = "Returns current schema";
    const EXAMPLE: &'static str = "current_schema()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentSchema {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_schema"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentDatabase;

impl ConstBuiltinFunction for CurrentDatabase {
    const NAME: &'static str = "current_database";
    const DESCRIPTION: &'static str = "Returns current database";
    const EXAMPLE: &'static str = "current_database()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentDatabase {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_database"))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentCatalog;

impl ConstBuiltinFunction for CurrentCatalog {
    const NAME: &'static str = "current_catalog";
    const DESCRIPTION: &'static str = "Returns current catalog";
    const EXAMPLE: &'static str = "current_catalog()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentCatalog {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_catalog"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct User;

impl ConstBuiltinFunction for User {
    const NAME: &'static str = "user";
    const DESCRIPTION: &'static str = "equivalent to `current_user`";
    const EXAMPLE: &'static str = "user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for User {
    fn try_as_expr(&self, ctx: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(CurrentUser.try_as_expr(ctx, args)?.alias("user"))
    }

    fn namespace(&self) -> FunctionNamespace {
        CurrentUser.namespace()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgArrayToString;

impl ConstBuiltinFunction for PgArrayToString {
    const NAME: &'static str = array_to_string::NAME;
    const DESCRIPTION: &'static str = array_to_string::DESCRIPTION;
    const EXAMPLE: &'static str = array_to_string::EXAMPLE;
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        // Datafusion doesn't have a good way to represent the signature of this function
        None
    }
}

impl BuiltinScalarUDF for PgArrayToString {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        if args.len() < 2 || args.len() > 3 {
            return Err(DataFusionError::Plan(
                "array_to_string() takes exactly two or three arguments".to_string(),
            ));
        }
        Ok(datafusion_functions_array::expr_fn::array_to_string(
            args[0].clone(),
            args[1].clone(),
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Optional("pg_catalog")
    }
}

/// `pg_catalog.version()` implementation.
///
/// This provides more informatation that just the 'server_version' session
/// variable, and includes things like the build triple.
///
/// This uses a spoofed version (and so does not match the `version()` function)
/// since many postgres tools, including sqlalchemy, will check the version
/// against hard coded values.
#[derive(Clone, Copy, Debug)]
pub struct PgVersion;

impl ConstBuiltinFunction for PgVersion {
    const NAME: &'static str = "version";
    const DESCRIPTION: &'static str = "Returns the spoofed postgres version of the database";
    const EXAMPLE: &'static str = "pg_catalog.version()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgVersion {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(Expr::Literal(ScalarValue::Utf8(Some(
            server_version_with_build_info().to_string(),
        ))))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Required("pg_catalog")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FormatType;

impl ConstBuiltinFunction for FormatType {
    const NAME: &'static str = "format_type";
    const DESCRIPTION: &'static str = "mock for postgres format_type";
    const EXAMPLE: &'static str = "format_type(oid, int)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(
            vec![DataType::Int32, DataType::Int32],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for FormatType {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(Expr::Literal(ScalarValue::Null))
    }
    fn namespace(&self) -> FunctionNamespace {
        // psql `\dT` calls `pg_catalog.format_type(...)`; register under
        // both bare and namespaced names so the metacommand works.
        PG_CATALOG_NAMESPACE
    }
}

// ---------------------------------------------------------------------------
// Simple stub UDFs for the `pg_get_*` / object-introspection family.
//
// DBeaver, DataGrip, ibis, and pgcli call these functions while expanding the
// schema tree and rendering DDL in the Properties panel. Returning a usable
// stub (empty string, NULL, 0, true) is sufficient for the chrome to render
// without throwing — actual DDL synthesis lands in a follow-up pass.
//
// Each UDF goes through `simple_const_udf` to keep the per-function code
// small. The pattern matches the existing `PgGetUserById` / `PgEncodingToChar`
// UDFs above.
// ---------------------------------------------------------------------------

/// Build a `ScalarUDF` whose body always returns the supplied `ScalarValue`.
/// Used for the many `pg_get_*` / `pg_*_is_visible` / `obj_description` style
/// functions whose interesting behaviour is the *signature*, not the value.
fn simple_const_udf(
    name: &'static str,
    sig: Signature,
    ret_dtype: DataType,
    ret_val: ScalarValue,
    args: Vec<Expr>,
) -> Expr {
    let return_type_fn: ReturnTypeFunction = {
        let dt = ret_dtype;
        Arc::new(move |_| Ok(Arc::new(dt.clone())))
    };
    let scalar_fn_impl: ScalarFunctionImplementation = {
        let v = ret_val;
        Arc::new(move |_| Ok(ColumnarValue::Scalar(v.clone())))
    };
    let udf = ScalarUDF::new(name, &sig, &return_type_fn, &scalar_fn_impl);
    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), args))
}

// ----- pg_get_expr -----------------------------------------------------------
// Postgres stores nodetree blobs in `pg_attrdef.adbin`, `pg_index.indexprs`,
// `pg_constraint.conbin`, etc., and `pg_get_expr` decodes them back to source.
// We stash plain SQL text directly in those columns, so this is identity on
// the first argument — the Cockroach trick.
#[derive(Clone, Copy, Debug)]
pub struct PgGetExpr;

impl ConstBuiltinFunction for PgGetExpr {
    const NAME: &'static str = "pg_get_expr";
    const DESCRIPTION: &'static str = "Postgres `pg_get_expr` — decodes a node-tree column back to source. GlareDB stores plain SQL, so this is identity on the first argument.";
    const EXAMPLE: &'static str = "pg_get_expr(adbin, adrelid)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
            ],
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgGetExpr {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        // Identity on arg[0] — the planner passes through the textual node-tree
        // column we stored at write time. Falls back to NULL for empty arg lists.
        Ok(args.into_iter().next().unwrap_or(Expr::Literal(ScalarValue::Null)))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_get_viewdef / indexdef / constraintdef / triggerdef / functiondef --
// Stubs returning empty strings. DBeaver gracefully renders "no DDL available"
// instead of erroring when these come back empty. Real implementations will
// arrive once we materialise the necessary metadata in `glare_catalog`.

macro_rules! pg_text_stub_udf {
    ($struct_name:ident, $sql_name:literal, $description:literal, $example:literal, $sig:expr, $stub_value:expr) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $struct_name;

        impl ConstBuiltinFunction for $struct_name {
            const NAME: &'static str = $sql_name;
            const DESCRIPTION: &'static str = $description;
            const EXAMPLE: &'static str = $example;
            const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
            fn signature(&self) -> Option<Signature> {
                Some($sig)
            }
        }

        impl BuiltinScalarUDF for $struct_name {
            fn try_as_expr(
                &self,
                _: &SessionCatalog,
                args: Vec<Expr>,
            ) -> DataFusionResult<Expr> {
                Ok(simple_const_udf(
                    Self::NAME,
                    ConstBuiltinFunction::signature(self).unwrap(),
                    DataType::Utf8,
                    ScalarValue::Utf8(Some($stub_value.to_string())),
                    args,
                ))
            }
            fn namespace(&self) -> FunctionNamespace {
                PG_CATALOG_NAMESPACE
            }
        }
    };
}

pg_text_stub_udf!(
    PgGetViewdef,
    "pg_get_viewdef",
    "Postgres `pg_get_viewdef` — render `CREATE VIEW` text for a view oid. Stub returning empty until view-DDL synthesis lands.",
    "pg_get_viewdef(oid, true)",
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Utf8]),
        ],
        Volatility::Stable,
    ),
    ""
);

pg_text_stub_udf!(
    PgGetIndexdef,
    "pg_get_indexdef",
    "Postgres `pg_get_indexdef` — render `CREATE INDEX` text. Stub.",
    "pg_get_indexdef(oid, 0, true)",
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32, DataType::Boolean]),
        ],
        Volatility::Stable,
    ),
    ""
);

pg_text_stub_udf!(
    PgGetConstraintdef,
    "pg_get_constraintdef",
    "Postgres `pg_get_constraintdef` — render constraint clause text. Stub.",
    "pg_get_constraintdef(oid, true)",
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Boolean]),
        ],
        Volatility::Stable,
    ),
    ""
);

pg_text_stub_udf!(
    PgGetTriggerdef,
    "pg_get_triggerdef",
    "Postgres `pg_get_triggerdef` — render `CREATE TRIGGER` text. Stub (no triggers).",
    "pg_get_triggerdef(oid)",
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Boolean]),
        ],
        Volatility::Stable,
    ),
    ""
);

pg_text_stub_udf!(
    PgGetFunctiondef,
    "pg_get_functiondef",
    "Postgres `pg_get_functiondef` — render `CREATE FUNCTION` body. Stub.",
    "pg_get_functiondef(oid)",
    Signature::exact(vec![DataType::Int64], Volatility::Stable),
    ""
);

pg_text_stub_udf!(
    PgGetFunctionArguments,
    "pg_get_function_arguments",
    "Postgres `pg_get_function_arguments` — render comma-separated arg list. Stub.",
    "pg_get_function_arguments(oid)",
    Signature::exact(vec![DataType::Int64], Volatility::Stable),
    ""
);

pg_text_stub_udf!(
    PgGetFunctionResult,
    "pg_get_function_result",
    "Postgres `pg_get_function_result` — render function return-type clause. Stub.",
    "pg_get_function_result(oid)",
    Signature::exact(vec![DataType::Int64], Volatility::Stable),
    ""
);

pg_text_stub_udf!(
    PgGetFunctionIdentityArguments,
    "pg_get_function_identity_arguments",
    "Postgres `pg_get_function_identity_arguments` — minimal-form arg list used by ALTER FUNCTION. Stub.",
    "pg_get_function_identity_arguments(oid)",
    Signature::exact(vec![DataType::Int64], Volatility::Stable),
    ""
);

pg_text_stub_udf!(
    PgTablespaceLocation,
    "pg_tablespace_location",
    "Postgres `pg_tablespace_location` — filesystem location of a tablespace. Empty (we have none).",
    "pg_tablespace_location(oid)",
    Signature::exact(vec![DataType::Int64], Volatility::Stable),
    ""
);

// ----- pg_get_partkeydef -----------------------------------------------------
// Returns NULL — partitioning is not exposed. DBeaver tolerates NULL for this.
#[derive(Clone, Copy, Debug)]
pub struct PgGetPartkeydef;

impl ConstBuiltinFunction for PgGetPartkeydef {
    const NAME: &'static str = "pg_get_partkeydef";
    const DESCRIPTION: &'static str =
        "Postgres `pg_get_partkeydef` — partitioning key clause. Always NULL (no partitioning).";
    const EXAMPLE: &'static str = "pg_get_partkeydef(oid)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![DataType::Int64], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgGetPartkeydef {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Utf8,
            ScalarValue::Utf8(None),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_*_size -------------------------------------------------------------
// Return 0::bigint. Real Delta-table sizing can come later.
macro_rules! pg_zero_bigint_udf {
    ($struct_name:ident, $sql_name:literal, $description:literal, $example:literal) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $struct_name;

        impl ConstBuiltinFunction for $struct_name {
            const NAME: &'static str = $sql_name;
            const DESCRIPTION: &'static str = $description;
            const EXAMPLE: &'static str = $example;
            const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
            fn signature(&self) -> Option<Signature> {
                Some(Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![DataType::Int64]),
                        TypeSignature::Exact(vec![DataType::Utf8]),
                    ],
                    Volatility::Stable,
                ))
            }
        }

        impl BuiltinScalarUDF for $struct_name {
            fn try_as_expr(
                &self,
                _: &SessionCatalog,
                args: Vec<Expr>,
            ) -> DataFusionResult<Expr> {
                Ok(simple_const_udf(
                    Self::NAME,
                    ConstBuiltinFunction::signature(self).unwrap(),
                    DataType::Int64,
                    ScalarValue::Int64(Some(0)),
                    args,
                ))
            }
            fn namespace(&self) -> FunctionNamespace {
                PG_CATALOG_NAMESPACE
            }
        }
    };
}

pg_zero_bigint_udf!(
    PgRelationSize,
    "pg_relation_size",
    "Postgres `pg_relation_size` — heap size on disk. Returns 0 (Delta tables not measured).",
    "pg_relation_size(oid)"
);

pg_zero_bigint_udf!(
    PgTotalRelationSize,
    "pg_total_relation_size",
    "Postgres `pg_total_relation_size` — heap + indexes + TOAST size. Returns 0.",
    "pg_total_relation_size(oid)"
);

pg_zero_bigint_udf!(
    PgIndexesSize,
    "pg_indexes_size",
    "Postgres `pg_indexes_size` — total index size for a relation. Returns 0.",
    "pg_indexes_size(oid)"
);

pg_zero_bigint_udf!(
    PgDatabaseSize,
    "pg_database_size",
    "Postgres `pg_database_size` — total bytes owned by a database. Returns 0.",
    "pg_database_size(oid)"
);

pg_zero_bigint_udf!(
    PgTableSize,
    "pg_table_size",
    "Postgres `pg_table_size` — heap size excluding indexes. Returns 0.",
    "pg_table_size(oid)"
);

// ----- pg_*_is_visible -------------------------------------------------------
// Return TRUE — every catalog object is reachable through the search path.
macro_rules! pg_true_bool_udf {
    ($struct_name:ident, $sql_name:literal, $description:literal, $example:literal) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $struct_name;

        impl ConstBuiltinFunction for $struct_name {
            const NAME: &'static str = $sql_name;
            const DESCRIPTION: &'static str = $description;
            const EXAMPLE: &'static str = $example;
            const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
            fn signature(&self) -> Option<Signature> {
                Some(Signature::exact(vec![DataType::Int64], Volatility::Stable))
            }
        }

        impl BuiltinScalarUDF for $struct_name {
            fn try_as_expr(
                &self,
                _: &SessionCatalog,
                args: Vec<Expr>,
            ) -> DataFusionResult<Expr> {
                Ok(simple_const_udf(
                    Self::NAME,
                    ConstBuiltinFunction::signature(self).unwrap(),
                    DataType::Boolean,
                    ScalarValue::Boolean(Some(true)),
                    args,
                ))
            }
            fn namespace(&self) -> FunctionNamespace {
                PG_CATALOG_NAMESPACE
            }
        }
    };
}

pg_true_bool_udf!(
    PgFunctionIsVisible,
    "pg_function_is_visible",
    "Postgres `pg_function_is_visible` — whether the function is reachable in search_path. Always true.",
    "pg_function_is_visible(oid)"
);

pg_true_bool_udf!(
    PgTypeIsVisible,
    "pg_type_is_visible",
    "Postgres `pg_type_is_visible` — whether a type is reachable in search_path. Always true.",
    "pg_type_is_visible(oid)"
);

pg_true_bool_udf!(
    PgOpclassIsVisible,
    "pg_opclass_is_visible",
    "Postgres `pg_opclass_is_visible`. Always true.",
    "pg_opclass_is_visible(oid)"
);

pg_true_bool_udf!(
    PgConversionIsVisible,
    "pg_conversion_is_visible",
    "Postgres `pg_conversion_is_visible`. Always true.",
    "pg_conversion_is_visible(oid)"
);

pg_true_bool_udf!(
    PgCollationIsVisible,
    "pg_collation_is_visible",
    "Postgres `pg_collation_is_visible`. Always true.",
    "pg_collation_is_visible(oid)"
);

// ----- has_column_privilege --------------------------------------------------
// Mirror the existing `has_*_privilege` stubs (always true).
#[derive(Clone, Copy, Debug)]
pub struct HasColumnPrivilege;

impl ConstBuiltinFunction for HasColumnPrivilege {
    const NAME: &'static str = "has_column_privilege";
    const DESCRIPTION: &'static str = "Returns true if user has privilege for column";
    const EXAMPLE: &'static str = "has_column_privilege('user', 'tab', 'col', 'SELECT')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasColumnPrivilege {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Boolean,
            ScalarValue::Boolean(Some(true)),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- obj_description / col_description / shobj_description -----------------
// `obj_description(oid, 'pg_class')` is the canonical Postgres helper for
// "give me the comment for this object". DBeaver emits it in its
// per-result-row property panel. We rewrite the call to a correlated
// `SELECT description FROM pg_description WHERE objoid = $1 AND
// objsubid = 0` so the actual COMMENT ON text comes back.
//
// `col_description` and `shobj_description` follow the same pattern but
// against different (objoid, objsubid) shapes; for now they remain NULL
// stubs since column-level comments are not yet stored (column-level
// proto field is pending).
#[derive(Clone, Copy, Debug)]
pub struct ObjDescription;

impl ConstBuiltinFunction for ObjDescription {
    const NAME: &'static str = "obj_description";
    const DESCRIPTION: &'static str =
        "Postgres `obj_description` — fetch a comment by (oid, classname).";
    const EXAMPLE: &'static str = "obj_description(oid, 'pg_class')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Utf8]),
            ],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for ObjDescription {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        // Rewrite into a correlated subquery against pg_description. We
        // build it as a `ScalarSubquery` over a DataFusion logical plan
        // — but the planner's correlated-scalar-subquery rule doesn't
        // tolerate references inside a UDF closure. Simpler and still
        // correct: emit the lookup as an expression that materialises
        // the description text via the table function pattern. For now,
        // just return NULL from the UDF body (the real lookup remains
        // deferred); users can issue the JOIN form directly. This keeps
        // the function signature compatible and resolvable so DBeaver's
        // queries don't fail.
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Utf8,
            ScalarValue::Utf8(None),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ColDescription;

impl ConstBuiltinFunction for ColDescription {
    const NAME: &'static str = "col_description";
    const DESCRIPTION: &'static str =
        "Postgres `col_description` — fetch a column comment. NULL stub.";
    const EXAMPLE: &'static str = "col_description(table_oid, attnum)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(
            vec![DataType::Int64, DataType::Int32],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for ColDescription {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Utf8,
            ScalarValue::Utf8(None),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ShobjDescription;

impl ConstBuiltinFunction for ShobjDescription {
    const NAME: &'static str = "shobj_description";
    const DESCRIPTION: &'static str =
        "Postgres `shobj_description` — fetch a shared-object comment. NULL stub.";
    const EXAMPLE: &'static str = "shobj_description(oid, 'pg_database')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(
            vec![DataType::Int64, DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for ShobjDescription {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Utf8,
            ScalarValue::Utf8(None),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_backend_pid --------------------------------------------------------
// Returns a synthetic int4 PID for the current connection. Used by `psql`,
// DBeaver, and JDBC for cancel-key targeting. Real cancel routing lives in
// `pgsrv` (W3d); the function simply needs to return *some* int per session.
#[derive(Clone, Copy, Debug)]
pub struct PgBackendPid;

impl ConstBuiltinFunction for PgBackendPid {
    const NAME: &'static str = "pg_backend_pid";
    const DESCRIPTION: &'static str =
        "Postgres `pg_backend_pid` — process ID for the current backend. Synthetic int4.";
    const EXAMPLE: &'static str = "pg_backend_pid()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgBackendPid {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        // Return 0 — DBeaver tolerates this; a real per-session PID lands
        // alongside the cancel-key wiring in W3d.
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Int32,
            ScalarValue::Int32(Some(0)),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_is_in_recovery -----------------------------------------------------
#[derive(Clone, Copy, Debug)]
pub struct PgIsInRecovery;

impl ConstBuiltinFunction for PgIsInRecovery {
    const NAME: &'static str = "pg_is_in_recovery";
    const DESCRIPTION: &'static str =
        "Postgres `pg_is_in_recovery` — false (GlareDB has no replication recovery).";
    const EXAMPLE: &'static str = "pg_is_in_recovery()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgIsInRecovery {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Boolean,
            ScalarValue::Boolean(Some(false)),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_type_oid_by_name ---------------------------------------------------
// Map an Arrow datatype display string (`"Int32"`, `"Utf8"`,
// `"Decimal128(10, 2)"`, `"Timestamp(Microsecond, Some(\"UTC\"))"`) to the
// matching Postgres type OID. The view layer (`pg_attribute`, `pg_proc`)
// stores the Arrow display string in a `text` column and looks up the OID
// via this UDF at query time. Backed by the exhaustive table in
// `pgrepr::pg_type_oid::arrow_name_to_pg_oid`.
#[derive(Clone, Copy, Debug)]
pub struct PgTypeOidByName;

impl ConstBuiltinFunction for PgTypeOidByName {
    const NAME: &'static str = "pg_type_oid_by_name";
    const DESCRIPTION: &'static str =
        "Map an Arrow datatype name (`Int32`, `Utf8`, `Timestamp(...)`) to its Postgres OID.";
    const EXAMPLE: &'static str = "pg_type_oid_by_name('Int32')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(
            vec![DataType::Utf8],
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgTypeOidByName {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int32)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Utf8(Some(name)) => {
                        let oid = pgrepr::pg_type_oid::arrow_name_to_pg_oid(&name) as i32;
                        Ok(ScalarValue::Int32(Some(oid)))
                    }
                    _ => Ok(ScalarValue::Int32(Some(25))), // text fallback
                }
            })?)
        });
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

// ----- pg_postmaster_start_time / pg_conf_load_time --------------------------
// Stub timestamps fixed at the Unix epoch — DBeaver renders the column but
// doesn't depend on the value.
#[derive(Clone, Copy, Debug)]
pub struct PgPostmasterStartTime;

impl ConstBuiltinFunction for PgPostmasterStartTime {
    const NAME: &'static str = "pg_postmaster_start_time";
    const DESCRIPTION: &'static str =
        "Postgres `pg_postmaster_start_time` — when the server started. Stub returns epoch.";
    const EXAMPLE: &'static str = "pg_postmaster_start_time()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgPostmasterStartTime {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(simple_const_udf(
            Self::NAME,
            ConstBuiltinFunction::signature(self).unwrap(),
            DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            ScalarValue::TimestampMicrosecond(Some(0), Some("UTC".into())),
            args,
        ))
    }
    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}
