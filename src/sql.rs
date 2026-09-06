//! The caller's SQL: a projection list and a row predicate.
//!
//! Expressions, never statements. Each piece is parsed on its own — one select item, one
//! boolean expression — and the parser must reach the end of the string, so there is no
//! `FROM` to hang a join off and no way to write a second query into either field. The
//! target is named by the request's path and its identity by `url`, neither of which the
//! expressions can reach.
//!
//! Parsing as an expression is not the whole check, though: an aggregate, a window
//! function and a call to `random()` are all expressions. So what the planner returns is
//! walked, and only the node kinds this service will actually run are let through.

use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, Volatility};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, ExprWithAlias};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::error::ApiError;

/// How deeply a caller's expression may nest. Enforced by the parser, so a pathological
/// input is refused while it is still text rather than after it has grown a stack of
/// planner frames. DataFusion's own default for the same limit.
const MAX_DEPTH: usize = 50;

/// How many nodes one expression may have.
///
/// Depth does not bound this: an `IN` list is one node wide and arbitrarily long, and a
/// chain of `OR`s is shallow. The cap is generous because a list of ten thousand object
/// ids is a request this service exists to answer — it is here to refuse the absurd
/// cheaply, not to be a budget anyone tunes.
const MAX_NODES: usize = 50_000;

/// One dialect, borrowed for the lifetime of the process so that a parser built from it
/// is not tied to the stack frame that made it.
static DIALECT: GenericDialect = GenericDialect;

/// The select list, as one expression per output column.
///
/// A bare dotted path is aliased to the path the caller wrote. Unaliased, DataFusion
/// names the column after the access it planned — `lightcurve.mag` comes back as
/// `t.lightcurve[mag]` — and the caller then has to guess the key their own request
/// produced.
pub fn projection(
    state: &SessionState,
    schema: &DFSchema,
    sql: &str,
) -> Result<Vec<Expr>, ApiError> {
    const FIELD: &str = "select";

    if sql.trim() == "*" {
        return Err(ApiError::bad_request(
            "omit select rather than writing *: absent means every column",
        ));
    }
    let items = parse(sql, FIELD, |parser| {
        parser.parse_comma_separated(Parser::parse_expr_with_alias)
    })?;
    items
        .into_iter()
        .map(|item| {
            let written = match item.alias {
                Some(_) => None,
                None => dotted_name(&item.expr),
            };
            let expr = plan(state, schema, item, FIELD)?;
            Ok(match written {
                Some(name) => expr.alias(name),
                None => expr,
            })
        })
        .collect()
}

/// The row predicate: one boolean expression, no alias and no second expression after it.
pub fn predicate(state: &SessionState, schema: &DFSchema, sql: &str) -> Result<Expr, ApiError> {
    const FIELD: &str = "where";

    let expr = parse(sql, FIELD, Parser::parse_expr)?;
    plan(state, schema, ExprWithAlias { expr, alias: None }, FIELD)
}

/// The dotted path a caller wrote, for a select item that is only a path.
fn dotted_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}

/// Run one of `sqlparser`'s own parsers over the whole string.
///
/// The end-of-input check is what makes this a parser for an expression rather than for
/// the first expression in something longer: without it, `1 UNION SELECT …` parses as
/// `1` and the rest is silently dropped.
fn parse<T>(
    sql: &str,
    field: &str,
    parse: impl FnOnce(&mut Parser<'static>) -> Result<T, ParserError>,
) -> Result<T, ApiError> {
    let refuse = |error: &ParserError| {
        ApiError::bad_request(format!("{field} is not a SQL expression: {error}"))
    };
    let mut parser = Parser::new(&DIALECT)
        .with_recursion_limit(MAX_DEPTH)
        .try_with_sql(sql)
        .map_err(|error| refuse(&error))?;
    let parsed = parse(&mut parser).map_err(|error| refuse(&error))?;
    if parser.peek_token().token != Token::EOF {
        return Err(ApiError::bad_request(format!(
            "{field} must be one expression, and this one does not end where it should"
        )));
    }
    Ok(parsed)
}

/// Plan one parsed expression against the file's schema, then check what came back.
///
/// The schema is what makes a literal typed — `objectid = 1383212200036217` against an
/// `Int64` column plans to an `Int64` literal, which is what row-group statistics, the
/// page index and a bloom filter can all prune on. Compared as a string it would read
/// the whole file and return nothing.
fn plan(
    state: &SessionState,
    schema: &DFSchema,
    expr: ExprWithAlias,
    field: &str,
) -> Result<Expr, ApiError> {
    // Every failure here is the caller's expression not fitting the caller's file:
    // an unknown column, a type that will not compare, a function that is not
    // registered.
    let expr = state
        .create_logical_expr_from_sql_expr(expr, schema)
        .map_err(|error| ApiError::bad_request(format!("{field}: {error}")))?;
    check(&expr, field)?;
    Ok(expr)
}

/// Walk the planned expression and refuse everything this service will not run.
fn check(expr: &Expr, field: &str) -> Result<(), ApiError> {
    let mut nodes = 0usize;
    let mut refusal = None;
    // The closure cannot fail, so the walk itself cannot: the refusal is carried out
    // rather than raised, and the walk stops at the first one.
    let _ = expr.apply(|node| {
        nodes += 1;
        if nodes > MAX_NODES {
            refusal = Some(format!("{field} is too large: more than {MAX_NODES} terms"));
            return Ok(TreeNodeRecursion::Stop);
        }
        match allowed(node) {
            Ok(()) => Ok(TreeNodeRecursion::Continue),
            Err(reason) => {
                refusal = Some(format!("{field}: {reason}"));
                Ok(TreeNodeRecursion::Stop)
            }
        }
    });
    match refusal {
        Some(reason) => Err(ApiError::bad_request(reason)),
        None => Ok(()),
    }
}

/// One node of a planned expression, judged.
///
/// Written as an allowlist with an exhaustive match rather than as a list of what is
/// refused: a DataFusion upgrade that adds an expression kind is then a compile error
/// here, and someone decides whether it belongs in a per-row expression instead of a
/// caller finding out that it already did.
fn allowed(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::Alias(_)
        | Expr::Column(_)
        | Expr::Literal(..)
        | Expr::BinaryExpr(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::Not(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::Between(_)
        | Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::InList(_) => Ok(()),

        // A function is allowed by its volatility rather than by name, so the rule holds
        // for one this crate never compiled in. Only `Immutable` qualifies: `random()`
        // is `Volatile` and `now()` is `Stable`, and both make one request's answer
        // differ from the next's for the same query — which is a wrong answer to cache
        // and a wrong answer to reproduce from a plan.
        Expr::ScalarFunction(call) => match call.func.signature().volatility {
            Volatility::Immutable => Ok(()),
            Volatility::Stable | Volatility::Volatile => Err(format!(
                "{}() does not answer the same way twice, so it cannot be used here",
                call.func.name()
            )),
        },

        Expr::AggregateFunction(_) | Expr::WindowFunction(_) | Expr::GroupingSet(_) => Err(
            "this is an expression over one row; aggregates, window functions and \
             grouping sets summarize many"
                .to_owned(),
        ),
        Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::SetComparison(_)
        | Expr::OuterReferenceColumn(..) => {
            Err("a subquery reads a second table, and a request names one".to_owned())
        }
        Expr::ScalarVariable(..) | Expr::Placeholder(_) => {
            Err("a variable has no value here; write the value itself".to_owned())
        }
        // Deprecated in DataFusion and unreachable through `projection`, which refuses a
        // bare `*` with the same words before parsing. Matched anyway, because the
        // exhaustive match is the point: the variant still exists, and `t.*` reaches it.
        #[expect(deprecated, reason = "the variant is still constructible")]
        Expr::Wildcard { .. } => {
            Err("omit select rather than writing *: absent means every column".to_owned())
        }
        Expr::Unnest(_) => {
            Err("unnest turns one row into many, so it is not an expression here".to_owned())
        }
        Expr::HigherOrderFunction(_) | Expr::Lambda(_) | Expr::LambdaVariable(_) => {
            Err("lambdas are not supported here".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::prelude::SessionContext;

    use super::*;

    /// A cut-down ZTF HATS schema: scalars, a mixed-case name, and a struct of lists.
    fn schema() -> DFSchema {
        let list = |name: &str| {
            Field::new(
                name,
                DataType::List(Arc::new(Field::new("element", DataType::Float32, true))),
                true,
            )
        };
        let lightcurve = DataType::Struct(Fields::from(vec![list("mag"), list("magerr")]));
        DFSchema::try_from(Schema::new(vec![
            Field::new("objectid", DataType::Int64, false),
            Field::new("objra", DataType::Float64, true),
            Field::new("Gmag", DataType::Float64, true),
            Field::new("lightcurve", lightcurve, true),
        ]))
        .unwrap()
    }

    /// A context configured the way a request's is, which is what decides whether an
    /// unquoted identifier keeps its case.
    fn state() -> SessionState {
        SessionContext::new_with_config(crate::query::session_config()).state()
    }

    fn names(sql: &str) -> Result<Vec<String>, ApiError> {
        Ok(projection(&state(), &schema(), sql)?
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect())
    }

    fn filter(sql: &str) -> Result<String, ApiError> {
        Ok(predicate(&state(), &schema(), sql)?.to_string())
    }

    #[test]
    fn a_select_list_keeps_the_names_the_caller_wrote() {
        assert_eq!(
            names("objectid, lightcurve.mag, objra AS ra").unwrap(),
            ["objectid", "lightcurve.mag", "ra"]
        );
    }

    #[test]
    fn a_select_item_can_compute() {
        assert_eq!(names("objra - 0.1 AS ra_corr").unwrap(), ["ra_corr"]);
    }

    /// Astronomy catalogs are full of names like `Gmag`, `Norder` and `objectId`, and a
    /// parser that lowercased them would report every one of them as missing.
    #[test]
    fn identifiers_keep_their_case() {
        assert_eq!(names("Gmag").unwrap(), ["Gmag"]);
        assert!(filter("Gmag < 20").is_ok());
        let error = names("gmag").unwrap_err().to_string();
        assert!(error.contains("gmag"), "{error}");
    }

    #[test]
    fn a_predicate_is_planned_against_the_column_types() {
        let planned = filter("objectid IN (1383212200036217, 1383212200036218)").unwrap();
        assert!(planned.contains("1383212200036217"), "{planned}");
        assert!(filter("objra BETWEEN 1.0 AND 2.0 AND objectid IS NOT NULL").is_ok());
    }

    /// Neither field is the head of a statement: what follows the expression is not
    /// quietly dropped.
    #[test]
    fn nothing_may_follow_the_expression() {
        for sql in [
            "objectid = 1 UNION SELECT 1",
            "objectid = 1; DROP TABLE t",
            "objectid = 1 FROM other",
        ] {
            let error = filter(sql).unwrap_err().to_string();
            assert!(error.contains("where"), "{sql}: {error}");
        }
    }

    #[test]
    fn a_select_item_cannot_smuggle_a_from() {
        let error = names("objectid FROM other").unwrap_err().to_string();
        assert!(error.contains("select"), "{error}");
    }

    #[test]
    fn statements_are_not_expressions() {
        for sql in ["EXISTS (SELECT 1)", "objectid IN (SELECT x FROM other)"] {
            assert!(filter(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn an_empty_expression_is_refused() {
        assert!(names("").is_err());
        assert!(filter("").is_err());
        assert!(filter("   ").is_err());
    }

    #[test]
    fn a_star_says_to_omit_the_field() {
        let error = names("*").unwrap_err().to_string();
        assert!(error.contains("omit select"), "{error}");
    }

    /// An unknown column is the caller's mistake, and is named back to them.
    #[test]
    fn an_unknown_column_is_a_bad_request() {
        let error = filter("nope = 1").unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert!(error.to_string().contains("nope"), "{error}");
    }

    #[test]
    fn a_predicate_may_not_summarize_rows() {
        let error = filter("count(objectid) > 1").unwrap_err().to_string();
        assert!(error.contains("one row"), "{error}");
    }

    /// The node cap is not depth: this is two levels deep and enormous.
    #[test]
    fn an_absurdly_long_list_is_refused() {
        let list = (0..MAX_NODES + 2)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let error = filter(&format!("objectid IN ({list})"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("too large"), "{error}");
        // And a list of the size this service is for is not.
        let list = (0..1000)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        assert!(filter(&format!("objectid IN ({list})")).is_ok());
    }

    #[test]
    fn deep_nesting_is_refused_by_the_parser() {
        let deep = format!(
            "{}objectid = 1{}",
            "(".repeat(MAX_DEPTH + 10),
            ")".repeat(MAX_DEPTH + 10)
        );
        assert!(filter(&deep).is_err());
    }

    /// `get_field` is what a dotted path plans to, so the volatility rule has to let it
    /// through — a rule that refused it would refuse every nested column.
    #[test]
    fn the_nested_field_access_is_allowed() {
        assert!(allowed(&projection(&state(), &schema(), "lightcurve.mag").unwrap()[0]).is_ok());
    }
}
