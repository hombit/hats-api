//! ADQL, translated into the SQL DataFusion plans.
//!
//! **A translation, not an interpreter.** ADQL is SQL with a handful of differences in how
//! things are spelled, and DataFusion already plans SQL: grouping, ordering, joins, subqueries
//! and set operations all come from the planner below, with nothing here deciding what they
//! mean. What this module does is rewrite the parsed statement where ADQL and DataFusion
//! disagree about a spelling, and refuse what neither side would answer correctly — and then
//! hand the statement on as a syntax tree, never as text.
//!
//! The differences it takes care of:
//!
//! - **`TOP n`** is ADQL's row limit, and becomes `LIMIT n`.
//! - **A region test compares with 1**, ADQL having no boolean: `1 = CONTAINS(p, r)` is the
//!   `contains(p, r)` that [`crate::geometry`] registers, and `0 = CONTAINS(p, r)` its
//!   negation. `INTERSECTS` against a point is the same test. `DISTANCE(p, c) < r` is the
//!   circle of radius `r` around `c`, said as a region test so that it prunes like one.
//! - **Four function names mean something else to DataFusion**: `CEILING`, `TRUNCATE`,
//!   `LOG` — the natural logarithm in ADQL — and `MOD`, which DataFusion writes as `%`.
//!
//! Everything ADQL has that this service does not answer is refused by name, so a caller is
//! told what was not understood rather than handed an unknown-function error about it.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    LimitClause, ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableFactor, Top,
    TopQuantity, UnaryOperator, Value, VisitMut, VisitorMut,
};

use crate::error::ApiError;
use crate::sql;

/// The field a statement arrives in, and what every refusal here names first.
const FIELD: &str = "query";

/// ADQL's geometry that this service does not answer, and why — said by name, since none of
/// these is a function DataFusion has and its own account would be of an unknown function.
///
/// The geometry functions are an optional ADQL feature declared one form at a time, so
/// refusing these costs the service nothing it claims.
const REFUSED: &[(&str, &str)] = &[
    (
        "AREA",
        "it takes a geometry as a value, and no column here holds one",
    ),
    (
        "BOX",
        "its edges are great circles, where the zone region's run along parallels; near a pole \
         the two differ by degrees",
    ),
    (
        "CENTROID",
        "it takes a geometry as a value, and no column here holds one",
    ),
    (
        "COORD1",
        "a position is two columns of the file; name the column",
    ),
    (
        "COORD2",
        "a position is two columns of the file; name the column",
    ),
    (
        "COORDSYS",
        "positions here are degrees in one frame and nothing is converted between frames",
    ),
    (
        "IVO_GEOM_TRANSFORM",
        "positions here are degrees in one frame and nothing is converted between frames",
    ),
    ("POLYGON", "no polygon is tested here yet"),
    (
        "REGION",
        "it takes an STC-S string, which ADQL 2.1 deprecated",
    ),
    (
        "RAND",
        "an answer that differs every time it is asked is not one this service returns yet",
    ),
];

/// ADQL's names for functions DataFusion registers under another name.
///
/// Written out rather than passed through, because each is a silent wrong answer if it is not:
/// DataFusion's `log` is refused outright here for being base ten in some SQL and natural in
/// the rest, and ADQL is unambiguous that its `LOG` is natural.
const RENAMED: &[(&str, &str)] = &[("CEILING", "ceil"), ("LOG", "ln"), ("TRUNCATE", "trunc")];

/// A statement, translated, and the tables it reads.
#[derive(Debug)]
pub struct Translated {
    /// Ready for DataFusion to plan. Still a syntax tree: nothing here is rendered to text.
    pub statement: Statement,
    /// Every table the statement names, as written, less the ones it defines itself with
    /// `WITH` — so exactly the names the request has to have declared.
    pub tables: BTreeSet<String>,
}

/// One ADQL statement, read and translated.
pub fn translate(query: &str, limits: sql::Limits) -> Result<Translated, ApiError> {
    let mut statement = sql::statement(query, FIELD, limits)?;
    if !matches!(statement, Statement::Query(_)) {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: only a SELECT is answered; this service reads and never writes"
        )));
    }
    let mut translator = Translator::default();
    if let ControlFlow::Break(refusal) = statement.visit(&mut translator) {
        return Err(refusal);
    }
    let tables = translator
        .relations
        .difference(&translator.defined)
        .cloned()
        .collect();
    Ok(Translated { statement, tables })
}

/// The rewrite, one node at a time.
#[derive(Debug, Default)]
struct Translator {
    /// Every name a `FROM` or a `JOIN` reads.
    relations: BTreeSet<String>,
    /// The names `WITH` defines, which are the statement's own and not the request's.
    defined: BTreeSet<String>,
}

impl VisitorMut for Translator {
    type Break = ApiError;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<ApiError> {
        if let Some(with) = &query.with {
            self.defined.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.clone()),
            );
        }
        into_result(top_as_limit(query))
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<ApiError> {
        let TableFactor::Table { name, args, .. } = factor else {
            return ControlFlow::Continue(());
        };
        // A table function reads what its arguments say rather than a table the request
        // declared — `generate_series(1, 1e12)` is a row source nobody named and nothing
        // bounds — so a name with arguments is not a table here.
        if args.is_some() {
            return ControlFlow::Break(ApiError::bad_request(format!(
                "{FIELD}: {name} is not one of this request's tables; FROM names a table the \
                 request declares"
            )));
        }
        match one_name(name) {
            Some(table) => {
                self.relations.insert(table);
                ControlFlow::Continue(())
            }
            None => ControlFlow::Break(ApiError::bad_request(format!(
                "{FIELD}: {name} is not a table name here; each table this request reads is \
                 named by one word in its tables"
            ))),
        }
    }

    /// Before the children, so a comparison is seen whole: after them, the `CONTAINS` inside
    /// `1 = CONTAINS(…)` would already have been met on its own and refused for standing
    /// outside a comparison.
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<ApiError> {
        into_result(region_test(expr).map(|test| {
            if let Some(test) = test {
                *expr = test;
            }
        }))
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<ApiError> {
        let Expr::Function(call) = expr else {
            return ControlFlow::Continue(());
        };
        let Some(name) = adql_name(call) else {
            return ControlFlow::Continue(());
        };
        if let Some((_, reason)) = REFUSED.iter().find(|(refused, _)| *refused == name) {
            return ControlFlow::Break(ApiError::bad_request(format!(
                "{FIELD}: {name} is not answered here — {reason}"
            )));
        }
        match name.as_str() {
            "CONTAINS" | "INTERSECTS" => ControlFlow::Break(ApiError::bad_request(format!(
                "{FIELD}: {name} is a region test and is compared with 1, as in \
                 1 = {name}(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))"
            ))),
            "DISTANCE" => ControlFlow::Break(ApiError::bad_request(format!(
                "{FIELD}: DISTANCE is answered as a bound on a separation, as in \
                 DISTANCE(POINT(ra, dec), POINT(45.0, -20.0)) < 0.1"
            ))),
            "MOD" => into_result(modulo(call).map(|remainder| *expr = remainder)),
            _ => {
                if let Some((_, datafusion)) = RENAMED.iter().find(|(adql, _)| *adql == name) {
                    call.name =
                        ObjectName(vec![ObjectNamePart::Identifier(Ident::new(*datafusion))]);
                }
                ControlFlow::Continue(())
            }
        }
    }
}

/// `TOP n`, moved to where DataFusion reads a limit.
///
/// Only from the one `SELECT` a query body is: `TOP` belongs to that select and a query's
/// limit to the query around it, which coincide exactly when the body is a plain select.
fn top_as_limit(query: &mut Query) -> Result<(), ApiError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Ok(());
    };
    let Some(Top {
        with_ties,
        percent,
        quantity,
    }) = select.top.take()
    else {
        return Ok(());
    };
    if with_ties || percent {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: TOP takes a number of rows, with nothing after it"
        )));
    }
    let rows = match quantity {
        Some(TopQuantity::Constant(rows)) => rows,
        _ => {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: TOP takes a whole number written out, as in TOP 100"
            )));
        }
    };
    if query.limit_clause.is_some() {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: TOP and LIMIT both say how many rows; write one of them"
        )));
    }
    query.limit_clause = Some(LimitClause::LimitOffset {
        limit: Some(Expr::value(Value::Number(rows.to_string(), false))),
        offset: None,
        limit_by: Vec::new(),
    });
    Ok(())
}

/// A region test in ADQL's spelling, as the boolean test DataFusion plans — or `None` where
/// this is not one.
fn region_test(expr: &Expr) -> Result<Option<Expr>, ApiError> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };
    if let Some((call, holds)) = compared_test(left, op, right) {
        let test = contains(call)?;
        return Ok(Some(match holds {
            true => test,
            false => Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(test),
            },
        }));
    }
    if let Some((call, radius)) = bounded_distance(left, op, right) {
        return within(call, radius).map(Some);
    }
    Ok(None)
}

/// `CONTAINS(…)` or `INTERSECTS(…)` compared with a truth value, and whether it was the test
/// holding that was asked for: `= 1`, `> 0` and their mirrors hold; `= 0` does not.
fn compared_test<'a>(
    left: &'a Expr,
    op: &BinaryOperator,
    right: &'a Expr,
) -> Option<(&'a Function, bool)> {
    let test = |expr: &'a Expr| {
        let Expr::Function(call) = unnest(expr) else {
            return None;
        };
        matches!(adql_name(call)?.as_str(), "CONTAINS" | "INTERSECTS").then_some(call)
    };
    let (call, value, flipped) = match (test(left), test(right)) {
        (Some(call), None) => (call, right, false),
        (None, Some(call)) => (call, left, true),
        _ => return None,
    };
    let value = number(value)?;
    // Written with the test on the left: `CONTAINS(…) > 0` holds, and `0 < CONTAINS(…)` is
    // the same comparison mirrored.
    let op = match (op, flipped) {
        (BinaryOperator::Gt, true) => BinaryOperator::Lt,
        (BinaryOperator::Lt, true) => BinaryOperator::Gt,
        (op, _) => op.clone(),
    };
    match (op, value) {
        (BinaryOperator::Eq, 1.0) | (BinaryOperator::Gt, 0.0) => Some((call, true)),
        (BinaryOperator::Eq, 0.0) | (BinaryOperator::NotEq, 1.0) => Some((call, false)),
        _ => None,
    }
}

/// `DISTANCE(…)` bounded above by a number written out, whichever side the bound is on.
fn bounded_distance<'a>(
    left: &'a Expr,
    op: &BinaryOperator,
    right: &'a Expr,
) -> Option<(&'a Function, &'a Expr)> {
    let distance = |expr: &'a Expr| {
        let Expr::Function(call) = unnest(expr) else {
            return None;
        };
        (adql_name(call)? == "DISTANCE").then_some(call)
    };
    match op {
        BinaryOperator::Lt | BinaryOperator::LtEq => Some((distance(left)?, right)),
        BinaryOperator::Gt | BinaryOperator::GtEq => Some((distance(right)?, left)),
        _ => None,
    }
}

/// `CONTAINS(point, region)` or `INTERSECTS` in either order, as `contains(point, region)`.
///
/// `INTERSECTS` is symmetric and `CONTAINS` is not: `CONTAINS(CIRCLE(…), POINT(…))` asks
/// whether a circle lies inside a point, which is a constant rather than a region test written
/// backwards, so only `INTERSECTS` has its arguments put in order.
fn contains(call: &Function) -> Result<Expr, ApiError> {
    let name = adql_name(call).unwrap_or_default();
    let args = arguments(call)?;
    let [first, second] = args.as_slice() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: {name} takes a position and a region"
        )));
    };
    let (point, region) = match (
        is_call(first, "POINT"),
        is_call(second, "POINT"),
        name.as_str(),
    ) {
        (true, _, _) => (first, second),
        (false, true, "INTERSECTS") => (second, first),
        _ => {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: {name} tests a row's POINT(ra, dec) against a region, and takes the \
                 point first"
            )));
        }
    };
    Ok(call_to(
        "contains",
        vec![(*point).clone(), (*region).clone()],
    ))
}

/// `DISTANCE(…) < r`, as the circle of radius `r` it is.
///
/// Both of ADQL's spellings: two points, and the four coordinates 2.1 added. The position
/// that is a pair of numbers written out is the centre; the other is the row's.
fn within(call: &Function, radius: &Expr) -> Result<Expr, ApiError> {
    let wrong = || {
        ApiError::bad_request(format!(
            "{FIELD}: a bounded DISTANCE compares a row's position with a position written as \
             two numbers, as in DISTANCE(POINT(ra, dec), POINT(45.0, -20.0)) < 0.1"
        ))
    };
    let args = arguments(call)?;
    let (row, (ra, dec)) = match args.as_slice() {
        [first, second] => match (constant_point(first), constant_point(second)) {
            (None, Some(centre)) => ((*first).clone(), centre),
            (Some(centre), None) => ((*second).clone(), centre),
            _ => return Err(wrong()),
        },
        [a, b, c, d] => match (constant(a) && constant(b), constant(c) && constant(d)) {
            (false, true) => (
                call_to("POINT", vec![(*a).clone(), (*b).clone()]),
                ((*c).clone(), (*d).clone()),
            ),
            (true, false) => (
                call_to("POINT", vec![(*c).clone(), (*d).clone()]),
                ((*a).clone(), (*b).clone()),
            ),
            _ => return Err(wrong()),
        },
        _ => return Err(wrong()),
    };
    if !is_call(&row, "POINT") {
        return Err(wrong());
    }
    Ok(call_to(
        "contains",
        vec![row, call_to("CIRCLE", vec![ra, dec, radius.clone()])],
    ))
}

/// `MOD(x, y)`, which DataFusion writes `x % y`.
fn modulo(call: &Function) -> Result<Expr, ApiError> {
    let args = arguments(call)?;
    let [dividend, divisor] = args.as_slice() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: MOD takes a dividend and a divisor"
        )));
    };
    Ok(Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new((*dividend).clone()),
        op: BinaryOperator::Modulo,
        right: Box::new((*divisor).clone()),
    })))
}

/// A `POINT(…)` of two numbers written out, as those two numbers.
fn constant_point(expr: &Expr) -> Option<(Expr, Expr)> {
    let Expr::Function(call) = unnest(expr) else {
        return None;
    };
    if adql_name(call)? != "POINT" {
        return None;
    }
    let args = arguments(call).ok()?;
    let [ra, dec] = args.as_slice() else {
        return None;
    };
    (constant(ra) && constant(dec)).then(|| ((*ra).clone(), (*dec).clone()))
}

/// A number written out, signed or not.
fn constant(expr: &Expr) -> bool {
    number(expr).is_some()
}

fn number(expr: &Expr) -> Option<f64> {
    match unnest(expr) {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => number(expr).map(|value| -value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => number(expr),
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

fn is_call(expr: &Expr, name: &str) -> bool {
    matches!(unnest(expr), Expr::Function(call) if adql_name(call).as_deref() == Some(name))
}

/// A call's name as ADQL reads it, which is without regard to case. `None` for a quoted name,
/// which names a function exactly and is DataFusion's to resolve — including the `contains`
/// this translation writes.
fn adql_name(call: &Function) -> Option<String> {
    let [ObjectNamePart::Identifier(ident)] = call.name.0.as_slice() else {
        return None;
    };
    ident
        .quote_style
        .is_none()
        .then(|| ident.value.to_ascii_uppercase())
}

/// A call's arguments, where they are the plain positional list ADQL writes.
fn arguments(call: &Function) -> Result<Vec<&Expr>, ApiError> {
    let name = adql_name(call).unwrap_or_default();
    let plain = || {
        ApiError::bad_request(format!(
            "{FIELD}: {name} takes its arguments in order, with nothing else in the parentheses"
        ))
    };
    let FunctionArguments::List(list) = &call.args else {
        return Err(plain());
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(plain());
    }
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(plain()),
        })
        .collect()
}

/// A call this translation writes.
///
/// `contains` is quoted: that is the mark that it is this translation's boolean test rather
/// than ADQL's `CONTAINS`, which answers 1 or 0 and must be compared. DataFusion reads a quoted
/// function name exactly, and the name is lowercase, so the two resolve to the same function.
fn call_to(name: &str, args: Vec<Expr>) -> Expr {
    let ident = match name {
        "contains" => Ident::with_quote('"', name),
        _ => Ident::new(name),
    };
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(ident)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(datafusion::sql::sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// A table name of one part, as written.
fn one_name(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Some(ident.value.clone()),
        _ => None,
    }
}

fn into_result(result: Result<(), ApiError>) -> ControlFlow<ApiError> {
    match result {
        Ok(()) => ControlFlow::Continue(()),
        Err(refusal) => ControlFlow::Break(refusal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: sql::Limits = sql::Limits {
        max_depth: 50,
        max_nodes: 10_000,
    };

    fn translated(adql: &str) -> String {
        translate(adql, LIMITS)
            .expect("this query should translate")
            .statement
            .to_string()
    }

    fn refusal(adql: &str) -> String {
        translate(adql, LIMITS)
            .expect_err("this query should be refused")
            .to_string()
    }

    #[test]
    fn top_becomes_a_limit() {
        assert_eq!(
            translated("SELECT TOP 10 ra FROM gaia"),
            "SELECT ra FROM gaia LIMIT 10"
        );
    }

    #[test]
    fn a_region_test_becomes_the_boolean_one() {
        let expected =
            r#"SELECT ra FROM gaia WHERE "contains"(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))"#;
        for comparison in [
            "1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))",
            "CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) = 1",
            "CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) > 0",
            "0 < CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))",
            "1 = INTERSECTS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))",
            "1 = INTERSECTS(CIRCLE(45.0, -20.0, 0.1), POINT(ra, dec))",
            "1 = contains(point(ra, dec), circle(45.0, -20.0, 0.1))",
        ] {
            assert_eq!(
                translated(&format!("SELECT ra FROM gaia WHERE {comparison}")).to_ascii_lowercase(),
                expected.to_ascii_lowercase(),
                "{comparison}"
            );
        }
    }

    #[test]
    fn a_region_test_compared_with_0_is_its_negation() {
        assert_eq!(
            translated("SELECT ra FROM gaia WHERE 0 = CONTAINS(POINT(ra, dec), CIRCLE(1, 2, 3))"),
            r#"SELECT ra FROM gaia WHERE NOT "contains"(POINT(ra, dec), CIRCLE(1, 2, 3))"#
        );
    }

    #[test]
    fn a_bounded_distance_is_a_circle() {
        let expected =
            r#"SELECT ra FROM gaia WHERE "contains"(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))"#;
        for bound in [
            "DISTANCE(POINT(ra, dec), POINT(45.0, -20.0)) < 0.1",
            "DISTANCE(POINT(45.0, -20.0), POINT(ra, dec)) <= 0.1",
            "0.1 > DISTANCE(POINT(ra, dec), POINT(45.0, -20.0))",
            "DISTANCE(ra, dec, 45.0, -20.0) < 0.1",
        ] {
            assert_eq!(
                translated(&format!("SELECT ra FROM gaia WHERE {bound}")),
                expected,
                "{bound}"
            );
        }
    }

    #[test]
    fn a_negative_centre_is_still_a_number() {
        assert_eq!(
            translated("SELECT ra FROM gaia WHERE DISTANCE(ra, dec, -45.0, -20.0) < 0.1"),
            r#"SELECT ra FROM gaia WHERE "contains"(POINT(ra, dec), CIRCLE(-45.0, -20.0, 0.1))"#
        );
    }

    #[test]
    fn the_functions_adql_names_differently_are_renamed() {
        assert_eq!(
            translated("SELECT CEILING(a), TRUNCATE(b, 2), LOG(c), LOG10(d), MOD(e, 3) FROM t"),
            "SELECT ceil(a), trunc(b, 2), ln(c), LOG10(d), (e % 3) FROM t"
        );
    }

    /// The planner answers these, which is the point of translating rather than interpreting.
    #[test]
    fn what_datafusion_plans_is_left_to_it() {
        for adql in [
            "SELECT band, COUNT(*) FROM ztf GROUP BY band HAVING COUNT(*) > 10",
            "SELECT DISTINCT band FROM ztf ORDER BY band",
            "SELECT a.id FROM a JOIN b ON a.id = b.id",
            "SELECT id FROM a UNION SELECT id FROM b",
            "SELECT id FROM a WHERE id IN (SELECT id FROM b)",
        ] {
            assert_eq!(translated(adql), adql);
        }
    }

    #[test]
    fn the_tables_are_the_ones_the_request_must_declare() {
        let tables = |adql: &str| translate(adql, LIMITS).unwrap().tables;
        assert_eq!(
            tables("SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.id IN (SELECT id FROM c)"),
            BTreeSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
        // A name the statement defines for itself is not one the request has to.
        assert_eq!(
            tables("WITH near AS (SELECT id FROM gaia) SELECT id FROM near"),
            BTreeSet::from(["gaia".to_owned()])
        );
    }

    #[test]
    fn a_geometry_this_service_does_not_answer_is_refused_by_name() {
        for (name, adql) in [
            (
                "BOX",
                "SELECT ra FROM t WHERE 1 = CONTAINS(POINT(ra, dec), BOX(1, 2, 3, 4))",
            ),
            (
                "POLYGON",
                "SELECT ra FROM t WHERE 1 = CONTAINS(POINT(ra, dec), POLYGON(1, 2, 3, 4, 5, 6))",
            ),
            ("AREA", "SELECT AREA(CIRCLE(1, 2, 3)) FROM t"),
            ("RAND", "SELECT RAND() FROM t"),
        ] {
            let refusal = refusal(adql);
            assert!(
                refusal.contains(name) && refusal.contains("not answered"),
                "{refusal}"
            );
        }
    }

    #[test]
    fn a_region_test_outside_a_comparison_is_refused() {
        let refusal = refusal("SELECT CONTAINS(POINT(ra, dec), CIRCLE(1, 2, 3)) FROM t");
        assert!(refusal.contains("compared with 1"), "{refusal}");
    }

    #[test]
    fn contains_takes_the_point_first() {
        let refusal =
            refusal("SELECT ra FROM t WHERE 1 = CONTAINS(CIRCLE(1, 2, 3), POINT(ra, dec))");
        assert!(refusal.contains("point first"), "{refusal}");
    }

    #[test]
    fn an_unbounded_distance_is_refused() {
        let refusal = refusal("SELECT DISTANCE(POINT(ra, dec), POINT(1, 2)) FROM t");
        assert!(refusal.contains("bound on a separation"), "{refusal}");
    }

    #[test]
    fn a_table_function_is_not_a_table() {
        let refusal = refusal("SELECT * FROM generate_series(1, 1000000000000)");
        assert!(
            refusal.contains("not one of this request's tables"),
            "{refusal}"
        );
    }

    #[test]
    fn a_qualified_table_name_is_refused() {
        let refusal = refusal("SELECT * FROM TAP_SCHEMA.tables");
        assert!(refusal.contains("one word"), "{refusal}");
    }

    #[test]
    fn top_and_limit_together_are_refused() {
        let refusal = refusal("SELECT TOP 5 ra FROM t LIMIT 10");
        assert!(refusal.contains("write one of them"), "{refusal}");
    }

    #[test]
    fn only_a_select_is_answered() {
        for statement in ["DELETE FROM t", "DROP TABLE t", "INSERT INTO t VALUES (1)"] {
            let refusal = refusal(statement);
            assert!(
                refusal.contains("only a SELECT") || refusal.contains("is not a query"),
                "{statement}: {refusal}"
            );
        }
    }

    #[test]
    fn a_second_statement_is_refused() {
        assert!(refusal("SELECT 1; SELECT 2").contains("one statement"));
    }

    /// The translation is only right if DataFusion plans what it hands over: the quoted
    /// `"contains"`, the uppercase constructors, the `LIMIT` moved out of the select. Run
    /// against two rows either side of the circle, so a test that was dropped, or read the
    /// wrong way round, returns the wrong one.
    #[tokio::test]
    async fn datafusion_plans_the_translation() {
        use std::sync::Arc;

        use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::Int64Type;
        use datafusion::sql::parser::Statement as DfStatement;

        let ctx = crate::query::session_context(false);
        crate::geometry::register(&ctx);
        let rows = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "ra",
                Arc::new(Float64Array::from(vec![45.0, 90.0])) as ArrayRef,
            ),
            (
                "dec",
                Arc::new(Float64Array::from(vec![-20.0, 10.0])) as ArrayRef,
            ),
        ])
        .unwrap();
        ctx.register_batch("gaia", rows).unwrap();

        let answer = async |adql: &str| {
            let translated = translate(adql, LIMITS).unwrap();
            let plan = ctx
                .state()
                .statement_to_plan(DfStatement::Statement(Box::new(translated.statement)))
                .await
                .unwrap();
            let batches = ctx
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<i64>>()
        };

        let inside = "1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 1.0))";
        assert_eq!(
            answer(&format!("SELECT TOP 5 id FROM gaia WHERE {inside}")).await,
            [1]
        );
        assert_eq!(
            answer(
                "SELECT id FROM gaia WHERE 0 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 1.0))"
            )
            .await,
            [2]
        );
        assert_eq!(
            answer("SELECT id FROM gaia WHERE DISTANCE(POINT(ra, dec), POINT(90.0, 10.0)) < 1.0")
                .await,
            [2]
        );
        assert_eq!(
            answer("SELECT TOP 1 id FROM gaia ORDER BY id DESC").await,
            [2]
        );
    }
}
