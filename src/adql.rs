//! ADQL, the query language IVOA services take, read as the request this service already
//! answers.
//!
//! What this module does is take one statement apart: the table it names, its select list,
//! its `TOP`, and its `WHERE` split into a region on the sky and whatever else the caller
//! asked. Nothing here is planned and nothing here is run — the pieces go back through
//! [`crate::sql`], which is still the only place a caller's expression becomes something
//! this service executes, and the region goes to [`crate::region`] like any other.
//!
//! **A geometry predicate is recognised or refused, never ignored.** `1 =
//! CONTAINS(POINT(ra, dec), CIRCLE(…))` has to come out as a [`Region`], because a
//! constraint the region machinery never sees is one no covering can prune on: the query
//! then reads every partition of the catalog and evaluates the trigonometry over all of
//! them, which against a real catalog is minutes and which a caller cannot tell from a slow
//! link. So every geometry name ADQL has is either lowered here or refused by name.
//!
//! **This is not all of ADQL and must not be described as it.** `GROUP BY`, `HAVING`,
//! `ORDER BY`, `DISTINCT`, joins, subqueries and set operations each need to combine rows
//! across partitions, which the fan-out below this cannot do, and each is refused by name
//! rather than answered wrongly. Three of them are in the language's mandatory core.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, ExprWithAlias, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, Select, SelectFlavor, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Top, TopQuantity, UnaryOperator, Value, visit_expressions,
    visit_expressions_mut,
};
use datafusion::sql::sqlparser::ast::{Query as SqlQuery, WildcardAdditionalOptions};

use crate::error::ApiError;
use crate::region::Region;
use crate::sql;

/// The field a statement arrives in, and what every refusal here names first.
const FIELD: &str = "query";

/// One ADQL statement, taken apart.
///
/// The expressions are still `sqlparser`'s, unplanned: which columns a name reaches and
/// whether a function may be called are the file's business and this module has no file.
/// What it decides is the shape of the statement — which is a question about the text alone.
#[derive(Debug)]
pub struct Query {
    /// The table `FROM` named, as written, which the request's own `tables` is asked for.
    ///
    /// The alias is not here. It is a name for the table inside the statement and nothing
    /// outside one can use it, so it is spent where it is understood — taking it off the
    /// column references written against it — and does not survive.
    pub table: String,
    /// The select list, one item per output column, or `None` for `*`.
    pub select: Option<Vec<ExprWithAlias>>,
    /// What is left of `WHERE` once the region has been taken out of it, or `None` where
    /// nothing was left.
    pub predicate: Option<SqlExpr>,
    /// The region the geometry lowered to, and the two columns `POINT` named.
    pub spatial: Option<Spatial>,
    /// `TOP n`, ADQL's spelling of a row limit.
    pub top: Option<usize>,
}

/// The table a statement reads, and the name its own column references are written against.
#[derive(Debug, PartialEq, Eq)]
struct Table {
    name: String,
    /// `FROM gaia AS g`. Where there is one it replaces the name rather than joining it:
    /// SQL's rule is that an alias is *the* name inside the statement, so `gaia.ra` beside
    /// `FROM gaia AS g` is not a column reference at all.
    alias: Option<String>,
}

impl Table {
    /// The one name a column reference in this statement may be qualified by.
    fn qualifier(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// The geometry of one statement: the shapes, and the columns they are tested against.
///
/// One set of columns for the whole statement, as [`crate::region::Spatial`] has it. A
/// caller who writes two region tests against two different pairs of columns is asking
/// something this shape cannot carry and is refused rather than answered about one pair.
#[derive(Debug, Clone, PartialEq)]
pub struct Spatial {
    /// A union: a row inside any of them qualifies, which is what `OR` between two region
    /// tests means and what the region field already does with an array.
    pub regions: Vec<Region>,
    pub ra_column: String,
    pub dec_column: String,
}

/// The geometry ADQL has that this service will not answer, and what to say about each.
///
/// Refused by name wherever it appears, rather than left to fail further down: none of
/// these is a function DataFusion has, so what a caller would get instead is an account of
/// an unknown function — which says nothing about the shape they asked for, and reads as a
/// fault in the service rather than as a limit of it.
///
/// The geometry functions are an optional ADQL feature, declared one form at a time, so
/// refusing these costs the service nothing it claimed.
const REFUSED: &[(&str, &str)] = &[
    (
        "AREA",
        "it takes a geometry as a value, and no column here holds one",
    ),
    (
        "BOX",
        "its edges are great circles, where the zone region's run along parallels; near a \
         pole the two differ by degrees, so neither name is given to the other's shape",
    ),
    (
        "CENTROID",
        "it takes a geometry as a value, and no column here holds one",
    ),
    (
        "COORD1",
        "it takes a geometry as a value; a position is two columns of the file",
    ),
    (
        "COORD2",
        "it takes a geometry as a value; a position is two columns of the file",
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
        "it takes an STC-S string, which ADQL 2.1 itself deprecated",
    ),
];

/// The geometry that is lowered rather than refused. Every one of these belongs inside a
/// region test in `WHERE` and nowhere else — a select list cannot return a shape, because
/// nothing here has a type to return it as.
const RECOGNISED: &[&str] = &[
    "CIRCLE",
    "CONTAINS",
    "DISTANCE",
    "INTERSECTS",
    "MOC",
    "POINT",
];

/// One ADQL statement, read.
pub fn parse(query: &str, limits: sql::Limits) -> Result<Query, ApiError> {
    let Statement::Query(body) = sql::statement(query, FIELD, limits)? else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: only SELECT is answered; this service reads and never writes"
        )));
    };
    let selected = one_select(*body)?;
    let table = one_table(selected.from)?;
    let top = row_limit(selected.top)?;
    let mut select = select_list(selected.projection)?;
    let mut selection = selected.selection;
    // Before anything reads a name, so that everything after this is looking at column
    // references alone. The table's own name inside the statement is a spelling of the
    // columns and not a thing in its own right, and the shortest life it can have is the
    // one it has here: understood once, and gone.
    if let Some(items) = &mut select {
        for item in items {
            strip_qualifier(&mut item.expr, table.qualifier());
        }
    }
    if let Some(selection) = &mut selection {
        strip_qualifier(selection, table.qualifier());
    }
    if let Some(items) = &select {
        for item in items {
            no_geometry(&item.expr, "a select list")?;
        }
    }
    let (predicate, spatial) = match selection {
        None => (None, None),
        Some(selection) => split(selection)?,
    };
    Ok(Query {
        table: table.name,
        select,
        predicate,
        spatial,
        top,
    })
}

/// Take the table's own name off the column references written against it.
///
/// **This is a rewrite the text alone can decide, which is why it is here.** A dotted name
/// means two things in this service — `lightcurve.mag` reaches into a struct column, and
/// `gaia.ra` is SQL's table beside the column — but only one head can be the table's, and
/// the statement says which. Every other dotted name is left exactly as written, to be read
/// as a path into a column the way it is on every other route.
///
/// The one case with two readings is a caller naming their table after a struct column of
/// the file, and there the table wins. It is the narrowest possible collision and the only
/// one of this service's ambiguities the caller can resolve alone: the table's name is
/// theirs, given in the request beside the query, and renaming it settles the matter. That
/// is what makes this a place the file's schema is not consulted — waiting for it would put
/// the rewrite three layers down, and buy a tie-break for a tie the caller can avoid.
fn strip_qualifier(expr: &mut SqlExpr, qualifier: &str) {
    let names_the_table = |head: &Ident| {
        head.value == qualifier
            // ADQL folds an unquoted name to uppercase, so a client that has done so writes
            // the table that way. A quoted one is exact, as it is everywhere here.
            || (head.quote_style.is_none() && head.value.eq_ignore_ascii_case(qualifier))
    };
    let _ = visit_expressions_mut::<_, (), _>(expr, |node| {
        if let SqlExpr::CompoundIdentifier(parts) = node
            && matches!(parts.as_slice(), [head, _, ..] if names_the_table(head))
        {
            parts.remove(0);
            // One segment left is a plain name and has to be spelled as one: a
            // `CompoundIdentifier` holding a single part is not what a bare column is
            // matched against anywhere below here.
            if parts.len() == 1
                && let Some(only) = parts.pop()
            {
                *node = SqlExpr::Identifier(only);
            }
        }
        ControlFlow::Continue(())
    });
}

/// The four pieces of a statement this route reads, once everything else in one has been
/// refused.
struct Selected {
    top: Option<Top>,
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<SqlExpr>,
}

/// One `SELECT`, with every clause that is not one of those four refused.
///
/// Both structures are taken apart field by field rather than read for the few fields that
/// matter. `sqlparser` parses a good deal more than ADQL writes, and a clause nobody here
/// has thought about is one that would be dropped in silence — so a version of it that
/// grows a field is a compile error and somebody decides what the field means here.
fn one_select(query: SqlQuery) -> Result<Selected, ApiError> {
    let SqlQuery {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;
    if with.is_some() {
        return Err(not_here(
            "WITH",
            "a common table expression is a second query, planned and joined to this one",
        ));
    }
    if order_by.is_some() {
        return Err(not_here(
            "ORDER BY",
            "a sort has to hold every matching row at once, and the rows here arrive one \
             partition at a time",
        ));
    }
    if limit_clause.is_some() {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: ADQL writes a row limit as TOP n, directly after SELECT"
        )));
    }
    if fetch.is_some()
        || !locks.is_empty()
        || for_clause.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || !pipe_operators.is_empty()
    {
        return Err(not_adql("what follows the query"));
    }
    let select = match *body {
        SetExpr::Select(select) => *select,
        SetExpr::SetOperation { .. } => {
            return Err(not_here(
                "UNION, INTERSECT and EXCEPT",
                "each side is a query of its own and the result is built from both at once",
            ));
        }
        _ => {
            return Err(not_here(
                "a subquery",
                "it is a second query, planned and its rows held while this one runs",
            ));
        }
    };
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        // Which side of `DISTINCT` the `TOP` was written is `sqlparser`'s record of how it
        // parsed; ADQL writes it after, which is where this dialect reads it.
        top_before_distinct: _,
        projection,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        flavor,
    } = select;
    if distinct.is_some() {
        return Err(not_here(
            "DISTINCT",
            "a duplicate can only be found by holding every matching row at once",
        ));
    }
    // The absent clause is an empty expression list rather than an `Option`, so this is what
    // "no GROUP BY" looks like.
    if !matches!(&group_by, GroupByExpr::Expressions(by, modifiers) if by.is_empty() && modifiers.is_empty())
    {
        return Err(not_here(
            "GROUP BY",
            "a group has to hold every matching row at once, and the rows here arrive one \
             partition at a time",
        ));
    }
    if having.is_some() {
        return Err(not_here(
            "HAVING",
            "it tests the groups GROUP BY would have made",
        ));
    }
    if !matches!(flavor, SelectFlavor::Standard)
        || !optimizer_hints.is_empty()
        || select_modifiers.is_some()
        || exclude.is_some()
        || into.is_some()
        || !lateral_views.is_empty()
        || prewhere.is_some()
        || !connect_by.is_empty()
        || !cluster_by.is_empty()
        || !distribute_by.is_empty()
        || !sort_by.is_empty()
        || !named_window.is_empty()
        || qualify.is_some()
        || value_table_mode.is_some()
    {
        return Err(not_adql("something written in this SELECT"));
    }
    Ok(Selected {
        top,
        projection,
        from,
        selection,
    })
}

/// The one table a statement reads.
fn one_table(mut from: Vec<TableWithJoins>) -> Result<Table, ApiError> {
    if from.len() != 1 {
        return Err(not_here(
            "more than one table",
            "each is read on its own here, and nothing combines two",
        ));
    }
    let Some(TableWithJoins { relation, joins }) = from.pop() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: FROM names the table to read"
        )));
    };
    if !joins.is_empty() {
        return Err(not_here(
            "JOIN",
            "it needs both sides at once, and each table is read on its own here",
        ));
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = relation
    else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: FROM takes the name of one of the tables this request declared"
        )));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(not_adql("what follows the table name"));
    }
    let [part] = name.0.as_slice() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: {name} is not a table name here; the request's own tables are named by \
             one word each"
        )));
    };
    let Some(name) = part.as_ident() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: a table is named, not computed"
        )));
    };
    Ok(Table {
        name: name.value.clone(),
        alias: alias.map(|alias| alias.name.value),
    })
}

/// `TOP n`, as a count of rows.
fn row_limit(top: Option<Top>) -> Result<Option<usize>, ApiError> {
    let Some(Top {
        with_ties,
        percent,
        quantity,
    }) = top
    else {
        return Ok(None);
    };
    if with_ties || percent {
        return Err(not_adql("this TOP"));
    }
    match quantity {
        Some(TopQuantity::Constant(rows)) => usize::try_from(rows).map(Some).map_err(|_| {
            ApiError::bad_request(format!("{FIELD}: TOP {rows} is more rows than exist"))
        }),
        // `TOP (n)` is another dialect's spelling, and `TOP (n + 1)` is an expression this
        // service would have to evaluate before it knew what it was reading.
        Some(TopQuantity::Expr(_)) => Err(ApiError::bad_request(format!(
            "{FIELD}: TOP takes a whole number written out, as in TOP 100"
        ))),
        None => Err(ApiError::bad_request(format!(
            "{FIELD}: TOP takes the number of rows to return"
        ))),
    }
}

/// The select list, as the expression-and-alias pairs the projection below takes.
///
/// `*` is every column, which is what leaving the projection out means everywhere else
/// here, so it comes back as no projection at all rather than as an item.
fn select_list(items: Vec<SelectItem>) -> Result<Option<Vec<ExprWithAlias>>, ApiError> {
    if let [SelectItem::Wildcard(options)] = items.as_slice() {
        plain_wildcard(options)?;
        return Ok(None);
    }
    items
        .into_iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => Ok(ExprWithAlias { expr, alias: None }),
            SelectItem::ExprWithAlias { expr, alias } => Ok(ExprWithAlias {
                expr,
                alias: Some(alias),
            }),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                Err(ApiError::bad_request(format!(
                    "{FIELD}: * is every column and cannot be one item among several; name \
                     the columns, or write SELECT * alone"
                )))
            }
            SelectItem::ExprWithAliases { .. } => Err(not_adql("several aliases for one column")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// `*` and nothing attached to it. The attachments are other dialects' ways of subtracting
/// from a wildcard, and a subtraction that went unread would return columns the caller
/// asked not to have.
fn plain_wildcard(options: &WildcardAdditionalOptions) -> Result<(), ApiError> {
    let WildcardAdditionalOptions {
        wildcard_token: _,
        opt_ilike,
        opt_exclude,
        opt_except,
        opt_replace,
        opt_rename,
        opt_alias,
    } = options;
    match opt_ilike.is_none()
        && opt_exclude.is_none()
        && opt_except.is_none()
        && opt_replace.is_none()
        && opt_rename.is_none()
        && opt_alias.is_none()
    {
        true => Ok(()),
        false => Err(not_adql("what follows the *")),
    }
}

/// `WHERE`, split into the region on the sky and the rest.
///
/// The split is over the top-level `AND` chain, which is the only place a region test can
/// be taken out of without changing what the caller asked: every conjunct has to hold, so
/// removing one and applying it separately is the same query. A conjunct mentioning
/// geometry anywhere inside it has to be a region test whole — that is what makes this
/// recognise rather than guess.
fn split(selection: SqlExpr) -> Result<(Option<SqlExpr>, Option<Spatial>), ApiError> {
    let mut residual: Option<SqlExpr> = None;
    let mut spatial: Option<Spatial> = None;
    for conjunct in conjuncts(selection) {
        no_refused_geometry(&conjunct)?;
        if !mentions(&conjunct, RECOGNISED) {
            residual = Some(match residual {
                None => conjunct,
                Some(left) => SqlExpr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(conjunct),
                },
            });
            continue;
        }
        if spatial.is_some() {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: one region test per query here; two ANDed together are the part of \
                 the sky both cover, which this service does not build yet"
            )));
        }
        spatial = Some(constraint(&conjunct)?);
    }
    Ok((residual, spatial))
}

/// One expression as the list of things `AND`ed together at its top level.
fn conjuncts(expr: SqlExpr) -> Vec<SqlExpr> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut parts = conjuncts(*left);
            parts.extend(conjuncts(*right));
            parts
        }
        // A parenthesised conjunction is that conjunction. Everything else keeps its
        // parentheses: the conjuncts are put back together with `AND` between them, and
        // `AND` binds tighter than `OR`, so an unwrapped disjunction would come back meaning
        // something the caller did not write.
        SqlExpr::Nested(inner)
            if matches!(
                *inner,
                SqlExpr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            conjuncts(*inner)
        }
        other => vec![other],
    }
}

/// One expression as the list of things `OR`ed together at its top level.
fn disjuncts(expr: &SqlExpr) -> Vec<&SqlExpr> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let mut parts = disjuncts(left);
            parts.extend(disjuncts(right));
            parts
        }
        SqlExpr::Nested(inner) => disjuncts(inner),
        other => vec![other],
    }
}

/// One conjunct that mentions geometry, as the region it tests.
///
/// `OR` between region tests is a union, which is what an array of shapes already means
/// here. Every one of them has to name the same two columns: the columns are the file's and
/// not the shape's, and a request testing one circle against `ra` and another against
/// `objra` is asking about two different files.
fn constraint(expr: &SqlExpr) -> Result<Spatial, ApiError> {
    let mut regions = Vec::new();
    let mut columns: Option<(String, String)> = None;
    for disjunct in disjuncts(expr) {
        let (region, ra_column, dec_column) = region_test(disjunct)?;
        match &columns {
            Some((ra, dec)) if *ra != ra_column || *dec != dec_column => {
                return Err(ApiError::bad_request(format!(
                    "{FIELD}: every region in one query is tested against the same pair of \
                     columns, and this one names {ra_column} and {dec_column} where another \
                     names {ra} and {dec}"
                )));
            }
            Some(_) => {}
            None => columns = Some((ra_column, dec_column)),
        }
        regions.push(region);
    }
    let Some((ra_column, dec_column)) = columns else {
        return Err(shape_of_a_region_test());
    };
    Ok(Spatial {
        regions,
        ra_column,
        dec_column,
    })
}

/// One region test, as the shape and the columns it reads a position out of.
///
/// ADQL has no boolean, so a region test is a comparison: `CONTAINS` and `INTERSECTS`
/// answer 1 or 0 and the caller writes which they wanted. `DISTANCE` answers an angle and
/// the caller bounds it, which is the same circle said another way.
fn region_test(expr: &SqlExpr) -> Result<(Region, String, String), ApiError> {
    let SqlExpr::BinaryOp { left, op, right } = unnest(expr) else {
        return Err(shape_of_a_region_test());
    };
    if let Some(call) = truth_value(left, op, right) {
        return inside(call);
    }
    if let Some((call, radius)) = bounded_distance(left, op, right) {
        return within(call, radius);
    }
    Err(shape_of_a_region_test())
}

/// `CONTAINS(…)` or `INTERSECTS(…)` compared against the truth value the caller wanted:
/// `= 1`, `1 =`, `> 0` and `0 <`.
fn truth_value<'a>(
    left: &'a SqlExpr,
    op: &BinaryOperator,
    right: &'a SqlExpr,
) -> Option<&'a Function> {
    let is_predicate = |expr: &'a SqlExpr| {
        let call = function(expr)?;
        matches!(called(call)?.as_str(), "CONTAINS" | "INTERSECTS").then_some(call)
    };
    let (call, value, wanted) = match op {
        BinaryOperator::Eq => match (is_predicate(left), is_predicate(right)) {
            (Some(call), None) => (call, right, 1.0),
            (None, Some(call)) => (call, left, 1.0),
            _ => return None,
        },
        BinaryOperator::Gt => (is_predicate(left)?, right, 0.0),
        BinaryOperator::Lt => (is_predicate(right)?, left, 0.0),
        _ => return None,
    };
    (number(value)? == wanted).then_some(call)
}

/// `DISTANCE(…)` bounded above, whichever side the bound was written on.
fn bounded_distance<'a>(
    left: &'a SqlExpr,
    op: &BinaryOperator,
    right: &'a SqlExpr,
) -> Option<(&'a Function, f64)> {
    let is_distance = |expr: &'a SqlExpr| {
        let call = function(expr)?;
        (called(call)? == "DISTANCE").then_some(call)
    };
    match op {
        BinaryOperator::Lt | BinaryOperator::LtEq => Some((is_distance(left)?, number(right)?)),
        BinaryOperator::Gt | BinaryOperator::GtEq => Some((is_distance(right)?, number(left)?)),
        _ => None,
    }
}

/// `CONTAINS(POINT(ra, dec), <region>)`, as the region and the two columns.
///
/// The point goes first. `CONTAINS(a, b)` asks whether `a` is inside `b`, so the other way
/// round asks whether a whole circle lies inside one row's position — which is a question
/// with a constant answer rather than a region test written backwards. `INTERSECTS` is
/// symmetric and takes either order, and for a point it is the same question as `CONTAINS`.
fn inside(call: &Function) -> Result<(Region, String, String), ApiError> {
    let symmetric = called(call).as_deref() == Some("INTERSECTS");
    let args = positional(call).ok_or_else(shape_of_a_region_test)?;
    let [first, second] = args.as_slice() else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: {} takes a position and a region",
            called(call).unwrap_or_default()
        )));
    };
    let (point, shape) = match (point_columns(first), symmetric) {
        (Ok(columns), _) => (columns, second),
        (Err(_), true) => (point_columns(second)?, first),
        (Err(error), false) => return Err(error),
    };
    let (ra_column, dec_column) = point;
    Ok((region(shape)?, ra_column, dec_column))
}

/// `DISTANCE(…) < r`, as the circle it is.
///
/// Both of ADQL's spellings: two points, and the four bare coordinates 2.1 added. The
/// position that is columns is the row's and the one that is numbers is the centre; both
/// columns or both constants is a comparison with nothing to search by.
fn within(call: &Function, radius: f64) -> Result<(Region, String, String), ApiError> {
    let args = positional(call).ok_or_else(shape_of_a_region_test)?;
    let (columns, centre) = match args.as_slice() {
        [first, second] => match (point_columns(first), point_position(second)) {
            (Ok(columns), Some(centre)) => (columns, centre),
            _ => match (point_position(first), point_columns(second)) {
                (Some(centre), Ok(columns)) => (columns, centre),
                _ => {
                    return Err(ApiError::bad_request(format!(
                        "{FIELD}: DISTANCE compares the row's position, written as two \
                         columns, with a position written as two numbers"
                    )));
                }
            },
        },
        [ra, dec, centre_ra, centre_dec] => match (
            column_pair(ra, dec),
            (number(centre_ra), number(centre_dec)),
        ) {
            (Ok(columns), (Some(centre_ra), Some(centre_dec))) => {
                (columns, (centre_ra, centre_dec))
            }
            _ => match (
                (number(ra), number(dec)),
                column_pair(centre_ra, centre_dec),
            ) {
                ((Some(centre_ra), Some(centre_dec)), Ok(columns)) => {
                    (columns, (centre_ra, centre_dec))
                }
                _ => {
                    return Err(ApiError::bad_request(format!(
                        "{FIELD}: DISTANCE compares the row's position, written as two \
                         columns, with a position written as two numbers"
                    )));
                }
            },
        },
        _ => {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: DISTANCE takes two positions, each as a POINT or as its two \
                 coordinates"
            )));
        }
    };
    let (ra_column, dec_column) = columns;
    let (ra, dec) = centre;
    // ADQL measures a separation in degrees, so the bound is a radius in degrees and needs
    // no unit named: the caller wrote the comparison, not a field.
    Ok((
        Region::Circle {
            ra,
            dec,
            radius_deg: Some(radius),
            radius_arcsec: None,
        },
        ra_column,
        dec_column,
    ))
}

/// The shape half of a region test.
fn region(expr: &SqlExpr) -> Result<Region, ApiError> {
    let call = function(expr).ok_or_else(|| {
        ApiError::bad_request(format!(
            "{FIELD}: a region is CIRCLE(ra, dec, radius) or MOC('4/30-33 38 52')"
        ))
    })?;
    match called(call).as_deref() {
        Some("CIRCLE") => circle(call),
        Some("MOC") => moc(call),
        Some("POINT") => Err(ApiError::bad_request(format!(
            "{FIELD}: a point is not a region; give a CIRCLE around it, or bound a DISTANCE"
        ))),
        Some(name) => Err(refused(name).unwrap_or_else(|| {
            ApiError::bad_request(format!(
                "{FIELD}: {name} is not a region this service tests"
            ))
        })),
        None => Err(ApiError::bad_request(format!(
            "{FIELD}: a region is named, not computed"
        ))),
    }
}

/// `CIRCLE(ra, dec, radius)`, every argument a number in degrees.
fn circle(call: &Function) -> Result<Region, ApiError> {
    let wrong_shape = || {
        ApiError::bad_request(format!(
            "{FIELD}: CIRCLE takes three numbers in degrees — the centre's right ascension \
             and declination, and the radius"
        ))
    };
    let args = positional(call).ok_or_else(wrong_shape)?;
    // ADQL 2.0 wrote a coordinate system first and 2.1 dropped it, so a four-argument call
    // is a caller writing the older spelling rather than one miscounting.
    if let [system, ..] = args.as_slice()
        && args.len() == 4
        && string(system).is_some()
    {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: CIRCLE takes no coordinate system; ADQL 2.1 dropped the argument, and \
             positions here are degrees in one frame"
        )));
    }
    let [ra, dec, radius] = args.as_slice() else {
        return Err(wrong_shape());
    };
    let (Some(ra), Some(dec), Some(radius)) = (number(ra), number(dec), number(radius)) else {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: a circle is the same for every row, so its three numbers are constants \
             rather than columns"
        )));
    };
    Ok(Region::Circle {
        ra,
        dec,
        radius_deg: Some(radius),
        radius_arcsec: None,
    })
}

/// `MOC('4/30-33 38 52')`: the cells themselves, at the depth they were written at.
///
/// Not in ADQL 2.1 — it is slated for a later version and this is the spelling the one
/// service that has it already uses. Worth having early because it is the cheapest region
/// this service tests: a MOC is cells, and a HEALPix-indexed catalog is already cells.
fn moc(call: &Function) -> Result<Region, ApiError> {
    let wrong_shape = || {
        ApiError::bad_request(format!(
            "{FIELD}: MOC takes one string, the IVOA ASCII serialization — \
             MOC('4/30-33 38 52 7/324-934')"
        ))
    };
    let args = positional(call).ok_or_else(wrong_shape)?;
    let [text] = args.as_slice() else {
        return Err(wrong_shape());
    };
    Ok(Region::Moc {
        ascii: Some(string(text).ok_or_else(wrong_shape)?.to_owned()),
        json: None,
    })
}

/// `POINT(ra, dec)` over two of the file's columns.
fn point_columns(expr: &SqlExpr) -> Result<(String, String), ApiError> {
    let wrong_shape = || {
        ApiError::bad_request(format!(
            "{FIELD}: a row's position is POINT(ra, dec), naming the two columns that hold it"
        ))
    };
    let call = function(expr).ok_or_else(wrong_shape)?;
    if called(call).as_deref() != Some("POINT") {
        return Err(wrong_shape());
    }
    let args = positional(call).ok_or_else(wrong_shape)?;
    let [ra, dec] = args.as_slice() else {
        return Err(wrong_shape());
    };
    column_pair(ra, dec)
}

/// `POINT(ra, dec)` at a fixed position.
fn point_position(expr: &SqlExpr) -> Option<(f64, f64)> {
    let call = function(expr)?;
    if called(call)? != "POINT" {
        return None;
    }
    let args = positional(call)?;
    let [ra, dec] = args.as_slice() else {
        return None;
    };
    Some((number(ra)?, number(dec)?))
}

/// Two column names.
///
/// Plain names by the time this runs: the table's own name has come off every reference in
/// the statement already, so a dotted name reaching here is a path into a column — and no
/// column holding a coordinate is one.
fn column_pair(ra: &SqlExpr, dec: &SqlExpr) -> Result<(String, String), ApiError> {
    let named = |expr: &SqlExpr| match unnest(expr) {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    };
    match (named(ra), named(dec)) {
        (Some(ra), Some(dec)) => Ok((ra, dec)),
        _ => Err(ApiError::bad_request(format!(
            "{FIELD}: a row's position is two of the file's columns, named and not computed"
        ))),
    }
}

/// No geometry where a shape cannot be the answer.
fn no_geometry(expr: &SqlExpr, place: &str) -> Result<(), ApiError> {
    no_refused_geometry(expr)?;
    match found(expr, RECOGNISED).into_iter().next() {
        None => Ok(()),
        Some(name) => Err(ApiError::bad_request(format!(
            "{FIELD}: {name} is part of a region test and belongs in WHERE, not in {place}; \
             nothing here returns a shape"
        ))),
    }
}

/// No geometry this service refuses outright, wherever it was written.
fn no_refused_geometry(expr: &SqlExpr) -> Result<(), ApiError> {
    match found(expr, &names(REFUSED)).into_iter().next() {
        None => Ok(()),
        Some(name) => Err(refused(&name).unwrap_or_else(|| not_adql(&name))),
    }
}

/// The refusal one of the named shapes gets, if it is one of them.
fn refused(name: &str) -> Option<ApiError> {
    REFUSED
        .iter()
        .find(|(refused, _)| *refused == name)
        .map(|(name, reason)| {
            ApiError::bad_request(format!("{FIELD}: {name} is not answered here — {reason}"))
        })
}

fn names(pairs: &'static [(&'static str, &'static str)]) -> Vec<&'static str> {
    pairs.iter().map(|(name, _)| *name).collect()
}

/// Whether any of these function names is called anywhere inside an expression.
fn mentions(expr: &SqlExpr, names: &[&str]) -> bool {
    !found(expr, names).is_empty()
}

/// The first of these function names called anywhere inside an expression.
///
/// A walk rather than a look at the top node: a name buried in an argument is still a call
/// this service would have to answer, and one that reached the planner would come back as
/// an unknown function rather than as a refusal naming the shape.
fn found(expr: &SqlExpr, names: &[&str]) -> Vec<String> {
    let mut seen = Vec::new();
    let _ = visit_expressions::<_, (), _>(expr, |node| {
        if let SqlExpr::Function(call) = node
            && let Some(name) = called(call)
            && names.contains(&name.as_str())
        {
            seen.push(name);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    seen
}

/// An expression with any parentheses around it taken off.
fn unnest(expr: &SqlExpr) -> &SqlExpr {
    match expr {
        SqlExpr::Nested(inner) => unnest(inner),
        other => other,
    }
}

/// The call an expression is, if it is one.
fn function(expr: &SqlExpr) -> Option<&Function> {
    match unnest(expr) {
        SqlExpr::Function(call) => Some(call),
        _ => None,
    }
}

/// What a call is called, in one case so that the comparisons below need not repeat
/// themselves. ADQL's function names are case-insensitive, and a quoted one is not a
/// function name at all.
fn called(call: &Function) -> Option<String> {
    let [part] = call.name.0.as_slice() else {
        return None;
    };
    Some(part.as_ident()?.value.to_ascii_uppercase())
}

/// A call's arguments, where they are the plain positional list ADQL writes.
///
/// Everything else `sqlparser` can hang off a call — a `FILTER`, an `OVER`, a named
/// argument, `DISTINCT` — is none of ADQL's, and a call carrying one is not the call this
/// service would be answering.
fn positional(call: &Function) -> Option<Vec<&SqlExpr>> {
    if call.uses_odbc_syntax
        || call.filter.is_some()
        || call.over.is_some()
        || call.null_treatment.is_some()
        || !call.within_group.is_empty()
        || !matches!(call.parameters, FunctionArguments::None)
    {
        return None;
    }
    let FunctionArguments::List(list) = &call.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
            _ => None,
        })
        .collect()
}

/// A number written out, with the sign in front of it if it has one.
fn number(expr: &SqlExpr) -> Option<f64> {
    match unnest(expr) {
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => number(expr).map(|value| -value),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => number(expr),
        SqlExpr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// A string literal, in the single quotes SQL writes one in.
fn string(expr: &SqlExpr) -> Option<&str> {
    match unnest(expr) {
        SqlExpr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => Some(text),
            _ => None,
        },
        _ => None,
    }
}

/// What a region test looks like, said once because every way of getting it wrong ends
/// here.
fn shape_of_a_region_test() -> ApiError {
    ApiError::bad_request(format!(
        "{FIELD}: a region test is 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) or \
         DISTANCE(POINT(ra, dec), POINT(45.0, -20.0)) < 0.1; ADQL has no boolean, so the \
         comparison is part of it"
    ))
}

/// Something `sqlparser` accepted that ADQL does not have.
fn not_adql(what: &str) -> ApiError {
    ApiError::bad_request(format!("{FIELD}: {what} is not ADQL"))
}

/// Something ADQL has that this route does not answer, and why it does not.
fn not_here(what: &str, because: &str) -> ApiError {
    ApiError::bad_request(format!("{FIELD}: {what} is not answered here — {because}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: sql::Limits = sql::Limits {
        max_depth: 50,
        max_nodes: 500,
    };

    fn parsed(query: &str) -> Query {
        parse(query, LIMITS).expect("this query should parse")
    }

    fn refusal(query: &str) -> String {
        parse(query, LIMITS)
            .expect_err("this query should be refused")
            .to_string()
    }

    fn text(expr: &Option<SqlExpr>) -> Option<String> {
        expr.as_ref().map(ToString::to_string)
    }

    fn select_text(query: &Query) -> Option<Vec<String>> {
        query.select.as_ref().map(|items| {
            items
                .iter()
                .map(|item| match &item.alias {
                    Some(alias) => format!("{} AS {alias}", item.expr),
                    None => item.expr.to_string(),
                })
                .collect()
        })
    }

    #[test]
    fn a_statement_becomes_a_table_a_projection_and_a_limit() {
        let query = parsed("SELECT TOP 100 source_id, ra, dec FROM gaia");
        assert_eq!(query.table, "gaia");
        assert_eq!(query.top, Some(100));
        assert_eq!(
            select_text(&query),
            Some(vec![
                "source_id".to_owned(),
                "ra".to_owned(),
                "dec".to_owned()
            ])
        );
        assert_eq!(text(&query.predicate), None);
        assert!(query.spatial.is_none());
    }

    #[test]
    fn a_wildcard_is_no_projection_at_all() {
        // Every other route here spells "every column" by leaving the field out, and this is
        // the same answer rather than an item that has to be understood further down.
        assert!(parsed("SELECT * FROM gaia").select.is_none());
    }

    #[test]
    fn an_alias_survives_into_the_select_list() {
        let query = parsed("SELECT phot_g_mean_mag AS g FROM gaia");
        assert_eq!(
            select_text(&query),
            Some(vec!["phot_g_mean_mag AS g".into()])
        );
    }

    #[test]
    fn the_table_keeps_the_spelling_it_was_written_in() {
        // Names answer to their own spelling here rather than to ADQL's fold to uppercase,
        // so the case is carried through untouched and the route decides what matches.
        assert_eq!(parsed("SELECT * FROM GaiaDR3").table, "GaiaDR3");
        assert_eq!(parsed(r#"SELECT * FROM "gaia dr3""#).table, "gaia dr3");
    }

    #[test]
    fn comments_and_a_trailing_semicolon_are_part_of_writing_a_query() {
        let query = parsed("SELECT ra -- the position\nFROM gaia;");
        assert_eq!(query.table, "gaia");
        assert_eq!(select_text(&query), Some(vec!["ra".to_owned()]));
    }

    #[test]
    fn a_second_statement_is_refused_rather_than_dropped() {
        assert!(refusal("SELECT ra FROM gaia; SELECT dec FROM gaia").contains("one statement"));
    }

    #[test]
    fn a_circle_becomes_a_region_and_the_columns_it_reads() {
        let query = parsed(
            "SELECT source_id FROM gaia \
             WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))",
        );
        assert_eq!(
            query.spatial,
            Some(Spatial {
                regions: vec![Region::Circle {
                    ra: 45.0,
                    dec: -20.0,
                    radius_deg: Some(0.1),
                    radius_arcsec: None,
                }],
                ra_column: "ra".to_owned(),
                dec_column: "dec".to_owned(),
            })
        );
        // The whole of the `WHERE` was the region, so nothing is left to plan.
        assert_eq!(text(&query.predicate), None);
    }

    #[test]
    fn every_spelling_of_the_comparison_is_the_same_region() {
        let circle = "CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))";
        let spellings = [
            format!("1 = {circle}"),
            format!("{circle} = 1"),
            format!("{circle} > 0"),
            format!("0 < {circle}"),
            "1 = INTERSECTS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))".to_owned(),
            // `INTERSECTS` is symmetric, so the point may be written second.
            "1 = INTERSECTS(CIRCLE(45.0, -20.0, 0.1), POINT(ra, dec))".to_owned(),
            "DISTANCE(POINT(ra, dec), POINT(45.0, -20.0)) < 0.1".to_owned(),
            "DISTANCE(POINT(45.0, -20.0), POINT(ra, dec)) <= 0.1".to_owned(),
            "0.1 > DISTANCE(POINT(ra, dec), POINT(45.0, -20.0))".to_owned(),
            "DISTANCE(ra, dec, 45.0, -20.0) < 0.1".to_owned(),
            "DISTANCE(45.0, -20.0, ra, dec) < 0.1".to_owned(),
        ];
        let expected = Spatial {
            regions: vec![Region::Circle {
                ra: 45.0,
                dec: -20.0,
                radius_deg: Some(0.1),
                radius_arcsec: None,
            }],
            ra_column: "ra".to_owned(),
            dec_column: "dec".to_owned(),
        };
        for spelling in spellings {
            let query = parsed(&format!("SELECT ra FROM gaia WHERE {spelling}"));
            assert_eq!(query.spatial, Some(expected.clone()), "{spelling}");
        }
    }

    #[test]
    fn the_rest_of_the_where_is_left_to_plan() {
        let query = parsed(
            "SELECT source_id FROM gaia \
             WHERE parallax > 1 \
             AND 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) \
             AND phot_g_mean_mag < 20",
        );
        assert!(query.spatial.is_some());
        assert_eq!(
            text(&query.predicate).as_deref(),
            Some("parallax > 1 AND phot_g_mean_mag < 20")
        );
    }

    #[test]
    fn a_disjunction_keeps_its_parentheses() {
        // The conjuncts are put back together with `AND`, which binds tighter than `OR`, so
        // dropping the parentheses here would return rows the caller did not ask for.
        let query = parsed(
            "SELECT ra FROM gaia \
             WHERE (parallax > 1 OR parallax < -1) \
             AND 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) \
             AND dec > 0",
        );
        assert_eq!(
            text(&query.predicate).as_deref(),
            Some("(parallax > 1 OR parallax < -1) AND dec > 0")
        );
    }

    #[test]
    fn regions_ored_together_are_a_union() {
        let query = parsed(
            "SELECT ra FROM gaia \
             WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) \
             OR 1 = CONTAINS(POINT(ra, dec), CIRCLE(10.0, 10.0, 0.2))",
        );
        let spatial = query.spatial.expect("both circles are one region field");
        assert_eq!(spatial.regions.len(), 2);
        assert_eq!(text(&query.predicate), None);
    }

    #[test]
    fn two_regions_anded_together_are_refused() {
        let refusal = refusal(
            "SELECT ra FROM gaia \
             WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) \
             AND 1 = CONTAINS(POINT(ra, dec), CIRCLE(10.0, 10.0, 0.2))",
        );
        assert!(refusal.contains("one region test per query"), "{refusal}");
    }

    #[test]
    fn two_regions_on_different_columns_are_refused() {
        let refusal = refusal(
            "SELECT ra FROM gaia \
             WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) \
             OR 1 = CONTAINS(POINT(objra, objdec), CIRCLE(10.0, 10.0, 0.2))",
        );
        assert!(refusal.contains("the same pair of columns"), "{refusal}");
    }

    #[test]
    fn a_moc_is_a_region() {
        let query =
            parsed("SELECT ra FROM gaia WHERE 1 = CONTAINS(POINT(ra, dec), MOC('4/30-33 38 52'))");
        assert_eq!(
            query.spatial.map(|spatial| spatial.regions),
            Some(vec![Region::Moc {
                ascii: Some("4/30-33 38 52".to_owned()),
                json: None,
            }])
        );
    }

    /// The table's own name comes off every reference written against it, whether the
    /// statement named the table or aliased it, and whether or not the client folded the
    /// name to uppercase on the way — which an ADQL client is entitled to do.
    #[test]
    fn the_tables_own_name_comes_off_a_column_reference() {
        for adql in [
            "SELECT gaia.source_id FROM gaia \
             WHERE gaia.parallax > 1 \
             AND 1 = CONTAINS(POINT(gaia.ra, gaia.dec), CIRCLE(45.0, -20.0, 0.1))",
            "SELECT g.source_id FROM gaia AS g \
             WHERE g.parallax > 1 \
             AND 1 = CONTAINS(POINT(g.ra, g.dec), CIRCLE(45.0, -20.0, 0.1))",
            "SELECT GAIA.source_id FROM gaia \
             WHERE GAIA.parallax > 1 \
             AND 1 = CONTAINS(POINT(GAIA.ra, GAIA.dec), CIRCLE(45.0, -20.0, 0.1))",
        ] {
            let query = parsed(adql);
            assert_eq!(
                select_text(&query),
                Some(vec!["source_id".to_owned()]),
                "{adql}"
            );
            assert_eq!(
                text(&query.predicate).as_deref(),
                Some("parallax > 1"),
                "{adql}"
            );
            assert_eq!(
                query
                    .spatial
                    .map(|spatial| (spatial.ra_column, spatial.dec_column)),
                Some(("ra".to_owned(), "dec".to_owned())),
                "{adql}"
            );
        }
    }

    /// An alias replaces the table's name rather than joining it, which is SQL's rule: with
    /// `AS g` in the statement, `gaia` is not a name for anything.
    #[test]
    fn an_alias_is_the_only_name_the_table_answers_to() {
        let query = parsed("SELECT gaia.source_id FROM gaia AS g");
        assert_eq!(select_text(&query), Some(vec!["gaia.source_id".to_owned()]));
    }

    /// A dotted name that is not the table's is a path into a column, and is left exactly as
    /// written — reading it as a qualifier would pack the column into a struct named after
    /// something that is not there.
    #[test]
    fn a_path_into_a_column_is_left_alone() {
        let query = parsed("SELECT lightcurve.mag FROM ztf WHERE lightcurve.band > 1");
        assert_eq!(select_text(&query), Some(vec!["lightcurve.mag".to_owned()]));
        assert_eq!(
            text(&query.predicate).as_deref(),
            Some("lightcurve.band > 1")
        );
    }

    /// The one collision, and the caller owns both halves of it: they named the table, and
    /// the table's name won. Renaming it in the request's own `tables` is what settles it,
    /// which is why nothing here consults the file to break the tie.
    #[test]
    fn the_table_wins_where_its_name_is_also_a_columns() {
        let query = parsed("SELECT lightcurve.mag FROM lightcurve");
        assert_eq!(select_text(&query), Some(vec!["mag".to_owned()]));
    }

    #[test]
    fn a_circle_is_constant() {
        let refusal =
            refusal("SELECT ra FROM gaia WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(ra, dec, 0.1))");
        assert!(
            refusal.contains("constants rather than columns"),
            "{refusal}"
        );
    }

    #[test]
    fn the_older_circle_with_a_frame_says_what_changed() {
        let refusal = refusal(
            "SELECT ra FROM gaia \
             WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE('ICRS', 45.0, -20.0, 0.1))",
        );
        assert!(refusal.contains("no coordinate system"), "{refusal}");
    }

    #[test]
    fn a_shape_this_service_does_not_test_is_refused_by_name() {
        for (name, predicate) in [
            (
                "POLYGON",
                "1 = CONTAINS(POINT(ra, dec), POLYGON(1, 2, 3, 4, 5, 6))",
            ),
            (
                "BOX",
                "1 = CONTAINS(POINT(ra, dec), BOX(45.0, -20.0, 1.0, 1.0))",
            ),
            (
                "REGION",
                "1 = CONTAINS(POINT(ra, dec), REGION('circle 45 -20 1'))",
            ),
            ("AREA", "AREA(CIRCLE(45.0, -20.0, 0.1)) > 1"),
        ] {
            let refusal = refusal(&format!("SELECT ra FROM gaia WHERE {predicate}"));
            assert!(refusal.contains(name), "{name}: {refusal}");
            assert!(refusal.contains("not answered here"), "{name}: {refusal}");
        }
    }

    #[test]
    fn a_geometry_predicate_is_never_merely_dropped() {
        // The failure this refusal exists for: a region test the region machinery never sees
        // is one no covering can prune on, and the query reads the whole catalog to answer it.
        let refusal = refusal(
            "SELECT ra FROM gaia WHERE CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1)) = 2",
        );
        assert!(refusal.contains("a region test is"), "{refusal}");
    }

    #[test]
    fn a_shape_cannot_be_returned() {
        let refusal = refusal("SELECT POINT(ra, dec) FROM gaia");
        assert!(refusal.contains("belongs in WHERE"), "{refusal}");
    }

    #[test]
    fn the_clauses_that_need_every_row_at_once_are_refused_by_name() {
        for (clause, query) in [
            ("GROUP BY", "SELECT ra FROM gaia GROUP BY ra"),
            // Without a `GROUP BY` beside it, which is refused first and would be what this
            // case actually proved.
            ("HAVING", "SELECT ra FROM gaia HAVING ra > 1"),
            ("ORDER BY", "SELECT ra FROM gaia ORDER BY ra"),
            ("DISTINCT", "SELECT DISTINCT ra FROM gaia"),
            ("WITH", "WITH x AS (SELECT ra FROM gaia) SELECT ra FROM x"),
            (
                "UNION, INTERSECT and EXCEPT",
                "SELECT ra FROM gaia UNION SELECT ra FROM ztf",
            ),
            ("JOIN", "SELECT ra FROM gaia JOIN ztf ON gaia.ra = ztf.ra"),
            ("more than one table", "SELECT ra FROM gaia, ztf"),
        ] {
            let refusal = refusal(query);
            assert!(refusal.contains(clause), "{query}: {refusal}");
        }
    }

    #[test]
    fn a_row_limit_is_spelled_top() {
        let refusal = refusal("SELECT ra FROM gaia LIMIT 10");
        assert!(refusal.contains("TOP n"), "{refusal}");
    }

    #[test]
    fn only_select_is_answered() {
        for statement in [
            "DELETE FROM gaia",
            "INSERT INTO gaia VALUES (1)",
            "DROP TABLE gaia",
        ] {
            let refusal = refusal(statement);
            assert!(
                refusal.contains("only SELECT") || refusal.contains("is not a query"),
                "{statement}: {refusal}"
            );
        }
    }
}
