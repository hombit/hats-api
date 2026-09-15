//! The caller's SQL: a list of column names, a row predicate, and the one statement the ADQL
//! route takes.
//!
//! `columns` and `filters` each arrive in two wire forms — a body's and a query string's —
//! which lower through the same code and meet the same allowlist below, so which one a
//! request used changes nothing about what it means.
//!
//! Expressions, never statements. Each piece is parsed on its own — one name, one boolean
//! expression — and the parser must reach the end of the string, so there is no `FROM` to
//! hang a join off and no way to write a second query into either field. The target is
//! named by the request's path and its identity by `url`, neither of which the expressions
//! can reach.
//!
//! Parsing as an expression is not the whole check, though: an aggregate, a window
//! function and a call to `random()` are all expressions. So what the planner returns is
//! walked, and only the node kinds this service will actually run are let through.

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::execution::context::SessionState;
// The one function this module *builds* rather than lets through: packing a nested column's
// fields back into it needs something that makes a struct. Imported rather than looked up in
// the registry, so a build without it is a compile error instead of a request that fails.
use datafusion::functions::core::expr_fn::named_struct;
use datafusion::logical_expr::{Expr, LogicalPlan, UNNAMED_TABLE, Volatility};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, ExprWithAlias, Ident, Statement, visit_expressions_mut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

use crate::adql;
use crate::config::LimitsConfig;
use crate::error::ApiError;

/// One dialect, borrowed for the lifetime of the process so that a parser built from it
/// is not tied to the stack frame that made it.
static DIALECT: GenericDialect = GenericDialect;

/// How much SQL one request may contain, as the operator set it.
///
/// Both bound the caller's text rather than the answer: how many rows come back is a
/// different question, and neither of these is an answer to it.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_depth: usize,
    pub max_nodes: usize,
}

impl From<&LimitsConfig> for Limits {
    fn from(config: &LimitsConfig) -> Self {
        Self {
            max_depth: config.max_expression_depth,
            max_nodes: config.max_expression_nodes,
        }
    }
}

/// One whole statement, parsed and no further.
///
/// The one thing here that reads a statement, for the route whose caller writes a query
/// rather than a pair of fields. Everything else in this module is an expression because an
/// expression is all those routes take; this parses and stops, and what becomes of the
/// statement is the caller's route's to decide. No text is ever assembled into one.
///
/// One statement, and the parser must reach the end of it. A trailing `;` is allowed and is
/// the only thing that may follow, so a second statement written after the first is refused
/// rather than dropped.
pub fn statement(sql: &str, field: &str, limits: Limits) -> Result<Statement, ApiError> {
    let refuse = |error: &dyn std::fmt::Display| {
        ApiError::bad_request(format!("{field} is not a query: {error}"))
    };
    let tokens = Tokenizer::new(&DIALECT, sql)
        .tokenize_with_location()
        .map_err(|error| refuse(&error))?;
    let mut parser = Parser::new(&DIALECT)
        .with_recursion_limit(limits.max_depth)
        .with_tokens_with_locations(tokens);
    let parsed = parser.parse_statement().map_err(|error| refuse(&error))?;
    if parser.peek_token().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token().token != Token::EOF {
        return Err(ApiError::bad_request(format!(
            "{field} must be one statement; this one does not end where it should"
        )));
    }
    Ok(parsed)
}

/// The projection as column names, one name per element.
///
/// A name, and nothing else. `Gmag` and `lightcurve.mag` are the whole language here;
/// `mag - 0.1 AS corrected` is a statement's to compute, and the ADQL route is where a
/// caller writes one.
///
/// A name that is not an ordinary identifier is quoted, the way SQL quotes one:
/// `"E(BP-RP)"`. Unquoted it parses as a call to a function named `E`, which is a
/// refusal rather than a wrong column. An element holding two names is refused as well:
/// the list is the separator here, so a comma inside one is a name with a comma in it.
pub fn columns(
    state: &SessionState,
    schema: &DFSchema,
    names: &[String],
    limits: Limits,
) -> Result<Vec<Expr>, ApiError> {
    const FIELD: &str = "columns";

    if names.is_empty() {
        return Err(ApiError::bad_request(
            "columns names no column; leave it out to get every column",
        ));
    }
    let parts = names
        .iter()
        .map(|name| {
            let item = parse(tokenize(name, FIELD)?, FIELD, limits, Parser::parse_expr)?;
            column_part(state, schema, item, limits)
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    regrouped(state, schema, parts, FIELD, limits)
}

/// The same names comma-separated in one string, which is all a url's query string can
/// carry.
///
/// The comma is the separator a body expresses as an array, so this is one wire form of the
/// list above rather than a second language: both map each name the same way and meet the
/// same allowlist. Parsed rather than split on the character, since a quoted name may hold
/// one — `"E(BP,RP)"` is one column.
pub fn column_text(
    state: &SessionState,
    schema: &DFSchema,
    list: &str,
    limits: Limits,
) -> Result<Vec<Expr>, ApiError> {
    const FIELD: &str = "columns";

    let items = parse(tokenize(list, FIELD)?, FIELD, limits, |parser| {
        parser.parse_comma_separated(Parser::parse_expr)
    })?;
    let parts = items
        .into_iter()
        .map(|item| column_part(state, schema, item, limits))
        .collect::<Result<Vec<Part>, ApiError>>()?;
    regrouped(state, schema, parts, FIELD, limits)
}

/// One name of a `columns` list, whichever wire form carried it.
fn column_part(
    state: &SessionState,
    schema: &DFSchema,
    mut item: SqlExpr,
    limits: Limits,
) -> Result<Part, ApiError> {
    const FIELD: &str = "columns";

    resolve_identifiers(&mut item, schema);
    // After resolving, so a path is grouped and named by the file's spelling rather
    // than the caller's.
    //
    // A name this file has not got is planned rather than refused here, so that the
    // message a caller gets is the planner's — which names the closest column it
    // has — instead of this one saying they wrote an expression when they did not.
    match projected_path(&item, schema) {
        Some(path) => Ok(Part::Path(path)),
        None if column_path(&item).is_some() => {
            plan(state, schema, item, FIELD, limits).map(Part::Planned)
        }
        None => Err(ApiError::bad_request(format!(
            "{FIELD} takes column names; write an ADQL query to compute one"
        ))),
    }
}

/// The row predicate: one boolean expression, and no second expression after it.
///
/// The separators a query string needs are not part of it: `&&`, `,` and `;` are
/// [`filter_text`]'s, for a carrier that has to join two conditions inside one parameter. A
/// body writes `AND`.
pub fn filters(
    state: &SessionState,
    schema: &DFSchema,
    text: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    const FIELD: &str = "filters";

    plan_predicate(state, schema, tokenize(text, FIELD)?, FIELD, limits)
}

/// The same predicate as one string, with `&&`, `,` and `;` accepted for `AND`, `AND` and
/// `OR`.
///
/// The file-server mode's alone. A query string carries one `filters=` and no arrays, so the
/// conjunction has to be written inside the value — and `&` ends a parameter, which is why
/// `AND` is typable and `&&` must be encoded; `,` and `;` are what `lsdb` sends. The rewrite
/// itself is on the token stream, where a separator inside a string literal is data — see
/// `separators_written_as_operators`.
pub fn filter_text(
    state: &SessionState,
    schema: &DFSchema,
    text: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    const FIELD: &str = "filters";

    let tokens = separators_written_as_operators(tokenize(text, FIELD)?);
    plan_predicate(state, schema, tokens, FIELD, limits)
}

/// The column a structured field names, in the file's own spelling of it.
///
/// `region` names the two columns it refines against, and a name there is a column name
/// rather than SQL: no quoting, no expression, no dotted path. So it resolves by the same
/// rule `resolve_identifiers` applies — the file's spelling, or that spelling in
/// lowercase, and nothing else — without going through a parser that would give a caller
/// somewhere else to put an expression.
///
/// The type is checked here because the alternative is worse. A coordinate column that
/// holds strings reaches the planner as a subtraction of a number from text, and what
/// comes back is DataFusion's account of a coercion rather than anything naming the field
/// the caller filled in.
pub fn coordinate_column(
    schema: &DFSchema,
    relation: Option<&TableReference>,
    name: &str,
    field: &str,
) -> Result<Expr, ApiError> {
    let (column, data_type) = named_column(schema, relation, name, field)?;
    if !data_type.is_numeric() {
        return Err(ApiError::bad_request(format!(
            "{field}: {name:?} holds {data_type:?}; a coordinate must be a number"
        )));
    }
    Ok(column)
}

/// The union of many terms, combined into a balanced tree rather than a left-deep chain.
///
/// `Iterator::reduce` folds left, so `n` terms come out as `(((a ∨ b) ∨ c) ∨ d) …` — a tree
/// of depth `n`. Every walk over an `Expr` is recursive: DataFusion's `TreeNode` traversals,
/// each optimizer pass, and `Expr`'s own derived `Drop`. So a left-deep chain needs a stack
/// frame per term, and the thread dies on a body this service otherwise accepts. It is an
/// abort rather than an error — a stack overflow is `SIGSEGV`, which no handler here turns
/// into a `500` — so it takes the process down with every other request in flight.
///
/// **Both of the shapes that reach this are unbounded by construction.** A `region` is one
/// term per shape and nothing counts them — `max_expression_nodes` is about `columns` and
/// `filters`, and never sees a structured field — so a cross-match sending one circle per
/// source is as many terms as the body holds. A `moc` is one term per range and skips
/// [`crate::sky::healpix`]'s range budget entirely, the ranges being the whole answer there
/// rather than a saving. Pairing terms until one is left makes both `⌈log₂ n⌉` deep instead:
/// thirty thousand circles is fifteen frames rather than thirty thousand.
///
/// The grouping is not observable in the answer — `OR` is associative, and DataFusion
/// flattens what it wants to flatten in its own normalization — so this buys the depth and
/// changes no rows.
pub fn any_of(terms: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    balanced(terms.into_iter().collect(), Expr::or)
}

/// [`any_of`]'s shape, for whichever operator: pair the terms up, then pair the pairs, until
/// one is left. An odd term at the end of a round is carried into the next one untouched.
fn balanced(mut terms: Vec<Expr>, combine: fn(Expr, Expr) -> Expr) -> Option<Expr> {
    while terms.len() > 1 {
        let mut paired = Vec::with_capacity(terms.len().div_ceil(2));
        let mut rest = terms.into_iter();
        while let Some(left) = rest.next() {
            paired.push(match rest.next() {
                Some(right) => combine(left, right),
                None => left,
            });
        }
        terms = paired;
    }
    terms.pop()
}

/// A column of whole numbers a structured field names, with the integer type it is written
/// at.
///
/// The type comes back because a bound on such a column is compared against a literal, and
/// a literal of another integer type is not a comparison DataFusion keeps as one: it widens
/// both sides to something no row-group statistic, page index or bloom filter is held in,
/// which turns the cheapest test in the plan into a scan. Which types those are is
/// [`crate::sky::healpix`]'s to decide, since what fits depends on the order the caller says the
/// column is at.
pub fn integer_column(
    schema: &DFSchema,
    relation: Option<&TableReference>,
    name: &str,
    field: &str,
) -> Result<(Expr, DataType), ApiError> {
    let (column, data_type) = named_column(schema, relation, name, field)?;
    if !data_type.is_integer() {
        return Err(ApiError::bad_request(format!(
            "{field}: {name:?} holds {data_type:?}; a HEALPix cell must be a whole number"
        )));
    }
    Ok((column, data_type))
}

/// One column name in the file's own spelling, with the type it holds.
/// **The relation narrows both halves, and it has to.** Where several tables are in scope a
/// bare `ra` is a column of each of them, so the name is resolved among one table's fields and
/// the column that comes back carries that table — an unqualified one is ambiguous, which the
/// planner answers with an error rather than a choice.
fn named_column(
    schema: &DFSchema,
    relation: Option<&TableReference>,
    name: &str,
    field: &str,
) -> Result<(Expr, DataType), ApiError> {
    let fields = fields_of(schema, relation);
    let mut ident = Ident::new(name);
    let Some(data_type) = resolve_segment(&mut ident, &fields) else {
        return Err(ApiError::bad_request(format!(
            "{field}: this file has no column named {name:?}"
        )));
    };
    Ok((
        Expr::Column(Column::new(relation.cloned(), ident.value)),
        data_type,
    ))
}

/// One table's fields, or every field where no table was named.
pub fn fields_of(schema: &DFSchema, relation: Option<&TableReference>) -> Fields {
    let Some(relation) = relation else {
        return schema.fields().clone();
    };
    schema
        .iter()
        .filter(|(qualifier, _)| *qualifier == Some(relation))
        .map(|(_, field)| Arc::clone(field))
        .collect()
}

/// One boolean expression, however it was spelled, and nothing after it.
fn plan_predicate(
    state: &SessionState,
    schema: &DFSchema,
    tokens: Vec<TokenWithSpan>,
    field: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    let expr = parse(tokens, field, limits, Parser::parse_expr)?;
    plan(state, schema, expr, field, limits)
}

/// Rewrite `&&` into `AND`, and a top-level `,` and `;` into `AND` and `OR`, on the tokens
/// rather than on the text.
///
/// Rewriting the text would be a parser written by accident: inside a string literal
/// `&&` is two characters of data, and nothing working on the characters can tell that
/// from the operator. The tokenizer has already made the distinction — a literal is one
/// token by the time this runs — so the rewrite is exact.
///
/// `&&` already has a meaning to the tokenizer — it is PostgreSQL's array-overlap
/// operator, `Token::Overlap` — so the rewrite is that one token becoming a keyword.
/// Left alone it plans as an overlap and fails as an unsupported operator, which is a
/// confusing way to be told that a spelling is not understood.
///
/// **`,` and `;` are what `lsdb` sends.** Reading a HATS catalog over `http(s)://`, it
/// pushes its predicate down as a query string in disjunctive normal form —
/// `_healpix_29>=a,_healpix_29<b;_healpix_29>=c,_healpix_29<d` — with `,` joining a
/// conjunction and `;` separating the alternatives. Refusing that spelling is refusing
/// every read `lsdb` makes, since it attaches one to every partition it fetches. SQL's own
/// precedence is what makes the plain rewrite correct: `AND` binds tighter than `OR`, so
/// the alternatives come out grouped the way the caller meant them.
///
/// **Only outside parentheses.** A comma inside one belongs to an `IN` list or a function's
/// arguments, where it is not a separator between predicates and rewriting it would change
/// what the caller asked. Depth is counted here rather than guessed from the text.
///
/// `||` gets no such treatment. It is SQL's string concatenation, and reading it as `OR`
/// would leave one spelling with two meanings and no way to ask for the other. `OR` is
/// written out.
fn separators_written_as_operators(tokens: Vec<TokenWithSpan>) -> Vec<TokenWithSpan> {
    let mut depth = 0i32;
    tokens
        .into_iter()
        .map(|token| {
            let keyword = match token.token {
                Token::LParen => {
                    depth += 1;
                    None
                }
                Token::RParen => {
                    depth -= 1;
                    None
                }
                Token::Overlap => Some("AND"),
                Token::Comma if depth == 0 => Some("AND"),
                Token::SemiColon if depth == 0 => Some("OR"),
                _ => None,
            };
            match keyword {
                Some(keyword) => TokenWithSpan {
                    token: Token::make_keyword(keyword),
                    span: token.span,
                },
                None => token,
            }
        })
        .collect()
}

/// The column a caller named, when what they wrote is a name and nothing else.
fn column_path(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
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

/// The path a caller named, when what they wrote is a name into *this file's* columns.
///
/// The first segment has to be one of the file's own fields. A compound identifier whose
/// head is not — a qualified column reference — is left to the planner, which is what
/// already reads one; treating it as a path would put the qualifier where a column name
/// goes and pack the column into a struct named after the table.
fn projected_path(expr: &SqlExpr, schema: &DFSchema) -> Option<Vec<Ident>> {
    let parts = match expr {
        SqlExpr::Identifier(ident) => vec![ident.clone()],
        SqlExpr::CompoundIdentifier(parts) => parts.clone(),
        _ => return None,
    };
    // A path with no head is not a name, whatever the parser made of it. Taken rather than
    // asserted, so that everything downstream has a column to group under.
    let head = parts.first()?;
    schema
        .fields()
        .iter()
        .any(|field| field.name() == &head.value)
        .then_some(parts)
}

/// One item of a projection, once it is known which of the two it is.
enum Part {
    /// A name into the file's columns, kept as its resolved segments so that the pieces
    /// naming one column can be put back together under it. Never empty.
    Path(Vec<Ident>),
    /// A name whose head is not one of the file's columns, planned as written so that the
    /// planner is the one that reads a qualifier or names the closest column.
    Planned(Expr),
}

/// Where one output column comes from, in the order the caller reached it.
enum Slot {
    Planned(Expr),
    /// A column packed from the names that reach into it. Carries no index: the roots are
    /// built in the order their slots were made, so they are taken in that same order.
    Packed,
}

/// The projection, with the pieces of a nested column packed back into it.
///
/// **A row's light curve is one value.** A caller who asks for `lightcurve.mag` and
/// `lightcurve.mjd` has asked for less of that value, not for two columns beside it — so
/// what comes back is one `lightcurve` holding those two fields, the way `pyarrow` reads a
/// subset of a struct. Handing back `mag` and `mjd` flat would make the reader above —
/// `nested_pandas`, `astropy` — put the row together again, against a schema that no longer
/// matches the file's.
///
/// **A name that reaches a whole column takes it whole.** `lightcurve` and
/// `lightcurve.mag` together are `lightcurve`, every field of it: the deeper name asks for
/// part of what the shallower one already returns, so the union is the column itself and
/// nothing the caller wrote is dropped.
///
/// The order is the caller's — a column appears where they first named it, and its fields
/// in the order they wrote them.
fn regrouped(
    state: &SessionState,
    schema: &DFSchema,
    parts: Vec<Part>,
    field: &str,
    limits: Limits,
) -> Result<Vec<Expr>, ApiError> {
    // One slot per output, in the order the caller reached it, so that a second mention of
    // a column adds a field to where it already is rather than a column after it.
    let mut slots: Vec<Slot> = Vec::new();
    let mut roots: Vec<(Ident, Vec<Vec<Ident>>)> = Vec::new();
    for part in parts {
        match part {
            Part::Planned(expr) => slots.push(Slot::Planned(expr)),
            Part::Path(path) => {
                let Some(head) = path.first().cloned() else {
                    continue;
                };
                match roots.iter_mut().find(|(root, _)| root.value == head.value) {
                    Some((_, paths)) => paths.push(path),
                    None => {
                        slots.push(Slot::Packed);
                        roots.push((head, vec![path]));
                    }
                }
            }
        }
    }
    let mut built = roots
        .iter()
        .map(|(root, paths)| {
            let under: Vec<&[Ident]> = paths.iter().map(Vec::as_slice).collect();
            packed(
                state,
                schema,
                std::slice::from_ref(root),
                &under,
                field,
                limits,
            )
        })
        .collect::<Result<VecDeque<Expr>, ApiError>>()?;
    // The slots and the roots were made in step, so taking from the front puts each column
    // back where the caller first named it.
    Ok(slots
        .into_iter()
        .filter_map(|slot| match slot {
            Slot::Planned(expr) => Some(expr),
            Slot::Packed => built.pop_front(),
        })
        .collect())
}

/// One column of the answer, from every name the caller wrote that reaches into it.
///
/// `prefix` is the path so far and `paths` are the full paths under it, each of which starts
/// with `prefix`. A path that stops here asks for everything below it, so the whole value at
/// `prefix` is the answer and the deeper names add nothing. Otherwise the fields named below
/// are built back into a struct, and each of those is this same question one level down.
fn packed(
    state: &SessionState,
    schema: &DFSchema,
    prefix: &[Ident],
    paths: &[&[Ident]],
    field: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    // Named for the path it came from, spelled the way the *file* spells it since the
    // segments have been resolved. Left alone only for a plain column, which DataFusion
    // already names after itself; everything else it names after the access or the call it
    // planned — `?table?.lightcurve[mag]`, `named_struct(Utf8("mag"),…)` — which is a key no
    // caller could predict from what they wrote.
    let named = |expr: Expr, plain: bool| match plain && prefix.len() == 1 {
        true => expr,
        false => expr.alias(dotted(prefix)),
    };
    // The fields named below this one, in the order the caller first named each. A path that
    // has nothing below the prefix is a name for the whole value here, and the deeper names
    // then add nothing — so it answers for all of them, wherever in the list it was written.
    let mut fields: Vec<(Ident, Vec<&[Ident]>)> = Vec::new();
    for path in paths {
        let Some(next) = path.get(prefix.len()) else {
            return plan_path(state, schema, prefix, field, limits).map(|expr| named(expr, true));
        };
        match fields.iter_mut().find(|(name, _)| name.value == next.value) {
            Some((_, under)) => under.push(path),
            None => fields.push((next.clone(), vec![path])),
        }
    }
    let mut arguments = Vec::with_capacity(fields.len() * 2);
    let mut at: Vec<Ident> = prefix.to_vec();
    for (name, under) in fields {
        arguments.push(Expr::Literal(ScalarValue::from(name.value.as_str()), None));
        at.push(name);
        arguments.push(packed(state, schema, &at, &under, field, limits)?.unalias());
        at.pop();
    }
    Ok(named(named_struct(arguments), false))
}

/// A resolved path, as the caller would write it back.
fn dotted(path: &[Ident]) -> String {
    path.iter()
        .map(|part| part.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// One path, planned as the access it is.
fn plan_path(
    state: &SessionState,
    schema: &DFSchema,
    path: &[Ident],
    field: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    let expr = match path {
        [one] => SqlExpr::Identifier(one.clone()),
        parts => SqlExpr::CompoundIdentifier(parts.to_vec()),
    };
    plan(state, schema, expr, field, limits)
}

/// The caller's text as tokens, which is as far as anything gets before the grammar has
/// a say.
fn tokenize(sql: &str, field: &str) -> Result<Vec<TokenWithSpan>, ApiError> {
    Tokenizer::new(&DIALECT, sql)
        .tokenize_with_location()
        .map_err(|error| ApiError::bad_request(format!("{field} is not a SQL expression: {error}")))
}

/// Run one of `sqlparser`'s own parsers over the whole token stream.
///
/// The end-of-input check is what makes this a parser for an expression rather than for
/// the first expression in something longer: without it, `1 UNION SELECT …` parses as
/// `1` and the rest is silently dropped.
fn parse<T>(
    tokens: Vec<TokenWithSpan>,
    field: &str,
    limits: Limits,
    parse: impl FnOnce(&mut Parser<'static>) -> Result<T, ParserError>,
) -> Result<T, ApiError> {
    let refuse = |error: &ParserError| {
        ApiError::bad_request(format!("{field} is not a SQL expression: {error}"))
    };
    let mut parser = Parser::new(&DIALECT)
        .with_recursion_limit(limits.max_depth)
        .with_tokens_with_locations(tokens);
    let parsed = parse(&mut parser).map_err(|error| refuse(&error))?;
    if parser.peek_token().token != Token::EOF {
        return Err(ApiError::bad_request(format!(
            "{field} must be one expression; this one does not end where it should"
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
    mut expr: SqlExpr,
    field: &str,
    limits: Limits,
) -> Result<Expr, ApiError> {
    // Idempotent, and `column_part` has already done it so that a bare path's output name
    // is the file's spelling rather than the caller's.
    resolve_identifiers(&mut expr, schema);
    // Every failure here is the caller's expression not fitting the caller's file:
    // an unknown column, a type that will not compare, a function that is not
    // registered.
    let expr = state
        .create_logical_expr_from_sql_expr(ExprWithAlias { expr, alias: None }, schema)
        .map_err(|error| {
            ApiError::bad_request(format!("{field}: {}", unqualified(&error.to_string())))
        })?;
    check(&expr, field, limits)?;
    Ok(expr)
}

/// Take DataFusion's name for the table it planned against out of a message meant for a
/// caller.
///
/// It calls an unnamed one `?table?`, and its schema errors quote it: `Did you mean
/// '"?table?"."Gmag"'?`. There is one table here and the caller never named it, so the
/// qualifier is noise wrapped around the part that would have helped. The message itself
/// is kept — it names the closest column, which is the useful half.
fn unqualified(message: &str) -> String {
    message.replace(&format!("\"{UNNAMED_TABLE}\"."), "")
}

/// Rewrite the names a caller wrote into the names the file actually uses.
///
/// **A column answers to its own name, and to its name in lowercase.** Nothing else.
///
/// SQL folds an unquoted identifier to lowercase, which puts every mixed-case column out
/// of reach without quotes — and `Gmag`, `Norder` and `objectId` are ordinary names in an
/// astronomy catalog, read straight off a file the caller is looking at. So the file's own
/// spelling has to work. Lowercase has to work too, because that is what SQL says an
/// unquoted name means and what a caller who has not looked will type.
///
/// Every other casing is refused rather than resolved. `GMAG` finding `Gmag` would mean
/// the set of names a column answers to depends on what else is in the file, and a caller
/// could not tell from their own request which column they had read.
///
/// The exact name is tried first, so two columns whose lowercase forms collide — `flux`
/// and `Flux` — are each still reachable by writing them out; only the lowercase form they
/// share resolves to neither. A quoted identifier is exact by definition and never
/// rewritten, and a name matching nothing is left for DataFusion, whose error already
/// names the closest column it has.
fn resolve_identifiers(expr: &mut SqlExpr, schema: &DFSchema) {
    let _ = visit_expressions_mut::<_, (), _>(expr, |node| {
        match node {
            SqlExpr::Identifier(ident) => resolve(std::slice::from_mut(ident), schema.fields()),
            SqlExpr::CompoundIdentifier(parts) => resolve(parts, schema.fields()),
            _ => {}
        }
        ControlFlow::Continue(())
    });
}

/// One dotted path, a segment at a time: the first against the file's columns, each next
/// against the struct the one before it turned out to be.
fn resolve(parts: &mut [Ident], fields: &Fields) {
    let mut fields = fields.clone();
    for part in parts {
        match resolve_segment(part, &fields) {
            Some(DataType::Struct(children)) => fields = children,
            // Not a struct, or a name that matched nothing: there is nothing left for the
            // rest of the path to resolve against.
            _ => return,
        }
    }
}

/// One segment, and the type it named — `None` when nothing matched.
fn resolve_segment(part: &mut Ident, fields: &Fields) -> Option<DataType> {
    let named = |name: &str| {
        fields
            .iter()
            .find(|field| field.name() == name)
            .map(|field| field.data_type().clone())
    };
    if part.quote_style.is_some() {
        return named(&part.value);
    }
    if let Some(data_type) = named(&part.value) {
        return Some(data_type);
    }
    // ASCII folding only, so that Unicode case folding is never what decides which column
    // a request read.
    let mut lowercased = fields
        .iter()
        .filter(|field| field.name().to_ascii_lowercase() == part.value);
    let field = lowercased.next()?;
    // Two columns share this lowercase form, so it is the one spelling that names neither
    // of them; each is still reachable by writing it out.
    if lowercased.next().is_some() {
        return None;
    }
    part.value = field.name().clone();
    // Quoted, so the resolved name is exact from here on whatever the parser is told to do
    // with unquoted identifiers.
    part.quote_style = Some('"');
    Some(field.data_type().clone())
}

/// A function whose name means one thing here and another somewhere the caller has been,
/// where both answers look ordinary.
///
/// **A refusal by name, which is what the volatility rule is written against** — so it is a
/// named exception rather than a widening of it. Volatility is about whether one request's
/// answer matches the next's; this is about whether the answer is the one the caller read
/// their own expression as asking for, which nothing in a signature can say.
///
/// The bar for adding a name is both readings being plausible *and* the wrong one coming
/// back as a number rather than as an error. `log` is that: DataFusion's is base ten, and
/// MySQL's, `numpy`'s and ADQL's are all the natural logarithm — so the same expression
/// means two things a factor of 2.3 apart, and a caller reads whichever their background
/// says. Most readers here arrive from Python, where it is the natural logarithm, which is
/// not the one this build would have given them.
///
/// `log10`, `log2` and `ln` each say which they are and are what the refusal points at, and
/// another base is `ln(x) / ln(b)`, so nothing is out of reach.
///
/// Not a place to put functions that are merely unwanted: those are absent from the build
/// instead, and an absent one is already an error naming it.
const AMBIGUOUS: &[(&str, &str)] = &[(
    "log",
    "log means base ten in some SQL and the natural logarithm in others, so it is not \
     answered here; write log10 for base ten, ln for the natural logarithm, log2 for base \
     two, and ln(x) / ln(b) for any other base",
)];

/// What to say about a function name, where it is one of those.
fn ambiguous(name: &str) -> Option<&'static str> {
    AMBIGUOUS
        .iter()
        .find(|(ambiguous, _)| *ambiguous == name)
        .map(|(_, reason)| *reason)
}

/// Walk the planned expression and refuse everything this service will not run.
fn check(expr: &Expr, field: &str, limits: Limits) -> Result<(), ApiError> {
    let mut nodes = 0usize;
    walk(expr, Shape::Row, field, limits, &mut nodes)
}

/// The same judgement over every expression of a planned statement.
///
/// **The rules that are about the answer hold; the one about a row does not.** A statement
/// is planned as a whole, so an aggregate, a window function and a subquery are what the
/// caller asked for rather than something a per-row expression smuggled in — and refusing
/// them here would refuse most of what a statement is written for. What still holds is
/// everything that is true of any answer this service gives: a volatile function makes one
/// request differ from the next, and the ambiguous-name list refuses a function whose answer
/// is not the one the caller read their own text as asking for.
///
/// **`rand` is the one exception, and it is this shape's alone.** ADQL makes it mandatory, so
/// a route answering ADQL has to have it; every other route is refused it, which is what
/// scoping the exception to a statement means. It is the first answer this service gives that
/// differs between two identical requests — see [`crate::adql::functions`].
///
/// The node budget is the whole plan's rather than one expression's. A statement has many
/// expressions and no single one of them is the size worth bounding.
pub fn check_plan(plan: &LogicalPlan, limits: Limits) -> Result<(), ApiError> {
    const FIELD: &str = "query";

    let mut nodes = 0usize;
    let mut refusal = None;
    // Subqueries too: they are expressions of the plan they sit in, and a `now()` inside one
    // is as unreproducible as a `now()` outside it.
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            match walk(expr, Shape::Statement, FIELD, limits, &mut nodes) {
                Ok(()) => Ok(TreeNodeRecursion::Continue),
                Err(error) => {
                    refusal = Some(error);
                    Ok(TreeNodeRecursion::Stop)
                }
            }
        })
    });
    match refusal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// One expression, judged node by node, counting into a budget it may share with others.
fn walk(
    expr: &Expr,
    shape: Shape,
    field: &str,
    limits: Limits,
    nodes: &mut usize,
) -> Result<(), ApiError> {
    let mut refusal = None;
    // The closure cannot fail, so the walk itself cannot: the refusal is carried out
    // rather than raised, and the walk stops at the first one.
    let _ = expr.apply(|node| {
        *nodes += 1;
        if *nodes > limits.max_nodes {
            refusal = Some(format!(
                "{field} is too large: more than {} terms",
                limits.max_nodes
            ));
            return Ok(TreeNodeRecursion::Stop);
        }
        match allowed(node, shape) {
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

/// What an expression is part of, which decides the one rule the two differ on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A field of a request, evaluated one row at a time. Nothing that combines rows.
    Row,
    /// An expression of a statement the planner built, where combining rows is the point.
    Statement,
}

/// One node of a planned expression, judged.
///
/// Written as an allowlist with an exhaustive match rather than as a list of what is
/// refused: a DataFusion upgrade that adds an expression kind is then a compile error
/// here, and someone decides whether it belongs in a per-row expression instead of a
/// caller finding out that it already did.
fn allowed(expr: &Expr, shape: Shape) -> Result<(), String> {
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
        //
        // `AMBIGUOUS` is checked first and is the one list of names here, for the one thing
        // volatility cannot see.
        Expr::ScalarFunction(call) => match ambiguous(call.func.name()) {
            Some(reason) => Err(reason.to_owned()),
            // The one name let *through* a rule it fails, where `AMBIGUOUS` is a list of names
            // refused by one they pass. Opposite senses, so they are two lists: under one
            // name, whoever comes next extends the wrong one.
            None if shape == Shape::Statement && call.func.name() == adql::functions::RAND => {
                Ok(())
            }
            None => match call.func.signature().volatility {
                Volatility::Immutable => Ok(()),
                Volatility::Stable | Volatility::Volatile => Err(format!(
                    "{}() does not answer the same way twice, so it cannot be used here",
                    call.func.name()
                )),
            },
        },

        // The two kinds that combine rows, and the one place the two shapes part. A field is
        // evaluated per row and a statement is planned whole, so what is smuggled into the
        // first is asked for outright by the second.
        Expr::AggregateFunction(_) | Expr::WindowFunction(_) | Expr::GroupingSet(_) => {
            match shape {
                Shape::Statement => Ok(()),
                Shape::Row => Err("this is an expression over one row; aggregates, window \
                               functions and grouping sets summarize many"
                    .to_owned()),
            }
        }
        Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::SetComparison(_)
        | Expr::OuterReferenceColumn(..) => match shape {
            Shape::Statement => Ok(()),
            Shape::Row => {
                Err("a subquery reads a second table, and a request names one".to_owned())
            }
        },
        Expr::ScalarVariable(..) | Expr::Placeholder(_) => {
            Err("a variable has no value here; write the value itself".to_owned())
        }
        // Nothing here can produce one: `sqlparser`'s expression parser has no wildcard,
        // so `*` is a parse error and `t.*` fails the end-of-input check —
        // `no_expression_is_a_wildcard` is what holds that true. The arm exists because
        // the match is exhaustive, which is the point of writing it that way.
        #[expect(deprecated, reason = "matched to keep the match exhaustive")]
        Expr::Wildcard { .. } => {
            Err("omit columns rather than writing *: absent means every column".to_owned())
        }
        // A statement's `SELECT *` is expanded by the planner into the columns it names, so
        // one reaching here is `UNNEST(…)` written out — which turns one row into many and
        // is not something either shape asks for yet.
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
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::lit;

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
        // Reproducibility decides how the scan is read back, and nothing about how an
        // identifier is parsed, so either value gives the same answer here.
        crate::engine::query::session_context(false).state()
    }

    /// The limits an operator who set none would get.
    fn limits() -> Limits {
        Limits::from(&LimitsConfig::default())
    }

    /// One way of writing a projection, as the test drives it: a string in, the output
    /// column names out.
    type Spelling = dyn Fn(&str) -> Result<Vec<String>, ApiError>;

    /// The list form, which is what a body sends.
    fn column_names(names: &[&str]) -> Result<Vec<String>, ApiError> {
        let names = names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>();
        Ok(columns(&state(), &schema(), &names, limits())?
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect())
    }

    /// The same names comma-separated, which is what a query string sends.
    fn names(list: &str) -> Result<Vec<String>, ApiError> {
        Ok(column_text(&state(), &schema(), list, limits())?
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect())
    }

    fn filter(text: &str) -> Result<String, ApiError> {
        Ok(filters(&state(), &schema(), text, limits())?.to_string())
    }

    fn filters_of_text(text: &str) -> Result<String, ApiError> {
        Ok(filter_text(&state(), &schema(), text, limits())?.to_string())
    }

    /// Names, in the file's own spelling of them.
    ///
    /// A name into a nested column comes back as that column — the row's light curve is one
    /// value, and asking for part of it is asking for less of that value rather than for a
    /// column beside it.
    #[test]
    fn columns_takes_names() {
        assert_eq!(
            column_names(&["objectid", "lightcurve.mag", "gmag"]).unwrap(),
            ["objectid", "lightcurve", "Gmag"]
        );
        // A name SQL will not take unquoted is written the way SQL writes one.
        assert_eq!(column_names(&["\"Gmag\""]).unwrap(), ["Gmag"]);
        // Whitespace around a name is nothing: the element is tokenized, and a tokenizer
        // skips it. A client that built the list by splitting a string keeps its spaces.
        assert_eq!(column_names(&["  objectid  "]).unwrap(), ["objectid"]);
        // The list is the separator, so an element holding two names is refused rather than
        // read as both: that spelling is the query string's, where there is nowhere else to
        // put the comma.
        assert!(column_names(&["objectid, objra"]).is_err());
        assert_eq!(names("objectid, objra").unwrap(), ["objectid", "objra"]);
    }

    /// Both wire forms of one field: a body writes the names as a list, a query string
    /// writes them separated by commas, and what comes back is the same columns.
    #[test]
    fn the_list_and_the_comma_separated_text_are_one_field() {
        for (list, text) in [
            (&["objectid", "gmag"][..], "objectid, gmag"),
            (
                &["lightcurve.mag", "lightcurve.mjd"],
                "lightcurve.mag, lightcurve.mjd",
            ),
            // Quoted in both, and the comma inside the quotes belongs to the name.
            (&["\"Gmag\""], "\"Gmag\""),
        ] {
            assert_eq!(column_names(list).unwrap(), names(text).unwrap(), "{text}");
        }
    }

    /// The pieces of a nested column come back as that column, and both wire forms agree
    /// about it.
    ///
    /// A row's light curve is one value. A caller who names two of its fields has asked for
    /// less of that value, so what comes back is one column carrying those two fields —
    /// handing back `mag` and `mjd` flat would make the reader above put the row together
    /// again, against a schema that no longer matches the file's.
    #[test]
    fn the_pieces_of_a_nested_column_come_back_as_that_column() {
        // The two spellings of one request: `columns` as a query string writes it, and
        // `columns` as a body writes it.
        // Split on the comma alone, so every element but the first arrives with a leading
        // space: an element is tokenized, and whitespace around a name is nothing to a
        // tokenizer.
        let as_a_list = |text: &str| column_names(&text.split(',').collect::<Vec<_>>());
        let spellings: [&Spelling; 2] = [&names, &as_a_list];
        for named in spellings {
            // Two fields of one column are one column, and it keeps its place in the list.
            assert_eq!(
                named("objectid, lightcurve.mag, objra, lightcurve.mjd").unwrap(),
                ["objectid", "lightcurve", "objra"]
            );
            // Naming it whole is every field, and naming it whole *and* in part is still
            // every field: the deeper name asks for part of what the shallower one already
            // returns, so neither is dropped and the union is the column.
            assert_eq!(named("lightcurve").unwrap(), ["lightcurve"]);
            assert_eq!(named("lightcurve.mag, lightcurve").unwrap(), ["lightcurve"]);
            assert_eq!(named("lightcurve, lightcurve.mag").unwrap(), ["lightcurve"]);
        }
    }

    /// The fields are the ones named, in the order they were named, and the column is a
    /// struct of exactly those.
    ///
    /// Checked on the planned expression rather than only on the output name, since the
    /// name alone would pass for a column that came back whole.
    #[test]
    fn a_packed_column_carries_only_the_fields_that_were_named() {
        let named = |names: &[&str]| {
            let names = names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>();
            columns(&state(), &schema(), &names, limits()).unwrap()
        };
        let shown = named(&["lightcurve.mag"])[0].to_string();
        assert!(shown.contains("mag"), "{shown}");
        assert!(!shown.contains("mjd"), "{shown}");

        // Both, in the caller's order rather than the file's.
        let shown = named(&["lightcurve.mjd", "lightcurve.mag"])[0].to_string();
        let mjd = shown.find("mjd").expect(&shown);
        let mag = shown.find("mag").expect(&shown);
        assert!(mjd < mag, "{shown}");

        // Whole is the column itself and not a struct rebuilt from its fields.
        let whole = &named(&["lightcurve"])[0];
        assert!(!whole.to_string().contains("named_struct"), "{whole}");
    }

    /// Names and nothing computed, and the refusal says where a computed column is written
    /// rather than leaving the caller to guess.
    #[test]
    fn columns_refuses_an_expression() {
        for name in ["objra - 0.1", "count(objectid)", "1"] {
            let error = column_names(&[name]).unwrap_err().to_string();
            assert!(error.contains("column names"), "{name}: {error}");
            assert!(error.contains("ADQL"), "{name}: {error}");
        }
        // An alias is refused by the grammar rather than by the check above: `columns`
        // parses one expression per name and `AS` is not part of one.
        assert!(column_names(&["objra AS ra"]).is_err());
    }

    /// The separators are the query string's, and only the query string's.
    ///
    /// `&&`, `,` and `;` exist because a url has one `filters=` and has to join two
    /// conditions inside it — `&` ends a parameter, so `AND` is what a caller can type and
    /// `&&` what a client encodes. A body writes `AND`, and accepting the separators there
    /// too would leave one meaning with two spellings for nothing.
    #[test]
    fn the_separators_are_the_query_strings_alone() {
        let expected = filter("objectid > 1 AND objra < 2").unwrap();
        assert_eq!(
            filters_of_text("objectid > 1 && objra < 2").unwrap(),
            expected
        );
        assert_eq!(
            filters_of_text("objectid > 1 AND objra < 2").unwrap(),
            expected
        );
        // In a body, `&&` is an operator this service does not run, and it is refused as one
        // rather than read as `AND`.
        assert!(filter("objectid > 1 && objra < 2").is_err());
        // A `;` is the end of one statement and the start of another, which is the one thing
        // this field is parsed so as never to accept.
        assert!(filter("objectid > 1;objra < 2").is_err());
    }

    /// The rewrite is on the tokens, so `&&` inside a string is data and stays data.
    /// On the text it would become the literal `'a AND b'`, and the caller would be
    /// comparing against something they never wrote.
    #[test]
    fn ampersands_inside_a_literal_are_not_an_operator() {
        let text = "cast(objectid AS VARCHAR) = 'a && b'";
        let planned = filters_of_text(text).unwrap();
        assert_eq!(planned, filter(text).unwrap());
        assert!(planned.contains("a && b"), "{planned}");
    }

    /// What `lsdb` sends: its predicate in disjunctive normal form, `,` joining a
    /// conjunction and `;` separating the alternatives.
    ///
    /// It attaches one to every partition it reads over `http(s)://`, so a spelling refused
    /// here is every read refused. SQL's own precedence is what groups the alternatives:
    /// `AND` binds tighter than `OR`.
    #[test]
    fn filters_spells_and_and_or_the_way_lsdb_sends_them() {
        let expected = filter("objectid >= 1 AND objectid < 2").unwrap();
        assert_eq!(filters_of_text("objectid>=1,objectid<2").unwrap(), expected);

        let expected =
            filter("objectid >= 1 AND objectid < 2 OR objectid >= 5 AND objectid < 6").unwrap();
        assert_eq!(
            filters_of_text("objectid>=1,objectid<2;objectid>=5,objectid<6").unwrap(),
            expected
        );
    }

    /// A comma inside parentheses belongs to what it is inside — an `IN` list, a function's
    /// arguments — and is not a separator between predicates. Rewriting it would ask a
    /// different question and get an answer nobody could tell from the right one.
    #[test]
    fn commas_inside_parentheses_are_not_separators() {
        for text in [
            "objectid IN (1, 2, 3)",
            "objra > 1 AND objectid IN (1, 2, 3)",
            "coalesce(objectid, 0) > 1",
        ] {
            assert_eq!(
                filters_of_text(text).unwrap(),
                filter(text).unwrap(),
                "{text}"
            );
        }
        // And the two together: a top-level comma joins, the ones inside the list do not.
        assert_eq!(
            filters_of_text("objectid IN (1, 2, 3),objra > 1").unwrap(),
            filter("objectid IN (1, 2, 3) AND objra > 1").unwrap()
        );
    }

    /// A column answers to its own name and to its name in lowercase. `Gmag` is what the
    /// file calls it and what a caller reads off the file; `gmag` is what SQL says an
    /// unquoted name means. Nested fields resolve the same way, a segment at a time.
    #[test]
    fn a_column_answers_to_its_name_and_to_its_lowercase() {
        for sql in ["Gmag < 20", "gmag < 20", "\"Gmag\" < 20"] {
            assert_eq!(filter(sql).unwrap(), "Gmag < Int64(20)", "{sql}");
        }
        assert_eq!(filter("objectid = 1").unwrap(), "objectid = Int64(1)");
        // The output key is the file's spelling, not the caller's.
        assert_eq!(names("gmag").unwrap(), ["Gmag"]);
        // Including the column a nested name is packed back into: written `LIGHTCURVE.MAG`
        // it would resolve to neither, and written in either accepted casing the answer is
        // the one the file spells.
        assert_eq!(names("lightcurve.mag").unwrap(), ["lightcurve"]);
    }

    /// Every other casing is refused rather than resolved: which names a column answers
    /// to must not depend on what else happens to be in the file.
    #[test]
    fn no_other_casing_resolves() {
        for sql in [
            // Neither the file's spelling nor all-lowercase.
            "GMAG < 20",
            "GMag < 20",
            "OBJECTID = 1",
            "ObjectId = 1",
            // Quoted is exact by definition.
            "\"GMAG\" < 20",
            "\"objectid\" = 1 AND \"Gmagg\" < 20",
        ] {
            assert!(filter(sql).is_err(), "{sql}");
        }
        assert!(names("LIGHTCURVE.MAG").is_err());
    }

    /// Two columns whose lowercase forms collide are each still reachable by writing
    /// them out; only the form they share names neither.
    #[test]
    fn a_shared_lowercase_form_names_neither_column() {
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("Flux", DataType::Float64, true),
            Field::new("FLUX", DataType::Float64, true),
        ]))
        .unwrap();
        assert!(filters(&state(), &schema, "Flux > 1", limits()).is_ok());
        assert!(filters(&state(), &schema, "FLUX > 1", limits()).is_ok());
        assert!(filters(&state(), &schema, "flux > 1", limits()).is_err());
    }

    /// A name matching nothing is left for DataFusion, whose message names the closest
    /// column it has — without the name it gave the table the caller never named.
    #[test]
    fn an_unresolvable_name_is_reported_as_written() {
        let error = filter("gmagg < 20").unwrap_err().to_string();
        assert!(error.contains("gmagg"), "{error}");
        assert!(!error.contains(UNNAMED_TABLE), "{error}");
    }

    /// `avg` summarizes rows, so `avg(lightcurve.mag)` averages a list column *across*
    /// rows rather than within one, whatever it looks like. Averaging inside a row's list
    /// would be a scalar function over the list, which is a different thing and is not
    /// what this spells.
    #[test]
    fn aggregating_a_list_column_is_still_aggregating_rows() {
        let error = filter("avg(lightcurve.mag) > 20").unwrap_err().to_string();
        assert!(error.contains("one row"), "{error}");
    }

    /// The wildcard arm in `allowed` is unreachable, and this is why: `sqlparser`'s
    /// expression parser has no `*` in it.
    #[test]
    fn no_expression_is_a_wildcard() {
        for sql in ["*", "t.*", "lightcurve.*"] {
            assert!(names(sql).is_err(), "{sql}");
        }
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
            assert!(error.contains("filters"), "{sql}: {error}");
        }
    }

    #[test]
    fn a_name_cannot_smuggle_a_from() {
        let error = names("objectid FROM other").unwrap_err().to_string();
        assert!(error.contains("columns"), "{error}");
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

    /// An unknown column is the caller's mistake, and is named back to them.
    #[test]
    fn an_unknown_column_is_a_bad_request() {
        let error = filter("nope = 1").unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert!(error.to_string().contains("nope"), "{error}");
    }

    /// Refused by the node kind DataFusion planned them into, not by their names — which
    /// is why no list of function names appears anywhere here.
    #[test]
    fn a_predicate_may_not_summarize_rows() {
        for sql in [
            "count(objectid) > 1",
            "sum(Gmag) > 1",
            "max(objra) > 1",
            "avg(Gmag) > 1",
        ] {
            let error = filter(sql).unwrap_err().to_string();
            assert!(error.contains("one row"), "{sql}: {error}");
        }
    }

    /// The arithmetic a caller needs to turn a column into a quantity: a magnitude into a
    /// flux, a parallax into a distance, degrees into radians.
    #[test]
    fn the_numeric_functions_are_callable() {
        for sql in [
            "sqrt(objra) > 1",
            "log10(Gmag) < 1",
            "log2(Gmag) < 1",
            "ln(Gmag) < 1",
            "power(10, -0.4 * Gmag) > 1e-6",
            "abs(objra - 180) < 1",
            "degrees(radians(objra)) > 1",
            "atan2(objra, Gmag) > 0",
            "round(Gmag) = 20",
            "isnan(Gmag)",
        ] {
            assert!(filter(sql).is_ok(), "{sql}: {:?}", filter(sql).err());
        }
    }

    /// One spelling, two answers a factor of 2.3 apart, and both of them numbers: which is
    /// why this one is refused by name where the rest are judged by volatility.
    #[test]
    fn the_ambiguous_logarithm_is_refused_rather_than_picked() {
        for sql in ["log(Gmag) < 1", "log(2, Gmag) < 1"] {
            let error = filter(sql).unwrap_err().to_string();
            assert!(error.contains("write log10"), "{sql}: {error}");
        }
    }

    /// The volatility rule, which until these functions were registered had nothing in the
    /// build to test it against.
    ///
    /// `random()` is exactly the case it is written for: an ordinary-looking scalar function
    /// that makes one request's answer differ from the next's for the same query, which is
    /// wrong to cache and wrong to reproduce from a plan.
    #[test]
    fn a_volatile_function_is_still_refused() {
        let error = filter("random() < 0.5").unwrap_err().to_string();
        assert!(error.contains("the same way twice"), "{error}");
    }

    /// The node cap counts terms, not rows and not depth: this list is two levels deep
    /// and arbitrarily wide. It is the operator's, so a smaller one refuses what the
    /// default allows.
    #[test]
    fn a_list_wider_than_the_node_cap_is_refused() {
        let list = |n: usize| (0..n).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
        let thousand = format!("objectid IN ({})", list(1000));
        assert!(filter(&thousand).is_ok());

        let narrow = Limits {
            max_nodes: 100,
            ..limits()
        };
        let error = filters(&state(), &schema(), &thousand, narrow)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("too large") && error.contains("100"),
            "{error}"
        );
    }

    /// Depth is the parser's, and also the operator's.
    #[test]
    fn nesting_deeper_than_the_depth_cap_is_refused() {
        let nested = |n: usize| format!("{}objectid = 1{}", "(".repeat(n), ")".repeat(n));
        assert!(filter(&nested(10)).is_ok());
        assert!(filter(&nested(limits().max_depth + 10)).is_err());

        let shallow = Limits {
            max_depth: 3,
            ..limits()
        };
        assert!(filters(&state(), &schema(), &nested(10), shallow).is_err());
    }

    /// `get_field` is what a dotted path plans to, so the volatility rule has to let it
    /// through — a rule that refused it would refuse every nested column.
    #[test]
    fn the_nested_field_access_is_allowed() {
        let expr = filters(&state(), &schema(), "lightcurve.mag IS NOT NULL", limits()).unwrap();
        assert!(expr.to_string().contains("lightcurve"), "{expr}");
        assert!(allowed(&expr, Shape::Row).is_ok());
    }

    /// The depth is what the shape is for, and it is logarithmic rather than merely smaller:
    /// a constant factor off a left-deep fold would still overflow, further along.
    #[test]
    fn a_union_of_many_terms_is_logarithmically_deep() {
        fn depth(expr: &Expr) -> usize {
            match expr {
                Expr::BinaryExpr(binary) => 1 + depth(&binary.left).max(depth(&binary.right)),
                _ => 1,
            }
        }
        // A literal, so that every level the walk counts is one this function put there.
        let term = |i: i32| lit(i);

        assert_eq!(any_of(std::iter::empty()), None);
        assert_eq!(any_of([term(1)]), Some(term(1)));
        assert_eq!(depth(&any_of((0..2).map(term)).unwrap()), 2);
        for (terms, levels) in [(4, 3), (16, 5), (1024, 11), (10_000, 15)] {
            assert_eq!(
                depth(&any_of((0..terms).map(term)).unwrap()),
                levels,
                "{terms} terms"
            );
        }
    }

    /// An odd term has nowhere to pair and is carried up untouched, which is the step that
    /// would otherwise drop it or double it.
    #[test]
    fn an_odd_number_of_terms_keeps_every_one_of_them() {
        fn leaves(expr: &Expr, into: &mut Vec<String>) {
            match expr {
                Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                    leaves(&binary.left, into);
                    leaves(&binary.right, into);
                }
                other => into.push(other.to_string()),
            }
        }
        for terms in [3_usize, 5, 7, 9, 11, 101] {
            let union = any_of((0..terms).map(|i| lit(i as u64))).unwrap();
            let mut found = Vec::new();
            leaves(&union, &mut found);
            assert_eq!(found.len(), terms, "{terms} terms");
            found.sort_unstable();
            found.dedup();
            assert_eq!(found.len(), terms, "{terms} terms, after dedup");
        }
    }
}
