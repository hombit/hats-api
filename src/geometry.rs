//! The region test as a function a query can call.
//!
//! `region.rs` decides what a shape means and `healpix.rs` what cells it covers; this is the
//! same two things reached from inside a query rather than from a field beside it —
//! `contains(point(ra, dec), circle(45.0, -20.0, 0.1))`, which is what ADQL's own region
//! test comes to once its comparison with 1 is taken off. [`register`] says which contexts
//! get them.
//!
//! **The functions are DataFusion's, and the rewrite is the whole design.** `contains`
//! declares itself and then, during the optimizer's simplify pass, replaces its own call
//! with the expression [`region::predicate`] builds — the HEALPix range test, the geometry,
//! and the `OR` between them, exactly as a request carrying a `region` field produces it. So
//! there is one account of what a circle on the sky means, and a query that says it as a
//! function prunes row groups the same way a query that says it as a field does.
//!
//! That rewrite has to be `simplify` rather than `ScalarUDFImpl::preimage`, which is the
//! other hook a function has for making itself prunable. `preimage` answers with *one
//! contiguous* interval, and a covering is tens of disjoint ranges — so it can say what a
//! monotone function inverts to and cannot say what a cone covers.
//!
//! **A region that prunes is a constant.** The covering is computed once, at plan time, from
//! the literals the caller wrote; a circle whose centre came out of a column is a different
//! circle per row, and a scan cannot be pruned by a different shape per row. So the
//! constructors answer with the region's own JSON — the same text the `region` field takes —
//! and only a circle built out of numbers reaches [`region::predicate`].
//!
//! A circle around a column is the other case and is answered rather than refused: it is a
//! crossmatch, `contains(point(b.ra, b.dec), circle(a.ra, a.dec, r))`, and what it becomes is
//! the separation and the bound with nothing in front of them. Each side's own region is what
//! chooses the partitions; this only says which of the pairs match.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::functions::math::expr_fn::{asin, degrees, sqrt};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::{SessionContext, lit, when};

use crate::region::{self, Region, Spatial};

/// The metadata key a table marks its own coordinate columns with, and the two values it takes.
///
/// A file says nothing about which of its columns are a position, so the caller naming them is
/// the only claim there is; a catalog does say, and [`crate::hats_table`] writes what it says
/// into the schema the planner sees. `declared_position` below is what reads it back.
pub const COORDINATE: &str = "hats.coordinate";
pub const RA: &str = "ra";
pub const DEC: &str = "dec";

/// What a position is, as a type: two fields and no values.
///
/// [`Point`] never evaluates — every use of one is inside a call that rewrites itself before
/// anything runs — so this exists to be type-checked against and for nothing else. A struct
/// rather than, say, a pair of floats because a function returns one value.
fn position() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("ra", DataType::Float64, true),
        Field::new("dec", DataType::Float64, true),
    ]))
}

/// Put the region functions on a context.
///
/// **Only a context that can prune by them.** On a single file the rewrite is the whole
/// saving, since the covering is what row-group statistics skip on. A catalog is chosen
/// partition by partition before any file is opened, and that choice is made from a
/// `region` field — a `contains` in the `where` of a catalog route would prune inside each
/// partition and still open every one of them. So these are not on the context every route
/// shares; a query that can say a region as a function is one whose planner also decides what
/// to scan, and that is the context which registers them.
pub fn register(ctx: &SessionContext) {
    for function in [
        ScalarUDF::new_from_impl(Point::default()),
        ScalarUDF::new_from_impl(Circle::default()),
        ScalarUDF::new_from_impl(Moc::default()),
        ScalarUDF::new_from_impl(Contains::default()),
        ScalarUDF::new_from_impl(Distance::default()),
    ] {
        ctx.register_udf(function);
    }
}

/// `point(ra, dec)` — the two columns a row's position is in.
///
/// It has a type and no value. A position is only ever an argument to a region test, which
/// is rewritten into a test over those two columns before the plan runs, so there is nothing
/// for this to compute; a call that survives to execution is one this service would not have
/// been able to answer anyway, and says so rather than producing something.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Point {
    signature: Signature,
}

impl Default for Point {
    fn default() -> Self {
        Self {
            // Two numbers, whatever width the file wrote them at. Coercion puts a cast
            // around a `Float32` column, which `Contains` looks through — the arithmetic is
            // widened to `f64` in `region.rs` regardless.
            signature: Signature::uniform(2, vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Point {
    fn name(&self) -> &str {
        "point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(position())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        Err(DataFusionError::Plan(
            "point(ra, dec) is a position to test a region against, not a value; write it \
             inside contains(...)"
                .to_owned(),
        ))
    }
}

/// `circle(ra, dec, radius)` — a cone, in degrees, the same one the `region` field spells
/// `{"type": "circle", …}`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Circle {
    signature: Signature,
}

impl Default for Circle {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(3, vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Circle {
    fn name(&self) -> &str {
        "circle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let [ra, dec, radius] = numbers(&args, "circle")?;
        encoded(&Region::Circle {
            ra,
            dec,
            radius_deg: Some(radius),
            radius_arcsec: None,
        })
    }
}

/// `moc('4/30-33 38 52')` — the IVOA ASCII serialization, as the `region` field takes it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Moc {
    signature: Signature,
}

impl Default for Moc {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Moc {
    fn name(&self) -> &str {
        "moc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let [ColumnarValue::Scalar(ScalarValue::Utf8(Some(ascii)))] = args.args.as_slice() else {
            return Err(constant("moc"));
        };
        encoded(&Region::Moc {
            ascii: Some(ascii.clone()),
            json: None,
        })
    }
}

/// `contains(point(ra, dec), region)` — whether a row's position is inside the shape.
///
/// **Boolean, where ADQL's own `CONTAINS` answers 1 or 0.** ADQL has no boolean type, so it
/// writes `1 = CONTAINS(…)`; SQL has one and a predicate is what a filter takes. Translating
/// the comparison belongs to whatever reads ADQL, being a fact about that language rather
/// than about this test.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Contains {
    signature: Signature,
}

impl Default for Contains {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![position(), DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Contains {
    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    /// **This is where the work happens.** The call becomes the predicate `region.rs` builds
    /// for the same shape, against the same schema, with the same covering in front of the
    /// same geometry.
    ///
    /// It either rewrites or fails: falling through would leave a call for the executor to
    /// make, and what it would evaluate is a haversine over every row of every partition —
    /// the right answer at a cost the caller cannot tell from a slow link, which is the thing
    /// the whole region machinery exists to avoid.
    ///
    /// **A circle whose centre is a row's own position is the other case**, and it is a
    /// crossmatch: `contains(point(b.ra, b.dec), circle(a.ra, a.dec, r))` is every pair of
    /// rows within `r` of each other. There is no covering to be had — the shape is a
    /// different one for every row of `a` — so what it becomes is the separation and the
    /// bound, and nothing prunes. Narrow both sides first; each one's own region is what
    /// chooses the partitions, and this only says which of the surviving pairs match.
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DfResult<ExprSimplifyResult> {
        let [point, shape] = args.as_slice() else {
            return Err(shape_of_a_region_test());
        };
        if let Some([ra, dec, radius]) = per_row_circle(shape) {
            let (row_ra, row_dec) = position_of(point)?;
            return Ok(ExprSimplifyResult::Simplified(region::within(
                &row_ra, &row_dec, ra, dec, radius,
            )));
        }
        let (ra_column, dec_column) = coordinates(point)?;
        let schema: &DFSchema = info.schema();
        declared_position(schema, &ra_column, &dec_column)?;
        let regions = [region_of(shape)?];
        let spatial = Spatial {
            regions: &regions,
            ra_column: Some(&ra_column.name),
            dec_column: Some(&dec_column.name),
            // Discovered from the file's own schema, which is the rule everywhere else: a
            // column named `_healpix_29` carries its order in its name, and any other index
            // column has to be named — which a function call has no way to do.
            healpix: None,
            // A function says nothing about a catalog above the file, so the covering gets
            // no partition to put a floor under its depth or to be cut to.
            partition: None,
            // The table the caller's own `point(...)` named. In a join both sides have an
            // `ra` and a `_healpix_29`, so this is what keeps the predicate about one of
            // them.
            relation: ra_column.relation.as_ref(),
        };
        region::predicate(schema, &spatial)
            .map(ExprSimplifyResult::Simplified)
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // `simplify` answers with the predicate or with an error, so nothing reaches here.
        Err(DataFusionError::Internal(
            "contains(...) was not rewritten before the plan ran".to_owned(),
        ))
    }
}

/// The two columns a `point(...)` names.
///
/// A cast around either is looked through. Type coercion puts one over a `Float32` column
/// before this runs, and the arithmetic below widens to `f64` anyway — so the cast says
/// nothing this needs, while the column underneath is the whole point: the covering has to
/// be a test on a column for row-group statistics to prune on it.
fn coordinates(point: &Expr) -> DfResult<(Column, Column)> {
    let Expr::ScalarFunction(call) = point else {
        return Err(shape_of_a_region_test());
    };
    if call.func.name() != "point" {
        return Err(shape_of_a_region_test());
    }
    let [ra, dec] = call.args.as_slice() else {
        return Err(shape_of_a_region_test());
    };
    match (column(ra), column(dec)) {
        (Some(ra), Some(dec)) => Ok((ra, dec)),
        _ => Err(DataFusionError::Plan(
            "point(ra, dec) names the two columns a position is in; they are columns of the \
             file, not expressions over them"
                .to_owned(),
        )),
    }
}

/// The column an expression is, under whatever the planner wrapped it in.
fn column(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(column) => Some(column.clone()),
        Expr::Cast(cast) => column(&cast.expr),
        Expr::TryCast(cast) => column(&cast.expr),
        Expr::Alias(alias) => column(&alias.expr),
        _ => None,
    }
}

/// Refuse a region over a table's columns other than the ones it says hold a position.
///
/// **A catalog's partitions are chosen by its HEALPix index, and that index describes one pair
/// of columns.** The covering this rewrite produces is a test on that index, so a cone written
/// over some other pair would be answered from partitions chosen by a column that says nothing
/// about it — and the partitions dropped could be exactly the ones holding the positions asked
/// for. Fewer rows than the shape contains, with nothing in the answer to say so.
///
/// Swapping the two is the way it actually happens: `point(dec, ra)` is a cone somewhere else
/// entirely, and over a small catalog it comes back empty rather than wrong.
///
/// A table that marks nothing constrains nothing. That is every parquet file, where which
/// columns hold a position is the caller's to say and this is the saying of it.
fn declared_position(schema: &DFSchema, ra: &Column, dec: &Column) -> DfResult<()> {
    // A column the planner left unqualified is looked up by name, so that a statement over one
    // table — where there is no qualifier to write — is checked like any other.
    let table = match &ra.relation {
        Some(relation) => Some(relation.clone()),
        None => schema
            .iter()
            .find(|(_, field)| field.name() == &ra.name)
            .and_then(|(relation, _)| relation.cloned()),
    };
    let role = |want: &str| {
        schema.iter().find_map(|(relation, field)| {
            (relation == table.as_ref()
                && field.metadata().get(COORDINATE).map(String::as_str) == Some(want))
            .then(|| field.name().clone())
        })
    };
    let (Some(its_ra), Some(its_dec)) = (role(RA), role(DEC)) else {
        return Ok(());
    };
    if ra.name == its_ra && dec.name == its_dec {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "this catalog's positions are in {its_ra} and {its_dec}, and its partitions are chosen \
         by an index over those two; a region over ({}, {}) cannot be answered from them",
        ra.name, dec.name
    )))
}

/// The shape an expression is, where it is a constant one.
///
/// Two forms reach here and both are ordinary. A call over literals is usually folded to its
/// string before this runs, since the constructors are immutable; whether it has been is the
/// optimizer's business and not something to depend on, so the unfolded call is read too.
fn region_of(shape: &Expr) -> DfResult<Region> {
    let json = match shape {
        Expr::Literal(ScalarValue::Utf8(Some(json)), _) => json.clone(),
        Expr::ScalarFunction(call) => {
            let arguments = call
                .args
                .iter()
                .map(literal)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| constant(call.func.name()))?;
            let folded = call
                .func
                .invoke_with_args(ScalarFunctionArgs {
                    args: arguments,
                    arg_fields: Vec::new(),
                    number_rows: 1,
                    return_field: Arc::new(Field::new("region", DataType::Utf8, false)),
                    config_options: Arc::new(Default::default()),
                })
                .map_err(|_| constant(call.func.name()))?;
            match folded {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))) => json,
                _ => return Err(constant(call.func.name())),
            }
        }
        _ => return Err(shape_of_a_region_test()),
    };
    serde_json::from_str(&json).map_err(|_| shape_of_a_region_test())
}

/// One argument of a constructor, where it is written out rather than read from a row.
fn literal(expr: &Expr) -> Option<ColumnarValue> {
    match expr {
        Expr::Literal(value, _) => Some(ColumnarValue::Scalar(value.clone())),
        _ => None,
    }
}

/// A constructor's arguments as the three numbers it takes.
fn numbers<const N: usize>(args: &ScalarFunctionArgs, name: &str) -> DfResult<[f64; N]> {
    let mut numbers = [0.0; N];
    if args.args.len() != N {
        return Err(constant(name));
    }
    for (slot, argument) in numbers.iter_mut().zip(&args.args) {
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) = argument else {
            return Err(constant(name));
        };
        *slot = *value;
    }
    Ok(numbers)
}

/// A region as the text the `region` field takes, which is what the constructors answer with
/// and what [`region_of`] reads back.
fn encoded(region: &Region) -> DfResult<ColumnarValue> {
    let json = serde_json::to_string(region).map_err(|error| {
        DataFusionError::Internal(format!("a region did not serialize: {error}"))
    })?;
    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))))
}

/// A shape built from something that is not the same for every row.
fn constant(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{name}(...) takes numbers written out: a region is the same for every row, and one \
         built from a column would need its own covering per row"
    ))
}

/// `distance(point(ra, dec), point(ra, dec))` — the angle between two positions, in degrees.
///
/// **A value, where `contains` is a test, and that is the whole difference between them.** A
/// region test is answered from one shape known at plan time, so it carries a covering and
/// prunes; a separation between two positions that are both a row's is known only once the
/// two rows are in front of each other, so there is nothing to prune with and it is
/// arithmetic like any other. That is what makes it the thing a join can be written on.
///
/// It rewrites itself into `region::separation` for the same reason `contains` does: one
/// account of what the angle between two points is, so a crossmatch and a cone agree about
/// which pairs are a degree apart.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Distance {
    signature: Signature,
}

impl Default for Distance {
    fn default() -> Self {
        Self {
            // Both of ADQL's spellings: two positions, and the four coordinates 2.1 added.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![position(), position()]),
                    TypeSignature::Uniform(4, vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Distance {
    fn name(&self) -> &str {
        "distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// The haversine, turned back into an angle.
    ///
    /// `contains` compares haversines and never pays this `asin`, the comparison being
    /// monotone either way. A value has to be the angle itself, so here it is paid — once per
    /// pair of rows, which is what asking for a distance costs.
    ///
    /// **The clamp is not decoration.** The haversine of a pair a whole turn apart is 1, and
    /// rounding can put the sum a bit over it, where `asin` is `NaN`. A separation reported as
    /// `NaN` instead of 180 is a value the caller cannot tell from a null coordinate.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> DfResult<ExprSimplifyResult> {
        let [ra_a, dec_a, ra_b, dec_b] = match args.as_slice() {
            [first, second] => {
                let (ra_a, dec_a) = position_of(first)?;
                let (ra_b, dec_b) = position_of(second)?;
                [ra_a, dec_a, ra_b, dec_b]
            }
            [ra_a, dec_a, ra_b, dec_b] => {
                [ra_a.clone(), dec_a.clone(), ra_b.clone(), dec_b.clone()]
            }
            _ => return Err(shape_of_a_separation()),
        };
        let haversine = region::separation(&ra_a, &dec_a, &ra_b, &dec_b);
        let bounded = when(haversine.clone().gt(lit(1.0)), lit(1.0)).otherwise(haversine)?;
        let radians = lit(2.0) * asin(sqrt(bounded));
        Ok(ExprSimplifyResult::Simplified(degrees(radians)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // `simplify` answers with the arithmetic or with an error, so nothing reaches here.
        Err(DataFusionError::Internal(
            "distance(...) was not rewritten before the plan ran".to_owned(),
        ))
    }
}

/// A `circle(...)` still standing as a call, which is a circle this plan has no one value for.
///
/// A circle over literals is folded to its encoded region before this runs — constant folding
/// and this rewrite are the same pass, and it works inwards out — so a call that is still a
/// call is one that took an argument from a column. That is the whole test, and it looks at no
/// argument to make it: what makes a shape prunable is being one shape for the whole scan, not
/// being written out of numbers.
fn per_row_circle(shape: &Expr) -> Option<&[Expr; 3]> {
    let Expr::ScalarFunction(call) = shape else {
        return None;
    };
    (call.func.name() == "circle").then(|| call.args.as_slice().try_into().ok())?
}

/// The two expressions a `point(...)` holds.
///
/// Anything numeric, unlike [`coordinates`]: a separation is arithmetic over whatever it is
/// given, and one of its two positions being a pair of literals is an ordinary way to ask how
/// far each row is from somewhere.
fn position_of(point: &Expr) -> DfResult<(Expr, Expr)> {
    let Expr::ScalarFunction(call) = point else {
        return Err(shape_of_a_separation());
    };
    if call.func.name() != "point" {
        return Err(shape_of_a_separation());
    }
    let [ra, dec] = call.args.as_slice() else {
        return Err(shape_of_a_separation());
    };
    Ok((ra.clone(), dec.clone()))
}

/// What a region test looks like, said once because every way of getting it wrong ends here.
fn shape_of_a_region_test() -> DataFusionError {
    DataFusionError::Plan(
        "a region test is contains(point(ra, dec), circle(45.0, -20.0, 0.1)), with a region \
         built by circle(...) or moc(...)"
            .to_owned(),
    )
}

/// What a separation looks like, for the same reason.
fn shape_of_a_separation() -> DataFusionError {
    DataFusionError::Plan(
        "a separation is distance(point(ra, dec), point(ra, dec)), in degrees".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
    use datafusion::logical_expr::UNNAMED_TABLE;
    use datafusion::prelude::DataFrame;

    use super::*;
    use crate::query::session_context;
    use crate::sql;

    /// One catalog-shaped row: a position, a HEALPix index the covering can prune on, and
    /// something to select.
    fn batch(index: bool) -> RecordBatch {
        let mut columns: Vec<(&str, ArrayRef)> = vec![
            ("objectid", Arc::new(Int64Array::from(vec![1]))),
            ("ra", Arc::new(Float64Array::from(vec![45.0]))),
            ("dec", Arc::new(Float64Array::from(vec![-20.0]))),
        ];
        if index {
            columns.push(("_healpix_29", Arc::new(Int64Array::from(vec![0]))));
        }
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn limits() -> sql::Limits {
        sql::Limits {
            max_depth: 50,
            max_nodes: 10_000,
        }
    }

    /// A table to plan against, on a context configured the way a request's is.
    fn frame(index: bool) -> (SessionContext, DataFrame) {
        let ctx = session_context(false);
        register(&ctx);
        let df = ctx.read_batch(batch(index)).unwrap();
        (ctx, df)
    }

    /// A filtered frame's plan after the optimizer has run, which is where `simplify` is
    /// called. Planning alone leaves the call as written, so a test that stopped there would
    /// check the spelling and not the rewrite.
    ///
    /// The table's name is taken out of the text. `DataFrame::filter` qualifies the columns of
    /// an expression it is handed, and the expression `simplify` produces arrives after that
    /// pass and stays unqualified — the same columns of the one table either way, and a
    /// difference in spelling that says nothing about what is pruned.
    fn optimized(df: DataFrame) -> Result<String, String> {
        df.into_optimized_plan()
            .map(|plan| {
                plan.display_indent()
                    .to_string()
                    .replace(&format!("{UNNAMED_TABLE}."), "")
            })
            .map_err(|error| error.to_string())
    }

    /// One `where`, planned and optimized the way a request's is.
    fn planned(sql: &str, index: bool) -> Result<String, String> {
        let (ctx, df) = frame(index);
        let expr = sql::predicate(&ctx.state(), df.schema(), sql, limits())
            .map_err(|error| error.to_string())?;
        optimized(df.filter(expr).map_err(|error| error.to_string())?)
    }

    /// The same region as the `region` field spells it, through the same optimizer.
    fn field(regions: &[Region], index: bool) -> String {
        let (_ctx, df) = frame(index);
        let spatial = Spatial {
            regions,
            ra_column: Some("ra"),
            dec_column: Some("dec"),
            healpix: None,
            partition: None,
            relation: None,
        };
        let expr = region::predicate(df.schema(), &spatial).expect("the field form should plan");
        optimized(df.filter(expr).unwrap()).expect("the field form should optimize")
    }

    /// **The claim the whole module rests on.** The function is not a second implementation
    /// of the region test that happens to agree; it becomes the first one. Asserted on the
    /// expression rather than on the rows, because what is at stake is the covering in front
    /// of the geometry — two expressions can return the same rows and read a different number
    /// of row groups, and that difference is the reason any of this exists.
    #[test]
    fn the_function_becomes_the_expression_the_field_builds() {
        let circle = Region::Circle {
            ra: 45.0,
            dec: -20.0,
            radius_deg: Some(0.1),
            radius_arcsec: None,
        };
        for index in [true, false] {
            assert_eq!(
                planned("contains(point(ra, dec), circle(45.0, -20.0, 0.1))", index).unwrap(),
                field(std::slice::from_ref(&circle), index),
                "with an index column: {index}"
            );
        }
    }

    /// A MOC is cells and needs no coordinates, but `point` still names them — which is what
    /// makes one spelling cover every shape.
    #[test]
    fn a_moc_becomes_the_expression_the_field_builds() {
        let moc = Region::Moc {
            ascii: Some("4/30-33 38 52".to_owned()),
            json: None,
        };
        assert_eq!(
            planned("contains(point(ra, dec), moc('4/30-33 38 52'))", true).unwrap(),
            field(std::slice::from_ref(&moc), true)
        );
    }

    /// The covering is what prunes, so a file with no index column has to come back as the
    /// geometry alone rather than as an error — the same answer, read more slowly.
    #[test]
    fn a_file_with_no_index_is_answered_by_the_geometry() {
        let planned = planned("contains(point(ra, dec), circle(45.0, -20.0, 0.1))", false)
            .expect("a file without an index column still answers");
        assert!(!planned.contains("_healpix_29"), "{planned}");
    }

    /// A `moc` has no coordinate test at all, so the covering is the whole of its answer and
    /// a file with nothing to test cells against is refused rather than answered with no rows.
    #[test]
    fn a_moc_needs_an_index_column() {
        let error = planned("contains(point(ra, dec), moc('4/30'))", false).unwrap_err();
        assert!(error.contains("HEALPix column"), "{error}");
    }

    /// A circle centred on a column is a different circle for every row, so it is answered as
    /// the separation between the two positions and carries no covering — which is what makes
    /// it a crossmatch rather than a region test.
    #[test]
    fn a_circle_around_a_column_is_a_pair_test() {
        let planned = planned("contains(point(ra, dec), circle(ra, dec, 0.1))", true)
            .expect("a circle around a column is a separation");
        let filter = planned
            .lines()
            .find(|line| line.trim_start().starts_with("Filter:"))
            .unwrap_or_default();
        // The file has an index column and the plan does not touch it: there is no covering
        // to be had, the shape being a different one for every row.
        assert!(!filter.contains("_healpix_29"), "{planned}");
    }

    /// A position is two of the file's columns. An expression over them cannot be pruned on,
    /// which is the whole reason the covering exists.
    #[test]
    fn a_position_must_be_columns() {
        let error = planned(
            "contains(point(ra + 1, dec), circle(45.0, -20.0, 0.1))",
            true,
        )
        .unwrap_err();
        assert!(error.contains("not expressions"), "{error}");
    }

    /// Nothing here returns a shape, and a caller who writes one outside a region test is
    /// told so rather than handed a value.
    #[tokio::test]
    async fn a_position_is_not_a_value() {
        let (ctx, df) = frame(true);
        let exprs = sql::projection(&ctx.state(), df.schema(), "point(ra, dec)", limits()).unwrap();
        let error = df.select(exprs).unwrap().collect().await.unwrap_err();
        assert!(error.to_string().contains("not a value"), "{error}");
    }

    /// The constructors evaluate to the region's own text, which is what the `region` field
    /// takes — so the two ways of saying a circle are the same document.
    #[tokio::test]
    async fn a_constructor_is_the_region_field_written_out() {
        let (ctx, df) = frame(true);
        let exprs = sql::projection(
            &ctx.state(),
            df.schema(),
            "circle(45.0, -20.0, 0.1)",
            limits(),
        )
        .expect("a constructor is an ordinary expression");
        let batches = df.select(exprs).unwrap().collect().await.unwrap();
        let column = batches[0].column(0).as_string::<i32>();
        assert_eq!(column.len(), 1);
        let region: Region =
            serde_json::from_str(column.value(0)).expect("the region field's own text");
        assert_eq!(
            region,
            Region::Circle {
                ra: 45.0,
                dec: -20.0,
                radius_deg: Some(0.1),
                radius_arcsec: None,
            }
        );
    }
}
