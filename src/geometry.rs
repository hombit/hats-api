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
//! **A region is a constant.** The covering is computed once, at plan time, from the
//! literals the caller wrote; a circle whose radius came out of a column would need a
//! different covering per row, which is not a thing a scan can be pruned by. So the
//! constructors answer with the region's own JSON — the same text the `region` field takes —
//! and a call over anything but constants is refused.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::region::{self, Region, Spatial};

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
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DfResult<ExprSimplifyResult> {
        let [point, shape] = args.as_slice() else {
            return Err(shape_of_a_region_test());
        };
        let (ra_column, dec_column) = coordinates(point)?;
        let regions = [region_of(shape)?];
        let spatial = Spatial {
            regions: &regions,
            ra_column: Some(&ra_column),
            dec_column: Some(&dec_column),
            // Discovered from the file's own schema, which is the rule everywhere else: a
            // column named `_healpix_29` carries its order in its name, and any other index
            // column has to be named — which a function call has no way to do.
            healpix: None,
            // A function says nothing about a catalog above the file, so the covering gets
            // no partition to put a floor under its depth or to be cut to.
            partition: None,
        };
        let schema: &DFSchema = info.schema();
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
fn coordinates(point: &Expr) -> DfResult<(String, String)> {
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
fn column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => column(&cast.expr),
        Expr::TryCast(cast) => column(&cast.expr),
        Expr::Alias(alias) => column(&alias.expr),
        _ => None,
    }
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

/// What a region test looks like, said once because every way of getting it wrong ends here.
fn shape_of_a_region_test() -> DataFusionError {
    DataFusionError::Plan(
        "a region test is contains(point(ra, dec), circle(45.0, -20.0, 0.1)), with a region \
         built by circle(...) or moc(...)"
            .to_owned(),
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

    /// The region has to be the same for every row, since the covering is computed once.
    #[test]
    fn a_region_built_from_a_column_is_refused() {
        let error = planned("contains(point(ra, dec), circle(ra, dec, 0.1))", true).unwrap_err();
        assert!(error.contains("written out"), "{error}");
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
