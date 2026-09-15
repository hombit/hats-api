//! The functions ADQL requires that DataFusion has not got.
//!
//! One so far. [`crate::sky::geometry`] is the other half of what an ADQL context registers, and
//! is a module of its own because a region is a thing this service already has its own
//! meaning for: those functions lower into the predicate `sky::region` builds, and could serve
//! a route that never mentions ADQL. These are here because the standard asks for them.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DfResult;
use datafusion::functions::math;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// ADQL's name for the random function, which is what a caller writes and what
/// [`crate::engine::sql`] lets through the volatility rule.
pub const RAND: &str = "rand";

/// Put ADQL's own functions on a context.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(Rand::default()));
}

/// `RAND()`, which ADQL 2.1 makes mandatory: a random value between 0 and 1.
///
/// **The argument is accepted and ignored, which is what the standard says it means.** ADQL
/// describes it as "originally intended to provide a random seed" and then states that it
/// "has undefined semantics", advising query writers to omit it. So a statement carrying one
/// runs — refusing it would refuse a spelling the standard defines — and nothing pretends it
/// seeds anything.
///
/// **Nothing here is reproducible, and the standard requires nothing.** Leaving the seed
/// undefined is the specification declining to promise that two runs agree, so this service
/// promises no more than that either. It is the first answer here that differs between two
/// identical requests, which is worth knowing when reading the rest of this crate: everything
/// else is written the other way.
///
/// It delegates rather than generating anything. DataFusion's `random()` is the same value
/// from the same generator, and a second implementation would be a second thing to be right
/// about — this one exists for the name, the arity and the volatility, not for the numbers.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Rand {
    signature: Signature,
}

impl Default for Rand {
    fn default() -> Self {
        Self {
            // Registered under `rand` rather than aliased onto `random`, which is what
            // DataFusion ships. Two reasons, and the first is the one that matters: the
            // volatility exception is written against a name, so sharing one with `random`
            // would let `random()` through a rule it is refused by everywhere else. The
            // second is the argument, which `random`'s nullary signature has no room for.
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::Numeric(1)],
                // A zero-argument immutable function is constant-folded — computed once and
                // glued onto every row — so this is what makes it a random column rather than
                // a random constant.
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for Rand {
    fn name(&self) -> &str {
        RAND
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        math::random().invoke_with_args(ScalarFunctionArgs {
            // The caller's argument is dropped here rather than at the boundary: it is
            // syntactically fine and semantically nothing, which is exactly what it is.
            args: Vec::new(),
            arg_fields: Vec::new(),
            number_rows: args.number_rows,
            return_field: Arc::new(Field::new(RAND, DataType::Float64, false)),
            config_options: args.config_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::Float64Type;

    use super::*;
    use crate::engine::query::session_context;

    /// Ten rows, so a column of one repeated value is visible as one.
    async fn values(query: &str) -> Vec<f64> {
        let ctx = session_context(false);
        register(&ctx);
        let rows = RecordBatch::try_from_iter([(
            "x",
            Arc::new(Float64Array::from_iter_values((0..10).map(f64::from))) as ArrayRef,
        )])
        .unwrap();
        ctx.register_batch("t", rows).unwrap();
        let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Float64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// **The failure the volatility is there to prevent.** An immutable zero-argument function
    /// is constant-folded, so the whole column would be one number — a random constant rather
    /// than a random column, and one a caller could not tell from a very unlucky draw.
    #[tokio::test]
    async fn rand_is_a_column_of_values_rather_than_one_value() {
        let values = values("SELECT rand() FROM t").await;
        assert_eq!(values.len(), 10);
        assert!(
            values.windows(2).any(|pair| pair[0] != pair[1]),
            "every row got the same number: {values:?}"
        );
        assert!(
            values.iter().all(|value| (0.0..=1.0).contains(value)),
            "outside the range the standard gives: {values:?}"
        );
    }

    /// The argument the standard defines and then declines to give a meaning. Accepted, so a
    /// statement that writes one runs; ignored, so nothing claims it seeded anything.
    #[tokio::test]
    async fn rand_takes_the_argument_the_standard_leaves_undefined() {
        let values = values("SELECT rand(42) FROM t").await;
        assert_eq!(values.len(), 10);
        assert!(values.windows(2).any(|pair| pair[0] != pair[1]));
    }

    /// ADQL's own spelling, which DataFusion lowercases before it looks a function up — so the
    /// name this registers is reached without the translation touching it.
    #[tokio::test]
    async fn rand_is_reached_by_adqls_spelling() {
        assert_eq!(values("SELECT RAND() FROM t").await.len(), 10);
    }
}
