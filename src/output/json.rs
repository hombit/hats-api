//! Writing the result back out as JSON: an array of row objects, nested columns included.

use std::fmt;
use std::io::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, PrimitiveArray, RecordBatch};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Float16Type, Float32Type, Float64Type, SchemaRef,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::json::writer::{
    Encoder, EncoderFactory, EncoderOptions, JsonArray, NullableEncoder, Writer,
};

use crate::engine::query::QueryResult;
use crate::error::ApiError;
use crate::output::stream;

/// Serialize the result as a JSON array of row objects, nested columns included.
///
/// Two things here are not the arrow writer's defaults, and both are about a value a
/// caller cannot otherwise tell apart from another one:
///
/// - **A null is written.** The default omits the key, so a row's shape depends on its
///   own values: a caller reading the keys of one row learns what that row happened to
///   have, and a null is indistinguishable from a column the projection never asked for.
/// - **A float that is not a number is written as a string.** The default writes `null`
///   for `NaN` and for either infinity, which are three different values reported as a
///   fourth. In a photometric column they are ordinary — a non-detection, a magnitude of
///   zero flux — and reading one back as "no measurement" is a wrong answer rather than
///   an error.
pub fn to_json(result: &QueryResult) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut rows = Rows::new();
    let buf = stream::collected(&mut rows, result, stream::Ending::default())?;
    if buf.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_slice(&buf)?)
}

/// The rows as a JSON array, written batch by batch.
///
/// `arrow`'s array writer opens the bracket on its first batch, puts a comma between
/// records and closes on `finish`, all into the sink it was built over — so taking the
/// bytes out between batches is the whole of what makes it a stream.
#[derive(Debug)]
pub struct Rows {
    writer: Option<Writer<stream::Pipe, JsonArray>>,
    pipe: stream::Pipe,
}

impl Default for Rows {
    fn default() -> Self {
        Self::new()
    }
}

impl Rows {
    pub fn new() -> Self {
        Self {
            writer: None,
            pipe: stream::Pipe::default(),
        }
    }

    fn writer(&mut self) -> &mut Writer<stream::Pipe, JsonArray> {
        let pipe = self.pipe.clone();
        self.writer.get_or_insert_with(|| {
            WriterBuilder::new()
                .with_explicit_nulls(true)
                .with_encoder_factory(Arc::new(NotANumber))
                .build::<_, JsonArray>(pipe)
        })
    }
}

impl stream::Encoder for Rows {
    fn begin(&mut self, _schema: &SchemaRef) -> Result<Vec<u8>, ApiError> {
        Ok(Vec::new())
    }

    fn rows(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ApiError> {
        let pipe = self.pipe.clone();
        self.writer().write(batch)?;
        Ok(pipe.take())
    }

    fn end(&mut self, _ending: stream::Ending) -> Result<Vec<u8>, ApiError> {
        // Only where something was written: `finish` on a writer that never opened its
        // bracket writes one that no reader asked for, and an answer of no rows is
        // rendered by whatever holds this rather than here.
        if self.writer.is_some() {
            self.writer().finish()?;
        }
        Ok(self.pipe.take())
    }
}

/// Writes `NaN` and the two infinities the way `float()` in Python and `Number()` in
/// JavaScript both read back, which is the nearest thing to a spelling JSON has for them.
///
/// Only the float columns are taken over, and only those that actually hold one of the
/// three: `None` here means the writer uses its own encoder, which formats through
/// `lexical_core` rather than through `std::fmt` and is the faster of the two. So a file
/// of ordinary measurements pays one pass over each float column and nothing else, and
/// the slower formatting is paid only by a column that needs it.
///
/// The pass reads the values buffer rather than the rows, nulls included — a null's slot
/// holds whatever the writer left there, so at worst an unlucky bit pattern costs a
/// column the fast path. It cannot put a value in the answer that is not in the file:
/// what the encoder writes for a null is decided by the null buffer, not by this.
#[derive(Debug)]
struct NotANumber;

impl EncoderFactory for NotANumber {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            // Its native type is `half::f16`, which is a dependency of arrow rather than
            // of this crate, so it is reached through the array and never named.
            DataType::Float16 => match array.as_primitive::<Float16Type>() {
                halves if halves.values().iter().all(|value| value.is_finite()) => {
                    return Ok(None);
                }
                halves => Box::new(Halves(halves)),
            },
            DataType::Float32 => match array.as_primitive::<Float32Type>() {
                floats if floats.values().iter().all(|value| value.is_finite()) => return Ok(None),
                floats => Box::new(Floats(floats)),
            },
            DataType::Float64 => match array.as_primitive::<Float64Type>() {
                floats if floats.values().iter().all(|value| value.is_finite()) => return Ok(None),
                floats => Box::new(Floats(floats)),
            },
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

struct Floats<'a, T: ArrowPrimitiveType>(&'a PrimitiveArray<T>);

impl<T: ArrowPrimitiveType> Encoder for Floats<'_, T>
where
    T::Native: JsonFloat,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.0.value(idx).encode(out);
    }
}

/// One float, as JSON. Implemented per width rather than over a converted `f64`, because
/// widening a `f32` first would print `1.1` as `1.100000023841858` — the same value, and
/// not the same answer.
trait JsonFloat {
    fn encode(self, out: &mut Vec<u8>);
}

/// The finite case is Rust's own shortest round-tripping rendering, which is what the
/// writer's default does with the same value.
fn encode(classify: f64, exact: &dyn fmt::Display, out: &mut Vec<u8>) {
    let spelled = if classify.is_nan() {
        "\"NaN\""
    } else if classify == f64::INFINITY {
        "\"Infinity\""
    } else if classify == f64::NEG_INFINITY {
        "\"-Infinity\""
    } else {
        let _ = write!(out, "{exact}");
        return;
    };
    out.extend_from_slice(spelled.as_bytes());
}

impl JsonFloat for f64 {
    fn encode(self, out: &mut Vec<u8>) {
        encode(self, &self, out);
    }
}

impl JsonFloat for f32 {
    fn encode(self, out: &mut Vec<u8>) {
        encode(f64::from(self), &self, out);
    }
}

/// A half, through `f32` — which is exact, and is what the writer's own encoder does with
/// one, so the number a caller reads is unchanged by any of this.
struct Halves<'a>(&'a PrimitiveArray<Float16Type>);

impl Encoder for Halves<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.0.value(idx).to_f32().encode(out);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    /// The four values a float column can hold that JSON has no number for, and a null
    /// beside them, since telling those five apart is the whole of what this is about.
    ///
    /// A photometric column holds all of them as a matter of course — a non-detection, a
    /// magnitude computed from zero flux — so a caller who reads `NaN` back as "no
    /// measurement" has been given a wrong answer rather than an error.
    #[test]
    fn a_float_json_cannot_write_is_not_reported_as_a_missing_value() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ]));
        // A second column, so that a null shows up as a key in a row that has one.
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..5));
        let batch =
            RecordBatch::try_from_iter_with_nullable([("id", ids, false), ("mag", values, true)])
                .unwrap();
        let result = QueryResult {
            schema: batch.schema(),
            batches: vec![batch],
            data_bytes_read: 0,
        };

        let rows = to_json(&result).unwrap();
        let mag: Vec<&serde_json::Value> = rows.iter().map(|row| &row["mag"]).collect();
        assert_eq!(mag[0], &serde_json::json!(1.5));
        assert_eq!(mag[1], &serde_json::json!("NaN"));
        assert_eq!(mag[2], &serde_json::json!("Infinity"));
        assert_eq!(mag[3], &serde_json::json!("-Infinity"));
        // The one that really is absent, and the only one written as `null`.
        assert_eq!(mag[4], &serde_json::Value::Null);
        // Written rather than left out, so every row has every column of the answer and a
        // null cannot be read as a column the projection did not ask for.
        assert!(rows[4].as_object().unwrap().contains_key("mag"));
    }

    /// A column of ordinary measurements keeps the writer's own rendering, which is both
    /// the faster path and the one whose spelling is not this crate's to change.
    #[test]
    fn an_ordinary_float_is_written_the_way_it_always_was() {
        let single: ArrayRef = Arc::new(Float32Array::from(vec![1.1f32]));
        let double: ArrayRef = Arc::new(Float64Array::from(vec![0.1 + 0.2]));
        let batch = RecordBatch::try_from_iter([("single", single), ("double", double)]).unwrap();
        let rows = to_json(&QueryResult {
            schema: batch.schema(),
            batches: vec![batch],
            data_bytes_read: 0,
        })
        .unwrap();
        // Not 1.100000023841858, which is what widening the f32 to an f64 would print.
        assert_eq!(rows[0]["single"], serde_json::json!(1.1));
        assert_eq!(rows[0]["double"], serde_json::json!(0.30000000000000004));
    }

    #[test]
    fn empty_result_serializes_as_an_empty_array() {
        let empty = QueryResult {
            schema: Arc::new(Schema::new(vec![Field::new(
                "objectid",
                DataType::Int64,
                false,
            )])),
            batches: Vec::new(),
            data_bytes_read: 0,
        };
        assert_eq!(to_json(&empty).unwrap(), Vec::<serde_json::Value>::new());
    }
}
