//! Writing the result back out as delimiter-separated values: CSV, and the same writer with
//! a tab.
//!
//! Two formats and one encoder, because they differ in a delimiter and a media type and in
//! nothing else. Writing them separately is how they come to disagree about quoting.
//!
//! **Flat columns only, and here that is the writer's rule rather than this crate's.**
//! `arrow-csv` refuses any `DataType::is_nested` outright — lists, structs, maps and unions
//! — because delimited text has no notion of structure and no standard invents one for it.
//! The refusal is moved to the front of the encoding, against the schema, so that a caller
//! naming a nested column gets a `400` naming the column rather than the writer's own error
//! raised halfway through a body that has already started.
//!
//! Two things a reader of the output cannot recover, both of which are the format's and
//! neither of which has a spelling to choose instead:
//!
//! - **A null is written as an empty field by default, and so is an empty string.** The two
//!   are one spelling until a caller says otherwise, which `dsv_null_value` is for: a
//!   sentinel separates them, and choosing one is the caller's because any text is text
//!   their own data might contain. This service picks none on their behalf — a default
//!   sentinel would turn some caller's real value into a null, which is worse than two
//!   values sharing a spelling. Whoever needs the difference and has no safe sentinel wants
//!   json or parquet.
//!
//!   An empty field is written `""` where it is the *only* column, and bare wherever there
//!   is another column beside it. So the spelling is not a fixed string and nothing may
//!   compare against one — and the quoting is load-bearing rather than cosmetic. A row whose
//!   single field is written bare is a blank line, which `csv.reader` returns as a record of
//!   *no* fields and `pandas.read_csv` drops outright under its default
//!   `skip_blank_lines=True`: the row disappears rather than arriving empty. With two columns
//!   the same row is `,`, which is unambiguous, which is why nothing is quoted there. Do not
//!   "tidy" the one-column case into a bare line.
//! - **A non-finite float is written the way Rust prints one** — `NaN`, `inf`, `-inf` — which
//!   no CSV convention settles and which `float()` in Python reads back correctly for all
//!   three. `to_json`'s three strings are a different set because JSON's number grammar
//!   excludes them and these are only a convention; the tests below pin what is actually
//!   written so a change in `arrow-cast` is caught here rather than in a caller's parser.
//!
//! An empty result still carries its header: the writer emits one on its first batch, so a
//! result with no batches at all is written as one empty batch instead. A body with no header
//! row would not say what the columns were, which is the whole of what a caller reads first.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::datatypes::Schema;

use crate::error::ApiError;
use crate::query::QueryResult;

/// Which of the two, which is a delimiter and the names that go with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dsv {
    Csv,
    Tsv,
}

impl Dsv {
    /// The media type served with it. `header=present` is the parameter DALI names, and it
    /// is true of every body written here — the header is not optional.
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Csv => "text/csv;header=present",
            Self::Tsv => "text/tab-separated-values",
        }
    }

    /// What a caller asks for it by, which is also the extension a saved body takes. The
    /// two coincide for both of these, so there is one name rather than two that must not
    /// drift apart.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
        }
    }

    const fn delimiter(self) -> u8 {
        match self {
            Self::Csv => b',',
            Self::Tsv => b'\t',
        }
    }
}

/// Serialize the result as delimited text.
///
/// The whole body is built in memory, which is what the other three encodings already do
/// with the same rows.
pub fn encode(result: &QueryResult, kind: Dsv, null: &str) -> Result<String, ApiError> {
    refuse_nested(&result.schema)?;

    let mut out = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_header(true)
        .with_delimiter(kind.delimiter())
        .with_null(null.to_owned())
        .build(&mut out);

    if result.batches.is_empty() {
        let empty = RecordBatch::new_empty(result.schema.clone());
        write_batch(&mut writer, &empty)?;
    } else {
        for batch in &result.batches {
            write_batch(&mut writer, batch)?;
        }
    }
    drop(writer);

    // The writer formats through `arrow-cast`'s display, which produces `str` throughout, so
    // the bytes cannot be invalid UTF-8 — but building the body out of an assumption rather
    // than a check is how that stops being true without anything saying so.
    String::from_utf8(out).map_err(|_| ApiError::internal("delimited output was not UTF-8"))
}

fn write_batch<W: std::io::Write>(
    writer: &mut datafusion::arrow::csv::Writer<W>,
    batch: &RecordBatch,
) -> Result<(), ApiError> {
    writer
        .write(batch)
        .map_err(|error| ApiError::internal(format!("writing delimited output failed: {error}")))
}

/// The longest a null sentinel may be, in bytes.
///
/// A sentinel is a token a reader compares whole cells against — `NULL`, `\N`, `NaN` — so
/// anything approaching this is a mistake rather than a use. The bound exists to keep one
/// from being a place to put a kilobyte, not because any particular length breaks.
pub const MAX_NULL_VALUE: usize = 128;

/// A separator or a row terminator, refused in a sentinel whichever format is being written.
///
/// Both of them, not the one this format uses: the option is checked before the format is
/// known to be the one that cares, and a sentinel that is legal under `csv` and ruinous under
/// `tsv` is a trap rather than a convenience.
const SEPARATORS: [char; 4] = [',', '\t', '\n', '\r'];

/// Check a caller's null sentinel before anything is written with it.
///
/// The writer quotes whatever it is given, so none of this is about the document staying
/// well-formed — a sentinel holding a comma does come out as a correctly quoted field. It is
/// about what the token is for. A sentinel is compared against a whole cell by whatever reads
/// the answer, and:
///
/// - **A separator or a terminator is refused.** CSV's quoting would carry one, but TSV has
///   no quoting convention every reader implements, so a tab inside a sentinel splits a row
///   for anyone who splits on tabs — which is most readers of a TSV. A newline does the same
///   to a row-at-a-time reader of either.
/// - **Any other control character is refused** as a token nothing would compare against.
/// - **A sentinel longer than [`MAX_NULL_VALUE`] is refused**, which no real one approaches.
///
/// The reason is returned rather than an `ApiError`, so that the sentence is this module's
/// and the status is the caller's.
pub fn check_null_value(value: &str) -> Result<(), String> {
    if value.len() > MAX_NULL_VALUE {
        return Err(format!(
            "it is {} bytes and at most {MAX_NULL_VALUE} are taken",
            value.len()
        ));
    }
    if let Some(bad) = value.chars().find(|c| SEPARATORS.contains(c)) {
        return Err(format!(
            "it holds {bad:?}, which separates one value or one row from the next"
        ));
    }
    if let Some(bad) = value.chars().find(|c| c.is_control()) {
        return Err(format!("it holds the control character {bad:?}"));
    }
    Ok(())
}

/// Refuse a nested column before any of the body exists.
///
/// The same `is_nested` the writer itself asks, so nothing it would reject reaches it and
/// the message a caller sees is this crate's rather than one about an arrow type.
fn refuse_nested(schema: &Schema) -> Result<(), ApiError> {
    for field in schema.fields() {
        if field.data_type().is_nested() {
            return Err(ApiError::bad_request(format!(
                "column {:?} is {}, and delimited text has no form for a nested column; \
                 ask for json or parquet, or leave the column out",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int64Array, ListArray, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;

    fn result(batch: RecordBatch) -> QueryResult {
        QueryResult {
            schema: batch.schema(),
            batches: vec![batch],
            data_bytes_read: 0,
        }
    }

    fn one(name: &str, values: ArrayRef) -> QueryResult {
        result(RecordBatch::try_from_iter_with_nullable([(name, values, true)]).unwrap())
    }

    /// The header is the first line either way, and only the separator differs.
    #[test]
    fn the_two_formats_differ_in_one_byte() {
        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "a",
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                true,
            ),
            (
                "b",
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();
        let rows = result(batch);

        assert_eq!(encode(&rows, Dsv::Csv, "").unwrap(), "a,b\n1,2\n");
        assert_eq!(encode(&rows, Dsv::Tsv, "").unwrap(), "a\tb\n1\t2\n");
    }

    /// A caller reads the columns off the first line, so a query that matched nothing still
    /// has to say what it would have returned. The writer emits a header with its first
    /// batch and a result with no batches has none to give it.
    #[test]
    fn an_empty_answer_still_carries_its_header() {
        let empty = QueryResult {
            schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            batches: Vec::new(),
            data_bytes_read: 0,
        };
        assert_eq!(encode(&empty, Dsv::Csv, "").unwrap(), "a,b\n");
    }

    /// What the three non-finite floats are actually written as. Pinned rather than assumed:
    /// the formatting is `arrow-cast`'s, and a caller's parser is what notices if it moves.
    #[test]
    fn a_non_finite_float_is_written_as_itself() {
        let values = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5];
        let document = encode(
            &one("x", Arc::new(Float64Array::from(values))),
            Dsv::Csv,
            "",
        )
        .unwrap();
        assert_eq!(document, "x\nNaN\ninf\n-inf\n1.5\n");
    }

    /// The same value at its own width, never widened first — `1.1_f32` as an `f64` prints
    /// `1.100000023841858`, which is the same number and not the same answer.
    #[test]
    fn a_float_is_formatted_at_its_own_width() {
        let document = encode(
            &one("x", Arc::new(Float32Array::from(vec![1.1_f32]))),
            Dsv::Csv,
            "",
        )
        .unwrap();
        assert_eq!(document, "x\n1.1\n");
    }

    /// The one thing the format cannot say without a sentinel, asserted so that it is a known
    /// limitation rather than a surprise: a null and an empty string are one spelling.
    ///
    /// `""` rather than a bare empty field because this answer has a single column, and the
    /// `csv` crate quotes a one-field record so that it is not an empty line. With a second
    /// column both come out bare — which is why the rule is "an empty field" and not a string
    /// anything may compare against.
    #[test]
    fn a_null_and_an_empty_string_are_one_spelling() {
        let document = encode(
            &one(
                "x",
                Arc::new(StringArray::from(vec![None, Some(""), Some("a")])),
            ),
            Dsv::Csv,
            "",
        )
        .unwrap();
        assert_eq!(document, "x\n\"\"\n\"\"\na\n");
    }

    /// A sentinel separates the two spellings the default leaves sharing one, which is the
    /// whole of what the option is for. Two columns, so that the empty string is written
    /// bare rather than quoted by the one-field rule above.
    #[test]
    fn a_sentinel_separates_a_null_from_an_empty_string() {
        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "id",
                Arc::new(Int64Array::from(vec![0_i64, 1, 2])) as ArrayRef,
                false,
            ),
            (
                "x",
                Arc::new(StringArray::from(vec![None, Some(""), Some("a")])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();
        assert_eq!(
            encode(&result(batch), Dsv::Csv, "\\N").unwrap(),
            "id,x\n0,\\N\n1,\n2,a\n"
        );
    }

    /// The writer quotes the sentinel like any other value, so a document written with an
    /// awkward one is still well-formed — which is why `check_null_value` is about what a
    /// reader can do with it rather than about the document.
    #[test]
    fn a_sentinel_is_quoted_like_any_other_value() {
        let document = encode(
            &one("x", Arc::new(StringArray::from(vec![None, Some("a")]))),
            Dsv::Csv,
            "p,q",
        )
        .unwrap();
        assert_eq!(document, "x\n\"p,q\"\na\n");
    }

    /// Both bounds, and the two that are fine either side of them.
    #[test]
    fn a_sentinel_is_checked_before_it_is_used() {
        for good in ["", "NULL", "\\N", "NaN", &"x".repeat(MAX_NULL_VALUE)] {
            assert!(check_null_value(good).is_ok(), "{good:?}");
        }

        let long = "x".repeat(MAX_NULL_VALUE + 1);
        let error = check_null_value(&long).unwrap_err();
        assert!(error.contains("at most"), "{error}");

        // Both delimiters and both terminators, whichever format is being written: a
        // sentinel legal under one and ruinous under the other is a trap.
        for bad in ["a,b", "a\tb", "a\nb", "a\rb"] {
            let error = check_null_value(bad).unwrap_err();
            assert!(error.contains("separates"), "{bad:?}: {error}");
        }

        let error = check_null_value("\u{0}").unwrap_err();
        assert!(error.contains("control character"), "{error}");
    }

    /// A value carrying the delimiter is quoted rather than splitting the row.
    #[test]
    fn a_value_holding_the_delimiter_is_quoted() {
        let document = encode(
            &one("x", Arc::new(StringArray::from(vec![Some("a,b")]))),
            Dsv::Csv,
            "",
        )
        .unwrap();
        assert_eq!(document, "x\n\"a,b\"\n");
    }

    /// Refused against the schema, before any of the body exists, and naming the column —
    /// which is what a caller can act on. `arrow-csv` would raise its own error part-way
    /// through a response that had already begun.
    #[test]
    fn a_nested_column_is_refused_by_name() {
        let values =
            ListArray::from_iter_primitive::<datafusion::arrow::datatypes::Int64Type, _, _>(vec![
                Some(vec![Some(1_i64)]),
            ]);
        for kind in [Dsv::Csv, Dsv::Tsv] {
            let error = encode(&one("lc", Arc::new(values.clone())), kind, "").unwrap_err();
            let message = format!("{error:?}");
            assert!(message.contains("lc"), "{message}");
            assert!(message.contains("json or parquet"), "{message}");
        }
    }

    /// A struct is refused for the same reason a list is, and so is the column beside it
    /// being flat — one nested column is enough to refuse the body.
    #[test]
    fn a_struct_column_is_refused_too() {
        let inner: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
        let values: ArrayRef = Arc::new(datafusion::arrow::array::StructArray::from(vec![(
            Arc::new(Field::new("mag", DataType::Int64, true)),
            inner,
        )]));
        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "id",
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                true,
            ),
            ("lc", values, true),
        ])
        .unwrap();
        let error = encode(&result(batch), Dsv::Csv, "").unwrap_err();
        assert!(format!("{error:?}").contains("lc"));
    }

    /// The media types are the ones DALI names, and the header is always present.
    #[test]
    fn each_format_names_its_media_type() {
        assert_eq!(Dsv::Csv.content_type(), "text/csv;header=present");
        assert_eq!(Dsv::Tsv.content_type(), "text/tab-separated-values");
        assert_eq!(Dsv::Csv.name(), "csv");
        assert_eq!(Dsv::Tsv.name(), "tsv");
    }
}
