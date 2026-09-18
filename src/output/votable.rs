//! Writing the result back out as a VOTable, the XML table format IVOA tools read.
//!
//! One `TABLE` in one `RESOURCE`, serialized as `TABLEDATA` — the XML-in-XML form, which
//! every reader supports and which needs no second request for a stream. `BINARY2` would
//! be smaller on the wire, but a response is compressed on the way out anyway, and base64
//! of a packed buffer is not.
//!
//! **Flat columns only.** A struct or a list column is refused by name. A nested column
//! becomes a `GROUP` of `FIELDref`s over dotted `FIELD`s, and what a `FIELD` under one
//! says is not settled: a variable-length array of variable-length strings has no
//! spelling in VOTable at all, and neither has a null inside an array. Guessing at those
//! would put a value in an answer that a reader cannot tell from a different value, so
//! the column is refused until they are decided.
//!
//! Three things the mapping decides, each because the alternative returns a wrong value
//! rather than an error:
//!
//! - **A float keeps its own width.** An `f32` widened to an `f64` before formatting
//!   prints `1.1` as `1.100000023841858` — the same value, and not the same answer.
//! - **`NaN` and the infinities are written, not nulled.** VOTable spells them, and in a
//!   photometric column all three are ordinary measurements.
//! - **An integer type only ever widens.** VOTable's integers are signed and its only
//!   8-bit type is unsigned, so `Int8` goes out as `short`; `UInt64` has nothing wide
//!   enough to hold it and is refused rather than wrapped.
//! - **An instant is text with an `xtype`, in UTC.** DALI §3.3.3 spells a date or a time
//!   `YYYY-MM-DD['T'hh:mm:ss[.SSS]]` and marks the `FIELD` `xtype="timestamp"`, which is
//!   what tells a client the characters are an instant rather than a string. A zone other
//!   than UTC has no spelling there at all, so a column carrying one is moved to UTC and
//!   written with the `Z` the section allows a civil time.
//!
//! One thing the format itself cannot say, and no writing of it can fix: an empty `TD` is
//! the only spelling a null has, and in a character column a reader cannot tell that from
//! an empty string. `VALUES`'s `null` attribute is for integers alone, so there is nothing
//! else to write. A caller who needs the difference wants json or parquet.

use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::engine::query::QueryResult;
use crate::error::ApiError;

/// The media type IVOA registers for a VOTable document.
pub const CONTENT_TYPE: &str = "application/x-votable+xml";

/// The version the document declares.
const VERSION: &str = "1.4";

/// The namespace that goes with it, which is `v1.3` for every 1.x version: the schema
/// moved on and the namespace deliberately did not, so that a reader written against an
/// earlier one still recognises the elements.
const NAMESPACE: &str = "http://www.ivoa.net/xml/VOTable/v1.3";

/// Serialize the result as a VOTable document.
///
/// The whole document is built in memory, which is what the JSON and parquet answers
/// already do with the same rows.
pub fn encode(result: &QueryResult) -> Result<String, ApiError> {
    document(result, false)
}

/// The same, for an answer that stopped at a row bound rather than at the last row.
///
/// The `OVERFLOW` marker goes *after* the table, which is DALI §4.4.1: the `OK` at the top
/// was written before the row count was known, and a document that has already claimed to
/// be whole says otherwise at the end. A client reading the stream therefore learns of the
/// truncation only once it has the rows, which is what the ordering is for.
pub fn encode_truncated(result: &QueryResult) -> Result<String, ApiError> {
    document(result, true)
}

fn document(result: &QueryResult, overflow: bool) -> Result<String, ApiError> {
    let mut out = String::new();
    prologue(&mut out);
    // DALI's spelling of "the query ran", which is what a client that reads VOTables from
    // an IVOA service looks for before it looks at the rows.
    out.push_str("<INFO name=\"QUERY_STATUS\" value=\"OK\"/>\n");
    let _ = writeln!(out, "<TABLE nrows=\"{}\">", result.num_rows());
    // An `ID` has to be unique in the document, and two columns may share a name — a
    // statement can select one twice. The second one keeps its name and goes without.
    let mut identified = Vec::new();
    for field in result.schema.fields() {
        let spelled = spelling(field)?;
        let name = attribute(field.name())?;
        let _ = write!(out, "<FIELD name=\"{name}\"");
        if is_xml_name(field.name()) && !identified.contains(&field.name()) {
            identified.push(field.name());
            let _ = write!(out, " ID=\"{name}\"");
        }
        let _ = write!(out, " datatype=\"{}\"", spelled.datatype);
        if let Some(size) = spelled.arraysize {
            let _ = write!(out, " arraysize=\"{size}\"");
        }
        if let Some(xtype) = spelled.xtype {
            let _ = write!(out, " xtype=\"{xtype}\"");
        }
        out.push_str("/>\n");
    }
    out.push_str("<DATA>\n<TABLEDATA>\n");
    for batch in &result.batches {
        push_rows(batch, &mut out)?;
    }
    out.push_str("</TABLEDATA>\n</DATA>\n</TABLE>\n");
    if overflow {
        out.push_str("<INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>\n");
    }
    out.push_str("</RESOURCE>\n</VOTABLE>\n");
    Ok(out)
}

/// A document saying the query was refused, which is what DALI §4.4.2 asks an error to be.
///
/// The `INFO` carries the status and the message is its content. There is no `TABLE`: the
/// status comes before one and there is nothing to put after it.
///
/// Infallible, unlike everything else here. An error document that could itself fail to
/// encode would leave a caller with nothing at all, so a character XML cannot carry is
/// dropped from the message rather than refused.
pub fn error(message: &str) -> String {
    let readable = message
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\t' | '\n'))
        .collect::<String>();
    let mut out = String::new();
    prologue(&mut out);
    let _ = writeln!(
        out,
        "<INFO name=\"QUERY_STATUS\" value=\"ERROR\">{}</INFO>",
        quick_xml::escape::escape(&readable)
    );
    out.push_str("</RESOURCE>\n</VOTABLE>\n");
    out
}

/// Everything above the first `INFO`, which every document here shares.
fn prologue(out: &mut String) {
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(out, "<VOTABLE version=\"{VERSION}\" xmlns=\"{NAMESPACE}\">");
    out.push_str("<RESOURCE type=\"results\">\n");
}

/// Whether a column's name can also stand as the `FIELD`'s `ID`.
///
/// The two clients read different attributes — one answer came back with `source_id`
/// through `pyvo` and `SOURCE_ID` through STILTS — so writing both with the same string is
/// what keeps a query written in TOPCAT working when it is pasted into a notebook.
///
/// `ID` is an XML ID, so it has to be an XML Name: a name that is not one would make the
/// document unparseable, which is a worse answer than the disagreement. A column out of
/// somebody's parquet file may be called anything at all, so this is asked rather than
/// assumed. The set is narrowed to ASCII, and the colon an XML Name also allows is left
/// out, being what a namespace prefix is written with.
fn is_xml_name(value: &str) -> bool {
    let mut characters = value.chars();
    let leads = characters
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    leads && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// How a column is declared: its `datatype`, the `arraysize` where the value has no fixed
/// width, and the `xtype` where DALI gives what the characters mean a name of its own.
///
/// Public because `TAP_SCHEMA.columns` and VOSI's table metadata publish the same three
/// about the same columns. A document that said one thing there and this wrote another
/// would be a client building a query against a type the answer does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spelling {
    pub datatype: &'static str,
    pub arraysize: Option<&'static str>,
    pub xtype: Option<&'static str>,
}

impl Spelling {
    /// A value of the given width, meaning whatever its characters say.
    const fn plain(datatype: &'static str, arraysize: Option<&'static str>) -> Self {
        Self {
            datatype,
            arraysize,
            xtype: None,
        }
    }

    /// A date or a time, which DALI §3.3.3 writes as characters and marks.
    const fn instant(arraysize: Option<&'static str>) -> Self {
        Self {
            datatype: "char",
            arraysize,
            xtype: Some(TIMESTAMP),
        }
    }
}

/// DALI §3.3.3's name for an instant, which is the one `xtype` this service writes.
pub const TIMESTAMP: &str = "timestamp";

/// What a column is declared as.
///
/// This is the one list of what can be written, and `writer` covers what it admits. A
/// type reaching neither is refused here, before a byte of the document exists.
pub fn spelling(field: &Field) -> Result<Spelling, ApiError> {
    let datatype = match field.data_type() {
        DataType::Boolean => "boolean",
        // The only 8-bit integer VOTable has is unsigned, so a signed byte has to widen.
        DataType::Int8 | DataType::Int16 => "short",
        DataType::UInt8 => "unsignedByte",
        DataType::Int32 | DataType::UInt16 => "int",
        DataType::Int64 | DataType::UInt32 => "long",
        // A half is exact in a `float`, and the text written for one is the half's own
        // shortest rendering rather than the widened value's.
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        // A string is an array of characters, and `char` is ASCII by definition, so a
        // column holding anything else would be a document that does not say what it
        // holds. `unicodeChar` is right for every string a parquet file can carry.
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            return Ok(Spelling::plain("unicodeChar", Some("*")));
        }
        // `char` and not `unicodeChar`: DALI's form is digits, hyphens, a `T`, a dot and a
        // `Z`, all of which are ASCII, and §3.3.3 names the datatype outright.
        //
        // The widths are that section's own. A date alone is ten characters and it says so
        // in as many words; anything that may carry a time is written `*`, since how many
        // digits of a second a column holds is a property of its values rather than of its
        // type.
        DataType::Timestamp(_, _) | DataType::Date64 => {
            return Ok(Spelling::instant(Some("*")));
        }
        DataType::Date32 => return Ok(Spelling::instant(Some("10"))),
        nested @ (DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)) => {
            return Err(ApiError::bad_request(format!(
                "column {:?} is {nested}, and a nested column has no VOTable form here yet; \
                 ask for json or parquet, or leave the column out",
                field.name()
            )));
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "column {:?} is {other}, which a VOTable has no type for; \
                 ask for json or parquet, or leave the column out",
                field.name()
            )));
        }
    };
    Ok(Spelling::plain(datatype, None))
}

/// Appends one row's value to the document. The row is already known not to be null.
type Push<'a> = Box<dyn Fn(usize, &mut String) -> Result<(), ApiError> + 'a>;

/// One batch's rows, as `TR` elements.
///
/// The writers are resolved once per column rather than once per cell: a batch is
/// columnar, and deciding what a value is on every one of them is the whole cost of
/// writing a wide table.
fn push_rows(batch: &RecordBatch, out: &mut String) -> Result<(), ApiError> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| Ok((array, writer(array)?)))
        .collect::<Result<Vec<(&ArrayRef, Push<'_>)>, ApiError>>()?;
    for row in 0..batch.num_rows() {
        out.push_str("<TR>");
        for (array, push) in &columns {
            // An empty cell is how TABLEDATA spells a null, in any column and at any
            // type. Nothing is written inside it, so nothing has to be told apart from a
            // value the column could genuinely hold.
            if array.is_null(row) {
                out.push_str("<TD/>");
                continue;
            }
            out.push_str("<TD>");
            push(row, out)?;
            out.push_str("</TD>");
        }
        out.push_str("</TR>\n");
    }
    Ok(())
}

/// How to write one column's values, downcast once for the whole batch.
///
/// The arms are [`spelling`]'s list, which has already run over the same schema — so the
/// refusal at the end is what keeps the two from drifting rather than a case a request
/// reaches.
fn writer(array: &ArrayRef) -> Result<Push<'_>, ApiError> {
    let push: Push<'_> = match array.data_type() {
        DataType::Boolean => {
            let array = array.as_boolean();
            // `T` and `F`, which is what the standard spells a boolean; `true` and
            // `false` are not among the forms it lists.
            Box::new(move |row, out| {
                out.push(if array.value(row) { 'T' } else { 'F' });
                Ok(())
            })
        }
        DataType::Int8 => integer::<Int8Type>(array),
        DataType::Int16 => integer::<Int16Type>(array),
        DataType::Int32 => integer::<Int32Type>(array),
        DataType::Int64 => integer::<Int64Type>(array),
        DataType::UInt8 => integer::<UInt8Type>(array),
        DataType::UInt16 => integer::<UInt16Type>(array),
        DataType::UInt32 => integer::<UInt32Type>(array),
        DataType::Float16 => float::<Float16Type>(array),
        DataType::Float32 => float::<Float32Type>(array),
        DataType::Float64 => float::<Float64Type>(array),
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            Box::new(move |row, out| push_text(array.value(row), out))
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            Box::new(move |row, out| push_text(array.value(row), out))
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            Box::new(move |row, out| push_text(array.value(row), out))
        }
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => instants(array)?,
        other => {
            return Err(ApiError::internal(format!(
                "a {other} column reached the VOTable writer"
            )));
        }
    };
    Ok(push)
}

/// The zone an instant is written in, which is the only one DALI §3.3.3 gives a spelling.
const UTC: &str = "UTC";

/// DALI §3.3.3's `YYYY-MM-DD['T'hh:mm:ss[.SSS]]`, as the four formats arrow picks between.
///
/// A `Z` goes on the zoned form alone: §3.3.3 has an astronomical value carry no zone
/// indicator at all and lets a civil one carry `Z`, and a column whose type names a zone is
/// a civil time by construction. `%.f` writes the fraction the value has and nothing where
/// it has none, which is why a second-resolution column comes out as the section's own
/// example rather than with three zeroes after it.
const INSTANT: FormatOptions<'static> = FormatOptions::new()
    .with_date_format(Some("%Y-%m-%d"))
    .with_datetime_format(Some("%Y-%m-%dT%H:%M:%S%.f"))
    .with_timestamp_format(Some("%Y-%m-%dT%H:%M:%S%.f"))
    .with_timestamp_tz_format(Some("%Y-%m-%dT%H:%M:%S%.fZ"));

/// One instant column, as the text DALI gives it.
///
/// **A zoned column is moved to UTC first.** Arrow holds the instant itself and the zone is
/// its type's, so this renames the zone the value is printed in without touching a value —
/// and it has to be done, because arrow would otherwise print the local time of whatever
/// zone the column names, which DALI has no form for.
///
/// The whole column is written up front rather than a cell at a time: a formatter is made
/// by downcasting the array, which is a thing to do once per column and not once per row.
fn instants(array: &ArrayRef) -> Result<Push<'_>, ApiError> {
    let utc: ArrayRef = match array.data_type() {
        DataType::Timestamp(unit, Some(_)) => match unit {
            TimeUnit::Second => Arc::new(
                array
                    .as_primitive::<TimestampSecondType>()
                    .clone()
                    .with_timezone(UTC),
            ),
            TimeUnit::Millisecond => Arc::new(
                array
                    .as_primitive::<TimestampMillisecondType>()
                    .clone()
                    .with_timezone(UTC),
            ),
            TimeUnit::Microsecond => Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .clone()
                    .with_timezone(UTC),
            ),
            TimeUnit::Nanosecond => Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .clone()
                    .with_timezone(UTC),
            ),
        },
        _ => Arc::clone(array),
    };
    let written = {
        let formatter = ArrayFormatter::try_new(utc.as_ref(), &INSTANT).map_err(|error| {
            ApiError::internal(format!("an instant column could not be written: {error}"))
        })?;
        (0..utc.len())
            .map(|row| formatter.value(row).to_string())
            .collect::<Vec<_>>()
    };
    Ok(Box::new(move |row, out| {
        // The row is the batch's own and the text was written from that batch's column, so
        // this is in range. Asked rather than indexed because the alternative to a refusal
        // here is a panic in a response handler.
        let value = written
            .get(row)
            .ok_or_else(|| ApiError::internal("an instant column ran out of rows"))?;
        out.push_str(value);
        Ok(())
    }))
}

fn integer<T>(array: &ArrayRef) -> Push<'_>
where
    T: ArrowPrimitiveType,
    T::Native: fmt::Display,
{
    let array = array.as_primitive::<T>();
    Box::new(move |row, out| {
        let _ = write!(out, "{}", array.value(row));
        Ok(())
    })
}

/// One float, at its own width.
///
/// `NaN`, `+Inf` and `-Inf` are the standard's own spellings, and the three are values a
/// photometric column holds rather than absences: a non-detection, a magnitude of zero
/// flux. Writing an empty cell for them would report three different values as a fourth.
///
/// The classification goes through `f64` because that is where the predicates are; the
/// text does not, so a `f32` prints as the shortest string that reads back as that `f32`.
fn float<T>(array: &ArrayRef) -> Push<'_>
where
    T: ArrowPrimitiveType,
    T::Native: fmt::Display + Into<f64>,
{
    let array = array.as_primitive::<T>();
    Box::new(move |row, out| {
        let value = array.value(row);
        let classify: f64 = value.into();
        let spelled = if classify.is_nan() {
            "NaN"
        } else if classify == f64::INFINITY {
            "+Inf"
        } else if classify == f64::NEG_INFINITY {
            "-Inf"
        } else {
            let _ = write!(out, "{value}");
            return Ok(());
        };
        out.push_str(spelled);
        Ok(())
    })
}

/// One string, as the text of a `TD`.
///
/// Escaping the markup characters is `quick-xml`'s. Two things are this function's, and
/// both are about the string that comes back out of a parser being the string that went
/// in:
///
/// - **A C0 control character is refused.** XML 1.0 cannot carry one at all — not
///   escaped, not raw — so writing it would produce a document no reader can parse, which
///   is worse than saying so. Tab, newline and carriage return are the three exceptions.
/// - **Those three are written as numeric references.** A parser normalizes a literal
///   carriage return to a newline, and readers trim the whitespace around a `TD`'s text,
///   so a value ending in a tab would come back a different string.
///
/// The check is over bytes, which is sound for the C0 range: those byte values appear in
/// UTF-8 only as themselves.
fn push_text(value: &str, out: &mut String) -> Result<(), ApiError> {
    if value
        .bytes()
        .any(|byte| byte < 0x20 && !matches!(byte, b'\t' | b'\n' | b'\r'))
    {
        return Err(ApiError::bad_request(
            "a string in this answer holds a control character, which XML cannot carry; \
             ask for json or parquet",
        ));
    }
    let escaped = quick_xml::escape::partial_escape(value);
    if !escaped.contains(['\t', '\n', '\r']) {
        out.push_str(&escaped);
        return Ok(());
    }
    for character in escaped.chars() {
        match character {
            '\t' => out.push_str("&#9;"),
            '\n' => out.push_str("&#10;"),
            '\r' => out.push_str("&#13;"),
            other => out.push(other),
        }
    }
    Ok(())
}

/// One attribute value — a column's name, which came out of a file someone else wrote or
/// out of an alias in the caller's own query. Both are markup until they are escaped, and
/// neither may carry a character XML has no form for.
fn attribute(value: &str) -> Result<String, ApiError> {
    if value.bytes().any(|byte| byte < 0x20) {
        return Err(ApiError::bad_request(format!(
            "column {value:?} has a control character in its name, \
             which XML cannot carry; ask for json or parquet"
        )));
    }
    Ok(quick_xml::escape::escape(value).into_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
        Int64Array, ListArray, StringArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema};

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

    /// The cells of the one column, in row order, with the `TD` markup stripped.
    fn cells(document: &str) -> Vec<String> {
        document
            .lines()
            .filter(|line| line.starts_with("<TR>"))
            .map(|line| {
                line.trim_start_matches("<TR>")
                    .trim_end_matches("</TR>")
                    .trim_start_matches("<TD>")
                    .trim_end_matches("</TD>")
                    .to_owned()
            })
            .collect()
    }

    #[test]
    fn a_document_declares_its_version_and_namespace() {
        let document = encode(&one("x", Arc::new(Int64Array::from(vec![1_i64])))).unwrap();
        assert!(document.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
        assert!(
            document.contains(&format!(
                "<VOTABLE version=\"{VERSION}\" xmlns=\"{NAMESPACE}\">"
            )),
            "{document}"
        );
        assert!(
            document.contains("<INFO name=\"QUERY_STATUS\" value=\"OK\"/>"),
            "{document}"
        );
        assert!(document.contains("<TABLE nrows=\"1\">"), "{document}");
    }

    /// DALI §4.4.1 puts it after the table: the `OK` at the top was written before the row
    /// count was known, so the document says it is whole and then says otherwise.
    #[test]
    fn a_truncated_answer_says_so_after_the_table() {
        let result = one("x", Arc::new(Int64Array::from(vec![1_i64, 2])));
        let document = encode_truncated(&result).unwrap();
        let (before, after) = document.split_once("</TABLE>").unwrap();
        assert!(before.contains("value=\"OK\""), "{document}");
        assert!(after.contains("<INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>"));
        // And the whole answer carries no such marker anywhere.
        assert!(!encode(&result).unwrap().contains("OVERFLOW"));
    }

    /// The other thing a `QUERY_STATUS` says, with no table to say it about.
    #[test]
    fn an_error_is_a_document_of_its_own() {
        let document = error("column \"a & b\" is <not> there");
        assert!(document.starts_with("<?xml version=\"1.0\""), "{document}");
        assert!(
            document.contains(
                "<INFO name=\"QUERY_STATUS\" value=\"ERROR\">column &quot;a &amp; b&quot; \
                 is &lt;not&gt; there</INFO>"
            ),
            "{document}"
        );
        assert!(!document.contains("<TABLE"), "{document}");

        // A character XML cannot carry is dropped rather than refused: a caller with a
        // failed error document has nothing at all.
        assert!(error("a\u{0}b").contains("ab"));
    }

    /// The two clients read different attributes, so both carry the same string — and a
    /// name XML has no ID for goes without rather than making the document unparseable.
    #[test]
    fn a_field_answers_to_one_name() {
        let document = encode(&one("source_id", Arc::new(Int64Array::from(vec![1_i64])))).unwrap();
        assert!(
            document.contains("<FIELD name=\"source_id\" ID=\"source_id\" datatype=\"long\"/>"),
            "{document}"
        );

        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "2mass",
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                true,
            ),
            (
                "g-r",
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();
        let document = encode(&result(batch)).unwrap();
        // A digit cannot lead an XML Name; a hyphen inside one is fine.
        assert!(
            document.contains("<FIELD name=\"2mass\" datatype=\"long\"/>"),
            "{document}"
        );
        assert!(
            document.contains("<FIELD name=\"g-r\" ID=\"g-r\" datatype=\"long\"/>"),
            "{document}"
        );
    }

    /// Every integer width maps to the narrowest VOTable type that holds all of it, and
    /// the one that nothing holds is refused rather than wrapped.
    #[test]
    fn an_integer_only_ever_widens() {
        let document = encode(&one("x", Arc::new(Int8Array::from(vec![-1_i8])))).unwrap();
        assert!(
            document.contains("<FIELD name=\"x\" ID=\"x\" datatype=\"short\"/>"),
            "{document}"
        );
        let refused = encode(&one("x", Arc::new(UInt64Array::from(vec![1_u64])))).unwrap_err();
        assert!(refused.to_string().contains("no type for"), "{refused}");
    }

    /// The three values JSON has no number for, and the reason the column is not simply
    /// nulled where it holds one.
    #[test]
    fn the_three_that_are_not_numbers_are_written_as_values() {
        let values = Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(1.5),
            None,
        ]);
        let document = encode(&one("flux", Arc::new(values))).unwrap();
        assert_eq!(cells(&document), ["NaN", "+Inf", "-Inf", "1.5", "<TD/>"]);
    }

    /// Widening to `f64` before formatting would print this as `1.100000023841858` —
    /// the same value, and not the same answer.
    #[test]
    fn a_float_is_formatted_at_its_own_width() {
        let document = encode(&one("mag", Arc::new(Float32Array::from(vec![1.1_f32])))).unwrap();
        assert_eq!(cells(&document), ["1.1"]);
        assert!(
            document.contains("<FIELD name=\"mag\" ID=\"mag\" datatype=\"float\"/>"),
            "{document}"
        );
    }

    /// DALI §3.3.3: characters in the FITS and STC convention, marked with an `xtype` so a
    /// client reads them as an instant. The fraction is the value's own, so a column
    /// resolved to the second comes out as that section's own example.
    #[test]
    fn an_instant_is_characters_with_an_xtype() {
        // 2020-01-02T03:04:05Z.
        let seconds = TimestampSecondArray::from(vec![Some(1_577_934_245_i64), None]);
        let document = encode(&one("observed", Arc::new(seconds))).unwrap();
        assert!(
            document.contains(
                "<FIELD name=\"observed\" ID=\"observed\" datatype=\"char\" \
                 arraysize=\"*\" xtype=\"timestamp\"/>"
            ),
            "{document}"
        );
        assert_eq!(cells(&document), ["2020-01-02T03:04:05", "<TD/>"]);

        let nanos = TimestampNanosecondArray::from(vec![1_577_934_245_123_456_789_i64]);
        assert_eq!(
            cells(&encode(&one("observed", Arc::new(nanos))).unwrap()),
            ["2020-01-02T03:04:05.123456789"]
        );

        // How many digits a fraction gets is the value's, not the column's: a half second
        // is three of them whatever resolution the column is stored at.
        let millis = TimestampMillisecondArray::from(vec![1_577_934_245_500_i64]);
        assert_eq!(
            cells(&encode(&one("observed", Arc::new(millis))).unwrap()),
            ["2020-01-02T03:04:05.500"]
        );
    }

    /// The same instant, in a column whose type names a zone two hours ahead. Printed in
    /// that zone it would read `05:04:05`, which is the hour the value is not: DALI has no
    /// form for a zone other than UTC, so the column is moved before it is written.
    #[test]
    fn a_zoned_instant_is_written_in_utc() {
        let values =
            TimestampSecondArray::from(vec![1_577_934_245_i64]).with_timezone("+02:00".to_owned());
        let document = encode(&one("observed", Arc::new(values))).unwrap();
        assert_eq!(cells(&document), ["2020-01-02T03:04:05Z"]);
        assert!(document.contains("xtype=\"timestamp\""), "{document}");
    }

    /// A column that is a date carries no time and says so in its width, which is the one
    /// `arraysize` DALI §3.3.3 names outright.
    #[test]
    fn a_date_is_ten_characters() {
        let document = encode(&one("day", Arc::new(Date32Array::from(vec![18_263])))).unwrap();
        assert!(
            document.contains("arraysize=\"10\" xtype=\"timestamp\""),
            "{document}"
        );
        assert_eq!(cells(&document), ["2020-01-02"]);

        // The 64-bit spelling holds milliseconds, so what it can carry is a time and its
        // width is the open one.
        let ms = Date64Array::from(vec![1_577_923_200_000_i64]);
        let document = encode(&one("day", Arc::new(ms))).unwrap();
        assert!(
            document.contains("arraysize=\"*\" xtype=\"timestamp\""),
            "{document}"
        );
        assert_eq!(cells(&document), ["2020-01-02T00:00:00"]);
    }

    /// `true` and `false` are not among the forms the standard lists for a boolean.
    #[test]
    fn a_boolean_is_t_or_f() {
        let values = BooleanArray::from(vec![Some(true), Some(false), None]);
        let document = encode(&one("flag", Arc::new(values))).unwrap();
        assert_eq!(cells(&document), ["T", "F", "<TD/>"]);
    }

    /// A string is data, and a column name is data too — both arrive out of a file
    /// someone else wrote.
    #[test]
    fn markup_in_a_value_or_a_name_is_escaped() {
        let values = StringArray::from(vec!["<TD>&</TD>"]);
        let batch =
            RecordBatch::try_from_iter_with_nullable([("a&b", Arc::new(values) as ArrayRef, true)])
                .unwrap();
        let document = encode(&result(batch)).unwrap();
        assert!(document.contains("<FIELD name=\"a&amp;b\""), "{document}");
        assert_eq!(cells(&document), ["&lt;TD&gt;&amp;&lt;/TD&gt;"]);
    }

    /// Readers trim the whitespace around a cell's text and normalize a carriage return
    /// to a newline, so the three XML does carry are written as references.
    #[test]
    fn the_whitespace_a_parser_would_change_is_written_as_a_reference() {
        let values = StringArray::from(vec!["a\tb\r\nc "]);
        let document = encode(&one("s", Arc::new(values))).unwrap();
        assert_eq!(cells(&document), ["a&#9;b&#13;&#10;c "]);
    }

    /// XML 1.0 cannot carry one at all, escaped or not, so the document would be one no
    /// reader could parse.
    #[test]
    fn a_control_character_is_refused_rather_than_written() {
        let values = StringArray::from(vec!["a\u{0}b"]);
        let refused = encode(&one("s", Arc::new(values))).unwrap_err();
        assert!(
            refused.to_string().contains("control character"),
            "{refused}"
        );
    }

    /// Not yet written, and refused by name rather than dropped from the answer.
    #[test]
    fn a_nested_column_is_refused_by_name() {
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let list = ListArray::new_null(item, 1);
        let refused = encode(&one("lightcurve", Arc::new(list))).unwrap_err();
        assert!(refused.to_string().contains("lightcurve"), "{refused}");
        assert!(refused.to_string().contains("nested"), "{refused}");

        let fields = Fields::from(vec![Field::new("mag", DataType::Float64, true)]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sources",
            DataType::Struct(fields),
            true,
        )]));
        let refused = encode(&QueryResult {
            schema,
            batches: Vec::new(),
            data_bytes_read: 0,
        })
        .unwrap_err();
        assert!(refused.to_string().contains("sources"), "{refused}");
    }

    /// The schema is the whole of what is left to say when nothing matched, and a reader
    /// still has to be able to parse the answer.
    #[test]
    fn no_rows_is_still_a_document_with_the_columns_in_it() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("objectid", DataType::Int64, false),
            Field::new("band", DataType::Utf8, true),
        ]));
        let document = encode(&QueryResult {
            schema,
            batches: Vec::new(),
            data_bytes_read: 0,
        })
        .unwrap();
        assert!(document.contains("<TABLE nrows=\"0\">"), "{document}");
        assert!(
            document.contains("<FIELD name=\"objectid\" ID=\"objectid\" datatype=\"long\"/>"),
            "{document}"
        );
        assert!(
            document.contains(
                "<FIELD name=\"band\" ID=\"band\" datatype=\"unicodeChar\" arraysize=\"*\"/>"
            ),
            "{document}"
        );
        assert!(document.contains("<TABLEDATA>\n</TABLEDATA>"), "{document}");
    }
}
