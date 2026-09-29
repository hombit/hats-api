//! Every VOTable under `tests/data/votable/`, read and held to what its writer put in it.
//!
//! Each document has a `<file>.json` beside it saying what the reader must make of it — the
//! columns as their `FIELD`s declared them and every cell — or that it must be refused. The
//! documents come from as many writers as could be found: astropy, STILTS, the CDS Rust
//! crate, hand-written edge cases, and the answers of TAP services in the wild, each of which
//! serializes with software of its own. `tests/data/votable/README.md` says which is which.
//!
//! The cell contract the JSON is written in, which is the reader's:
//!
//! - a null is `null`; `NaN` and the infinities are the strings `"NaN"`, `"Infinity"` and
//!   `"-Infinity"`;
//! - an integer is exact, and a `float` is the `f32`'s own value;
//! - a string is cut at its first NUL, and a fixed-width one is trimmed of trailing blanks;
//! - an array is a flat list of its items in file order, a complex number is `[re, im]`, a
//!   bit is a boolean, and a two-dimensional `char` is a list of strings.
//!
//! All failures are collected before the test fails, so one run says everything that is
//! wrong rather than the first thing.

use std::path::{Path, PathBuf};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt8Type,
};
use hats_api::engine::query::QueryResult;
use hats_api::output;
use hats_api::votable::{self, field};
use serde_json::Value;

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/votable")
}

/// Every document with an expectation beside it, below `dir`.
fn documents(dir: &Path, into: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().map(|entry| entry.path()).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            documents(&path, into);
        } else if let Some(name) = path.file_name().and_then(|name| name.to_str())
            && let Some(document) = name.strip_suffix(".json")
            && path.with_file_name(document).is_file()
        {
            into.push(path.with_file_name(document));
        }
    }
}

#[test]
fn every_document_in_the_corpus_reads_as_its_writer_wrote_it() {
    let mut found = Vec::new();
    documents(&corpus(), &mut found);
    // A corpus that went missing, or whose naming drifted from `<document>.json`, would
    // otherwise pass with nothing checked.
    assert!(
        found.len() >= 150,
        "only {} documents found under {}",
        found.len(),
        corpus().display()
    );
    let mut failures = Vec::new();
    for document in &found {
        let label = document
            .strip_prefix(corpus())
            .unwrap_or(document)
            .display()
            .to_string();
        if let Err(why) = check(document) {
            failures.push(format!("{label}: {why}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} documents disagree:\n{}",
        failures.len(),
        found.len(),
        failures.join("\n")
    );
}

/// Every document the reader takes, written back out as an answer and read again, comes back
/// as itself: the same columns declared the same way, the same Arrow types, the same cells.
///
/// This is the writer held to the reader over every shape a dozen writers produce, which a
/// round trip of one table through two clients cannot be. Two things are one value on the way
/// back, because an answer is TABLEDATA and TABLEDATA says them the same way (VOTable 1.5
/// §5.5): an empty string and a null, and a zero-length array and a null. The one thing the
/// writer refuses is a list of strings holding one its padding would change; any other
/// refusal fails.
#[test]
fn every_document_the_corpus_reads_writes_back_as_itself() {
    let mut found = Vec::new();
    documents(&corpus(), &mut found);
    assert!(found.len() >= 150, "only {} documents found", found.len());
    let (mut written, mut failures) = (0usize, Vec::new());
    for document in &found {
        let label = document
            .strip_prefix(corpus())
            .unwrap_or(document)
            .display()
            .to_string();
        match round_trip(document) {
            Ok(true) => written += 1,
            Ok(false) => {}
            Err(why) => failures.push(format!("{label}: {why}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} documents do not come back as themselves:\n{}",
        failures.len(),
        found.len(),
        failures.join("\n")
    );
    // Most of the corpus has to have been through the writer, or this checked little.
    assert!(written >= 100, "only {written} documents were written back");
}

/// How much of a file a url upload reads to recognise it, which is `app::uploaded`'s `HEAD`.
const HEAD: usize = 4096;

/// Every VOTable in the corpus is recognised as one from its head, and nothing else is.
///
/// A url with no `UPLOAD_TYPE` is judged by its first bytes, so a VOTable this misses is
/// opened as a catalog directory and refused as one. What sits ahead of the root in the wild —
/// DaCHS's stylesheet instruction, IRSA's DOCTYPE, MAST's comment, a BOM — is what this runs
/// into.
#[test]
fn every_votable_in_the_corpus_is_recognised_from_its_head() {
    let mut found = Vec::new();
    documents(&corpus(), &mut found);
    let mut wrong = Vec::new();
    for document in &found {
        let bytes = std::fs::read(document).unwrap();
        let head = &bytes[..bytes.len().min(HEAD)];
        let text = decoded(head);
        // Its root is a VOTABLE, in any namespace: the element name after a `<` or a
        // prefix's `:`, and before whatever ends a name.
        let rooted = ["<", ":"].iter().any(|before| {
            ["VOTABLE ", "VOTABLE>", "VOTABLE\n", "VOTABLE\r", "VOTABLE/"]
                .iter()
                .any(|name| text.contains(&format!("{before}{name}")))
        });
        if votable::is_votable(head) != rooted {
            wrong.push(format!(
                "{}: is_votable says {}, and its root {} VOTABLE",
                document.display(),
                !rooted,
                if rooted { "is" } else { "is not" }
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The head as text, UTF-16 where it has that byte-order mark.
fn decoded(head: &[u8]) -> String {
    match head {
        [0xFF, 0xFE, rest @ ..] => String::from_utf16_lossy(
            &rest
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>(),
        ),
        [0xFE, 0xFF, rest @ ..] => String::from_utf16_lossy(
            &rest
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u16::from_be_bytes(*pair))
                .collect::<Vec<_>>(),
        ),
        _ => String::from_utf8_lossy(head).into_owned(),
    }
}

/// No document in the corpus, cut short anywhere or with a byte of it changed, makes the
/// reader panic.
///
/// An upload is somebody else's bytes, and a panic in the reader is a request that takes a
/// worker down rather than a `400`. Every document is cut at forty points and corrupted at
/// forty — in its markup, its TABLEDATA and its base64, which is a BINARY stream's bytes
/// changed — and each reading has to come back as a table or a refusal.
#[test]
fn no_document_cut_or_corrupted_makes_the_reader_panic() {
    let mut found = Vec::new();
    documents(&corpus(), &mut found);
    let mut panicked = Vec::new();
    for document in &found {
        let bytes = std::fs::read(document).unwrap();
        let step = (bytes.len() / 40).max(1);
        let mut variants = Vec::new();
        for at in (0..bytes.len()).step_by(step) {
            variants.push((format!("cut at {at}"), bytes[..at].to_vec()));
            let mut changed = bytes.clone();
            // Within ASCII, and not the byte it was, so a base64 character stays a character
            // and becomes a different one.
            changed[at] = match changed[at] {
                b'A' => b'z',
                b'0'..=b'9' => b'Q',
                _ => b'A',
            };
            variants.push((format!("byte {at} changed"), changed));
        }
        for (how, variant) in variants {
            let read = std::panic::catch_unwind(|| votable::read(&variant).map(|_| ()));
            if read.is_err() {
                panicked.push(format!("{}: {how}", document.display()));
            }
        }
    }
    assert!(panicked.is_empty(), "{}", panicked.join("\n"));
}

/// `Ok(true)` where the document went round, `Ok(false)` where there was nothing to send
/// round — a document the reader refuses, or one holding a list of strings.
fn round_trip(document: &Path) -> Result<bool, String> {
    let bytes = std::fs::read(document).map_err(|error| error.to_string())?;
    let Ok(table) = votable::read(&bytes) else {
        return Ok(false);
    };
    let first = concat_batches(&table.schema, &table.batches).map_err(|error| error.to_string())?;
    let result = QueryResult {
        schema: table.schema.clone(),
        batches: table.batches,
        data_bytes_read: 0,
    };
    let answer = match output::votable::encode(&result) {
        Ok(answer) => answer,
        Err(refused)
            if holds_a_list_of_strings(&first)
                && refused
                    .to_string()
                    .contains("fixed-width strings cannot carry") =>
        {
            return Ok(false);
        }
        Err(refused) => return Err(format!("the writer refused it: {refused}")),
    };
    let again = votable::read(answer.as_bytes())
        .map_err(|error| format!("the reader refused the writer's answer: {error}"))?;
    let second =
        concat_batches(&again.schema, &again.batches).map_err(|error| error.to_string())?;
    same_table(&first, &second)?;
    Ok(true)
}

fn holds_a_list_of_strings(batch: &RecordBatch) -> bool {
    batch.schema().fields().iter().any(|field| {
        matches!(
            field.data_type(),
            DataType::List(item) | DataType::FixedSizeList(item, _)
                if item.data_type() == &DataType::Utf8
        )
    })
}

/// The two readings of one table agree: names, types, what each FIELD declared, and cells.
fn same_table(first: &RecordBatch, second: &RecordBatch) -> Result<(), String> {
    let (one, two) = (first.schema(), second.schema());
    if one.fields().len() != two.fields().len() || first.num_rows() != second.num_rows() {
        return Err(format!(
            "{} columns and {} rows became {} and {}",
            one.fields().len(),
            first.num_rows(),
            two.fields().len(),
            second.num_rows()
        ));
    }
    let mut wrong = Vec::new();
    for (at, (before, after)) in one.fields().iter().zip(two.fields()).enumerate() {
        let name = before.name();
        if before.name() != after.name() {
            wrong.push(format!("column {name} came back as {}", after.name()));
            continue;
        }
        if !same_type(before.data_type(), after.data_type()) {
            wrong.push(format!(
                "column {name} was {} and came back {}",
                before.data_type(),
                after.data_type()
            ));
            continue;
        }
        for key in [
            field::DATATYPE,
            field::ARRAYSIZE,
            field::XTYPE,
            field::UNIT,
            field::UCD,
            field::UTYPE,
            field::DESCRIPTION,
        ] {
            let (was, is) = (said(before, key), said(after, key));
            // A one-character string is declared without an arraysize, and `1` is the
            // deprecated spelling of the same thing; a text arraysize of any other shape is
            // the writer's `*`. What has to agree is what the column holds, which the type
            // comparison above has already asked.
            if key == field::ARRAYSIZE && before.data_type() == &DataType::Utf8 {
                continue;
            }
            if was != is {
                wrong.push(format!("column {name}: {key} {was:?} came back {is:?}"));
            }
        }
        // A magic value means something only for an integer, and is only written for one.
        let integer = matches!(
            said(before, field::DATATYPE).as_deref(),
            Some("unsignedByte" | "short" | "int" | "long")
        );
        if integer && said(before, field::NULL) != said(after, field::NULL) {
            wrong.push(format!("column {name}: its VALUES null did not come back"));
        }
        let (a, b) = (first.column(at), second.column(at));
        for row in 0..first.num_rows() {
            let (was, is) = (tabledata_value(cell(a, row)), tabledata_value(cell(b, row)));
            if !same(&was, &is, holds_f32(a.data_type())) {
                wrong.push(format!(
                    "row {}, column {name}: {was} came back {is}",
                    row + 1
                ));
                break;
            }
        }
    }
    match wrong.is_empty() {
        true => Ok(()),
        false => Err(wrong.join("; ")),
    }
}

/// The same Arrow type, list items being nullable or not on either side.
fn same_type(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::List(x), DataType::List(y)) => same_type(x.data_type(), y.data_type()),
        (DataType::FixedSizeList(x, n), DataType::FixedSizeList(y, m)) => {
            n == m && same_type(x.data_type(), y.data_type())
        }
        _ => a == b,
    }
}

fn said(field: &datafusion::arrow::datatypes::Field, key: &str) -> Option<String> {
    field
        .metadata()
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
}

/// A cell as TABLEDATA can say it: an empty string and an empty array are a null there.
fn tabledata_value(value: Value) -> Value {
    match &value {
        Value::String(text) if text.is_empty() => Value::Null,
        Value::Array(items) if items.is_empty() => Value::Null,
        _ => value,
    }
}

fn check(document: &Path) -> Result<(), String> {
    let bytes = std::fs::read(document).map_err(|error| error.to_string())?;
    let expected: Value = serde_json::from_slice(
        &std::fs::read(format!("{}.json", document.display()))
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("the expectation is not JSON: {error}"))?;
    let read = votable::read(&bytes);
    if let Some(why) = expected.get("refuse").and_then(Value::as_str) {
        return match read {
            Err(_) => Ok(()),
            Ok(_) => Err(format!("read, and should have been refused ({why})")),
        };
    }
    let table = read.map_err(|error| format!("refused: {error}"))?;
    let batch = concat_batches(&table.schema, &table.batches).map_err(|error| error.to_string())?;
    columns(&batch, &expected)?;
    rows(&batch, &expected)
}

/// The columns as their `FIELD`s declared them.
fn columns(batch: &RecordBatch, expected: &Value) -> Result<(), String> {
    let declared = expected
        .get("columns")
        .and_then(Value::as_array)
        .ok_or("the expectation has no columns")?;
    let schema = batch.schema();
    if declared.len() != schema.fields().len() {
        return Err(format!(
            "{} columns, expected {}",
            schema.fields().len(),
            declared.len()
        ));
    }
    let mut wrong = Vec::new();
    for (field, want) in schema.fields().iter().zip(declared) {
        let name = want.get("name").and_then(Value::as_str).unwrap_or_default();
        if field.name() != name {
            wrong.push(format!("column {:?} is called {:?}", name, field.name()));
            continue;
        }
        for (key, metadata) in [
            ("datatype", field::DATATYPE),
            ("arraysize", field::ARRAYSIZE),
            ("xtype", field::XTYPE),
            ("unit", field::UNIT),
            ("ucd", field::UCD),
            ("utype", field::UTYPE),
            ("null", field::NULL),
        ] {
            let want = want
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty());
            let got = field
                .metadata()
                .get(metadata)
                .map(String::as_str)
                .filter(|value| !value.is_empty());
            if want != got {
                wrong.push(format!(
                    "column {name}: {key} is {got:?}, expected {want:?}"
                ));
            }
        }
    }
    match wrong.is_empty() {
        true => Ok(()),
        false => Err(wrong.join("; ")),
    }
}

/// Every cell, against the contract.
fn rows(batch: &RecordBatch, expected: &Value) -> Result<(), String> {
    let rows = expected
        .get("rows")
        .and_then(Value::as_array)
        .ok_or("the expectation has no rows")?;
    if rows.len() != batch.num_rows() {
        return Err(format!(
            "{} rows, expected {}",
            batch.num_rows(),
            rows.len()
        ));
    }
    let schema = batch.schema();
    let mut wrong = Vec::new();
    for (at, want) in rows.iter().enumerate() {
        let want = want.as_array().ok_or("a row is not a list")?;
        for (column, (array, want)) in batch.columns().iter().zip(want).enumerate() {
            let got = cell(array, at);
            if !same(&got, want, holds_f32(array.data_type())) {
                wrong.push(format!(
                    "row {}, column {}: {got}, expected {want}",
                    at + 1,
                    schema.field(column).name()
                ));
            }
        }
        // Enough to see what is wrong without burying it.
        if wrong.len() > 12 {
            break;
        }
    }
    match wrong.is_empty() {
        true => Ok(()),
        false => Err(wrong.join("; ")),
    }
}

/// One cell as the contract writes it.
fn cell(array: &ArrayRef, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).into(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).into(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).into(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).into(),
        DataType::Float32 => float(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_owned()),
        DataType::List(_) => items(&array.as_list::<i32>().value(row)),
        DataType::FixedSizeList(_, _) => items(&array.as_fixed_size_list().value(row)),
        other => Value::String(format!("<{other} is not in the contract>")),
    }
}

fn items(values: &ArrayRef) -> Value {
    Value::Array((0..values.len()).map(|at| cell(values, at)).collect())
}

fn float(value: f64) -> Value {
    if value.is_nan() {
        Value::String("NaN".to_owned())
    } else if value == f64::INFINITY {
        Value::String("Infinity".to_owned())
    } else if value == f64::NEG_INFINITY {
        Value::String("-Infinity".to_owned())
    } else {
        serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

/// Two doubles a few units in the last place apart at most.
///
/// Not a tolerance on the reader: a `BINARY` double is its own bits and a `TABLEDATA` one is
/// parsed correctly rounded. It is on the expectations, which the tools that wrote them print
/// at a precision that can land an ulp or two from the value — astropy's `TABLEDATA` parser is
/// not correctly rounded, and STILTS prints sixteen significant digits.
fn ulps_apart(got: f64, want: f64) -> bool {
    got == want || (got - want).abs() <= 4.0 * f64::EPSILON * got.abs().max(want.abs())
}

/// Whether a column's numbers are `float`s, which are compared at that width.
fn holds_f32(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 => true,
        DataType::List(item) | DataType::FixedSizeList(item, _) => holds_f32(item.data_type()),
        _ => false,
    }
}

/// Equal, a number being equal to a number of the same value however it was written — `1`
/// and `1.0` are one double — and a `float` being equal to any double that rounds to it,
/// since the tools that wrote the expectations print an `f32` through a decimal string that
/// may land an ulp of a double away from the `f32`'s exact value.
fn same(got: &Value, want: &Value, f32_wide: bool) -> bool {
    match (got, want) {
        (Value::Number(got), Value::Number(want)) => match (got.as_i64(), want.as_i64()) {
            (Some(got), Some(want)) => got == want,
            _ if f32_wide => {
                got.as_f64().map(|value| value as f32) == want.as_f64().map(|value| value as f32)
            }
            _ => match (got.as_f64(), want.as_f64()) {
                (Some(got), Some(want)) => ulps_apart(got, want),
                _ => false,
            },
        },
        (Value::Array(got), Value::Array(want)) => {
            got.len() == want.len()
                && got
                    .iter()
                    .zip(want)
                    .all(|(got, want)| same(got, want, f32_wide))
        }
        _ => got == want,
    }
}
