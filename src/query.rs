//! Reading rows out of a single parquet file: a projection, a row predicate and a row
//! cap, each of which the caller may leave out. Says nothing about where the data lives.
//!
//! Nothing is cached between requests: every call builds its own session, its own
//! object store, and reads the file's metadata from scratch.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

use crate::error::ApiError;
use crate::sql;
use crate::storage::RemoteFile;

/// Which columns come back, and in whose vocabulary the caller asked.
#[derive(Debug, Default, Clone, Copy)]
pub enum Projection<'a> {
    /// Every column the file has.
    #[default]
    All,
    /// A SQL select list — `objectid, lightcurve.mag AS mag`.
    Select(&'a str),
    /// Comma-separated column names, and no expressions.
    Columns(&'a str),
}

/// Which rows come back, and in whose vocabulary the caller asked.
#[derive(Debug, Default, Clone, Copy)]
pub enum Predicate<'a> {
    /// Every row.
    #[default]
    All,
    /// One boolean SQL expression over this file's columns.
    Where(&'a str),
    /// The same, with `&&` accepted for `AND`.
    Filters(&'a str),
}

/// What to read. The two fields are each one of two spellings, and the request shape is
/// what refuses a caller who sent both — by the time it is here, one has been chosen.
#[derive(Debug, Default)]
pub struct Selection<'a> {
    pub projection: Projection<'a>,
    pub predicate: Predicate<'a>,
    /// Most rows to return. `None` is however many match.
    pub limit: Option<usize>,
}

/// The rows a [`Selection`] matched, plus the schema they have — which is the
/// projection's schema, not the file's, and is the only thing left to describe the
/// result when no row matched at all.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// How many rows matched, across every batch.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

/// The shape of the answer, not the answer. A derived `Debug` would print every row,
/// so a result that turned up in a log line or a panic message would be the query
/// result itself — which is the caller's data, and can be millions of values.
impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("schema", &self.schema)
            .field("num_batches", &self.batches.len())
            .field("num_rows", &self.num_rows())
            .finish()
    }
}

/// Everything we know about parquet point lookups, in one place.
///
/// None of it assumes anything about the file: DataFusion uses a page index or a bloom
/// filter when the file happens to have one, and falls back to row-group statistics
/// when it does not.
pub(crate) fn session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    // `sql::resolve_identifiers` decides which spellings of a column name reach it, and
    // it can only do that if nothing else is folding case behind it: with normalization
    // on, DataFusion lowercases whatever the pass left alone, so a name the rule refuses
    // would find its column anyway.
    options.sql_parser.enable_ident_normalization = false;
    let parquet = &mut options.execution.parquet;
    // Off by default, and worth more than everything else combined: it evaluates the
    // predicate during decoding, so the data columns of non-matching row groups and
    // pages are never fetched.
    parquet.pushdown_filters = true;
    parquet.reorder_filters = true;
    // On by default; set explicitly because the whole service depends on them.
    parquet.pruning = true;
    parquet.enable_page_index = true;
    parquet.bloom_filter_on_read = true;
    config
}

pub async fn run(
    file: &RemoteFile,
    selection: &Selection<'_>,
    limits: sql::Limits,
) -> Result<QueryResult, ApiError> {
    let ctx = SessionContext::new_with_config(session_config());
    ctx.register_object_store(&file.base, Arc::clone(&file.store));

    // The url names one object, and it has already been decided that this is a parquet
    // file — by the magic bytes under a mount, and by the caller naming it in API mode.
    // `ParquetReadOptions` otherwise filters on the name ending in `.parquet` and reports
    // anything else as an execution failure, which would put a HATS catalog's `_metadata`
    // and `_common_metadata` out of reach: both are parquet files with no extension.
    let options = ParquetReadOptions {
        file_extension: "",
        ..ParquetReadOptions::default()
    };
    let df = ctx.read_parquet(file.url.as_str(), options).await?;
    let state = ctx.state();

    // The predicate first, so it may name a column the projection does not return —
    // filtering on `filterid` while asking only for `mag` is the ordinary case.
    let df = match selection.predicate {
        Predicate::All => df,
        Predicate::Where(sql) => {
            let expr = sql::predicate(&state, df.schema(), sql, limits)?;
            df.filter(expr)?
        }
        Predicate::Filters(text) => {
            let expr = sql::filters(&state, df.schema(), text, limits)?;
            df.filter(expr)?
        }
    };
    let df = match selection.projection {
        Projection::All => df,
        Projection::Select(sql) => {
            let exprs = sql::projection(&state, df.schema(), sql, limits)?;
            df.select(exprs)?
        }
        Projection::Columns(list) => {
            let exprs = sql::columns(&state, df.schema(), list, limits)?;
            df.select(exprs)?
        }
    };
    let df = match selection.limit {
        Some(rows) => df.limit(0, Some(rows))?,
        None => df,
    };

    let schema = Arc::new(df.schema().as_arrow().clone());
    Ok(QueryResult {
        schema,
        batches: df.collect().await?,
    })
}

/// Serialize the result as a JSON array of row objects, nested columns included.
pub fn to_json(result: &QueryResult) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut buf = Vec::new();
    let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
    for batch in &result.batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    if buf.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_slice(&buf)?)
}

#[cfg(test)]
pub(crate) mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;

    use super::*;

    /// Ten rows with an id and a band, as a parquet file. Small on purpose: what the
    /// tests using it are about is which rows and columns came back, not how they were
    /// fetched.
    pub(crate) fn fixture() -> Vec<u8> {
        fixture_of(10)
    }

    /// The same file with a chosen number of rows, for a test that needs the data to be
    /// large next to the footer rather than the other way round.
    pub(crate) fn fixture_of(rows: i64) -> Vec<u8> {
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
        let band: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| if i % 2 == 0 { "g" } else { "r" }),
        ));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("band", band, true),
        ])
        .expect("the fixture batch");

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("a writer");
        writer.write(&batch).expect("write the batch");
        writer.close().expect("close the file");
        buffer
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
        };
        assert_eq!(to_json(&empty).unwrap(), Vec::<serde_json::Value>::new());
    }
}
