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

/// What to read, in the caller's own SQL.
#[derive(Debug, Default)]
pub struct Selection<'a> {
    /// A SQL select list — `objectid, lightcurve.mag AS mag`. `None` is every column.
    pub select: Option<&'a str>,
    /// One boolean SQL expression over this file's columns. `None` is every row.
    pub predicate: Option<&'a str>,
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
    // Astronomy column names are mixed-case as a matter of course — `Gmag`, `Norder`,
    // `objectId`. Normalizing an unquoted identifier to lowercase, which is DataFusion's
    // default and ordinary SQL's rule, would report every one of them as missing, and
    // the caller would have to know to quote a name they can see in the file.
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

pub async fn run(file: &RemoteFile, selection: &Selection<'_>) -> Result<QueryResult, ApiError> {
    let ctx = SessionContext::new_with_config(session_config());
    ctx.register_object_store(&file.base, Arc::clone(&file.store));

    let df = ctx
        .read_parquet(file.url.as_str(), ParquetReadOptions::default())
        .await?;
    let state = ctx.state();

    // The predicate first, so it may name a column the projection does not return —
    // filtering on `filterid` while asking only for `mag` is the ordinary case.
    let df = match selection.predicate {
        Some(sql) => {
            let expr = sql::predicate(&state, df.schema(), sql)?;
            df.filter(expr)?
        }
        None => df,
    };
    let df = match selection.select {
        Some(sql) => {
            let exprs = sql::projection(&state, df.schema(), sql)?;
            df.select(exprs)?
        }
        None => df,
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
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

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
