//! The one query shape we support: `column == value` in a single parquet file.
//!
//! Nothing is cached between requests: every call builds its own session, its own
//! object store, and reads the file's metadata from scratch.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::{Expr, lit};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

use crate::error::ApiError;
use crate::storage::RemoteFile;

/// A single-value lookup: `filter_column = filter_value`, returning `columns`.
/// Says nothing about where the data lives.
#[derive(Debug)]
pub struct Selection<'a> {
    pub filter_column: &'a str,
    pub filter_value: &'a str,
    /// Columns to return, as dotted paths into nested structs
    /// (`lightcurve.mag`). `None` returns every column.
    pub columns: Option<&'a [String]>,
}

/// The rows a [`Selection`] matched, plus the schema they have — which is the
/// projection's schema, not the file's, and is the only thing left to describe the
/// result when no row matched at all.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// The shape of the answer, not the answer. A derived `Debug` would print every row,
/// so a result that turned up in a log line or a panic message would be the query
/// result itself — which is the caller's data, and can be millions of values.
impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("schema", &self.schema)
            .field("num_batches", &self.batches.len())
            .field(
                "num_rows",
                &self
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
            )
            .finish()
    }
}

/// Everything we know about parquet point lookups, in one place.
///
/// None of it assumes anything about the file: DataFusion uses a page index or a bloom
/// filter when the file happens to have one, and falls back to row-group statistics
/// when it does not.
fn session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    let parquet = &mut config.options_mut().execution.parquet;
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

    let data_type = top_level_field(df.schema(), selection.filter_column)?
        .data_type()
        .clone();
    let scalar = parse_scalar(&data_type, selection.filter_value)?;
    let predicate = Expr::Column(Column::new_unqualified(selection.filter_column)).eq(lit(scalar));

    let df = df.filter(predicate)?;
    let df = match selection.columns {
        Some(paths) => {
            let exprs = column_exprs(df.schema(), paths)?;
            df.select(exprs)?
        }
        None => df,
    };
    let schema = Arc::new(df.schema().as_arrow().clone());
    Ok(QueryResult {
        schema,
        batches: df.collect().await?,
    })
}

fn top_level_field<'a>(
    schema: &'a DFSchema,
    name: &str,
) -> Result<&'a Arc<datafusion::arrow::datatypes::Field>, ApiError> {
    schema.field_with_unqualified_name(name).map_err(|_| {
        ApiError::bad_request(format!(
            "column {:?} not found; file has {}",
            name,
            column_names(schema.fields())
        ))
    })
}

/// Resolve the requested dotted column paths into expressions.
///
/// `objectid` is a plain column; `lightcurve.mag` reaches one leaf of a struct column,
/// which is the whole point — DataFusion pushes that down to the parquet leaf instead
/// of reading every field of the struct. Each expression is aliased to the path the
/// caller wrote, so the JSON keys are the ones they asked for.
fn column_exprs(schema: &DFSchema, paths: &[String]) -> Result<Vec<Expr>, ApiError> {
    paths.iter().map(|path| column_expr(schema, path)).collect()
}

fn column_expr(schema: &DFSchema, path: &str) -> Result<Expr, ApiError> {
    // A top-level column whose own name contains a dot wins over a nested reading.
    if schema.field_with_unqualified_name(path).is_ok() {
        return Ok(Expr::Column(Column::new_unqualified(path)));
    }
    let (head, rest) = path.split_once('.').unwrap_or((path, ""));
    let field = top_level_field(schema, head)?;
    let mut data_type = field.data_type().clone();
    let mut expr = Expr::Column(Column::new_unqualified(head));

    for segment in rest.split('.').filter(|s| !s.is_empty()) {
        let DataType::Struct(fields) = &data_type else {
            return Err(ApiError::bad_request(format!(
                "cannot select {segment:?} from {path:?}: it is of type {data_type}, not a struct"
            )));
        };
        let child = fields
            .iter()
            .find(|field| field.name() == segment)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "{path:?} not found; {head:?} has fields {}",
                    column_names(fields)
                ))
            })?;
        data_type = child.data_type().clone();
        expr = get_field(expr, segment);
    }
    Ok(expr.alias(path))
}

fn column_names(fields: &datafusion::arrow::datatypes::Fields) -> String {
    fields
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Parse the value into the column's own type.
///
/// A typed literal is what makes statistics, page index and bloom filter pruning
/// possible; comparing everything as a string would silently read the whole file.
fn parse_scalar(data_type: &DataType, raw: &str) -> Result<ScalarValue, ApiError> {
    fn parse<T: std::str::FromStr>(raw: &str, data_type: &DataType) -> Result<T, ApiError> {
        raw.parse::<T>().map_err(|_| {
            ApiError::bad_request(format!(
                "value {raw:?} is not valid for column of type {data_type}"
            ))
        })
    }

    Ok(match data_type {
        DataType::Boolean => ScalarValue::Boolean(Some(parse(raw, data_type)?)),
        DataType::Int8 => ScalarValue::Int8(Some(parse(raw, data_type)?)),
        DataType::Int16 => ScalarValue::Int16(Some(parse(raw, data_type)?)),
        DataType::Int32 => ScalarValue::Int32(Some(parse(raw, data_type)?)),
        DataType::Int64 => ScalarValue::Int64(Some(parse(raw, data_type)?)),
        DataType::UInt8 => ScalarValue::UInt8(Some(parse(raw, data_type)?)),
        DataType::UInt16 => ScalarValue::UInt16(Some(parse(raw, data_type)?)),
        DataType::UInt32 => ScalarValue::UInt32(Some(parse(raw, data_type)?)),
        DataType::UInt64 => ScalarValue::UInt64(Some(parse(raw, data_type)?)),
        DataType::Float32 => ScalarValue::Float32(Some(parse(raw, data_type)?)),
        DataType::Float64 => ScalarValue::Float64(Some(parse(raw, data_type)?)),
        DataType::Utf8 => ScalarValue::Utf8(Some(raw.to_owned())),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(raw.to_owned())),
        DataType::Utf8View => ScalarValue::Utf8View(Some(raw.to_owned())),
        other => {
            return Err(ApiError::bad_request(format!(
                "filtering on columns of type {other} is not supported yet"
            )));
        }
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
    use datafusion::arrow::datatypes::{Field, Fields, Schema};

    use super::*;

    /// A cut-down version of the ZTF HATS schema: scalars plus a struct of lists.
    fn test_schema() -> DFSchema {
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
            Field::new("objra", DataType::Float32, true),
            Field::new("lightcurve", lightcurve, true),
        ]))
        .unwrap()
    }

    fn output_names(paths: &[&str]) -> Result<Vec<String>, ApiError> {
        let paths: Vec<String> = paths.iter().map(|p| (*p).to_owned()).collect();
        Ok(column_exprs(&test_schema(), &paths)?
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect())
    }

    #[test]
    fn returns_plain_and_nested_columns_under_the_requested_names() {
        assert_eq!(
            output_names(&["objectid", "lightcurve.mag", "objra"]).unwrap(),
            vec!["objectid", "lightcurve.mag", "objra"]
        );
    }

    #[test]
    fn reports_the_available_columns_for_an_unknown_top_level_name() {
        let error = output_names(&["nope"]).unwrap_err();
        assert!(error.to_string().contains("not found"), "{error}");
        assert!(error.to_string().contains("objectid"), "{error}");
    }

    #[test]
    fn reports_the_available_fields_for_an_unknown_struct_field() {
        let error = output_names(&["lightcurve.nope"]).unwrap_err();
        assert!(error.to_string().contains("mag, magerr"), "{error}");
    }

    #[test]
    fn refuses_to_walk_into_something_that_is_not_a_struct() {
        let error = output_names(&["objra.x"]).unwrap_err();
        assert!(error.to_string().contains("not a struct"), "{error}");
        // A list of scalars has no named fields to reach either.
        let error = output_names(&["lightcurve.mag.element"]).unwrap_err();
        assert!(error.to_string().contains("not a struct"), "{error}");
    }

    #[test]
    fn parses_values_into_the_column_type() {
        assert_eq!(
            parse_scalar(&DataType::Int64, "3445524782181585918").unwrap(),
            ScalarValue::Int64(Some(3445524782181585918))
        );
        assert_eq!(
            parse_scalar(&DataType::Utf8, "ZTF18abc").unwrap(),
            ScalarValue::Utf8(Some("ZTF18abc".to_owned()))
        );
        assert_eq!(
            parse_scalar(&DataType::Float32, "1.5").unwrap(),
            ScalarValue::Float32(Some(1.5))
        );
    }

    #[test]
    fn rejects_values_that_do_not_fit_the_column() {
        // An int64 column must not silently accept a float, or a value that overflows.
        let error = parse_scalar(&DataType::Int64, "1.5").unwrap_err();
        assert!(error.to_string().contains("not valid"), "{error}");
        let error = parse_scalar(&DataType::Int32, "3445524782181585918").unwrap_err();
        assert!(error.to_string().contains("not valid"), "{error}");
    }

    #[test]
    fn rejects_unsupported_column_types() {
        let error = parse_scalar(&DataType::Binary, "x").unwrap_err();
        assert!(error.to_string().contains("not supported yet"), "{error}");
    }

    #[test]
    fn empty_result_serializes_as_an_empty_array() {
        let empty = QueryResult {
            schema: Arc::new(test_schema().as_arrow().clone()),
            batches: Vec::new(),
        };
        assert_eq!(to_json(&empty).unwrap(), Vec::<serde_json::Value>::new());
    }
}
