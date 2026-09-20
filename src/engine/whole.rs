//! Whether a query would hand back the file it was asked about.
//!
//! **This is what an `lsdb` client sends for a partition it has not projected.** The url
//! it asks for names every column of the file and a predicate that is the partition's own
//! HEALPix cell bounds — a request that narrows nothing, spelled as a query, because that
//! is the only spelling the client has. Answered as a query it is a full read and a full
//! re-encode of the partition, repeated for each of the three requests a parquet reader
//! makes; answered as the file it is the bytes, ranged, with nothing read but the footer.
//! For a partition carrying a nested column the difference is minutes against seconds.
//!
//! What is decided here is only *whether the answer would be the file*. Nothing about the
//! bytes changes: the caller gets the same rows and the same columns, in the file's own
//! layout rather than in a fresh one.
//!
//! Three conditions, and the third is the one with teeth:
//!
//! - **No `limit`, no region, and parquet out.** Each of those makes the answer something
//!   other than the file.
//! - **The projection is the file's own columns**, as a set. Order is not required to
//!   match: a client that asked for the columns in another order gets a parquet file whose
//!   schema is in the file's, which every reader of one addresses by name. Requiring the
//!   order would turn this off for exactly the request it exists for — `lsdb` appends the
//!   index column to a list that begins with the catalog's own order, and a HATS partition
//!   holds it first.
//! - **The predicate matches every row**, proved against the footer rather than by reading
//!   rows: a `PruningPredicate` over the *negation* of the filter, asked of the row-group
//!   statistics. If no row group can hold a row satisfying `NOT filter`, every row in the
//!   file satisfies `filter`.
//!
//! The null check is what makes that third proof sound. Row-group statistics describe the
//! values that are there, so a column of nulls has whatever min and max the non-null rows
//! had — and `null >= 0` is null, which is not true, so those rows would *not* come back
//! from the query while the file carries them. A column the predicate names is therefore
//! required to have no nulls anywhere in the file.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::common::{Column, DFSchema};
use datafusion::execution::SessionState;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::{PruningPredicateBuilder, PruningStatistics};
use datafusion::scalar::ScalarValue;

use crate::engine::query::{Predicate, Projection, Selection};
use crate::engine::sql;

/// Whether answering `selection` against this file would return the file itself.
///
/// Conservative in one direction only: everything it cannot prove is `false`, which costs
/// a query that need not have run. It is never `true` for a request whose answer would
/// differ from the file in any row, column or value.
pub fn answers_with_the_file(
    selection: &Selection<'_>,
    metadata: &ParquetMetaData,
    limits: sql::Limits,
) -> bool {
    // Why a query ran is worth being able to ask, since the answer is the difference
    // between reading a footer and re-encoding a partition — and nothing in a correct
    // answer says which happened.
    if selection.limit.is_some() || selection.spatial.is_some() {
        tracing::debug!("a query: it narrows by a limit or a region");
        return false;
    }
    let Some(schema) = file_schema(metadata) else {
        tracing::debug!("a query: this file's schema will not convert");
        return false;
    };
    // This crate's own context, not a default one. `enable_ident_normalization` is off in
    // it, and with the default on, `OBJID` is lowercased to `objid` and planning fails
    // against a file whose column is spelled in capitals — every astronomy catalog. The
    // decision has to plan the way the query plans, or it answers about a different query.
    let context = crate::engine::query::session_context(true);
    let state = context.state();
    let Ok(df_schema) = DFSchema::try_from(Arc::clone(&schema)) else {
        tracing::debug!("a query: this file's schema will not convert");
        return false;
    };

    if !every_column(&state, &df_schema, selection, limits) {
        tracing::debug!("a query: the projection is not the file's own columns");
        return false;
    }

    let filter = match selection.predicate {
        Predicate::All => return true,
        Predicate::Filters(text) => sql::filters(&state, &df_schema, text, limits),
        Predicate::FilterText(text) => sql::filter_text(&state, &df_schema, text, limits),
    };
    // A predicate that does not parse is the caller's error and it is the query's to
    // report: answering with the file would turn a `400` into a download.
    let Ok(filter) = filter else {
        tracing::debug!("a query: the predicate does not plan against this file");
        return false;
    };
    let keeps = keeps_every_row(&filter, &schema, &df_schema, metadata);
    if !keeps {
        tracing::debug!("a query: the predicate is not provably true of every row");
    }
    keeps
}

/// The file's schema, as arrow reads it.
fn file_schema(metadata: &ParquetMetaData) -> Option<SchemaRef> {
    let file = metadata.file_metadata();
    datafusion::parquet::arrow::parquet_to_arrow_schema(
        file.schema_descr(),
        file.key_value_metadata(),
    )
    .ok()
    .map(Arc::new)
}

/// Whether the projection would return the file's own schema.
///
/// Compared as the *fields the projection produces*, not as the names the caller wrote,
/// and that is what makes it work for the request this exists for. `lsdb` does not ask for
/// a nested column by name: it writes one dotted path per field inside it —
/// `spectra.flux`, `spectra.ivar`, and four more — which this service packs back into one
/// `spectra`. Comparing names would see six things the file has not got; comparing the
/// packed field against the file's says they are the same column.
///
/// Name and type, so a column reordered is still the file and a column narrowed is not:
/// a struct missing one of its fields has a different type, which is what a partial pack
/// shows up as.
fn every_column(
    state: &SessionState,
    schema: &DFSchema,
    selection: &Selection<'_>,
    limits: sql::Limits,
) -> bool {
    let exprs = match selection.projection {
        Projection::All => return true,
        Projection::Columns(names) => sql::columns(state, schema, names, limits),
        Projection::ColumnText(text) => sql::column_text(state, schema, text, limits),
    };
    let exprs = match exprs {
        Ok(exprs) => exprs,
        Err(error) => {
            tracing::debug!(%error, "the projection does not plan against this file");
            return false;
        }
    };
    let mut projected: Vec<(String, DataType)> = Vec::with_capacity(exprs.len());
    for expr in &exprs {
        let Ok((_, field)) = expr.to_field(schema) else {
            tracing::debug!(%expr, "this projected column has no field");
            return false;
        };
        projected.push((field.name().clone(), shape(field.data_type())));
    }
    let mut held: Vec<(String, DataType)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), shape(field.data_type())))
        .collect();
    projected.sort_by(|left, right| left.0.cmp(&right.0));
    held.sort_by(|left, right| left.0.cmp(&right.0));
    // The first column that differs, rather than both schemas: one name and two types is
    // what says why a partition was re-encoded, and a catalog's schema is hundreds of
    // lines of log otherwise.
    if projected != held {
        let differs = projected
            .iter()
            .zip(&held)
            .find(|(mine, theirs)| mine != theirs);
        tracing::debug!(
            projected = projected.len(),
            held = held.len(),
            column = differs.map(|(mine, _)| mine.0.as_str()).unwrap_or("-"),
            asked = ?differs.map(|(mine, _)| &mine.1),
            file = ?differs.map(|(_, theirs)| &theirs.1),
            "the projection does not reproduce this file"
        );
    }
    projected == held
}

/// A type with every nullability claim dropped, all the way down.
///
/// Packing a struct back together from the names that reach into it marks its fields
/// nullable, where the file says they are not — the same values under a looser promise.
/// Compared literally, every nested column would read as a difference, and that is the
/// only difference there is: what gets served is the file, whose schema is the stricter
/// of the two.
fn shape(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(loose).collect()),
        DataType::List(field) => DataType::List(loose(field)),
        DataType::LargeList(field) => DataType::LargeList(loose(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(loose(field), *size),
        DataType::Map(field, sorted) => DataType::Map(loose(field), *sorted),
        other => other.clone(),
    }
}

fn loose(field: &FieldRef) -> FieldRef {
    Arc::new(Field::new(field.name(), shape(field.data_type()), true))
}

/// Whether every row of the file satisfies `filter`, judged from the footer alone.
fn keeps_every_row(
    filter: &Expr,
    schema: &SchemaRef,
    df_schema: &DFSchema,
    metadata: &ParquetMetaData,
) -> bool {
    // A column the predicate names must have no nulls: statistics describe the values that
    // are there, and a null satisfies no comparison, so a file with one would hand back
    // more rows than the query would.
    for name in filter.column_refs() {
        let Ok(index) = schema.index_of(name.name()) else {
            return false;
        };
        let nulls = metadata.row_groups().iter().map(|group| {
            group
                .column(index)
                .statistics()
                .and_then(|s| s.null_count_opt())
        });
        if !nulls.into_iter().all(|count| count == Some(0)) {
            return false;
        }
    }

    // Simplified, not merely wrapped in a `NOT`. Pruning works on comparisons against
    // row-group statistics, and it makes nothing of a negation sitting on top of one: the
    // simplifier is what turns `NOT (h >= lo AND h < hi)` into the comparisons
    // `h < lo OR h >= hi` that statistics can answer.
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(df_schema.clone()))
        .build();
    let Ok(negated) = ExprSimplifier::new(context).simplify(Expr::Not(Box::new(filter.clone())))
    else {
        return false;
    };
    let Ok(physical) = create_physical_expr(
        &negated,
        df_schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    ) else {
        return false;
    };
    let Some(pruning) = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(schema))
        .build(physical)
    else {
        // The negation is trivially true, or is one the builder cannot use. Either way
        // nothing has been proved.
        return false;
    };
    let statistics = RowGroups {
        schema: Arc::clone(schema),
        metadata,
    };
    // Every row group refused: no row anywhere in the file satisfies `NOT filter`.
    pruning
        .prune(&statistics)
        .is_ok_and(|keep| keep.iter().all(|keep| !keep))
}

/// The file's row groups, as something a `PruningPredicate` can ask about.
struct RowGroups<'a> {
    schema: SchemaRef,
    metadata: &'a ParquetMetaData,
}

impl RowGroups<'_> {
    fn converter(&self, column: &Column) -> Option<StatisticsConverter<'_>> {
        StatisticsConverter::try_new(
            column.name(),
            &self.schema,
            self.metadata.file_metadata().schema_descr(),
        )
        .ok()
    }
}

impl PruningStatistics for RowGroups<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)?
            .row_group_mins(self.metadata.row_groups())
            .ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)?
            .row_group_maxes(self.metadata.row_groups())
            .ok()
    }

    fn num_containers(&self) -> usize {
        self.metadata.num_row_groups()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)?
            .row_group_null_counts(self.metadata.row_groups())
            .ok()
            .map(|counts| Arc::new(counts) as ArrayRef)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let counts: UInt64Array = self
            .metadata
            .row_groups()
            .iter()
            .map(|group| u64::try_from(group.num_rows()).ok())
            .collect();
        Some(Arc::new(counts))
    }

    /// Whether a row group is known to hold only values from a set. Nothing here answers
    /// it, which leaves the predicate to decide on the other four.
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::query::Predicate;
    use bytes::Bytes;
    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StructArray};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::properties::WriterProperties;

    /// Shaped like a HATS partition: an index column first, then data.
    fn fixture(nulls: bool) -> Arc<ParquetMetaData> {
        let index: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40]));
        let mag: ArrayRef = match nulls {
            false => Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0, 4.0])),
            true => Arc::new(Float64Array::from(vec![
                Some(1.0_f64),
                None,
                Some(3.0),
                None,
            ])),
        };
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("_healpix_29", index, false),
            ("mag", mag, nulls),
        ])
        .unwrap();
        written(&batch)
    }

    /// The batch as a parquet file, read back for its footer — which is all this decides
    /// from.
    fn written(batch: &RecordBatch) -> Arc<ParquetMetaData> {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            batch.schema(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))
            .unwrap()
            .metadata()
            .clone()
    }

    /// A partition with a packed column in it, which is what a light curve or a spectrum
    /// is: `spectra` holds two fields, and a caller reaches them by writing both.
    fn nested_fixture() -> Arc<ParquetMetaData> {
        let index: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20]));
        let flux = Arc::new(Float64Array::from(vec![1.0_f64, 2.0])) as ArrayRef;
        let ivar = Arc::new(Float64Array::from(vec![3.0_f64, 4.0])) as ArrayRef;
        let spectra: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("flux", DataType::Float64, false)), flux),
            (Arc::new(Field::new("ivar", DataType::Float64, false)), ivar),
        ]));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("_healpix_29", index, false),
            ("spectra", spectra, false),
        ])
        .unwrap();
        written(&batch)
    }

    fn asked(columns: Option<&'static str>, filters: Option<&'static str>) -> Selection<'static> {
        Selection {
            projection: match columns {
                None => Projection::All,
                Some(text) => Projection::ColumnText(text),
            },
            predicate: match filters {
                None => Predicate::All,
                Some(text) => Predicate::FilterText(text),
            },
            ..Selection::default()
        }
    }

    fn decide(selection: &Selection<'_>, metadata: &ParquetMetaData) -> bool {
        answers_with_the_file(
            selection,
            metadata,
            sql::Limits {
                max_depth: 50,
                max_nodes: 500,
            },
        )
    }

    #[test]
    fn a_query_that_narrows_nothing_is_the_file() {
        let metadata = fixture(false);
        assert!(decide(&asked(None, None), &metadata));
        // Every column, in an order that is not the file's — which is the order `lsdb`
        // writes, having appended the index to the catalog's own list.
        assert!(decide(&asked(Some("mag,_healpix_29"), None), &metadata));
    }

    /// The request an `lsdb` client actually sends: every column, and the partition's own
    /// cell bounds as a predicate, which every row in it satisfies.
    #[test]
    fn a_predicate_every_row_satisfies_is_the_file() {
        let metadata = fixture(false);
        assert!(decide(
            &asked(
                Some("_healpix_29,mag"),
                Some("_healpix_29>=0,_healpix_29<100")
            ),
            &metadata
        ));
    }

    #[test]
    fn a_predicate_that_drops_a_row_is_a_query() {
        let metadata = fixture(false);
        assert!(!decide(&asked(None, Some("_healpix_29>=20")), &metadata));
        assert!(!decide(&asked(None, Some("mag>2.5")), &metadata));
    }

    /// A column with nulls in it satisfies no comparison, so the file holds rows the query
    /// would not return — however wide the bounds are.
    #[test]
    fn a_null_makes_even_a_wide_predicate_a_query() {
        let metadata = fixture(true);
        assert!(!decide(&asked(None, Some("mag>-1000")), &metadata));
        // The index column has no nulls, so a predicate over that one still passes.
        assert!(decide(&asked(None, Some("_healpix_29>=0")), &metadata));
    }

    #[test]
    fn asking_for_less_than_the_file_is_a_query() {
        let metadata = fixture(false);
        assert!(!decide(&asked(Some("mag"), None), &metadata));
        assert!(!decide(&asked(Some("_healpix_29"), None), &metadata));
    }

    /// A nested column asked for one field at a time, which is how `lsdb` writes one: the
    /// six dotted names it sends for SDSS DR7's `spectra` are that column, and this service
    /// packs them back into it. Named as fields the projection produces rather than as
    /// names the caller wrote, which is the only comparison that can see that.
    #[test]
    fn a_nested_column_asked_for_field_by_field_is_the_file() {
        let metadata = nested_fixture();
        assert!(decide(
            &asked(Some("_healpix_29,spectra.flux,spectra.ivar"), None),
            &metadata
        ));
        // One field short of the column is one column short of the file.
        assert!(!decide(
            &asked(Some("_healpix_29,spectra.flux"), None),
            &metadata
        ));
    }

    /// A column spelled in capitals, which is what astronomy catalogs are full of — SDSS
    /// writes `OBJID`, `RA`, `DEC`.
    ///
    /// The decision has to plan the way the query plans. Against a default DataFusion
    /// context, whose `enable_ident_normalization` is on, `OBJID` is lowered to `objid`,
    /// nothing resolves, and every such request is re-encoded in full while looking
    /// perfectly correct from outside.
    #[test]
    fn a_column_in_capitals_is_matched_as_the_query_matches_it() {
        let index: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20]));
        let objid: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("_healpix_29", index, false),
            ("OBJID", objid, false),
        ])
        .unwrap();
        let metadata = written(&batch);

        assert!(decide(&asked(Some("OBJID,_healpix_29"), None), &metadata));
    }

    #[test]
    fn a_limit_is_a_query() {
        let metadata = fixture(false);
        let selection = Selection {
            limit: Some(2),
            ..asked(None, None)
        };
        assert!(!decide(&selection, &metadata));
    }
}
