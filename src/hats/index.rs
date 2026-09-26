//! A collection's index catalogs: which partitions hold a given value of an indexed column.
//!
//! **An index catalog is a parquet dataset of `(Norder, Npix, <column>)` rows**, one for each
//! partition of the primary table a value appears in, sorted by the value. A collection names
//! its indexes in `all_indexes`, each a directory inside the collection, and nothing else says
//! a catalog has one: a catalog named directly, outside its collection, is read without them.
//!
//! **What is kept is the index's layout, not its rows.** The value range each of its files
//! covers is read once, from `dataset/_metadata` where the index has one and from each file's
//! own footer where it has not, and kept with the catalog like any other part. A lookup reads
//! only the files whose range can hold one of the values asked for, and within those the
//! row-group statistics do the rest, the rows being sorted by the value.
//!
//! **The index is the catalog's claim and is trusted.** A value it does not list is a value no
//! partition is read for; an index older than the table it indexes misses rows, which is the
//! catalog's fault in the way a wrong `partition_info.csv` is.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, lit};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::ParquetReadOptions;
use futures::{StreamExt, TryStreamExt, stream};

use crate::access::data::DataFiles;
use crate::error::ApiError;
use crate::storage::RemoteDir;

use super::partitions::{DATASET_DIR, METADATA};

/// The index's two columns that name a partition of the primary table.
const ORDER: &str = "Norder";
const PIXEL: &str = "Npix";

/// How many index footers are read at once, for an index with no `_metadata`.
const FOOTER_CONCURRENCY: usize = 16;

/// How many values one pruning pass over the files is asked about.
///
/// `PruningPredicate` turns an `IN` list into a comparison against each file's range only up
/// to a length of its own choosing, and past it answers "maybe" for every file — so a lookup
/// of many values is put to it in pieces this short, and the files any piece keeps are read.
const VALUES_PER_PASS: usize = 16;

/// What an index catalog holds, by file, without any of its rows.
#[derive(Debug)]
pub struct IndexLayout {
    /// The indexed column, as the primary table and the index both name it.
    column: String,
    /// Its type in the index, which a looked-up value is cast to.
    data_type: DataType,
    /// Every column the index files carry, which says whether a lookup can also answer with
    /// the rows' HEALPix values.
    columns: Vec<String>,
    files: Vec<IndexFile>,
}

/// One file of an index, and the values it can hold.
#[derive(Debug)]
struct IndexFile {
    /// Below the index's `dataset/` directory.
    path: String,
    /// `None` where a file's statistics say nothing, which is a file every lookup reads.
    range: Option<(ScalarValue, ScalarValue)>,
    /// The compressed size of the indexed column, `Norder` and `Npix`, in every row group — far
    /// more than a lookup reads, row groups being pruned within the file, and what the budget
    /// is checked against. A HEALPix column read beside them is not counted in it.
    bytes: u64,
}

/// What one lookup came to.
#[derive(Debug)]
pub(crate) struct Lookup {
    /// The partitions holding a value asked for, as `(order, pixel)`.
    pub cells: HashSet<(u8, u64)>,
    /// The HEALPix values of the rows found, where the index carries the table's HEALPix
    /// column beside the value. A partition is sorted by that column and not by the indexed
    /// one, so this is what lets its row groups be pruned the way a cone's are.
    pub healpix: Option<HashSet<ScalarValue>>,
    /// What reading the index fetched.
    pub bytes_read: u64,
}

impl IndexLayout {
    /// Roughly how many bytes this holds, for a cache bounded by them.
    pub(super) fn weight(&self) -> u64 {
        let per_file = |file: &IndexFile| {
            let range = file
                .range
                .as_ref()
                .map_or(0, |(min, max)| min.size() + max.size());
            u64::try_from(file.path.len() + range + 72).unwrap_or(u64::MAX)
        };
        let columns = self
            .columns
            .iter()
            .map(|name| u64::try_from(name.len() + 24).unwrap_or(u64::MAX))
            .sum::<u64>();
        self.files.iter().map(per_file).sum::<u64>() + columns
    }

    /// Read an index's layout: its column's type, and the range of values each file covers.
    ///
    /// `dataset/_metadata` first, which is every file's footer in one: one request. Over
    /// `max_metadata_bytes`, or where the index has none, each file's own footer is read
    /// instead, a ranged read apiece.
    pub(super) async fn read(
        dir: &RemoteDir,
        column: &str,
        data: &DataFiles,
        max_metadata_bytes: u64,
    ) -> Result<Self, ApiError> {
        let footers = match read_combined(dir, max_metadata_bytes).await? {
            Some(combined) => from_combined(&combined)?,
            None => read_each(dir, data).await?,
        };
        let mut data_type = None;
        let mut columns: Option<Vec<String>> = None;
        let mut files = Vec::new();
        for (path, metadata) in footers {
            let schema = parquet_to_arrow_schema(
                metadata.file_metadata().schema_descr(),
                metadata.file_metadata().key_value_metadata(),
            )
            .map_err(ApiError::SourceMetadata)?;
            // The two columns that name a partition. The HATS note does not list an index's
            // columns; `hats`' own lookup groups by these two, so an index without them is not
            // one any reader can use.
            for name in [column, ORDER, PIXEL] {
                if schema.column_with_name(name).is_none() {
                    return Err(ApiError::bad_request(format!(
                        "this collection's index for {column} has no {name} column"
                    )));
                }
            }
            if let Some((_, field)) = schema.column_with_name(column) {
                data_type.get_or_insert_with(|| field.data_type().clone());
            }
            // Only what every file carries, since a lookup reads one column list from all of
            // the files it chooses.
            let here = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            columns = Some(match columns {
                None => here,
                Some(before) => before
                    .into_iter()
                    .filter(|name| here.contains(name))
                    .collect(),
            });
            files.push(IndexFile {
                range: range_of(&metadata, &schema, column),
                bytes: read_bytes(&metadata, &schema, column),
                path,
            });
        }
        Ok(Self {
            column: column.to_owned(),
            data_type: data_type.unwrap_or(DataType::Null),
            columns: columns.unwrap_or_default(),
            files,
        })
    }

    /// The index files whose range can hold one of `values`, as DataFusion's
    /// `PruningPredicate` judges them from the ranges kept here — the same judgement it makes
    /// of a parquet file's row groups, one level up.
    fn candidates(&self, values: &[ScalarValue]) -> Result<Vec<&IndexFile>, ApiError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            &self.column,
            self.data_type.clone(),
            true,
        )]));
        let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
        let statistics = FileRanges(self);
        let mut keep = vec![false; self.files.len()];
        for pass in values.chunks(VALUES_PER_PASS) {
            let predicate =
                named(&self.column).in_list(pass.iter().cloned().map(lit).collect(), false);
            let physical = create_physical_expr(
                &predicate,
                &df_schema,
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )?;
            let kept = match PruningPredicateBuilder::new()
                .with_file_schema(Arc::clone(&schema))
                .build(physical)
            {
                Some(pruning) => pruning.prune(&statistics)?,
                // Nothing it could prune on, which keeps every file.
                None => vec![true; self.files.len()],
            };
            for (keep, kept) in keep.iter_mut().zip(kept) {
                *keep |= kept;
            }
        }
        Ok(self
            .files
            .iter()
            .zip(keep)
            .filter(|(_, keep)| *keep)
            .map(|(file, _)| file)
            .collect())
    }

    /// The partitions of the primary table holding any of `values`.
    ///
    /// Only the files that can hold one of them are read, and a value no file can hold is one
    /// no partition holds: the answer is then empty, which is a catalog holding no such row
    /// rather than a failure. `None` where reading those files could cost more than
    /// `max_bytes`, which is the index declining rather than the request failing.
    ///
    /// `healpix` is the table's HEALPix column. Where the index carries one of that name, the
    /// rows' values of it come back too.
    pub(super) async fn partitions_for(
        &self,
        dir: &RemoteDir,
        values: &HashSet<ScalarValue>,
        max_bytes: u64,
        healpix: Option<&str>,
    ) -> Result<Option<Lookup>, ApiError> {
        let healpix = healpix.filter(|name| self.columns.iter().any(|column| column == name));
        let values = values
            .iter()
            .map(|value| value.cast_to(&self.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        let files = self.candidates(&values)?;
        let estimate = files.iter().map(|file| file.bytes).sum::<u64>();
        if estimate > max_bytes {
            tracing::info!(
                column = self.column,
                files = files.len(),
                estimate,
                max_bytes,
                "an index lookup would read more than max_bytes_fetched; not using the index"
            );
            return Ok(None);
        }
        if files.is_empty() {
            return Ok(Some(Lookup {
                cells: HashSet::new(),
                healpix: healpix.map(|_| HashSet::new()),
                bytes_read: 0,
            }));
        }
        let urls = files
            .iter()
            .map(|file| {
                dir.child(&format!("{DATASET_DIR}/{}", file.path))
                    .map(|file| file.url.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ctx = crate::engine::query::session_context(false);
        ctx.register_object_store(&dir.base, Arc::clone(&dir.store));
        // The index's own files are named whatever its writer chose, as a catalog's are.
        let options = ParquetReadOptions {
            file_extension: "",
            ..Default::default()
        };
        // Row groups within the files are DataFusion's to prune, from each footer's own
        // statistics, the rows being sorted by the value.
        let plan = ctx
            .read_parquet(urls, options)
            .await?
            .filter(named(&self.column).in_list(values.into_iter().map(lit).collect(), false))?
            .select(
                [named(ORDER), named(PIXEL)]
                    .into_iter()
                    .chain(healpix.map(named))
                    .collect::<Vec<_>>(),
            )?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        let bytes_read = crate::engine::query::data_bytes_read(plan.as_ref());
        let mut cells = HashSet::new();
        let mut found_healpix = healpix.map(|_| HashSet::new());
        for batch in &batches {
            if let Some(found) = found_healpix.as_mut() {
                let column = batch.column(2);
                for row in 0..batch.num_rows() {
                    if column.is_valid(row) {
                        found.insert(ScalarValue::try_from_array(column, row)?);
                    }
                }
            }
            let as_u64 = |at: usize| -> Result<UInt64Array, ApiError> {
                let column = cast(batch.column(at), &DataType::UInt64)?;
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .cloned()
                    .ok_or_else(|| ApiError::internal("a cast to UInt64 did not give UInt64"))
            };
            let (orders, pixels) = (as_u64(0)?, as_u64(1)?);
            for row in 0..batch.num_rows() {
                if orders.is_null(row) || pixels.is_null(row) {
                    continue;
                }
                let order = u8::try_from(orders.value(row)).map_err(|_| {
                    ApiError::bad_request(format!(
                        "this collection's index names an order of {}, which no partition has",
                        orders.value(row)
                    ))
                })?;
                cells.insert((order, pixels.value(row)));
            }
        }
        Ok(Some(Lookup {
            cells,
            healpix: found_healpix,
            bytes_read,
        }))
    }
}

fn named(name: &str) -> Expr {
    Expr::Column(Column::new_unqualified(name.to_owned()))
}

/// Each index file's range as statistics `PruningPredicate` can read, one container per file.
struct FileRanges<'a>(&'a IndexLayout);

impl FileRanges<'_> {
    fn bound(&self, column: &Column, max: bool) -> Option<ArrayRef> {
        if column.name != self.0.column {
            return None;
        }
        let empty = ScalarValue::try_from(&self.0.data_type).ok()?;
        let values = self.0.files.iter().map(|file| match &file.range {
            Some((low, high)) => match max {
                true => high.clone(),
                false => low.clone(),
            },
            None => empty.clone(),
        });
        ScalarValue::iter_to_array(values).ok()
    }
}

impl PruningStatistics for FileRanges<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bound(column, false)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bound(column, true)
    }

    fn num_containers(&self) -> usize {
        self.0.files.len()
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// The compressed size of the indexed column, `Norder` and `Npix`, across a file's row groups.
fn read_bytes(metadata: &ParquetMetaData, schema: &Schema, column: &str) -> u64 {
    let descriptor = metadata.file_metadata().schema_descr();
    let leaves = [column, ORDER, PIXEL]
        .into_iter()
        .filter_map(|name| {
            StatisticsConverter::try_new(name, schema, descriptor)
                .ok()?
                .parquet_column_index()
        })
        .collect::<Vec<_>>();
    metadata
        .row_groups()
        .iter()
        .flat_map(|group| {
            leaves
                .iter()
                .map(move |&leaf| u64::try_from(group.column(leaf).compressed_size()).unwrap_or(0))
        })
        .sum()
}

/// The lowest and highest value of `column` in a file, across its row groups.
fn range_of(
    metadata: &ParquetMetaData,
    schema: &Schema,
    column: &str,
) -> Option<(ScalarValue, ScalarValue)> {
    let converter =
        StatisticsConverter::try_new(column, schema, metadata.file_metadata().schema_descr())
            .ok()?;
    let mins = converter.row_group_mins(metadata.row_groups()).ok()?;
    let maxes = converter.row_group_maxes(metadata.row_groups()).ok()?;
    // A row group with no statistics says nothing, so neither does the file.
    if mins.null_count() > 0 || maxes.null_count() > 0 || mins.is_empty() {
        return None;
    }
    let scalar = |values: &dyn Array, at| ScalarValue::try_from_array(values, at).ok();
    let mut low = scalar(mins.as_ref(), 0)?;
    let mut high = scalar(maxes.as_ref(), 0)?;
    for at in 1..mins.len() {
        let (min, max) = (scalar(mins.as_ref(), at)?, scalar(maxes.as_ref(), at)?);
        if min < low {
            low = min;
        }
        if max > high {
            high = max;
        }
    }
    Some((low, high))
}

/// `dataset/_metadata`, where the index has one no larger than this service will fetch.
async fn read_combined(
    dir: &RemoteDir,
    max_metadata_bytes: u64,
) -> Result<Option<ParquetMetaData>, ApiError> {
    match dir.size(METADATA).await? {
        Some(size) if size <= max_metadata_bytes => {
            let bytes = dir.read(METADATA).await?;
            Ok(Some(
                ParquetMetaDataReader::new()
                    .parse_and_finish(&bytes)
                    .map_err(ApiError::SourceMetadata)?,
            ))
        }
        _ => Ok(None),
    }
}

/// `_metadata`'s row groups split back into one footer per file, by the path each carries.
fn from_combined(combined: &ParquetMetaData) -> Result<Vec<(String, ParquetMetaData)>, ApiError> {
    let mut by_file: Vec<(String, Vec<_>)> = Vec::new();
    for group in combined.row_groups() {
        let Some(path) = group.columns().first().and_then(|chunk| chunk.file_path()) else {
            continue;
        };
        match by_file.iter_mut().find(|(seen, _)| seen == path) {
            Some((_, groups)) => groups.push(group.clone()),
            None => by_file.push((path.to_owned(), vec![group.clone()])),
        }
    }
    Ok(by_file
        .into_iter()
        .map(|(path, groups)| {
            (
                path,
                ParquetMetaData::new(combined.file_metadata().clone(), groups),
            )
        })
        .collect())
}

/// Every data file below `dataset/`, and its footer, read several at a time.
async fn read_each(
    dir: &RemoteDir,
    data: &DataFiles,
) -> Result<Vec<(String, ParquetMetaData)>, ApiError> {
    let listing = dir.list(DATASET_DIR).await?;
    let reads = listing
        .into_iter()
        .filter(|entry| data.matches(entry.name.rsplit('/').next().unwrap_or(&entry.name)))
        .map(|entry| async move {
            let relative = format!("{DATASET_DIR}/{}", entry.name);
            let meta = dir.meta(&relative).await?.ok_or_else(|| {
                ApiError::bad_request(format!(
                    "{relative:?} was listed in this index and is not there"
                ))
            })?;
            // DataFusion's own footer read, the one its scans make: ranged, with no page
            // index, since only the row-group statistics are wanted here.
            let metadata = DFParquetMetadata::new(dir.store.as_ref(), &meta)
                .with_page_index_policy(Some(PageIndexPolicy::Skip))
                .fetch_metadata()
                .await?;
            Ok::<_, ApiError>((entry.name, Arc::unwrap_or_clone(metadata)))
        })
        .collect::<Vec<_>>();
    stream::iter(reads)
        .buffered(FOOTER_CONCURRENCY)
        .try_collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hats::query::tests::{first_id_in, fixture_rows, indexed_collection};

    fn ids(values: impl IntoIterator<Item = i64>) -> HashSet<ScalarValue> {
        values
            .into_iter()
            .map(|id| ScalarValue::Int64(Some(id)))
            .collect()
    }

    async fn layout_of(dir: &std::path::Path) -> (RemoteDir, IndexLayout) {
        let index = crate::storage::open_mounted_dir(&dir.join("id_index")).unwrap();
        let layout = IndexLayout::read(&index, "id", &DataFiles::default(), u64::MAX)
            .await
            .unwrap();
        (index, layout)
    }

    /// A lookup reads only the index files whose range can hold a value asked for. With the
    /// first file gone after the layout was read, a value from the second half is still found
    /// and one from the first half fails to read — which it would not, had the lookup skipped
    /// nothing. The same for forty values at once: put to `PruningPredicate` as one `IN` list
    /// they keep every file, the deleted one included, which is what `VALUES_PER_PASS` is for.
    #[tokio::test]
    async fn a_lookup_reads_only_the_files_whose_range_holds_the_value() {
        for metadata in [false, true] {
            let dir = indexed_collection(metadata, true);
            let (index, layout) = layout_of(dir.path()).await;
            assert_eq!(layout.files.len(), 2, "{metadata}");
            std::fs::remove_file(dir.path().join("id_index/dataset/index/part.0.parquet")).unwrap();

            let late = ids([first_id_in(3)]);
            let found = layout
                .partitions_for(&index, &late, u64::MAX, None)
                .await
                .unwrap();
            assert_eq!(found.unwrap().cells.len(), 1, "{metadata}");

            let half = i64::try_from(fixture_rows() / 2).unwrap();
            let many = ids(half + 1..=half + 40);
            let found = layout
                .partitions_for(&index, &many, u64::MAX, None)
                .await
                .unwrap();
            let found = found.unwrap();
            assert!(!found.cells.is_empty(), "{metadata}");
            assert!(found.bytes_read > 0, "{metadata}");

            let early = ids([first_id_in(0)]);
            assert!(
                layout
                    .partitions_for(&index, &early, u64::MAX, None)
                    .await
                    .is_err(),
                "{metadata}"
            );
        }
    }

    /// A lookup that would read more than it may is declined, and nothing is read: the index
    /// is an optimization, and declining it leaves the scan to answer without one.
    #[tokio::test]
    async fn a_lookup_over_its_budget_is_declined() {
        let dir = indexed_collection(true, true);
        let (index, layout) = layout_of(dir.path()).await;
        std::fs::remove_dir_all(dir.path().join("id_index/dataset/index")).unwrap();
        let found = layout
            .partitions_for(&index, &ids([first_id_in(0)]), 1, None)
            .await
            .unwrap();
        assert!(found.is_none());
    }

    /// `Norder` and `Npix` are what name a partition, and an index without them is not used.
    #[tokio::test]
    async fn an_index_without_norder_and_npix_is_refused() {
        let dir = indexed_collection(false, true);
        let file = dir.path().join("id_index/dataset/index/part.0.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                1_i64,
            ]))],
        )
        .unwrap();
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&file).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let index = crate::storage::open_mounted_dir(&dir.path().join("id_index")).unwrap();
        let error = IndexLayout::read(&index, "id", &DataFiles::default(), u64::MAX)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Norder"), "{error}");
    }
}
