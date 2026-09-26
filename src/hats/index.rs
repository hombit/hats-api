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

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::logical_expr::{Expr, lit};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
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

/// What an index catalog holds, by file, without any of its rows.
#[derive(Debug)]
pub struct IndexLayout {
    /// The indexed column, as the primary table and the index both name it.
    column: String,
    /// Its type in the index, which a looked-up value is cast to.
    data_type: DataType,
    files: Vec<IndexFile>,
}

/// One file of an index, and the values it can hold.
#[derive(Debug)]
struct IndexFile {
    /// Below the index's `dataset/` directory.
    path: String,
    /// `None` where a file's statistics say nothing, which is a file every lookup reads.
    range: Option<(ScalarValue, ScalarValue)>,
}

impl IndexLayout {
    /// Roughly how many bytes this holds, for a cache bounded by them.
    pub(super) fn weight(&self) -> u64 {
        let per_file = |file: &IndexFile| {
            let range = file
                .range
                .as_ref()
                .map_or(0, |(min, max)| min.size() + max.size());
            u64::try_from(file.path.len() + range + 64).unwrap_or(u64::MAX)
        };
        self.files.iter().map(per_file).sum()
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
        let mut files = Vec::new();
        for (path, metadata) in footers {
            let schema = parquet_to_arrow_schema(
                metadata.file_metadata().schema_descr(),
                metadata.file_metadata().key_value_metadata(),
            )
            .map_err(ApiError::SourceMetadata)?;
            let (_, field) = schema.column_with_name(column).ok_or_else(|| {
                ApiError::bad_request(format!(
                    "this collection's index for {column} has no {column} column"
                ))
            })?;
            data_type.get_or_insert_with(|| field.data_type().clone());
            files.push(IndexFile {
                path,
                range: range_of(&metadata, &schema, column),
            });
        }
        Ok(Self {
            column: column.to_owned(),
            data_type: data_type.unwrap_or(DataType::Null),
            files,
        })
    }

    /// The partitions of the primary table holding any of `values`, as `(order, pixel)`.
    ///
    /// Only the files whose range can hold one of them are read, and a value no file's range
    /// holds is one no partition holds: the answer is then empty, which is a catalog holding
    /// no such row rather than a failure.
    pub(super) async fn partitions_for(
        &self,
        dir: &RemoteDir,
        values: &HashSet<ScalarValue>,
    ) -> Result<HashSet<(u8, u64)>, ApiError> {
        let values = values
            .iter()
            .map(|value| value.cast_to(&self.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        let urls = self
            .files
            .iter()
            .filter(|file| match &file.range {
                None => true,
                Some((min, max)) => values.iter().any(|value| value >= min && value <= max),
            })
            .map(|file| {
                dir.child(&format!("{DATASET_DIR}/{}", file.path))
                    .map(|file| file.url.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if urls.is_empty() {
            return Ok(HashSet::new());
        }
        let ctx = crate::engine::query::session_context(false);
        ctx.register_object_store(&dir.base, Arc::clone(&dir.store));
        // The index's own files are named whatever its writer chose, as a catalog's are.
        let options = ParquetReadOptions {
            file_extension: "",
            ..Default::default()
        };
        let named = |name: &str| Expr::Column(Column::new_unqualified(name.to_owned()));
        let batches = ctx
            .read_parquet(urls, options)
            .await?
            .filter(named(&self.column).in_list(values.into_iter().map(lit).collect(), false))?
            .select(vec![named(ORDER), named(PIXEL)])?
            .collect()
            .await?;
        let mut cells = HashSet::new();
        for batch in &batches {
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
        Ok(cells)
    }
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
    use crate::hats::query::tests::{first_id_in, indexed_collection};

    /// A lookup reads only the index files whose range can hold a value asked for. With the
    /// first file gone after the layout was read, a value from the second half is still found
    /// and one from the first half fails to read — which it would not, had the lookup skipped
    /// nothing.
    #[tokio::test]
    async fn a_lookup_reads_only_the_files_whose_range_holds_the_value() {
        for metadata in [false, true] {
            let dir = indexed_collection(metadata);
            let index = crate::storage::open_mounted_dir(&dir.path().join("id_index")).unwrap();
            let layout = IndexLayout::read(&index, "id", &DataFiles::default(), u64::MAX)
                .await
                .unwrap();
            assert_eq!(layout.files.len(), 2, "{metadata}");
            std::fs::remove_file(dir.path().join("id_index/dataset/index/part.0.parquet")).unwrap();

            let late = HashSet::from([ScalarValue::Int64(Some(first_id_in(3)))]);
            let found = layout.partitions_for(&index, &late).await.unwrap();
            assert_eq!(found.len(), 1, "{metadata}: {found:?}");

            let early = HashSet::from([ScalarValue::Int64(Some(first_id_in(0)))]);
            assert!(
                layout.partitions_for(&index, &early).await.is_err(),
                "{metadata}"
            );
        }
    }
}
