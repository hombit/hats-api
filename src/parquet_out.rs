//! Writing the result back out as parquet, laid out the way the source file is.
//!
//! A point lookup returns a handful of rows out of a file someone else wrote, and the
//! obvious thing to hand back is a file of the same shape: the same codec per column,
//! the same encodings, statistics and bloom filters where the original has them. That
//! costs one extra footer read of the source file and makes the answer round-trip
//! through anything that reads the original.
//!
//! What is inherited, per leaf column, matched by its parquet path:
//!
//! - compression codec (its *level* is not stored in a parquet file; the writer's
//!   default level for that codec is used)
//! - dictionary encoding, and the fallback encoding when the column is not
//!   dictionary-encoded
//! - statistics: page-level when the source carries a column index, chunk-level when it
//!   only has chunk statistics, off when it has neither
//! - bloom filters, when the source column has one
//!
//! And per file: the largest row group row count, and the writer version, inferred from
//! the encodings in use because parquet does not record it.
//!
//! What is not inherited: the source file's key/value metadata. It describes the
//! source's own schema (`ARROW:schema`, pandas metadata), and a projection of a few
//! rows is not that file.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::arrow::async_reader::{MetadataFetch, MetadataSuffixFetch};
use datafusion::parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use datafusion::parquet::basic::{Compression, Encoding, Type as PhysicalType};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use crate::error::ApiError;
use crate::query::QueryResult;
use crate::storage::RemoteFile;

/// How the source file writes each of its leaf columns, keyed by the dotted parquet
/// path of the leaf (`lightcurve.list.element.mag`).
#[derive(Debug)]
pub struct SourceLayout {
    columns: HashMap<String, ColumnLayout>,
    max_row_group_rows: usize,
    writer_version: WriterVersion,
}

impl Default for SourceLayout {
    /// Nothing inherited: the writer's own defaults, which is what a file we could not
    /// learn anything from leaves us with.
    fn default() -> Self {
        Self {
            columns: HashMap::new(),
            max_row_group_rows: 0,
            writer_version: WriterVersion::PARQUET_1_0,
        }
    }
}

/// Just enough of a reader to pull a footer out of the object store. Suffix requests
/// mean the file's size does not have to be asked for first.
struct FooterFetch {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl MetadataFetch for FooterFetch {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for FooterFetch {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async move {
            let options = object_store::GetOptions {
                range: Some(object_store::GetRange::Suffix(suffix as u64)),
                ..Default::default()
            };
            let response = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            response
                .bytes()
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
struct ColumnLayout {
    compression: Compression,
    /// The non-dictionary encoding to fall back on, when we can tell what it was.
    encoding: Option<Encoding>,
    dictionary: bool,
    statistics: EnabledStatistics,
    bloom_filter: bool,
}

/// Read the source file's footer. One extra request against the object store, made only
/// when the caller asked for parquet back.
pub async fn read_layout(file: &RemoteFile) -> Result<SourceLayout, ApiError> {
    let path = Path::from_url_path(file.url.path()).map_err(|error| {
        ApiError::bad_request(format!(
            "url {} is not a valid object path: {error}",
            file.url
        ))
    })?;
    let fetch = FooterFetch {
        store: Arc::clone(&file.store),
        path,
    };
    let metadata = ParquetMetaDataReader::new()
        .load_via_suffix_and_finish(fetch)
        .await
        .map_err(ApiError::SourceMetadata)?;
    Ok(SourceLayout::from_metadata(&metadata))
}

impl SourceLayout {
    fn from_metadata(metadata: &ParquetMetaData) -> Self {
        let mut layout = Self::default();
        for row_group in metadata.row_groups() {
            // A row count that does not fit a `usize` cannot describe a row group we
            // could hold anyway, so saturating is the honest conversion.
            layout.max_row_group_rows = layout
                .max_row_group_rows
                .max(usize::try_from(row_group.num_rows()).unwrap_or(usize::MAX));
            for chunk in row_group.columns() {
                let encodings: Vec<Encoding> = chunk.encodings().collect();
                if uses_data_page_v2_encoding(&encodings) {
                    layout.writer_version = WriterVersion::PARQUET_2_0;
                }
                // The first row group that mentions a column defines its layout; a file
                // that encodes the same column differently in different row groups has
                // no single answer to inherit.
                layout
                    .columns
                    .entry(chunk.column_path().string())
                    .or_insert_with(|| ColumnLayout {
                        compression: chunk.compression(),
                        encoding: fallback_encoding(
                            &encodings,
                            chunk.column_descr().physical_type(),
                        ),
                        dictionary: encodings.iter().any(is_dictionary),
                        statistics: if chunk.column_index_offset().is_some() {
                            EnabledStatistics::Page
                        } else if chunk.statistics().is_some() {
                            EnabledStatistics::Chunk
                        } else {
                            EnabledStatistics::None
                        },
                        bloom_filter: chunk.bloom_filter_offset().is_some(),
                    });
            }
        }
        layout
    }

    /// Build writer properties for `schema`, giving every leaf we recognise from the
    /// source its own settings and leaving the rest at the writer's defaults.
    fn writer_properties(&self, schema: &Schema) -> Result<WriterProperties, ApiError> {
        let descriptor = ArrowSchemaConverter::new()
            .convert(schema)
            .map_err(ApiError::ParquetWrite)?;

        let mut builder = WriterProperties::builder().set_writer_version(self.writer_version);
        if self.max_row_group_rows > 0 {
            builder = builder.set_max_row_group_row_count(Some(self.max_row_group_rows));
        }
        for column in descriptor.columns() {
            let path = column.path();
            let Some(source) = self.columns.get(&path.string()) else {
                continue;
            };
            builder = builder
                .set_column_compression(path.clone(), source.compression)
                .set_column_dictionary_enabled(path.clone(), source.dictionary)
                .set_column_statistics_enabled(path.clone(), source.statistics)
                .set_column_bloom_filter_enabled(path.clone(), source.bloom_filter);
            // Only meaningful without a dictionary: with one, this would be the
            // encoding of the dictionary fallback pages, and the writer picks that.
            if let Some(encoding) = source.encoding.filter(|_| !source.dictionary) {
                builder = builder.set_column_encoding(path.clone(), encoding);
            }
        }
        Ok(builder.build())
    }
}

/// Parquet does not record which encoding held the *data*: the chunk's encoding list
/// mixes in the encoding of the dictionary page and of the definition and repetition
/// levels. RLE is a level encoding for everything but booleans, PLAIN is what a
/// dictionary page uses, so what is left is the data encoding — if anything is.
fn fallback_encoding(encodings: &[Encoding], physical_type: PhysicalType) -> Option<Encoding> {
    encodings
        .iter()
        .copied()
        .filter(|encoding| !is_dictionary(encoding))
        .filter(|encoding| !(*encoding == Encoding::RLE && physical_type != PhysicalType::BOOLEAN))
        .find(|encoding| writable(*encoding, physical_type))
}

fn is_dictionary(encoding: &Encoding) -> bool {
    matches!(
        encoding,
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
    )
}

/// Encodings that only exist in data page v2 files; seeing one says the source was
/// written by a 2.0 writer, which parquet stores nowhere else.
fn uses_data_page_v2_encoding(encodings: &[Encoding]) -> bool {
    encodings.iter().any(|encoding| {
        matches!(
            encoding,
            Encoding::DELTA_BINARY_PACKED
                | Encoding::DELTA_LENGTH_BYTE_ARRAY
                | Encoding::DELTA_BYTE_ARRAY
                | Encoding::BYTE_STREAM_SPLIT
        )
    })
}

/// Whether the writer can actually use this encoding for this physical type. A source
/// column and its copy have the same type, but the parquet spec still pairs encodings
/// with types, and asking for an impossible pair is a write error rather than a
/// fallback.
fn writable(encoding: Encoding, physical_type: PhysicalType) -> bool {
    use Encoding::*;
    use PhysicalType::*;
    match encoding {
        PLAIN => true,
        RLE => physical_type == BOOLEAN,
        DELTA_BINARY_PACKED => matches!(physical_type, INT32 | INT64),
        DELTA_LENGTH_BYTE_ARRAY => physical_type == BYTE_ARRAY,
        DELTA_BYTE_ARRAY => matches!(physical_type, BYTE_ARRAY | FIXED_LEN_BYTE_ARRAY),
        BYTE_STREAM_SPLIT => matches!(
            physical_type,
            FLOAT | DOUBLE | INT32 | INT64 | FIXED_LEN_BYTE_ARRAY
        ),
        _ => false,
    }
}

/// Serialize the result as a parquet file in memory.
///
/// The whole file is buffered: it holds the rows of one point lookup, which is the
/// same thing the JSON response already holds in memory.
pub fn encode(result: &QueryResult, layout: &SourceLayout) -> Result<Vec<u8>, ApiError> {
    let properties = layout.writer_properties(&result.schema)?;
    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, Arc::clone(&result.schema), Some(properties))
            .map_err(ApiError::ParquetWrite)?;
    for batch in &result.batches {
        writer.write(batch).map_err(ApiError::ParquetWrite)?;
    }
    writer.close().map_err(ApiError::ParquetWrite)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Float32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::metadata::ParquetMetaData;

    use super::*;

    fn sample_batch() -> RecordBatch {
        let objectid: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let objra: ArrayRef = Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0]));
        let filter: ArrayRef = Arc::new(StringArray::from(vec!["g", "r", "g"]));
        RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("objra", objra, true),
            ("filter", filter, true),
        ])
        .unwrap()
    }

    /// Write a file with known properties, then read its metadata back: that is the
    /// only honest stand-in for "the file someone else wrote".
    fn write_source(properties: WriterProperties) -> Arc<ParquetMetaData> {
        let batch = sample_batch();
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))
            .unwrap()
            .metadata()
            .clone()
    }

    fn result(batch: RecordBatch) -> QueryResult {
        QueryResult {
            schema: batch.schema(),
            batches: vec![batch],
        }
    }

    fn round_trip(source: WriterProperties) -> Arc<ParquetMetaData> {
        let layout = SourceLayout::from_metadata(&write_source(source));
        let bytes = encode(&result(sample_batch()), &layout).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .metadata()
            .clone()
    }

    fn chunk_of<'a>(
        metadata: &'a ParquetMetaData,
        name: &str,
    ) -> &'a datafusion::parquet::file::metadata::ColumnChunkMetaData {
        metadata
            .row_group(0)
            .columns()
            .iter()
            .find(|chunk| chunk.column_path().string() == name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    #[test]
    fn inherits_per_column_compression() {
        let source = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_column_compression(
                datafusion::parquet::schema::types::ColumnPath::from("objra"),
                Compression::SNAPPY,
            )
            .build();
        let written = round_trip(source);
        assert_eq!(
            chunk_of(&written, "objra").compression(),
            Compression::SNAPPY
        );
        assert_eq!(
            chunk_of(&written, "objectid").compression(),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn inherits_whether_a_column_is_dictionary_encoded() {
        let source = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_column_dictionary_enabled(
                datafusion::parquet::schema::types::ColumnPath::from("filter"),
                true,
            )
            .build();
        let written = round_trip(source);
        assert!(
            chunk_of(&written, "filter")
                .encodings()
                .any(|e| is_dictionary(&e))
        );
        assert!(
            !chunk_of(&written, "objectid")
                .encodings()
                .any(|e| is_dictionary(&e))
        );
    }

    #[test]
    fn inherits_the_encoding_of_a_column_without_a_dictionary() {
        let source = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_column_encoding(
                datafusion::parquet::schema::types::ColumnPath::from("objectid"),
                Encoding::DELTA_BINARY_PACKED,
            )
            .set_column_encoding(
                datafusion::parquet::schema::types::ColumnPath::from("objra"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .build();
        let written = round_trip(source);
        assert!(
            chunk_of(&written, "objectid")
                .encodings()
                .any(|e| e == Encoding::DELTA_BINARY_PACKED)
        );
        assert!(
            chunk_of(&written, "objra")
                .encodings()
                .any(|e| e == Encoding::BYTE_STREAM_SPLIT)
        );
    }

    #[test]
    fn inherits_statistics_and_bloom_filters() {
        let source = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_column_statistics_enabled(
                datafusion::parquet::schema::types::ColumnPath::from("objectid"),
                EnabledStatistics::Chunk,
            )
            .set_column_bloom_filter_enabled(
                datafusion::parquet::schema::types::ColumnPath::from("filter"),
                true,
            )
            .build();
        let written = round_trip(source);
        assert!(chunk_of(&written, "objectid").statistics().is_some());
        assert!(chunk_of(&written, "objra").statistics().is_none());
        assert!(chunk_of(&written, "filter").bloom_filter_offset().is_some());
        assert!(
            chunk_of(&written, "objectid")
                .bloom_filter_offset()
                .is_none()
        );
    }

    #[test]
    fn an_empty_result_is_still_a_readable_file_with_the_right_schema() {
        let layout = SourceLayout::default();
        let schema = sample_batch().schema();
        let bytes = encode(
            &QueryResult {
                schema: Arc::clone(&schema),
                batches: Vec::new(),
            },
            &layout,
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.schema().fields(), schema.fields());
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
    }

    #[test]
    fn a_column_the_source_does_not_have_keeps_the_defaults() {
        // Nothing to inherit: the write must still succeed.
        let layout = SourceLayout::default();
        let bytes = encode(&result(sample_batch()), &layout).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);
    }

    #[test]
    fn recognises_the_data_encoding_behind_the_level_and_dictionary_encodings() {
        // What a v1 dictionary-encoded chunk looks like: PLAIN is the dictionary page.
        assert_eq!(
            fallback_encoding(
                &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
                PhysicalType::BYTE_ARRAY
            ),
            Some(Encoding::PLAIN)
        );
        // RLE here is the level encoding, not the data encoding.
        assert_eq!(
            fallback_encoding(
                &[Encoding::RLE, Encoding::DELTA_BINARY_PACKED],
                PhysicalType::INT64
            ),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        // For a boolean column RLE is a real data encoding.
        assert_eq!(
            fallback_encoding(&[Encoding::RLE], PhysicalType::BOOLEAN),
            Some(Encoding::RLE)
        );
        // An encoding the writer cannot use for this type is not inherited.
        assert_eq!(
            fallback_encoding(&[Encoding::DELTA_BYTE_ARRAY], PhysicalType::INT64),
            None
        );
    }
}
