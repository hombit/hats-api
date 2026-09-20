//! Writing the result back out as parquet.
//!
//! **How an answer is written is this service's decision, not the source file's.** An
//! answer is read once, by a client that asked a question over a network, which is a
//! different life from the file it came out of — that one was written once to be read for
//! years. So three things are fixed here whatever the source did:
//!
//! - **Snappy, always.** A query answer is not compressed by the HTTP layer — parquet is
//!   excluded from it, being compressed already — so the codec chosen here *is* what
//!   crosses the network.
//! - **Page statistics, always**, which is what writes the page index. A reader that seeks
//!   inside the answer can then skip pages, and a reader that does not pays a few
//!   kilobytes of metadata.
//! - **`BYTE_STREAM_SPLIT` for float and double columns, a dictionary for everything
//!   else.** Splitting a float column into its byte planes puts the exponents together and
//!   the mantissa noise together, which is what makes a general codec able to do anything
//!   with a column of measurements. The two are exclusive per column: a dictionary is what
//!   the writer reaches for first, so a float column asking for the split has to say it
//!   wants no dictionary as well.
//!
//! What is still the source's: the largest row group row count, the writer version —
//! inferred from the encodings in use, because parquet does not record it — and which
//! columns carry a bloom filter. Those are statements about the data rather than about
//! taste: a bloom filter says this column is the one people look rows up by.
//!
//! What is never inherited: the source file's key/value metadata. It describes the
//! source's own schema (`ARROW:schema`, pandas metadata), and a projection of a few
//! rows is not that file.

use std::collections::HashSet;
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

use crate::engine::query::QueryResult;
use crate::error::ApiError;
use crate::storage::RemoteFile;

/// What an answer keeps from the file it came out of.
///
/// Not how its columns are written — that is fixed, and said in `writer_properties`.
/// These are the three the source is still the authority on.
#[derive(Debug)]
pub struct SourceLayout {
    /// Leaves the source carries a bloom filter for, by the dotted parquet path of the
    /// leaf (`lightcurve.list.element.mag`).
    bloom_filters: HashSet<String>,
    max_row_group_rows: usize,
    writer_version: WriterVersion,
}

impl Default for SourceLayout {
    /// Nothing inherited: the writer's own defaults, which is what a file we could not
    /// learn anything from leaves us with.
    fn default() -> Self {
        Self {
            bloom_filters: HashSet::new(),
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

/// With no hint, `ParquetMetaDataReader` prefetches only the 8-byte trailer, learns the
/// footer's length from it, and pays a second request for the footer itself — every time,
/// against any origin. DataFusion's own scan avoids that by prefetching this many bytes
/// from the end of the file on the first request, which is enough for the footer whenever
/// it is smaller than the hint; matched here so our own footer read costs the same one
/// request DataFusion's does, rather than two.
const FOOTER_PREFETCH: usize = 512 * 1024;

/// The source file's own footer: its layout to copy, and the statistics that say whether
/// the query needs running at all.
///
/// One request, and the same one either way — the layout is derived from the metadata
/// rather than read separately.
pub async fn read_source(
    file: &RemoteFile,
) -> Result<(SourceLayout, Arc<ParquetMetaData>), ApiError> {
    let metadata = read_metadata(file).await?;
    Ok((SourceLayout::from_metadata(&metadata), metadata))
}

/// Read the source file's footer. One extra request against the object store, made only
/// when the caller asked for parquet back.
pub async fn read_layout(file: &RemoteFile) -> Result<SourceLayout, ApiError> {
    read_metadata(file)
        .await
        .map(|metadata| SourceLayout::from_metadata(&metadata))
}

/// The footer itself, parsed.
async fn read_metadata(file: &RemoteFile) -> Result<Arc<ParquetMetaData>, ApiError> {
    // Neither the url nor the error goes into the message. For a local file `file.url`
    // is where it sits on the disk, and `object_store`'s own path errors print the path
    // they were given — so both would put a mount's `source` in a response.
    let path = Path::from_url_path(file.url.path()).map_err(|error| {
        tracing::warn!(%error, "a source url is not a valid object path");
        ApiError::bad_request("this url is not a valid object path")
    })?;
    let fetch = FooterFetch {
        store: Arc::clone(&file.store),
        path,
    };
    ParquetMetaDataReader::new()
        .with_prefetch_hint(Some(FOOTER_PREFETCH))
        .load_via_suffix_and_finish(fetch)
        .await
        .map(Arc::new)
        .map_err(ApiError::SourceMetadata)
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
                // A bloom filter is a statement about the data — this is the column rows
                // are looked up by — so it is kept where the source has one.
                if chunk.bloom_filter_offset().is_some() {
                    layout.bloom_filters.insert(chunk.column_path().string());
                }
            }
        }
        layout
    }

    /// How this answer is written: the fixed choices, and the few the source still makes.
    fn writer_properties(&self, schema: &Schema) -> Result<WriterProperties, ApiError> {
        let descriptor = ArrowSchemaConverter::new()
            .convert(schema)
            .map_err(ApiError::ParquetWrite)?;

        let mut builder = WriterProperties::builder()
            .set_writer_version(self.writer_version)
            .set_compression(Compression::SNAPPY)
            // The writer's own default, said out loud: it is what writes the page index,
            // and an answer this service generates is meant to be seekable.
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(true);
        if self.max_row_group_rows > 0 {
            builder = builder.set_max_row_group_row_count(Some(self.max_row_group_rows));
        }
        for column in descriptor.columns() {
            let path = column.path();
            if is_floating(column.physical_type()) {
                // Both halves, or neither has any effect: with a dictionary enabled the
                // writer dictionary-encodes and reaches the requested encoding only for
                // the pages after the dictionary overflows.
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path.clone(), Encoding::BYTE_STREAM_SPLIT);
            }
            if self.bloom_filters.contains(&path.string()) {
                builder = builder.set_column_bloom_filter_enabled(path.clone(), true);
            }
        }
        Ok(builder.build())
    }
}

/// The two types `BYTE_STREAM_SPLIT` is asked for here.
///
/// Parquet 2.8 allows it on the integer and fixed-length types as well, and readers are
/// far behind that: `pyarrow` has read it for floats since 8.0 and for the rest only
/// since 15. Floats are where it pays anyway — a column of measurements is an exponent
/// that barely moves and a mantissa that is noise, and splitting the planes is what lets
/// a codec compress the first.
fn is_floating(physical_type: PhysicalType) -> bool {
    matches!(physical_type, PhysicalType::FLOAT | PhysicalType::DOUBLE)
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{ArrayRef, Float32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::metadata::ParquetMetaData;
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;

    /// Counts `get_opts` calls, so a footer read can be judged by how many requests it
    /// cost rather than by reading the code that made them.
    #[derive(Debug, Default)]
    struct Counting {
        inner: InMemory,
        requests: AtomicUsize,
    }

    impl std::fmt::Display for Counting {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "counting({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// With no prefetch hint, `ParquetMetaDataReader` fetches the 8-byte trailer to learn
    /// the footer's length and then fetches the footer itself — two requests for one
    /// answer, against any origin. `read_layout` sets a hint generous enough to cover an
    /// ordinary footer in the trailer's own request, so this should cost one.
    #[tokio::test]
    async fn the_footer_is_read_in_one_request() {
        let counting = Arc::new(Counting::default());
        let batch = sample_batch();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        counting
            .inner
            .put(&Path::from("fixture.parquet"), Bytes::from(buffer).into())
            .await
            .unwrap();
        let file = RemoteFile::over(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            Url::parse("mem:///").unwrap(),
            Url::parse("mem:///fixture.parquet").unwrap(),
        );

        read_layout(&file).await.unwrap();

        assert_eq!(counting.requests.load(Ordering::SeqCst), 1);
    }

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
            data_bytes_read: 0,
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

    /// The split for floats and a dictionary for the rest, which is one decision: a
    /// column cannot have both, and a dictionary is what the writer reaches for unless
    /// told otherwise.
    #[test]
    fn a_float_column_is_byte_stream_split_and_the_others_keep_a_dictionary() {
        let written = round_trip(WriterProperties::builder().build());

        let floats = chunk_of(&written, "objra");
        assert!(
            floats.encodings().any(|e| e == Encoding::BYTE_STREAM_SPLIT),
            "a float column was not split: {:?}",
            floats.encodings().collect::<Vec<_>>()
        );
        assert!(
            !floats.encodings().any(is_dictionary),
            "a split column still has a dictionary, so the split is only its fallback"
        );

        for name in ["objectid", "filter"] {
            assert!(
                chunk_of(&written, name).encodings().any(is_dictionary),
                "{name} lost its dictionary"
            );
            assert!(
                !chunk_of(&written, name)
                    .encodings()
                    .any(|e| e == Encoding::BYTE_STREAM_SPLIT),
                "{name} is not a float and was split anyway"
            );
        }
    }

    /// Whether a dictionary page is in the chunk's encodings, which is how a chunk says
    /// it has one.
    fn is_dictionary(encoding: Encoding) -> bool {
        matches!(
            encoding,
            Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
        )
    }

    #[test]
    fn an_empty_result_is_still_a_readable_file_with_the_right_schema() {
        let layout = SourceLayout::default();
        let schema = sample_batch().schema();
        let bytes = encode(
            &QueryResult {
                schema: Arc::clone(&schema),
                batches: Vec::new(),
                data_bytes_read: 0,
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
}
