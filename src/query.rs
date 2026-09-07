//! Reading rows out of a single parquet file: a projection, a row predicate and a row
//! cap, each of which the caller may leave out. Says nothing about where the data lives.
//!
//! Nothing is cached between requests: every call builds its own session, its own
//! object store, and reads the file's metadata from scratch.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, collect_partitioned,
    execute_stream_partitioned,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;

use crate::error::ApiError;
use crate::region::{self, Spatial};
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

/// What to read. The two expression fields are each one of two spellings, and the request
/// shape is what refuses a caller who sent both — by the time it is here, one has been
/// chosen.
#[derive(Debug, Default)]
pub struct Selection<'a> {
    pub projection: Projection<'a>,
    pub predicate: Predicate<'a>,
    /// A shape on the sky, and the columns to test it against. Separate from
    /// [`Self::predicate`] rather than folded into it because a spatial constraint has to
    /// be recognisable to be planned on, and the two are conjoined: a row must be inside
    /// the region *and* satisfy the predicate.
    pub spatial: Option<Spatial<'a>>,
    /// Most rows to return. `None` is however many match.
    pub limit: Option<usize>,
}

/// What the interface carrying this request promises about the order of the rows.
///
/// Not a preference: the two interfaces promise different things, so this decides what a
/// request is allowed to return.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// No promise about the order the rows arrive in.
    ///
    /// It says nothing about *which* rows: a `limit` is still answered reproducibly,
    /// whichever interface asked.
    #[default]
    Unspecified,
    /// The file's own row order.
    ///
    /// A file server hands over bytes, and a caller who asks a file for a subset of
    /// itself is asking about that file rather than for a bag of rows — so the rows
    /// arrive in the order they are stored, and the same request twice is the same answer
    /// twice.
    File,
}

/// Whether this request must be answered the same way twice.
///
/// A `limit` is the reason this is not simply [`Order`]. Without a fixed order, a limit
/// turns "how many rows" into "which rows", and the same request returns a different
/// subset each time — which a caller cannot distinguish from the data having changed. So
/// a limited request is answered reproducibly whatever its interface promises about
/// order, and an unlimited one under [`Order::Unspecified`] needs nothing, since every
/// matching row comes back and the set cannot differ.
fn reproducible(selection: &Selection<'_>, order: Order) -> bool {
    order == Order::File || selection.limit.is_some()
}

/// The rows a [`Selection`] matched, plus the schema they have — which is the
/// projection's schema, not the file's, and is the only thing left to describe the
/// result when no row matched at all.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// How many bytes of the source file the scan fetched to answer this.
    ///
    /// Next to the row count it says what the predicate and the projection were worth: a
    /// point lookup that reads a megabyte of a gigabyte file pruned, and one that reads
    /// the gigabyte did not.
    pub data_bytes_read: u64,
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
            .field("data_bytes_read", &self.data_bytes_read)
            .finish()
    }
}

/// Everything we know about parquet point lookups, in one place.
///
/// None of it assumes anything about the file: DataFusion uses a page index or a bloom
/// filter when the file happens to have one, and falls back to row-group statistics
/// when it does not.
pub(crate) fn session_config(reproducible: bool) -> SessionConfig {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    // A file scan hands byte-range morsels out to whichever partition goes idle, so the
    // partition holding the start of the file is not reliably partition 0 — and reading
    // the partitions back in index order is the whole of how a stable order is kept. Each
    // partition's own rows are contiguous and ascending either way, so with this left on
    // the answer comes out *nearly* ordered, and often exactly ordered, which is worse
    // than plainly wrong: it passes a spot check and fails in production.
    //
    // The cost is that a partition which finishes early no longer helps a slow sibling,
    // so this is off only for the requests that need it.
    options.execution.enable_file_stream_work_stealing = !reproducible;
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
    order: Order,
) -> Result<QueryResult, ApiError> {
    execute(file, selection, limits, order)
        .await
        .map(|(result, _)| result)
}

/// The same, and how many partitions the scan was split into.
///
/// The count is what tells a test that a file was actually read in parallel. Without it a
/// test of ordering passes on any file DataFusion decided not to split — which is any
/// file below `repartition_file_min_size` — while checking nothing at all.
pub(crate) async fn execute(
    file: &RemoteFile,
    selection: &Selection<'_>,
    limits: sql::Limits,
    order: Order,
) -> Result<(QueryResult, usize), ApiError> {
    let reproducible = reproducible(selection, order);
    let ctx = SessionContext::new_with_config(session_config(reproducible));
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

    // The region ahead of the caller's predicate, which is the order the two are cheapest
    // in: a region lowers to comparisons against the coordinate columns that row-group
    // statistics can prune on, so it decides what there is for the predicate to run over.
    // Both are pushed into the scan, and `reorder_filters` is what settles the order they
    // actually run in.
    let df = match &selection.spatial {
        None => df,
        Some(spatial) => {
            let expr = region::predicate(df.schema(), spatial)?;
            df.filter(expr)?
        }
    };
    // The predicate next, so it may name a column the projection does not return —
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
    // `DataFrame::limit` puts a `CoalescePartitionsExec` above the scan, which takes
    // whichever rows arrive first. That is not a symptom of the unstable answer, it is
    // the cause — so a request that has to be reproducible keeps the limit out of the
    // plan and applies it while reading instead.
    let df = match (selection.limit, reproducible) {
        (Some(rows), false) => df.limit(0, Some(rows))?,
        _ => df,
    };

    let schema = Arc::new(df.schema().as_arrow().clone());
    let plan = df.create_physical_plan().await?;
    let partitions = plan.output_partitioning().partition_count();
    let task = ctx.task_ctx();
    // Every arm hands the plan on by clone rather than by value: the metrics are read off
    // it once it has run, so it has to outlive the execution.
    let batches = match (reproducible, selection.limit) {
        (false, _) => collect(Arc::clone(&plan), task).await?,
        // Every partition at once, then put back in index order: the read is as parallel
        // as it ever was, and nothing is merged on completion.
        (true, None) => collect_partitioned(Arc::clone(&plan), task)
            .await?
            .into_iter()
            .flatten()
            .collect(),
        (true, Some(rows)) => first_rows_in_order(Arc::clone(&plan), task, rows).await?,
    };
    let data_bytes_read = data_bytes_read(plan.as_ref());
    Ok((
        QueryResult {
            schema,
            batches,
            data_bytes_read,
        },
        partitions,
    ))
}

/// How many bytes the scan fetched, read off the plan once it has finished running.
///
/// Free: DataFusion counts this whether or not anything asks for it, and the counter is
/// final as soon as the last batch is in. Summed over the tree because it belongs to the
/// scan node rather than to whatever sits above it.
///
/// What it counts is the ranges the parquet reader asked for — the data pages, and the
/// bloom filters the predicate consulted. It does not count the footer or the page index:
/// DataFusion fetches those from the store directly rather than through the reader that
/// holds the counter, so no counter sits on them. Counting them too would mean wrapping
/// the store in one of our own.
fn data_bytes_read(plan: &dyn ExecutionPlan) -> u64 {
    let own = plan
        .metrics()
        // DataFusion's own name for it.
        .and_then(|metrics| metrics.sum_by_name("bytes_scanned"))
        .map_or(0, |value| value.as_usize() as u64);
    own + plan
        .children()
        .iter()
        .map(|child| data_bytes_read(child.as_ref()))
        .sum::<u64>()
}

/// The first `limit` rows, reading partitions in index order and stopping there.
///
/// One partition at a time, on purpose. Reading them concurrently would be pointless
/// work: the answer is the front of the file, so a partition covering the middle of it
/// contributes nothing until everything before it is exhausted. A stream that is never
/// polled never reads its byte range, which is what keeps `limit=100` cheap on a large
/// file — and what a plan-level limit cannot do without also choosing arbitrary rows.
async fn first_rows_in_order(
    plan: Arc<dyn ExecutionPlan>,
    task: Arc<TaskContext>,
    limit: usize,
) -> Result<Vec<RecordBatch>, ApiError> {
    let mut collected = Vec::new();
    let mut rows = 0;
    for mut stream in execute_stream_partitioned(plan, task)? {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if rows + batch.num_rows() >= limit {
                collected.push(batch.slice(0, limit - rows));
                return Ok(collected);
            }
            rows += batch.num_rows();
            collected.push(batch);
        }
    }
    Ok(collected)
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
    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::parquet::schema::types::ColumnPath;

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

    /// How a fixture is written, for the cases that care.
    ///
    /// Every field here changes what the engine has to work with rather than what the
    /// answer should be, which is the point: the ordering guarantee cannot depend on how
    /// a file was written, because a caller's file is written by whichever importer they
    /// used.
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Shape {
        pub rows: i64,
        pub row_group_rows: usize,
        /// `Page` writes the page index as well as row-group statistics, `Chunk` only the
        /// latter, `None` neither.
        pub statistics: EnabledStatistics,
        pub bloom_filter: bool,
    }

    impl std::fmt::Display for Shape {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{} rows in groups of {}, statistics {:?}, bloom filter {}",
                self.rows, self.row_group_rows, self.statistics, self.bloom_filter
            )
        }
    }

    /// A file big enough to be read in parallel, with an ascending `objectid` so that
    /// file order is checkable and a scattered `mag` so that ZSTD cannot shrink it below
    /// the size at which DataFusion bothers to split a file.
    pub(crate) fn shaped(shape: Shape) -> Vec<u8> {
        let rows = shape.rows;
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
        // A value that looks nothing like its neighbours, so a row group's min/max spans
        // most of the range and prunes nothing — and so that ZSTD cannot shrink the file
        // below the size DataFusion bothers to split. Every conversion here is exact: a
        // `u32` is representable in an `f64`, which a `u64` is not.
        let scattered = |i: i64| {
            let mixed = i.cast_unsigned().wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let bits = u32::try_from(mixed >> 32).expect("a u64 shifted by 32 fits a u32");
            f64::from(bits) / f64::from(u32::MAX)
        };
        let mag: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|i| scattered(i) * 25.0),
        ));
        let noise: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|i| scattered(i + 7)),
        ));
        let band: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| if i % 2 == 0 { "g" } else { "r" }),
        ));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("mag", mag, true),
            ("noise", noise, true),
            ("band", band, true),
        ])
        .expect("the fixture batch");

        let mut properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(shape.row_group_rows))
            .set_statistics_enabled(shape.statistics)
            .set_compression(Compression::ZSTD(Default::default()));
        if shape.bloom_filter {
            properties =
                properties.set_column_bloom_filter_enabled(ColumnPath::from("objectid"), true);
        }
        let properties = properties.build();

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).expect("a writer");
        writer.write(&batch).expect("write the batch");
        writer.close().expect("close the file");
        buffer
    }

    /// The shapes worth crossing with the requests below.
    ///
    /// Each row count is above `repartition_file_min_size`, which is 1 MiB and the size
    /// below which DataFusion does not split a file at all — a smaller fixture would make
    /// every case here pass while reading one partition and checking nothing.
    /// The row count is the smallest that clears that threshold with room to spare —
    /// writing these files is most of what this test costs, and a bigger one buys
    /// nothing once the file is split.
    fn shapes() -> Vec<Shape> {
        let mut shapes = Vec::new();
        // Few large groups and many small ones: how many there are is what decides how
        // much there is to hand out to threads.
        for row_group_rows in [2_000, 25_000] {
            for statistics in [
                EnabledStatistics::Page,
                EnabledStatistics::Chunk,
                EnabledStatistics::None,
            ] {
                shapes.push(Shape {
                    rows: 120_000,
                    row_group_rows,
                    statistics,
                    // Written only where the page index is, so that the two cases differ
                    // in more than one structure and a file with everything is covered.
                    bloom_filter: statistics == EnabledStatistics::Page,
                });
            }
        }
        shapes
    }

    /// The `objectid` column, in the order the rows came back.
    fn ids(result: &QueryResult) -> Vec<i64> {
        let mut ids = Vec::new();
        for batch in &result.batches {
            let column = batch.column_by_name("objectid").expect("objectid");
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column");
            ids.extend(values.values().iter().copied());
        }
        ids
    }

    /// A fixture on disk, and the way a mount opens one.
    fn on_disk(bytes: &[u8]) -> (tempfile::TempDir, RemoteFile) {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("part0.parquet");
        std::fs::write(&path, bytes).expect("write the fixture");
        let file = crate::storage::open_mounted(&path).expect("open the fixture");
        (dir, file)
    }

    /// The file-server mode's promise, across every shape: the rows arrive in the file's
    /// own order.
    ///
    /// `objectid` ascends with the file, so file order is exactly an ascending result.
    /// Each case also asserts the scan really was split, since the guarantee is only
    /// interesting when there was more than one partition to put back together.
    #[tokio::test(flavor = "multi_thread")]
    async fn file_order_holds_however_the_file_was_written() {
        for shape in shapes() {
            let bytes = shaped(shape);
            assert!(
                bytes.len() > 1024 * 1024,
                "{shape} is below the size DataFusion splits, so this would check nothing"
            );
            let (_dir, file) = on_disk(&bytes);

            for (what, selection) in [
                (
                    "every row",
                    Selection {
                        projection: Projection::All,
                        predicate: Predicate::All,
                        spatial: None,
                        limit: None,
                    },
                ),
                (
                    "a projection",
                    Selection {
                        projection: Projection::Columns("objectid, mag"),
                        predicate: Predicate::All,
                        spatial: None,
                        limit: None,
                    },
                ),
                (
                    "a predicate matching rows throughout",
                    Selection {
                        projection: Projection::All,
                        predicate: Predicate::Where("mag < 0.5"),
                        spatial: None,
                        limit: None,
                    },
                ),
                (
                    "a predicate and a projection",
                    Selection {
                        projection: Projection::Columns("objectid"),
                        predicate: Predicate::Where("mag < 0.5"),
                        spatial: None,
                        limit: None,
                    },
                ),
            ] {
                let (result, partitions) = execute(&file, &selection, limits(), Order::File)
                    .await
                    .unwrap_or_else(|error| panic!("{what} on {shape}: {error}"));
                assert!(
                    partitions > 1,
                    "{what} on {shape} ran in {partitions} partition(s), so nothing was \
                     put back together and this case proves nothing"
                );
                let ids = ids(&result);
                assert!(!ids.is_empty(), "{what} on {shape} matched nothing");
                let ordered = ids.windows(2).all(|pair| pair[0] < pair[1]);
                assert!(
                    ordered,
                    "{what} on {shape} came back out of file order: {} of {} adjacent \
                     pairs are descending",
                    ids.windows(2).filter(|pair| pair[0] >= pair[1]).count(),
                    ids.len() - 1
                );
            }
        }
    }

    /// The same request twice is the same answer twice — which is the half of the
    /// guarantee that a single ordered run cannot show.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_file_server_answers_the_same_way_twice() {
        let shape = Shape {
            rows: 120_000,
            row_group_rows: 5_000,
            statistics: EnabledStatistics::Page,
            bloom_filter: false,
        };
        let (_dir, file) = on_disk(&shaped(shape));
        let selection = Selection {
            projection: Projection::Columns("objectid"),
            predicate: Predicate::Where("mag < 1.0"),
            spatial: None,
            limit: None,
        };
        let first = ids(&run(&file, &selection, limits(), Order::File).await.unwrap());
        for again in 0..3 {
            let repeated = ids(&run(&file, &selection, limits(), Order::File).await.unwrap());
            assert_eq!(first, repeated, "run {again} differed");
        }
    }

    /// A `limit` returns the same rows every time, in both modes.
    ///
    /// The API promises nothing about order, but which rows come back is a different
    /// question: a limit over an unstable order is a different subset per request, and a
    /// caller cannot tell that from the data having changed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_limit_returns_the_same_rows_every_time() {
        let shape = Shape {
            rows: 120_000,
            row_group_rows: 5_000,
            statistics: EnabledStatistics::Page,
            bloom_filter: false,
        };
        let (_dir, file) = on_disk(&shaped(shape));

        for order in [Order::File, Order::Unspecified] {
            for predicate in [Predicate::All, Predicate::Where("mag < 1.0")] {
                let selection = Selection {
                    projection: Projection::Columns("objectid"),
                    predicate,
                    spatial: None,
                    limit: Some(100),
                };
                let first = ids(&run(&file, &selection, limits(), order).await.unwrap());
                assert_eq!(first.len(), 100, "{order:?} {predicate:?}");
                for again in 0..3 {
                    let repeated = ids(&run(&file, &selection, limits(), order).await.unwrap());
                    assert_eq!(
                        first, repeated,
                        "{order:?} {predicate:?}: run {again} returned a different set of rows"
                    );
                }
            }
        }
    }

    /// And under the file-server's order, a limit is the *first* rows rather than an
    /// arbitrary hundred of them.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_limit_in_file_order_takes_the_front_of_the_file() {
        let shape = Shape {
            rows: 120_000,
            row_group_rows: 5_000,
            statistics: EnabledStatistics::Chunk,
            bloom_filter: false,
        };
        let (_dir, file) = on_disk(&shaped(shape));
        let selection = Selection {
            projection: Projection::Columns("objectid"),
            predicate: Predicate::All,
            spatial: None,
            limit: Some(100),
        };
        let result = run(&file, &selection, limits(), Order::File).await.unwrap();
        assert_eq!(ids(&result), (0..100).collect::<Vec<i64>>());
    }

    /// A result says how much of the file it read, and the number moves with what was
    /// asked.
    ///
    /// Both halves are the test. A counter wired to nothing reports zero, and one summed
    /// off the wrong node — or read before the scan finished — reports the same number
    /// for a point lookup as for reading every row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_result_says_how_much_of_the_file_it_read() {
        let shape = Shape {
            rows: 120_000,
            row_group_rows: 5_000,
            statistics: EnabledStatistics::Page,
            bloom_filter: true,
        };
        let bytes = shaped(shape);
        let (_dir, file) = on_disk(&bytes);

        let everything = run(
            &file,
            &Selection {
                projection: Projection::All,
                predicate: Predicate::All,
                spatial: None,
                limit: None,
            },
            limits(),
            Order::Unspecified,
        )
        .await
        .unwrap();
        assert!(
            everything.data_bytes_read > 0,
            "reading every row of a {} byte file scanned nothing",
            bytes.len()
        );

        let lookup = run(
            &file,
            &Selection {
                projection: Projection::Columns("objectid"),
                predicate: Predicate::Where("objectid = 61234"),
                spatial: None,
                limit: None,
            },
            limits(),
            Order::Unspecified,
        )
        .await
        .unwrap();
        assert_eq!(lookup.num_rows(), 1);
        // A lookup that read nothing at all would satisfy the comparison below without
        // the counter working.
        assert!(lookup.data_bytes_read > 0, "a point lookup scanned nothing");
        assert!(
            lookup.data_bytes_read * 10 < everything.data_bytes_read,
            "a point lookup scanned {} bytes against {} for the whole file, so the count \
             is not following what was read",
            lookup.data_bytes_read,
            everything.data_bytes_read
        );
    }

    /// Limits generous enough not to be what any of the above is measuring.
    fn limits() -> sql::Limits {
        (&crate::config::LimitsConfig::default()).into()
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
            data_bytes_read: 0,
        };
        assert_eq!(to_json(&empty).unwrap(), Vec::<serde_json::Value>::new());
    }
}
