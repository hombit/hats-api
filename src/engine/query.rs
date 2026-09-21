//! Reading rows out of a single parquet file: a projection, a row predicate and a row
//! cap, each of which the caller may leave out. Says nothing about where the data lives.
//!
//! Nothing is cached between requests: every call builds its own session, its own
//! object store, and reads the file's metadata from scratch.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, collect_partitioned, execute_stream,
    execute_stream_partitioned,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::engine::sql;
use crate::error::ApiError;
use crate::sky::region::{self, Spatial};
use crate::storage::RemoteFile;

/// Which columns come back.
#[derive(Debug, Default, Clone, Copy)]
pub enum Projection<'a> {
    /// Every column the file has.
    #[default]
    All,
    /// Column names, one per element, and no expressions.
    Columns(&'a [String]),
    /// The same names comma-separated in one string, which is all a url's query string can
    /// carry. One wire form of [`Self::Columns`]: the two lower through the same code and
    /// mean the same thing.
    ColumnText(&'a str),
}

/// Which rows come back.
#[derive(Debug, Default, Clone, Copy)]
pub enum Predicate<'a> {
    /// Every row.
    #[default]
    All,
    /// One boolean SQL expression over this file's columns.
    Filters(&'a str),
    /// The same again, where `&&`, `,` and `;` stand in for `AND`, `AND` and `OR` — the
    /// spellings a url's query string needs to join two conditions inside one parameter.
    FilterText(&'a str),
}

/// The same two, owning their text.
///
/// A streamed answer outlives the request it came from — the body is still being written
/// when the handler returns — so what the rows are selected by cannot borrow from the
/// request body. These own it, and [`OwnedProjection::view`] hands back the borrowed form
/// the query layer takes, so there is one selection type below this and two ways of
/// holding its strings.
#[derive(Debug, Default, Clone)]
pub enum OwnedProjection {
    #[default]
    All,
    Columns(Vec<String>),
    ColumnText(String),
}

impl OwnedProjection {
    pub fn view(&self) -> Projection<'_> {
        match self {
            Self::All => Projection::All,
            Self::Columns(names) => Projection::Columns(names),
            Self::ColumnText(text) => Projection::ColumnText(text),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub enum OwnedPredicate {
    #[default]
    All,
    Filters(String),
    FilterText(String),
}

impl OwnedPredicate {
    pub fn view(&self) -> Predicate<'_> {
        match self {
            Self::All => Predicate::All,
            Self::Filters(text) => Predicate::Filters(text),
            Self::FilterText(text) => Predicate::FilterText(text),
        }
    }
}

impl From<Projection<'_>> for OwnedProjection {
    fn from(projection: Projection<'_>) -> Self {
        match projection {
            Projection::All => Self::All,
            Projection::Columns(names) => Self::Columns(names.to_vec()),
            Projection::ColumnText(text) => Self::ColumnText(text.to_owned()),
        }
    }
}

impl From<Predicate<'_>> for OwnedPredicate {
    fn from(predicate: Predicate<'_>) -> Self {
        match predicate {
            Predicate::All => Self::All,
            Predicate::Filters(text) => Self::Filters(text.to_owned()),
            Predicate::FilterText(text) => Self::FilterText(text.to_owned()),
        }
    }
}

/// What to read. The projection and the predicate each arrive in one of two wire forms, a
/// body's or a query string's, and mean the same thing in either.
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
impl fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    // Off by default. Evaluates the predicate during decoding, so the data columns of
    // non-matching row groups and pages are never fetched. Worth a fifth of the bytes on a
    // limited scan and nothing at all where statistics already ruled the row groups out;
    // it never cost anything measurable. `reorder_filters` is what keeps that true — with
    // it off, a limited scan read *more* than with no pushdown at all.
    parquet.pushdown_filters = true;
    parquet.reorder_filters = true;
    // All three on by default, and all three set here because which of them carries a
    // query is decided by how the file was written — which this service does not get to
    // assume. None is redundant with the others:
    //
    // - Many row groups: `pruning` does the work. A point lookup over twenty of them reads
    //   5 MB of a 100 MB file rather than all of it, and the page index trims 5% more.
    // - One row group: `pruning` does nothing whatever, since there is no second row group
    //   to rule out. The page index is then the only thing between a lookup and a full
    //   scan, and it is worth on its own what `pruning` is worth on the other shape —
    //   84 MB and 102 ms become 16 MB and 18 ms.
    // - A bloom filter is worthless wherever min/max has already found the rows, and on
    //   one row group of unsorted ids it takes a lookup for an absent id from 6 MB and
    //   7 ms to 1 MB and 0.4 ms.
    //
    // The two read-side switches cost nothing on a file whose writer wrote neither
    // structure — same bytes, same time — so leaving them on is insurance with no premium.
    // Which structures a file carries is the writer's decision and changes without notice,
    // so the only thing this side can do is be ready for each of them.
    parquet.pruning = true;
    parquet.enable_page_index = true;
    parquet.bloom_filter_on_read = true;
    // `target_partitions` and `batch_size` are deliberately left at DataFusion's defaults.
    // The default partition count is the core count, and a full scan is six times slower
    // at one; batch size made no difference at any size tried.
    config
}

/// The context every request is answered from.
///
/// One function rather than a `SessionContext` built at each call site, so that what a
/// caller may call is one list: the registry a request plans against is decided here and
/// the configuration above it, and a second construction somewhere else would be a second
/// answer to both.
pub(crate) fn session_context(reproducible: bool) -> SessionContext {
    SessionContext::new_with_config(session_config(reproducible))
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

/// The rows of one query, as they come off the scan.
///
/// What a collected answer and a streamed one share is everything up to the first batch:
/// the same context, the same plan, the same decision about order. They part company at
/// how the batches are taken — `execute` takes them all and this hands them over one at
/// a time — so the planning is here, once, and each of the two says only what it does with
/// what comes out.
/// `SessionContext` has no `Debug`, and what this holds is a plan rather than an answer.
pub struct Planned {
    pub schema: SchemaRef,
    /// Kept alive for as long as the rows are: a plan runs on its context's task pool, and
    /// a stream still being polled after the context went would be reading from nothing.
    context: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    reproducible: bool,
    limit: Option<usize>,
}

impl fmt::Debug for Planned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Planned")
            .field("schema", &self.schema)
            .field("partitions", &self.partitions())
            .field("reproducible", &self.reproducible)
            .field("limit", &self.limit)
            .finish()
    }
}

impl Planned {
    /// How many partitions the scan was split into.
    pub fn partitions(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }

    /// What the scan fetched. Final once the last batch is in, and zero before the first.
    pub fn data_bytes_read(&self) -> u64 {
        data_bytes_read(self.plan.as_ref())
    }

    /// The batches, in whatever order this query promised.
    ///
    /// Three shapes, and they are the streaming counterparts of what `execute` collects:
    ///
    /// - **Nothing promised**: the merged stream, every partition polled at once, which is
    ///   what `collect` does without the collecting.
    /// - **The file's order**: the partitions chained in index order. That is the cost of
    ///   streaming rather than collecting — `collect_partitioned` runs them concurrently
    ///   and puts them back in order afterwards, which needs them all in memory, and this
    ///   does not. A partition is not polled until the one before it is done, so a
    ///   streamed answer in file order is read serially.
    /// - **The file's order, with a limit**: the same chain, stopped. A stream that is
    ///   never polled reads no bytes, so the partitions past the limit cost nothing.
    pub fn batches(&self) -> Result<BoxStream<'static, Result<RecordBatch, ApiError>>, ApiError> {
        let task = self.context.task_ctx();
        let plan = Arc::clone(&self.plan);
        Ok(match (self.reproducible, self.limit) {
            (false, _) => execute_stream(plan, task)?
                .map(|batch| batch.map_err(ApiError::from))
                .boxed(),
            (true, None) => in_index_order(plan, task, None)?,
            (true, Some(rows)) => in_index_order(plan, task, Some(rows))?,
        })
    }
}

/// Plan the query and stop there, with nothing read.
pub async fn plan(
    file: &RemoteFile,
    selection: &Selection<'_>,
    limits: sql::Limits,
    order: Order,
) -> Result<Planned, ApiError> {
    let (context, plan, schema) = planned(file, selection, limits, order).await?;
    Ok(Planned {
        schema,
        context,
        plan,
        reproducible: reproducible(selection, order),
        limit: selection.limit,
    })
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
    let (ctx, plan, schema) = planned(file, selection, limits, order).await?;
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

/// The context and the physical plan, with nothing read.
///
/// Both ways of taking the rows start here, so what a query *means* — the region, the
/// predicate, the projection, whether the limit goes into the plan — is decided once.
async fn planned(
    file: &RemoteFile,
    selection: &Selection<'_>,
    limits: sql::Limits,
    order: Order,
) -> Result<(SessionContext, Arc<dyn ExecutionPlan>, SchemaRef), ApiError> {
    let reproducible = reproducible(selection, order);
    let ctx = session_context(reproducible);
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
    // A file of no bytes at all is not a parquet file, but schema inference reads it as a
    // table of no columns rather than failing, and every query against it then answers
    // "no rows" — which a caller cannot tell from a file that really is empty. The
    // parquet output path already refuses it, when it goes to read the footer to copy the
    // layout, so without this the status depends on the format that was asked for.
    if df.schema().fields().is_empty() {
        return Err(ApiError::bad_request(
            "this file describes no columns; it is empty, or not a parquet file",
        ));
    }
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
        Predicate::Filters(text) => {
            let expr = sql::filters(&state, df.schema(), text, limits)?;
            df.filter(expr)?
        }
        Predicate::FilterText(text) => {
            let expr = sql::filter_text(&state, df.schema(), text, limits)?;
            df.filter(expr)?
        }
    };
    let df = match selection.projection {
        Projection::All => df,
        Projection::Columns(names) => {
            let exprs = sql::columns(&state, df.schema(), names, limits)?;
            df.select(exprs)?
        }
        Projection::ColumnText(list) => {
            let exprs = sql::column_text(&state, df.schema(), list, limits)?;
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
    Ok((ctx, plan, schema))
}

/// The partitions chained in index order, stopping at `limit` rows where there is one.
///
/// One partition at a time, on purpose, which is the same reason [`first_rows_in_order`]
/// has: the front of the file is the front of partition zero, so a partition covering the
/// middle contributes nothing until everything before it is exhausted, and a stream that
/// is never polled never reads its byte range.
fn in_index_order(
    plan: Arc<dyn ExecutionPlan>,
    task: Arc<TaskContext>,
    limit: Option<usize>,
) -> Result<BoxStream<'static, Result<RecordBatch, ApiError>>, ApiError> {
    // No rows wanted, so no partition is polled and no byte of the file is read.
    if limit == Some(0) {
        return Ok(futures::stream::empty().boxed());
    }
    let streams = execute_stream_partitioned(plan, task)?;
    Ok(futures::stream::iter(streams)
        .flatten()
        .map(|batch| batch.map_err(ApiError::from))
        // `scan` rather than `take_while`, because the batch that reaches the limit is cut
        // to it rather than dropped: a limit is how many rows, and the batch boundaries
        // are the file's business rather than the caller's.
        .scan(0_usize, move |taken, batch| {
            let sent = match (limit, batch) {
                (None, batch) => Some(batch),
                (Some(_), Err(error)) => Some(Err(error)),
                (Some(limit), Ok(batch)) if *taken < limit => {
                    let room = limit - *taken;
                    *taken += batch.num_rows();
                    Some(Ok(match batch.num_rows() > room {
                        true => batch.slice(0, room),
                        false => batch,
                    }))
                }
                // Enough rows already: the partitions after this one are never polled, so
                // their byte ranges are never read.
                (Some(_), Ok(_)) => None,
            };
            futures::future::ready(sent)
        })
        .boxed())
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
pub(crate) fn data_bytes_read(plan: &dyn ExecutionPlan) -> u64 {
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
    // No rows wanted, so no partition is polled and no byte of the file is read. The
    // answer is then the schema alone, which is what asks a file what columns it has.
    // Without this the first batch is fetched and sliced away to nothing.
    if limit == 0 {
        return Ok(Vec::new());
    }
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

#[cfg(test)]
pub(crate) mod tests {
    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
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

    /// The same ten rows with a mixed-case column beside them, which is what an astronomy
    /// catalog's columns actually look like — `Gmag`, `objectId`, read off the file.
    ///
    /// Which spellings reach such a column is a rule rather than an accident, so a test of it
    /// needs a file whose own spelling is neither what SQL would fold to nor what a caller
    /// would type.
    pub(crate) fn mixed_case_fixture() -> Vec<u8> {
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
        let gmag: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..10).map(|i| 15.0 + f64::from(i)),
        ));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("Gmag", gmag, true),
        ])
        .expect("the fixture batch");

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("a writer");
        writer.write(&batch).expect("write the batch");
        writer.close().expect("close the file");
        buffer
    }

    /// Three rows holding a null, an empty string and an ordinary value in one text column.
    ///
    /// The first two are what the delimited formats cannot tell apart without a sentinel, so
    /// a test of `dsv_null_value` needs a file carrying both rather than either alone.
    pub(crate) fn null_fixture() -> Vec<u8> {
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..3));
        let band: ArrayRef = Arc::new(StringArray::from(vec![None, Some(""), Some("g")]));
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

    /// Ten rows spread along a line of declination, with an id to select.
    ///
    /// Positions rather than values, for the tests that ask a region something: they are far
    /// enough apart that a small circle holds exactly one of them, so a test can say which row
    /// and not merely how many.
    pub(crate) fn sky_fixture() -> Vec<u8> {
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
        let ra: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..10).map(|i| 40.0 + f64::from(i)),
        ));
        let dec: ArrayRef = Arc::new(Float64Array::from_iter_values((0..10).map(|_| -20.0)));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("ra", ra, true),
            ("dec", dec, true),
        ])
        .expect("the fixture batch");

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("a writer");
        writer.write(&batch).expect("write the batch");
        writer.close().expect("close the file");
        buffer
    }

    /// A file with a struct column, which is how a HATS catalog carries a light curve: one
    /// row per object, and the measurements packed into a field per column.
    ///
    /// `sources.mjd` is the spelling that reaches one, so this is what a test of nested
    /// names needs. The struct holds a list and a scalar, since a caller's reason for
    /// reaching into one is usually the list.
    pub(crate) fn nested_fixture() -> Vec<u8> {
        use datafusion::arrow::array::{ArrayRef, Float64Builder, ListBuilder, StructArray};
        use datafusion::arrow::datatypes::{DataType, Field};

        let mut mjd = ListBuilder::new(Float64Builder::new());
        for row in 0..3 {
            for point in 0..3 {
                mjd.values().append_value(f64::from(row * 10 + point));
            }
            mjd.append(true);
        }
        let mjd: ArrayRef = Arc::new(mjd.finish());
        let band: ArrayRef = Arc::new(StringArray::from_iter_values(["g", "r", "i"]));
        let sources: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "mjd",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                )),
                mjd,
            ),
            (Arc::new(Field::new("band", DataType::Utf8, true)), band),
        ]));
        let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..3));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("objectid", objectid, false),
            ("sources", sources, true),
        ])
        .expect("the nested fixture batch");

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

    impl fmt::Display for Shape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                        projection: Projection::ColumnText("objectid, mag"),
                        predicate: Predicate::All,
                        spatial: None,
                        limit: None,
                    },
                ),
                (
                    "a predicate matching rows throughout",
                    Selection {
                        projection: Projection::All,
                        predicate: Predicate::Filters("mag < 0.5"),
                        spatial: None,
                        limit: None,
                    },
                ),
                (
                    "a predicate and a projection",
                    Selection {
                        projection: Projection::ColumnText("objectid"),
                        predicate: Predicate::Filters("mag < 0.5"),
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
            projection: Projection::ColumnText("objectid"),
            predicate: Predicate::Filters("mag < 1.0"),
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
            for predicate in [Predicate::All, Predicate::Filters("mag < 1.0")] {
                let selection = Selection {
                    projection: Projection::ColumnText("objectid"),
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
            projection: Projection::ColumnText("objectid"),
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
                projection: Projection::ColumnText("objectid"),
                predicate: Predicate::Filters("objectid = 61234"),
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
}
