//! What DataFusion actually does with the requests this service builds.
//!
//! Measurement rather than assertion. Every case here prints what it observed and none
//! of them fail on a number, because the numbers are the output: a threshold written in
//! now would encode today's engine as the intended behaviour, which is the opposite of
//! what this is for. What it can fail on is a claim being *false* — that is what the
//! `order` cases assert, once the answer is known.
//!
//! Skipped unless `HATS_ENGINE_MEASURE` is set. Building the fixture writes a
//! hundred-odd MB and the whole binary takes minutes, neither of which belongs in
//! `cargo test`. `HATS_ENGINE_ROWS` overrides the row count.
//!
//! ```text
//! HATS_ENGINE_MEASURE=1 cargo test --release --test engine -- --nocapture --test-threads=1
//! ```
//!
//! `--release` matters: a debug build measures arrow's bounds checks rather than the
//! engine's choices. `--test-threads=1` because two cases sharing the CPU measure each
//! other.

// The report *is* the result here. `print_stdout` is warned about across the crate
// because a service's stdout is its log and nothing in a request path may write there;
// this binary is not a request path, and a measurement nobody can read measures nothing.
#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use object_store::ObjectStore;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, collect_partitioned, displayable,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use hats_api::query::{self, Order, Predicate, Projection, Selection};
use hats_api::sql::Limits;
use hats_api::storage::{self, RemoteFile};
use tempfile::TempDir;

/// Generous, and not what is being measured: every expression here is a few nodes.
const LIMITS: Limits = Limits {
    max_depth: 50,
    max_nodes: 50_000,
};

/// Rows in the fixture, and rows per row group. Several row groups is the whole point —
/// one row group cannot show a difference in what was pruned or in what order it came
/// back.
const DEFAULT_ROWS: i64 = 1_000_000;
const DEFAULT_ROW_GROUP_ROWS: usize = 50_000;

fn rows() -> i64 {
    std::env::var("HATS_ENGINE_ROWS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_ROWS)
}

/// Rows per row group. Worth varying independently of the row count: how many row groups
/// there are is what decides how much there is to hand out to threads, and a file can be
/// large with few of them or small with many.
fn row_group_rows() -> usize {
    std::env::var("HATS_ENGINE_ROW_GROUP")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_ROW_GROUP_ROWS)
}

fn enabled() -> bool {
    std::env::var_os("HATS_ENGINE_MEASURE").is_some()
}

/// A value that looks nothing like its neighbours, so that a row group's min/max covers
/// most of the range and prunes nothing. Cheap and reproducible; the point is the
/// scatter, not the distribution.
fn scattered(i: i64) -> f64 {
    let mixed = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (mixed >> 11) as f64 / (1u64 << 53) as f64 * 25.0
}

/// A file shaped like a HATS partition, with a column for each thing worth asking about.
///
/// - `objectid` ascending, which makes it both the point-lookup target and the witness
///   for row order: the fixture's file order is its sorted order, so a result that comes
///   back ascending came back in file order.
/// - `objra`/`objdec` ascending, for a range over a sorted column — the shape where
///   row-group statistics are at their most useful.
/// - `mag` scattered, so its statistics are present and useless. This is the case a
///   HATS caller actually has: a magnitude cut over a file sorted by position.
/// - `mag_nostats` the same values with statistics turned off at write time, which is a
///   different thing from statistics that do not help.
/// - eight more columns so that a wide projection and a narrow one differ.
fn fixture(rows: i64) -> Vec<u8> {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let ra: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| 320.0 + (i as f64) * 1e-5),
    ));
    let dec: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| -12.0 - (i as f64) * 1e-5),
    ));
    let mag: ArrayRef = Arc::new(Float64Array::from_iter_values((0..rows).map(scattered)));
    let band: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|i| ["g", "r", "i", "z"][(i % 4) as usize]),
    ));

    let mut columns: Vec<(&str, ArrayRef, bool)> = vec![
        ("objectid", ids, false),
        ("objra", ra, true),
        ("objdec", dec, true),
        ("mag", Arc::clone(&mag), true),
        ("mag_nostats", mag, true),
        ("band", band, true),
    ];
    let padding: Vec<ArrayRef> = (0..8)
        .map(|k| {
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|i| scattered(i + k * 7)),
            )) as ArrayRef
        })
        .collect();
    // Leaked so the names can be `&'static str` alongside the literals above; this is a
    // test binary that writes one file and exits.
    for (k, column) in padding.into_iter().enumerate() {
        let name: &'static str = Box::leak(format!("extra{k}").into_boxed_str());
        columns.push((name, column, true));
    }

    let batch = RecordBatch::try_from_iter_with_nullable(columns).expect("fixture batch");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows()))
        // A bloom filter exists only if the writer wrote one, so the read-side switch has
        // nothing to use unless this is here. Whether the files this service actually
        // reads have them is a separate question, and one only their writer answers.
        .set_column_bloom_filter_enabled(ColumnPath::from("objectid"), true)
        .set_column_statistics_enabled(ColumnPath::from("mag_nostats"), EnabledStatistics::None)
        // What a real HATS partition is compressed with — `parquet-cpp-arrow` writing
        // ZSTD, read off one of `lsdb`'s catalogs. Snappy would understate the decode
        // cost, which is part of every number here.
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).expect("writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close");
    buffer
}

/// The fixture on disk, opened the way a file-server request opens one.
struct Fixture {
    _dir: TempDir,
    file: RemoteFile,
    bytes: usize,
}

fn write_fixture() -> Fixture {
    let rows = rows();
    let started = Instant::now();
    let bytes = fixture(rows);
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("part0.parquet");
    std::fs::write(&path, &bytes).expect("write fixture");
    println!(
        "fixture: {rows} rows, {} row groups, {:.1} MiB, written in {:.1}s",
        rows as usize / row_group_rows(),
        bytes.len() as f64 / (1 << 20) as f64,
        started.elapsed().as_secs_f64()
    );
    let file = storage::open_mounted(&path).expect("open the fixture");
    Fixture {
        _dir: dir,
        file,
        bytes: bytes.len(),
    }
}

/// The engine settings, so a case can vary one and leave the rest as the service ships
/// them.
///
/// Mirrors `query::session_config`, which is crate-private — deliberately duplicated
/// rather than exposed, since the point is to run settings this service does *not* ship
/// and a knob reachable from outside is a knob a request could reach.
#[derive(Debug, Clone, Copy)]
struct Knobs {
    pushdown_filters: bool,
    reorder_filters: bool,
    pruning: bool,
    enable_page_index: bool,
    bloom_filter_on_read: bool,
    /// `None` leaves DataFusion's default, which is the CPU count.
    target_partitions: Option<usize>,
    /// `None` leaves DataFusion's default of 8192.
    batch_size: Option<usize>,
    /// DataFusion's `enable_file_stream_work_stealing`, on by default: an idle partition
    /// reads byte-range morsels assigned to a sibling. That is what decides whether a
    /// partition's *index* still corresponds to its place in the file.
    work_stealing: bool,
}

impl Knobs {
    /// What the service ships today.
    const SHIPPED: Self = Self {
        pushdown_filters: true,
        reorder_filters: true,
        pruning: true,
        enable_page_index: true,
        bloom_filter_on_read: true,
        target_partitions: None,
        batch_size: None,
        work_stealing: true,
    };

    /// Every parquet switch off, as the floor to measure the others against. Not
    /// upstream's default — upstream leaves `pruning`, `enable_page_index` and
    /// `bloom_filter_on_read` on.
    const NOTHING: Self = Self {
        pushdown_filters: false,
        reorder_filters: false,
        pruning: false,
        enable_page_index: false,
        bloom_filter_on_read: false,
        ..Self::SHIPPED
    };

    fn config(self) -> SessionConfig {
        let mut config = SessionConfig::new();
        if let Some(partitions) = self.target_partitions {
            config = config.with_target_partitions(partitions);
        }
        if let Some(size) = self.batch_size {
            config = config.with_batch_size(size);
        }
        let options = config.options_mut();
        options.sql_parser.enable_ident_normalization = false;
        let parquet = &mut options.execution.parquet;
        parquet.pushdown_filters = self.pushdown_filters;
        parquet.reorder_filters = self.reorder_filters;
        parquet.pruning = self.pruning;
        parquet.enable_page_index = self.enable_page_index;
        parquet.bloom_filter_on_read = self.bloom_filter_on_read;
        options.execution.enable_file_stream_work_stealing = self.work_stealing;
        config
    }
}

/// What one run of one request cost, as the engine counted it.
#[derive(Debug, Default)]
struct Observed {
    num_rows: usize,
    elapsed: Duration,
    metrics: BTreeMap<String, usize>,
    /// The `objectid` of every row, in the order it came back. Empty for a projection
    /// that did not ask for it.
    ids: Vec<i64>,
    /// How many partitions the plan's last node produced, which is how many ways the
    /// scan was actually split.
    partitions: usize,
    plan: String,
}

impl Observed {
    fn metric(&self, name: &str) -> usize {
        self.metrics.get(name).copied().unwrap_or_default()
    }

    /// Whether the rows came back in the file's order, which for this fixture is
    /// ascending `objectid`.
    fn in_file_order(&self) -> bool {
        self.ids.windows(2).all(|pair| pair[0] < pair[1])
    }

    /// Where the order first breaks, as `(row index, id, next id)`.
    fn first_inversion(&self) -> Option<(usize, i64, i64)> {
        self.ids
            .windows(2)
            .enumerate()
            .find(|(_, pair)| pair[0] >= pair[1])
            .map(|(at, pair)| (at, pair[0], pair[1]))
    }

    fn inversions(&self) -> usize {
        self.ids
            .windows(2)
            .filter(|pair| pair[0] >= pair[1])
            .count()
    }
}

/// How the partitions' results are put back together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gather {
    /// `collect`, which is what `query::run` does today. DataFusion coalesces the
    /// partitions into one stream and takes whichever batch is ready, so the order the
    /// rows arrive in is the order the threads happened to finish.
    Coalesced,
    /// `collect_partitioned`, concatenated by partition index. Order then depends on
    /// whether DataFusion hands out the file's byte ranges to partitions in the file's
    /// own order, which is what the case below is for.
    ByPartition,
}

async fn observe(file: &RemoteFile, selection: &Selection<'_>, knobs: Knobs) -> Observed {
    observe_with(file, selection, knobs, Gather::Coalesced).await
}

/// One request, mirroring `query::run` closely enough to measure it, with the physical
/// plan kept so its metrics can be read.
async fn observe_with(
    file: &RemoteFile,
    selection: &Selection<'_>,
    knobs: Knobs,
    gather_mode: Gather,
) -> Observed {
    let ctx = SessionContext::new_with_config(knobs.config());
    ctx.register_object_store(&file.base, Arc::clone(&file.store));
    let options = ParquetReadOptions {
        file_extension: "",
        ..ParquetReadOptions::default()
    };
    let df = ctx
        .read_parquet(file.url.as_str(), options)
        .await
        .expect("read the fixture");
    let state = ctx.state();

    let df = match selection.predicate {
        Predicate::All => df,
        Predicate::Where(sql) => {
            let expr = hats_api::sql::predicate(&state, df.schema(), sql, LIMITS).expect("where");
            df.filter(expr).expect("filter")
        }
        Predicate::Filters(text) => {
            let expr = hats_api::sql::filters(&state, df.schema(), text, LIMITS).expect("filters");
            df.filter(expr).expect("filter")
        }
    };
    let df = match selection.projection {
        Projection::All => df,
        Projection::Select(sql) => {
            let exprs =
                hats_api::sql::projection(&state, df.schema(), sql, LIMITS).expect("select");
            df.select(exprs).expect("project")
        }
        Projection::Columns(list) => {
            let exprs = hats_api::sql::columns(&state, df.schema(), list, LIMITS).expect("columns");
            df.select(exprs).expect("project")
        }
    };
    let df = match selection.limit {
        Some(cap) => df.limit(0, Some(cap)).expect("limit"),
        None => df,
    };

    let plan = df.create_physical_plan().await.expect("physical plan");
    let started = Instant::now();
    let batches = match gather_mode {
        Gather::Coalesced => collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect"),
        // Concatenated by partition index rather than by whichever finished first. That
        // is the whole difference: the scan is just as parallel, and nothing here merges
        // on completion.
        Gather::ByPartition => collect_partitioned(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect_partitioned")
            .into_iter()
            .flatten()
            .collect(),
    };
    let elapsed = started.elapsed();

    let mut metrics = BTreeMap::new();
    gather(&plan, &mut metrics);
    Observed {
        num_rows: batches.iter().map(RecordBatch::num_rows).sum(),
        elapsed,
        metrics,
        ids: ids_of(&batches),
        partitions: plan.output_partitioning().partition_count(),
        plan: displayable(plan.as_ref()).indent(false).to_string(),
    }
}

/// Per output partition: how many rows it produced, and the first and last `objectid` in
/// it. Partitions are kept apart rather than concatenated, which is what makes it visible
/// whether each one covers a contiguous, ascending stretch of the file.
async fn partition_spans(
    file: &RemoteFile,
    selection: &Selection<'_>,
    knobs: Knobs,
) -> Vec<(usize, i64, i64)> {
    let ctx = SessionContext::new_with_config(knobs.config());
    ctx.register_object_store(&file.base, Arc::clone(&file.store));
    let options = ParquetReadOptions {
        file_extension: "",
        ..ParquetReadOptions::default()
    };
    let df = ctx
        .read_parquet(file.url.as_str(), options)
        .await
        .expect("read the fixture");
    let df = match selection.projection {
        Projection::Columns(list) => {
            let state = ctx.state();
            let exprs = hats_api::sql::columns(&state, df.schema(), list, LIMITS).expect("columns");
            df.select(exprs).expect("project")
        }
        _ => df,
    };
    let plan = df.create_physical_plan().await.expect("physical plan");
    collect_partitioned(plan, ctx.task_ctx())
        .await
        .expect("collect_partitioned")
        .iter()
        .map(|batches| {
            let ids = ids_of(batches);
            let first = ids.first().copied().unwrap_or(-1);
            let last = ids.last().copied().unwrap_or(-1);
            (ids.len(), first, last)
        })
        .collect()
}

/// Every metric in the plan tree, summed by name across nodes and partitions.
fn gather(plan: &Arc<dyn ExecutionPlan>, out: &mut BTreeMap<String, usize>) {
    if let Some(set) = plan.metrics() {
        for metric in set.iter() {
            let value = metric.value();
            // A timestamp summed across partitions is meaningless, and the elapsed times
            // are reported per node anyway.
            if matches!(
                value,
                MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_)
            ) {
                continue;
            }
            *out.entry(value.name().to_owned()).or_default() += value.as_usize();
        }
    }
    for child in plan.children() {
        gather(child, out);
    }
}

fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in batches {
        let Ok(column) = batch.column_by_name("objectid").ok_or(()) else {
            return Vec::new();
        };
        let Some(values) = column.as_any().downcast_ref::<Int64Array>() else {
            return Vec::new();
        };
        ids.extend(values.values().iter().copied());
    }
    ids
}

/// The requests this service actually builds, named as the report names them.
fn shapes(rows: i64) -> Vec<(&'static str, Selection<'static>)> {
    // Leaked for the same reason as the column names: one process, one file.
    let point: &'static str = Box::leak(format!("objectid = {}", rows / 2).into_boxed_str());
    let range: &'static str =
        Box::leak(format!("objra > {} AND objra < {}", 320.0 + 0.4, 320.0 + 0.6).into_boxed_str());
    vec![
        (
            "whole file",
            Selection {
                projection: Projection::All,
                predicate: Predicate::All,
                spatial: None,
                limit: None,
            },
        ),
        (
            "narrow projection",
            Selection {
                projection: Projection::Columns("objectid, objra, objdec"),
                predicate: Predicate::All,
                spatial: None,
                limit: None,
            },
        ),
        (
            "point lookup by id",
            Selection {
                projection: Projection::All,
                predicate: Predicate::Where(point),
                spatial: None,
                limit: None,
            },
        ),
        (
            "range over a sorted column",
            Selection {
                projection: Projection::All,
                predicate: Predicate::Where(range),
                spatial: None,
                limit: None,
            },
        ),
        (
            "cut on scattered values",
            Selection {
                projection: Projection::All,
                predicate: Predicate::Where("mag < 0.05"),
                spatial: None,
                limit: None,
            },
        ),
        (
            "the same, no statistics written",
            Selection {
                projection: Projection::All,
                predicate: Predicate::Where("mag_nostats < 0.05"),
                spatial: None,
                limit: None,
            },
        ),
        (
            "narrow projection and a cut",
            Selection {
                projection: Projection::Columns("objectid, mag"),
                predicate: Predicate::Where("mag < 0.05"),
                spatial: None,
                limit: None,
            },
        ),
        (
            "limit alone",
            Selection {
                projection: Projection::All,
                predicate: Predicate::All,
                spatial: None,
                limit: Some(100),
            },
        ),
        (
            "limit after a cut",
            Selection {
                projection: Projection::All,
                predicate: Predicate::Where("mag < 0.05"),
                spatial: None,
                limit: Some(100),
            },
        ),
    ]
}

/// **1. Row order.** Whether the rows come back in the file's order, per shape, and
/// whether the answer is even stable from one run to the next.
#[tokio::test(flavor = "multi_thread")]
async fn row_order() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    let parallelism = std::thread::available_parallelism().map_or(0, |it| it.get());
    println!("\n=== row order (available_parallelism = {parallelism}) ===");

    for (name, selection) in shapes(rows()) {
        // Three runs, because an order that varies between them is a different finding
        // from one that is wrong the same way every time — and only the first tells a
        // caller they cannot depend on what they observed.
        let mut orders = Vec::new();
        for _ in 0..3 {
            let observed = observe(&fixture.file, &selection, Knobs::SHIPPED).await;
            orders.push(observed);
        }
        let first = &orders[0];
        if first.ids.is_empty() {
            println!("{name:34} no objectid in the projection");
            continue;
        }
        let ordered = orders.iter().filter(|it| it.in_file_order()).count();
        let same = orders.iter().all(|it| it.ids == first.ids);
        print!(
            "{name:34} rows={:<8} in file order {ordered}/3, identical across runs: {same}",
            first.num_rows
        );
        if let Some((at, before, after)) = first.first_inversion() {
            print!(
                ", first break at row {at} ({before} then {after}), {} breaks",
                first.inversions()
            );
        }
        println!();
    }

    println!("\n--- the same, one partition ---");
    for (name, selection) in shapes(rows()) {
        let one = Knobs {
            target_partitions: Some(1),
            ..Knobs::SHIPPED
        };
        let observed = observe(&fixture.file, &selection, one).await;
        if observed.ids.is_empty() {
            continue;
        }
        println!(
            "{name:34} rows={:<8} in file order: {}",
            observed.num_rows,
            observed.in_file_order()
        );
    }

    println!("\n--- the same, no filter pushdown ---");
    for (name, selection) in shapes(rows()) {
        let no_pushdown = Knobs {
            pushdown_filters: false,
            reorder_filters: false,
            ..Knobs::SHIPPED
        };
        let observed = observe(&fixture.file, &selection, no_pushdown).await;
        if observed.ids.is_empty() {
            continue;
        }
        println!(
            "{name:34} rows={:<8} in file order: {}",
            observed.num_rows,
            observed.in_file_order()
        );
    }
}

/// Whether file order survives a parallel scan when the partitions are concatenated by
/// index instead of merged on completion.
///
/// This is the interesting question, because the alternative — one partition — serialises
/// the read. If DataFusion hands the file's byte ranges out to partitions in the file's
/// own order, then partition 0 holds the first rows, partition 1 the next, and
/// concatenating them in order costs nothing and keeps every thread busy. If it does not,
/// the whole approach is dead and the report should say so.
#[tokio::test(flavor = "multi_thread")]
async fn row_order_by_partition() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    println!(
        "\n=== collect_partitioned, concatenated by index (available_parallelism = {}) ===",
        std::thread::available_parallelism().map_or(0, |it| it.get())
    );

    for (name, selection) in shapes(rows()) {
        let mut runs = Vec::new();
        for _ in 0..3 {
            runs.push(
                observe_with(
                    &fixture.file,
                    &selection,
                    Knobs::SHIPPED,
                    Gather::ByPartition,
                )
                .await,
            );
        }
        let first = &runs[0];
        if first.ids.is_empty() {
            continue;
        }
        let ordered = runs.iter().filter(|it| it.in_file_order()).count();
        let stable = runs.iter().all(|it| it.ids == first.ids);
        print!(
            "{name:34} rows={:<8} partitions={:<3} in file order {ordered}/3, identical across runs: {stable}",
            first.num_rows, first.partitions
        );
        if let Some((at, before, after)) = first.first_inversion() {
            print!(
                ", first break at row {at} ({before} then {after}), {} breaks",
                first.inversions()
            );
        }
        println!();
    }

    println!("\n--- the same, work stealing off ---");
    let settled = Knobs {
        work_stealing: false,
        ..Knobs::SHIPPED
    };
    for (name, selection) in shapes(rows()) {
        let mut runs = Vec::new();
        for _ in 0..3 {
            runs.push(observe_with(&fixture.file, &selection, settled, Gather::ByPartition).await);
        }
        let first = &runs[0];
        if first.ids.is_empty() {
            continue;
        }
        let ordered = runs.iter().filter(|it| it.in_file_order()).count();
        let stable = runs.iter().all(|it| it.ids == first.ids);
        println!(
            "{name:34} rows={:<8} partitions={:<3} in file order {ordered}/3, identical across runs: {stable}",
            first.num_rows, first.partitions
        );
    }

    // Per partition, the span of ids it produced. Each partition's own contents are
    // contiguous and ascending either way; what work stealing decides is whether the
    // partition holding the file's first rows is partition 0.
    println!("\n--- what each partition produced, whole file ---");
    if let Some((_, selection)) = shapes(rows())
        .into_iter()
        .find(|(it, _)| *it == "whole file")
    {
        for (label, knobs) in [("stealing on ", Knobs::SHIPPED), ("stealing off", settled)] {
            for run in 0..3 {
                let spans = partition_spans(&fixture.file, &selection, knobs).await;
                let described: Vec<String> = spans
                    .iter()
                    .map(|(rows, first, last)| format!("{first}..{last} ({rows})"))
                    .collect();
                println!("{label} run {run}: {}", described.join("  "));
            }
        }
    }

    // The plan for two shapes, since what decides the answer is which nodes are in it:
    // a `RepartitionExec` that round-robins, or a `CoalescePartitionsExec` under a limit,
    // would each defeat the index order regardless of how the scan was split.
    for shape in ["whole file", "limit after a cut"] {
        if let Some((name, selection)) = shapes(rows()).into_iter().find(|(it, _)| *it == shape) {
            let observed = observe_with(
                &fixture.file,
                &selection,
                Knobs::SHIPPED,
                Gather::ByPartition,
            )
            .await;
            println!("\n--- plan for {name} ---\n{}", observed.plan);
        }
    }
}

/// **2. The settings.** What each knob costs and what it buys, per shape.
#[tokio::test(flavor = "multi_thread")]
async fn settings() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    println!("\n=== settings ({} MiB file) ===", fixture.bytes >> 20);

    let variants: Vec<(&str, Knobs)> = vec![
        ("everything off", Knobs::NOTHING),
        (
            "pruning only",
            Knobs {
                pruning: true,
                ..Knobs::NOTHING
            },
        ),
        (
            "+ page index",
            Knobs {
                pruning: true,
                enable_page_index: true,
                ..Knobs::NOTHING
            },
        ),
        (
            "+ bloom filter",
            Knobs {
                pruning: true,
                enable_page_index: true,
                bloom_filter_on_read: true,
                ..Knobs::NOTHING
            },
        ),
        ("shipped", Knobs::SHIPPED),
        (
            "shipped without reorder",
            Knobs {
                reorder_filters: false,
                ..Knobs::SHIPPED
            },
        ),
        (
            "shipped without pushdown",
            Knobs {
                pushdown_filters: false,
                reorder_filters: false,
                ..Knobs::SHIPPED
            },
        ),
    ];

    for (name, selection) in shapes(rows()) {
        println!("\n{name}");
        for (label, knobs) in &variants {
            // One warm run first: the first read of the file pays for the page cache, and
            // that cost belongs to neither variant.
            let _ = observe(&fixture.file, &selection, *knobs).await;
            let observed = observe(&fixture.file, &selection, *knobs).await;
            println!(
                "  {label:24} {:>7.1}ms rows={:<8} scanned={:>9} groups pruned: stats={} bloom={} rows pushed down={}",
                observed.elapsed.as_secs_f64() * 1e3,
                observed.num_rows,
                observed.metric("bytes_scanned"),
                observed.metric("row_groups_pruned_statistics"),
                observed.metric("row_groups_pruned_bloom_filter"),
                observed.metric("pushdown_rows_pruned"),
            );
        }
    }
}

/// The two settings `query::session_config` does not touch, over the shapes where they
/// could matter: `target_partitions` decides how many row groups are read at once, and
/// `batch_size` how much is decoded per step.
#[tokio::test(flavor = "multi_thread")]
async fn unset_settings() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    let parallelism = std::thread::available_parallelism().map_or(1, |it| it.get());
    println!("\n=== target_partitions and batch_size (default partitions = {parallelism}) ===");

    for (name, selection) in shapes(rows()) {
        println!("\n{name}");
        for partitions in [Some(1), Some(2), Some(4), None] {
            let knobs = Knobs {
                target_partitions: partitions,
                ..Knobs::SHIPPED
            };
            let _ = observe(&fixture.file, &selection, knobs).await;
            let observed = observe(&fixture.file, &selection, knobs).await;
            let label = match partitions {
                Some(n) => n.to_string(),
                None => format!("default ({parallelism})"),
            };
            println!(
                "  partitions {label:14} {:>7.1}ms rows={}",
                observed.elapsed.as_secs_f64() * 1e3,
                observed.num_rows
            );
        }
        for size in [1024, 8192, 65536] {
            let knobs = Knobs {
                batch_size: Some(size),
                ..Knobs::SHIPPED
            };
            let _ = observe(&fixture.file, &selection, knobs).await;
            let observed = observe(&fixture.file, &selection, knobs).await;
            println!(
                "  batch {size:<20} {:>7.1}ms rows={}",
                observed.elapsed.as_secs_f64() * 1e3,
                observed.num_rows
            );
        }
    }
}

/// An object store that counts what was asked of it, so a round trip can be counted
/// rather than reasoned about.
///
/// Delegation only — every method forwards. What is counted is requests, not bytes,
/// because a request against an origin costs its latency whatever its size, and that is
/// the thing worth not making twice.
#[derive(Debug)]
struct Counting {
    inner: Arc<dyn ObjectStore>,
    requests: AtomicUsize,
    bytes: AtomicUsize,
}

impl Counting {
    fn wrap(file: &RemoteFile) -> (RemoteFile, Arc<Self>) {
        let counting = Arc::new(Self {
            inner: Arc::clone(&file.store),
            requests: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        });
        let wrapped = RemoteFile {
            store: Arc::clone(&counting) as Arc<dyn ObjectStore>,
            base: file.base.clone(),
            url: file.url.clone(),
        };
        (wrapped, counting)
    }

    fn reset(&self) {
        self.requests.store(0, Ordering::SeqCst);
        self.bytes.store(0, Ordering::SeqCst);
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn bytes(&self) -> usize {
        self.bytes.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} requests, {} bytes", self.requests(), self.bytes())
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.get_opts(location, options).await?;
        if let Some(size) = result.range.end.checked_sub(result.range.start) {
            self.bytes.fetch_add(size as usize, Ordering::SeqCst);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// How many requests one answer costs, and how many of those this crate added.
///
/// `query::run` infers the schema and scans; DataFusion caches the footer it parsed for
/// the first of those and the scan reuses it. `parquet_out::read_layout` then reads the
/// footer again through a fetcher of this crate's own, which knows nothing about that
/// cache — so a `format=parquet` answer pays for the same footer twice. Over an origin
/// that is the whole latency of an extra round trip, for bytes already in memory.
#[tokio::test(flavor = "multi_thread")]
async fn round_trips_per_answer() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    let (file, counter) = Counting::wrap(&fixture.file);
    println!("\n=== requests per answer ===");

    for (name, selection) in shapes(rows()) {
        counter.reset();
        let result = query::run(&file, &selection, LIMITS, Order::Unspecified)
            .await
            .expect("the shipped path");
        let scan = counter.requests();
        let scan_bytes = counter.bytes();

        counter.reset();
        let layout = hats_api::parquet_out::read_layout(&file).await;
        let footer = counter.requests();
        assert!(layout.is_ok(), "{name}");

        println!(
            "{name:34} rows={:<8} json: {scan} requests / {scan_bytes} bytes, \
             parquet: {} requests (+{footer} for the layout)",
            result.num_rows(),
            scan + footer,
        );
    }
}

/// What a real file gives the read-side switches to work with.
///
/// `bloom_filter_on_read` and `enable_page_index` can only save a read if the writer
/// wrote those structures, and nothing on this side can tell without looking: a file with
/// neither pays the lookup and gets nothing back. So this reports what is actually in the
/// catalogs this service is for, rather than what the fixture above was told to write.
///
/// `HATS_ENGINE_CATALOG` names a directory to walk; without it the fixture is described
/// instead, which says only what this file asked for.
#[tokio::test(flavor = "multi_thread")]
async fn what_a_real_file_carries() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    println!("\n=== what the files carry ===");
    let Some(root) = std::env::var_os("HATS_ENGINE_CATALOG") else {
        println!("HATS_ENGINE_CATALOG unset; nothing real to look at");
        return;
    };
    let mut described = 0;
    for entry in walkdir(std::path::Path::new(&root)) {
        if described >= 8 {
            break;
        }
        let Ok(bytes) = std::fs::read(&entry) else {
            continue;
        };
        let Ok(metadata) =
            ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::from(bytes))
        else {
            continue;
        };
        described += 1;
        let groups = metadata.num_row_groups();
        let first = metadata.row_group(0);
        let statistics = (0..first.num_columns())
            .filter(|&i| first.column(i).statistics().is_some())
            .count();
        let bloom = (0..first.num_columns())
            .filter(|&i| first.column(i).bloom_filter_offset().is_some())
            .count();
        let page_index = (0..first.num_columns())
            .filter(|&i| first.column(i).column_index_offset().is_some())
            .count();
        println!(
            "{:60} groups={groups:<4} columns={:<4} with statistics={statistics} with bloom filter={bloom} with page index={page_index}",
            entry
                .file_name()
                .map_or_else(String::new, |it| it.to_string_lossy().into_owned()),
            first.num_columns(),
        );
        println!(
            "{:60} written by {}",
            "",
            metadata
                .file_metadata()
                .created_by()
                .unwrap_or("(not recorded)")
        );
    }
    if described == 0 {
        println!("no parquet files found under {root:?}");
    }
}

/// Every `.parquet` under a directory, depth first. Enough for a catalog tree; not a
/// general-purpose walker, and it follows nothing it is not handed.
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => stack.push(path),
                Ok(kind) if kind.is_file() => {
                    let name = path.file_name().unwrap_or_default().to_string_lossy();
                    if name.ends_with(".parquet") || name.ends_with(".parq") {
                        found.push(path);
                    }
                }
                _ => {}
            }
        }
    }
    found.sort();
    found
}

/// Whether the shipped path and the mirror above agree, so that a finding from `observe`
/// is a finding about this service and not about the mirror.
#[tokio::test(flavor = "multi_thread")]
async fn the_mirror_agrees_with_the_shipped_path() {
    if !enabled() {
        eprintln!("engine: skipped, set HATS_ENGINE_MEASURE to run");
        return;
    }
    let fixture = write_fixture();
    println!("\n=== mirror against query::run ===");
    for (name, selection) in shapes(rows()) {
        let shipped = query::run(&fixture.file, &selection, LIMITS, Order::Unspecified)
            .await
            .expect("the shipped path");
        let mirrored = observe(&fixture.file, &selection, Knobs::SHIPPED).await;
        let shipped_rows = shipped.num_rows();
        assert_eq!(shipped_rows, mirrored.num_rows, "{name}");
        println!("{name:34} rows={shipped_rows} agree");
    }
}
