//! Reading a catalog's partitions one after another, as a statement pulls rows.
//!
//! A statement over a catalog is planned against the partitions a region leaves, and a
//! region-free one leaves all of them — twelve thousand for ZTF DR24. Enumerating those before
//! reading is what cannot be afforded: a directory-partitioned catalog is listed one partition
//! at a time, and a parquet scan asks after every file it is given. So nothing here is opened
//! until the stream reaches it.
//!
//! **Stopping is DataFusion's, and it is free.** A `LIMIT`, or a filter that has already found
//! its rows, stops pulling; a partition this stream has not started is then never listed and
//! never read. `SELECT TOP 1000 …` over a whole catalog reads the front of it, and so does
//! `SELECT TOP 1000 … WHERE mag < 10`, whose limit DataFusion never hands to the scan.
//!
//! **What bounds the rest is a count of partitions opened.** A statement that needs every row —
//! a `COUNT(*)` with no region — keeps pulling, and the stream ends with a refusal when it would
//! open one more partition than the operator allows. Never with the rows so far: an aggregate
//! over part of a catalog is a wrong number, not a short answer.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DataFusionError, Result as DfResult};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr, create_physical_expr,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, ReplaceChildrenOptions, collect,
};
use futures::{StreamExt, TryStreamExt, stream};

use crate::access::data::DataFiles;
use crate::engine::query::data_bytes_read;
use crate::hats::{HatsCatalog, HatsPartition};

/// What one read of a catalog's partitions is given.
pub(super) struct PartitionScan {
    pub catalog: Arc<HatsCatalog>,
    pub data: DataFiles,
    /// The catalog's whole schema, which every partition is read against.
    pub table_schema: SchemaRef,
    /// The partitions the region left, in the order they are to be read.
    pub partitions: Vec<HatsPartition>,
    /// What the collection's index lookups read while planning, which were reads of this
    /// statement and are reported with the scan's own.
    pub index_bytes: u64,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    /// The rows a partition need yield, where DataFusion said the statement wants no more.
    pub limit: Option<usize>,
    /// How many partitions may be opened before the stream refuses to open another.
    pub max_partitions: usize,
    /// How many are read at once. The overshoot past the rows wanted is these.
    pub concurrency: usize,
    /// The session the plan was made in, which reading a partition's files is planned in too.
    pub state: SessionState,
    /// The spatial index column, where the catalog's files have one. It is what the rows can
    /// be put in order by without reading the catalog to find out.
    pub index: Option<String>,
}

/// The execution node: one output stream, which is the partitions in order.
pub(super) struct CatalogScanExec {
    scan: Arc<PartitionScan>,
    /// Whether the rows come out sorted by the index column, and which way. `None` is the
    /// partitions in catalog order with each one's rows as the files hold them.
    order: Option<SortOptions>,
    properties: Arc<PlanProperties>,
    /// What the partitions' own scans fetched. Those scans are planned while this one runs,
    /// so they are not in the plan tree where a caller's `data_bytes_read` is added up, and
    /// their count is carried here under DataFusion's own name for it.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for CatalogScanExec {
    /// The count and not the list, which for a real catalog is twelve thousand cells.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogScanExec")
            .field("url", &self.scan.catalog.dir().url.as_str())
            .field("partitions", &self.scan.partitions.len())
            .field("limit", &self.scan.limit)
            .field("order", &self.order)
            .finish()
    }
}

impl CatalogScanExec {
    pub(super) fn new(scan: PartitionScan) -> DfResult<Self> {
        Self::with_order(Arc::new(scan), None)
    }

    fn with_order(scan: Arc<PartitionScan>, order: Option<SortOptions>) -> DfResult<Self> {
        let schema = match &scan.projection {
            Some(indices) => Arc::new(scan.table_schema.project(indices)?),
            None => Arc::clone(&scan.table_schema),
        };
        // The ordering the rows really have, declared so that DataFusion's own analysis is
        // what decides a sort above is redundant — never a guess of this node's.
        let equivalences = match (order, index_of(&scan, &schema)) {
            (Some(options), Some(position)) => {
                let name = schema.field(position).name().clone();
                EquivalenceProperties::new_with_orderings(
                    Arc::clone(&schema),
                    [[PhysicalSortExpr::new(
                        Arc::new(PhysicalColumn::new(&name, position)),
                        options,
                    )]],
                )
            }
            _ => EquivalenceProperties::new(schema),
        };
        // One partition of output. The catalog's partitions are read several at a time inside
        // it, and the parallelism DataFusion would otherwise add by splitting the output is
        // exactly what would put the rows out of catalog order.
        let properties = PlanProperties::new(
            equivalences,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            scan,
            order,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The same scan with its rows sorted by the index column, or `None` where it cannot be.
    ///
    /// **Ordering by the index is known before anything is read, and that is the whole of why
    /// this is cheap.** A partition is one HEALPix cell, so its `_healpix_29` values lie in a
    /// range fixed by `Norder` and `Npix`, and the ranges of two partitions do not overlap.
    /// Walking the partitions forwards or backwards and sorting the rows *within* each one
    /// therefore sorts all of them, one partition at a time — so a `TOP n` stops after the
    /// partitions that hold `n` rows rather than after all of them. What is not known is the
    /// order inside a file, which is the importer's, so that part is always sorted.
    fn ordered(&self, options: SortOptions) -> DfResult<Option<Self>> {
        let schema = self.schema();
        if index_of(&self.scan, &schema).is_none() {
            return Ok(None);
        }
        Self::with_order(Arc::clone(&self.scan), Some(options)).map(Some)
    }
}

/// Where the index column is in the scan's own output, if it is there at all.
fn index_of(scan: &PartitionScan, schema: &SchemaRef) -> Option<usize> {
    let index = scan.index.as_ref()?;
    schema.index_of(index).ok()
}

impl DisplayAs for CatalogScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CatalogScanExec: partitions={}, limit={:?}, order={:?}",
            self.scan.partitions.len(),
            self.scan.limit,
            self.order
        )
    }
}

impl ExecutionPlan for CatalogScanExec {
    fn name(&self) -> &'static str {
        "CatalogScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CatalogScanExec has one output partition, and {partition} was asked for"
            )));
        }
        let scan = Arc::clone(&self.scan);
        let order = self.order;
        let allowed = scan.max_partitions;
        let concurrency = scan.concurrency.max(1);
        let beyond = scan.partitions.len() > allowed;
        // Partitions are kept in the order their index ranges start in, so descending is the
        // same list from the other end.
        let walk: Box<dyn Iterator<Item = &HatsPartition>> = match order {
            Some(options) if options.descending => Box::new(scan.partitions.iter().rev()),
            _ => Box::new(scan.partitions.iter()),
        };
        let partitions = walk.take(allowed).cloned().collect::<Vec<_>>();
        let bytes = MetricBuilder::new(&self.metrics).global_counter(BYTES_SCANNED);
        bytes.add(usize::try_from(scan.index_bytes).unwrap_or(usize::MAX));
        // Each partition is a future that does nothing until polled, so `buffered` starts only
        // as many as it is about to yield — and none past what the consumer pulls.
        let reads = stream::iter(partitions)
            .map(move |cell| {
                read_partition(
                    Arc::clone(&scan),
                    cell,
                    order,
                    Arc::clone(&context),
                    bytes.clone(),
                )
            })
            .buffered(concurrency);
        // Past the bound, one more item: the refusal. It is reached only by a consumer that
        // pulled every row of the partitions it was allowed, which is the statement that
        // needed more than a limit could have spared it.
        let refusal = stream::iter(beyond.then(|| {
            // Not the catalog's url: for a mounted catalog that is the operator's path on disk.
            Err(DataFusionError::Plan(format!(
                "this query reaches more than {allowed} partitions of the catalog; this server \
                 reads at most {allowed} in one request. Narrow it with a region, or with TOP"
            )))
        }));
        let batches = reads
            .chain(refusal)
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            batches,
        )))
    }
}

/// One partition's rows, read the way every other parquet file here is.
///
/// `ListingTable` is what reads it — the row-group pruning the covering drives, the projection,
/// the filter pushdown and the limit — handed this one partition's files. What is planned per
/// partition rather than once is the one request apiece a directory-partitioned catalog needs to
/// learn its files, and the look at each file a scan takes: both are costs of a partition that
/// is actually read.
///
/// With an `order`, the partition's rows come back sorted by the index column, and no limit is
/// taken per partition: the first rows a file holds are not the ones that sort first.
async fn read_partition(
    scan: Arc<PartitionScan>,
    cell: HatsPartition,
    order: Option<SortOptions>,
    context: Arc<TaskContext>,
    bytes: Count,
) -> DfResult<Vec<datafusion::arrow::array::RecordBatch>> {
    let files = scan
        .catalog
        .files(&cell, &scan.data)
        .await
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let paths = files
        .iter()
        .map(|file| ListingTableUrl::parse(file.url.as_str()))
        .collect::<DfResult<Vec<_>>>()?;
    let options = ListingOptions::new(Arc::new(
        datafusion::datasource::file_format::parquet::ParquetFormat::default(),
    ))
    // A partition's name ends in `.parquet` for most catalogs and in nothing recognisable for
    // some, `hats_npix_suffix` being the catalog's to choose. The files were chosen here rather
    // than found by a glob, so the extension has nothing left to decide.
    .with_file_extension("");
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        // The catalog's own schema rather than one inferred from this partition, so every
        // partition yields batches of the one schema the plan was made against.
        .with_schema(Arc::clone(&scan.table_schema));
    let table = ListingTable::try_new(config)?;
    let limit = match order {
        Some(_) => None,
        None => scan.limit,
    };
    let mut plan = table
        .scan(&scan.state, scan.projection.as_ref(), &scan.filters, limit)
        .await?;
    // **The filter goes on top and the optimizer runs, or the parquet reader prunes nothing.**
    // `ListingTable` hands the reader no predicate for an ordinary column; what does is the
    // physical optimizer's filter pushdown, moving a `FilterExec`'s predicate into the scan
    // below it. A scan planned outside the statement's plan misses that pass, and the covering
    // then skips no row group and no page — the same rows for half again the bytes.
    if let Some(filter) = scan.filters.iter().cloned().reduce(Expr::and) {
        let schema = DFSchema::try_from(plan.schema())?;
        let predicate = create_physical_expr(
            &filter,
            &schema,
            scan.state.execution_props(),
            &PhysicalPlanningContext::default(),
        )?;
        plan = Arc::new(FilterExec::try_new(predicate, plan)?);
    }
    if let Some(options) = order {
        let schema = plan.schema();
        let Some(position) = index_of(&scan, &schema) else {
            return Err(DataFusionError::Internal(
                "an ordered catalog scan was planned without its index column".to_owned(),
            ));
        };
        let key = PhysicalSortExpr::new(
            Arc::new(PhysicalColumn::new(schema.field(position).name(), position)),
            options,
        );
        let Some(ordering) = LexOrdering::new([key]) else {
            return Err(DataFusionError::Internal(
                "an ordering of one column was empty".to_owned(),
            ));
        };
        plan = Arc::new(SortExec::new(ordering, plan));
    }
    for rule in scan.state.physical_optimizers() {
        plan = rule.optimize(plan, scan.state.config_options())?;
    }
    let batches = collect(Arc::clone(&plan), context).await?;
    bytes.add(usize::try_from(data_bytes_read(plan.as_ref())).unwrap_or(usize::MAX));
    Ok(batches)
}

/// DataFusion's name for the bytes a scan fetched, which is what `data_bytes_read` sums.
const BYTES_SCANNED: &str = "bytes_scanned";

/// `ORDER BY` a catalog's index column, answered by the order the partitions are read in.
///
/// A sort consumes all of its input before it yields a row, so `ORDER BY _healpix_29 DESC`
/// with a `TOP` would read every partition a region leaves — and be refused at the partition
/// bound over a whole catalog. The ordered scan yields the rows already sorted, so the sort
/// can go and the limit stops the reading after the partitions that hold the rows.
///
/// **A sort with further keys after the index keeps its sort, over the ordered scan.** Its
/// first key is then already in order, and a `TOP`'s heap stops pulling once a batch's last
/// row is past the worst row it holds on that key — `ORDER BY _healpix_29 DESC, mag` reads the
/// partitions that hold its rows and no others. Without a `TOP` every row is wanted anyway, so
/// such a sort is left alone rather than given a per-partition sort to do as well.
///
/// **Whether the sort can go is DataFusion's to say, never this rule's.** The rule swaps in the
/// ordered scan beneath nodes that keep their input's order — a filter, a projection, batching —
/// rebuilds them so that each derives its own ordering from its child, and removes the sort
/// only where the rebuilt input's equivalence properties say they satisfy it. An aliased
/// column, an expression over the index, a sort on something else: all of that is the
/// equivalence analysis's, and all of it leaves the plan as it was.
///
/// DataFusion's own `try_pushdown_sort` is the hook for this, and it cannot be used: a
/// `FilterExec` does not pass it down, and a region is always a filter above the scan.
#[derive(Debug, Default)]
pub struct OrderByIndex;

impl PhysicalOptimizerRule for OrderByIndex {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let rewritten = plan.transform_down(|node| {
            let Some(sort) = node.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(node));
            };
            match (ordered_input(sort)?, sort.fetch()) {
                (Some(Ordered::Whole(input)), Some(fetch)) => Ok(Transformed::yes(Arc::new(
                    GlobalLimitExec::new(input, 0, Some(fetch)),
                ))),
                (Some(Ordered::Whole(input)), None) => Ok(Transformed::yes(input)),
                // Rebuilt rather than given a new child as-is, so that the sort learns its
                // input's order: that shared prefix is what its heap stops on.
                (Some(Ordered::Prefix(input)), Some(_)) => {
                    Ok(Transformed::yes(Arc::clone(&node).replace_children(
                        vec![input],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )?))
                }
                (Some(Ordered::Prefix(_)), None) | (None, _) => Ok(Transformed::no(node)),
            }
        })?;
        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // The fetch a removed sort carried now sits above nodes that never had it. A
        // `FilterExec` without one gathers a whole batch before it yields, pulling partition
        // after partition for rows the limit will drop — so the limit is pushed down again, by
        // DataFusion's own rule, which ran before this one did.
        LimitPushdown::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "OrderByIndex"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A sort's input rebuilt over an ordered catalog scan, and how much of the sort that answers.
enum Ordered {
    /// All of it, so the sort can go.
    Whole(Arc<dyn ExecutionPlan>),
    /// Its first key, the one the partitions are walked by.
    Prefix(Arc<dyn ExecutionPlan>),
}

/// The sort's input with an ordered catalog scan at the bottom, where that satisfies at least
/// the sort's first key.
fn ordered_input(sort: &SortExec) -> DfResult<Option<Ordered>> {
    // The first key decides the walk. Its options are the ones asked for, `nulls_first`
    // included: the index holds no nulls, so either placement is true of the rows.
    let key = sort.expr().first();
    // Down through the nodes that keep their input's order, to the scan.
    let mut chain = Vec::new();
    let mut current = Arc::clone(sort.input());
    let scan = loop {
        if let Some(scan) = current.downcast_ref::<CatalogScanExec>() {
            break scan;
        }
        let children = current.children();
        let ([child], [true]) = (
            children.as_slice(),
            current.maintains_input_order().as_slice(),
        ) else {
            return Ok(None);
        };
        let child = Arc::clone(child);
        chain.push(current);
        current = child;
    };
    let Some(ordered) = scan.ordered(key.options)? else {
        return Ok(None);
    };
    // Back up, each node rebuilt over its new child so that it works out its own ordering.
    let mut rebuilt: Arc<dyn ExecutionPlan> = Arc::new(ordered);
    for node in chain.into_iter().rev() {
        rebuilt = node.replace_children(
            vec![rebuilt],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
    }
    let properties = rebuilt.equivalence_properties();
    if properties.ordering_satisfy(sort.expr().iter().cloned())? {
        return Ok(Some(Ordered::Whole(rebuilt)));
    }
    if properties.ordering_satisfy([key.clone()])? {
        return Ok(Some(Ordered::Prefix(rebuilt)));
    }
    Ok(None)
}
