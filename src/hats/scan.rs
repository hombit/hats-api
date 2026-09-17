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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DataFusionError, Result as DfResult};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, collect,
};
use futures::{StreamExt, TryStreamExt, stream};

use crate::access::data::DataFiles;
use crate::engine::query::data_bytes_read;
use crate::hats::{Catalog, HatsPartition};

/// What one read of a catalog's partitions is given.
pub(super) struct PartitionScan {
    pub catalog: Arc<Catalog>,
    pub data: DataFiles,
    /// The catalog's whole schema, which every partition is read against.
    pub table_schema: SchemaRef,
    /// The partitions the region left, in the order they are to be read.
    pub partitions: Vec<HatsPartition>,
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
}

/// The execution node: one output stream, which is the partitions in order.
pub(super) struct CatalogScanExec {
    scan: Arc<PartitionScan>,
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
            .finish()
    }
}

impl CatalogScanExec {
    pub(super) fn new(scan: PartitionScan) -> DfResult<Self> {
        let schema = match &scan.projection {
            Some(indices) => Arc::new(scan.table_schema.project(indices)?),
            None => Arc::clone(&scan.table_schema),
        };
        // One partition of output. The catalog's partitions are read several at a time inside
        // it, and the parallelism DataFusion would otherwise add by splitting the output is
        // exactly what would put the rows out of catalog order.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            scan: Arc::new(scan),
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CatalogScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CatalogScanExec: partitions={}, limit={:?}",
            self.scan.partitions.len(),
            self.scan.limit
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
        let allowed = scan.max_partitions;
        let concurrency = scan.concurrency.max(1);
        let beyond = scan.partitions.len() > allowed;
        let partitions = scan
            .partitions
            .iter()
            .take(allowed)
            .cloned()
            .collect::<Vec<_>>();
        let bytes = MetricBuilder::new(&self.metrics).global_counter(BYTES_SCANNED);
        // Each partition is a future that does nothing until polled, so `buffered` starts only
        // as many as it is about to yield — and none past what the consumer pulls.
        let reads = stream::iter(partitions)
            .map(move |cell| {
                read_partition(Arc::clone(&scan), cell, Arc::clone(&context), bytes.clone())
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
async fn read_partition(
    scan: Arc<PartitionScan>,
    cell: HatsPartition,
    context: Arc<TaskContext>,
    bytes: Count,
) -> DfResult<Vec<datafusion::arrow::array::RecordBatch>> {
    let files = scan
        .catalog
        .partition(&cell)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?
        .files(&scan.data)
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
    let mut plan = table
        .scan(
            &scan.state,
            scan.projection.as_ref(),
            &scan.filters,
            scan.limit,
        )
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
    for rule in scan.state.physical_optimizers() {
        plan = rule.optimize(plan, scan.state.config_options())?;
    }
    let batches = collect(Arc::clone(&plan), context).await?;
    bytes.add(usize::try_from(data_bytes_read(plan.as_ref())).unwrap_or(usize::MAX));
    Ok(batches)
}

/// DataFusion's name for the bytes a scan fetched, which is what `data_bytes_read` sums.
const BYTES_SCANNED: &str = "bytes_scanned";
