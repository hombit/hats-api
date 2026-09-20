//! A HATS catalog as a table a statement can name.
//!
//! `hats::query` answers a catalog by fanning a selection out over its partitions; this
//! answers one by handing the planner a table and letting it decide. The two read the same
//! catalog through the same `hats/` code and choose the same partitions for the same region —
//! what differs is who is asking, and therefore what may be asked.
//!
//! **Partitions are pruned by statistics, not by recognising a shape.** A region test does not
//! survive to be read: `geometry::contains` rewrites itself during the optimizer's simplify
//! pass, which runs before a filter reaches a scan, so what arrives is the covering and the
//! haversine. That turns out to be the better place to stand. A HATS partition *is* one
//! HEALPix cell, so [`HatsPartition::span`] gives its `_healpix_29` range exactly and without
//! reading anything — statistics better than a parquet footer's and free — and
//! `PruningPredicate` is what DataFusion prunes every other container with. A caller who
//! writes `_healpix_29 BETWEEN …` by hand gets the same partitions as one who writes a circle,
//! which no pattern over `CONTAINS` would have given them.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, DataFusionError, Result as DfResult, ScalarValue};
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::{PruningPredicateBuilder, PruningStatistics};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::access::data::DataFiles;
use crate::error::ApiError;
use crate::hats::partitions::COMMON_METADATA;
use crate::hats::scan::{CatalogScanExec, PartitionScan};
use crate::hats::{Catalog, HatsPartition};
use crate::sky::geometry;
use crate::storage::RemoteDir;

/// What one catalog table may spend.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// How many partitions one scan may open.
    ///
    /// Counted as they are opened rather than against the list pruning left, which is what lets
    /// `SELECT TOP 1000 …` over a whole catalog through: it opens a partition or two and stops.
    /// A statement that keeps pulling is refused on opening one more than this — after that
    /// much work rather than before any, which is the price of not enumerating a catalog to
    /// find out. A refusal rather than a work list, this route having no plan to answer with.
    pub max_partitions: usize,
    /// How many partitions are read at once.
    pub max_concurrent_partitions: usize,
    pub max_metadata_bytes: u64,
}

/// One catalog, ready to be named in a statement.
pub struct HatsTable {
    /// Shared with every scan planned against it, a scan reading partitions after planning ends.
    catalog: Arc<Catalog>,
    data: DataFiles,
    schema: SchemaRef,
    /// The index column the partitions are pruned on, where the catalog's files have one.
    /// `None` leaves every partition to be read, which is correct and slow.
    index: Option<String>,
    limits: Limits,
}

impl std::fmt::Debug for HatsTable {
    /// The schema and not the partition list, which for a real catalog is twelve thousand
    /// cells and says nothing a reader of a log line wanted.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HatsTable")
            .field("url", &self.catalog.dir().url.as_str())
            .field("partitions", &self.catalog.partitions().cells().len())
            .field("index", &self.index)
            .finish()
    }
}

impl HatsTable {
    /// Open a catalog and read what a statement needs to know about it before planning.
    ///
    /// Two reads: the catalog's own metadata, which `hats/` decides the cost of, and its schema.
    /// The second is what the `simple` routes do without — they learn the columns from the
    /// first partition they were going to open anyway — and a planner cannot, a statement being
    /// checked against a schema before anything is read.
    pub async fn open(
        ctx: &SessionContext,
        dir: &RemoteDir,
        data: &DataFiles,
        limits: Limits,
    ) -> Result<Self, ApiError> {
        let catalog = Catalog::open(dir.clone(), limits.max_metadata_bytes).await?;
        let data = data.clone();
        let schema = schema_of(ctx, &catalog, &data).await?;
        let columns = catalog.columns().ok();
        // The catalog's own name for it, else the one name a file can be recognised by. Both
        // are checked against the schema, so a catalog naming a column its files have not got
        // prunes nothing rather than failing a scan.
        let index = columns
            .as_ref()
            .map(|columns| columns.healpix.0.to_owned())
            .or_else(|| Some(crate::sky::healpix::DEFAULT_HEALPIX_COLUMN_NAME.to_owned()))
            .filter(|name| schema.field_with_name(name).is_ok());
        let schema = match &columns {
            Some(columns) => marked(&schema, columns.ra, columns.dec),
            None => schema,
        };
        Ok(Self {
            catalog: Arc::new(catalog),
            data,
            schema,
            index,
            limits,
        })
    }

    /// Which of the catalog's columns hold a position, as the catalog declares them.
    ///
    /// The catalog's claim rather than a guess from the names, and unchecked against the
    /// schema: what reads it is the metadata this service publishes, and a mark naming a
    /// column the files have not got simply matches none of them.
    pub fn coordinates(&self) -> Option<(&str, &str)> {
        self.catalog
            .columns()
            .ok()
            .map(|columns| (columns.ra, columns.dec))
    }

    /// The spatial index column a query prunes on, where the files have one.
    pub fn index(&self) -> Option<&str> {
        self.index.as_deref()
    }

    /// Which cells the catalog is cut into, read while it was opened.
    ///
    /// What reads it besides the scan is the `/examples` resource: a partition exists
    /// because rows are there, so one of these cells is a position the catalog holds rows
    /// at — and it is known without opening a file.
    pub fn partitions(&self) -> &crate::hats::HatsPartitionList {
        self.catalog.partitions()
    }

    /// The partitions a filter cannot rule out.
    ///
    /// Every partition where there is nothing to prune on — no index column, or no filter that
    /// mentions it. `PruningPredicate` answers "might match", so a partition it keeps may still
    /// hold no matching row; what it promises is that one it drops holds none.
    fn reached(&self, filters: &[Expr]) -> DfResult<Vec<HatsPartition>> {
        let cells = self.catalog.partitions().cells();
        let Some(index) = &self.index else {
            return Ok(cells.to_vec());
        };
        let Some(predicate) = conjunction(filters) else {
            return Ok(cells.to_vec());
        };
        // Against the catalog's whole schema and not the one column being pruned on. A region
        // test reaches here as the covering `OR`ed with the coordinate bounds and the
        // haversine, so a schema holding only the index column is one the filter cannot even
        // be planned against — and what that costs is not a wrong answer but every partition
        // kept, silently. `Spans` answers `None` for every column but the index, which is how
        // a predicate says it has no statistics for one: the container is kept on that term
        // and pruned on the others.
        let Ok(field) = self.schema.field_with_name(index) else {
            return Ok(cells.to_vec());
        };
        let data_type = field.data_type().clone();
        let schema = Arc::clone(&self.schema);
        let Ok(df_schema) = DFSchema::try_from(Arc::clone(&schema)) else {
            return Ok(cells.to_vec());
        };
        // The default planning context, which is what a caller outside physical planning is
        // told to pass: a scalar subquery then fails to convert rather than being resolved
        // against a plan this has no part of, and a failure here keeps every partition.
        let Ok(physical) = create_physical_expr(
            &predicate,
            &df_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        ) else {
            return Ok(cells.to_vec());
        };
        let Some(pruning) = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(&schema))
            .build(physical)
        else {
            // Trivially true, or a predicate the builder could not use. Both mean every
            // partition stays.
            return Ok(cells.to_vec());
        };
        let statistics = Spans {
            column: index.clone(),
            data_type,
            cells,
        };
        let keep = pruning.prune(&statistics).map_err(|error| {
            DataFusionError::Plan(format!(
                "this catalog's partitions could not be pruned: {error}"
            ))
        })?;
        Ok(cells
            .iter()
            .zip(keep)
            .filter(|(_, keep)| *keep)
            .map(|(cell, _)| cell.clone())
            .collect())
    }
}

#[async_trait::async_trait]
impl TableProvider for HatsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// **Inexact for every filter, and that is two statements.** Inexact because pruning a
    /// partition is not testing a row: the filter has to run over what is read, and a
    /// partition kept may hold nothing that matches. Every filter because which of them prunes
    /// is the pruning predicate's to work out — a filter this returned `Unsupported` for would
    /// be one the planner stopped handing down, and the useful ones are not recognisable by
    /// their shape.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Pruning reads nothing — a partition's span is its cell — so choosing among twelve
        // thousand is arithmetic. Nothing past that is done here: which files a partition holds
        // and what is in them is `CatalogScanExec`'s to learn, one partition at a time and only
        // for those a statement actually pulls.
        let partitions = self.reached(filters)?;
        // The planning session, which each partition's own scan is planned in too. It is the
        // only `Session` DataFusion builds, so anything else reaching here is a context this
        // table was never registered in.
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal("a catalog is scanned in a SessionState".to_owned())
            })?
            .clone();
        let exec = CatalogScanExec::new(PartitionScan {
            catalog: Arc::clone(&self.catalog),
            data: self.data.clone(),
            table_schema: Arc::clone(&self.schema),
            partitions,
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
            max_partitions: self.limits.max_partitions,
            concurrency: self.limits.max_concurrent_partitions,
            state,
            index: self.index.clone(),
        })?;
        Ok(Arc::new(exec))
    }
}

/// Every partition's `_healpix_29` range, as statistics to prune against.
///
/// Exact rather than estimated, and known without reading anything: a partition is one HEALPix
/// cell and a cell at order *k* is a contiguous range of order-29 values. Nothing is null and
/// nothing is unknown, which is why the counts below are what they are.
struct Spans<'a> {
    column: String,
    /// The width the catalog's files wrote the column at.
    data_type: DataType,
    cells: &'a [HatsPartition],
}

impl Spans<'_> {
    /// One end of every partition's range, as the column `PruningPredicate` asked for.
    ///
    /// **Cast to the column's own type, which is not always the one a cell is counted in.**
    /// HATS recommends `_healpix_29` and recommends nothing about its type, so a catalog may
    /// write it at any integer width that holds its order — an order-13 catalog fits `Int32`.
    /// Statistics of another type than the column are statistics the predicate cannot compare
    /// against its literals, and what that costs is not a wrong answer but a silent loss of
    /// all pruning, which is the failure this whole module exists to avoid.
    fn ends(&self, column: &Column, end: impl Fn(&HatsPartition) -> u64) -> Option<ArrayRef> {
        if column.name != self.column {
            return None;
        }
        let values = self.cells.iter().map(end).collect::<UInt64Array>();
        cast(&values, &self.data_type).ok()
    }
}

impl PruningStatistics for Spans<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.ends(column, |cell| cell.span().start)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        // Inclusive, where the span's end is not: a range that ends where the next begins would
        // claim a cell holds the first value of its neighbour.
        self.ends(column, |cell| cell.span().end.saturating_sub(1))
    }

    fn num_containers(&self) -> usize {
        self.cells.len()
    }

    /// None of them, and known rather than assumed: a partition's cell is what its rows are
    /// indexed by, so a null there would be a row belonging to no cell.
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        Some(
            Arc::new(std::iter::repeat_n(0u64, self.cells.len()).collect::<UInt64Array>())
                as ArrayRef,
        )
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    /// Which values a container is known to hold, which a span does not say: a cell's range is
    /// where its rows *can* be, not what is there.
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }
}

/// Every filter as one expression, or `None` where there are none.
fn conjunction(filters: &[Expr]) -> Option<Expr> {
    filters.iter().cloned().reduce(Expr::and)
}

/// The two columns `hats_col_ra` and `hats_col_dec` name, marked in the schema the planner sees.
///
/// What reads the mark is [`crate::sky::geometry`], which refuses a region over any other pair: the
/// partitions here are chosen by an index over these two columns and describe no others. The
/// catalog is the only thing that can say which they are, and the schema is the only thing that
/// reaches the expression where the question is asked.
fn marked(schema: &SchemaRef, ra: &str, dec: &str) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let role = match field.name().as_str() {
                name if name == ra => geometry::RA,
                name if name == dec => geometry::DEC,
                _ => return Arc::clone(field),
            };
            let mut metadata = field.metadata().clone();
            metadata.insert(geometry::COORDINATE.to_owned(), role.to_owned());
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Fields>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The catalog's schema, which a statement is checked against before anything is read.
///
/// `dataset/_common_metadata` first: it is the schema and no rows, so it is one small `GET`
/// and it describes every partition rather than the one that answered. A catalog that has not
/// got it falls back to the first partition, which is what the `simple` routes do — they
/// learn the columns from the file they were going to open anyway. The fallback is a footer
/// read of a real partition, so it is the more expensive of the two and second for that
/// reason.
async fn schema_of(
    ctx: &SessionContext,
    catalog: &Catalog,
    data: &DataFiles,
) -> Result<SchemaRef, ApiError> {
    let dir = catalog.dir();
    ctx.register_object_store(&dir.base, Arc::clone(&dir.store));
    if let Ok(file) = dir.child(COMMON_METADATA)
        && let Ok(schema) = read_schema(ctx, file.url.as_str()).await
    {
        return Ok(schema);
    }
    let Some(first) = catalog.partitions().cells().first() else {
        return Err(ApiError::bad_request(
            "this catalog has no partitions, so nothing says what columns it has",
        ));
    };
    let files = catalog.partition(first)?.files(data).await?;
    let Some(file) = files.first() else {
        return Err(ApiError::bad_request(
            "this catalog's first partition holds no data file, so nothing says what columns \
             it has",
        ));
    };
    read_schema(ctx, file.url.as_str()).await
}

/// One parquet file's schema, with no rows read.
async fn read_schema(ctx: &SessionContext, url: &str) -> Result<SchemaRef, ApiError> {
    // The extension filter is off for the reason it is off everywhere here: a HATS partition
    // may be named anything its `hats_npix_suffix` says, and `_common_metadata` has no
    // extension at all.
    let options = datafusion::prelude::ParquetReadOptions {
        file_extension: "",
        ..Default::default()
    };
    let frame = ctx.read_parquet(url, options).await.map_err(|error| {
        ApiError::bad_request(format!("this catalog's columns could not be read: {error}"))
    })?;
    Ok(SchemaRef::from(frame.schema().as_arrow().clone()))
}
