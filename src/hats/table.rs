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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, DataFusionError, Result as DfResult, ScalarValue};
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::utils::{Guarantee, LiteralGuarantee};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_optimizer::pruning::{PruningPredicateBuilder, PruningStatistics};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::access::data::DataFiles;
use crate::error::ApiError;
use crate::hats::scan::{CatalogScanExec, PartitionScan};
use crate::hats::{CatalogCache, HatsCatalog, HatsPartition};
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
    /// How many partitions a statement must still reach, after its region, for a collection's
    /// index to be asked. Below it the partitions are few enough to read without asking.
    pub min_partitions_for_index: usize,
    /// The most an index lookup may read. One that would read more is skipped, never refused:
    /// the index narrows a scan, and the scan is answered without it.
    pub max_index_bytes: u64,
}

/// What the collection's indexes made of a scan's partitions.
struct Indexed {
    partitions: Vec<HatsPartition>,
    /// A filter on the HEALPix column from the cells of the rows the indexes found.
    healpix: Option<Expr>,
    /// What reading the indexes fetched.
    bytes_read: u64,
}

/// One catalog, ready to be named in a statement.
pub struct HatsTable {
    /// Shared with every scan planned against it, a scan reading partitions after planning ends.
    catalog: Arc<HatsCatalog>,
    data: DataFiles,
    schema: SchemaRef,
    /// The index column the partitions are pruned on, where the catalog's files have one.
    /// `None` leaves every partition to be read, which is correct and slow.
    index: Option<String>,
    limits: Limits,
}

impl std::fmt::Debug for HatsTable {
    /// Not the partition list, which for a real catalog is twelve thousand cells and says
    /// nothing a reader of a log line wanted.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HatsTable")
            .field("url", &self.catalog.dir().url.as_str())
            .field("index", &self.index)
            .finish()
    }
}

impl HatsTable {
    /// Open a catalog and read what a statement needs to know about it before planning.
    ///
    /// The catalog's properties and its schema, which a statement is checked against before
    /// anything is read. The partition list waits for the scan, which is where pruning needs
    /// it; a statement that fails to plan never asks for it.
    pub async fn open(
        ctx: &SessionContext,
        dir: &RemoteDir,
        data: &DataFiles,
        limits: Limits,
        cache: &CatalogCache,
    ) -> Result<Self, ApiError> {
        let catalog = HatsCatalog::open(dir.clone(), cache, limits.max_metadata_bytes).await?;
        let data = data.clone();
        // The scan reads the partitions through this context, so the store is registered
        // here whether or not the schema had to be read to learn the columns.
        ctx.register_object_store(&dir.base, Arc::clone(&dir.store));
        let schema = catalog.schema(&data).await?;
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

    /// The catalog behind the table: its partition list, its own files, its properties.
    ///
    /// What reads it besides the scan is the `/examples` resource, which wants a position
    /// the catalog holds a row at and a partition to size a cone by.
    pub fn catalog(&self) -> &HatsCatalog {
        &self.catalog
    }

    /// The partitions a filter cannot rule out.
    ///
    /// Every partition where there is nothing to prune on — no index column, or no filter that
    /// mentions it. `PruningPredicate` answers "might match", so a partition it keeps may still
    /// hold no matching row; what it promises is that one it drops holds none.
    fn reached(&self, cells: &[HatsPartition], filters: &[Expr]) -> DfResult<Vec<HatsPartition>> {
        let Some(index) = &self.index else {
            return Ok(cells.to_vec());
        };
        // `Spans` answers `None` for every column but the index, which is how a predicate says
        // it has no statistics for one: the container is kept on that term and pruned on the
        // others.
        let Ok(field) = self.schema.field_with_name(index) else {
            return Ok(cells.to_vec());
        };
        let data_type = field.data_type().clone();
        let Some(physical) = self.physical(filters) else {
            return Ok(cells.to_vec());
        };
        let Some(pruning) = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(&self.schema))
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

    /// The partitions the collection's index catalogs leave, for a statement that says which
    /// values of an indexed column it wants.
    ///
    /// What a statement wants is read off its filters by `LiteralGuarantee` — `object_id = 7`,
    /// `object_id IN (7, 8)`, either beside anything else under an `AND` — so nothing here
    /// recognises a shape of its own, and a filter it cannot prove such a set for asks no index.
    /// Two indexed columns each constrained keep only the partitions both indexes name.
    ///
    /// Every partition where no index answers: none covers the column, the catalog was named
    /// outside its collection, the index could not be read, or reading it would cost more
    /// than a request may fetch. Asked only where at least `min_partitions_for_index` are
    /// left, fewer being cheaper to read than to look up.
    ///
    /// **Where an index carries the table's HEALPix column, the rows' cells come back too**, and
    /// become a filter on that column for the partitions' own scans. A partition is sorted by
    /// HEALPix and not by the indexed column, so without it a lookup reads every row group of
    /// the partition it found; with it, the row groups are pruned the way a cone's are. The
    /// filter is exact wherever the index is — the rows asked for are in those cells — and it is
    /// trusted the same way.
    async fn indexed(&self, cells: Vec<HatsPartition>, filters: &[Expr]) -> Indexed {
        let unasked = |partitions| Indexed {
            partitions,
            healpix: None,
            bytes_read: 0,
        };
        if cells.len() < self.limits.min_partitions_for_index {
            return unasked(cells);
        }
        let Some(physical) = self.physical(filters) else {
            return unasked(cells);
        };
        let mut chosen: Option<HashSet<(u8, u64)>> = None;
        let mut healpix: Option<HashSet<ScalarValue>> = None;
        let mut bytes_read = 0;
        for guarantee in LiteralGuarantee::analyze(&physical) {
            if guarantee.guarantee != Guarantee::In {
                continue;
            }
            let Some(found) = self
                .catalog
                .indexed_partitions(
                    &guarantee.column.name,
                    &guarantee.literals,
                    &self.data,
                    self.limits.max_index_bytes,
                    self.index.as_deref(),
                )
                .await
            else {
                continue;
            };
            bytes_read += found.bytes_read;
            chosen = Some(match chosen {
                None => found.cells,
                Some(before) => before.intersection(&found.cells).copied().collect(),
            });
            // A row satisfies every guarantee at once, so its cell is in every set an index
            // gave; an index that gave none constrains nothing here.
            if let Some(found) = found.healpix {
                healpix = Some(match healpix {
                    None => found,
                    Some(before) => before.intersection(&found).cloned().collect(),
                });
            }
        }
        let partitions = match chosen {
            None => cells,
            Some(chosen) => cells
                .into_iter()
                .filter(|cell| chosen.contains(&(cell.order, cell.pixel)))
                .collect(),
        };
        Indexed {
            partitions,
            healpix: healpix.and_then(|values| self.healpix_filter(&values)),
            bytes_read,
        }
    }

    /// `<healpix column> IN (values)`, the values cast to the table's own type for the column.
    fn healpix_filter(&self, values: &HashSet<ScalarValue>) -> Option<Expr> {
        let column = self.index.as_deref()?;
        let data_type = self
            .schema
            .field_with_name(column)
            .ok()?
            .data_type()
            .clone();
        let values = values
            .iter()
            .map(|value| value.cast_to(&data_type).map(datafusion::logical_expr::lit))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        (!values.is_empty()).then(|| {
            Expr::Column(Column::new_unqualified(column.to_owned())).in_list(values, false)
        })
    }

    /// The statement's filters as one physical predicate over the catalog's whole schema.
    ///
    /// The whole schema and not the one column being pruned on: a region test reaches here as
    /// the covering `OR`ed with the coordinate bounds and the haversine, so a schema holding
    /// only the index column is one the filter cannot even be planned against — and what that
    /// costs is not a wrong answer but every partition kept, silently.
    ///
    /// The default planning context, which is what a caller outside physical planning is told
    /// to pass: a scalar subquery then fails to convert rather than being resolved against a
    /// plan this has no part of, and `None` keeps every partition.
    fn physical(&self, filters: &[Expr]) -> Option<Arc<dyn PhysicalExpr>> {
        let predicate = conjunction(filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema)).ok()?;
        create_physical_expr(
            &predicate,
            &df_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .ok()
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
        // Pruning reads nothing past the partition list — a partition's span is its cell — so
        // choosing among twelve thousand is arithmetic. Nothing past that is done here: which
        // files a partition holds and what is in them is `CatalogScanExec`'s to learn, one
        // partition at a time and only for those a statement actually pulls.
        let listed = self
            .catalog
            .partitions()
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let reached = self.reached(listed.cells(), filters)?;
        let after_region = reached.len();
        let indexed = self.indexed(reached, filters).await;
        tracing::info!(
            url = %self.catalog.dir().url,
            partitions = listed.len(),
            after_region,
            after_index = indexed.partitions.len(),
            index_bytes = indexed.bytes_read,
            index_cells = indexed.healpix.is_some(),
            "catalog scan"
        );
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
            partitions: indexed.partitions,
            index_bytes: indexed.bytes_read,
            projection: projection.cloned(),
            filters: filters.to_vec(),
            narrowing: indexed.healpix,
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
        _values: &HashSet<ScalarValue>,
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
