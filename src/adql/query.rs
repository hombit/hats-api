//! A statement against the tables a request declared.
//!
//! `engine::query` runs a selection against one file and `hats::query` fans one out over a
//! catalog's partitions; this is the third shape a request can have — a whole statement, over
//! tables the caller named, planned and executed by DataFusion. What it shares with the other
//! two is the storage layer, the region covering and the answer writers. What it does not
//! share is the fan-out: a planned statement has its own `ORDER BY`, its own aggregates and
//! its own joins, and nothing here promises an order the statement did not ask for.
//!
//! **Three things bound it, and each answers something the others cannot.** A memory pool
//! refuses a query whose working set is too large, with spilling to disk off so the bound
//! cannot quietly become a disk one. A row cap refuses an answer too large to send, which the
//! pool does not see — a `GROUP BY` streams its output. And the clock over the whole router
//! refuses one that is merely slow. The partition count is not one of them: a statement has no
//! partition list of its own, so that bound belongs to each catalog table it names.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{MemorySchemaProvider, TableProvider};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::parser::Statement as PlannerStatement;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Ident, Statement, visit_expressions_mut};
use futures::StreamExt;
use url::Url;

use crate::access::data::DataFiles;
use crate::adql::{Translated, functions};
use crate::engine::query::{QueryResult, data_bytes_read, session_config};
use crate::engine::sql;
use crate::error::ApiError;
use crate::hats;
use crate::sky::geometry;
use crate::storage::{RemoteDir, RemoteFile};

/// What one statement may spend.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// How much memory the query's own working set may take — the hash tables of a join, the
    /// groups of an aggregate, the heap of a `TopK`.
    pub max_memory_bytes: u64,
    /// How many rows the answer may hold, which is the operator's `max_rows` — or less,
    /// where the request asked for less.
    pub max_rows: usize,
    /// What reaching that bound means.
    pub rows: Rows,
    /// How much SQL the statement may contain.
    pub sql: sql::Limits,
    /// What a catalog table may spend, and how much of its metadata may be read.
    pub catalog: hats::table::Limits,
}

/// What happens when the answer reaches [`Limits::max_rows`].
///
/// The difference is whether the answer can say it was cut. A JSON or parquet body has
/// nowhere to put that, so an answer stopping at the bound would be one the caller cannot
/// tell from the whole of it — which is why every route but TAP refuses instead. A VOTable
/// has the `OVERFLOW` marker, and DALI §4.4.1 puts it after the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rows {
    /// Refuse the request, the way a bound this service will not exceed is refused
    /// everywhere else.
    Refuse,
    /// Answer what fits and report that it was cut.
    Truncate,
}

impl From<&crate::config::LimitsConfig> for Limits {
    fn from(config: &crate::config::LimitsConfig) -> Self {
        Self {
            max_memory_bytes: config.max_query_memory_bytes.as_u64(),
            max_rows: config.max_rows,
            rows: Rows::Refuse,
            sql: config.into(),
            catalog: hats::table::Limits {
                max_partitions: config.max_partitions,
                max_concurrent_partitions: config.max_concurrent_partitions,
                max_metadata_bytes: config.max_catalog_metadata_bytes.as_u64(),
            },
        }
    }
}

/// What running a statement produced.
#[derive(Debug)]
pub struct Answer {
    pub result: QueryResult,
    /// Whether there were more rows than the bound allowed, which only a [`Rows::Truncate`]
    /// request can be told — under [`Rows::Refuse`] it is an error instead.
    pub overflow: bool,
}

/// One table of a request: the name the caller gave it, and what it is.
#[derive(Debug)]
pub struct Table {
    /// As the request declared it, which is what a statement's `FROM` is matched against.
    pub name: String,
    pub source: Source,
}

/// What a table's url turned out to name.
///
/// Opened by the route before either reaches here, since both go through the access policy
/// and a catalog's own metadata is read to open one at all.
#[derive(Debug)]
pub enum Source {
    /// One parquet file.
    File(RemoteFile),
    /// A whole catalog, which chooses the partitions a scan reads.
    Catalog(RemoteDir),
    /// A table this service holds rather than reads — `TAP_SCHEMA`'s own five, which
    /// describe what it publishes and have no file anywhere.
    Memory(Arc<dyn TableProvider>),
}

/// Run a translated statement over these tables.
///
/// `data` is which names inside a catalog are read as rows, which a catalog whose partitions
/// are directories needs in order to list one. It is not a bound and so not one of `limits`:
/// it is the operator's answer to which files a query is about at all.
pub async fn run(
    translated: &Translated,
    tables: &[Table],
    data: &DataFiles,
    limits: Limits,
) -> Result<Answer, ApiError> {
    let ctx = context(limits)?;
    // The region functions go on this context and not on the one every other route shares:
    // `contains` prunes row groups inside a file, and a catalog route chooses its partitions
    // before a file is opened, so there it would prune within every partition and open them
    // all. Here the planner is what decides what to scan.
    geometry::register(&ctx);
    // ADQL's own, which is `rand` — mandatory, and the one function let through the
    // volatility rule, on this route and nowhere else.
    functions::register(&ctx);

    // Registered under the name the *statement* used rather than the one the request
    // declared, so that a table answers to its own spelling and to its lowercase the way a
    // column does. The two names are the same string in every ordinary request; where they
    // differ, the statement's is what every reference in it is written against.
    let mut schemas = Vec::new();
    for spelling in &translated.tables {
        let table = declared(spelling, tables)?;
        let reference = reference(spelling);
        if let Some(schema) = reference.schema() {
            ensure_schema(&ctx, schema)?;
        }
        let schema = match &table.source {
            Source::File(file) => {
                ctx.register_object_store(&file.base, Arc::clone(&file.store));
                // The url names one object and it has already been decided to be parquet, so
                // the extension filter that would hide a HATS `_metadata` is turned off, as it
                // is for every other route here.
                let options = ParquetReadOptions {
                    file_extension: "",
                    ..ParquetReadOptions::default()
                };
                ctx.register_parquet(reference.clone(), file.url.as_str(), options)
                    .await
                    .map_err(|error| opening(&file.url, &error))?;
                ctx.table_provider(reference)
                    .await
                    .map_err(|error| opening(&file.url, &error))?
                    .schema()
            }
            Source::Catalog(dir) => {
                let url = dir.url.clone();
                let table = hats::table::HatsTable::open(&ctx, dir, data, limits.catalog).await?;
                let schema = TableProvider::schema(&table);
                ctx.register_table(reference, Arc::new(table))
                    .map_err(|error| opening(&url, &error))?;
                schema
            }
            Source::Memory(provider) => {
                let schema = TableProvider::schema(provider.as_ref());
                ctx.register_table(reference, Arc::clone(provider))
                    .map_err(|error| {
                        ApiError::internal(format!("{spelling} could not be registered: {error}"))
                    })?;
                schema
            }
        };
        schemas.push(schema);
    }

    let mut statement = translated.statement.clone();
    resolve_identifiers(&mut statement, &schemas);
    let plan = ctx
        .state()
        .statement_to_plan(PlannerStatement::Statement(Box::new(statement)))
        .await
        .map_err(|error| refusal(&error))?;
    // After planning, so what is judged is every expression the statement really produced —
    // including the ones a `SELECT *` expanded into and the ones a subquery carries.
    sql::check_plan(&plan, limits.sql)?;

    let df = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|error| refusal(&error))?;
    let schema = SchemaRef::from(df.schema().as_arrow().clone());
    // No rows wanted at all, so the statement is planned and never run. DALI §3.4.4 asks
    // for "metadata, no results, and an overflow indicator", and says the service is not
    // required to execute the query — which is what makes this the cheap way a client
    // inspects a table rather than a scan whose rows are thrown away.
    if limits.rows == Rows::Truncate && limits.max_rows == 0 {
        return Ok(Answer {
            result: QueryResult {
                schema,
                batches: Vec::new(),
                data_bytes_read: 0,
            },
            overflow: true,
        });
    }
    let physical = df
        .create_physical_plan()
        .await
        .map_err(|error| refusal(&error))?;
    let mut stream =
        execute_stream(Arc::clone(&physical), ctx.task_ctx()).map_err(|error| refusal(&error))?;

    // Read as it arrives and stop at the cap, rather than collecting and measuring. The
    // memory pool bounds the query's working set and not its output, so an answer larger
    // than this service will send is refused while it is still being made.
    let mut batches = Vec::new();
    let mut rows = 0usize;
    let mut overflow = false;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| refusal(&error))?;
        rows += batch.num_rows();
        if rows > limits.max_rows {
            if limits.rows == Rows::Refuse {
                return Err(ApiError::too_much_work(format!(
                    "this query returns more than {} rows; this server returns at most that in \
                     one request",
                    limits.max_rows
                )));
            }
            // The batch that crossed the bound is cut where the bound is, and the stream is
            // dropped unpolled — so the rows past it are never read.
            let kept = limits.max_rows + batch.num_rows() - rows;
            if kept > 0 {
                batches.push(batch.slice(0, kept));
            }
            overflow = true;
            break;
        }
        batches.push(batch);
    }
    Ok(Answer {
        result: QueryResult {
            schema,
            batches,
            data_bytes_read: data_bytes_read(physical.as_ref()),
        },
        overflow,
    })
}

/// The reference the planner will resolve a name to, built from the parts rather than
/// parsed.
///
/// [`TableReference::parse_str`] lowercases an unquoted identifier, and identifier
/// normalization is off here precisely so a mixed-case name is left as written — so
/// parsing would register `Gaia.Source` under a name the statement never mentions.
fn reference(spelling: &str) -> TableReference {
    match spelling.split_once('.') {
        Some((schema, table)) => TableReference::partial(schema.to_owned(), table.to_owned()),
        None => TableReference::bare(spelling.to_owned()),
    }
}

/// Make a schema exist, so that a table qualified by one can be registered in it.
///
/// A context starts with one schema, and a name like `gaia_dr3.gaia_source` is resolved
/// against `gaia_dr3` — which nothing has created. It holds no tables of its own; what it
/// is for is that the two halves of a published name mean to the planner what they mean to
/// the client that wrote them.
fn ensure_schema(ctx: &SessionContext, name: &str) -> Result<(), ApiError> {
    let missing = || ApiError::internal(format!("the query context has no home for {name}"));
    let default = ctx
        .state()
        .config()
        .options()
        .catalog
        .default_catalog
        .clone();
    let catalog = ctx.catalog(&default).ok_or_else(missing)?;
    if catalog.schema(name).is_some() {
        return Ok(());
    }
    catalog
        .register_schema(name, Arc::new(MemorySchemaProvider::new()))
        .map_err(|error| ApiError::internal(format!("{name} could not be registered: {error}")))?;
    Ok(())
}

/// The request's table for a name the statement used.
///
/// The same rule a column answers by, which here is ADQL's: a name written without quotes is
/// case-insensitive. An exact match wins, so two declarations differing only in case are each
/// reachable by writing them out; the form they share names neither.
fn declared<'a>(spelling: &str, tables: &'a [Table]) -> Result<&'a Table, ApiError> {
    if let Some(table) = tables.iter().find(|table| table.name == spelling) {
        return Ok(table);
    }
    let mut folded = tables
        .iter()
        .filter(|table| table.name.eq_ignore_ascii_case(spelling));
    match (folded.next(), folded.next()) {
        (Some(table), None) => Ok(table),
        _ => Err(ApiError::bad_request(format!(
            "query: this request declares no table named {spelling:?}; it declares {}",
            match tables.is_empty() {
                true => "none".to_owned(),
                false => tables
                    .iter()
                    .map(|table| table.name.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            }
        ))),
    }
}

/// A context with the memory this statement may take and nowhere to spill it.
fn context(limits: Limits) -> Result<SessionContext, ApiError> {
    let runtime = RuntimeEnvBuilder::new()
        // A `GreedyMemoryPool`, which refuses the reservation that would cross the bound and
        // so fails the query rather than the process. The fraction is what of the bound the
        // pool may hand out, and 1.0 is the bound itself.
        .with_memory_limit(
            usize::try_from(limits.max_memory_bytes).unwrap_or(usize::MAX),
            1.0,
        )
        // Without this a memory bound is a disk bound: an aggregate over more groups than the
        // pool allows would spill to the operator's scratch space instead of being refused,
        // and nothing here says how much of that there is to spend.
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()
        .map_err(|error| {
            ApiError::internal(format!("the query runtime could not be built: {error}"))
        })?;
    // `reproducible` is the scan's ordering knob, and a statement makes no promise about the
    // order of its rows unless it carries an `ORDER BY`, which the planner answers. So the
    // faster read: a partition that finishes early helps a slow sibling.
    let mut config = session_config(false);
    // **No round-robin repartition above a scan.** A catalog is read lazily, one partition
    // after another as rows are pulled, and a `RepartitionExec` between the scan and a filter
    // drains its input in a task of its own — so `TOP 10 … WHERE mag < 10` read on through the
    // catalog and was refused at the partition bound, where it wanted one partition. The
    // reading stays parallel inside the scan; what runs on one thread is the filter over rows
    // already read.
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    Ok(SessionContext::new_with_config_rt(config, runtime))
}

/// Rewrite the names a caller wrote into the names their files actually use.
///
/// **This is ADQL's own rule and not the one the other routes follow.** An identifier written
/// without quotes is case-insensitive, and a delimited one is exact — ADQL 2.1 §2.1.3, and
/// what every service answering that language does. So `GMAG`, `gmag` and `Gmag` all reach a
/// column the file spells `Gmag`, and `"Gmag"` reaches that one and no other.
///
/// The `simple` routes and the file server keep `sql::resolve_identifiers`, which is stricter:
/// a name there answers to its own spelling and to its lowercase and to nothing else. Those
/// routes have no `TAP_SCHEMA` for a caller to have read the spelling out of, so what they
/// trade is a guess against a refusal. Here the language settles it.
///
/// It is needed at all because `enable_ident_normalization` is off — with it on DataFusion
/// would lowercase `Gmag` and put every mixed-case astronomy column out of reach without
/// quotes, which is the same interoperability problem from the other side.
///
/// **Across every table at once, and only where the answer is unambiguous.** A bare column in
/// a join could belong to either side, so a folded form that two tables spell differently is
/// left exactly as written and the planner says it found no such column — which is the honest
/// answer, one spelling naming two columns being something nobody could have meant.
fn resolve_identifiers(statement: &mut Statement, schemas: &[SchemaRef]) {
    let _ = visit_expressions_mut::<_, (), _>(statement, |node| {
        match node {
            SqlExpr::Identifier(ident) => resolve(ident, schemas),
            // The last segment and never one before it. Everything ahead of the column names
            // the table it is in — `gaia.ra`, or an alias's `g.ra` — which is the planner's to
            // resolve and not a spelling this knows anything about. A statement has no other
            // reading of a dotted name: what the `simple` routes read as a step into a
            // struct column is ADQL's table beside its column.
            SqlExpr::CompoundIdentifier(parts) => {
                if let Some(column) = parts.last_mut() {
                    resolve(column, schemas);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
}

/// One name, against the fields of every table.
fn resolve(ident: &mut Ident, schemas: &[SchemaRef]) {
    if ident.quote_style.is_some() {
        return;
    }
    let fields = schemas.iter().flat_map(|schema| schema.fields().iter());
    if fields.clone().any(|field| field.name() == &ident.value) {
        return;
    }
    let mut spellings = fields
        .map(|field| field.name())
        // ASCII folding only, so that Unicode case folding is never what decides which
        // column a request read.
        .filter(|name| name.eq_ignore_ascii_case(&ident.value))
        .collect::<Vec<_>>();
    spellings.sort_unstable();
    spellings.dedup();
    let [spelling] = spellings.as_slice() else {
        return;
    };
    ident.value = (*spelling).clone();
    // Quoted, so the resolved name is exact from here on whatever the planner is told to do
    // with unquoted identifiers.
    ident.quote_style = Some('"');
}

/// What the planner said, as a refusal naming the caller's own text.
///
/// Every failure here is the statement not fitting the files it names — an unknown column, a
/// type that will not compare, a region built from a column, which `geometry::Contains` raises
/// while the optimizer runs. Left alone it would read as this service failing rather than as
/// the request being wrong.
fn refusal(error: &DataFusionError) -> ApiError {
    // The root and not the outermost. An optimizer rule wraps whatever it raised in "Optimizer
    // rule 'simplify_expressions' failed", which is where the failure happened and says nothing
    // about what the caller wrote — and `geometry::Contains` raises every one of its refusals
    // from inside that rule.
    ApiError::bad_request(format!(
        "query: {}",
        first_line(&error.find_root().to_string())
    ))
}

/// Opening one of the request's tables, which is a statement about that file rather than
/// about the query.
fn opening(url: &Url, error: &DataFusionError) -> ApiError {
    let refusal = ApiError::bad_request(format!(
        "tables: {url} could not be read as parquet: {}",
        first_line(&error.to_string())
    ));
    // A local url resolved to a place on disk the caller did not write and must not be shown.
    match url.to_file_path().ok() {
        Some(path) => refusal.from_mount(&path),
        None => refusal,
    }
}

/// DataFusion's errors carry a backtrace and a context chain below the sentence that says
/// what went wrong; a caller needs the sentence.
fn first_line(message: &str) -> String {
    message
        .lines()
        .next()
        .unwrap_or(message)
        .trim_end()
        .to_owned()
}
