//! Reading what this service publishes, for the resources that describe it.
//!
//! `TAP_SCHEMA`, VOSI's `/tables`, `/examples` and the page at the base url are built
//! from this. It costs a few small
//! reads per published catalog — its properties, its partition list and the one
//! `dataset/_common_metadata` that holds every partition's columns and no rows.
//!
//! `/examples` adds one more, and it is the only place here that touches data: a single row,
//! for a position to centre a cone on. `data_thumbnail.parquet` answers it in one small `GET`
//! where a catalog has one, and where it has not, the first row group of two columns of its
//! smallest partition does. See [`example_position`] for why nothing cheaper works.
//!
//! **Read per request rather than held.** Nothing in this service is a registry that
//! accumulates across requests, and a catalog an operator republished is then described as
//! it is now rather than as it was when the process started.

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

use crate::app::service::Service;
use crate::error::ApiError;
use crate::hats::partitions::DATA_THUMBNAIL;
use crate::hats::table::HatsTable;
use crate::hats::{HatsPartition, HatsPartitionList};
use crate::storage::{self, StorageOptions};
use crate::tap::metadata::{self, Marks, TableMetadata};
use crate::tap::{TapTable, schema};

/// One published table, opened, as the resources that describe it need it.
///
/// Everything here comes from one opening of the catalog. Two resources want overlapping
/// halves of it — the metadata documents want the columns, `/examples` wants a position and
/// four column names — and reading it twice would be reading it twice.
pub(super) struct Published<'a> {
    /// What the operator published it as.
    pub table: &'a TapTable,
    /// What the two metadata documents say about it.
    pub metadata: TableMetadata,
    /// The two columns holding a position, as the catalog names them. `None` where the
    /// catalog names neither, which is a catalog no cone can be written against.
    pub coordinates: Option<(String, String)>,
    /// The catalog's deepest partition — see [`deepest`]. What reads it is the radius of
    /// the example cone, a cell being the one length scale a catalog offers.
    pub cell: Option<HatsPartition>,
    /// A position the catalog holds a row at, as `(ra, dec)` in degrees — see
    /// [`example_position`].
    pub position: Option<(f64, f64)>,
    /// `hats_nrows`, where the catalog states it.
    pub rows: Option<u64>,
    /// `obs_title`, where the catalog states it.
    pub title: Option<String>,
}

/// Every table this service publishes, opened and described.
///
/// **A catalog that cannot be read fails the whole document.** Leaving it out would tell a
/// client the table does not exist, which is the one thing it cannot tell from the truth;
/// a refusal naming the table is something an operator can act on and a client can retry.
pub(super) async fn open_each(service: &Service) -> Result<Vec<Published<'_>>, ApiError> {
    let mut published = Vec::new();
    for table in service.tap_tables.iter() {
        // A context per table, because a context is what a store is registered into and
        // DataFusion keys one by authority — see "One store per authority". Two published
        // tables under two mounts in one bucket hold two sets of credentials, and sharing
        // a context would have the second registration decide both. Nothing is carried
        // between tables here anyway: each is opened, described, and let go.
        let ctx = SessionContext::new();
        let url = table.url();
        let dir = storage::open_dir(
            url,
            &StorageOptions::default(),
            &service.policy,
            &service.transfers,
        )?;
        let data = service.data_files_for(url).clone();
        let catalog = HatsTable::open(&ctx, &dir, &data, service.adql_limits.catalog)
            .await
            .map_err(|error| describing(table.qualified(), &error))?;
        let coordinates = catalog.coordinates();
        let metadata = metadata::describe(
            table.qualified(),
            table.schema(),
            &TableProvider::schema(&catalog),
            Marks {
                ra: coordinates.map(|(ra, _)| ra),
                dec: coordinates.map(|(_, dec)| dec),
                healpix: catalog.index(),
            },
            None,
        );
        let properties = catalog.catalog().properties();
        // Shown beside the name and never read as a fact about the rows, so a count that
        // will not parse is a table listed without one rather than a page that fails.
        let rows = properties.rows().ok().flatten();
        let title = properties.get("obs_title").map(str::to_owned);
        let cell = deepest(catalog.catalog().partitions());
        let position = match coordinates {
            Some((ra, dec)) => {
                example_position(&ctx, &catalog, &data, cell.as_ref(), ra, dec).await
            }
            None => None,
        };
        published.push(Published {
            table,
            metadata,
            coordinates: coordinates.map(|(ra, dec)| (ra.to_owned(), dec.to_owned())),
            cell,
            position,
            rows,
            title,
        });
    }
    Ok(published)
}

/// A position the catalog really holds a row at, read out of one of its own rows.
///
/// **A cone has to be centred on something that is there, and only a row says where that
/// is.** Everything cheaper is a guess that fails on some real catalog: a partition's centre
/// is empty wherever the data fills a corner of its cell, which is every catalog covering a
/// patch of sky rather than the whole of it, and a cone wide enough to cover the cell
/// instead is no longer an example of the query anybody writes. `hats` reaches the same
/// conclusion in `io/summary_file.py`, which takes the `ra` and `dec` of an example row.
///
/// `data_thumbnail.parquet` first, which is what `hats` writes at a catalog's root for
/// exactly this — a handful of rows, so reading it is one small `GET`. A catalog without one
/// costs the first row group of two columns of its smallest partition instead, which is the
/// price of an example that works; most published catalogs predate the thumbnail.
///
/// `None` wherever neither can be read, and the example is then a plain first-rows query
/// rather than a cone that might return nothing.
async fn example_position(
    ctx: &SessionContext,
    catalog: &HatsTable,
    data: &crate::access::data::DataFiles,
    cell: Option<&HatsPartition>,
    ra: &str,
    dec: &str,
) -> Option<(f64, f64)> {
    let catalog = catalog.catalog();
    if let Ok(thumbnail) = catalog.dir().child(DATA_THUMBNAIL)
        && let Some(found) = read_position(ctx, thumbnail.url.as_str(), ra, dec).await
    {
        return Some(found);
    }
    let files = catalog.partition(cell?).ok()?.files(data).await.ok()?;
    read_position(ctx, files.first()?.url.as_str(), ra, dec).await
}

/// The two coordinates of one row of one parquet file.
///
/// The columns are named as `Column`s rather than parsed from text: a catalog's column may be
/// mixed-case or carry a character the parser would read as structure, and this context has
/// DataFusion's own identifier normalization on. `limit(1)` is what keeps it to a row group.
async fn read_position(ctx: &SessionContext, url: &str, ra: &str, dec: &str) -> Option<(f64, f64)> {
    let options = datafusion::prelude::ParquetReadOptions {
        // A HATS partition is named whatever `hats_npix_suffix` says, and the thumbnail is
        // read the same way for the same reason the schema is.
        file_extension: "",
        ..Default::default()
    };
    let named = |name: &str| Expr::Column(Column::new_unqualified(name.to_owned()));
    let batches = ctx
        .read_parquet(url, options)
        .await
        .ok()?
        .select(vec![named(ra), named(dec)])
        .ok()?
        .limit(0, Some(1))
        .ok()?
        .collect()
        .await
        .ok()?;
    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
    // Cast rather than match: ZTF DR24 writes its coordinates as `Float32`, and a catalog is
    // free to write them at any width.
    let value = |at: usize| -> Option<f64> {
        let column = cast(batch.column(at), &DataType::Float64).ok()?;
        let column = column.as_any().downcast_ref::<Float64Array>()?;
        column.is_valid(0).then(|| column.value(0))
    };
    let (ra, dec) = (value(0)?, value(1)?);
    (ra.is_finite() && dec.is_finite()).then_some((ra, dec))
}

/// The catalog's deepest partition — the first of them, so that the same catalog answers
/// the same way twice.
///
/// **The deepest rather than the first, because that is where the rows are.** HATS splits a
/// cell when it holds too many rows, so the deepest order in the list is the most crowded
/// part of the sky this catalog covers; the front of the list is wherever HEALPix numbering
/// happens to start, which for a catalog covering one patch of sky is as likely to be its
/// emptiest cell as its fullest. It also makes the cell small, and a cell a cone has to
/// cover is a cone as wide as the cell.
fn deepest(partitions: &HatsPartitionList) -> Option<HatsPartition> {
    let order = partitions.order()?;
    partitions
        .cells()
        .iter()
        .find(|cell| cell.order == order)
        .cloned()
}

/// Every table this service publishes, described — `TAP_SCHEMA`'s own five first, since
/// that is what a client queries before it knows any other name.
pub(super) async fn describe(service: &Service) -> Result<Vec<TableMetadata>, ApiError> {
    let mut described = schema::self_description();
    described.extend(
        open_each(service)
            .await?
            .into_iter()
            .map(|published| published.metadata),
    );
    Ok(described)
}

/// A published table this service could not read, said as a failure to describe it rather
/// than as whatever the store said about a url the caller never wrote.
fn describing(name: &str, error: &ApiError) -> ApiError {
    tracing::warn!(table = name, error = %error, "a published table could not be described");
    ApiError::internal(format!(
        "{name} is published here and could not be read, so this service cannot say what \
         it holds"
    ))
}
