//! Reading what this service publishes, for the resources that describe it.
//!
//! `TAP_SCHEMA`, VOSI's `/tables`, `/examples` and the page at the base url are built
//! from this. It costs a few small
//! reads per published catalog — its properties, its partition list and the one
//! `dataset/_common_metadata` that holds every partition's columns and no rows.
//!
//! `/examples` adds one more, and it is the only place here that touches data: a single row,
//! for a position to centre a cone on — see [`crate::hats::HatsCatalog::example_position`]
//! for why nothing cheaper works.
//!
//! **All of it is read through the catalog cache**, so a client fetching the three documents
//! in a row pays for the reads once, and a catalog an operator republished is described as
//! it is now once its mount's lifetime is over.

use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::app::service::Service;
use crate::error::ApiError;
use crate::hats::HatsPartition;
use crate::hats::table::HatsTable;
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
    /// The catalog's deepest partition — see [`crate::hats::HatsPartitionList::deepest`].
    /// What reads it is the radius of the example cone, a cell being the one length scale a
    /// catalog offers.
    pub cell: Option<HatsPartition>,
    /// A position the catalog holds a row at, as `(ra, dec)` in degrees — see
    /// [`crate::hats::HatsCatalog::example_position`].
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
        let catalog = HatsTable::open(
            &ctx,
            &dir,
            &data,
            service.adql_limits.catalog,
            &service.catalogs_for(url),
        )
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
        let cell = catalog
            .catalog()
            .partitions()
            .await
            .map_err(|error| describing(table.qualified(), &error))?
            .deepest()
            .cloned();
        // `None` wherever no row can be read, and the example is then a plain first-rows
        // query rather than a cone that might return nothing.
        let position = catalog.catalog().example_position(&data).await;
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
