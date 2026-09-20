//! Reading what this service publishes, for the resources that describe it.
//!
//! `TAP_SCHEMA`, VOSI's `/tables` and `/examples` are built from this. It costs a few small
//! reads per published catalog — its properties, its partition list and the one
//! `dataset/_common_metadata` that holds every partition's columns and no rows — and no
//! partition is opened.
//!
//! **Read per request rather than held.** Nothing in this service is a registry that
//! accumulates across requests, and a catalog an operator republished is then described as
//! it is now rather than as it was when the process started.

use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::app::service::Service;
use crate::error::ApiError;
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
    /// The catalog's deepest partition — see [`deepest`]. A partition exists because rows
    /// are there, so a cone covering this cell holds some of them, and that is known
    /// without reading a data file.
    pub cell: Option<HatsPartition>,
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
        published.push(Published {
            table,
            metadata,
            coordinates: coordinates.map(|(ra, dec)| (ra.to_owned(), dec.to_owned())),
            cell: deepest(catalog.partitions()),
        });
    }
    Ok(published)
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
