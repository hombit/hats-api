//! Reading what this service publishes, for the two resources that describe it.
//!
//! `TAP_SCHEMA` and VOSI's `/tables` are the same facts in two documents, so both are
//! built from this. It costs a few small reads per published catalog — its properties, its
//! partition list and the one `dataset/_common_metadata` that holds every partition's
//! columns and no rows — and no partition is opened.
//!
//! **Read per request rather than held.** Nothing in this service is a registry that
//! accumulates across requests, and a catalog an operator republished is then described as
//! it is now rather than as it was when the process started.

use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::app::service::Service;
use crate::error::ApiError;
use crate::hats::table::HatsTable;
use crate::storage::{self, StorageOptions};
use crate::tap::metadata::{self, Marks, TableMetadata};
use crate::tap::schema;

/// Every table this service publishes, described — `TAP_SCHEMA`'s own five first, since
/// that is what a client queries before it knows any other name.
///
/// **A catalog that cannot be read fails the whole document.** Leaving it out would tell a
/// client the table does not exist, which is the one thing it cannot tell from the truth;
/// a refusal naming the table is something an operator can act on and a client can retry.
pub(super) async fn describe(service: &Service) -> Result<Vec<TableMetadata>, ApiError> {
    let mut described = schema::self_description();
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
        described.push(metadata::describe(
            table.qualified(),
            table.schema(),
            &TableProvider::schema(&catalog),
            Marks {
                ra: coordinates.map(|(ra, _)| ra),
                dec: coordinates.map(|(_, dec)| dec),
                healpix: catalog.index(),
            },
            None,
        ));
    }
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
