//! Reading the published catalogs into the cache before anyone asks, and again before what was
//! read expires.
//!
//! **A `[[tap.table]]` is a catalog this service knows it will be asked about**, and a TAP client
//! fetches `/tables`, `TAP_SCHEMA` and `/examples` before it asks for a row — each of them built
//! out of every published catalog. So those catalogs are read once at startup, in the
//! background, and read again at nine tenths of their lifetime, so that the first client after
//! startup and the first one after an expiry both find them there. Nothing else is warmed: no
//! catalog under a mount is discovered to be read.
//!
//! **Nothing waits on this and nothing fails for it.** The service answers from its first
//! request whether or not a table has been read yet, and a table that cannot be read is logged
//! and tried again a minute later; a request for it meanwhile reads it itself and gets whatever
//! refusal it would have got anyway.
//!
//! A refresh reads through [`CatalogCache::renewed`], so the catalog as it was keeps answering
//! until the catalog as it is has been read, and a refresh that fails leaves the old one in
//! place to expire when it would have.

use std::time::{Duration, Instant};

use url::Url;

use crate::app::service::Service;
use crate::error::ApiError;
use crate::hats::{CatalogCache, HatsCatalog, Lifetime};
use crate::storage::{self, StorageOptions};

/// How long after a failed read a table is tried again.
///
/// Short against any lifetime worth warming for, and long enough that a catalog whose store
/// is down is not asked about more than once a minute.
const RETRY: Duration = Duration::from_secs(60);

/// Start keeping every published table warm, one task per table, and return at once.
pub fn warm(service: &Service) {
    for table in service.tap_tables.iter() {
        let service = service.clone();
        let name = table.qualified().to_owned();
        let url = table.url().clone();
        tokio::spawn(async move { keep_warm(&service, &name, &url).await });
    }
}

/// Read one table, and read it again before it expires, for as long as the process runs.
async fn keep_warm(service: &Service, name: &str, url: &Url) {
    let mut renew = false;
    loop {
        let cache = service.catalogs_for(url);
        let lifetime = cache.lifetime();
        // Nowhere to keep what would be read.
        if lifetime == Lifetime::Off {
            return;
        }
        let cache = match renew {
            true => cache.renewed(),
            false => cache,
        };
        let started = Instant::now();
        let wait = match read(service, url, &cache).await {
            Ok(()) => {
                tracing::info!(
                    table = name,
                    elapsed_ms = started.elapsed().as_millis(),
                    "published table read into the catalog cache"
                );
                renew = true;
                match refresh_after(lifetime) {
                    Some(wait) => wait,
                    None => return,
                }
            }
            Err(error) => {
                tracing::warn!(table = name, %error, "cannot read a published table ahead of time");
                refresh_after(lifetime).map_or(RETRY, |wait| wait.min(RETRY))
            }
        };
        tokio::time::sleep(wait).await;
    }
}

/// When a table read now is read again: at nine tenths of its lifetime, so the next reading is
/// in before this one expires. Never, for a catalog kept until evicted.
fn refresh_after(lifetime: Lifetime) -> Option<Duration> {
    match lifetime {
        Lifetime::Off | Lifetime::Forever => None,
        Lifetime::For(duration) => Some(duration.mul_f64(0.9)),
    }
}

/// Everything a request against the table would read about it: the properties, the partition
/// list, the schema, a position to centre an example on, and — where the partitions are
/// directories — the names inside every one of them.
async fn read(service: &Service, url: &Url, cache: &CatalogCache) -> Result<(), ApiError> {
    // A published table carries no storage options: what reaches it is its mount's.
    let dir = storage::open_dir(
        url,
        &StorageOptions::default(),
        &service.policy,
        &service.transfers,
    )?;
    let data = service.data_files_for(url);
    let catalog = HatsCatalog::open(dir, cache, service.catalog_limits.max_metadata_bytes).await?;
    let partitions = catalog.partitions().await?;
    catalog.schema(data).await?;
    catalog.example_position(data).await;
    if catalog.properties().partition_is_a_directory() {
        catalog.names(partitions.cells(), data).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::app::testing::{serving, with_tap};
    use crate::config::{ApiConfig, LimitsConfig, ServerConfig, TapConfig, TapTableConfig};

    fn published(dir: &std::path::Path) -> Service {
        let tap = TapConfig {
            tables: vec![TapTableConfig {
                name: "sky.objects".to_owned(),
                path: "/".to_owned(),
                examples: Vec::new(),
            }],
            jobs: Default::default(),
        };
        with_tap(
            serving(dir),
            &ApiConfig::default(),
            &LimitsConfig::default(),
            &ServerConfig::default(),
            &tap,
        )
    }

    fn table_url(service: &Service) -> Url {
        service.tap_tables.iter().next().unwrap().url().clone()
    }

    /// Once read ahead of time, a published catalog answers from the cache: its own files can
    /// go and a request still finds its properties and partitions.
    #[tokio::test]
    async fn a_warmed_table_is_answered_without_reading_its_catalog() {
        let dir = crate::hats::query::tests::fixture(true);
        let service = published(dir.path());
        let url = table_url(&service);
        read(&service, &url, &service.catalogs_for(&url))
            .await
            .unwrap();

        fs::remove_file(dir.path().join("hats.properties")).unwrap();
        fs::remove_file(dir.path().join(crate::hats::partitions::PARTITION_INFO)).unwrap();
        let dir_handle = storage::open_dir(
            &url,
            &StorageOptions::default(),
            &service.policy,
            &service.transfers,
        )
        .unwrap();
        let catalog = HatsCatalog::open(dir_handle, &service.catalogs_for(&url), u64::MAX)
            .await
            .unwrap();
        assert!(!catalog.partitions().await.unwrap().is_empty());
    }

    /// A refresh reads the catalog as it is now, even though what was held has not expired —
    /// and until it has succeeded the old reading keeps answering.
    #[tokio::test]
    async fn a_refresh_replaces_what_was_held_before_it_expires() {
        let dir = crate::hats::query::tests::fixture(true);
        let service = published(dir.path());
        let url = table_url(&service);
        let open = async |cache: &CatalogCache| {
            let handle = storage::open_dir(
                &url,
                &StorageOptions::default(),
                &service.policy,
                &service.transfers,
            )
            .unwrap();
            HatsCatalog::open(handle, cache, u64::MAX).await
        };
        read(&service, &url, &service.catalogs_for(&url))
            .await
            .unwrap();
        let before = open(&service.catalogs_for(&url)).await.unwrap();
        let name = before.properties().name().map(str::to_owned);

        let properties = dir.path().join("hats.properties");
        let text = fs::read_to_string(&properties).unwrap();
        let name_line = format!("obs_collection={}", name.as_deref().unwrap());
        fs::write(
            &properties,
            text.replace(&name_line, "obs_collection=renewed"),
        )
        .unwrap();
        // Unexpired, so an ordinary open still finds the old reading.
        let held = open(&service.catalogs_for(&url)).await.unwrap();
        assert_eq!(held.properties().name().map(str::to_owned), name);

        read(&service, &url, &service.catalogs_for(&url).renewed())
            .await
            .unwrap();
        let after = open(&service.catalogs_for(&url)).await.unwrap();
        assert_eq!(after.properties().name(), Some("renewed"));

        // A refresh that fails keeps what was held.
        fs::remove_file(&properties).unwrap();
        assert!(
            read(&service, &url, &service.catalogs_for(&url).renewed())
                .await
                .is_err()
        );
        let kept = open(&service.catalogs_for(&url)).await.unwrap();
        assert_eq!(kept.properties().name(), Some("renewed"));
    }

    /// A catalog is read again at nine tenths of its lifetime, and one kept until evicted, or
    /// not kept at all, is never read again.
    #[test]
    fn a_table_is_read_again_before_it_expires() {
        assert_eq!(
            refresh_after(Lifetime::For(Duration::from_secs(1000))),
            Some(Duration::from_secs(900))
        );
        assert_eq!(refresh_after(Lifetime::Forever), None);
        assert_eq!(refresh_after(Lifetime::Off), None);
    }
}
