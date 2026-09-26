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
//!
//! **Tables that do not fit are read once and never refreshed.** Every published table shares
//! `[limits] max_catalog_cache_bytes` with every other catalog, so where the tables together
//! weigh more than it holds, keeping them warm is each refresh evicting what the last one read:
//! a whole round of catalog reads per lifetime, with nothing kept to show for it, and every
//! catalog a caller is reading pushed out along the way. So the operator is told once, with
//! the two numbers, and from then on a table is read when a request asks for it, the way any
//! other catalog is. A renewal is also the moment two readings of a table are held at once,
//! the old one until it expires, so a set that only just fits is exactly the one it would push
//! over.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
    let footprint = Arc::new(Footprint::default());
    for table in service.tap_tables.iter() {
        let service = service.clone();
        let name = table.qualified().to_owned();
        let url = table.url().clone();
        let footprint = Arc::clone(&footprint);
        tokio::spawn(async move { keep_warm(&service, &name, &url, &footprint).await });
    }
}

/// What the published tables weigh together, as each was last read, against the one budget
/// they share — and whether they have been found not to fit it.
#[derive(Debug, Default)]
struct Footprint {
    weights: Mutex<HashMap<String, u64>>,
    /// Once set, stays set: a table that shrank on its next reading would only be found to by
    /// reading it, which is the refresh this stops.
    overflowed: AtomicBool,
}

impl Footprint {
    /// Record what one table weighs, and say whether the tables still fit `max_bytes`. The
    /// first reading that does not fit is logged; every one after it is only answered.
    fn fits(&self, name: &str, weight: u64, max_bytes: u64) -> bool {
        if self.overflowed.load(Ordering::Relaxed) {
            return false;
        }
        let total = {
            let Ok(mut weights) = self.weights.lock() else {
                return false;
            };
            weights.insert(name.to_owned(), weight);
            weights
                .values()
                .fold(0u64, |total, weight| total.saturating_add(*weight))
        };
        if total <= max_bytes {
            return true;
        }
        if !self.overflowed.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                table = name,
                published_bytes = total,
                max_catalog_cache_bytes = max_bytes,
                "the published tables do not fit the catalog cache; they will not be \
                 refreshed ahead of time, and are read when a request asks for them"
            );
        }
        false
    }

    fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Relaxed)
    }
}

/// Read one table, and read it again before it expires, for as long as the process runs and
/// the published tables fit the cache.
async fn keep_warm(service: &Service, name: &str, url: &Url, footprint: &Footprint) {
    let mut renew = false;
    loop {
        if footprint.overflowed() {
            return;
        }
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
            Ok(weight) => {
                tracing::info!(
                    table = name,
                    elapsed_ms = started.elapsed().as_millis(),
                    bytes = weight,
                    "published table read into the catalog cache"
                );
                if !footprint.fits(name, weight, cache.max_bytes()) {
                    return;
                }
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
/// directories — the names inside every one of them. Answers what all of it weighs in the cache.
async fn read(service: &Service, url: &Url, cache: &CatalogCache) -> Result<u64, ApiError> {
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
    catalog.cached_weight()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::app::testing::{serving, with_tap};
    use crate::config::{ApiConfig, LimitsConfig, ServerConfig, TapConfig, TapTableConfig};

    fn published(dir: &std::path::Path) -> Service {
        published_within(dir, &LimitsConfig::default())
    }

    fn published_within(dir: &std::path::Path, limits: &LimitsConfig) -> Service {
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
            limits,
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

    /// Reading a table says what it weighs in the cache, and a refresh — which reads it all
    /// again — weighs the same.
    #[tokio::test]
    async fn a_read_says_what_the_table_weighs() {
        let dir = crate::hats::query::tests::fixture(true);
        let service = published(dir.path());
        let url = table_url(&service);
        let cache = service.catalogs_for(&url);
        let first = read(&service, &url, &cache).await.unwrap();
        assert!(first > 0);
        assert_eq!(read(&service, &url, &cache.renewed()).await.unwrap(), first);
    }

    /// Tables that together outgrow the budget stop being refreshed — every one of them, not
    /// only the table that tipped it over — and stay stopped.
    #[test]
    fn tables_that_do_not_fit_are_not_refreshed() {
        let footprint = Footprint::default();
        assert!(footprint.fits("a", 600, 1000));
        assert!(footprint.fits("a", 700, 1000));
        assert!(!footprint.fits("b", 400, 1000));
        assert!(footprint.overflowed());
        assert!(!footprint.fits("a", 1, 1000));
    }

    /// A table that alone outgrows the cache is read once and not kept warm: the task ends
    /// rather than sleeping until the next refresh, and says why.
    #[tokio::test]
    async fn a_table_larger_than_the_cache_is_read_once() {
        let dir = crate::hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_catalog_cache_bytes: bytesize::ByteSize::b(1),
            ..LimitsConfig::default()
        };
        let service = published_within(dir.path(), &limits);
        let url = table_url(&service);
        assert_eq!(service.catalogs_for(&url).max_bytes(), 1);
        let footprint = Footprint::default();
        tokio::time::timeout(
            Duration::from_secs(10),
            keep_warm(&service, "sky.objects", &url, &footprint),
        )
        .await
        .expect("a table that does not fit is not refreshed");
        assert!(footprint.overflowed());
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
