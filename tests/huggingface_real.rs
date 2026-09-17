//! The Hugging Face backend against the Hub itself.
//!
//! `huggingface.rs` covers the protocol against a stand-in, which is what CI runs. What this
//! adds is the two things a stand-in cannot be: a real redirect to a real CDN over TLS, and a
//! real HATS catalog nobody here wrote.
//!
//! It reaches the network, so it runs only when asked for:
//!
//! ```bash
//! HATS_API_TEST_HF=1 cargo test --test huggingface_real
//!
//! # or any other public repository, named in full
//! HATS_API_TEST_HF_URL=hf://datasets/owner/name cargo test --test huggingface_real
//! ```
//!
//! The target is public and anonymous, and should stay that way: a repository needing a token
//! does not belong in a test anyone can run. What the token does is covered against the
//! stand-in, where the credential can be watched on the wire.

mod common;

use common::skip_or_fail;
use hats_api::access::AccessPolicy;
use hats_api::storage::{self, RemoteDir, StorageOptions};
use object_store::ObjectStoreExt;

/// A HATS collection published by the Multimodal Universe project: small (under 3 MB), a
/// real `hats.properties`, and partitions written as LFS objects — which is what makes the
/// read go through the redirect rather than straight to the Hub.
const DEFAULT_URL: &str = "hf://datasets/UniverseTBD/mmu_gz10";

/// The catalog inside that collection. A collection's `hats_primary_table_url` points at it,
/// and naming it here keeps this test about the backend rather than about collection
/// following, which `hats/` covers over every backend at once.
const CATALOG: &str = "mmu_gz10";

/// One partition of it, named outright rather than discovered.
///
/// The Hub rate-limits its API, and a listing of this catalog is 766 partitions — so each
/// test that walked it to find a file would spend the quota that the one test about listing
/// needs. Naming it is also what `s3_remote.rs` does, for the same reason: the test about
/// reading should not pay for a discovery it is not testing.
///
/// **A 429 here is the Hub's rate limit and not a failure of this backend.** Re-run after a
/// few minutes rather than reading it as a bug.
const PARTITION: &str = "mmu_gz10/dataset/Norder=10/Dir=1150000/Npix=1159494.parquet";

fn target() -> Option<String> {
    if let Ok(url) = std::env::var("HATS_API_TEST_HF_URL")
        && !url.trim().is_empty()
    {
        return Some(url);
    }
    std::env::var("HATS_API_TEST_HF")
        .is_ok_and(|value| value == "1")
        .then(|| DEFAULT_URL.to_owned())
}

/// The default access policy, deliberately: what this also says is that a deployment which
/// has configured nothing can read the Hub, since `[api.access.hf]` with no list is any
/// endpoint and the Hub needs no loopback or cleartext permission.
fn open(raw: &str) -> RemoteDir {
    let url = storage::parse_url(raw).expect("a valid url");
    storage::open_dir(
        &url,
        &StorageOptions::default(),
        &AccessPolicy::default(),
        &common::transfers(),
    )
    .expect("the default policy reads the Hub")
}

/// The listing route, against a repository with more files than one page holds. This is the
/// half of the backend the `resolve` route cannot do at all.
#[tokio::test]
async fn the_hub_lists_a_real_repository() {
    let Some(url) = target() else {
        return skip_or_fail("HATS_API_TEST_HF");
    };
    let dir = open(&url);

    let entries = dir.list(CATALOG).await.expect("the Hub lists the catalog");
    let names: Vec<&str> = entries.iter().map(|entry| entry.name.as_str()).collect();
    assert!(
        names.contains(&"hats.properties"),
        "the catalog's own properties file is not in the listing: {names:?}"
    );
    assert!(
        names.iter().any(|name| name.ends_with(".parquet")),
        "no partition is in the listing: {names:?}"
    );
    // A size is what a plan reports and what `_metadata` would otherwise be read for, so a
    // listing that answered zero for everything would be one this service acts on.
    assert!(
        entries
            .iter()
            .filter(|entry| entry.name.ends_with(".parquet"))
            .all(|entry| entry.size > 0),
        "a partition was listed with no size"
    );
}

/// A catalog several levels inside a repository, addressed as its own url.
///
/// A repository is not a catalog: one can hold several, or hold one under a prefix. So the
/// first two segments are the repository — which is what the Hub's routes take — and
/// everything after is a path inside it, which both routes carry through. This is the same
/// catalog the tests above reach through the collection at the root, addressed directly.
#[tokio::test]
async fn a_catalog_deeper_in_the_repository_is_a_url_of_its_own() {
    let Some(url) = target() else {
        return skip_or_fail("HATS_API_TEST_HF");
    };
    let dir = open(&format!("{url}/{CATALOG}"));

    let properties = dir
        .read("hats.properties")
        .await
        .expect("the catalog's properties, addressed from inside the repository");
    assert!(
        String::from_utf8_lossy(&properties).contains("hats_col_ra"),
        "not a HATS properties file"
    );
}

/// A file the Hub serves directly, which is the hop that stays on one origin — so the
/// token, where there is one, goes with it.
#[tokio::test]
async fn a_small_file_is_read_from_the_hub() {
    let Some(url) = target() else {
        return skip_or_fail("HATS_API_TEST_HF");
    };
    let dir = open(&url);

    let properties = dir
        .read(&format!("{CATALOG}/hats.properties"))
        .await
        .expect("the catalog's properties");
    let text = String::from_utf8_lossy(&properties);
    assert!(
        text.contains("hats_col_ra"),
        "not a HATS properties file:\n{text}"
    );
}

/// The one this test binary exists for: a partition stored in LFS, which the Hub answers by
/// redirecting to a presigned url on a CDN over TLS. Against the stand-in the hop is
/// cleartext to cleartext on loopback; here it is the real thing, including that the range
/// survives the hop and the CDN answers a `206` rather than the whole object.
#[tokio::test]
async fn a_partition_is_read_through_the_redirect_to_the_cdn() {
    let Some(url) = target() else {
        return skip_or_fail("HATS_API_TEST_HF");
    };
    let dir = open(&url);
    let key = object_store::path::Path::from(format!(
        "{}{PARTITION}",
        dir.url.path().trim_start_matches('/')
    ));

    // How large it is comes from the Hub rather than from a listing, which is one request
    // against the route this test is about rather than a walk of the whole catalog.
    let size = dir
        .store
        .head(&key)
        .await
        .expect("the partition's metadata")
        .size;
    assert!(
        size > 4,
        "a partition of {size} bytes is not a parquet file"
    );

    // The last four bytes of a parquet file are its magic. Asked for as a range, so what
    // this shows is a ranged read arriving at the CDN rather than a whole file being pulled
    // down and sliced here — which is the difference between this backend working and this
    // backend appearing to work.
    let tail = dir
        .store
        .get_range(&key, size - 4..size)
        .await
        .expect("a ranged read of a partition");
    assert_eq!(&tail[..], b"PAR1", "the range did not survive the redirect");
}
