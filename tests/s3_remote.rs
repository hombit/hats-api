//! S3 against public deployments someone else runs.
//!
//! These reach the network, so they run only when asked for. `s3_local.rs` covers the
//! protocol against an in-process server and `s3_minio.rs` covers a real MinIO; what
//! these add is what neither can fake — a catalog nobody here wrote, at a scale nobody
//! here would generate, over TLS, behind whatever the operator put in front of it.
//!
//! Both targets are anonymous, so no secret is involved and none should be: an object
//! that needs a credential does not belong in a scheduled public test.
//!
//! ```bash
//! # the public AWS bucket the README documents, with its built-in configuration
//! HATS_API_TEST_AWS=1 cargo test --test s3_remote
//!
//! # any other deployment, named in full
//! HATS_API_TEST_SNAD_URL=s3://bucket/key.parquet \
//! HATS_API_TEST_SNAD_ENDPOINT=https://example.com \
//! HATS_API_TEST_SNAD_COLUMN=objectid HATS_API_TEST_SNAD_VALUE=1 \
//!   cargo test --test s3_remote
//! ```

mod common;

use common::{Defaults, RemoteTarget, lookup, permissive_policy, row_count, skip_or_fail};
use hats_api::error::ApiError;

/// The object the README's example uses. Public, anonymous, and a real HATS partition
/// — the `Norder=`/`Dir=`/`Npix=` key, a nested `lightcurve` column, and ~100 MB, so a
/// point lookup is only quick if ranged reads and pruning are working.
///
/// Using the README's own numbers means this test also keeps the README honest.
fn aws_defaults() -> Defaults {
    Defaults {
        url: "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/\
              Norder=5/Dir=10000/Npix=12240/part0.snappy.parquet",
        column: "_healpix_29",
        value: "3445524782181585918",
        endpoint: None,
        expected_rows: Some(1),
    }
}

fn aws() -> Option<RemoteTarget> {
    RemoteTarget::from_env("HATS_API_TEST_AWS", Some(aws_defaults()))
}

/// A ZTF catalog on the SNAD object store: 497,376 rows in 4 row groups, ~380 MB, with
/// a page index — which is the point of it. `objectid` is unique across the file, so
/// the lookup below matches exactly one row, and matching it quickly is a claim about
/// page-level pruning rather than about reading 380 MB.
///
/// Anonymous, and served from a MinIO behind nginx: a different endpoint shape from
/// AWS, and path-style rather than virtual-host.
fn snad_defaults() -> Defaults {
    Defaults {
        url: "s3://tests/pageidx_64k.parquet",
        endpoint: Some("https://s3.lpc.snad.space"),
        column: "objectid",
        value: "390204400004344",
        expected_rows: Some(1),
    }
}

fn snad() -> Option<RemoteTarget> {
    RemoteTarget::from_env("HATS_API_TEST_SNAD", Some(snad_defaults()))
}

/// The lookup the target says should match. This is the whole path against a real
/// server: policy, store construction, footer read, pruning, ranged reads, projection.
async fn reads_the_documented_row(target: &RemoteTarget) {
    let result = lookup(
        &target.url,
        &target.options,
        &permissive_policy(),
        &target.column,
        &target.value,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("{}: the lookup failed: {error}", target.name));

    let rows = row_count(&result);
    match target.expected_rows {
        Some(expected) => assert_eq!(rows, expected, "{}: wrong row count", target.name),
        // Not knowing the count is fine; reading the file at all is the point.
        None => assert!(rows > 0, "{}: expected at least one row", target.name),
    }
    assert!(
        !result.schema.fields().is_empty(),
        "{}: the result has no schema",
        target.name
    );
}

/// A projection over the same lookup. Asking for fewer columns must return exactly
/// those, which is also what keeps a wide catalog's read small.
async fn honours_a_projection(target: &RemoteTarget) {
    let columns = vec![target.column.clone()];
    let result = lookup(
        &target.url,
        &target.options,
        &permissive_policy(),
        &target.column,
        &target.value,
        Some(&columns),
    )
    .await
    .unwrap_or_else(|error| panic!("{}: the projected lookup failed: {error}", target.name));

    assert_eq!(
        result.schema.fields().len(),
        1,
        "{}: expected one column, got {:?}",
        target.name,
        result
            .schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>()
    );
}

/// A key that is not there, against a server that is. Real services differ from a
/// local one here — some answer 404, some 403 to hide existence — and either way it
/// must not come back as a policy refusal or a 500, and must not carry a credential.
async fn a_missing_object_fails_cleanly(target: &RemoteTarget) {
    let error = common::expect_error(
        lookup(
            &target.sibling_url("definitely-not-here-9f3a1c.parquet"),
            &target.options,
            &permissive_policy(),
            &target.column,
            &target.value,
            None,
        )
        .await,
        &format!("{}: a missing object should fail", target.name),
    );
    // Forbidden is this service's own refusal, and the policy allowed this url.
    assert!(
        !matches!(error, ApiError::Forbidden(_)),
        "{}: a missing object was reported as a policy refusal: {error}",
        target.name
    );
    assert_no_credentials(&error.to_string(), &target.url, target.name);
}

/// Whatever the url carried, the error must not repeat it.
fn assert_no_credentials(message: &str, url: &str, name: &str) {
    for (key, value) in url
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
    {
        if matches!(key, "secret_access_key" | "session_token") && !value.is_empty() {
            assert!(
                !message.contains(value),
                "{name}: the error leaked {key}: {message}"
            );
        }
    }
}

/// Also the virtual-host addressing test: the target names no `endpoint`, which is the
/// path where the bucket becomes a subdomain and the region picks the host. No local
/// server exercises that, and asserting it here costs nothing — whereas a second test
/// reading the same hundred-megabyte partition would cost a second read of it.
#[tokio::test]
async fn aws_reads_the_documented_row() {
    let Some(target) = aws() else {
        return skip_or_fail("HATS_API_TEST_AWS");
    };
    assert!(
        !target.url.contains("endpoint="),
        "the AWS target should name no endpoint, so that this exercises virtual-host \
         addressing; it is {}",
        target.url
    );
    reads_the_documented_row(&target).await;
}

#[tokio::test]
async fn aws_honours_a_projection() {
    let Some(target) = aws() else {
        return skip_or_fail("HATS_API_TEST_AWS");
    };
    honours_a_projection(&target).await;
}

#[tokio::test]
async fn aws_reports_a_missing_object_cleanly() {
    let Some(target) = aws() else {
        return skip_or_fail("HATS_API_TEST_AWS");
    };
    a_missing_object_fails_cleanly(&target).await;
}

#[tokio::test]
async fn snad_reads_the_documented_row() {
    let Some(target) = snad() else {
        return skip_or_fail("HATS_API_TEST_SNAD");
    };
    reads_the_documented_row(&target).await;
}

#[tokio::test]
async fn snad_honours_a_projection() {
    let Some(target) = snad() else {
        return skip_or_fail("HATS_API_TEST_SNAD");
    };
    honours_a_projection(&target).await;
}

#[tokio::test]
async fn snad_reports_a_missing_object_cleanly() {
    let Some(target) = snad() else {
        return skip_or_fail("HATS_API_TEST_SNAD");
    };
    a_missing_object_fails_cleanly(&target).await;
}
