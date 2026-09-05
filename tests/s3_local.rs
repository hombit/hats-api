//! S3 against a real S3 server, in-process.
//!
//! `s3s-fs` speaks the S3 protocol over a temporary directory, so these run everywhere
//! `cargo test` runs — no Docker, no network, no credentials — and they cover the half
//! of the S3 path the unit tests cannot reach: that a parquet file is actually
//! readable over the wire, in ranged pieces, with the keys HATS uses.

mod common;

use common::{ACCESS_KEY_ID, FIXTURE_ROWS, TestS3, lookup, permissive_policy, row_count};
use hats_api::error::ApiError;

/// The baseline: a file put in a bucket comes back through the whole path, and the
/// lookup finds the one row it should.
#[tokio::test]
async fn reads_a_parquet_file_over_s3() {
    let server = TestS3::anonymous().await;
    server.put_parquet("catalog/part0.parquet");

    let result = lookup(
        &server.url("catalog/part0.parquet", ""),
        &permissive_policy(),
        "objectid",
        "42",
        None,
    )
    .await
    .expect("the lookup should succeed");

    assert_eq!(row_count(&result), 1);
    assert_eq!(result.schema.fields().len(), 4);
}

/// A point lookup in a file of many row groups. If ranged reads were broken — a store
/// returning the whole object for every range, or the wrong offset — the footer would
/// not parse and this would fail rather than quietly reading too much.
#[tokio::test]
async fn a_point_lookup_finds_one_row_among_fifty_thousand() {
    let server = TestS3::anonymous().await;
    server.put_parquet("catalog/part0.parquet");

    let last = FIXTURE_ROWS - 1;
    let result = lookup(
        &server.url("catalog/part0.parquet", ""),
        &permissive_policy(),
        "objectid",
        &last.to_string(),
        Some(&["objectid".to_owned(), "objra".to_owned()]),
    )
    .await
    .expect("the lookup should succeed");

    assert_eq!(row_count(&result), 1);
    // The projection, not the file's schema.
    assert_eq!(result.schema.fields().len(), 2);
}

/// A HATS partition key has `=` in it. It survives url parsing (a unit test covers
/// that) but it also has to survive signing and being put in a request path, which
/// only a real server can confirm — a mis-escaped key is a signature mismatch or a
/// 404, not a parse error.
#[tokio::test]
async fn reads_a_hats_partition_key() {
    let server = TestS3::anonymous().await;
    let key = "dataset/Norder=5/Dir=0/Npix=12240/part0.parquet";
    server.put_parquet(key);

    let result = lookup(
        &server.url(key, ""),
        &permissive_policy(),
        "objectid",
        "7",
        None,
    )
    .await
    .expect("a key with = in it should read");
    assert_eq!(row_count(&result), 1);
}

/// The same, signed: escaping a key wrongly breaks SigV4 in a way anonymous reads
/// never show, because there is no canonical request to get wrong.
#[tokio::test]
async fn reads_a_hats_partition_key_with_credentials() {
    let server = TestS3::authenticated().await;
    let key = "dataset/Norder=5/Dir=0/Npix=12240/part0.parquet";
    server.put_parquet(key);

    let result = lookup(
        &server.url(key, &server.credentialed_options()),
        &permissive_policy(),
        "objectid",
        "7",
        None,
    )
    .await
    .expect("a signed read of a key with = in it should work");
    assert_eq!(row_count(&result), 1);
}

/// The rule end to end: credentials come from the request. The right ones work.
#[tokio::test]
async fn credentials_from_the_url_open_a_private_bucket() {
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let result = lookup(
        &server.url("private/part0.parquet", &server.credentialed_options()),
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await
    .expect("the right credentials should open it");
    assert_eq!(row_count(&result), 1);
}

/// And the wrong ones do not. This is the test that would catch the service falling
/// back to an ambient identity: if it ever signed with something other than what the
/// request carried, a bad secret would stop mattering.
#[tokio::test]
async fn the_wrong_credentials_are_refused() {
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let options = format!(
        "access_key_id={ACCESS_KEY_ID}&secret_access_key=not-the-right-secret&allow_http=true"
    );
    let error = common::expect_error(
        lookup(
            &server.url("private/part0.parquet", &options),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "a bad secret must not read the object",
    );
    assert!(
        !error.to_string().contains("not-the-right-secret"),
        "leaked the secret: {error}"
    );
}

/// A server that requires signing, asked anonymously. The caller sent no credentials,
/// so the request is unsigned and the origin refuses it — rather than the service
/// quietly signing with an identity of its own.
#[tokio::test]
async fn an_anonymous_request_cannot_read_a_private_bucket() {
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let error = common::expect_error(
        lookup(
            &server.url("private/part0.parquet", ""),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "an unsigned request must not read a private object",
    );
    // Whatever it is, it is not a successful read.
    assert!(!matches!(error, ApiError::NotFound(_)), "{error}");
}

/// A missing object is the caller's 404, not a 500 and not a policy refusal.
#[tokio::test]
async fn a_missing_object_is_not_found() {
    let server = TestS3::anonymous().await;
    server.put_parquet("catalog/part0.parquet");

    let error = common::expect_error(
        lookup(
            &server.url("catalog/absent.parquet", ""),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "a missing object should fail",
    );
    assert!(!matches!(error, ApiError::Forbidden(_)), "{error}");
}

/// An object that is not parquet is a bad request about the file, not a crash.
#[tokio::test]
async fn an_object_that_is_not_parquet_is_rejected() {
    let server = TestS3::anonymous().await;
    server.put_bytes("catalog/not-parquet.parquet", b"this is not a parquet file");

    let error = common::expect_error(
        lookup(
            &server.url("catalog/not-parquet.parquet", ""),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "a non-parquet object should fail",
    );
    assert!(!matches!(error, ApiError::Forbidden(_)), "{error}");
}

/// The policy is consulted before a connection is made, so an endpoint that is not on
/// the list is refused even though the server behind it is running and would answer.
#[tokio::test]
async fn an_endpoint_off_the_policy_is_refused_though_it_would_answer() {
    let server = TestS3::anonymous().await;
    server.put_parquet("catalog/part0.parquet");

    // It works when the policy names this endpoint...
    let allowed = common::policy_for_endpoints(&[&server.endpoint]);
    assert!(
        lookup(
            &server.url("catalog/part0.parquet", ""),
            &allowed,
            "objectid",
            "1",
            None,
        )
        .await
        .is_ok()
    );

    // ...and not when it names another one, though nothing else changed.
    let elsewhere = common::policy_for_endpoints(&["https://minio.example.com"]);
    let error = common::expect_error(
        lookup(
            &server.url("catalog/part0.parquet", ""),
            &elsewhere,
            "objectid",
            "1",
            None,
        )
        .await,
        "an unlisted endpoint must be refused",
    );
    assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
}

/// Cleartext with credentials needs saying so, and the refusal happens before the
/// request rather than after it succeeded.
#[tokio::test]
async fn credentials_over_cleartext_need_allow_http() {
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let without_flag = format!(
        "access_key_id={ACCESS_KEY_ID}&secret_access_key={}",
        common::SECRET_ACCESS_KEY
    );
    let error = common::expect_error(
        lookup(
            &server.url("private/part0.parquet", &without_flag),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "cleartext credentials need allow_http",
    );
    assert!(error.to_string().contains("cleartext"), "{error}");
    assert!(
        !error.to_string().contains(common::SECRET_ACCESS_KEY),
        "leaked: {error}"
    );
}

/// Two servers at once, each with its own credentials: nothing is cached across
/// requests, so one caller's credentials cannot open another's bucket. The service is
/// stateless per request, and any cache added later has to key on the credential for
/// the same reason — both rest on this.
#[tokio::test]
async fn two_servers_do_not_share_credentials() {
    let public = TestS3::anonymous().await;
    let private = TestS3::authenticated().await;
    public.put_parquet("catalog/part0.parquet");
    private.put_parquet("catalog/part0.parquet");
    let policy = permissive_policy();

    // Read the private one first, so any leak would be there to find.
    assert!(
        lookup(
            &private.url("catalog/part0.parquet", &private.credentialed_options()),
            &policy,
            "objectid",
            "1",
            None,
        )
        .await
        .is_ok()
    );
    // The public one, anonymously, still works.
    assert!(
        lookup(
            &public.url("catalog/part0.parquet", ""),
            &policy,
            "objectid",
            "1",
            None,
        )
        .await
        .is_ok()
    );
    // And the private one anonymously still does not.
    assert!(
        lookup(
            &private.url("catalog/part0.parquet", ""),
            &policy,
            "objectid",
            "1",
            None,
        )
        .await
        .is_err()
    );
}
