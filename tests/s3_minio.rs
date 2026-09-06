//! S3 against a real MinIO.
//!
//! `s3_local.rs` covers the protocol against an in-process server, which is enough for
//! everything that is our own logic. What it cannot cover is a real implementation's
//! own behaviour — its error bodies, its `ETag` and `Content-Range` spellings, its
//! keep-alive and chunking — and MinIO is what the deployments this serves actually
//! run.
//!
//! The fixture is written here rather than uploaded by CI, through OpenDAL, so the file
//! read back is the same one `tests/common` defines and there is no upload step to
//! drift from it. Writing is the test's business only: the service itself never writes,
//! and this uses its own OpenDAL operator to do it rather than anything under `src/`.
//!
//! CI starts the MinIO and creates the bucket; these run when
//! `HATS_API_TEST_MINIO_ENDPOINT` names one, and skip otherwise.

mod common;

use common::{expect_error, lookup, parquet_fixture, permissive_policy, row_count, skip_or_fail};
use hats_api::error::ApiError;
use hats_api::storage::StorageOptions;
use opendal::{HttpTransporter, OperationContext, Operator, services};

/// Where the MinIO under test is, and how to write to it.
struct Minio {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
}

impl Minio {
    /// `None` when no MinIO was configured, so a plain `cargo test` does not try to
    /// reach one.
    fn from_env() -> Option<Self> {
        let var = |name: &str| {
            std::env::var(format!("HATS_API_TEST_MINIO_{name}"))
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let endpoint = var("ENDPOINT")?;
        Some(Self {
            endpoint,
            access_key: var("ACCESS_KEY").unwrap_or_else(|| "minioadmin".to_owned()),
            secret_key: var("SECRET_KEY").unwrap_or_else(|| "minioadmin".to_owned()),
            bucket: var("BUCKET").unwrap_or_else(|| "hats-test".to_owned()),
        })
    }

    /// An operator for putting fixtures in place. The service has no write path, so
    /// this is the test's own client and deliberately separate from `src/storage.rs`.
    ///
    /// It carries its own transport for the same reason the service's does: nothing
    /// installs a process-wide default, so an operator built without one fails rather
    /// than quietly finding a client that answers to no policy. Leaving that fallback
    /// switched off is what makes this the one binary where a store reaching a real
    /// server proves the service attached its own.
    #[expect(
        clippy::disallowed_methods,
        reason = "the fixture writer is not a request path; it talks to the MinIO this \
                  test was given, and the policy it is proving things about is the one \
                  the service builds on the read side"
    )]
    fn writer(&self) -> Operator {
        let builder = services::S3::default()
            .bucket(&self.bucket)
            .region("us-east-1")
            .endpoint(&self.endpoint)
            .access_key_id(&self.access_key)
            .secret_access_key(&self.secret_key)
            .disable_config_load()
            .disable_ec2_metadata();
        Operator::new(builder)
            .expect("an operator for the test MinIO")
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    opendal_http_transport_reqwest::ReqwestTransport::default(),
                )),
            )
    }

    /// Put the fixture at `key` and return the url a caller would send to read it.
    async fn put_fixture(&self, key: &str) -> String {
        self.writer()
            .write(key, parquet_fixture())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "could not write {key} to the test bucket {}; does it exist? {error}",
                    self.bucket
                )
            });
        self.url(key)
    }

    fn url(&self, key: &str) -> String {
        format!("s3://{}/{key}", self.bucket)
    }

    /// Enough to find this server, and nothing that would sign a request.
    fn options(&self) -> StorageOptions {
        StorageOptions {
            endpoint: Some(self.endpoint.clone()),
            ..Default::default()
        }
    }

    fn credentialed_options(&self) -> StorageOptions {
        StorageOptions {
            access_key_id: Some(self.access_key.clone().into()),
            secret_access_key: Some(self.secret_key.clone().into()),
            allow_http: true,
            ..self.options()
        }
    }
}

/// The whole path against a real MinIO: write a partition, then read one row out of it
/// the way a request does. Several row groups, so it is a sequence of ranged reads.
#[tokio::test]
async fn reads_a_partition_from_minio() {
    let Some(minio) = Minio::from_env() else {
        return skip_or_fail("HATS_API_TEST_MINIO");
    };
    let url = minio.put_fixture("catalog/part0.parquet").await;

    let result = lookup(
        &url,
        &minio.credentialed_options(),
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

/// A HATS key, signed against a real implementation. Key escaping and SigV4 canonical
/// requests are exactly where two S3 implementations can disagree.
#[tokio::test]
async fn reads_a_hats_partition_key_from_minio() {
    let Some(minio) = Minio::from_env() else {
        return skip_or_fail("HATS_API_TEST_MINIO");
    };
    let url = minio
        .put_fixture("dataset/Norder=5/Dir=0/Npix=12240/part0.parquet")
        .await;

    let result = lookup(
        &url,
        &minio.credentialed_options(),
        &permissive_policy(),
        "objectid",
        "7",
        None,
    )
    .await
    .expect("a signed read of a key with = in it should work");
    assert_eq!(row_count(&result), 1);
}

#[tokio::test]
async fn honours_a_projection_against_minio() {
    let Some(minio) = Minio::from_env() else {
        return skip_or_fail("HATS_API_TEST_MINIO");
    };
    let url = minio.put_fixture("catalog/projected.parquet").await;

    let columns = ["objectid".to_owned(), "objra".to_owned()];
    let result = lookup(
        &url,
        &minio.credentialed_options(),
        &permissive_policy(),
        "objectid",
        "9",
        Some(&columns),
    )
    .await
    .expect("the projected lookup should succeed");
    assert_eq!(result.schema.fields().len(), 2);
    assert_eq!(row_count(&result), 1);
}

/// A MinIO bucket is private by default, so the same object without credentials is
/// refused — the service sends an unsigned request rather than an identity of its own.
#[tokio::test]
async fn an_anonymous_request_cannot_read_a_private_minio_bucket() {
    let Some(minio) = Minio::from_env() else {
        return skip_or_fail("HATS_API_TEST_MINIO");
    };
    minio.put_fixture("catalog/private.parquet").await;

    let error = expect_error(
        lookup(
            &minio.url("catalog/private.parquet"),
            &minio.options(),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "an unsigned request must not read a private bucket",
    );
    assert!(!matches!(error, ApiError::Forbidden(_)), "{error}");
}

/// A missing object against a real MinIO, whose 404 body differs from the in-process
/// server's. It must not surface as this service's own refusal.
#[tokio::test]
async fn a_missing_object_in_minio_is_not_a_policy_refusal() {
    let Some(minio) = Minio::from_env() else {
        return skip_or_fail("HATS_API_TEST_MINIO");
    };

    let error = expect_error(
        lookup(
            &minio.url("catalog/definitely-absent.parquet"),
            &minio.credentialed_options(),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "a missing object should fail",
    );
    assert!(!matches!(error, ApiError::Forbidden(_)), "{error}");
    assert!(
        !error.to_string().contains(&minio.secret_key),
        "leaked the secret: {error}"
    );
}
