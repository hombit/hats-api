//! Shared fixtures for the S3 integration tests.
//!
//! The unit tests in `src/storage.rs` stop at the request head, which is enough to see
//! whether something was signed but says nothing about whether a parquet file can
//! actually be read over S3. These drive the real path — `storage::open` into
//! `query::run` — against a real S3 protocol implementation, so that ranged reads,
//! footer fetches, path-style addressing and key escaping are exercised rather than
//! assumed.

#![allow(dead_code)] // Each test binary uses a different part of this.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use hats_api::access::AccessPolicy;
use hats_api::config::{AccessConfig, EndpointConfig, HttpConfig, LimitsConfig, NetworkConfig};
use hats_api::error::ApiError;
use hats_api::materialize::Transfers;
use hats_api::mount::Mounts;
use hats_api::query::{Predicate, Projection, QueryResult, Selection};
use hats_api::storage::{self, StorageOptions};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::FileSystem;
use tempfile::TempDir;

pub const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
pub const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

/// Rows in the fixture file. Enough, with the row-group size below, to make a read a
/// sequence of ranged requests rather than one whole-object fetch — which is the thing
/// these tests exist to check.
pub const FIXTURE_ROWS: i64 = 50_000;
const ROW_GROUP_SIZE: usize = 5_000;

/// An S3 server on the loopback interface, backed by a temporary directory.
///
/// A bucket is a directory under the root and an object is a file in it, so a fixture
/// is written with `std::fs` and served over the real protocol — no PUT path, no
/// client SDK, and nothing to go wrong between writing the file and reading it back.
pub struct TestS3 {
    pub endpoint: String,
    pub bucket: String,
    root: TempDir,
}

impl TestS3 {
    /// A server that serves anyone, which is how the public astronomy buckets behave.
    pub async fn anonymous() -> Self {
        Self::start(None).await
    }

    /// A server that requires SigV4 with [`ACCESS_KEY_ID`] and [`SECRET_ACCESS_KEY`].
    pub async fn authenticated() -> Self {
        Self::start(Some((ACCESS_KEY_ID, SECRET_ACCESS_KEY))).await
    }

    async fn start(credentials: Option<(&str, &str)>) -> Self {
        let root = TempDir::new().expect("temp dir");
        let bucket = "hats-test".to_owned();
        std::fs::create_dir_all(root.path().join(&bucket)).expect("bucket dir");

        let fs = FileSystem::new(root.path()).expect("s3s-fs root");
        let mut builder = S3ServiceBuilder::new(fs);
        if let Some((access_key_id, secret_access_key)) = credentials {
            builder.set_auth(SimpleAuth::from_single(access_key_id, secret_access_key));
        }
        // `S3Service` is itself a hyper service and clones cheaply, one per connection.
        let service = builder.build();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("local addr").port();

        // Runs for the lifetime of the test process. There is nothing to clean up
        // that the temp dir does not already own.
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let service = service.clone();
                tokio::spawn(async move {
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        Self {
            endpoint: format!("http://127.0.0.1:{port}"),
            bucket,
            root,
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.root.path().join(&self.bucket).join(key)
    }

    /// Put a parquet fixture at `key` and return the `s3://` url naming it.
    pub fn put_parquet(&self, key: &str) -> String {
        let path = self.object_path(key);
        std::fs::create_dir_all(path.parent().expect("a key has a parent")).expect("key dirs");
        std::fs::write(&path, parquet_fixture()).expect("write fixture");
        format!("s3://{}/{key}", self.bucket)
    }

    /// Put arbitrary bytes, for the cases that are about the object not being a
    /// readable parquet file.
    pub fn put_bytes(&self, key: &str, bytes: &[u8]) -> String {
        let path = self.object_path(key);
        std::fs::create_dir_all(path.parent().expect("a key has a parent")).expect("key dirs");
        std::fs::write(&path, bytes).expect("write bytes");
        format!("s3://{}/{key}", self.bucket)
    }

    /// The url naming an object here. Just the object: how to reach the server is
    /// [`Self::options`], sent beside it.
    pub fn url(&self, key: &str) -> String {
        format!("s3://{}/{key}", self.bucket)
    }

    /// Enough to find this server, and nothing that would sign a request.
    pub fn options(&self) -> StorageOptions {
        StorageOptions {
            endpoint: Some(self.endpoint.clone()),
            ..Default::default()
        }
    }

    /// The same, with the credentials [`Self::authenticated`] expects. `allow_http`
    /// because the test server has no certificate.
    pub fn credentialed_options(&self) -> StorageOptions {
        StorageOptions {
            access_key_id: Some(ACCESS_KEY_ID.to_owned().into()),
            secret_access_key: Some(SECRET_ACCESS_KEY.to_owned().into()),
            allow_http: true,
            ..self.options()
        }
    }
}

/// A parquet file shaped like a HATS partition: an id to look up, a position, and a
/// band. Written in several row groups so that reading it takes several ranged GETs.
pub fn parquet_fixture() -> Vec<u8> {
    let objectid: ArrayRef = Arc::new(Int64Array::from_iter_values(0..FIXTURE_ROWS));
    let objra: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..FIXTURE_ROWS).map(|i| 320.0 + (i as f64) * 1e-6),
    ));
    let objdec: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..FIXTURE_ROWS).map(|i| -12.0 - (i as f64) * 1e-6),
    ));
    let band: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..FIXTURE_ROWS).map(|i| if i % 2 == 0 { "g" } else { "r" }),
    ));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("objectid", objectid, false),
        ("objra", objra, true),
        ("objdec", objdec, true),
        ("band", band, true),
    ])
    .expect("fixture batch");

    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .build();
    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).expect("writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close");
    buffer
}

/// Network rules that reach the loopback interface, which is where every test server
/// here listens.
pub fn loopback() -> NetworkConfig {
    NetworkConfig {
        allow_loopback: true,
        ..Default::default()
    }
}

/// A policy that will talk to anything, including the loopback interface — the tests
/// here are about storage, not about the policy, which has its own tests.
pub fn permissive_policy() -> AccessPolicy {
    AccessPolicy::new(
        &AccessConfig {
            network: loopback(),
            // Cleartext too, since every test server here is a plain http one on
            // loopback.
            http: HttpConfig {
                endpoints: None,
                allow_plain_http: true,
            },
            ..Default::default()
        },
        &Mounts::default(),
    )
    .expect("permissive policy")
}

/// A policy restricted to exactly these endpoints, for the tests that check a refusal.
pub fn policy_for_endpoints(endpoints: &[&str]) -> AccessPolicy {
    AccessPolicy::new(
        &AccessConfig {
            network: loopback(),
            s3: EndpointConfig {
                endpoints: Some(endpoints.iter().map(|e| (*e).to_owned()).collect()),
            },
            ..Default::default()
        },
        &Mounts::default(),
    )
    .expect("endpoint policy")
}

/// The scratch budget, at its defaults. Only the http backend consults it, and only for
/// a server that will not serve byte ranges, so every test here gets the same one.
pub fn transfers() -> Arc<Transfers> {
    Arc::new(Transfers::new(&LimitsConfig::default()))
}

/// Open a url and run one point lookup through it: the whole path a request takes.
///
/// The column and the value are separate because that is how a target is configured —
/// two environment variables — rather than because the service takes them that way; the
/// predicate is assembled here, the way a caller would write it.
pub async fn lookup(
    raw_url: &str,
    options: &StorageOptions,
    policy: &AccessPolicy,
    filter_column: &str,
    filter_value: &str,
    columns: Option<&[String]>,
) -> Result<QueryResult, ApiError> {
    let select = columns.map(|columns| columns.join(", "));
    let predicate = format!("{filter_column} = {}", sql_literal(filter_value));
    query(
        raw_url,
        options,
        policy,
        &Selection {
            projection: match select.as_deref() {
                Some(list) => Projection::Select(list),
                None => Projection::All,
            },
            predicate: Predicate::Where(&predicate),
            limit: None,
        },
    )
    .await
}

/// A value as SQL. A number is written as one so that it compares against a numeric
/// column; anything else is a quoted string.
fn sql_literal(value: &str) -> String {
    match value.parse::<f64>() {
        Ok(_) => value.to_owned(),
        Err(_) => format!("'{}'", value.replace('\'', "''")),
    }
}

/// Open a url and run an arbitrary selection through it.
pub async fn query(
    raw_url: &str,
    options: &StorageOptions,
    policy: &AccessPolicy,
    selection: &Selection<'_>,
) -> Result<QueryResult, ApiError> {
    let url = storage::parse_url(raw_url)?;
    let file = storage::open(&url, options, policy, &transfers())?;
    // These read through a url the way the API does, so they take the API's order.
    hats_api::query::run(
        &file,
        selection,
        (&LimitsConfig::default()).into(),
        hats_api::query::Order::Unspecified,
    )
    .await
}

pub fn row_count(result: &QueryResult) -> usize {
    result.batches.iter().map(RecordBatch::num_rows).sum()
}

/// `QueryResult` holds arrow batches and is deliberately not `Debug`, so `expect_err`
/// cannot be used on one. This says the same thing without printing the rows.
pub fn expect_error(result: Result<QueryResult, ApiError>, context: &str) -> ApiError {
    match result {
        Ok(ok) => panic!("{context}: expected an error, got {} rows", row_count(&ok)),
        Err(error) => error,
    }
}

/// A real deployment to read from: a public AWS bucket, the SNAD MinIO, or anything
/// else with the same shape. Configured from the environment, so the same tests serve
/// all of them and CI holds the credentials.
///
/// The url names the object and nothing else; everything storage-specific — endpoint,
/// region, credentials — is in `options`, exactly as a caller would send it. That keeps
/// the test from having a credential path of its own.
pub struct RemoteTarget {
    pub name: &'static str,
    pub url: String,
    pub options: StorageOptions,
    /// A lookup known to match in this file, and how many rows it should return.
    pub column: String,
    pub value: String,
    pub expected_rows: Option<usize>,
}

/// A target's built-in configuration, used when the environment names no other.
#[derive(Clone, Copy)]
pub struct Defaults {
    pub url: &'static str,
    /// `None` is AWS itself.
    pub endpoint: Option<&'static str>,
    pub column: &'static str,
    pub value: &'static str,
    pub expected_rows: Option<usize>,
}

impl RemoteTarget {
    /// `None` when this target was not asked for, so a laptop running `cargo test`
    /// does not start reaching across the network.
    ///
    /// A target runs when `{prefix}_URL` names one, or when `{prefix}=1` opts into the
    /// built-in one. `{prefix}_COLUMN`, `{prefix}_VALUE` and `{prefix}_EXPECTED_ROWS`
    /// override the rest.
    pub fn from_env(prefix: &'static str, defaults: Option<Defaults>) -> Option<Self> {
        let var = |suffix: &str| {
            std::env::var(format!("{prefix}_{suffix}"))
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let opted_in = std::env::var(prefix).is_ok_and(|value| value == "1");

        let url = match (var("URL"), opted_in.then_some(defaults).flatten()) {
            (Some(url), _) => url,
            (None, Some(defaults)) => defaults.url.to_owned(),
            (None, None) => return None,
        };
        let defaults = defaults.unwrap_or(Defaults {
            url: "",
            endpoint: None,
            column: "",
            value: "",
            expected_rows: None,
        });

        let column = var("COLUMN").unwrap_or_else(|| defaults.column.to_owned());
        let value = var("VALUE").unwrap_or_else(|| defaults.value.to_owned());
        assert!(
            !column.is_empty() && !value.is_empty(),
            "{prefix}_URL is set, so {prefix}_COLUMN and {prefix}_VALUE must be too"
        );
        let expected_rows = match var("EXPECTED_ROWS") {
            Some(raw) => Some(
                raw.parse()
                    .unwrap_or_else(|_| panic!("{prefix}_EXPECTED_ROWS must be a number")),
            ),
            None => defaults.expected_rows,
        };

        let options = StorageOptions {
            endpoint: var("ENDPOINT").or_else(|| defaults.endpoint.map(ToOwned::to_owned)),
            region: var("REGION"),
            access_key_id: var("ACCESS_KEY_ID").map(Into::into),
            secret_access_key: var("SECRET_ACCESS_KEY").map(Into::into),
            ..Default::default()
        };

        Some(Self {
            name: prefix,
            url,
            options,
            column,
            value,
            expected_rows,
        })
    }

    /// The url with its object key replaced, for the tests about absence. The options
    /// are unchanged, so the answer is about the object and not the server.
    pub fn sibling_url(&self, name: &str) -> String {
        let url = self.url.as_str();
        let parent = url.rsplit_once('/').map_or(url, |(parent, _)| parent);
        format!("{parent}/{name}")
    }
}

/// Skipping a test because its target is not configured is right on a laptop and wrong
/// in the CI job whose whole purpose is that target. `HATS_API_TEST_REQUIRE_REMOTE`
/// turns "not configured" into a failure, so a workflow that lost its variables fails
/// loudly rather than passing without having tested anything.
pub fn skip_or_fail(prefix: &str) {
    assert!(
        std::env::var("HATS_API_TEST_REQUIRE_REMOTE").is_err(),
        "{prefix} is not configured, but HATS_API_TEST_REQUIRE_REMOTE says it must be"
    );
    eprintln!("skipping {prefix}: not configured");
}

/// A one-shot HTTP server on the loopback interface: it takes one request, hands the
/// head back, and answers 404. Whether a request was signed — and with what — is not
/// visible on the builder, only on the wire.
pub fn capture_one_request() -> (u16, std::sync::mpsc::Receiver<String>) {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte) {
                Ok(1) => head.push(byte[0]),
                _ => break,
            }
        }
        // 404 rather than a hang: a retry would find nothing listening.
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
        let _ = sender.send(String::from_utf8_lossy(&head).into_owned());
    });
    (port, receiver)
}

/// A local directory that looks like the fixture, for comparing a remote read against
/// a local one.
pub fn write_local_fixture(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, parquet_fixture()).expect("local fixture");
    path
}
