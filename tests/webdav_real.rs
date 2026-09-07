//! WebDAV against a real server.
//!
//! `webdav.rs` covers the protocol against an in-process server, which is enough for
//! everything that is our own logic. What it cannot cover is a real implementation's
//! own behaviour, and for WebDAV that is a larger gap than for most backends: the
//! object's size arrives as an XML `PROPFIND` body, and servers differ in their
//! namespace prefixes, their `href` spelling and which properties they return at all. A
//! hand-written `207` proves that OpenDAL parses *our* XML.
//!
//! CI runs `rclone serve webdav` from a pinned image, so what these tests are held
//! against is a server someone chose rather than whatever the runner's distribution
//! packaged. Any other server can be pointed at instead through the same variables.
//!
//! The fixture is written here rather than uploaded by CI, through OpenDAL, so the file
//! read back is the same one `tests/common` defines and there is no upload step to
//! drift from it. Writing is the test's business only: the service itself never writes.
//!
//! These run when `HATS_API_TEST_WEBDAV_ENDPOINT` names a server, and skip otherwise.

mod common;

use common::{expect_error, lookup, parquet_fixture, row_count, skip_or_fail};
use hats_api::access::AccessPolicy;
use hats_api::config::{AccessConfig, EndpointConfig, NetworkConfig};
use hats_api::storage::StorageOptions;
use opendal::{HttpTransporter, OperationContext, Operator, services};
use std::sync::LazyLock;
use tokio::sync::Mutex;

/// Fixture writes go one at a time, process-wide.
///
/// The harness runs these tests concurrently and each one writes to the same server
/// before reading, so without this it is several writers mutating one tree — and a
/// write is three requests, since OpenDAL creates the parent by statting it and sending
/// `MKCOL` when the stat says it is absent. How a server interleaves that is its own
/// business, and the ways it can go wrong are not all statuses: a concurrent write can
/// be answered by closing the connection with the body already sent, which arrives here
/// as a transport error rather than as a response. Whether the server serializes writes
/// well is not what these tests are for — the service under test has no write path at
/// all — so the writes are serialized here instead, which at five files costs nothing.
static FIXTURE_WRITES: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Where the WebDAV server under test is, and how to write to it.
struct Webdav {
    /// The transport url — `http://host:port` — which is both what the fixture writer
    /// talks to and what an operator names in `access.webdav.endpoints`.
    endpoint: String,
    username: String,
    password: String,
}

impl Webdav {
    /// `None` when no server was configured, so a plain `cargo test` does not try to
    /// reach one.
    fn from_env() -> Option<Self> {
        let var = |name: &str| {
            std::env::var(format!("HATS_API_TEST_WEBDAV_{name}"))
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Some(Self {
            endpoint: var("ENDPOINT")?,
            username: var("USERNAME").unwrap_or_else(|| "hats".to_owned()),
            password: var("PASSWORD").unwrap_or_else(|| "hats-test".to_owned()),
        })
    }

    /// An operator for putting fixtures in place. The service has no write path, so this
    /// is the test's own client and deliberately separate from `src/storage.rs`.
    ///
    /// It carries its own transport for the same reason the service's does: nothing
    /// installs a process-wide default, so an operator built without one fails rather
    /// than quietly finding a client that answers to no policy.
    #[expect(
        clippy::disallowed_methods,
        reason = "the fixture writer is not a request path; it talks to the WebDAV \
                  server this test was given, and the policy it is proving things about \
                  is the one the service builds on the read side"
    )]
    fn writer(&self) -> Operator {
        let builder = services::Webdav::default()
            .endpoint(&self.endpoint)
            .username(&self.username)
            .password(&self.password);
        Operator::new(builder)
            .expect("an operator for the test WebDAV server")
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    opendal_http_transport_reqwest::ReqwestTransport::default(),
                )),
            )
    }

    /// Put the fixture at `key` and return the url a caller would send to read it.
    ///
    /// Every test here writes under a directory of its own, so no test's tree is any
    /// other's. That matters most for the missing-object test, which is only asking a
    /// real question if the directory it reads from is one it put there itself.
    async fn put_fixture(&self, key: &str) -> String {
        let _serialized = FIXTURE_WRITES.lock().await;
        self.writer()
            .write(key, parquet_fixture())
            .await
            .unwrap_or_else(|error| panic!("could not write {key} to the test server: {error}"));
        self.url(key)
    }

    /// The `webdav://` url for a key: the server's authority, with the scheme naming the
    /// protocol rather than the transport.
    fn url(&self, key: &str) -> String {
        let authority = self
            .endpoint
            .split_once("://")
            .map_or(self.endpoint.as_str(), |(_, rest)| rest)
            .trim_end_matches('/');
        format!("webdav://{authority}/{key}")
    }

    /// Cleartext WebDAV is reachable only by being named — there is no `allow_plain_http`
    /// for this backend — so the policy every test here uses names this one server.
    fn policy(&self) -> AccessPolicy {
        AccessPolicy::new(
            &AccessConfig {
                network: NetworkConfig {
                    allow_loopback: true,
                    ..Default::default()
                },
                webdav: EndpointConfig {
                    endpoints: Some(vec![self.endpoint.trim_end_matches('/').to_owned()]),
                },
                ..Default::default()
            },
            &hats_api::mount::Mounts::default(),
        )
        .expect("the policy should build")
    }

    /// Enough to find this server, and nothing that would authenticate a request.
    fn options(&self) -> StorageOptions {
        StorageOptions {
            transport: Some(hats_api::storage::WebdavTransport::Http),
            ..Default::default()
        }
    }

    fn credentialed_options(&self) -> StorageOptions {
        StorageOptions {
            username: Some(self.username.clone().into()),
            password: Some(self.password.clone().into()),
            // The caller's half of the cleartext decision, which Basic over http needs.
            allow_http: true,
            ..self.options()
        }
    }
}

/// The whole path against a real server: write a partition, then read one row out of it
/// the way a request does. Several row groups, so it is a sequence of ranged reads — and
/// the size that bounds them comes from the server's own `PROPFIND` body.
#[tokio::test]
async fn reads_a_partition_over_real_webdav() {
    let Some(server) = Webdav::from_env() else {
        return skip_or_fail("HATS_API_TEST_WEBDAV");
    };
    let url = server.put_fixture("partition/part0.parquet").await;

    let result = lookup(
        &url,
        &server.credentialed_options(),
        &server.policy(),
        "objectid",
        "42",
        None,
    )
    .await
    .expect("the lookup should succeed");
    assert_eq!(row_count(&result), 1);
    assert_eq!(result.schema.fields().len(), 4);
}

/// A HATS key, whose `=` and `/` go through the server's own path handling. Percent
/// encoding in a `PROPFIND` `href` is exactly where two implementations disagree.
#[tokio::test]
async fn reads_a_hats_partition_key_over_real_webdav() {
    let Some(server) = Webdav::from_env() else {
        return skip_or_fail("HATS_API_TEST_WEBDAV");
    };
    let url = server
        .put_fixture("dataset/Norder=5/Dir=0/Npix=12240/part0.parquet")
        .await;

    let result = lookup(
        &url,
        &server.credentialed_options(),
        &server.policy(),
        "objectid",
        "7",
        None,
    )
    .await
    .expect("a key with = in it should be readable");
    assert_eq!(row_count(&result), 1);
}

#[tokio::test]
async fn honours_a_projection_over_real_webdav() {
    let Some(server) = Webdav::from_env() else {
        return skip_or_fail("HATS_API_TEST_WEBDAV");
    };
    let url = server.put_fixture("projection/part0.parquet").await;

    let columns = ["objectid".to_owned(), "objra".to_owned()];
    let result = lookup(
        &url,
        &server.credentialed_options(),
        &server.policy(),
        "objectid",
        "9",
        Some(&columns),
    )
    .await
    .expect("the projected lookup should succeed");
    assert_eq!(result.schema.fields().len(), 2);
    assert_eq!(row_count(&result), 1);
}

/// The server requires Basic, so the same object without credentials is refused — the
/// service sends an unauthenticated request rather than an identity of its own.
#[tokio::test]
async fn an_anonymous_request_cannot_read_a_protected_webdav_server() {
    let Some(server) = Webdav::from_env() else {
        return skip_or_fail("HATS_API_TEST_WEBDAV");
    };
    server.put_fixture("anonymous/private.parquet").await;

    let error = expect_error(
        lookup(
            &server.url("anonymous/private.parquet"),
            &server.options(),
            &server.policy(),
            "objectid",
            "42",
            None,
        )
        .await,
        "an anonymous read of a protected server",
    );
    assert!(
        !error.to_string().contains(&server.password),
        "the password must not be echoed: {error}"
    );
}

/// A missing object fails as a missing object rather than as a refusal: a caller who is
/// allowed here and asked for something absent must not be told they were not allowed.
/// The same assertion the S3 backends make, for the same reason.
#[tokio::test]
async fn a_missing_object_over_real_webdav_is_not_a_policy_refusal() {
    let Some(server) = Webdav::from_env() else {
        return skip_or_fail("HATS_API_TEST_WEBDAV");
    };
    // A sibling, so the directory exists and the object alone is missing — otherwise
    // this asks about a tree that was never there. It also makes the test say something
    // when the server is unreachable: a refused connection is neither a `Forbidden` nor
    // an echoed password, so both assertions below would hold against nothing at all.
    server.put_fixture("missing/present.parquet").await;

    let error = expect_error(
        lookup(
            &server.url("missing/absent.parquet"),
            &server.credentialed_options(),
            &server.policy(),
            "objectid",
            "42",
            None,
        )
        .await,
        "a missing object",
    );
    assert!(
        !matches!(error, hats_api::error::ApiError::Forbidden(_)),
        "{error}"
    );
    assert!(
        !error.to_string().contains(&server.password),
        "the password must not be echoed: {error}"
    );
}
