//! No credential in a log line, including at `debug` and `trace`.
//!
//! Its own test binary, because it installs a process-wide tracing subscriber at
//! `TRACE` and reads everything back. Every layer that could log is in the path —
//! `tower_http`'s request span, the service's own spans and errors, OpenDAL, reqwest,
//! DataFusion — and the assertion is the same each time: the secret is not in the
//! buffer, in any form.
//!
//! `TRACE` matters. A leak at `info` would be found by reading the code; a leak at
//! `trace` inside a dependency would not, and `RUST_LOG=trace` is the first thing
//! anyone does when a request misbehaves in production.

mod common;

use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use common::{
    ACCESS_KEY_ID, SECRET_ACCESS_KEY, TestS3, capture_one_request, lookup, permissive_policy,
    transfers,
};
use hats_api::access::AccessPolicy;
use hats_api::app;
use hats_api::logging;
use hats_api::storage::StorageOptions;
use http_body_util::BodyExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

/// Everything the process logged, kept in memory.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer")).into_owned()
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// One subscriber for the binary; the tests share the buffer and each asserts over all
/// of it, so a leak in any of them fails whichever runs the assertion.
fn logs() -> CapturedLogs {
    static LOGS: OnceLock<CapturedLogs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let captured = CapturedLogs::default();
        // `trace` for everything, through the filter the binary actually installs —
        // so this asserts the guarantee the service ships, not a laboratory one.
        //
        // `s3s` is the exception, and it is the test server rather than anything under
        // test: it logs the request headers it receives, `authorization` and
        // `x-amz-security-token` included. That is the fixture seeing the credential
        // it was sent, which is the point of sending it. It is a dev-dependency, so no
        // deployment of this service contains it.
        let filter = logging::silence_credential_loggers(EnvFilter::new("trace"))
            .add_directive("s3s=off".parse().expect("a valid directive"));
        tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_env_filter(filter)
            .with_ansi(false)
            .with_target(true)
            .init();
        captured
    })
    .clone()
}

/// The secret, and the things a leak could turn it into before reaching a log: the
/// url-encoded spelling, and the `/` that a query string often escapes.
fn assert_no_secret(logs: &str, context: &str) {
    for form in [
        SECRET_ACCESS_KEY.to_owned(),
        SECRET_ACCESS_KEY.replace('/', "%2F"),
        SECRET_ACCESS_KEY.replace('/', "%2f"),
    ] {
        assert!(
            !logs.contains(&form),
            "{context}: the secret reached the logs as {form:?}\n--- logs ---\n{logs}"
        );
    }
    // A session token is a credential too.
    assert!(
        !logs.contains("session-token-value"),
        "{context}: the session token reached the logs\n--- logs ---\n{logs}"
    );
}

/// The other backends' credentials, which take different paths through different
/// signers than s3's do.
const AZURE_KEY: &str = "YXp1cmVBY2NvdW50S2V5VGhhdE11c3ROb3RCZUxvZ2dlZA==";
const GCS_TOKEN: &str = "ya29.gcs-access-token-that-must-not-be-logged";
const SAS_TOKEN: &str = "sv=2021-06-08&sig=sas-signature-that-must-not-be-logged";
/// The http backend's credential. Both halves are the caller's, so both are checked: a
/// token in a header *name* is a caller's mistake that would still land in our log.
const HEADER_TOKEN: &str = "bearer-token-that-must-not-be-logged";
const HEADER_NAME: &str = "x-secret-name-that-must-not-be-logged";

fn assert_no_other_secret(logs: &str, context: &str) {
    for secret in [AZURE_KEY, GCS_TOKEN, SAS_TOKEN, HEADER_TOKEN, HEADER_NAME] {
        assert!(
            !logs.contains(secret),
            "{context}: a credential reached the logs as {secret:?}\n--- logs ---\n{logs}"
        );
    }
}

/// GCS and Azure, through the same filter, with the credential each of their signers
/// actually uses. There is no test server for either, and none is needed: the signer
/// runs before anything reaches the wire, and it is the signer that logs.
#[tokio::test]
async fn the_other_backends_credentials_never_reach_the_logs() {
    use hats_api::storage::{self, StorageOptions};

    let captured = logs();
    // One case per signer, since each takes its own path to the wire.
    for credential in [
        "gcs access token",
        "azure shared key",
        "azure sas token",
        "http headers",
    ] {
        let (port, _receiver) = capture_one_request();
        let endpoint = Some(format!("http://127.0.0.1:{port}"));
        let azure = || StorageOptions {
            endpoint: endpoint.clone(),
            account: Some("hatsdata".to_owned()),
            allow_http: true,
            ..Default::default()
        };
        let (raw, options) = match credential {
            "gcs access token" => (
                "gs://bucket/key.parquet",
                StorageOptions {
                    endpoint: endpoint.clone(),
                    access_token: Some(GCS_TOKEN.to_owned().into()),
                    allow_http: true,
                    ..Default::default()
                },
            ),
            "azure shared key" => (
                "az://container/key.parquet",
                StorageOptions {
                    access_key: Some(AZURE_KEY.to_owned().into()),
                    ..azure()
                },
            ),
            "azure sas token" => (
                "az://container/key.parquet",
                StorageOptions {
                    sas_token: Some(SAS_TOKEN.to_owned().into()),
                    ..azure()
                },
            ),
            // The http backend, whose credential goes out as a header rather than
            // through a signer. Its url carries the server, so the endpoint the others
            // take as an option is the url itself here.
            _ => (
                // Leaked into `raw` deliberately: this is the one backend whose address
                // is the url, and the port is the capturing server's.
                Box::leak(format!("http://127.0.0.1:{port}/key.parquet").into_boxed_str()) as &str,
                StorageOptions {
                    headers: serde_json::from_value(serde_json::json!({
                        "Authorization": format!("Bearer {HEADER_TOKEN}"),
                        HEADER_NAME: "value",
                    }))
                    .expect("the headers should deserialize"),
                    allow_http: true,
                    ..Default::default()
                },
            ),
        };

        let url = storage::parse_url(raw).expect("a valid url");
        let file = storage::open(&url, &options, &permissive_policy(), &transfers())
            .expect("it should open");

        // Both `Debug`s, which is what a handler holds and one `?value` from a log.
        tracing::debug!(?options, ?file, store = ?file.store, "the state a handler holds");

        use object_store::ObjectStoreExt;
        let _ = file
            .store
            .get(&object_store::path::Path::from("key.parquet"))
            .await;
    }

    assert_no_other_secret(&captured.contents(), "gcs, azure and http headers");
    assert!(
        captured.contents().contains("azblob"),
        "the azure store logged nothing, so nothing was checked"
    );
}

/// A read that succeeds. The happy path logs the most — spans opened and closed, the
/// url registered with DataFusion, the store's own requests.
#[tokio::test]
async fn a_successful_credentialed_read_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let result = lookup(
        &server.url("private/part0.parquet"),
        &server.credentialed_options(),
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await;
    assert!(result.is_ok(), "the read should have succeeded");

    let logs = captured.contents();
    assert_no_secret(&logs, "successful read");
    // The test is only worth anything if something was actually logged.
    assert!(
        !logs.is_empty(),
        "nothing was logged, so nothing was checked"
    );
}

/// A read that fails at the origin. Failures log more, and at higher levels, than
/// successes — and an error carrying the url is the classic way a secret escapes.
#[tokio::test]
async fn a_failed_credentialed_read_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    // A key that is not there, with credentials that are: the origin answers 404 and
    // the error travels back through every layer.
    let error = common::expect_error(
        lookup(
            &server.url("private/absent.parquet"),
            &server.credentialed_options(),
            &permissive_policy(),
            "objectid",
            "1",
            None,
        )
        .await,
        "a missing object should fail",
    );
    // The error itself, which is what a handler would log and return.
    assert_no_secret(&error.to_string(), "the error message");

    tracing::error!(%error, "logging the error the way a handler would");
    assert_no_secret(&captured.contents(), "failed read");
}

/// A session token is a third credential, and it takes a different path through the
/// builder than the key pair.
#[tokio::test]
async fn a_session_token_never_reaches_the_logs() {
    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let options = StorageOptions {
        session_token: Some("session-token-value".to_owned().into()),
        ..server.credentialed_options()
    };
    // It will not authenticate — the server knows no such token — which is fine: the
    // question is what got logged on the way.
    let _ = lookup(
        &server.url("private/part0.parquet"),
        &options,
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await;

    assert_no_secret(&captured.contents(), "session token");
}

/// The whole HTTP path, with the credential where it actually arrives: in the request
/// body. A body is not in the URI, so it is not in `tower_http`'s span by default — but
/// the router still narrows that span to method and path, and nothing between the
/// extractor and the handler may write the body out.
#[tokio::test]
async fn a_credentialed_request_through_the_router_logs_no_secret() {
    use tower::ServiceExt;

    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let body = serde_json::json!({
        "url": server.url("private/part0.parquet"),
        "storage": {
            "endpoint": server.endpoint,
            "access_key_id": ACCESS_KEY_ID,
            "secret_access_key": SECRET_ACCESS_KEY,
            "allow_http": true,
        },
        "column": "objectid",
        "value": "1",
    });

    let router = app::router(
        app::Service::new(
            permissive_policy(),
            &hats_api::config::LimitsConfig::default(),
            Arc::default(),
            &hats_api::config::ApiConfig::default(),
            &hats_api::config::DataConfig::default(),
            &hats_api::config::ServerConfig::default(),
        )
        .expect("the API alone is a service"),
    );
    let response = router
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/api/v1/select")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body);

    // Whatever the outcome, neither the response nor the logs may carry the secret.
    assert_no_secret(&body, &format!("the {status} response body"));
    assert_no_secret(&captured.contents(), "the request span");
}

/// The `Debug` of everything a handler holds while serving a credentialed request.
///
/// Every one of these is one `?value` away from a log line, and the types are built so
/// that writing it is harmless — this is the test that keeps them that way when a field
/// is added or a `#[derive(Debug)]` comes back.
#[tokio::test]
async fn debug_formatting_the_request_state_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let raw = server.url("private/part0.parquet");
    let options = server.credentialed_options();
    let source = hats_api::storage::SourceUrl::from(raw.clone());
    let url = hats_api::storage::parse_url(&raw).expect("the url should parse");
    let file = hats_api::storage::open(&url, &options, &permissive_policy(), &transfers())
        .expect("it should open");

    tracing::debug!(?source, ?options, ?file, store = ?file.store, "the state a handler holds");
    assert_no_secret(&captured.contents(), "debug-formatted request state");
    assert!(
        captured.contents().contains("the state a handler holds"),
        "the line was filtered out, so nothing was checked"
    );
}

/// A refusal the policy makes, before any store is built. The url is formatted into
/// the message on this path, which is exactly where a naive implementation leaks.
#[tokio::test]
async fn a_policy_refusal_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;

    // A policy that will not talk to this endpoint at all.
    let policy = AccessPolicy::new(
        &hats_api::config::AccessConfig {
            network: common::loopback(),
            s3: hats_api::config::EndpointConfig {
                endpoints: Some(vec!["https://minio.example.com".to_owned()]),
            },
            ..Default::default()
        },
        Arc::default(),
    )
    .expect("policy");

    let error = common::expect_error(
        lookup(
            &server.url("private/part0.parquet"),
            &server.credentialed_options(),
            &policy,
            "objectid",
            "1",
            None,
        )
        .await,
        "an unlisted endpoint must be refused",
    );
    assert_no_secret(&error.to_string(), "the refusal message");

    tracing::warn!(%error, "logging the refusal");
    assert_no_secret(&captured.contents(), "policy refusal");
}
