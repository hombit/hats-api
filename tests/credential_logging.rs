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

use common::{ACCESS_KEY_ID, SECRET_ACCESS_KEY, TestS3, lookup, permissive_policy};
use hats_api::access::AccessPolicy;
use hats_api::app;
use hats_api::logging;
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

/// A read that succeeds. The happy path logs the most — spans opened and closed, the
/// url registered with DataFusion, the store's own requests.
#[tokio::test]
async fn a_successful_credentialed_read_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let result = lookup(
        &server.url("private/part0.parquet", &server.credentialed_options()),
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

    let options = format!(
        "access_key_id={ACCESS_KEY_ID}&secret_access_key={SECRET_ACCESS_KEY}&allow_http=true"
    );
    // A key that is not there, with credentials that are: the origin answers 404 and
    // the error travels back through every layer.
    let error = common::expect_error(
        lookup(
            &server.url("private/absent.parquet", &options),
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

    let options = format!(
        "access_key_id={ACCESS_KEY_ID}&secret_access_key={SECRET_ACCESS_KEY}\
         &session_token=session-token-value&allow_http=true"
    );
    // It will not authenticate — the server knows no such token — which is fine: the
    // question is what got logged on the way.
    let _ = lookup(
        &server.url("private/part0.parquet", &options),
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await;

    assert_no_secret(&captured.contents(), "session token");
}

/// The whole HTTP path, which is where the credential actually arrives today: in the
/// query string of a `GET`. `tower_http`'s default span carries the whole URI, so the
/// router replaces it with method and path — this is the test that keeps it replaced.
#[tokio::test]
async fn the_request_span_does_not_carry_the_query_string() {
    use tower::ServiceExt;

    let captured = logs();
    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");

    let target = server.url("private/part0.parquet", &server.credentialed_options());
    let request_uri = format!(
        "/api/v1/select?url={}&column=objectid&value=1",
        urlencode(&target)
    );

    let router = app::router(Arc::new(permissive_policy()));
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(&request_uri)
                .body(axum::body::Body::empty())
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

    // Whatever the outcome, neither the body nor the logs may carry the secret.
    assert_no_secret(&body, &format!("the {status} response body"));
    assert_no_secret(&captured.contents(), "the request span");
}

/// A refusal the policy makes, before any store is built. The url is formatted into
/// the message on this path, which is exactly where a naive implementation leaks.
#[tokio::test]
async fn a_policy_refusal_logs_no_secret() {
    let captured = logs();
    let server = TestS3::authenticated().await;

    // A policy that will not talk to this endpoint at all.
    let policy = AccessPolicy::new(&hats_api::config::AccessConfig {
        allow_loopback: true,
        s3: hats_api::config::S3Config {
            endpoints: Some(vec!["https://minio.example.com".to_owned()]),
        },
        ..Default::default()
    })
    .expect("policy");

    let error = common::expect_error(
        lookup(
            &server.url("private/part0.parquet", &server.credentialed_options()),
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

/// Percent-encode a url so it survives being one query parameter inside another.
fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}
