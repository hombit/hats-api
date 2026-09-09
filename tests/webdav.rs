//! Reading parquet over `webdav://`.
//!
//! WebDAV is HTTP with two additions that matter here: the object's size comes from a
//! `PROPFIND` rather than a `HEAD`, and the credential is Basic rather than a bearer
//! token. Both are things a mock cannot stand in for — OpenDAL parses the `PROPFIND`
//! body as XML, and the Basic header has to reach two different clients — so the server
//! below answers `PROPFIND` for real, the way `tests/http_ranges.rs` answers `Range` for
//! real, and the tests check the rows that come back.
//!
//! A WebDAV server that ignores `Range` is the case worth the most attention. The
//! credential goes out as a header so that [`materialize`]'s probe carries it too, and a
//! probe that got a `401` would conclude the server ranges and hand the reader the head
//! of the file at every offset. That is a wrong answer rather than an error, and
//! `basic_authentication_reaches_the_probe_as_well` is what catches it.

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{parquet_fixture, row_count};
use hats_api::access::AccessPolicy;
use hats_api::config::{AccessConfig, EndpointConfig, LimitsConfig, NetworkConfig};
use hats_api::error::ApiError;
use hats_api::materialize::Transfers;
use hats_api::query::QueryResult;
use hats_api::storage::{self, StorageOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const USERNAME: &str = "reader";
const PASSWORD: &str = "s3cret-and-not-in-any-log";

/// How the server under test answers a request carrying a `Range` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ranges {
    /// `206` with the bytes that were asked for.
    Honour,
    /// `200` with the whole body, whatever was asked for.
    Ignore,
}

/// A WebDAV server over one fixture, counting the requests it answered.
struct Server {
    port: u16,
    requests: Arc<AtomicUsize>,
}

impl Server {
    fn start(body: Vec<u8>, ranges: Ranges) -> Self {
        Self::start_inner(body, ranges, false)
    }

    /// The same, answering `401` unless the request carries the Basic credential above.
    fn start_requiring_auth(body: Vec<u8>, ranges: Ranges) -> Self {
        Self::start_inner(body, ranges, true)
    }

    fn start_inner(body: Vec<u8>, ranges: Ranges, require_auth: bool) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let port = listener.local_addr().expect("a bound address").port();
        listener.set_nonblocking(true).expect("nonblocking");
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        let body = Arc::new(body);

        tokio::spawn(async move {
            let listener = TcpListener::from_std(listener).expect("a tokio listener");
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let body = Arc::clone(&body);
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    let _ = serve(stream, &body, ranges, require_auth, &counter).await;
                });
            }
        });
        Self { port, requests }
    }

    fn url(&self, key: &str) -> String {
        format!("webdav://127.0.0.1:{}/{key}", self.port)
    }

    /// What an operator has to write to allow this server: the transport underneath the
    /// `webdav://` url, not the url itself. Cleartext WebDAV is reachable only by being
    /// named, so every test here also exercises the endpoint rule.
    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
}

/// One connection, answering every request on it until the peer goes away. A parquet
/// read is many requests and they share a connection.
async fn serve(
    mut stream: tokio::net::TcpStream,
    body: &[u8],
    ranges: Ranges,
    require_auth: bool,
    counter: &AtomicUsize,
) -> std::io::Result<()> {
    let mut pending = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        while find_head(&pending).is_none() {
            let read = stream.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }
            pending.extend_from_slice(buffer.get(..read).unwrap_or_default());
        }
        let Some(end) = find_head(&pending) else {
            return Ok(());
        };
        let head = String::from_utf8_lossy(pending.get(..end).unwrap_or_default()).into_owned();
        pending.drain(..end);
        // A PROPFIND carries a body, which has to be drained off the connection before
        // the next request head can be read from it.
        let length = content_length(&head).unwrap_or(0);
        while pending.len() < length {
            let read = stream.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }
            pending.extend_from_slice(buffer.get(..read).unwrap_or_default());
        }
        pending.drain(..length);
        counter.fetch_add(1, Ordering::Relaxed);

        let response = match require_auth && !carries_basic(&head) {
            true => b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec(),
            false => answer(&head, body, ranges),
        };
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

/// Whether the request head carries the Basic credential the server was started with.
/// Encoded here rather than compared to a constant, so the test asserts the encoding is
/// the one a server would accept rather than the one this crate happens to produce.
fn carries_basic(head: &str) -> bool {
    use base64::Engine;
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{USERNAME}:{PASSWORD}"))
    );
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(name, value)| name.eq_ignore_ascii_case("authorization") && value.trim() == expected)
}

fn find_head(pending: &[u8]) -> Option<usize> {
    pending
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|start| start + 4)
}

fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
}

fn method(head: &str) -> &str {
    head.split_whitespace().next().unwrap_or_default()
}

fn answer(head: &str, body: &[u8], ranges: Ranges) -> Vec<u8> {
    match method(head) {
        "PROPFIND" => propfind(head, body.len()),
        _ => get(head, body, ranges),
    }
}

/// A `207 Multi-Status` describing one file. Only the length and the resource type are
/// read here; the rest is what a server sends and is present so this looks like one.
fn propfind(head: &str, length: usize) -> Vec<u8> {
    let path = head.split_whitespace().nth(1).unwrap_or("/");
    let xml = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>{path}</D:href>
    <D:propstat>
      <D:prop>
        <D:getcontentlength>{length}</D:getcontentlength>
        <D:getlastmodified>Sun, 01 May 2022 06:39:47 GMT</D:getlastmodified>
        <D:getcontenttype>application/octet-stream</D:getcontenttype>
        <D:resourcetype/>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#
    );
    let mut response = Vec::new();
    let _ = write!(
        response,
        "HTTP/1.1 207 Multi-Status\r\nContent-Type: application/xml\r\n\
         Content-Length: {}\r\n\r\n",
        xml.len()
    );
    response.extend_from_slice(xml.as_bytes());
    response
}

fn get(head: &str, body: &[u8], ranges: Ranges) -> Vec<u8> {
    let wanted = requested_range(head, body.len() as u64);
    let mut response = Vec::new();
    match (ranges, wanted) {
        (Ranges::Honour, Some(range)) => {
            let slice = body
                .get(range.0 as usize..range.1 as usize)
                .unwrap_or_default();
            let _ = write!(
                response,
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
                 Content-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\n\r\n",
                slice.len(),
                range.0,
                range.1.saturating_sub(1),
                body.len()
            );
            response.extend_from_slice(slice);
        }
        _ => {
            let _ = write!(
                response,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            response.extend_from_slice(body);
        }
    }
    response
}

/// The `Range` header as a half-open byte range, resolved against the body's length.
fn requested_range(head: &str, len: u64) -> Option<(u64, u64)> {
    let line = head
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("range:"))?;
    let spec = line.split_once('=')?.1.trim();
    let (start, end) = spec.split_once('-')?;
    match (start.is_empty(), end.is_empty()) {
        (true, false) => {
            let suffix: u64 = end.parse().ok()?;
            Some((len.saturating_sub(suffix), len))
        }
        (false, true) => Some((start.parse().ok()?, len)),
        (false, false) => Some((
            start.parse().ok()?,
            end.parse::<u64>().ok()?.min(len - 1) + 1,
        )),
        (true, true) => None,
    }
}

/// Loopback, with this one cleartext WebDAV server named. Naming it is the only way to
/// reach it: `access.webdav` has no `allow_plain_http`.
fn policy(endpoints: &[&str]) -> AccessPolicy {
    AccessPolicy::new(
        &AccessConfig {
            network: NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            webdav: EndpointConfig {
                endpoints: Some(endpoints.iter().map(|entry| (*entry).to_owned()).collect()),
            },
            ..Default::default()
        },
        Arc::default(),
    )
    .expect("the policy should build")
}

fn options(value: serde_json::Value) -> StorageOptions {
    serde_json::from_value(value).expect("the options should deserialize")
}

/// Over cleartext, since a test server has no certificate.
fn cleartext() -> StorageOptions {
    options(serde_json::json!({"transport": "http"}))
}

fn credentialed() -> StorageOptions {
    options(serde_json::json!({
        "transport": "http",
        "username": USERNAME,
        "password": PASSWORD,
        // The caller's own half of the cleartext decision, which a password needs.
        "allow_http": true,
    }))
}

async fn read_one_row(
    url: &str,
    policy: &AccessPolicy,
    options: StorageOptions,
    limits: &LimitsConfig,
) -> Result<QueryResult, ApiError> {
    let parsed = storage::parse_url(url)?;
    let file = storage::open(&parsed, &options, policy, &Arc::new(Transfers::new(limits)))?;
    hats_api::query::run(
        &file,
        &hats_api::query::Selection {
            predicate: hats_api::query::Predicate::Where("objectid = 42"),
            ..Default::default()
        },
        limits.into(),
        hats_api::query::Order::Unspecified,
    )
    .await
}

/// The ordinary case: `PROPFIND` for the size, then a range at a time for the bytes.
#[tokio::test]
async fn reads_a_partition_over_webdav() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    let result = read_one_row(
        &server.url("part0.parquet"),
        &policy(&[&server.endpoint()]),
        cleartext(),
        &LimitsConfig::default(),
    )
    .await
    .expect("the lookup should succeed");
    assert_eq!(row_count(&result), 1);
    assert_eq!(result.schema.fields().len(), 4);
    assert!(
        server.requests() > 1,
        "a ranging server should be read a range at a time, got {}",
        server.requests()
    );
}

/// WebDAV servers that generate their responses ignore `Range` as readily as plain HTTP
/// ones do, which is why this backend is behind `MaterializingStore` too. Checked by the
/// rows, because a `200` to a ranged read is indistinguishable from a `206` at every
/// layer above the bytes.
#[tokio::test]
async fn a_server_that_ignores_ranges_still_answers_with_the_right_rows() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let result = read_one_row(
        &server.url("part0.parquet"),
        &policy(&[&server.endpoint()]),
        cleartext(),
        &LimitsConfig::default(),
    )
    .await
    .expect("a server that ignores Range must still be readable");
    assert_eq!(row_count(&result), 1);
}

/// The credential has to reach OpenDAL *and* the materialization probe, which are two
/// clients making two different requests. A probe that went out unauthenticated would
/// get a `401`, conclude from it that the server ranges, and hand the reader whole-body
/// responses to every ranged read — so this server both requires Basic and ignores
/// `Range`, and the assertion is on the rows.
#[tokio::test]
async fn basic_authentication_reaches_the_probe_as_well() {
    let server = Server::start_requiring_auth(parquet_fixture().to_vec(), Ranges::Ignore);
    let policy = policy(&[&server.endpoint()]);

    let result = read_one_row(
        &server.url("part0.parquet"),
        &policy,
        credentialed(),
        &LimitsConfig::default(),
    )
    .await
    .expect("the credential should open the file");
    assert_eq!(row_count(&result), 1);

    // And it is the credential doing it, not the server letting anyone in.
    let anonymous = read_one_row(
        &server.url("part0.parquet"),
        &policy,
        cleartext(),
        &LimitsConfig::default(),
    )
    .await;
    assert!(
        anonymous.is_err(),
        "an anonymous read should not have succeeded"
    );
}

/// A `webdav://` url over cleartext is refused unless the operator named that server.
/// There is no `allow_plain_http` for this backend, so naming it is the whole of the
/// decision — and the refusal happens before a connection is opened.
#[tokio::test]
async fn cleartext_webdav_needs_the_server_named() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    let error = read_one_row(
        &server.url("part0.parquet"),
        // Loopback is allowed by the network rules; this is the endpoint rules refusing.
        &policy(&[]),
        cleartext(),
        &LimitsConfig::default(),
    )
    .await
    .expect_err("an unnamed cleartext server must be refused");
    assert_eq!(error.status(), http::StatusCode::FORBIDDEN, "{error}");
    assert_eq!(server.requests(), 0, "nothing should have been requested");
}

/// The default transport is TLS, so a url that says nothing does not quietly fall back
/// to cleartext — it tries `https` against a server speaking `http` and fails to connect
/// rather than sending anything in the clear.
#[tokio::test]
async fn the_transport_defaults_to_tls() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    let error = read_one_row(
        &server.url("part0.parquet"),
        // Named for cleartext, which is the entry that would allow it if the default
        // were `http`.
        &policy(&[&server.endpoint()]),
        StorageOptions::default(),
        &LimitsConfig::default(),
    )
    .await
    .expect_err("https against an http server cannot succeed");
    assert!(
        !error.to_string().to_lowercase().contains(PASSWORD),
        "{error}"
    );
}
