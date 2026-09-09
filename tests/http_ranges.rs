//! Reading parquet over `http(s)://`, against servers that honour `Range` and servers
//! that do not.
//!
//! The second kind is the whole reason `materialize` exists, and it cannot be tested by
//! reading the code: a server that ignores `Range` and answers `200` with the whole body
//! looks, to every layer in between, exactly like a server that honoured it. What
//! separates them is the bytes the parquet reader ends up with, so these tests check the
//! rows that come back.
//!
//! Both servers here are a few lines of `tokio` rather than a real HTTP stack, because
//! what is under test is precisely the behaviour a real stack would not let us produce.

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{parquet_fixture, row_count};
use hats_api::access::AccessPolicy;
use hats_api::config::{AccessConfig, HttpConfig, LimitsConfig, NetworkConfig};
use hats_api::error::ApiError;
use hats_api::materialize::Transfers;
use hats_api::query::QueryResult;
use hats_api::storage::{self, StorageOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How the server under test answers a request carrying a `Range` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ranges {
    /// `206` with the bytes that were asked for, the way a static file server does.
    Honour,
    /// `200` with the whole body, whatever was asked for. Nginx with certain modules,
    /// anything generating its response, and vizcat itself.
    Ignore,
    /// `200` with the whole body and no `Content-Length` at all, which is what a service
    /// generating parquet on the fly answers with.
    IgnoreAndChunk,
}

/// A little HTTP server over one fixture, counting the requests it answered.
struct Server {
    port: u16,
    requests: Arc<AtomicUsize>,
}

impl Server {
    /// Serves `body` at every path, for as long as the test runs.
    fn start(body: Vec<u8>, ranges: Ranges) -> Self {
        Self::start_inner(body, ranges, None)
    }

    /// The same, but answering `401` unless the request carries this exact
    /// `Authorization` header — which is what a caller sets `headers` for.
    fn start_requiring_auth(body: Vec<u8>, ranges: Ranges, token: &str) -> Self {
        Self::start_inner(body, ranges, Some(format!("Bearer {token}")))
    }

    fn start_inner(body: Vec<u8>, ranges: Ranges, require: Option<String>) -> Self {
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
                let require = require.clone();
                tokio::spawn(async move {
                    let _ = serve(stream, &body, ranges, require.as_deref(), &counter).await;
                });
            }
        });
        Self { port, requests }
    }

    fn url(&self, key: &str) -> String {
        format!("http://127.0.0.1:{}/{key}", self.port)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
}

/// One connection, answering every request on it until the peer goes away. Keep-alive
/// matters: a parquet read is many requests, and this has to count them all.
async fn serve(
    mut stream: tokio::net::TcpStream,
    body: &[u8],
    ranges: Ranges,
    require: Option<&str>,
    counter: &AtomicUsize,
) -> std::io::Result<()> {
    let mut pending = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        // Read until at least one whole request head is in hand.
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
        counter.fetch_add(1, Ordering::Relaxed);

        let response = match require {
            Some(expected) if !carries_authorization(&head, expected) => {
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec()
            }
            _ => answer(&head, body, ranges),
        };
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

/// Whether the request head carries exactly this `Authorization`. Header names are
/// case-insensitive; the value is compared as sent.
fn carries_authorization(head: &str, expected: &str) -> bool {
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

/// The response, which is where the two kinds of server differ.
fn answer(head: &str, body: &[u8], ranges: Ranges) -> Vec<u8> {
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
        // Either no range was asked for, or this server does not do them: the whole
        // body, with a `200` that claims nothing was partial.
        (Ranges::IgnoreAndChunk, _) => {
            let _ = write!(
                response,
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            );
            for chunk in body.chunks(8192) {
                let _ = write!(response, "{:x}\r\n", chunk.len());
                response.extend_from_slice(chunk);
                response.extend_from_slice(b"\r\n");
            }
            response.extend_from_slice(b"0\r\n\r\n");
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
        // `bytes=-8`, a suffix.
        (true, false) => {
            let suffix: u64 = end.parse().ok()?;
            Some((len.saturating_sub(suffix), len))
        }
        // `bytes=100-`, everything from an offset.
        (false, true) => Some((start.parse().ok()?, len)),
        // `bytes=100-199`, inclusive at both ends.
        (false, false) => Some((
            start.parse().ok()?,
            end.parse::<u64>().ok()?.min(len - 1) + 1,
        )),
        (true, true) => None,
    }
}

/// Loopback and cleartext, which is what a test server on `127.0.0.1` is.
fn policy() -> AccessPolicy {
    AccessPolicy::new(
        &AccessConfig {
            network: NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            http: HttpConfig {
                endpoints: None,
                allow_plain_http: true,
            },
            ..Default::default()
        },
        Arc::default(),
    )
    .expect("the policy should build")
}

async fn read_one_row(url: &str, limits: &LimitsConfig) -> Result<QueryResult, ApiError> {
    read_one_row_with(url, limits, StorageOptions::default()).await
}

async fn read_one_row_with(
    url: &str,
    limits: &LimitsConfig,
    options: StorageOptions,
) -> Result<QueryResult, ApiError> {
    let parsed = storage::parse_url(url)?;
    let file = storage::open(
        &parsed,
        &options,
        &policy(),
        &Arc::new(Transfers::new(limits)),
    )?;
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

/// The ordinary case, and the baseline for the counts below: a server that honours
/// ranges is read a range at a time and the object is never copied anywhere.
#[tokio::test]
async fn reads_a_partition_from_a_server_that_honours_ranges() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    let result = read_one_row(&server.url("part0.parquet"), &LimitsConfig::default())
        .await
        .expect("the lookup should succeed");
    assert_eq!(row_count(&result), 1);
    assert_eq!(result.schema.fields().len(), 4);
}

/// The case this exists for. Every layer between here and the socket would accept the
/// `200` as an answer to a ranged request, and the reader would take the head of the
/// file for its footer — so the check is that the right row comes back.
#[tokio::test]
async fn reads_a_partition_from_a_server_that_ignores_ranges() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let result = read_one_row(&server.url("part0.parquet"), &LimitsConfig::default())
        .await
        .expect("a server that ignores Range must still be readable");
    assert_eq!(row_count(&result), 1);
    assert_eq!(result.schema.fields().len(), 4);
}

/// No `Content-Length` either, which is how a service generating parquet on the fly
/// answers. The size is only known once the body has been counted.
#[tokio::test]
async fn reads_a_partition_from_a_server_that_answers_chunked() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::IgnoreAndChunk);
    let result = read_one_row(&server.url("part0.parquet"), &LimitsConfig::default())
        .await
        .expect("a chunked whole-body answer must still be readable");
    assert_eq!(row_count(&result), 1);
}

/// The cost argument, measured rather than asserted in a comment. Passing the failure
/// through would fetch the whole file once per ranged read; the copy makes it once, plus
/// the probe that discovered the problem — and the probe *is* the copy, so it is one
/// transfer in total.
#[tokio::test]
async fn a_non_ranging_server_is_read_once_rather_than_once_per_range() {
    let ignoring = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    read_one_row(&ignoring.url("part0.parquet"), &LimitsConfig::default())
        .await
        .expect("the lookup should succeed");
    assert_eq!(
        ignoring.requests(),
        1,
        "the whole object should be fetched exactly once"
    );

    // And the ranging server, for contrast: many small requests rather than one big one.
    let honouring = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    read_one_row(&honouring.url("part0.parquet"), &LimitsConfig::default())
        .await
        .expect("the lookup should succeed");
    assert!(
        honouring.requests() > 1,
        "a ranging server should be read a range at a time, got {}",
        honouring.requests()
    );
}

/// Over the cap, from a server that declares its length. Refused before a byte is
/// written, and as a 413 rather than as the origin's fault.
#[tokio::test]
async fn an_object_larger_than_the_cap_is_refused() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let limits = LimitsConfig {
        max_materialize_bytes: bytesize::ByteSize::b(16),
        ..Default::default()
    };
    let error = read_one_row(&server.url("part0.parquet"), &limits)
        .await
        .expect_err("an object over the cap must be refused");
    assert_eq!(
        error.status(),
        http::StatusCode::PAYLOAD_TOO_LARGE,
        "{error}"
    );
    assert!(error.to_string().contains("byte ranges"), "{error}");
}

/// The same cap against a server that declares nothing, where it can only be enforced
/// while the bytes are arriving.
#[tokio::test]
async fn a_chunked_object_over_the_cap_is_refused_while_it_streams() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::IgnoreAndChunk);
    let limits = LimitsConfig {
        max_materialize_bytes: bytesize::ByteSize::b(16),
        ..Default::default()
    };
    let error = read_one_row(&server.url("part0.parquet"), &limits)
        .await
        .expect_err("an object over the cap must be refused");
    assert_eq!(
        error.status(),
        http::StatusCode::PAYLOAD_TOO_LARGE,
        "{error}"
    );
    assert!(
        error.to_string().contains("did not say how large"),
        "{error}"
    );
}

/// A zero cap is the operator saying this service does not copy objects to disk, which
/// makes a non-ranging server unreadable rather than expensive. A ranging one is
/// untouched by it.
#[tokio::test]
async fn a_zero_cap_turns_copying_off_without_affecting_a_ranging_server() {
    let limits = LimitsConfig {
        max_materialize_bytes: bytesize::ByteSize::b(0),
        ..Default::default()
    };

    let ignoring = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let error = read_one_row(&ignoring.url("part0.parquet"), &limits)
        .await
        .expect_err("copying is off, so this cannot be read");
    assert!(
        error.to_string().contains("max_materialize_bytes"),
        "{error}"
    );

    let honouring = Server::start(parquet_fixture().to_vec(), Ranges::Honour);
    assert_eq!(
        row_count(
            &read_one_row(&honouring.url("part0.parquet"), &limits)
                .await
                .expect("a ranging server needs no copy")
        ),
        1
    );
}

/// The scratch copy is deleted with the request that made it, whether it finished or
/// not — so a directory of its own is empty again afterwards.
#[tokio::test]
async fn the_scratch_copy_does_not_outlive_the_request() {
    let scratch = tempfile::TempDir::new().expect("a scratch directory");
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let limits = LimitsConfig {
        scratch_dir: Some(scratch.path().to_owned()),
        ..Default::default()
    };
    read_one_row(&server.url("part0.parquet"), &limits)
        .await
        .expect("the lookup should succeed");

    let left = std::fs::read_dir(scratch.path())
        .expect("the scratch directory should still be there")
        .count();
    assert_eq!(left, 0, "a scratch copy outlived its request");
}

/// The whole point of the `headers` option: a server that authenticates. Both the probe
/// and every read the store makes have to carry the token, and against a ranging server
/// that is several requests over a reused connection.
#[tokio::test]
async fn a_caller_header_authenticates_every_request_of_a_read() {
    const TOKEN: &str = "the-caller-s-own-token";
    let server = Server::start_requiring_auth(parquet_fixture().to_vec(), Ranges::Honour, TOKEN);
    let authenticated = |token: &str| StorageOptions {
        headers: serde_json::from_value(serde_json::json!({
            "Authorization": format!("Bearer {token}"),
        }))
        .expect("the headers should deserialize"),
        allow_http: true,
        ..Default::default()
    };

    let result = read_one_row_with(
        &server.url("part0.parquet"),
        &LimitsConfig::default(),
        authenticated(TOKEN),
    )
    .await
    .expect("the token should authenticate the read");
    assert_eq!(row_count(&result), 1);
    assert!(
        server.requests() > 1,
        "only one request was made, so the store's own reads were not covered"
    );

    // Without it, and with the wrong one: the server refuses and the read fails rather
    // than quietly returning nothing.
    for options in [StorageOptions::default(), authenticated("wrong-token")] {
        let error = read_one_row_with(
            &server.url("part0.parquet"),
            &LimitsConfig::default(),
            options,
        )
        .await
        .expect_err("an unauthenticated read must fail");
        assert!(!error.to_string().contains(TOKEN), "leaked: {error}");
    }
}

/// The same against a server that does not range, where the token has to be on the one
/// request that fetches the whole object.
#[tokio::test]
async fn a_caller_header_authenticates_a_materialized_read() {
    const TOKEN: &str = "the-caller-s-own-token";
    let server = Server::start_requiring_auth(parquet_fixture().to_vec(), Ranges::Ignore, TOKEN);
    let result = read_one_row_with(
        &server.url("part0.parquet"),
        &LimitsConfig::default(),
        StorageOptions {
            headers: serde_json::from_value(serde_json::json!({
                "Authorization": format!("Bearer {TOKEN}"),
            }))
            .expect("the headers should deserialize"),
            allow_http: true,
            ..Default::default()
        },
    )
    .await
    .expect("the token should authenticate the copy");
    assert_eq!(row_count(&result), 1);
}

/// A key with `=` in it, which is every HATS partition path. The probe builds the url
/// itself, so it has its own chance to mangle one.
#[tokio::test]
async fn reads_a_hats_partition_key_from_a_non_ranging_server() {
    let server = Server::start(parquet_fixture().to_vec(), Ranges::Ignore);
    let result = read_one_row(
        &server.url("dataset/Norder=5/Dir=0/Npix=12240/part0.parquet"),
        &LimitsConfig::default(),
    )
    .await
    .expect("a key with = in it should read");
    assert_eq!(row_count(&result), 1);
}
