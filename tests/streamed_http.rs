//! Streamed answers over a real HTTP connection, with the layers a browser turns on.
//!
//! **Every other test drives the router with `oneshot`**, which is a `tower::Service` call
//! and not an HTTP client: it polls the body once to completion and stops, and it sends
//! whatever headers the test wrote. That is what let a streamed answer panic the worker on
//! every request a browser made while the whole suite stayed green — `tower_http`'s
//! compression wraps a body only where the request said `accept-encoding`, and it polls
//! that body once more after it ends, which an unfused `unfold` answers with a panic rather
//! than with `None`.
//!
//! So this file asks over a socket: a real connection, a real client, real header
//! negotiation, and a body read to the end. What it is for is the seam between this
//! service's streams and everything wrapped around them, which is the one thing a
//! `oneshot` cannot reach.
//!
//! The panic is the case it was written for, and a panic is why the assertions are about
//! the *whole* answer. It happens on a worker rather than in the handler's future, so the
//! response carries no status to check and no message to match: what a client sees is a
//! body that stops. A test that only read the status would pass through it.

mod common;

use std::io::Read;
use std::sync::Arc;

use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use flate2::read::GzDecoder;
use hats_api::access::AccessPolicy;
use hats_api::access::mount::Mounts;
use hats_api::app::{Service, router};
use hats_api::config::{
    AccessConfig, ApiConfig, DataConfig, LimitsConfig, MountConfig, ServerConfig, TapConfig,
};
use hats_api::storage::StorageOptions;
use tempfile::TempDir;

/// A service publishing one parquet file, answering on a port of its own.
struct Served {
    base: String,
    // Held so the directory outlives the server reading from it.
    _dir: TempDir,
}

impl Served {
    async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("part0.parquet"), common::parquet_fixture())
            .expect("write the fixture");

        let mount = MountConfig {
            path: "/".to_owned(),
            source: dir.path().display().to_string(),
            serve: true,
            follow_symlinks: false,
            immutable: false,
            storage: StorageOptions::default(),
            filenames: None,
        };
        let mounts = Arc::new(Mounts::new(&[mount], &DataConfig::default()).expect("mounts"));
        let policy = AccessPolicy::new(&AccessConfig::default(), Arc::clone(&mounts), None)
            .expect("access policy");
        let service = Service::new(
            policy,
            &LimitsConfig::default(),
            mounts,
            &ApiConfig::default(),
            &DataConfig::default(),
            &TapConfig::default(),
            &ServerConfig::default(),
        )
        .expect("service");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("local addr").port();
        // For the lifetime of the test process; the temp dir owns everything to clean up.
        tokio::spawn(async move {
            let _ = axum::serve(listener, router(service)).await;
        });

        Self {
            base: format!("http://127.0.0.1:{port}"),
            _dir: dir,
        }
    }
}

/// A client that neither asks for compression nor decodes it, so each test says for itself
/// what it negotiated. `reqwest` is built here without its `gzip` feature, which would do
/// both behind the test's back.
#[expect(
    clippy::disallowed_methods,
    reason = "this is the caller, not a request path: the rule keeps this service's own \
              outbound requests on the client `NetworkPolicy` builds, and what this one \
              does is knock on the socket that service is listening to"
)]
fn client() -> reqwest::Client {
    reqwest::Client::builder().build().expect("client")
}

/// The body, decompressed where the answer says it is compressed.
///
/// Read rather than trusted: a `content-encoding` the body does not match is the same class
/// of fault as a `200` to a ranged read, and it would otherwise show up as a parse error
/// somewhere further along.
fn read_body(encoding: Option<&str>, bytes: &[u8]) -> Vec<u8> {
    match encoding {
        Some("gzip") => {
            let mut out = Vec::new();
            GzDecoder::new(bytes)
                .read_to_end(&mut out)
                .expect("the body did not decode as the gzip it said it was");
            out
        }
        None => bytes.to_vec(),
        Some(other) => panic!("unexpected content-encoding {other}"),
    }
}

/// What a browser sends. The compression layer acts on this header and on nothing else, so
/// it is the whole difference between the request that worked and the one that panicked.
const AS_A_BROWSER: &str = "gzip, deflate, br";

/// The same negotiation, narrowed to the encoding these tests can read back.
///
/// Offered every encoding the service compiles in, it answers `br`, and reading that would
/// mean a brotli decoder as a dev-dependency. It would buy nothing: what is under test is a
/// body being wrapped and polled after it ends, which is one layer over any encoding, and
/// the test that sends the full browser header below asserts the half that does not need
/// decoding.
const ACCEPTS_GZIP: &str = "gzip";

/// A streamed answer survives being compressed, which is every answer to a browser.
///
/// This is the request the directory page's own preview makes. The assertion is that the
/// whole document arrives: the panic left a body that stopped, which reads as a truncated
/// JSON document rather than as any status.
#[tokio::test]
async fn a_streamed_answer_to_a_browser_arrives_whole() {
    let served = Served::start().await;
    let asked = format!(
        "{}/part0.parquet?columns=objectid&limit=10&format=json&streaming=true",
        served.base
    );

    let response = client()
        .get(&asked)
        .header(reqwest::header::ACCEPT_ENCODING, ACCEPTS_GZIP)
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(response.status(), 200);
    let encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .map(|value| value.to_str().expect("ascii").to_owned());
    assert_eq!(
        encoding.as_deref(),
        Some("gzip"),
        "the body was not compressed, so this did not exercise the layer"
    );
    // A streamed body has no length to send, whatever the encoding.
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .is_none()
    );
    let bytes = response.bytes().await.expect("the body stopped part-way");

    let body = read_body(encoding.as_deref(), &bytes);
    let answer: serde_json::Value =
        serde_json::from_slice(&body).expect("the document did not arrive whole");
    assert_eq!(answer["num_rows"], 10);
    assert_eq!(answer["rows"].as_array().expect("rows").len(), 10);
    // The counts a collected answer puts in headers are in the document, and a streamed
    // answer that ran to the end says nothing about having stopped.
    assert!(answer["data_bytes_read"].as_u64().expect("bytes") > 0);
    assert!(answer["refused"].is_null(), "{answer}");
}

/// The encoding a browser actually gets, read as far as a reader can without decoding it.
///
/// Offered the real header the service answers `br`, and the panic showed up as a body that
/// stopped: the worker died mid-answer, the connection closed without the terminating
/// chunk, and `hyper` reports that as an incomplete message rather than as any status. So
/// reading the body to the end is the assertion, and it holds without a brotli decoder.
#[tokio::test]
async fn the_encoding_a_browser_is_given_arrives_terminated() {
    let served = Served::start().await;
    let asked = format!(
        "{}/part0.parquet?columns=objectid&limit=10&format=json&streaming=true",
        served.base
    );

    let response = client()
        .get(&asked)
        .header(reqwest::header::ACCEPT_ENCODING, AS_A_BROWSER)
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_ENCODING],
        "br",
        "a browser was not offered the encoding this asserts about"
    );
    let bytes = response.bytes().await.expect("the body stopped part-way");
    assert!(!bytes.is_empty());
}

/// The same over the API route, whose body is the one the page's snippets post.
#[tokio::test]
async fn a_streamed_api_answer_to_a_browser_arrives_whole() {
    let served = Served::start().await;

    let response = client()
        .post(format!("{}/api/v1/simple/parquet", served.base))
        .header(reqwest::header::ACCEPT_ENCODING, ACCEPTS_GZIP)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "url": "file:///part0.parquet",
                "columns": ["objectid", "band"],
                "limit": 25,
                "format": "json",
                "streaming": true,
            })
            .to_string(),
        )
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(response.status(), 200);
    let encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .map(|value| value.to_str().expect("ascii").to_owned());
    let bytes = response.bytes().await.expect("the body stopped part-way");

    let answer: serde_json::Value = serde_json::from_slice(&read_body(encoding.as_deref(), &bytes))
        .expect("the document did not arrive whole");
    assert_eq!(answer["num_rows"], 25);
    assert_eq!(answer["rows"][0]["objectid"], 0);
}

/// A streamed parquet answer is not compressed, and is a file when it lands.
///
/// The compression layer is excluded by content type rather than by route, so the same
/// `accept-encoding` that compresses the JSON above has to leave this alone — a parquet
/// body wrapped in gzip is one whose `content-length` a reader could not have trusted, and
/// the exclusion is what the ranged reads elsewhere depend on.
#[tokio::test]
async fn a_streamed_parquet_answer_is_left_uncompressed_and_is_a_file() {
    let served = Served::start().await;
    let asked = format!(
        "{}/part0.parquet?columns=objectid&limit=40&streaming=true",
        served.base
    );

    let response = client()
        .get(&asked)
        .header(reqwest::header::ACCEPT_ENCODING, AS_A_BROWSER)
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(response.status(), 200);
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_none(),
        "a parquet answer was compressed"
    );
    // Nothing to seek in, and it says so rather than leaving a client to find out.
    assert_eq!(
        response.headers()[reqwest::header::ACCEPT_RANGES],
        "none",
        "a streamed body offered ranges"
    );
    let bytes = response.bytes().await.expect("the body stopped part-way");

    let rows: usize = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("the answer was not a parquet file")
        .build()
        .expect("reader")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 40);
}

/// A collected answer to the same browser is still seekable.
///
/// The compression exclusion and the ranges are one decision — a body a client is told to
/// seek in has to be one whose bytes are the ones it was told the length of — so the
/// negotiation that leaves parquet alone is what keeps `lsdb` able to read a query answer
/// at all. Asserted over a socket because a `oneshot` never negotiates.
#[tokio::test]
async fn a_collected_parquet_answer_still_answers_a_range_to_a_browser() {
    let served = Served::start().await;
    let asked = format!("{}/part0.parquet?columns=objectid&limit=40", served.base);

    let whole = client()
        .get(&asked)
        .header(reqwest::header::ACCEPT_ENCODING, AS_A_BROWSER)
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(whole.status(), 200);
    assert_eq!(whole.headers()[reqwest::header::ACCEPT_RANGES], "bytes");
    let length: usize = whole.headers()[reqwest::header::CONTENT_LENGTH]
        .to_str()
        .expect("ascii")
        .parse()
        .expect("a length");
    let whole = whole.bytes().await.expect("body");
    assert_eq!(whole.len(), length, "the length did not describe the body");

    // The last four bytes are where a parquet reader starts.
    let tail = client()
        .get(&asked)
        .header(reqwest::header::ACCEPT_ENCODING, AS_A_BROWSER)
        .header(reqwest::header::RANGE, "bytes=-4")
        .send()
        .await
        .expect("the request did not complete");
    assert_eq!(tail.status(), 206);
    let tail = tail.bytes().await.expect("body");
    assert_eq!(&tail[..], b"PAR1");
    assert_eq!(&tail[..], &whole[whole.len() - 4..]);
}
