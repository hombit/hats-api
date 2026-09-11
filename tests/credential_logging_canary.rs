//! Which targets would leak a credential if nothing stopped them.
//!
//! `credential_logging.rs` asserts the guarantee: with the filter the binary installs,
//! no credential reaches the logs. That test keeps passing if a newly added dependency
//! starts logging secrets, because the assertion is about the output, not the cause —
//! so it would pass right up until someone removed an entry from the deny list.
//!
//! This one runs the same request with the deny list *off* and reports who prints the
//! secret. Every such target must already be named in
//! [`hats_api::logging::CREDENTIAL_UNSAFE_TARGETS`]; a new one fails the test and has to
//! be triaged deliberately — silenced there, or fixed upstream.
//!
//! Its own binary because a process has one subscriber and this one must be unfiltered.

mod common;

use std::io;
use std::sync::{Arc, Mutex};

use common::{
    SECRET_ACCESS_KEY, TestS3, capture_one_request, lookup, permissive_policy, transfers,
};
use hats_api::logging::CREDENTIAL_UNSAFE_TARGETS;
use hats_api::storage::{self, AzureOptions, GcsOptions, HttpOptions, StorageOptions};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

/// Credentials for the backends with no test server, one per signer, each distinct so a
/// leak says which one it was. Azure's is base64 because its signer requires that.
const AZURE_KEY: &str = "Y2FuYXJ5QXp1cmVBY2NvdW50S2V5Rm9yTG9nZ2luZw==";
const GCS_TOKEN: &str = "ya29.canary-gcs-access-token";
/// The http backend's, which reaches the wire as a header rather than through a signer —
/// so it is the one that would be logged by the HTTP client rather than by a signer.
const HEADER_TOKEN: &str = "canary-http-bearer-token";

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

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

/// One credentialed `GET` against a one-shot loopback server, for its side effect on
/// the logs. Whether it succeeds is beside the point — the signer has run either way,
/// which is what this file is watching.
async fn signing_request(raw: &str, options: impl FnOnce(String) -> StorageOptions) {
    let (port, _receiver) = capture_one_request();
    let url = storage::parse_url(raw).expect("a valid url");
    let file = storage::open(
        &url,
        &options(format!("http://127.0.0.1:{port}")),
        &permissive_policy(),
        &transfers(),
    )
    .expect("the policy allows loopback");

    use object_store::ObjectStoreExt;
    let _ = file
        .store
        .get(&object_store::path::Path::from("key.parquet"))
        .await;
}

/// One request to an `http(s)://` url carrying a caller's header, for its side effect on
/// the logs. The url is the address here, so there is no endpoint option to point
/// elsewhere with.
async fn http_request_with_headers(token: &str) {
    let (port, _receiver) = capture_one_request();
    let raw = format!("http://127.0.0.1:{port}/key.parquet");
    let url = storage::parse_url(&raw).expect("a valid url");
    let options = StorageOptions {
        http: HttpOptions {
            headers: serde_json::from_value(serde_json::json!({
                "Authorization": format!("Bearer {token}"),
            }))
            .expect("the headers should deserialize"),
        },
        allow_http: true,
        ..Default::default()
    };
    let file = storage::open(&url, &options, &permissive_policy(), &transfers())
        .expect("the policy allows loopback and cleartext");

    use object_store::ObjectStoreExt;
    let _ = file
        .store
        .get(&object_store::path::Path::from("key.parquet"))
        .await;
}

#[tokio::test]
async fn every_target_that_logs_a_credential_is_already_known() {
    let captured = CapturedLogs::default();
    // Everything, unfiltered, except the test server itself — `s3s` receives the
    // credential by design and logs the headers it was sent.
    tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_env_filter(EnvFilter::new("trace").add_directive("s3s=off".parse().expect("valid")))
        .with_ansi(false)
        .with_target(true)
        .init();

    let server = TestS3::authenticated().await;
    server.put_parquet("private/part0.parquet");
    let _ = lookup(
        &server.url("private/part0.parquet"),
        &server.credentialed_options(),
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await;

    // GCS and Azure have no test server here, and do not need one: what is being
    // watched is the signer, which runs before anything goes on the wire. A one-shot
    // server on loopback is enough to make the store actually sign and send.
    signing_request("gs://bucket/key.parquet", |endpoint| StorageOptions {
        endpoint: Some(endpoint),
        allow_http: true,
        gcs: GcsOptions {
            access_token: Some(GCS_TOKEN.to_owned().into()),
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    signing_request("az://container/key.parquet", |endpoint| StorageOptions {
        endpoint: Some(endpoint),
        allow_http: true,
        azure: AzureOptions {
            account: Some("hatsdata".to_owned()),
            access_key: Some(AZURE_KEY.to_owned().into()),
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    // The http backend has no signer: its credential goes out as a header this service
    // puts on the request, so what could log it is the HTTP client rather than a signer.
    // Its url is its own endpoint, so this one is built rather than passed an endpoint.
    http_request_with_headers(HEADER_TOKEN).await;

    let logs = String::from_utf8_lossy(&captured.0.lock().expect("log buffer")).into_owned();
    let leaking: Vec<&str> = logs
        .lines()
        .filter(|line| {
            [SECRET_ACCESS_KEY, AZURE_KEY, GCS_TOKEN, HEADER_TOKEN]
                .iter()
                .any(|secret| line.contains(secret))
        })
        .collect();

    let unknown: Vec<&&str> = leaking
        .iter()
        .filter(|line| {
            !CREDENTIAL_UNSAFE_TARGETS
                .iter()
                .any(|target| line.contains(target))
        })
        .collect();
    assert!(
        unknown.is_empty(),
        "a target not in CREDENTIAL_UNSAFE_TARGETS logged the secret. Add it there \
         (with a reason) or fix it upstream:\n{}",
        unknown
            .iter()
            .map(|line| format!("  {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // Not an assertion: upstream fixing its logging is good news, and the only cost is
    // a stale entry. Saying so is enough to get it removed.
    for target in CREDENTIAL_UNSAFE_TARGETS {
        if !leaking.iter().any(|line| line.contains(target)) {
            eprintln!(
                "note: {target} is in CREDENTIAL_UNSAFE_TARGETS but logged no credential \
                 here; if upstream fixed it, the entry can go"
            );
        }
    }
    assert!(
        !logs.is_empty(),
        "nothing was logged, so nothing was checked"
    );
    // A backend whose signer never ran is a backend this file is not watching, and the
    // silence would read exactly like a pass.
    for service in ["s3", "gcs", "azblob", "http"] {
        assert!(
            logs.contains(service),
            "{service} logged nothing at all, so it was not checked:\n{logs}"
        );
    }
}
