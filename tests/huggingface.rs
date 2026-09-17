//! Reading a Hugging Face repository, against a stand-in for the Hub.
//!
//! Two things here cannot be seen by reading the code, and both are what this backend is:
//!
//! - **The read is two requests to two servers.** The Hub answers `resolve` with a redirect
//!   to a presigned url on a CDN, and the token must go to the first and not the second. A
//!   test that watched only one of them would pass with the hop missing entirely, or with the
//!   credential going wherever the first server pointed.
//! - **A listing is paginated.** The Hub answers a thousand entries and a `Link` header, so a
//!   reader that stopped at the first page would report a catalog missing most of its
//!   partitions — which reads as a small catalog rather than as a failure.
//!
//! The servers are plain HTTP on loopback, so the hop here is cleartext to cleartext. That is
//! the case the rule permits; the one it refuses — a hop that gives up TLS — is a unit test in
//! `storage::redirect`, where it can be asked without standing up a certificate.

mod common;

use std::io::{Read, Write};
use std::sync::mpsc::Receiver;

use common::{permissive_policy, transfers};
use hats_api::storage::{self, HfOptions, S3Options, StorageOptions};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

const TOKEN: &str = "hf_token-that-only-the-hub-may-see";

/// A server that answers whatever the test decides from the request head, and hands every
/// head it saw back.
///
/// It keeps answering rather than serving a fixed sequence: a read through a store is more
/// requests than a test should have to predict, and one that guessed wrong would fail for a
/// reason that has nothing to do with what it is checking.
struct Stub {
    port: u16,
    heads: Receiver<String>,
}

impl Stub {
    fn start(answer: impl Fn(&str) -> String + Send + 'static) -> Self {
        let (listener, port) = Self::reserve();
        Self::serve(listener, port, answer)
    }

    /// The port, before there is anything answering on it — for a test whose responses have
    /// to name the server they are served from.
    fn reserve() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local addr").port();
        (listener, port)
    }

    fn serve(
        listener: std::net::TcpListener,
        port: u16,
        answer: impl Fn(&str) -> String + Send + 'static,
    ) -> Self {
        let (sender, heads) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte) {
                        Ok(1) => head.push(byte[0]),
                        _ => break,
                    }
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let _ = stream.write_all(answer(&head).as_bytes());
                let _ = stream.flush();
                if sender.send(head).is_err() {
                    return;
                }
            }
        });
        Self { port, heads }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Every request this server saw, lowercased — header names are case-insensitive and
    /// nothing below cares.
    fn seen(&self) -> Vec<String> {
        let mut seen = Vec::new();
        while let Ok(head) = self
            .heads
            .recv_timeout(std::time::Duration::from_millis(500))
        {
            seen.push(head.to_ascii_lowercase());
        }
        seen
    }
}

/// `Connection: close`, so the next request opens a fresh connection rather than reusing one
/// this server has stopped reading from.
fn respond(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn options(endpoint: &str, token: Option<&str>) -> StorageOptions {
    StorageOptions {
        endpoint: Some(endpoint.to_owned()),
        // The token is the caller's own secret going to a cleartext server, which is theirs
        // to allow. A real Hub is https and this never comes up.
        allow_http: token.is_some(),
        hf: HfOptions {
            token: token.map(|token| token.to_owned().into()),
        },
        ..Default::default()
    }
}

/// The whole read: the Hub answers `resolve` with a redirect the way it does for a file in
/// LFS, and the bytes come from somewhere else.
///
/// What it holds is the credential boundary. The token authenticates to the Hub, which is the
/// server the operator's rules named; the CDN url is presigned and the token must not go to
/// it, since that host is the Hub's choice and not the operator's.
#[tokio::test]
async fn a_read_follows_the_hub_to_its_cdn_and_leaves_the_token_behind() {
    let cdn = Stub::start(|_| {
        respond(
            "206 Partial Content",
            "Content-Range: bytes 0-7/8\r\n",
            "PAR1data",
        )
    });
    let cdn_url = cdn.url();
    let hub = Stub::start(move |_| {
        respond(
            "307 Temporary Redirect",
            &format!("Location: {cdn_url}/xet-bridge-us/abc?Signature=def\r\n"),
            "",
        )
    });

    let url = storage::parse_url("hf://datasets/UniverseTBD/mmu_gz10/dataset/x.parquet")
        .expect("a valid url");
    let file = storage::open(
        &url,
        &options(&hub.url(), Some(TOKEN)),
        &permissive_policy(),
        &transfers(),
    )
    .expect("the policy allows loopback");

    let got = file
        .store
        .get(&ObjectPath::from("UniverseTBD/mmu_gz10/dataset/x.parquet"))
        .await
        .expect("the read follows the redirect")
        .bytes()
        .await
        .expect("the CDN's body");
    assert_eq!(&got[..], b"PAR1data");

    let asked = hub.seen();
    assert!(!asked.is_empty(), "the Hub was never asked");
    for head in &asked {
        // The repository and the revision are in the path, and the file is named below
        // `resolve` — which is the whole of what the url mapping has to get right.
        assert!(
            head.contains("get /datasets/universetbd/mmu_gz10/resolve/main/dataset/x.parquet"),
            "the Hub was asked for the wrong thing:\n{head}"
        );
        assert!(
            head.contains(&format!("authorization: bearer {TOKEN}")),
            "the Hub was asked without the token:\n{head}"
        );
    }

    let followed = cdn.seen();
    assert!(!followed.is_empty(), "the redirect was not followed");
    for head in &followed {
        assert!(
            !head.contains("authorization:"),
            "the token was carried to the CDN:\n{head}"
        );
        assert!(
            !head.contains(&TOKEN.to_ascii_lowercase()),
            "the token reached the CDN:\n{head}"
        );
        assert!(
            head.contains("/xet-bridge-us/abc"),
            "the hop went somewhere else:\n{head}"
        );
    }
}

/// A public repository is read with no token at all, which is the common case and the one a
/// deployment with a Hugging Face login on it must not quietly improve on.
#[tokio::test]
async fn an_anonymous_read_sends_no_authorization_at_all() {
    let hub = Stub::start(|_| respond("200 OK", "", "PAR1data"));

    let url = storage::parse_url("hf://datasets/UniverseTBD/mmu_gz10/dataset/x.parquet")
        .expect("a valid url");
    let file = storage::open(
        &url,
        &options(&hub.url(), None),
        &permissive_policy(),
        &transfers(),
    )
    .expect("the policy allows loopback");
    let _ = file
        .store
        .get(&ObjectPath::from("UniverseTBD/mmu_gz10/dataset/x.parquet"))
        .await;

    let asked = hub.seen();
    assert!(!asked.is_empty(), "the Hub was never asked");
    for head in &asked {
        assert!(
            !head.contains("authorization:"),
            "an anonymous read was signed:\n{head}"
        );
    }
}

/// The listing walks every page the Hub offers. One page is a thousand entries and a catalog
/// is more than that, so a reader that took the first page for the answer would report a
/// catalog with most of its partitions missing.
#[tokio::test]
async fn a_listing_walks_every_page_and_skips_directories() {
    // The first page's `next` link names this server, so the port has to be known before the
    // responses are written.
    let (listener, port) = Stub::reserve();
    let hub = Stub::serve(listener, port, move |head| {
        match head.contains("cursor=second") {
            false => respond(
                "200 OK",
                &format!(
                    "Content-Type: application/json\r\nLink: \
                 <http://127.0.0.1:{port}/api/datasets/o/n/tree/main?cursor=second>; \
                 rel=\"next\"\r\n"
                ),
                r#"[{"type":"directory","path":"dataset/Norder=0","size":0},
                {"type":"file","path":"dataset/Norder=0/Dir=0/Npix=1.parquet","size":4096}]"#,
            ),
            true => respond(
                "200 OK",
                "Content-Type: application/json\r\n",
                r#"[{"type":"file","path":"dataset/Norder=0/Dir=0/Npix=2.parquet","size":8192}]"#,
            ),
        }
    });

    let url = storage::parse_url("hf://datasets/o/n").expect("a valid url");
    let dir = storage::open_dir(
        &url,
        &options(&hub.url(), None),
        &permissive_policy(),
        &transfers(),
    )
    .expect("the policy allows loopback");

    let mut names: Vec<String> = dir
        .list("dataset")
        .await
        .expect("the Hub lists the repository")
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "Norder=0/Dir=0/Npix=1.parquet".to_owned(),
            "Norder=0/Dir=0/Npix=2.parquet".to_owned(),
        ],
        "a page was dropped, or a directory became an entry"
    );
}

/// A url that names no repository is a 400 saying how one is written, rather than a request
/// that goes to the Hub and comes back as whatever the Hub says about it.
#[tokio::test]
async fn a_url_that_names_no_repository_is_refused_here() {
    for raw in ["hf://datasets/owner", "hf://owner/name/x.parquet"] {
        let url = storage::parse_url(raw).expect("a valid url");
        let error = storage::open(
            &url,
            &StorageOptions::default(),
            &permissive_policy(),
            &transfers(),
        )
        .expect_err("the url names no repository");
        assert!(
            error.to_string().contains("hf://<type>/<owner>/<name>"),
            "{raw}: {error}"
        );
    }
}

/// `token` is the hf backend's option and no other's, and an option belonging elsewhere is
/// refused rather than ignored — a `secret_access_key` sent to the Hub is a credential in the
/// wrong place whichever way the caller meant it.
#[tokio::test]
async fn the_token_is_the_only_option_this_backend_takes() {
    let url = storage::parse_url("hf://datasets/o/n/x.parquet").expect("a valid url");
    let misdirected = StorageOptions {
        s3: S3Options {
            region: Some("us-east-1".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };
    let error = storage::open(&url, &misdirected, &permissive_policy(), &transfers())
        .expect_err("region is not an hf option");
    assert!(error.to_string().contains("they take token"), "{error}");
}
