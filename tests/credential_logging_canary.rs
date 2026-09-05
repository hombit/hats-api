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

use common::{SECRET_ACCESS_KEY, TestS3, lookup, permissive_policy};
use hats_api::logging::CREDENTIAL_UNSAFE_TARGETS;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

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
        &server.url("private/part0.parquet", &server.credentialed_options()),
        &permissive_policy(),
        "objectid",
        "1",
        None,
    )
    .await;

    let logs = String::from_utf8_lossy(&captured.0.lock().expect("log buffer")).into_owned();
    let leaking: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains(SECRET_ACCESS_KEY))
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
}
