//! The request is the only source of credentials.
//!
//! The service must never answer a caller with an identity of its own. A caller who
//! sends no credentials gets an unsigned request — not the process's environment, not
//! `~/.aws`, not an instance profile, not the metadata server. Otherwise a request for
//! `s3://someone-elses-private-bucket/x.parquet` would be served with whatever the
//! deployment happens to be entitled to.
//!
//! Its own test binary because it sets `AWS_*` in the environment, which is
//! process-wide: run alongside other tests, it would change what they are testing. One
//! test here, and the variables are set before anything builds a store.
//!
//! The check has to be on the wire. Whether a store would sign is not visible on the
//! builder, and both a signed and an unsigned request to a nonexistent object fail —
//! so only the request head distinguishes them.

// The package denies `unsafe_code`. Setting an environment variable is unsafe in
// edition 2024, and proving the service ignores the environment requires setting it.
// This is the only place in the package that is allowed to, and it is a test.
#![allow(unsafe_code)]

mod common;

use common::{capture_one_request, permissive_policy};
use hats_api::storage::{self, StorageOptions};

/// Values that are syntactically valid, so that anything picking them up would sign
/// successfully rather than erroring for an unrelated reason.
const AMBIENT_ACCESS_KEY_ID: &str = "AKIAAMBIENTNOTVALID1";
const AMBIENT_SECRET: &str = "ambientSecretThatMustNeverBeUsed12345678";

#[tokio::test]
async fn a_request_with_no_credentials_ignores_the_environment() {
    // SAFETY: this binary holds one test, so nothing else is reading the environment
    // while it is written. The variables must be set before the store is built —
    // OpenDAL reads them at build time.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", AMBIENT_ACCESS_KEY_ID);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", AMBIENT_SECRET);
        std::env::set_var("AWS_SESSION_TOKEN", "ambient-session-token");
        std::env::set_var("AWS_REGION", "eu-central-1");
        std::env::set_var("AWS_DEFAULT_REGION", "eu-central-1");
        // `AWS_ENDPOINT_URL` is the other half: it would redirect a request the policy
        // already decided was going to AWS.
        std::env::set_var("AWS_ENDPOINT_URL", "https://ambient.example.com");
        // The EC2 metadata service, the classic way a deployment's identity leaks into
        // a request it should not have had.
        std::env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://169.254.170.2/creds",
        );
        std::env::set_var("AWS_PROFILE", "ambient-profile");
    }

    let (port, receiver) = capture_one_request();
    let url = storage::parse_url("s3://bucket/key.parquet").expect("a valid url");
    // The request carries no credentials at all, which is the whole point: whatever
    // the environment holds, an unsigned request is what must go out.
    let options = StorageOptions {
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        ..Default::default()
    };
    let file =
        storage::open(&url, &options, &permissive_policy()).expect("the policy allows loopback");

    use object_store::ObjectStoreExt;
    let _ = file
        .store
        .get(&object_store::path::Path::from("key.parquet"))
        .await;

    let head = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the store made no request");

    // The request went where the url said, not where the environment said.
    assert!(
        head.to_ascii_lowercase()
            .contains("get /bucket/key.parquet"),
        "the request did not go to the url's endpoint:\n{head}"
    );
    // And it carried no identity at all.
    let lowered = head.to_ascii_lowercase();
    assert!(
        !lowered.contains("authorization:"),
        "the request was signed with an ambient credential:\n{head}"
    );
    assert!(
        !lowered.contains("x-amz-security-token:"),
        "the request carried an ambient session token:\n{head}"
    );
    assert!(
        !head.contains(AMBIENT_ACCESS_KEY_ID) && !head.contains(AMBIENT_SECRET),
        "an environment credential reached the wire:\n{head}"
    );
}

// What this test does and does not pin, so the next person does not assume more:
//
// It fails if `skip_signature` goes missing — without it the builder walks the whole
// ambient chain (environment, profile, then the metadata server, slowly) and signs.
// That is the guarantee that matters, and the one a caller would notice.
//
// It does not fail if `disable_config_load` goes missing. That guard's other job is to
// stop `AWS_ENDPOINT_URL` redirecting a request the policy authorised for AWS, and the
// redirect is not observable from here: a url with no `endpoint` option uses
// virtual-host addressing, so a redirected endpoint becomes `bucket.<host>`, which does
// not resolve. The guard is still right — it is what keeps a profile or an instance
// role out of the credential chain in the first place — but it is defence in depth
// behind `skip_signature` rather than something this test measures.
