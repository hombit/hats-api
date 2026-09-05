//! The tracing filter, and the one thing it is not allowed to let through.
//!
//! No credential may appear in a log line, including at `debug` and `trace`. That holds
//! for the code in this repository, which never formats a credential anywhere. It does
//! not hold for everything under it: a dependency is free to derive `Debug` on a struct
//! holding a secret and log it, and one does.
//!
//! So the filter is not purely the operator's to choose. Whatever `RUST_LOG` or the
//! config file says, the targets known to print credentials are turned off afterwards,
//! where a later directive for a more specific target wins. An operator debugging a
//! signing problem cannot turn them back on by raising the log level, which is the
//! point: `RUST_LOG=trace` is the first thing anyone reaches for when a request
//! misbehaves, and it must not be the thing that writes a caller's secret to disk.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Directive;

/// Targets that log credentials, and must therefore stay silent.
///
/// Each entry needs a reason, so that it can be removed when the upstream stops doing
/// it rather than being carried forever:
///
/// - `reqsign_core` — the SigV4 signer OpenDAL uses. `reqsign_core::api` logs
///   `Trying credential provider: StaticCredentialProvider { .. }` and
///   `Successfully loaded credential from provider: ..` at `DEBUG`, and the derived
///   `Debug` prints `secret_access_key` and `session_token` in full.
///
/// Silencing a target loses its diagnostics, which is a real cost and the reason this
/// list is a list rather than a wildcard. What a caller needs to see about a failed
/// request — that it was refused, and by whom — is reported by [`crate::error`]
/// regardless.
pub const CREDENTIAL_UNSAFE_TARGETS: &[&str] = &["reqsign_core"];

/// The filter to log through: what was asked for, minus what may not be said.
///
/// `RUST_LOG` wins over `configured` the way it does everywhere else — but neither
/// wins over [`CREDENTIAL_UNSAFE_TARGETS`].
pub fn filter(configured: &str) -> EnvFilter {
    let requested =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(configured));
    silence_credential_loggers(requested)
}

/// Applied separately from [`filter`] so that a test, which builds its own subscriber,
/// gets the same protection the binary does rather than a copy of the list.
#[expect(
    clippy::expect_used,
    reason = "the directives are built from a compile-time list of target names; one \
              that failed to parse would be a silencer that silently did nothing, \
              which is the one outcome this module must not have"
)]
pub fn silence_credential_loggers(filter: EnvFilter) -> EnvFilter {
    CREDENTIAL_UNSAFE_TARGETS
        .iter()
        .fold(filter, |filter, target| {
            let directive: Directive = format!("{target}=off")
                .parse()
                .expect("a target name and a level parse as a directive");
            filter.add_directive(directive)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole module exists for: an operator asking for everything
    /// still does not get these targets.
    #[test]
    fn a_trace_filter_does_not_reenable_a_credential_logger() {
        let filter = silence_credential_loggers(EnvFilter::new("trace"));
        let rendered = filter.to_string();
        for target in CREDENTIAL_UNSAFE_TARGETS {
            assert!(rendered.contains(&format!("{target}=off")), "{rendered}");
        }
    }

    /// And naming the target explicitly does not either, which is the case a plain
    /// "append a directive" would get wrong if a more specific one could win.
    #[test]
    fn naming_the_target_explicitly_does_not_reenable_it() {
        for target in CREDENTIAL_UNSAFE_TARGETS {
            let filter = silence_credential_loggers(EnvFilter::new(format!("{target}=trace")));
            let rendered = filter.to_string();
            assert!(rendered.contains(&format!("{target}=off")), "{rendered}");
            assert!(!rendered.contains(&format!("{target}=trace")), "{rendered}");
        }
    }
}
