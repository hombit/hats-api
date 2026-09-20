//! A job's identifier, which is also the only thing protecting it.
//!
//! UWS §2.2.1 asks one thing of an identifier — that it "should be a legal URI path
//! element" — and says nothing about how it is chosen. What decides that here is §3, whose
//! only access control is authentication and a `403`: this service has no authentication, so
//! a job is visible to whoever holds its id and the id is the capability.
//!
//! **So it is 128 bits from the OS CSPRNG and nothing else.** Never a counter, a timestamp, a
//! UUIDv7 or a ULID: each of those is guessable to within a window, and a window is all an
//! attacker enumerating ids needs. `getrandom` is the system entropy directly rather than a
//! seeded generator, which is the whole of what is wanted for sixteen bytes.
//!
//! Written base64url without padding — 22 characters, every one of them legal in a path
//! segment and in a filename, since a result file is named after its job.

use std::fmt;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::error::ApiError;

/// How many bytes of entropy an id carries.
const BYTES: usize = 16;

/// How many characters that is, base64url encoding three bytes in four.
pub const LENGTH: usize = 22;

/// One job's identifier.
///
/// The string rather than the bytes, because every use of it is textual — a path segment, a
/// filename, a map key, a log field — and decoding it back to bytes would answer nothing.
/// Comparison is on the whole string and is case-sensitive: base64url distinguishes case, so
/// two ids differing only in case are two ids.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(String);

impl JobId {
    /// A new id, from the system's entropy.
    ///
    /// Fallible, and never falls back. An OS that will not give random bytes is one this
    /// service cannot make an unguessable id on, and an id from anywhere weaker is one the
    /// next caller can guess — so the job is not created and the caller gets a `500`.
    pub fn new() -> Result<Self, ApiError> {
        let mut bytes = [0u8; BYTES];
        getrandom::fill(&mut bytes)
            .map_err(|error| ApiError::internal(format!("no entropy for a job id: {error}")))?;
        Ok(Self(URL_SAFE_NO_PAD.encode(bytes)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Read an id out of a url.
///
/// **Checked for shape, and the refusal says nothing about whether the job exists.** A
/// malformed id and an id naming no job answer alike — UWS §2.2's `404` — because telling
/// them apart tells a caller which of their guesses had the right shape.
impl FromStr for JobId {
    type Err = ApiError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let looks_right = text.len() == LENGTH
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
        match looks_right {
            true => Ok(Self(text.to_owned())),
            false => Err(ApiError::not_found("no job of that name")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn an_id_is_twenty_two_url_safe_characters() {
        let id = JobId::new().unwrap();
        assert_eq!(id.as_str().len(), LENGTH);
        assert!(
            id.as_str()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
            "{id}"
        );
        // A path segment and a filename both, since a result is named after its job.
        assert!(!id.as_str().contains(['/', '.', '%', '+']), "{id}");
    }

    /// Not a proof of entropy — nothing here can be — but it does catch the mistake that
    /// matters, which is an id that is constant or seeded the same way every time.
    #[test]
    fn ids_do_not_repeat() {
        let made: HashSet<_> = (0..1000).map(|_| JobId::new().unwrap()).collect();
        assert_eq!(made.len(), 1000);
    }

    #[test]
    fn an_id_survives_being_written_and_read_back() {
        let id = JobId::new().unwrap();
        assert_eq!(id.as_str().parse::<JobId>().unwrap(), id);
    }

    /// The refusal is the one a missing job gets, so the shape of a guess is not reported
    /// back to whoever is guessing.
    #[test]
    fn anything_else_is_refused_as_a_job_that_is_not_there() {
        for text in [
            "",
            "short",
            "../../etc/passwd",
            "a/b",
            &"x".repeat(LENGTH - 1),
            &"x".repeat(LENGTH + 1),
            // The right length, in the alphabet base64url does not use: `+`, `/` and the
            // `=` padding all belong to standard base64 and none is a path segment.
            &format!("{}+", "x".repeat(LENGTH - 1)),
            &format!("{}/", "x".repeat(LENGTH - 1)),
            &format!("{}==", "x".repeat(LENGTH - 2)),
        ] {
            let refused = text.parse::<JobId>().unwrap_err();
            assert_eq!(refused.to_string(), "no job of that name", "{text:?}");
        }
    }
}
