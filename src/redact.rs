//! Values that must not be printed, in types that cannot print them.
//!
//! A credential is a [`secrecy::SecretString`], re-exported as [`Secret`]. It has no
//! `Display` and its `Debug` says only `[REDACTED]`, so a `#[derive(Debug)]` on a struct
//! holding one prints everything except the credential, and `{}` on it does not compile.
//! Reach the value with `ExposeSecret::expose_secret`.

use std::fmt;

pub use secrecy::{ExposeSecret, SecretString as Secret};

/// A url as the caller wrote it — the raw parameter, before
/// [`crate::storage::parse_url`], so it may not even be a url. `Debug` prints it cut at
/// the first `?`, which is the most that can be said about an unparsed string.
#[derive(Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct SourceUrl(String);

impl SourceUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The part safe to put in a message: everything before the query string.
    pub fn redacted(&self) -> &str {
        redact(&self.0)
    }
}

impl From<String> for SourceUrl {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Debug for SourceUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.redacted())
    }
}

/// Cut a url string at its query string. For a url that parses,
/// [`crate::storage::parse_url`] strips the query properly; this is for the ones that
/// do not, where there is nothing to strip properly with.
pub fn redact(raw: &str) -> &str {
    // Also the fragment: `#` before `?` means there is no query string at all, and
    // whatever follows is not something to echo either.
    let end = raw.find(['?', '#']).unwrap_or(raw.len());
    raw.get(..end).unwrap_or(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMIsecretKEY";

    /// A derive on a struct holding a credential prints the rest of it and not that.
    #[test]
    fn a_derived_debug_around_a_secret_does_not_print_it() {
        #[derive(Debug)]
        #[expect(dead_code, reason = "the fields exist to be printed by the derive")]
        struct Holder {
            name: &'static str,
            secret: Secret,
        }
        let holder = Holder {
            name: "key",
            secret: Secret::from(SECRET.to_owned()),
        };
        let shown = format!("{holder:?}");
        assert!(shown.contains("key"), "{shown}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert_eq!(holder.secret.expose_secret(), SECRET);
    }

    #[test]
    fn a_source_url_prints_without_its_query_string() {
        let url = SourceUrl::from(format!(
            "s3://bucket/key.parquet?access_key_id=AKIA1&secret_access_key={SECRET}"
        ));
        let shown = format!("{url:?}");
        assert!(shown.contains("s3://bucket/key.parquet"), "{shown}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        // The value itself is untouched; only the printing is.
        assert!(url.as_str().contains(SECRET));
    }

    #[test]
    fn redacting_leaves_a_url_without_a_query_string_alone() {
        assert_eq!(redact("s3://bucket/key.parquet"), "s3://bucket/key.parquet");
        assert_eq!(redact("not-a-url"), "not-a-url");
        assert_eq!(redact(""), "");
        // A fragment is not a query string, and is not ours to echo either.
        assert_eq!(redact("s3://b/k#frag?a=1"), "s3://b/k");
        assert_eq!(redact("?everything"), "");
    }
}
