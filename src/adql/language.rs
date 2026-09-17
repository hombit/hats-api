//! Which version of ADQL a request says its statement is written in.
//!
//! One list, read by both routes that take a statement: TAP's `LANG` parameter, which TAP
//! §2.7.1 makes mandatory, and the `lang` field of this service's own ADQL route. A caller
//! who moves a query between the two writes the same value.
//!
//! **What is implemented is 2.1, and a request naming 2.0 is answered by it.** The two are
//! one language and 2.1 is the later version of it: everything 2.0 wrote is still valid
//! 2.1, the coordinate system argument of a geometry included — ADQL 2.1 §4.2.5 deprecated
//! that argument and made it optional rather than removing it. So there is nothing for the
//! version to select, and it is checked rather than acted on.
//!
//! What a version does not promise is the optional half of the language. ADQL 2.1 makes
//! `CAST`, `COALESCE`, `ILIKE`, `WITH`, the set operators, `OFFSET` and the geometry each an
//! optional feature a service declares for itself, so the capabilities document is where a
//! client reads what it may write. Naming 2.1 is not a claim to all of it, and no service
//! implements all of it.

use crate::error::ApiError;

/// What the name of the language may be, on its own or with a version after it.
///
/// Written out rather than parsed into a name and a number: three strings are the whole of
/// what is accepted, and a parser would invite `ADQL-2.7` to be answered by whatever the
/// comparison happened to say.
pub const ACCEPTED: [&str; 3] = ["ADQL", "ADQL-2.0", "ADQL-2.1"];

/// The version this service implements, as TAPRegExt writes one.
pub const VERSION: &str = "2.1";

/// The version a 2.0-era client asks for, which [`VERSION`] answers.
pub const EARLIER_VERSION: &str = "2.0";

/// Check what a request said its statement is written in.
///
/// Matched case-insensitively. DALI §3.1 makes only a parameter's *name* insensitive, so
/// this is a choice rather than a requirement: `adql` has exactly one reading, and refusing
/// it would refuse a query every other service answers.
///
/// `field` is what the refusal names — `LANG` over TAP, `lang` in a body — since a caller
/// has to know which of the two they wrote.
pub fn check(field: &str, asked: &str) -> Result<(), ApiError> {
    match ACCEPTED
        .iter()
        .any(|known| known.eq_ignore_ascii_case(asked.trim()))
    {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "{field} {asked:?} is not a language this service answers; it answers {}",
            ACCEPTED.join(", ")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_is_accepted_with_or_without_its_number() {
        for asked in ["ADQL", "adql", "ADQL-2.0", "adql-2.1", " ADQL "] {
            assert!(check("lang", asked).is_ok(), "{asked}");
        }
    }

    /// A language this service does not answer is refused naming what it does, whichever
    /// field carried it.
    #[test]
    fn another_language_is_refused_naming_the_field() {
        let refused = check("LANG", "PQL").unwrap_err().to_string();
        assert!(
            refused.contains("LANG") && refused.contains("PQL"),
            "{refused}"
        );
        assert!(refused.contains("ADQL-2.1"), "{refused}");

        // A version nobody has written, which a comparison would otherwise wave through.
        assert!(check("lang", "ADQL-2.7").is_err());
        assert!(check("lang", "").is_err());
    }
}
