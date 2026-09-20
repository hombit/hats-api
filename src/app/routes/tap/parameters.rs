//! The parameters a TAP request carries, out of a query string or a form body.
//!
//! Both carriers are the same pairs, so this reads pairs and neither handler knows which it
//! came from. What it enforces is DALI §3: a name is case-insensitive (§3.1), a repeated
//! single-valued parameter is an error (§3.2), and the standard parameters mean what §3.4
//! says they mean.
//!
//! **A name the standards define and this service does not implement is refused; any other
//! name is ignored.** DALI settles the case of a parameter name and is silent on an
//! unrecognised one, so the reference decides it: `taplint` adds a parameter of its own to
//! every query and reports a service that then refuses as breaking it, which is also how
//! every HTTP server treats a query string it has no use for and what the file server here
//! already does. The house rule is not weakened by that — it is about a parameter this
//! service *acts on*, and one nobody defines is not one. A misspelled `MAXREC` does go
//! unread, and unlike a dropped `filters` it is visible in the answer: more rows come back
//! than were asked for, with no overflow marker on them.

use std::collections::BTreeMap;

use axum::extract::rejection::StringRejection;
use axum::http::{HeaderMap, StatusCode, header};

use crate::adql::language;
use crate::app::routes::tap::upload::Uploads;
use crate::error::ApiError;

/// The pairs of a query string or a form body, which are the same encoding.
pub(super) fn pairs(text: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(text.as_bytes())
        .into_owned()
        .collect()
}

/// The pairs of a `POST`, whichever resource it arrived at.
///
/// Both query resources take the same body in the same encoding and refuse the same things,
/// so this is one function rather than a copy apiece: a rule that held on `/sync` and not on
/// `/async` would be a difference no caller could have predicted.
pub(super) fn posted(
    headers: &HeaderMap,
    body: Result<String, StringRejection>,
) -> Result<Vec<(String, String)>, ApiError> {
    // A `multipart/form-data` body is how TAP carries an inline `UPLOAD`, which this
    // service does not implement — so saying that is more use than reading the bytes as
    // form-encoded and reporting that they hold no QUERY.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("multipart/") {
        return Err(ApiError::bad_request(
            "this service takes a POST as application/x-www-form-urlencoded; multipart is \
             for an inline UPLOAD, which it does not implement",
        ));
    }
    // Asked of the rejection's status rather than by naming a variant: the body limit
    // surfaces through whichever buffering error the extractor wraps, and the status is the
    // part of that axum promises.
    let body = body.map_err(|rejection| match rejection.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::body_too_large(
            "the request body is larger than this service accepts; send a shorter statement",
        ),
        _ => ApiError::bad_request("the request body is not text this service can read"),
    })?;
    Ok(pairs(&body))
}

/// Every name this service reads.
const TAKEN: [&str; 10] = [
    "QUERY",
    "LANG",
    "RESPONSEFORMAT",
    "FORMAT",
    "MAXREC",
    "RUNID",
    "REQUEST",
    "UPLOAD",
    "UPLOAD_TYPE",
    "UPLOAD_STORAGE_OPTION",
];

/// The ones a request may give more than once.
///
/// `UPLOAD` is TAP §2.5.2's, which carries several tables in one value and may also be
/// repeated; `UPLOAD_TYPE` is this service's own and follows it. Everything else is
/// single-valued, and DALI §3.2 says a repeat is an error.
const REPEATABLE: [&str; 3] = ["UPLOAD", "UPLOAD_TYPE", "UPLOAD_STORAGE_OPTION"];

/// The one value `REQUEST` may take.
const DO_QUERY: &str = "doQuery";

/// How long a `RUNID` may be. It is a tag in somebody else's job system written into this
/// service's log, so what it needs is a bound rather than a meaning.
const MAX_RUNID: usize = 64;

/// What a `/sync` request asked for.
///
/// `LANG` and `REQUEST` are checked and not carried: nothing below this acts on either, and
/// a field holding a value nobody reads is one somebody will later read the wrong way.
#[derive(Debug)]
pub(super) struct Parameters {
    /// The ADQL statement, unchanged.
    pub query: String,
    /// `RESPONSEFORMAT`, or the `FORMAT` that TAP §2.7.3 makes equivalent to it.
    pub format: Option<String>,
    /// How many rows the answer may hold. `Some(0)` is the metadata and no rows.
    pub maxrec: Option<usize>,
    /// The caller's tag for a larger job, for the log and nowhere else (DALI §3.4.6).
    pub runid: Option<String>,
    /// The tables the request named by url, queried as `TAP_UPLOAD.<name>`.
    pub uploads: Uploads,
}

impl Parameters {
    /// Read one request's pairs.
    pub fn read(pairs: &[(String, String)]) -> Result<Self, ApiError> {
        let mut given: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for (name, value) in pairs {
            let upper = name.to_ascii_uppercase();
            // A name nobody defines is somebody's own: a client's session tag, a proxy's
            // cache-buster, a validator checking that one does not break the query.
            if TAKEN.contains(&upper.as_str()) {
                given.entry(upper).or_default().push(value);
            }
        }
        // DALI §3.2 says a service must answer with an error where a single-valued parameter
        // is given twice. Which of the two values was meant is not something to guess at.
        if let Some((name, values)) = given
            .iter()
            .filter(|(name, _)| !REPEATABLE.contains(&name.as_str()))
            .find(|(_, values)| values.len() > 1)
        {
            return Err(ApiError::bad_request(format!(
                "{name} was given {} times and is single-valued",
                values.len()
            )));
        }
        let one = |name: &str| given.get(name).and_then(|values| values.first()).copied();
        let every = |name: &str| given.get(name).cloned().unwrap_or_default();

        let query = one("QUERY")
            .ok_or_else(|| ApiError::bad_request("QUERY is the statement to run, and is required"))?
            .to_owned();
        language(one("LANG"))?;
        request(one("REQUEST"))?;
        Ok(Self {
            query,
            format: format(one("RESPONSEFORMAT"), one("FORMAT"))?,
            maxrec: maxrec(one("MAXREC"))?,
            runid: runid(one("RUNID"))?,
            uploads: Uploads::read(
                &every("UPLOAD"),
                &every("UPLOAD_TYPE"),
                &every("UPLOAD_STORAGE_OPTION"),
            )?,
        })
    }
}

/// `LANG`, which is required and names the query language.
///
/// Which values are taken is [`crate::adql::language`]'s, so the `lang` of this service's own ADQL
/// route and this parameter answer to the same three.
fn language(asked: Option<&str>) -> Result<(), ApiError> {
    let Some(asked) = asked else {
        return Err(ApiError::bad_request(format!(
            "LANG says which query language the statement is written in, and is required; \
             this service answers {}",
            language::ACCEPTED.join(", ")
        )));
    };
    language::check("LANG", asked)
}

/// `REQUEST`, which TAP 1.1 removed (Appendix A.3) and a 1.0-era client still sends.
///
/// Accepted rather than ignored, and accepted for one value only. The parameter carries
/// nothing this service acts on, so refusing the request over its presence would refuse a
/// query that would otherwise run — while ignoring it would read `REQUEST=doSomethingElse`
/// as a `doQuery`.
fn request(asked: Option<&str>) -> Result<(), ApiError> {
    match asked {
        None => Ok(()),
        Some(value) if value.trim().eq_ignore_ascii_case(DO_QUERY) => Ok(()),
        Some(value) => Err(ApiError::bad_request(format!(
            "REQUEST {value:?} is not something this service does; the /sync resource \
             answers {DO_QUERY}"
        ))),
    }
}

/// `RESPONSEFORMAT`, or `FORMAT`, which TAP §2.7.3 makes equivalent to it.
///
/// "Specifying both FORMAT and RESPONSEFORMAT is undefined", says §2.7.3 — so two that
/// disagree are refused rather than resolved by a rule of this service's own that no caller
/// could have known. Two that agree are one request written twice and are answered.
fn format(response_format: Option<&str>, legacy: Option<&str>) -> Result<Option<String>, ApiError> {
    match (response_format, legacy) {
        (Some(one), Some(other)) if !one.eq_ignore_ascii_case(other) => {
            Err(ApiError::bad_request(format!(
                "RESPONSEFORMAT {one:?} and FORMAT {other:?} are the same parameter and \
                 disagree; send one of them"
            )))
        }
        (Some(asked), _) | (None, Some(asked)) => Ok(Some(asked.to_owned())),
        (None, None) => Ok(None),
    }
}

/// `MAXREC`, the most rows the answer may hold (DALI §3.4.4).
///
/// A number that is not one is refused rather than taken for absent, which would answer the
/// whole of a query the caller asked for ten rows of.
fn maxrec(asked: Option<&str>) -> Result<Option<usize>, ApiError> {
    let Some(asked) = asked else {
        return Ok(None);
    };
    asked.trim().parse::<usize>().map(Some).map_err(|_| {
        ApiError::bad_request(format!(
            "MAXREC {asked:?} is not a row count; it is a whole number, and 0 asks for the \
             columns and no rows"
        ))
    })
}

/// `RUNID`, which goes to the log and nowhere else.
fn runid(asked: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(asked) = asked else {
        return Ok(None);
    };
    if asked.chars().count() > MAX_RUNID {
        return Err(ApiError::bad_request(format!(
            "RUNID is longer than the {MAX_RUNID} characters this service records"
        )));
    }
    Ok(Some(asked.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(pairs: &[(&str, &str)]) -> Result<Parameters, String> {
        let owned = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        Parameters::read(&owned).map_err(|error| error.to_string())
    }

    /// DALI §3.1: a service must treat upper-, lower- and mixed-case names as equal.
    #[test]
    fn a_name_is_read_whatever_its_case() {
        let params = read(&[
            ("query", "SELECT 1"),
            ("Lang", "ADQL"),
            ("MaxRec", "10"),
            ("RUNID", "job-7"),
        ])
        .unwrap();
        assert_eq!(params.query, "SELECT 1");
        assert_eq!(params.maxrec, Some(10));
        assert_eq!(params.runid.as_deref(), Some("job-7"));
    }

    /// DALI §3.2. Which of the two was meant is not something to guess at.
    #[test]
    fn a_single_valued_parameter_given_twice_is_refused() {
        let refused = read(&[
            ("QUERY", "SELECT 1"),
            ("LANG", "ADQL"),
            ("MAXREC", "10"),
            ("maxrec", "1000"),
        ])
        .unwrap_err();
        assert!(refused.contains("MAXREC"), "{refused}");
        assert!(refused.contains("single-valued"), "{refused}");
    }

    /// A name nobody defines is somebody's own — a session tag, a proxy's cache-buster, a
    /// validator checking that one does not break the query — and refusing it is what
    /// `taplint` reports as a service breaking a query that should have run.
    #[test]
    fn a_parameter_nobody_defines_is_ignored_and_a_standard_one_is_refused() {
        let taken = read(&[
            ("QUERY", "SELECT 1"),
            ("LANG", "ADQL"),
            ("DUMMY", "ignore-me"),
            ("MAXRECS", "1"),
        ])
        .unwrap();
        assert_eq!(taken.query, "SELECT 1");
        // And the misspelling really was not read as the parameter it resembles.
        assert_eq!(taken.maxrec, None);

        // The half of a standard parameter this service has not got says so, rather than
        // being ignored into a refusal about a table nobody mentioned.
        let refused = read(&[
            ("QUERY", "SELECT 1"),
            ("LANG", "ADQL"),
            ("UPLOAD", "t,param:doc"),
        ])
        .unwrap_err();
        assert!(refused.contains("inline"), "{refused}");
        assert!(refused.contains("not implement"), "{refused}");
    }

    /// TAP §2.5.2 has `UPLOAD` carry several tables and be repeatable, so a repeat is not
    /// the DALI §3.2 error that a second `MAXREC` is.
    #[test]
    fn upload_may_be_given_more_than_once() {
        let taken = read(&[
            ("QUERY", "SELECT 1"),
            ("LANG", "ADQL"),
            ("UPLOAD", "a,file:///hats/a"),
            ("upload", "b,file:///hats/b"),
        ])
        .unwrap();
        assert_eq!(taken.uploads.names(), ["a", "b"]);
    }

    /// Both are required: TAP §2.7.1 says the client must provide a LANG, and a request
    /// with no statement has nothing to run.
    #[test]
    fn the_statement_and_the_language_are_both_required() {
        let refused = read(&[("LANG", "ADQL")]).unwrap_err();
        assert!(refused.contains("QUERY"), "{refused}");
        let refused = read(&[("QUERY", "SELECT 1")]).unwrap_err();
        assert!(refused.contains("LANG"), "{refused}");
        let refused = read(&[("QUERY", "SELECT 1"), ("LANG", "PQL")]).unwrap_err();
        assert!(
            refused.contains("PQL") && refused.contains("ADQL"),
            "{refused}"
        );
    }

    /// A version after the name is TAP §2.7.1's own spelling, and the case of the value is
    /// not what decides whether a query runs.
    #[test]
    fn the_language_is_named_with_or_without_its_version() {
        for lang in ["ADQL", "adql", "ADQL-2.0", "adql-2.1"] {
            assert!(
                read(&[("QUERY", "SELECT 1"), ("LANG", lang)]).is_ok(),
                "{lang}"
            );
        }
    }

    /// The two names are one parameter, so two values that disagree are refused rather
    /// than one of them being dropped.
    #[test]
    fn format_and_responseformat_are_the_same_parameter() {
        let taken = |pairs: &[(&str, &str)]| read(pairs).unwrap().format;
        let base = [("QUERY", "SELECT 1"), ("LANG", "ADQL")];
        assert_eq!(taken(&base), None);
        assert_eq!(
            taken(&[base[0], base[1], ("FORMAT", "csv")]).as_deref(),
            Some("csv")
        );
        assert_eq!(
            taken(&[
                base[0],
                base[1],
                ("RESPONSEFORMAT", "csv"),
                ("FORMAT", "CSV")
            ])
            .as_deref(),
            Some("csv")
        );
        let refused = read(&[
            base[0],
            base[1],
            ("RESPONSEFORMAT", "votable"),
            ("FORMAT", "csv"),
        ])
        .unwrap_err();
        assert!(refused.contains("disagree"), "{refused}");
    }

    /// A 1.0-era client sends it; the one value it may carry is the one thing this
    /// resource does.
    #[test]
    fn request_is_accepted_for_one_value_only() {
        let base = [("QUERY", "SELECT 1"), ("LANG", "ADQL")];
        assert!(read(&[base[0], base[1], ("REQUEST", "doQuery")]).is_ok());
        let refused = read(&[base[0], base[1], ("REQUEST", "getCapabilities")]).unwrap_err();
        assert!(refused.contains("getCapabilities"), "{refused}");
    }

    /// Taken for absent, a MAXREC that is not a number answers the whole query.
    #[test]
    fn a_maxrec_that_is_not_a_row_count_is_refused() {
        let base = [("QUERY", "SELECT 1"), ("LANG", "ADQL")];
        assert_eq!(
            read(&[base[0], base[1], ("MAXREC", "0")]).unwrap().maxrec,
            Some(0)
        );
        for asked in ["-1", "ten", "1.5", ""] {
            let refused = read(&[base[0], base[1], ("MAXREC", asked)]).unwrap_err();
            assert!(refused.contains("MAXREC"), "{asked}: {refused}");
        }
    }

    #[test]
    fn a_runid_longer_than_the_log_records_is_refused() {
        let base = [("QUERY", "SELECT 1"), ("LANG", "ADQL")];
        let long = "j".repeat(MAX_RUNID + 1);
        let refused = read(&[base[0], base[1], ("RUNID", &long)]).unwrap_err();
        assert!(refused.contains("RUNID"), "{refused}");
    }
}
