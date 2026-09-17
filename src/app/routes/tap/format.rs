//! What a TAP request may ask its answer to be, and what that answer is labelled.

use crate::app::request::Format;
use crate::error::ApiError;
use crate::output::{dsv, votable};

/// One spelling a request may write, and what it settles.
#[derive(Debug, Clone, Copy)]
pub(super) struct Answering {
    /// As a request writes it — a short name or a media type, DALI §3.4.3 listing both.
    asked: &'static str,
    /// Which encoder writes the answer.
    pub format: Format,
    /// What the answer is labelled with. Not always `asked`: a request naming a media type
    /// is answered with that media type, which §3.4.3 requires in so many words, while a
    /// request naming a short name is answered with the format's own.
    pub content_type: &'static str,
}

/// Every spelling this service answers to, and the one place a format's name lives.
///
/// VOTable is first because it is the default and the one format TAP §2.7.3 requires of
/// every service; `csv` and `tsv` are a SHOULD there and two of the four reference services
/// offer neither. `json` and `parquet` are this service's own and are deliberately absent:
/// what a nested column looks like in them is not settled, and a name published here is one
/// a client may then ask for.
const SPELLINGS: &[Answering] = &[
    Answering {
        asked: "votable",
        format: Format::Votable,
        content_type: votable::CONTENT_TYPE,
    },
    Answering {
        asked: votable::CONTENT_TYPE,
        format: Format::Votable,
        content_type: votable::CONTENT_TYPE,
    },
    // DALI's table gives a VOTable two media types. A client that asked for this one is
    // answered with it — which is the whole reason the label is carried beside the format
    // rather than derived from it.
    Answering {
        asked: "text/xml",
        format: Format::Votable,
        content_type: "text/xml",
    },
    // Every body written here carries its header line, and TAP §2.7.3 says such a body is
    // `text/csv;header=present` — so that is the label whichever of the two was asked for.
    Answering {
        asked: "csv",
        format: Format::Dsv(dsv::Dsv::Csv),
        content_type: dsv::Dsv::Csv.content_type(),
    },
    Answering {
        asked: "text/csv",
        format: Format::Dsv(dsv::Dsv::Csv),
        content_type: dsv::Dsv::Csv.content_type(),
    },
    Answering {
        asked: dsv::Dsv::Csv.content_type(),
        format: Format::Dsv(dsv::Dsv::Csv),
        content_type: dsv::Dsv::Csv.content_type(),
    },
    Answering {
        asked: "tsv",
        format: Format::Dsv(dsv::Dsv::Tsv),
        content_type: dsv::Dsv::Tsv.content_type(),
    },
    Answering {
        asked: dsv::Dsv::Tsv.content_type(),
        format: Format::Dsv(dsv::Dsv::Tsv),
        content_type: dsv::Dsv::Tsv.content_type(),
    },
];

/// What the request asked for, or VOTable where it asked for nothing.
///
/// **A format this service has not got is refused**, which is DALI §3.4.3: a service
/// "should fail where the RESPONSEFORMAT parameter specifies a format not supported". The
/// alternative is a body in some other format than the one the client's parser was chosen
/// for, which it reads as corrupt rather than as a refusal.
///
/// The spelling is matched case-insensitively. DALI §3.1 makes only parameter *names*
/// insensitive, so this is a choice rather than a requirement: `VOTable` and `votable` have
/// one reading, and refusing one of them would refuse a query every other service answers.
pub(super) fn resolve(asked: Option<&str>) -> Result<Answering, ApiError> {
    let Some(asked) = asked else {
        // The first entry, which is VOTable — TAP §2.7.3's default where neither FORMAT nor
        // RESPONSEFORMAT was written.
        return SPELLINGS
            .first()
            .copied()
            .ok_or_else(|| ApiError::internal("this service has no output format"));
    };
    let asked = asked.trim();
    SPELLINGS
        .iter()
        .find(|spelling| spelling.asked.eq_ignore_ascii_case(asked))
        .copied()
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "RESPONSEFORMAT {asked:?} is not a format this service writes; it writes {}",
                names().join(", ")
            ))
        })
}

/// The short names, for a refusal to list.
///
/// The media types are left out: they name the same formats, and a list of every spelling
/// is longer to read without saying anything more.
pub(super) fn names() -> Vec<&'static str> {
    declared().into_iter().map(|(alias, _)| alias).collect()
}

/// Each format once, as the alias and the media type the capabilities document declares.
///
/// Derived from the same list the query resource reads, so the document cannot offer a
/// format a request would then be refused for asking about — which is the failure a second
/// list would produce and the reason there is not one.
pub(super) fn declared() -> Vec<(&'static str, &'static str)> {
    SPELLINGS
        .iter()
        .filter(|spelling| !spelling.asked.contains('/'))
        .map(|spelling| (spelling.asked, spelling.content_type))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_asked_for_is_a_votable() {
        let answering = resolve(None).unwrap();
        assert_eq!(answering.format, Format::Votable);
        assert_eq!(answering.content_type, votable::CONTENT_TYPE);
    }

    /// A short name and a media type both select the format; the media type a request
    /// named is also what the answer carries.
    #[test]
    fn a_format_is_named_either_way() {
        for asked in ["votable", "VOTable", "application/x-votable+xml"] {
            let answering = resolve(Some(asked)).unwrap();
            assert_eq!(answering.format, Format::Votable, "{asked}");
            assert_eq!(answering.content_type, votable::CONTENT_TYPE, "{asked}");
        }
        // DALI's second media type for the same format, answered as it was asked for.
        assert_eq!(resolve(Some("text/xml")).unwrap().content_type, "text/xml");

        for asked in ["csv", "text/csv", "text/csv;header=present"] {
            let answering = resolve(Some(asked)).unwrap();
            assert_eq!(answering.format, Format::Dsv(dsv::Dsv::Csv), "{asked}");
            // The header line is always written, so this is the label either way.
            assert_eq!(answering.content_type, "text/csv;header=present", "{asked}");
        }
        assert_eq!(
            resolve(Some("tsv")).unwrap().format,
            Format::Dsv(dsv::Dsv::Tsv)
        );
    }

    /// DALI §3.4.3 says to fail rather than answer in some other format, which is a body
    /// the client's parser reads as corrupt instead of as a refusal.
    #[test]
    fn a_format_this_service_has_not_got_is_refused() {
        for asked in [
            "fits",
            "html",
            "text",
            "application/fits",
            "json",
            "parquet",
        ] {
            let refused = resolve(Some(asked)).unwrap_err().to_string();
            assert!(refused.contains(asked), "{asked}: {refused}");
            assert!(refused.contains("votable"), "{asked}: {refused}");
        }
    }

    #[test]
    fn the_names_are_the_short_ones() {
        assert_eq!(names(), ["votable", "csv", "tsv"]);
    }
}
