//! What every API body has in common: the refusal of a field a route has not got, the refusal
//! of a body that will not deserialize, and the encoding an answer is asked for in.

use std::collections::BTreeMap;

use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use serde::de::IgnoredAny;

use crate::engine::query::{Predicate, Projection};
use crate::error::ApiError;
use crate::output::dsv;

/// A body's `columns`, lowered. Absent is every column.
///
/// A list because a body has arrays, and a comma between names is a separator a caller would
/// otherwise have to quote around.
pub(in crate::app) fn projection_of(columns: Option<&[String]>) -> Projection<'_> {
    columns.map_or(Projection::All, Projection::Columns)
}

/// A body's `filters`, lowered. Absent is every row.
///
/// One string rather than a list: it is one expression whichever way it is carried, and a list
/// of them would be a second way to write the `AND` the expression already has.
pub(in crate::app) fn predicate_of(filters: Option<&str>) -> Predicate<'_> {
    filters.map_or(Predicate::All, Predicate::Filters)
}

/// What a caller most often gets wrong about `columns` and `filters`, said in the refusal a body
/// that would not deserialize meets. Serde's own message names the field and then quotes the
/// value, which is why it is not the one shown — so the type has to be said here or not at all.
const QUERY_NOTE: &str = "columns is a list of names and filters one condition";

/// An endpoint's field list as the sentence a refusal ends with. The url is the one field a
/// body must carry, and it is first in every list, so the two halves are said apart.
pub(in crate::app) fn takes(fields: &[&str]) -> String {
    let [url, optional @ ..] = fields else {
        return String::new();
    };
    format!("{url}, and optionally {}", optional.join(", "))
}

/// A body carrying a name this endpoint has no field for, refused rather than ignored.
///
/// Each request type collects them into a flattened map and every route refuses them before it
/// does anything: a request this service cannot read as written is not one it may answer part
/// of. A misspelled `filters` dropped would return every row — which the caller cannot tell
/// from a predicate that matched them all, the failure this service keeps finding in a new
/// place.
///
/// The refusal names what the endpoint does take, which is also where a caller who wrote
/// another route's field reads that it is not this one's.
pub(in crate::app) fn refuse_unknown(
    unknown: &BTreeMap<String, IgnoredAny>,
    takes: &str,
) -> Result<(), ApiError> {
    match unknown.is_empty() {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "not accepted here: {}; this route takes {takes}",
            unknown.keys().cloned().collect::<Vec<_>>().join(", ")
        ))),
    }
}

/// What the caller wants back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::app) enum Format {
    Json,
    Parquet,
    /// The XML table format IVOA tools read. Flat columns only — `output::votable` says which
    /// ones are refused and why.
    Votable,
    /// Delimiter-separated values, one encoder and two delimiters — `output::dsv` says what
    /// neither of them can carry. One variant carrying which, rather than two beside each
    /// other, so that every site handling it is handed the kind instead of recovering it.
    Dsv(dsv::Dsv),
}

impl Format {
    /// Every format, in the order a refusal lists them. The default is the first.
    const ALL: [Self; 5] = [
        Self::Json,
        Self::Parquet,
        Self::Votable,
        Self::Dsv(dsv::Dsv::Csv),
        Self::Dsv(dsv::Dsv::Tsv),
    ];

    /// Whether this format writes delimiter-separated values, which is what
    /// `dsv_null_value` applies to and what the other three refuse it for.
    const fn is_dsv(self) -> bool {
        matches!(self, Self::Dsv(_))
    }

    /// The names that do take a `dsv_null_value`, for a refusal to list.
    fn dsv_names() -> String {
        Self::ALL
            .into_iter()
            .filter(|format| format.is_dsv())
            .map(Self::name)
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// The one place a format's name is written. [`Self::parse`] and the list in a
    /// refusal are both derived from it, so a format cannot be renamed in one and not
    /// the others, or added and left unparseable.
    pub(in crate::app) fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Parquet => "parquet",
            Self::Votable => "votable",
            Self::Dsv(kind) => kind.name(),
        }
    }

    /// The default is the caller's mode, not this type's: an API request asks for rows
    /// and gets JSON, while a file-server request asks a parquet file for less of itself
    /// and gets a parquet file back. Adding a query string should not change what media
    /// type a path answers with.
    fn parse(raw: Option<&str>, default: Self) -> Result<Self, ApiError> {
        let Some(raw) = raw else {
            return Ok(default);
        };
        Self::ALL
            .into_iter()
            .find(|format| format.name() == raw)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "unknown format {raw:?}; supported formats are {}",
                    Self::ALL.map(Self::name).join(", ")
                ))
            })
    }
}

/// The encoding an answer is written in, and what writing it takes.
///
/// One type rather than two arguments travelling together, so that the refusal below happens
/// once: a `dsv_null_value` is meaningless for the three formats that have a null of their
/// own, and a parameter this service acts on is honoured or refused, never dropped.
#[derive(Debug, Clone)]
pub(in crate::app) struct Output {
    pub(in crate::app) format: Format,
    /// What a null is written as, where the format is one that has no other way to say it.
    /// The request's own, or [`DEFAULT_DSV_NULL`] where it named none.
    pub(in crate::app) dsv_null: String,
}

/// What a null is written as in `csv` and `tsv` where a request asks for nothing else.
///
/// A constant rather than a configured default, so that the document at `/docs` states one
/// answer rather than whatever this deployment was started with — a caller reading it
/// somewhere else would otherwise be told the wrong thing. Empty is also what `arrow-csv`
/// does on its own, so the two cannot drift.
const DEFAULT_DSV_NULL: &str = "";

impl Output {
    /// The default format is the caller's mode, as [`Format::parse`] has it.
    pub(in crate::app) fn parse(
        raw_format: Option<&str>,
        raw_null: Option<&str>,
        default_format: Format,
    ) -> Result<Self, ApiError> {
        let format = Format::parse(raw_format, default_format)?;
        let dsv_null = match raw_null {
            None => DEFAULT_DSV_NULL.to_owned(),
            Some(value) => {
                if !format.is_dsv() {
                    return Err(ApiError::bad_request(format!(
                        "dsv_null_value applies to {}, and this request asked for {}",
                        Format::dsv_names(),
                        format.name()
                    )));
                }
                dsv::check_null_value(value).map_err(|reason| {
                    ApiError::bad_request(format!("dsv_null_value {value:?}: {reason}"))
                })?;
                value.to_owned()
            }
        };
        Ok(Self { format, dsv_null })
    }
}

/// A body we could not read, said without quoting it back.
///
/// The body carries the credentials, and serde's type errors quote the offending value
/// — `invalid type: string "AKIA…"`. So the message is ours, except for the two serde
/// phrasings that name a key rather than a value. The rest state what was expected,
/// which is what the caller needed anyway.
///
/// Recognising those two by their wording is the weak part: serde could reword them, and
/// the only cost would be a caller who stops being told which key they misspelled. It
/// fails towards the safe message, and the two tests below are what notice.
pub(in crate::app) fn body_error(rejection: &JsonRejection, takes: &str) -> ApiError {
    // What this endpoint takes, and not what any endpoint takes: the two differ, and a
    // sentence naming both leaves the caller to work out which half is theirs. The note is
    // here because this is the message a wrong *type* lands on, and serde's own — which does
    // say the type — quotes the value beside it.
    let shape = format!("expected a JSON object with {takes}; {QUERY_NOTE}");

    // A body past `[limits] max_request_body_bytes` is refused before a byte of it is
    // parsed, so there is no shape to describe and nothing the caller could respell. Asked
    // of the rejection's own status rather than by naming a variant: the limit surfaces
    // through whichever buffering error the extractor wraps, and the status is the part of
    // that axum promises.
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::body_too_large(
            "the request body is larger than this service accepts; send fewer terms, or \
             ask the operator to raise the body limit",
        );
    }

    match rejection {
        // A parse failure quotes the position, not the contents.
        JsonRejection::JsonSyntaxError(error) => {
            ApiError::bad_request(format!("the request body is not valid JSON: {error}"))
        }
        JsonRejection::JsonDataError(error) => {
            let message = error.body_text();
            // `unknown field \`regoin\`` and `missing field \`url\`` name a key. Every
            // other message may quote a value.
            let names_a_key = ["unknown field", "missing field"]
                .iter()
                .any(|prefix| message.contains(prefix));
            match names_a_key {
                true => ApiError::bad_request(message),
                false => ApiError::bad_request(format!("the request body does not fit: {shape}")),
            }
        }
        _ => ApiError::bad_request(format!("{}; {shape}", rejection.body_text())),
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;

    use crate::app::testing::{SECRET, api_only, mounted, post_json, post_parquet, send};
    use crate::config::ApiConfig;
    use crate::engine::query;

    use super::*;

    #[tokio::test]
    async fn a_missing_field_is_named() {
        let (status, body) = post_parquet(serde_json::json!({"filters": "x = 1"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("url"), "{body}");
    }

    /// A body that does not fit is described, never quoted: a mistyped `storage` is
    /// exactly where a secret would be sitting.
    #[tokio::test]
    async fn a_body_that_does_not_fit_is_not_quoted_back() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet",
            "storage": SECRET,
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains(SECRET), "leaked: {body}");
        assert!(body.contains("storage"), "{body}");
    }

    #[tokio::test]
    async fn a_body_that_is_not_json_is_rejected() {
        let (status, body) = send(
            Request::builder()
                .method("POST")
                .uri("/api/v1/simple/parquet")
                .header("content-type", "application/json"),
            Body::from(format!("{{\"url\": \"{SECRET}\"")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains(SECRET), "leaked: {body}");
    }

    #[test]
    fn parses_the_format_parameter() {
        assert_eq!(
            Format::parse(Some("json"), Format::Parquet).unwrap(),
            Format::Json
        );
        assert_eq!(
            Format::parse(Some("parquet"), Format::Json).unwrap(),
            Format::Parquet
        );
        assert_eq!(
            Format::parse(Some("csv"), Format::Json).unwrap(),
            Format::Dsv(dsv::Dsv::Csv)
        );
        assert_eq!(
            Format::parse(Some("tsv"), Format::Json).unwrap(),
            Format::Dsv(dsv::Dsv::Tsv)
        );
        // Absent is the mode's own default, which is why it is passed in.
        assert_eq!(Format::parse(None, Format::Json).unwrap(), Format::Json);
        assert_eq!(
            Format::parse(None, Format::Parquet).unwrap(),
            Format::Parquet
        );
    }

    #[tokio::test]
    async fn unknown_formats_are_rejected() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet", "format": "arrow",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("unknown format"), "{body}");
        assert!(body.contains("json, parquet, votable, csv, tsv"), "{body}");
    }

    /// A field a route has not got is refused, and the refusal names the fields it does take,
    /// so the caller can see what to write instead.
    ///
    /// Dropped, a misspelled `filters` would return every row — which the caller cannot tell
    /// from a predicate that matched them all.
    #[tokio::test]
    async fn a_misspelled_field_is_refused_and_placed() {
        for (route, field, takes) in [
            ("/api/v1/simple/parquet", "column", "columns"),
            ("/api/v1/simple/parquet", "filter", "filters"),
            ("/api/v1/simple/hats", "filter", "filters"),
        ] {
            let (status, body) = post_json(
                api_only(),
                route,
                serde_json::json!({"url": "s3://b/k.parquet", field: "objectid"}),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route} {field}: {body}");
            assert!(
                body.contains(&format!("{field};")),
                "{route} {field}: {body}"
            );
            assert!(body.contains(takes), "{route} {field}: {body}");
        }
    }

    /// `columns` is a list in a body, and the refusal says so when it is handed the comma
    /// separated text a url carries.
    ///
    /// A client moving a url's parameters into a body would otherwise be asking for a column
    /// named `objectid, band` — and the refusal has to name the shape itself, since serde's
    /// own message says it beside the value it quotes, which is the one thing this service
    /// does not repeat back.
    #[tokio::test]
    async fn a_body_takes_a_list_of_columns() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let ask = async |body| {
            post_json(
                mounted(dir.path(), &ApiConfig::default()),
                "/api/v1/simple/parquet",
                body,
            )
            .await
        };

        let (status, body) = ask(serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": ["objectid", "band"],
            "filters": "objectid < 3 AND band = 'g'",
        }))
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        let names = answer["schema"]
            .as_array()
            .unwrap()
            .iter()
            .map(|column| column["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["objectid", "band"], "{body}");
        // The columns came back in the order they were named, and the condition held.
        for row in answer["rows"].as_array().unwrap() {
            assert!(row["objectid"].as_i64().unwrap() < 3, "{row}");
            assert_eq!(row["band"], "g", "{row}");
        }

        // The url's spelling of the same request, sent to the body's route.
        let (status, body) = ask(serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": "objectid, band",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("list of names"), "{body}");
    }

    /// A misspelled field is named too, rather than ignored.
    #[tokio::test]
    async fn an_unknown_field_is_named_rather_than_dropped() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet",
            "wehre": "objectid = 1",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("wehre"), "{body}");
    }
}
