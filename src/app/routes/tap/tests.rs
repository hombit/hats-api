//! The two TAP query resources end to end: the parameters, the documents, the row bound,
//! and a job from its creation to its rows.
//!
//! Both are here rather than in a file each because almost everything under them is one
//! path — the same statement, the same parameters, the same format table — so what these
//! are really checking is the little that differs, and the two sitting side by side is
//! what makes that visible.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use tower::ServiceExt;

use crate::app::routes::tap::Jobs;
use crate::app::service::{Service, router, router_with};
use crate::app::testing::{serving, with_tap};
use crate::config::{
    ApiConfig, AsyncConfig, LimitsConfig, ServerConfig, TapConfig, TapTableConfig,
};
use crate::hats;

/// The catalog fixture, published as `sky.objects`.
///
/// One mount at `/`, one table over it, which is the smallest deployment that has a TAP
/// surface at all.
fn published(dir: &std::path::Path, limits: &LimitsConfig) -> Service {
    let tap = TapConfig {
        tables: vec![TapTableConfig {
            name: "sky.objects".to_owned(),
            path: "/".to_owned(),
        }],
        jobs: Default::default(),
    };
    with_tap(
        serving(dir),
        &ApiConfig::default(),
        limits,
        &ServerConfig::default(),
        &tap,
    )
}

/// A `GET` to `/tap/sync` with these parameters.
async fn ask(service: Service, pairs: &[(&str, &str)]) -> (StatusCode, String, String) {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish();
    send(
        service,
        Request::builder()
            .uri(format!("/api/v1/tap/sync?{query}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// The same parameters in a form body, which is what a long statement is sent as.
async fn post(service: Service, pairs: &[(&str, &str)]) -> (StatusCode, String, String) {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish();
    send(
        service,
        Request::builder()
            .method("POST")
            .uri("/api/v1/tap/sync")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
}

/// The status, the content type, and the body.
async fn send(service: Service, request: Request<Body>) -> (StatusCode, String, String) {
    let response = router(service).oneshot(request).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        content_type,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

/// A statement naming the published table, answered as a VOTable on both verbs.
///
/// TAP §2.1 has `/sync` answer `GET` and `POST` alike, and a client that cannot fit its
/// statement in a url uses the second — so the two returning the same document is the whole
/// of what makes that choice free.
#[tokio::test]
async fn a_statement_is_answered_on_both_verbs() {
    let dir = hats::query::tests::fixture(true);
    let pairs = [
        ("QUERY", "SELECT id FROM sky.objects ORDER BY id"),
        ("LANG", "ADQL"),
    ];

    let (status, content_type, got) =
        ask(published(dir.path(), &LimitsConfig::default()), &pairs).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(content_type, "application/x-votable+xml");
    assert!(
        got.contains("<INFO name=\"QUERY_STATUS\" value=\"OK\"/>"),
        "{got}"
    );
    assert!(got.contains("<FIELD name=\"id\" ID=\"id\""), "{got}");
    assert!(got.contains("<TR>"), "{got}");

    let (status, _, posted) = post(published(dir.path(), &LimitsConfig::default()), &pairs).await;
    assert_eq!(status, StatusCode::OK, "{posted}");
    assert_eq!(posted, got);
}

/// A catalog the request names by url, queried as `TAP_UPLOAD.<name>`.
///
/// This is what a TAP client has no other way to ask for: the `/adql` body declares its own
/// tables, and until now `/sync` answered only what the operator published.
#[tokio::test]
async fn a_catalog_named_by_url_is_queried_as_an_upload() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT TOP 3 id FROM TAP_UPLOAD.mine ORDER BY id"),
            ("LANG", "ADQL"),
            ("UPLOAD", "mine,file:///"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(content_type, "application/x-votable+xml");
    assert!(body.contains("<FIELD name=\"id\" ID=\"id\""), "{body}");
    assert_eq!(body.matches("<TR>").count(), 3, "{body}");
}

/// The other kind of url, and the one `UPLOAD_TYPE` need not name: a file matching the
/// data-file globs is a parquet file, and a directory is a catalog.
#[tokio::test]
async fn an_upload_is_a_parquet_file_or_a_catalog() {
    let dir = hats::query::tests::fixture(true);
    let file = "file:///dataset/Norder=3/Dir=0/Npix=64.parquet";
    let ask_with = async |pairs: Vec<(&str, &str)>| {
        ask(published(dir.path(), &LimitsConfig::default()), &pairs).await
    };

    let (status, _, body) = ask_with(vec![
        ("QUERY", "SELECT TOP 2 id FROM TAP_UPLOAD.one ORDER BY id"),
        ("LANG", "ADQL"),
        ("UPLOAD", &format!("one,{file}")),
    ])
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 2, "{body}");

    // Named outright, both ways round, which is what a url the guess would read the other
    // way needs.
    let (status, _, body) = ask_with(vec![
        ("QUERY", "SELECT TOP 2 id FROM TAP_UPLOAD.one"),
        ("LANG", "ADQL"),
        ("UPLOAD", &format!("one,{file}")),
        ("UPLOAD_TYPE", "one,parquet"),
    ])
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A directory called a parquet file says what a parquet url looks like, rather than
    // failing somewhere inside a reader.
    let (status, _, body) = ask_with(vec![
        ("QUERY", "SELECT TOP 2 id FROM TAP_UPLOAD.one"),
        ("LANG", "ADQL"),
        ("UPLOAD", "one,file:///"),
        ("UPLOAD_TYPE", "one,parquet"),
    ])
    .await;
    assert!(status.is_client_error(), "{status} {body}");
    assert!(body.contains("data file"), "{body}");
}

/// The storage options a url needs reach the store that opens it.
///
/// A `file://` url takes none, so an option meant for another backend is refused by the
/// same check every other route's options go through — which is what shows the parameter
/// is read and handed on rather than parsed and dropped.
#[tokio::test]
async fn upload_storage_options_reach_the_store() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT TOP 1 id FROM TAP_UPLOAD.mine"),
            ("LANG", "ADQL"),
            ("UPLOAD", "mine,file:///"),
            ("UPLOAD_STORAGE_OPTION", "mine,region,us-east-1"),
        ],
    )
    .await;
    assert!(status.is_client_error(), "{status} {body}");
    assert!(body.contains("storage options"), "{body}");
}

/// Two uploads at one authority is DataFusion's one store for it, so different credentials
/// for the two would leave one running under the other's — refused before either is opened
/// for real, naming both uploads and neither secret.
#[tokio::test]
async fn two_uploads_at_one_authority_with_different_credentials_are_refused() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT one.id FROM TAP_UPLOAD.one, TAP_UPLOAD.two"),
            ("LANG", "ADQL"),
            ("UPLOAD", "one,s3://bucket/a.parquet"),
            ("UPLOAD", "two,s3://bucket/b.parquet"),
            ("UPLOAD_STORAGE_OPTION", "one,access_key_id,AKIA1"),
            (
                "UPLOAD_STORAGE_OPTION",
                "one,secret_access_key,first-secret",
            ),
            ("UPLOAD_STORAGE_OPTION", "two,access_key_id,AKIA1"),
            (
                "UPLOAD_STORAGE_OPTION",
                "two,secret_access_key,second-secret",
            ),
        ],
    )
    .await;
    assert!(status.is_client_error(), "{status} {body}");
    assert!(body.contains("one"), "{body}");
    assert!(body.contains("two"), "{body}");
    assert!(!body.contains("first-secret"), "leaked: {body}");
    assert!(!body.contains("second-secret"), "leaked: {body}");
}

/// A table nobody published is a refusal naming what is published, rather than a planner
/// message about a relation.
#[tokio::test]
async fn a_table_this_service_does_not_publish_is_refused() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id FROM gaia_dr3.gaia_source"),
            ("LANG", "ADQL"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    // The refusal is a document a TAP client can read, not this service's own JSON.
    assert_eq!(content_type, "application/x-votable+xml");
    assert!(body.contains("value=\"ERROR\""), "{body}");
    assert!(body.contains("gaia_dr3.gaia_source"), "{body}");
    assert!(body.contains("sky.objects"), "{body}");
}

/// Every refusal is a document with a `QUERY_STATUS` of ERROR in it, whatever went wrong.
/// A TAP client reads the document and has nothing to tell a user where it finds none.
#[tokio::test]
async fn every_refusal_is_an_error_document() {
    let dir = hats::query::tests::fixture(true);
    for pairs in [
        // No statement, and no language.
        [("LANG", "ADQL"), ("", "")].as_slice(),
        [("QUERY", "SELECT id FROM sky.objects")].as_slice(),
        // A statement that will not parse, and one that names a column the catalog has not.
        [("QUERY", "SELECT FROM WHERE"), ("LANG", "ADQL")].as_slice(),
        [
            ("QUERY", "SELECT no_such_column FROM sky.objects"),
            ("LANG", "ADQL"),
        ]
        .as_slice(),
        // A language and a format this service does not answer.
        [("QUERY", "SELECT id FROM sky.objects"), ("LANG", "PQL")].as_slice(),
        [
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "fits"),
        ]
        .as_slice(),
        // The half of UPLOAD this service does not implement: a table in the request.
        [
            ("QUERY", "SELECT id FROM TAP_UPLOAD.t"),
            ("LANG", "ADQL"),
            ("UPLOAD", "t,param:doc"),
        ]
        .as_slice(),
        // A statement naming an upload the request did not make.
        [("QUERY", "SELECT id FROM TAP_UPLOAD.t"), ("LANG", "ADQL")].as_slice(),
    ] {
        let asked = pairs.iter().filter(|(name, _)| !name.is_empty());
        let (status, content_type, body) = ask(
            published(dir.path(), &LimitsConfig::default()),
            &asked.copied().collect::<Vec<_>>(),
        )
        .await;
        assert!(status.is_client_error(), "{pairs:?}: {status} {body}");
        assert_eq!(content_type, "application/x-votable+xml", "{pairs:?}");
        assert!(
            body.contains("<INFO name=\"QUERY_STATUS\" value=\"ERROR\">"),
            "{pairs:?}: {body}"
        );
        // And it says what was wrong, an empty error document being a conforming one and
        // a useless one.
        assert!(body.len() > 200, "{pairs:?}: {body}");
    }
}

/// DALI §4.4.1's marker, after the table because the `OK` before it was written first.
#[tokio::test]
async fn an_answer_cut_by_maxrec_says_so_after_the_table() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    let (status, _, body) = ask(
        service(),
        &[
            ("QUERY", "SELECT id FROM sky.objects ORDER BY id"),
            ("LANG", "ADQL"),
            ("MAXREC", "2"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 2, "{body}");
    let (before, after) = body.split_once("</TABLE>").unwrap();
    assert!(before.contains("value=\"OK\""), "{body}");
    assert!(after.contains("value=\"OVERFLOW\""), "{body}");

    // A MAXREC larger than the answer is not a truncation, so no marker.
    let (status, _, whole) = ask(
        service(),
        &[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("MAXREC", "100000"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{whole}");
    assert!(!whole.contains("OVERFLOW"), "{whole}");
}

/// TAP §2.7.4: the truncation happens "after any limitations imposed by the query", so a
/// `TOP` smaller than `MAXREC` is what decides and the answer is not a truncated one.
#[tokio::test]
async fn maxrec_and_top_are_whichever_is_smaller() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    let asked = async |top: usize, maxrec: usize| {
        let query = format!("SELECT TOP {top} id FROM sky.objects ORDER BY id");
        let maxrec = maxrec.to_string();
        ask(
            service(),
            &[("QUERY", &query), ("LANG", "ADQL"), ("MAXREC", &maxrec)],
        )
        .await
    };

    // TOP is the smaller: its rows, and no overflow — the answer really is the whole of
    // what the query asked for.
    let (status, _, body) = asked(2, 10).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 2, "{body}");
    assert!(!body.contains("OVERFLOW"), "{body}");

    // MAXREC is the smaller: its rows, and the marker.
    let (status, _, body) = asked(10, 3).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 3, "{body}");
    assert!(body.contains("OVERFLOW"), "{body}");
}

/// DALI §3.4.4: "metadata, no results, and an overflow indicator". TOPCAT sends it to
/// learn a table's columns, so the answer has to carry them and cost nothing.
#[tokio::test]
async fn maxrec_zero_is_the_columns_and_no_rows() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id, ra, dec FROM sky.objects"),
            ("LANG", "ADQL"),
            ("MAXREC", "0"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<FIELD name=\"id\""), "{body}");
    assert!(body.contains("<FIELD name=\"ra\""), "{body}");
    assert!(!body.contains("<TR>"), "{body}");
    // The indicator is there whether or not the query would have matched anything, which
    // is what TAP §2.7.4 says it does not mean.
    assert!(body.contains("value=\"OVERFLOW\""), "{body}");
}

/// The operator's `max_rows` is the other bound, and on this resource reaching it is a
/// truncation that says so rather than the refusal every other route answers with. It is
/// the marker that makes the difference: there is one here and nowhere else.
#[tokio::test]
async fn the_operators_row_bound_is_a_truncation_here() {
    let dir = hats::query::tests::fixture(true);
    let limits = LimitsConfig {
        max_rows: 2,
        ..LimitsConfig::default()
    };
    let (status, _, body) = ask(
        published(dir.path(), &limits),
        &[
            ("QUERY", "SELECT id FROM sky.objects ORDER BY id"),
            ("LANG", "ADQL"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 2, "{body}");
    assert!(body.contains("value=\"OVERFLOW\""), "{body}");

    // And a MAXREC over it does not raise it.
    let (status, _, body) = ask(
        published(dir.path(), &limits),
        &[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("MAXREC", "1000"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 2, "{body}");
}

/// The two formats beside VOTable, each labelled with the media type the standards name.
#[tokio::test]
async fn the_delimited_formats_are_answered_and_labelled() {
    let dir = hats::query::tests::fixture(true);
    for (asked, content_type, separator) in [
        ("csv", "text/csv;header=present", ","),
        ("tsv", "text/tab-separated-values", "\t"),
    ] {
        let (status, got, body) = ask(
            published(dir.path(), &LimitsConfig::default()),
            &[
                ("QUERY", "SELECT TOP 2 id, ra FROM sky.objects ORDER BY id"),
                ("LANG", "ADQL"),
                ("RESPONSEFORMAT", asked),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{asked}: {body}");
        assert_eq!(got, content_type, "{asked}");
        assert!(
            body.starts_with(&format!("id{separator}ra")),
            "{asked}: {body}"
        );
    }
}

/// A delimited body has nowhere to put the overflow marker, so the fact is stated in a
/// header rather than nowhere at all.
#[tokio::test]
async fn a_truncated_delimited_answer_says_so_in_a_header() {
    let dir = hats::query::tests::fixture(true);
    let request = Request::builder()
        .uri(
            "/api/v1/tap/sync?LANG=ADQL&RESPONSEFORMAT=csv&MAXREC=1\
             &QUERY=SELECT+id+FROM+sky.objects",
        )
        .body(Body::empty())
        .unwrap();
    let response = router(published(dir.path(), &LimitsConfig::default()))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-hats-overflow"], "true");
}

/// A `GET` to one of the resources that describe the service.
async fn fetch(service: Service, path: &str) -> (StatusCode, String, String) {
    send(
        service,
        Request::builder()
            .uri(format!("/api/v1/tap{path}"))
            .header(header::HOST, "data.example.org")
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// A client asks this before anything else, and a registry asks it on a schedule.
#[tokio::test]
async fn availability_says_the_service_is_up() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, body) = fetch(
        published(dir.path(), &LimitsConfig::default()),
        "/availability",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(content_type, "text/xml");
    assert!(
        body.contains("<vosi:available>true</vosi:available>"),
        "{body}"
    );
}

/// A client picks its interface, its language and its format out of this document, so
/// what is in it has to be there — and what is not there must not be in it.
#[tokio::test]
async fn capabilities_declare_what_is_there_and_nothing_else() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    let (status, content_type, body) = fetch(service(), "/capabilities").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(content_type, "text/xml");

    for declared in [
        "ivo://ivoa.net/std/TAP",
        "ivo://ivoa.net/std/VOSI#capabilities",
        "ivo://ivoa.net/std/VOSI#availability",
        "ivo://ivoa.net/std/VOSI#tables-1.1",
    ] {
        assert!(
            body.contains(declared),
            "{declared} is not declared: {body}"
        );
    }
    assert!(body.contains("<name>ADQL</name>"), "{body}");
    // Both versions, because a LANG of either is answered.
    assert!(body.contains(">2.1</version>"), "{body}");
    assert!(body.contains(">2.0</version>"), "{body}");
    // The optional features that answer, and only the forms of them that do.
    for declared in [
        "#features-adqlgeo",
        "#features-adql-sets",
        "<form>CONTAINS</form>",
        "<form>OFFSET</form>",
        "#features-udf",
    ] {
        assert!(
            body.contains(declared),
            "{declared} is not declared: {body}"
        );
    }
    for absent in [
        "<form>BOX</form>",
        "<form>POLYGON</form>",
        "#features-adql-unit",
    ] {
        assert!(!body.contains(absent), "{absent} is declared: {body}");
    }
    // Every format the query resource answers, and no other.
    for mime in [
        "application/x-votable+xml",
        "text/csv;header=present",
        "text/tab-separated-values",
    ] {
        assert!(body.contains(mime), "{mime} is not declared: {body}");
    }
    assert!(!body.to_lowercase().contains("fits"), "{body}");
    // Not implemented, so not offered: a client told about one has no way back.
    assert!(!body.contains("uploadMethod"), "{body}");
    assert!(!body.contains("async"), "{body}");
    // The row bound a client reads MAXREC against.
    assert!(body.contains("<hard unit=\"row\">1000000</hard>"), "{body}");

    // The access urls are where this request arrived, and each of them answers.
    assert!(
        body.contains("<accessURL use=\"base\">http://data.example.org/api/v1/tap</accessURL>"),
        "{body}"
    );
    for resource in ["availability", "capabilities", "tables"] {
        let declared = format!("http://data.example.org/api/v1/tap/{resource}");
        assert!(
            body.contains(&declared),
            "{declared} is not declared: {body}"
        );
        let (status, _, _) = fetch(service(), &format!("/{resource}")).await;
        assert_eq!(status, StatusCode::OK, "{declared} is declared and absent");
    }
}

/// The document TOPCAT fills its table browser from: the published tables, their columns,
/// and what is known about each.
#[tokio::test]
async fn tables_lists_what_is_published_with_its_columns() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, body) =
        fetch(published(dir.path(), &LimitsConfig::default()), "/tables").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(content_type, "text/xml");
    assert!(body.contains("<name>sky.objects</name>"), "{body}");
    // TAP_SCHEMA is a table a client queries, so it is published like any other.
    assert!(body.contains("<name>TAP_SCHEMA.columns</name>"), "{body}");
    assert!(body.contains("<name>ra</name>"), "{body}");
    // How a client told nothing else finds the position.
    assert!(body.contains("<ucd>pos.eq.ra;meta.main</ucd>"), "{body}");
    assert!(body.contains("<unit>deg</unit>"), "{body}");
    assert!(body.contains("<flag>indexed</flag>"), "{body}");
    assert!(body.contains("VOTableType"), "{body}");
}

/// `detail=min` is the list without the columns, and one table by name is how a client
/// that has the name already avoids fetching every column of every table.
#[tokio::test]
async fn the_tables_resource_answers_less_when_asked() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());

    let (status, _, body) = fetch(service(), "/tables?detail=min").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<name>sky.objects</name>"), "{body}");
    assert!(!body.contains("<column>"), "{body}");

    let (status, _, body) = fetch(service(), "/tables/sky.objects").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<name>ra</name>"), "{body}");
    assert!(!body.contains("TAP_SCHEMA"), "{body}");

    // A name nobody published, and a `detail` this resource does not know — answered with
    // the whole document, the second would be a caller who asked for less and cannot tell
    // that from being ignored.
    let (status, _, _) = fetch(service(), "/tables/nope.nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = fetch(service(), "/tables?detail=everything").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The five tables of TAP §4 answer a query, and they describe this service's own tables
/// as well as the published ones — a client queries `TAP_SCHEMA.columns` before it knows
/// any other name.
#[tokio::test]
async fn tap_schema_is_queryable() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    for table in ["schemas", "tables", "columns", "keys", "key_columns"] {
        let (status, _, body) = ask(
            service(),
            &[
                ("QUERY", &format!("SELECT * FROM TAP_SCHEMA.{table}")),
                ("LANG", "ADQL"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{table}: {body}");
        assert!(body.contains("value=\"OK\""), "{table}: {body}");
    }

    // The published table is described, and so is TAP_SCHEMA itself.
    let (status, _, body) = ask(
        service(),
        &[
            ("QUERY", "SELECT table_name FROM TAP_SCHEMA.tables"),
            ("LANG", "ADQL"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<TD>sky.objects</TD>"), "{body}");
    assert!(body.contains("<TD>TAP_SCHEMA.columns</TD>"), "{body}");
}

/// A name written without quotes is case-insensitive, which is ADQL §2.1.3 — for the fixed
/// names a client hardcodes and for the ones an operator published alike.
#[tokio::test]
async fn a_table_name_is_read_whatever_its_case() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    for spelling in [
        "TAP_SCHEMA.tables",
        "tap_schema.tables",
        "Tap_Schema.Tables",
    ] {
        let (status, _, body) = ask(
            service(),
            &[
                ("QUERY", &format!("SELECT table_name FROM {spelling}")),
                ("LANG", "ADQL"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{spelling}: {body}");
    }
    for spelling in ["sky.objects", "SKY.OBJECTS", "Sky.Objects"] {
        let (status, _, body) = ask(
            service(),
            &[
                ("QUERY", &format!("SELECT TOP 1 id FROM {spelling}")),
                ("LANG", "ADQL"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{spelling}: {body}");
    }
}

/// A name `TAP_SCHEMA.columns` publishes is a name a query can be built out of, which is
/// what TAP §4.3 asks the published name to be — and what a client's table browser
/// amounts to.
#[tokio::test]
async fn a_published_column_name_can_be_selected() {
    let dir = hats::query::tests::fixture(true);
    let service = || published(dir.path(), &LimitsConfig::default());
    let (status, _, body) = ask(
        service(),
        &[
            (
                "QUERY",
                "SELECT column_name FROM TAP_SCHEMA.columns \
                 WHERE table_name = 'sky.objects'",
            ),
            ("LANG", "ADQL"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body
        .split("<TR><TD>")
        .skip(1)
        .filter_map(|row| row.split("</TD>").next())
        .collect();
    assert!(!names.is_empty(), "{body}");

    let (status, _, selected) = ask(
        service(),
        &[
            (
                "QUERY",
                &format!("SELECT TOP 1 {} FROM sky.objects", names.join(", ")),
            ),
            ("LANG", "ADQL"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "asked for {names:?}: {selected}");
    assert_eq!(
        selected.matches("<FIELD ").count(),
        names.len(),
        "{selected}"
    );
}

/// A parameter nobody defines is ignored, which is what every HTTP server does with a
/// query string it has no use for — and what `taplint` checks by adding one of its own to
/// a query it expects to still run.
#[tokio::test]
async fn a_parameter_this_service_does_not_read_does_not_break_the_query() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT TOP 1 id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("DUMMY", "ignore-me"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.matches("<TR>").count(), 1, "{body}");
}

/// A deployment that published nothing has no TAP surface at all, rather than one that
/// refuses every query.
#[tokio::test]
async fn a_service_with_no_published_table_has_no_tap_resource() {
    let dir = hats::query::tests::fixture(true);
    let service = with_tap(
        serving(dir.path()),
        &ApiConfig::default(),
        &LimitsConfig::default(),
        &ServerConfig::default(),
        &TapConfig::default(),
    );
    let (status, _, _) = ask(service, &[("QUERY", "SELECT 1"), ("LANG", "ADQL")]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ------------------------------------------------------------------ `/tap/async`

/// A service and the job resource behind it, kept together.
///
/// One `Jobs` across every request, because the store is in it: a router built fresh per
/// request would forget each job the moment it was made.
struct Jobbed {
    service: Service,
    jobs: Arc<Jobs>,
}

impl Jobbed {
    fn new(dir: &std::path::Path) -> Self {
        let limits = LimitsConfig::default();
        let service = published(dir, &limits);
        let jobs =
            Jobs::new(service.clone(), &limits, &AsyncConfig::default()).expect("the job resource");
        Self {
            service,
            jobs: Arc::new(jobs),
        }
    }

    async fn send(&self, request: Request<Body>) -> http::Response<Body> {
        router_with(self.service.clone(), Some(Arc::clone(&self.jobs)))
            .oneshot(request)
            .await
            .expect("a response")
    }

    async fn get(&self, uri: &str) -> http::Response<Body> {
        self.send(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
    }

    /// Post form parameters, the way every TAP client sends them.
    async fn form(&self, uri: &str, pairs: &[(&str, &str)]) -> http::Response<Body> {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(pairs)
            .finish();
        self.send(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }

    /// Create a job and return the path of the one that was made.
    ///
    /// The `303` and its `Location` are asserted here rather than in a test of their own,
    /// so that every job below is one UWS §2.2.3.1 was satisfied by.
    async fn submit(&self, pairs: &[(&str, &str)]) -> String {
        let response = self.form("/api/v1/tap/async", pairs).await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let location = response.headers()[header::LOCATION]
            .to_str()
            .expect("a text location")
            .to_owned();
        let path = location
            .split_once("/api/v1")
            .map(|(_, rest)| format!("/api/v1{rest}"))
            .expect("a location under the api prefix");
        assert!(path.starts_with("/api/v1/tap/async/"), "{path}");
        path
    }

    /// Poll the phase until the job stops, so nothing here depends on how long it took.
    async fn settled(&self, job: &str) -> String {
        for _ in 0..600 {
            let phase = text_of(self.get(&format!("{job}/phase")).await).await;
            if ["COMPLETED", "ERROR", "ABORTED"].contains(&phase.as_str()) {
                return phase;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the job never finished");
    }
}

async fn text_of(response: http::Response<Body>) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&body).into_owned()
}

/// A job runs and its rows come back from the resource TAP names.
///
/// The headers are half of what is being checked. A result is a file rather than a body
/// built in memory precisely so that it carries a length and admits a range, and a client
/// collecting a large answer is the reason — so a regression there is a regression in the
/// thing the design was for, not a cosmetic one.
#[tokio::test]
async fn a_job_answers_its_rows_as_a_file() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id FROM sky.objects ORDER BY id"),
            ("LANG", "ADQL"),
            ("PHASE", "RUN"),
        ])
        .await;

    assert_eq!(harness.settled(&job).await, "COMPLETED");
    let document = text_of(harness.get(&job).await).await;
    assert!(
        document.contains("<uws:phase>COMPLETED</uws:phase>"),
        "{document}"
    );
    assert!(document.contains("/results/result\"/>"), "{document}");

    let response = harness.get(&format!("{job}/results/result")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(headers[header::CONTENT_TYPE], "application/x-votable+xml");
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert!(headers.contains_key(header::CONTENT_LENGTH));
    assert_eq!(headers["x-hats-overflow"], "false");
    let rows = text_of(response).await;
    assert!(rows.contains("<FIELD name=\"id\" ID=\"id\""), "{rows}");
    assert!(rows.contains("<TR>"), "{rows}");
}

/// The half a body built in memory could not offer.
///
/// A client resuming a large download asks for a range, and a `200` carrying the whole file
/// where a `206` was asked for is the wrong-answer shape this service refuses everywhere
/// else — a reader that trusts the range gets the head of the file and nothing says so.
#[tokio::test]
async fn a_job_result_is_served_by_range() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("PHASE", "RUN"),
        ])
        .await;
    assert_eq!(harness.settled(&job).await, "COMPLETED");

    let whole = text_of(harness.get(&format!("{job}/results/result")).await).await;
    let response = harness
        .send(
            Request::builder()
                .uri(format!("{job}/results/result"))
                .header(header::RANGE, "bytes=0-19")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let part = text_of(response).await;
    assert_eq!(part.len(), 20, "{part:?}");
    assert!(whole.starts_with(&part), "{part:?}");
}

/// TAP §2.7: a parameter is enforced when the query runs, not when the job is made.
///
/// So a submission with no `QUERY` is a job, and the refusal is that job's. Getting this
/// the other way round — refusing the `POST` — breaks the one workflow the rule exists for,
/// which is creating a job `PENDING` and posting its parameters one at a time.
#[tokio::test]
async fn a_missing_parameter_is_the_jobs_error_and_not_the_submissions() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    // Accepted, though there is nothing to run.
    let job = harness.submit(&[("LANG", "ADQL")]).await;
    assert_eq!(
        text_of(harness.get(&format!("{job}/phase")).await).await,
        "PENDING"
    );

    let response = harness
        .form(&format!("{job}/phase"), &[("PHASE", "RUN")])
        .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(harness.settled(&job).await, "ERROR");

    // And the reason is readable where TAP §2.2 says it is, as the document DALI §4.4
    // specifies rather than as this service's own JSON.
    let response = harness.get(&format!("{job}/error")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let document = text_of(response).await;
    assert!(document.contains("<VOTABLE"), "{document}");
    assert!(
        document.contains("name=\"QUERY_STATUS\" value=\"ERROR\""),
        "{document}"
    );
    assert!(document.contains("QUERY"), "{document}");
    // A failed job has no rows anywhere, so the result resource is not a place to look.
    assert_eq!(
        harness.get(&format!("{job}/results/result")).await.status(),
        StatusCode::NOT_FOUND
    );
}

/// The job list is a document and describes nothing, whatever is in the store.
///
/// UWS §2.2.2.1 asks for the jobs "that the client can see in the current security
/// context", and §3 leaves what that means to the service. With no authentication a job is
/// visible to whoever holds its id, so an anonymous caller's context holds nothing — the
/// alternative being that every caller is handed every other caller's results.
#[tokio::test]
async fn the_job_list_describes_no_one_elses_job() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[("QUERY", "SELECT id FROM sky.objects"), ("LANG", "ADQL")])
        .await;
    let id = job.rsplit('/').next().expect("an id");

    let response = harness.get("/api/v1/tap/async").await;
    assert_eq!(response.status(), StatusCode::OK);
    let document = text_of(response).await;
    assert!(document.contains("<uws:jobs"), "{document}");
    assert!(!document.contains("<uws:jobref"), "{document}");
    assert!(!document.contains(id), "{document}");
    // And the holder of the id still reaches it, which is what makes the empty list a
    // policy rather than a hole.
    assert_eq!(harness.get(&job).await.status(), StatusCode::OK);
}

/// An id naming no job is a `404`, and so is a malformed one.
///
/// The same answer to both, deliberately. UWS §3 asks for a `403` where a caller may not
/// see a job, which would confirm that the id exists — and the id is the whole of the
/// protection, so telling the two apart tells whoever is guessing which guess was closer.
#[tokio::test]
async fn an_id_naming_no_job_says_nothing_about_which_ids_exist() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    // A real job first, so this cannot pass against a service with no resource at all.
    let job = harness
        .submit(&[("QUERY", "SELECT id FROM sky.objects"), ("LANG", "ADQL")])
        .await;
    assert_eq!(harness.get(&job).await.status(), StatusCode::OK);

    for id in ["AAAAAAAAAAAAAAAAAAAAAA", "short", "../../../etc/passwd"] {
        let response = harness.get(&format!("/api/v1/tap/async/{id}")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{id}");
    }
}

/// Destroying a job forgets it and takes its rows with it.
///
/// UWS §2.1.7: "any results from the job are destroyed and storage reclaimed; the service
/// forgets that the job existed". A record that went while its file stayed would be a
/// result nothing points at, held until the process ends.
#[tokio::test]
async fn destroying_a_job_takes_its_result_too() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("PHASE", "RUN"),
        ])
        .await;
    assert_eq!(harness.settled(&job).await, "COMPLETED");
    let id = job.rsplit('/').next().expect("an id");
    let file = harness
        .jobs
        .result_path(id)
        .await
        .expect("a written result");
    assert!(file.exists(), "{}", file.display());

    let response = harness
        .send(
            Request::builder()
                .method("DELETE")
                .uri(&job)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(harness.get(&job).await.status(), StatusCode::NOT_FOUND);
    assert!(!file.exists(), "the rows outlived the job that held them");
}
