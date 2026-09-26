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
use bytesize::ByteSize;
use futures::StreamExt as _;
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
    published_with(dir, limits, Vec::new())
}

/// The same, with examples the operator wrote for the table.
fn published_with(
    dir: &std::path::Path,
    limits: &LimitsConfig,
    examples: Vec<crate::config::TapExampleConfig>,
) -> Service {
    let tap = TapConfig {
        tables: vec![TapTableConfig {
            name: "sky.objects".to_owned(),
            path: "/".to_owned(),
            examples,
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
    let (status, content_type, body) = send_bytes(service, request).await;
    (
        status,
        content_type,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

/// The same, keeping the bytes that came off the wire.
///
/// Parquet is a format that is not text, so a body read through `from_utf8_lossy` is one
/// that has already been changed by the reading — every byte outside ASCII becomes a
/// replacement character, and what is left will not open.
async fn send_bytes(service: Service, request: Request<Body>) -> (StatusCode, String, Vec<u8>) {
    let response = router(service).oneshot(request).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, content_type, body.to_vec())
}

/// A `GET` to `/tap/sync`, answered in bytes.
async fn ask_bytes(service: Service, pairs: &[(&str, &str)]) -> (StatusCode, String, Vec<u8>) {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish();
    send_bytes(
        service,
        Request::builder()
            .uri(format!("/api/v1/tap/sync?{query}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// One parquet answer, read back the way a client reads it.
///
/// Reading it is the assertion: a body that is not a parquet file fails here rather than
/// at a byte comparison that says nothing about why.
fn read_parquet(bytes: &[u8]) -> (Vec<String>, usize) {
    let reader =
        datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::copy_from_slice(bytes),
        )
        .expect("the answer is a parquet file");
    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let rows = reader.metadata().file_metadata().num_rows();
    (columns, usize::try_from(rows).expect("a row count"))
}

/// The examples document, as a client fetches it.
async fn fetch_examples(service: Service) -> (StatusCode, String, String) {
    send(
        service,
        Request::builder()
            .uri("/api/v1/tap/examples")
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

/// The document, read by a strict XML parser.
///
/// Which is what a client uses: `pyvo` runs `xml.etree.ElementTree` over the bytes, with no
/// HTML tolerance anywhere in it, and `taplint` validates it as a document. So a page a
/// browser renders happily and an XML parser rejects is one that reaches an astronomer as
/// an empty menu rather than as a complaint — the failure this whole resource is for.
fn parsed_as_xml(page: &str) {
    let mut reader = quick_xml::Reader::from_str(page);
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Eof) => return,
            Ok(_) => {}
            Err(error) => panic!(
                "not well-formed XML at byte {}: {error}\n{page}",
                reader.buffer_position()
            ),
        }
    }
}

/// The queries a document publishes, read out of it the way a client reads them.
fn queries(page: &str) -> Vec<String> {
    page.split("<pre property=\"query\">")
        .skip(1)
        .map(|rest| {
            let text = rest.split("</pre>").next().expect("a closed query element");
            quick_xml::escape::unescape(text).unwrap().into_owned()
        })
        .collect()
}

/// One example per published table, written from what the catalog says about itself.
///
/// Every attribute asserted here is one a client matches on — DALI §2.3's `vocab`,
/// `typeof`, `resource` and `name`, TAP §2.6's `query` and `table` — so a document missing
/// any of them is one TOPCAT shows an empty menu for rather than one it complains about.
#[tokio::test]
async fn the_examples_document_offers_a_cone_per_published_table() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, page) =
        fetch_examples(published(dir.path(), &LimitsConfig::default())).await;

    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(content_type, "text/html; charset=utf-8");
    parsed_as_xml(&page);
    assert!(
        page.contains("vocab=\"http://www.ivoa.net/rdf/examples#\""),
        "{page}"
    );
    assert!(page.contains("typeof=\"example\""), "{page}");
    assert!(
        page.contains("<div id=\"sky.objects\" resource=\"#sky.objects\""),
        "{page}"
    );
    assert!(page.contains("property=\"name\""), "{page}");
    assert!(
        page.contains("<span property=\"table\">sky.objects</span>"),
        "{page}"
    );

    let found = queries(&page);
    let [query] = found.as_slice() else {
        panic!("expected one example, got {found:?}")
    };
    // A line per clause, asserted as lines rather than by searching in them: this is the
    // text a reader sees and then edits, so the layout is part of what is published.
    //
    // Named columns and a row bound are what keep it from being the slow query a reader
    // concludes the service is broken by; the cone is the spelling nobody guesses. The
    // position leads, this being a cone search whose answer has to say where its rows are,
    // and the HEALPix index is absent — the importer's machinery rather than the catalog's
    // content, which nobody opening a menu asked for.
    let lines: Vec<&str> = query.lines().collect();
    let [select, from, where_] = lines.as_slice() else {
        panic!("expected three lines:\n{query}")
    };
    assert_eq!(*select, "SELECT TOP 10 ra, dec, id");
    assert_eq!(*from, "FROM sky.objects");
    assert!(
        where_.starts_with("WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE("),
        "{query}"
    );
    assert!(where_.ends_with("))"), "{query}");
}

/// **The generated example runs, and returns rows.** That is the whole of what this
/// resource is worth: a menu entry that 400s, or that comes back empty, is worse than no
/// menu — a new user reads either as the service being broken rather than as the example
/// being wrong.
///
/// The position is what this really holds. It is the centre of one of the catalog's own
/// partition cells, which is a place the catalog has rows by construction; a position
/// guessed from anything else is one that happens to work on the catalog it was written
/// against.
#[tokio::test]
async fn the_generated_example_is_a_query_that_returns_rows() {
    let dir = hats::query::tests::fixture(true);
    let (_, _, page) = fetch_examples(published(dir.path(), &LimitsConfig::default())).await;
    let found = queries(&page);
    let [query] = found.as_slice() else {
        panic!("expected one example, got {found:?}")
    };

    let (status, content_type, answer) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[("QUERY", query), ("LANG", "ADQL")],
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{query}\n{answer}");
    assert_eq!(content_type, "application/x-votable+xml");
    assert!(
        answer.contains("<INFO name=\"QUERY_STATUS\" value=\"OK\"/>"),
        "{answer}"
    );
    assert!(answer.contains("<TR>"), "{query}\n{answer}");
}

/// An operator who wrote examples for a table has said what that table's examples are, so
/// the generated one is gone rather than sitting underneath them.
#[tokio::test]
async fn an_operators_examples_replace_the_generated_one() {
    let dir = hats::query::tests::fixture(true);
    let wrote = |name: &str, query: &str| crate::config::TapExampleConfig {
        name: name.to_owned(),
        query: query.to_owned(),
    };
    let service = published_with(
        dir.path(),
        &LimitsConfig::default(),
        vec![
            wrote("Lowest ids", "SELECT TOP 3 id FROM sky.objects ORDER BY id"),
            // A name and a query carrying markup, both of which come out of a file this
            // service did not write.
            wrote("Ampersand & <angle>", "SELECT TOP 1 id FROM sky.objects"),
        ],
    );

    let (status, _, page) = fetch_examples(service).await;
    assert_eq!(status, StatusCode::OK, "{page}");
    parsed_as_xml(&page);
    assert_eq!(
        queries(&page),
        [
            "SELECT TOP 3 id FROM sky.objects ORDER BY id",
            "SELECT TOP 1 id FROM sky.objects",
        ]
    );
    // Nothing of the generated one is left.
    assert!(!page.contains("CONTAINS"), "{page}");
    // Two examples under one table need two fragments, or a client referencing one reaches
    // whichever the parser saw first.
    assert!(page.contains("id=\"sky.objects-1\""), "{page}");
    assert!(page.contains("id=\"sky.objects-2\""), "{page}");
    // A configured string is markup until it is escaped.
    assert!(page.contains("Ampersand &amp; &lt;angle&gt;"), "{page}");
}

/// The base url, opened in a browser: the tables, the examples, and snippets written against
/// this deployment's own url and first example, so what a reader copies runs as it stands.
#[tokio::test]
async fn the_base_url_is_a_page_for_a_person() {
    let dir = hats::query::tests::fixture(true);
    let (status, content_type, page) = send(
        published(dir.path(), &LimitsConfig::default()),
        Request::builder()
            .uri("/api/v1/tap")
            .header(header::HOST, "example.com")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(content_type, "text/html; charset=utf-8");

    let base = "http://example.com/api/v1/tap";
    assert!(
        page.contains(&format!(
            "<a href=\"{base}/tables/sky.objects\"><code>sky.objects</code></a>"
        )),
        "{page}"
    );
    let (_, _, examples) = fetch_examples(published(dir.path(), &LimitsConfig::default())).await;
    let found = queries(&examples);
    let [query] = found.as_slice() else {
        panic!("expected one example, got {found:?}")
    };
    // A box per client, each holding this example: the query itself, then the clients.
    let escaped = quick_xml::escape::escape(query.as_str()).into_owned();
    assert!(
        page.contains(&format!(
            "<pre class=\"client-code\" data-client=\"ADQL\">{escaped}</pre>"
        )),
        "{page}"
    );
    for client in ["ADQL", "pyvo", "TOPCAT", "STILTS", "curl"] {
        assert!(
            page.contains(&format!(
                "<button class=\"client-tab\" data-client=\"{client}\">"
            )),
            "{client}\n{page}"
        );
        assert!(
            page.contains(&format!(
                "<pre class=\"client-code\" data-client=\"{client}\">"
            )),
            "{client}\n{page}"
        );
    }
    assert!(
        page.contains(&format!("pyvo.dal.TAPService(&quot;{base}&quot;)")),
        "{page}"
    );
    assert!(
        page.contains("RESPONSEFORMAT=&quot;parquet&quot;"),
        "{page}"
    );
    assert!(
        page.contains(&format!("stilts tapquery tapurl={base}")),
        "{page}"
    );
    assert!(page.contains("out=rows.parquet"), "{page}");
}

/// A client picks the resource out of `/capabilities` and has no other way to find it, so
/// the declaration and the route are one change. DALI §2.3 puts it the other way round too:
/// a service that does not implement `/examples` answers 404 there.
#[tokio::test]
async fn the_examples_resource_is_declared() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, document) = send(
        published(dir.path(), &LimitsConfig::default()),
        Request::builder()
            .uri("/api/v1/tap/capabilities")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert!(
        document.contains("standardID=\"ivo://ivoa.net/std/DALI#examples\""),
        "{document}"
    );
    assert!(
        document
            .contains("<accessURL use=\"full\">http://localhost/api/v1/tap/examples</accessURL>"),
        "{document}"
    );
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

/// The format this service added to TAP, answered as a file a parquet reader opens.
///
/// Both spellings, because a client picks one out of the capabilities document — where the
/// media type is what is published — and a person writing a url by hand picks the other.
#[tokio::test]
async fn a_parquet_answer_is_a_parquet_file() {
    let dir = hats::query::tests::fixture(true);
    for asked in ["parquet", "application/vnd.apache.parquet"] {
        let (status, content_type, body) = ask_bytes(
            published(dir.path(), &LimitsConfig::default()),
            &[
                ("QUERY", "SELECT TOP 2 id, ra FROM sky.objects ORDER BY id"),
                ("LANG", "ADQL"),
                ("RESPONSEFORMAT", asked),
            ],
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{asked}");
        assert_eq!(content_type, "application/vnd.apache.parquet", "{asked}");
        let (columns, rows) = read_parquet(&body);
        assert_eq!(columns, ["id", "ra"], "{asked}");
        assert_eq!(rows, 2, "{asked}");
    }
}

/// **A parquet body is never compressed and always carries its length.**
///
/// Both are what make the answer openable rather than merely correct: a parquet file is
/// read from its footer backwards, so a reader needs the length to find the footer and
/// ranges to fetch it. The compression layer excludes the body by its content type, which
/// is one rule covering a file off a mount, a query answer and now a TAP answer — so this
/// asks for `gzip` the way every HTTP client does and checks it was declined.
#[tokio::test]
async fn a_parquet_answer_is_not_compressed_and_says_how_long_it_is() {
    let dir = hats::query::tests::fixture(true);
    let request = Request::builder()
        .uri(
            "/api/v1/tap/sync?LANG=ADQL&RESPONSEFORMAT=parquet\
             &QUERY=SELECT+id+FROM+sky.objects",
        )
        .header(header::ACCEPT_ENCODING, "gzip, deflate, br")
        .body(Body::empty())
        .unwrap();
    let response = router(published(dir.path(), &LimitsConfig::default()))
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !response.headers().contains_key(header::CONTENT_ENCODING),
        "a parquet answer was compressed: {:?}",
        response.headers()
    );
    let length: usize = response.headers()[header::CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), length);
    read_parquet(&body);
}

/// A parquet file has nowhere to say its rows were cut, so the fact is stated in the same
/// header `csv` and `tsv` carry it in — and the file itself is still whole and readable,
/// which is what a truncation must not cost.
#[tokio::test]
async fn a_truncated_parquet_answer_says_so_in_a_header() {
    let dir = hats::query::tests::fixture(true);
    let request = Request::builder()
        .uri(
            "/api/v1/tap/sync?LANG=ADQL&RESPONSEFORMAT=parquet&MAXREC=1\
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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(read_parquet(&body).1, 1);
}

/// `MAXREC=0` is the columns and no rows, which for parquet is a file holding a schema —
/// the shape a client asks for when it wants to know what it would get.
#[tokio::test]
async fn a_parquet_answer_with_no_rows_is_still_a_file() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask_bytes(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id, ra, dec FROM sky.objects"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "parquet"),
            ("MAXREC", "0"),
        ],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let (columns, rows) = read_parquet(&body);
    assert_eq!(columns, ["id", "ra", "dec"]);
    assert_eq!(rows, 0);
}

/// `STREAMING=true` sends the same file, without waiting for it to be built.
///
/// The bytes are the assertion: one writer drives both, so a streamed answer and a collected
/// one over the same statement are the same parquet file. What differs is only when it
/// leaves, which the headers below are how a client can tell.
#[tokio::test]
async fn a_streamed_parquet_answer_is_the_file_a_collected_one_would_have_sent() {
    let dir = hats::query::tests::fixture(true);
    let statement: &[(&str, &str)] = &[
        ("QUERY", "SELECT id, ra FROM sky.objects ORDER BY id"),
        ("LANG", "ADQL"),
        ("RESPONSEFORMAT", "parquet"),
    ];
    let asked = async |streaming: bool| {
        let pairs: Vec<(&str, &str)> = statement
            .iter()
            .copied()
            .chain(streaming.then_some(("STREAMING", "true")))
            .collect();
        ask_bytes(published(dir.path(), &LimitsConfig::default()), &pairs).await
    };

    let (status, content_type, streamed) = asked(true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "application/vnd.apache.parquet");
    let (_, _, collected) = asked(false).await;

    assert_eq!(streamed, collected);
    let (columns, rows) = read_parquet(&streamed);
    assert_eq!(columns, ["id", "ra"]);
    assert_eq!(rows, hats::query::tests::fixture_rows());
}

/// What a streamed answer gives up, said in the headers rather than left to be discovered.
///
/// No `Content-Length`, because the rows had not been read when the headers went; therefore
/// `Accept-Ranges: none`, because a client that sends a `Range` and gets a plain `200` may
/// otherwise read that many bytes off the front and treat them as the range it asked for. And
/// no `x-hats-overflow`, which is the same fact one step further in: whether the bound cut
/// the answer is known once the rows have run out.
#[tokio::test]
async fn a_streamed_answer_has_no_length_no_ranges_and_no_overflow_header() {
    let dir = hats::query::tests::fixture(true);
    let request = Request::builder()
        .uri(
            "/api/v1/tap/sync?LANG=ADQL&RESPONSEFORMAT=parquet&STREAMING=true\
             &QUERY=SELECT+id+FROM+sky.objects",
        )
        // What a browser sends, and the only thing the compression layer acts on. A parquet
        // body is excluded from it by content type rather than by route, so the exclusion has
        // to hold for a streamed answer as it does for a collected one — a compressed body is
        // one whose bytes are not the file the reader was told it was getting.
        .header(header::ACCEPT_ENCODING, "gzip, deflate, br")
        .body(Body::empty())
        .unwrap();
    let response = router(published(dir.path(), &LimitsConfig::default()))
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(headers[header::ACCEPT_RANGES], "none");
    assert!(!headers.contains_key(header::CONTENT_LENGTH), "{headers:?}");
    assert!(!headers.contains_key("x-hats-overflow"), "{headers:?}");
    assert!(
        !headers.contains_key(header::CONTENT_ENCODING),
        "a streamed parquet answer was compressed: {headers:?}"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    read_parquet(&body);
}

/// A streamed answer the row bound cut is a body that stops, never a file that looks whole.
///
/// The collected answer says it was cut in `x-hats-overflow`; a streamed one sent its headers
/// before it knew. Parquet has nowhere in the document to say it either, so what is left is
/// to end without the footer — which every reader refuses. A file closed over a truncation
/// would hold fewer rows than the query matched with nothing in it saying so, which is the
/// one failure a caller cannot tell from data.
#[tokio::test]
async fn a_streamed_parquet_answer_cut_by_maxrec_will_not_open() {
    let dir = hats::query::tests::fixture(true);
    let request = Request::builder()
        .uri(
            "/api/v1/tap/sync?LANG=ADQL&RESPONSEFORMAT=parquet&STREAMING=true&MAXREC=1\
             &QUERY=SELECT+id+FROM+sky.objects",
        )
        .body(Body::empty())
        .unwrap();
    let response = router(published(dir.path(), &LimitsConfig::default()))
        .oneshot(request)
        .await
        .unwrap();

    // The status and the head of the document are long gone by the time the bound is known,
    // so this is a `200` whose body does not finish.
    assert_eq!(response.status(), StatusCode::OK);
    // Collected a frame at a time rather than in one call, because the transfer is meant to
    // fail: a body that ends in an error is the whole point, and anything that unwraps the
    // collection asserts nothing about what a reader is left holding.
    let mut frames = response.into_body().into_data_stream();
    let mut cut = Vec::new();
    let mut broke = false;
    while let Some(frame) = frames.next().await {
        match frame {
            Ok(chunk) => cut.extend_from_slice(&chunk),
            Err(_) => {
                broke = true;
                break;
            }
        }
    }
    assert!(broke, "the body ended cleanly on a cut answer");
    assert!(
        datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(cut.clone())
        )
        .is_err(),
        "a cut answer was readable as a whole file: {} bytes",
        cut.len()
    );

    // And the same request with room to finish is the file it claims to be, so what the
    // refusal above proves is the bound and not that a streamed TAP answer cannot be parquet.
    let (status, _, whole) = ask_bytes(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "parquet"),
            ("STREAMING", "true"),
            ("MAXREC", "1000"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(read_parquet(&whole).1, hats::query::tests::fixture_rows());
}

/// `STREAMING` is read as the request spells it, and presence alone is not `true`.
///
/// A whole answer built where a stream was wanted, or a stream where a seekable body was,
/// are both answers the caller cannot tell from the one they asked for — so a value that is
/// neither word is refused rather than resolved.
#[tokio::test]
async fn streaming_takes_true_or_false() {
    let dir = hats::query::tests::fixture(true);
    for value in ["", "yes", "1", "TRUE", "on"] {
        let (status, _, body) = ask(
            published(dir.path(), &LimitsConfig::default()),
            &[
                ("QUERY", "SELECT TOP 1 id FROM sky.objects"),
                ("LANG", "ADQL"),
                ("STREAMING", value),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "STREAMING={value}: {body}");
        // A refusal is a document a TAP client reads, whatever it is refusing.
        assert!(body.contains("QUERY_STATUS"), "{body}");
    }

    let (status, _, _) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT TOP 1 id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("STREAMING", "false"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

/// `MAXREC=0` is the columns whether or not the answer is streamed.
///
/// It reaches the row bound by construction, the statement being planned and never run, so a
/// streamed answer that ended its document on every bound would send three of the four
/// formats as a body no reader opens — and what this request asked for is the schema. It is
/// the one bound nothing is hidden by: the caller wrote the zero, so there is no truncation
/// for the ending to warn them about. DALI §3.4.4 has the columns come back with the
/// indicator beside them, which is what VOTable writes here.
#[tokio::test]
async fn a_streamed_answer_of_no_rows_is_still_a_document() {
    let dir = hats::query::tests::fixture(true);
    let (status, _, body) = ask_bytes(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id, ra, dec FROM sky.objects"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "parquet"),
            ("STREAMING", "true"),
            ("MAXREC", "0"),
        ],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let (columns, rows) = read_parquet(&body);
    assert_eq!(columns, ["id", "ra", "dec"]);
    assert_eq!(rows, 0);

    // And the marker is still said where the format has somewhere to say it, so what this
    // dropped is the end of a document and not the fact itself.
    let (status, _, votable) = ask(
        published(dir.path(), &LimitsConfig::default()),
        &[
            ("QUERY", "SELECT id FROM sky.objects"),
            ("LANG", "ADQL"),
            ("STREAMING", "true"),
            ("MAXREC", "0"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(votable.contains(r#"value="OVERFLOW""#), "{votable}");
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
    // Every format the query resource answers, and no other. Parquet is this service's
    // own — DALI §3.4.3 provides for one beyond the standard's list — and it is here
    // because a client that never heard of it reads the media type and skips it, while one
    // that did has no other way to find out this is where a nested column is answered.
    for mime in [
        "application/x-votable+xml",
        "text/csv;header=present",
        "text/tab-separated-values",
        "application/vnd.apache.parquet",
    ] {
        assert!(body.contains(mime), "{mime} is not declared: {body}");
    }
    assert!(body.contains("<alias>parquet</alias>"), "{body}");
    // Still deliberately absent: what a nested column looks like in JSON is not settled.
    assert!(!body.contains("application/json"), "{body}");
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
        Self::keeping(dir, &AsyncConfig::default())
    }

    /// The same, with what a job may keep spelled out.
    fn keeping(dir: &std::path::Path, config: &AsyncConfig) -> Self {
        let limits = LimitsConfig::default();
        let service = published(dir, &limits);
        let jobs = Jobs::new(service.clone(), &limits, config).expect("the job resource");
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

/// One statement, two resources, one document.
///
/// `/sync` builds its answer and measures it; a job writes its own into the file as the
/// rows arrive. Both drive the same encoder, and two drivers over one format is exactly the
/// thing that drifts — so what is checked here is that they have not. The one difference is
/// `nrows`: it is an attribute at the head of the `TABLE` and the count is known only once
/// the rows have run out, so a written document leaves it out, which VOTable allows.
#[tokio::test]
async fn a_job_writes_the_document_sync_would_have_built() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let statement = &[
        ("QUERY", "SELECT id, ra, dec FROM sky.objects ORDER BY id"),
        ("LANG", "ADQL"),
    ];

    let synced = text_of(harness.form("/api/v1/tap/sync", statement).await).await;
    let job = harness
        .submit(&[statement.as_slice(), &[("PHASE", "RUN")]].concat())
        .await;
    assert_eq!(harness.settled(&job).await, "COMPLETED");
    let written = text_of(harness.get(&format!("{job}/results/result")).await).await;

    let counted = synced
        .split_once("<TABLE nrows=")
        .and_then(|(_, rest)| rest.split_once('>'))
        .map(|(count, _)| format!("<TABLE nrows={count}>"))
        .expect("a built document says how many rows it holds");
    assert_eq!(written, synced.replace(&counted, "<TABLE>"));
}

/// An answer larger than a job may keep is that job's failure, and it is refused while the
/// document is being written rather than once all of it is in memory.
#[tokio::test]
async fn a_job_over_what_it_may_keep_fails_and_says_so() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::keeping(
        dir.path(),
        &AsyncConfig {
            max_result_bytes: ByteSize::b(200),
            ..AsyncConfig::default()
        },
    );
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id, ra, dec FROM sky.objects"),
            ("LANG", "ADQL"),
            ("PHASE", "RUN"),
        ])
        .await;

    assert_eq!(harness.settled(&job).await, "ERROR");
    let document = text_of(harness.get(&format!("{job}/error")).await).await;
    assert!(document.contains("200 bytes"), "{document}");
    // And nothing is left pointing at half an answer.
    let id = job.rsplit('/').next().expect("an id");
    assert!(harness.jobs.result_path(id).await.is_none());
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

/// A job answers parquet, and this is the path the row-group writer is on.
///
/// `/sync` builds its body whole; a job's answer is written into the file as the rows
/// arrive, so this is the only place the three encoder calls run over batches that had not
/// been read when the first of them was made. What the file being openable proves is that
/// the footer was written last and the pieces before it were written in order — a parquet
/// file assembled out of order reads as corrupt rather than as short.
///
/// The headers are the other half. A parquet reader opening this url needs the length and
/// the ranges the file gives it, which is the same thing that made the result a file in the
/// first place.
#[tokio::test]
async fn a_job_answers_parquet_as_a_file_a_reader_opens() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id, ra FROM sky.objects ORDER BY id"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "parquet"),
            ("PHASE", "RUN"),
        ])
        .await;
    assert_eq!(harness.settled(&job).await, "COMPLETED");

    let response = harness.get(&format!("{job}/results/result")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "application/vnd.apache.parquet"
    );
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert!(headers.contains_key(header::CONTENT_LENGTH));
    assert_eq!(headers["x-hats-overflow"], "false");

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let (columns, rows) = read_parquet(&body);
    assert_eq!(columns, ["id", "ra"]);
    assert_eq!(rows, hats::query::tests::fixture_rows());
}

/// One statement, two resources, one file.
///
/// The collected writer and the written one are the same encoder driven two ways, and two
/// drivers over one format is exactly the thing that drifts. VOTable's own version of this
/// allows for `nrows`, which a streamed document cannot know; parquet has no such
/// difference — the footer is written last either way — so the bytes are the same bytes.
#[tokio::test]
async fn a_job_writes_the_parquet_sync_would_have_built() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let statement = &[
        ("QUERY", "SELECT id, ra, dec FROM sky.objects ORDER BY id"),
        ("LANG", "ADQL"),
        ("RESPONSEFORMAT", "parquet"),
    ];

    let synced = harness.form("/api/v1/tap/sync", statement).await;
    let synced = synced.into_body().collect().await.unwrap().to_bytes();
    let job = harness
        .submit(&[statement.as_slice(), &[("PHASE", "RUN")]].concat())
        .await;
    assert_eq!(harness.settled(&job).await, "COMPLETED");
    let written = harness.get(&format!("{job}/results/result")).await;
    let written = written.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(written, synced);
    assert_eq!(read_parquet(&written).1, hats::query::tests::fixture_rows());
}

/// `STREAMING` is `/sync`'s, and a job ignores it rather than refusing it.
///
/// TAP §2.7: a spurious parameter "must" be ignored, answered normally and not reported as an
/// error — and on this resource the name is spurious, being neither TAP's nor one a job has
/// anything to do with. A client that sets it for every request it makes would otherwise get
/// a failed job for a name every other TAP service ignores. Nothing is promised by accepting
/// it either: a job's answer is written as it is read whatever it says, and comes back as a
/// file with a length and ranged reads.
#[tokio::test]
async fn a_job_ignores_the_streaming_parameter() {
    let dir = hats::query::tests::fixture(true);
    let harness = Jobbed::new(dir.path());
    let job = harness
        .submit(&[
            ("QUERY", "SELECT id, ra FROM sky.objects ORDER BY id"),
            ("LANG", "ADQL"),
            ("RESPONSEFORMAT", "parquet"),
            ("STREAMING", "true"),
            ("PHASE", "RUN"),
        ])
        .await;

    assert_eq!(harness.settled(&job).await, "COMPLETED");
    let response = harness.get(&format!("{job}/results/result")).await;
    assert_eq!(response.status(), StatusCode::OK);
    // The answer the parameter did not change: a file with a length and ranged reads, which
    // is what a job's result is whatever was asked for.
    let headers = response.headers().clone();
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert!(headers.contains_key(header::CONTENT_LENGTH));
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(read_parquet(&body).1, hats::query::tests::fixture_rows());
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
