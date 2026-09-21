//! A `[[mount]]` whose `source` is a bucket, against a real S3 server.
//!
//! The unit tests settle what the config means and what each layer does with it; what
//! they cannot reach is the thing this mode actually is — this service standing in front
//! of a store, answering a range, a listing and a catalog query over one. `s3s-fs` speaks
//! the protocol over a temporary directory, so these run everywhere `cargo test` runs.
//!
//! Three things are checked here and nowhere else. The bytes come back whole and by range,
//! which is how an `lsdb` client reads a partition. A directory answers with the names one
//! level down, which a store has no directories to answer with and a `list_with_delimiter`
//! stands in for. And nothing in any of it — no answer, no refusal — says which bucket,
//! which endpoint or which prefix the mount really is.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use common::{FIXTURE_ROWS, TestS3, parquet_fixture};
use hats_api::access::AccessPolicy;
use hats_api::access::mount::Mounts;
use hats_api::app::{Service, router};
use hats_api::config::{
    AccessConfig, ApiConfig, DataConfig, LimitsConfig, MountConfig, NetworkConfig, ServerConfig,
    TapConfig, TapTableConfig,
};
use hats_api::hats::HatsPartition;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

/// The order and cell the fixture catalog's one partition is written at. Its rows are not
/// inside that cell and nothing here asks a spatial question: what is under test is the
/// store, and the geometry has its own fixtures.
const ORDER: u8 = 1;
const CELL: u64 = 0;

/// Where the catalog lives inside the bucket, below the prefix the mount publishes — so a
/// caller writing `/hats/cat` reaches `hats/cat` and never learns either half.
const PREFIX: &str = "hats";
const CATALOG: &str = "cat";

/// A service with one mount over `server`'s bucket, published at `/hats`, and the tables
/// in `tables` over it.
fn mounted(server: &TestS3, tables: &[(&str, &str)]) -> Service {
    let mount = MountConfig {
        path: "/hats".to_owned(),
        source: format!("s3://{}/{PREFIX}", server.bucket),
        // The whole point of the mode: the file server publishes what is in the bucket.
        serve: true,
        follow_symlinks: false,
        immutable: false,
        // The operator's own credential, written once. Nothing a caller sends carries one.
        storage: server.credentialed_options(),
        filenames: None,
    };
    let mounts = Arc::new(Mounts::new(&[mount], &DataConfig::default()).expect("the mount"));
    // The test server is on the loopback interface, so a *caller* naming it has to be
    // allowed to reach it. The mount does not, and that is worth seeing here: its source
    // is the same address, reached whatever this says, because a configured source is not
    // judged by the rules a caller's url is judged by.
    let access = AccessConfig {
        network: NetworkConfig {
            allow_loopback: true,
            ..NetworkConfig::default()
        },
        ..AccessConfig::default()
    };
    let policy = AccessPolicy::new(&access, Arc::clone(&mounts), None).expect("the policy");
    let tap = TapConfig {
        tables: tables
            .iter()
            .map(|(name, path)| TapTableConfig {
                name: (*name).to_owned(),
                path: (*path).to_owned(),
                examples: Vec::new(),
            })
            .collect(),
        jobs: Default::default(),
    };
    Service::new(
        policy,
        &LimitsConfig::default(),
        mounts,
        &ApiConfig::default(),
        &DataConfig::default(),
        &tap,
        &ServerConfig::default(),
    )
    .expect("the service")
}

/// A HATS catalog in the bucket: the file that describes it, the partition list, and one
/// partition. Enough for a catalog url to answer, which is all the store is being asked
/// about.
fn put_catalog(server: &TestS3) {
    server.put_bytes(
        &format!("{PREFIX}/{CATALOG}/hats.properties"),
        b"obs_collection=fixture\nhats_col_ra=objra\nhats_col_dec=objdec\nhats_order=1\n",
    );
    server.put_bytes(
        &format!("{PREFIX}/{CATALOG}/partition_info.csv"),
        format!("Norder,Npix\n{ORDER},{CELL}\n").as_bytes(),
    );
    server.put_bytes(
        &format!(
            "{PREFIX}/{CATALOG}/{}",
            HatsPartition::new(ORDER, CELL).path(".parquet")
        ),
        &parquet_fixture(),
    );
}

async fn respond(service: Service, request: http::request::Builder) -> http::Response<Body> {
    router(service)
        .oneshot(request.body(Body::empty()).expect("the request"))
        .await
        .expect("a response")
}

async fn bytes_of(response: http::Response<Body>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("the body")
        .to_bytes()
        .to_vec()
}

async fn text_of(response: http::Response<Body>) -> String {
    String::from_utf8_lossy(&bytes_of(response).await).into_owned()
}

/// A JSON body posted to one of the API's own routes.
async fn ask_at(route: &str, server: &TestS3, body: serde_json::Value) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri(route)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("the request");
    let response = router(mounted(server, &[]))
        .oneshot(request)
        .await
        .expect("a response");
    let status = response.status();
    (status, text_of(response).await)
}

/// The bytes, whole, with the headers a client sizes a read from — and the type the HATS
/// readers look at, which no store says for a `.parquet` key.
#[tokio::test]
async fn a_mounted_object_is_served_whole() {
    let server = TestS3::authenticated().await;
    let fixture = parquet_fixture();
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &fixture);

    let response = respond(
        mounted(&server, &[]),
        Request::builder().uri("/hats/part0.parquet"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/vnd.apache.parquet"
    );
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        fixture.len().to_string()
    );
    assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
    assert!(response.headers().contains_key(header::LAST_MODIFIED));
    assert_eq!(bytes_of(response).await, fixture);
}

/// The half that is not optional. `fsspec` — which is what `lsdb` and `nested-pandas`
/// read a catalog through — asks for the footer by range and trusts the answer without
/// checking for a `206`, so a `200` carrying the head of the file is a wrong answer with
/// nothing in it to say so.
#[tokio::test]
async fn a_mounted_object_is_served_by_range() {
    let server = TestS3::authenticated().await;
    let fixture = parquet_fixture();
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &fixture);
    let service = || mounted(&server, &[]);
    let asking = |range: &'static str| {
        Request::builder()
            .uri("/hats/part0.parquet")
            .header(header::RANGE, range)
    };

    // The tail, which is where a parquet footer is.
    let response = respond(service(), asking("bytes=-8")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()[header::CONTENT_RANGE],
        format!(
            "bytes {}-{}/{}",
            fixture.len() - 8,
            fixture.len() - 1,
            fixture.len()
        )
    );
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "8");
    assert_eq!(bytes_of(response).await, fixture[fixture.len() - 8..]);

    // And a range from the front.
    let response = respond(service(), asking("bytes=0-3")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(bytes_of(response).await, b"PAR1");

    // A range past the end is refused as one rather than answered whole, which a client
    // could not tell from the slice it asked for.
    let response = respond(service(), asking("bytes=999999999999-")).await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers()[header::CONTENT_RANGE],
        format!("bytes */{}", fixture.len())
    );
}

/// The origin's own validator, handed back to it. What it buys a client reading a
/// partition repeatedly is that the second read costs a request and no bytes.
#[tokio::test]
async fn a_mounted_object_carries_the_origins_validator() {
    let server = TestS3::authenticated().await;
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &parquet_fixture());

    let response = respond(
        mounted(&server, &[]),
        Request::builder().uri("/hats/part0.parquet"),
    )
    .await;
    let etag = response.headers()[header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();

    let again = respond(
        mounted(&server, &[]),
        Request::builder()
            .uri("/hats/part0.parquet")
            .header(header::IF_NONE_MATCH, &etag),
    )
    .await;
    assert_eq!(again.status(), StatusCode::NOT_MODIFIED);
    assert!(bytes_of(again).await.is_empty());
}

/// A `HEAD` is the same answer with no body, which is what a client sizing a file sends.
#[tokio::test]
async fn a_mounted_object_answers_head() {
    let server = TestS3::authenticated().await;
    let fixture = parquet_fixture();
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &fixture);

    let response = respond(
        mounted(&server, &[]),
        Request::builder().method("HEAD").uri("/hats/part0.parquet"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        fixture.len().to_string()
    );
    assert!(bytes_of(response).await.is_empty());
}

/// One level of the tree, which a flat namespace has to be asked for with a delimiter: a
/// recursive listing of a catalog's `Norder=` level is millions of keys to draw a page of
/// ten entries from.
#[tokio::test]
async fn a_directory_in_a_bucket_is_listed_one_level_at_a_time() {
    let server = TestS3::authenticated().await;
    put_catalog(&server);
    server.put_bytes(&format!("{PREFIX}/notes.txt"), b"hello");
    let service = || mounted(&server, &[]);

    let response = respond(service(), Request::builder().uri("/hats")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listing: serde_json::Value = serde_json::from_str(&text_of(response).await).unwrap();
    assert_eq!(listing["path"], "/hats");
    // The catalog directory and the one file beside it, and nothing from under either.
    let names: Vec<&str> = listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, [CATALOG, "notes.txt"]);
    assert_eq!(listing["entries"][0]["type"], "directory");
    assert_eq!(listing["entries"][0]["url"], "/hats/cat");
    assert_eq!(listing["entries"][1]["type"], "file");
    assert_eq!(listing["entries"][1]["size"], 5);
    assert!(listing["entries"][1]["modified"].is_string());

    // A level down, which is the catalog's own directory.
    let response = respond(service(), Request::builder().uri("/hats/cat")).await;
    let listing: serde_json::Value = serde_json::from_str(&text_of(response).await).unwrap();
    assert_eq!(listing["parent"], "/hats");
    let names: Vec<&str> = listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["dataset", "hats.properties", "partition_info.csv"]);
}

/// A prefix nothing is under is a name that is not there, and answering it `404` is the
/// only thing that stays true in a flat namespace: "nobody wrote this prefix" and "nothing
/// is under it" are one state, and a store has no empty directories to be the other one.
///
/// The `200` it used to answer was not merely generous, it broke a working catalog.
/// `hats` opens one by asking for `hats.properties`, then `properties`, then
/// `collection.properties`; an empty listing answered `200` is a file as far as any client
/// can tell, so the first probe "succeeds", the JSON is parsed as a Java properties file,
/// and `lsdb.open_catalog` fails with a validation error naming fields no listing has.
/// Catalogs on S3 that read perfectly well directly could not be opened through this
/// service at all.
#[tokio::test]
async fn a_prefix_with_nothing_under_it_is_not_there() {
    let server = TestS3::authenticated().await;
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), b"PAR1");
    let service = || mounted(&server, &[]);

    let response = respond(service(), Request::builder().uri("/hats/nothing-here")).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The probe a client actually makes, which is what made this worth fixing: a name
    // inside the mount that no key matches.
    let response = respond(service(), Request::builder().uri("/hats/hats.properties")).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // And a prefix that does have something under it is still listed.
    let response = respond(service(), Request::builder().uri("/hats")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listing: serde_json::Value = serde_json::from_str(&text_of(response).await).unwrap();
    assert_eq!(listing["entries"].as_array().unwrap().len(), 1);
}

/// The page, which is the same page a local mount draws — including the catalog panel,
/// found by the walk upwards that a store answers with a listing per level.
#[tokio::test]
async fn a_catalog_in_a_bucket_offers_its_search_on_the_page() {
    let server = TestS3::authenticated().await;
    put_catalog(&server);

    let response = respond(
        mounted(&server, &[]),
        Request::builder()
            .uri("/hats/cat/dataset")
            .header(header::ACCEPT, "text/html"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = text_of(response).await;
    // The catalog's own url under the mount, from two levels inside it.
    assert!(body.contains("data-catalog=\"/hats/cat\""), "{body}");
    assert!(body.contains("HATS catalog"), "{body}");
    assert!(body.contains("<code>fixture</code>"), "{body}");
    // And nothing about where it really is.
    assert!(!body.contains(&server.bucket), "leaked the bucket: {body}");
    assert!(
        !body.contains(&server.endpoint),
        "leaked the endpoint: {body}"
    );
}

/// A query string against a mounted object is the same question it is on disk, answered
/// from the store by ranged reads.
#[tokio::test]
async fn a_mounted_object_answers_a_query() {
    let server = TestS3::authenticated().await;
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &parquet_fixture());

    let response = respond(
        mounted(&server, &[]),
        Request::builder()
            .uri("/hats/part0.parquet?columns=objectid&filters=objectid%3C3&format=json"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let answer: serde_json::Value = serde_json::from_str(&text_of(response).await).unwrap();
    assert_eq!(answer["num_rows"], 3);
    assert_eq!(answer["rows"][0]["objectid"], 0);
    // A projection, so the file was not read whole.
    assert!(answer["rows"][0].get("objra").is_none());
}

/// The catalog's own url answers, which is the whole of what this mode is for: a HATS
/// catalog in somebody's bucket, browsable and queryable at an address the operator chose.
#[tokio::test]
async fn a_catalog_url_in_a_bucket_answers_a_query() {
    let server = TestS3::authenticated().await;
    put_catalog(&server);

    let response = respond(
        mounted(&server, &[]),
        Request::builder().uri("/hats/cat?limit=4&columns=objectid&format=json"),
    )
    .await;
    let status = response.status();
    let body = text_of(response).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["num_rows"], 4, "{body}");
    assert_eq!(answer["num_partitions"], 1, "{body}");
}

/// The API names the same catalog by the same address, and supplies no credential: the
/// mount carries one and the caller writes a path.
#[tokio::test]
async fn the_api_reads_a_mounted_bucket_by_the_mounts_path() {
    let server = TestS3::authenticated().await;
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &parquet_fixture());
    put_catalog(&server);
    let ask = async |body: serde_json::Value| ask_at("/api/v1/simple/parquet", &server, body).await;

    let (status, body) = ask(serde_json::json!({
        "url": "file:///hats/part0.parquet",
        "columns": ["objectid"],
        "limit": 2,
    }))
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["num_rows"], 2, "{body}");

    // A `file://` url naming a directory is a catalog to the API the same way, and the
    // plan route names the caller's own url back rather than where it really is.
    let catalog = serde_json::json!({"url": "file:///hats/cat", "limit": 2});
    let (status, body) = ask_at("/api/v1/simple/hats", &server, catalog.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["num_rows"],
        2,
        "{body}"
    );

    let (status, body) = ask_at("/api/v1/simple/hats/plan", &server, catalog).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("file:///hats/cat"), "{body}");
    assert!(!body.contains(&server.bucket), "leaked the bucket: {body}");

    // **The mount's credential is the mount's.** A caller who writes the source url
    // outright gets an unsigned request, the same as against any other private bucket —
    // the options are attached to the source the operator configured and to nothing else.
    //
    // The server is reachable at all because the operator named it, which is the grant an
    // `endpoints` entry gives too; what may be read there is the endpoint rules' question
    // and never the mount's.
    let (status, body) = ask(serde_json::json!({
        "url": format!("s3://{}/{PREFIX}/part0.parquet", server.bucket),
        "storage": {"endpoint": server.endpoint},
        "columns": ["objectid"],
    }))
    .await;
    assert_ne!(status, StatusCode::OK, "an unsigned read succeeded: {body}");
    assert!(!body.contains(common::SECRET_ACCESS_KEY), "leaked: {body}");
}

/// A mount publishes a directory, not where that directory really is — whichever scheme
/// it is in. A store's own account of a key names the bucket, the endpoint and the
/// operator's prefix, and none of the three is any part of what the caller wrote.
#[tokio::test]
async fn a_failure_against_a_mounted_bucket_says_nothing_about_the_bucket() {
    let server = TestS3::authenticated().await;
    // On the data-file list, so a query string is a question about it, and not parquet.
    server.put_bytes(&format!("{PREFIX}/broken.parquet"), b"not a parquet file");

    let response = respond(
        mounted(&server, &[]),
        Request::builder().uri("/hats/broken.parquet?columns=objectid"),
    )
    .await;
    let status = response.status();
    let body = text_of(response).await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "{body}"
    );
    assert!(!body.contains(&server.bucket), "leaked the bucket: {body}");
    assert!(
        !body.contains(&server.endpoint),
        "leaked the endpoint: {body}"
    );
    assert!(!body.contains(PREFIX), "leaked the prefix: {body}");

    // The credential is the operator's and never leaves: it reaches the store and
    // nothing else.
    assert!(!body.contains(common::SECRET_ACCESS_KEY), "leaked: {body}");
}

/// A published table over a store-backed mount, which is the arrangement `[[tap.table]]`
/// takes a path for: the catalog needs a credential, the mount carries it, and the
/// section that describes a public surface holds a name and an address.
#[tokio::test]
async fn a_tap_table_reads_a_catalog_in_a_bucket() {
    let server = TestS3::authenticated().await;
    put_catalog(&server);
    let service = || mounted(&server, &[("sky.objects", "/hats/cat")]);

    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs([
            ("REQUEST", "doQuery"),
            ("LANG", "ADQL"),
            ("QUERY", "SELECT TOP 3 objectid FROM sky.objects"),
        ])
        .finish();
    let response = respond(
        service(),
        Request::builder().uri(format!("/api/v1/tap/sync?{query}")),
    )
    .await;
    let status = response.status();
    let body = text_of(response).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A VOTable, which is what a TAP client reads: three rows and no error.
    assert!(body.contains("QUERY_STATUS\" value=\"OK\""), "{body}");
    assert_eq!(body.matches("<TR>").count(), 3, "{body}");

    // And the metadata resources read it too, which is what publishes the columns.
    let response = respond(service(), Request::builder().uri("/api/v1/tap/tables")).await;
    let status = response.status();
    let body = text_of(response).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("sky.objects"), "{body}");
    assert!(body.contains("objectid"), "{body}");
    assert!(!body.contains(&server.bucket), "leaked the bucket: {body}");
}

/// The fixture is big enough that a ranged read is the only way the query could have
/// answered without pulling the whole object down. Not a measurement of the store — this
/// is the number the answer carries, which says the scan pruned rather than read it all.
#[tokio::test]
async fn a_query_over_a_mounted_bucket_reads_less_than_the_whole_object() {
    let server = TestS3::authenticated().await;
    let fixture = parquet_fixture();
    server.put_bytes(&format!("{PREFIX}/part0.parquet"), &fixture);

    let response = respond(
        mounted(&server, &[]),
        Request::builder().uri(format!(
            "/hats/part0.parquet?columns=objectid&filters=objectid%3D{}&format=json",
            FIXTURE_ROWS - 1
        )),
    )
    .await;
    let body = text_of(response).await;
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["num_rows"], 1, "{body}");
    let read = answer["data_bytes_read"].as_u64().unwrap();
    assert!(
        read > 0 && read < fixture.len() as u64,
        "read {read} of {}",
        fixture.len()
    );
}

/// **A mount's credentials are not a caller's to borrow.**
///
/// DataFusion keys a registered object store by `scheme://host[:port]` and drops the path,
/// so a table reached through a mount and a table the request named outright land on one
/// store — and the second registration would decide both. A mount grants its own prefix;
/// a store shared with a url the caller chose would grant the whole bucket.
///
/// What decides it is the credentials the two stores were built from, not which side each
/// came from: built from one key they are one store, and which registration survives
/// changes nothing.
#[tokio::test]
async fn a_mounted_store_is_not_shared_with_a_url_the_caller_named() {
    let server = TestS3::authenticated().await;
    put_catalog(&server);
    let direct = format!("s3://{}/{PREFIX}/{CATALOG}", server.bucket);
    let statement = "SELECT TOP 1 a.objectid FROM mounted AS a, direct AS b \
                     WHERE a.objectid = b.objectid";
    let joining = |storage: serde_json::Value| {
        serde_json::json!({
            "query": statement,
            "tables": {
                "mounted": {"type": "hats", "url": "file:///hats/cat"},
                "direct": {"type": "hats", "url": direct, "storage": storage},
            },
        })
    };

    // Nothing to sign with, which is what the shared store would quietly have gained.
    let (status, body) = ask_at(
        "/api/v1/adql",
        &server,
        joining(serde_json::json!({"endpoint": server.endpoint})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("different credentials"), "{body}");
    assert!(!body.contains(common::SECRET_ACCESS_KEY), "leaked: {body}");

    // **The same key, written out by the caller, is allowed.** They had it already, so
    // the store they would share hands them none of the operator's reach.
    let (status, body) = ask_at(
        "/api/v1/adql",
        &server,
        joining(serde_json::json!({
            "endpoint": server.endpoint,
            "allow_http": true,
            "access_key_id": common::ACCESS_KEY_ID,
            "secret_access_key": common::SECRET_ACCESS_KEY,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // As are two tables through the one mount, which is the ordinary crossmatch.
    let (status, body) = ask_at(
        "/api/v1/adql",
        &server,
        serde_json::json!({
            "query": statement,
            "tables": {
                "mounted": {"type": "hats", "url": "file:///hats/cat"},
                "direct": {"type": "hats", "url": "file:///hats/cat"},
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
