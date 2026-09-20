//! One statement, from the parameters to the bytes of the answer.
//!
//! Both TAP query resources run this and differ only in what becomes of what it returns:
//! `/sync` puts the bytes in the response, `/async` will put them in a file. Keeping it one
//! callable is what makes the uploads, `MAXREC`, the `TAP_SCHEMA` tables and the format table
//! answer the same on both — by construction, rather than by a test that the two have not
//! drifted.
//!
//! Nothing here knows it is inside a request future. The caller supplies the ceiling a route
//! allows and reads the answer out; the clock over the router is the sync route's alone.

use crate::access::data::DataFiles;
use crate::adql;
use crate::adql::query::{Answer, Rows, Source, Table};
use crate::app::request::Format;
use crate::app::routes::tap::format::Answering;
use crate::app::routes::tap::parameters::Parameters;
use crate::app::routes::tap::published::describe;
use crate::app::routes::tap::upload::{Kind, UPLOAD_SCHEMA, Upload};
use crate::app::service::Service;
use crate::error::ApiError;
use crate::output::{dsv, votable};
use crate::storage::{self, Authorities, StorageOptions};
use crate::tap::schema;

/// What a null is written as in `csv` and `tsv` here.
///
/// Empty, which is `arrow-csv`'s own. TAP has no parameter for it, so there is nothing for a
/// caller to choose and no reason to answer differently from the writer's default.
const DSV_NULL: &str = "";

/// One statement's answer: the bytes, what they are, and what is worth saying about them.
///
/// The rows are gone by the time this exists — the encoders take a collected result and
/// build the whole document, so carrying both would hold two copies of one answer. What is
/// kept beside the bytes is the two numbers a log line wants and the overflow flag a body
/// cannot always carry.
#[derive(Debug)]
pub(super) struct Answered {
    pub body: String,
    pub content_type: &'static str,
    /// Whether the answer stopped at the row bound. Only a VOTable can say so in the
    /// document; the delimited formats have nowhere to put it.
    pub overflow: bool,
    pub num_rows: usize,
    pub data_bytes_read: u64,
    /// The tables the statement named, as it named them, for the log.
    pub tables: Vec<String>,
}

/// Run one request's statement against the tables it names.
///
/// `ceiling` is what the route allows; `MAXREC` narrows it and never widens it. The row
/// bound truncates rather than refusing — on the TAP resources alone, `OVERFLOW` being the
/// in-band statement whose absence makes a cut answer indistinguishable from a whole one.
pub(super) async fn run(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
) -> Result<Answered, ApiError> {
    let translated = adql::translate(&parameters.query, service.sql_limits)?;

    // Read once, and only where the statement names one of TAP_SCHEMA's tables: describing
    // what is published costs a few small reads per catalog, which a query that names none
    // of it should not pay.
    let described = match translated
        .tables
        .iter()
        .any(|spelling| schema::resolve(spelling).is_some())
    {
        true => describe(service).await?,
        false => Vec::new(),
    };

    let mut tables = Vec::new();
    let mut data_files: Option<DataFiles> = None;
    // Two tables naming one authority — an upload and a published catalog, or two uploads —
    // would share DataFusion's one store for it; see `Authorities`. A published table has no
    // storage of its own, so every one of them shares this empty value.
    let no_storage = StorageOptions::default();
    let mut authorities = Authorities::default();
    for spelling in &translated.tables {
        let source = match schema::resolve(spelling) {
            Some(fixed) => {
                let table = fixed.rsplit('.').next().unwrap_or_default();
                Source::Memory(schema::provider(table, &described)?)
            }
            None if names_an_upload(spelling) => {
                let upload = parameters.uploads.named(spelling).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "query: {spelling} names no table this request uploaded; it uploads {}",
                        match parameters.uploads.is_empty() {
                            true => "nothing".to_owned(),
                            false => parameters.uploads.names().join(", "),
                        }
                    ))
                })?;
                let (source, files) = uploaded(service, upload, spelling, &mut authorities)?;
                if let Some(files) = files {
                    data_files = Some(files);
                }
                source
            }
            None => {
                let published = service.tap_tables.lookup(spelling).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "query: this service publishes no table named {spelling}; it \
                         publishes {}",
                        match service.tap_tables.is_empty() {
                            true => "none".to_owned(),
                            false => service.tap_tables.names().join(", "),
                        }
                    ))
                })?;
                let url = published.url();
                // Whichever mount governs the catalog, which is what says which names
                // inside it hold rows. A published table carries no storage options, so a
                // catalog that needs a credential is not one this resource can read.
                data_files = Some(service.data_files_for(url).clone());
                let dir = storage::open_dir(url, &no_storage, &service.policy, &service.transfers)?;
                authorities.check(spelling, dir.opened(&no_storage))?;
                Source::Catalog(dir)
            }
        };
        tables.push(Table {
            name: spelling.clone(),
            source,
        });
    }
    let data_files = data_files.unwrap_or_else(|| service.data_files.as_ref().clone());

    // The row bound is the smaller of what the caller asked for and what the operator
    // allows, and reaching it is a truncation rather than a refusal. `MAXREC` is *not*
    // applied to the statement's own `TOP`: TAP §2.7.4 has the truncation happen "after any
    // limitations imposed by the query", so `TOP 5` with `MAXREC=10` is five rows and no
    // overflow.
    let limits = adql::query::Limits {
        max_rows: parameters
            .maxrec
            .map_or(ceiling.max_rows, |asked| asked.min(ceiling.max_rows)),
        rows: Rows::Truncate,
        ..ceiling
    };
    let answer = adql::query::run(&translated, &tables, &data_files, limits).await?;
    Ok(Answered {
        body: encode(&answer, answering)?,
        content_type: answering.content_type,
        overflow: answer.overflow,
        num_rows: answer.result.num_rows(),
        data_bytes_read: answer.result.data_bytes_read,
        tables: translated.tables.iter().cloned().collect(),
    })
}

/// Whether a statement's table name is one of `TAP_UPLOAD`'s.
///
/// Asked before the uploads are searched so that a name under that schema is answered by
/// what the request uploaded, rather than falling through to a refusal naming the tables
/// this service publishes — which is not where the caller's table was going to be.
fn names_an_upload(spelling: &str) -> bool {
    spelling
        .split_once('.')
        .is_some_and(|(schema, _)| schema.eq_ignore_ascii_case(UPLOAD_SCHEMA))
}

/// One uploaded table as something a statement can read, and the data-file list a catalog
/// brings with it.
///
/// **Where `UPLOAD_TYPE` said nothing, the url's own name decides**: one matching the
/// data-file globs is a parquet file and anything else is a catalog directory. A directory
/// that is not one fails where a catalog is opened, which is the read that looks for
/// `hats.properties`, `properties` and `collection.properties` and says so by name.
fn uploaded<'a>(
    service: &'a Service,
    upload: &'a Upload,
    spelling: &'a str,
    authorities: &mut Authorities<'a>,
) -> Result<(Source, Option<DataFiles>), ApiError> {
    let files = service.data_files_for(&upload.url);
    let kind = upload.kind.unwrap_or(match files.matches_url(&upload.url) {
        true => Kind::Parquet,
        false => Kind::Hats,
    });
    match kind {
        Kind::Parquet => {
            if !files.matches_url(&upload.url) {
                return Err(ApiError::not_found(format!(
                    "UPLOAD {} names no data file; a parquet url ends in a name matching {}",
                    upload.name,
                    files.describe()
                )));
            }
            let file = storage::open(
                &upload.url,
                &upload.storage,
                &service.policy,
                &service.transfers,
            )?;
            authorities.check(spelling, file.opened(&upload.storage))?;
            Ok((Source::File(file), None))
        }
        Kind::Hats => {
            let files = files.clone();
            let dir = storage::open_dir(
                &upload.url,
                &upload.storage,
                &service.policy,
                &service.transfers,
            )?;
            authorities.check(spelling, dir.opened(&upload.storage))?;
            Ok((Source::Catalog(dir), Some(files)))
        }
    }
}

/// The rows, in the format the request asked for.
fn encode(answer: &Answer, answering: Answering) -> Result<String, ApiError> {
    match (answering.format, answer.overflow) {
        (Format::Votable, false) => votable::encode(&answer.result),
        (Format::Votable, true) => votable::encode_truncated(&answer.result),
        (Format::Dsv(kind), _) => dsv::encode(&answer.result, kind, DSV_NULL),
        // The spelling table is the only source of a format here, and it holds these two.
        (other, _) => Err(ApiError::internal(format!(
            "{} is not a format this resource writes",
            other.name()
        ))),
    }
}
