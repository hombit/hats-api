//! One statement, from the parameters to the bytes of the answer.
//!
//! Both TAP query resources run this and differ only in where the bytes go: `/sync` puts
//! them in the response and `/async` puts them in a file. Everything up to the rows is one
//! callable, which is what makes the uploads, `MAXREC`, the `TAP_SCHEMA` tables and the
//! format table answer the same on both — by construction, rather than by a test that the
//! two have not drifted.
//!
//! **What differs after that is whether the answer has to exist all at once.** A `/sync`
//! body carries a length, so the document is built and measured; a job's answer is a file,
//! so it is written as it is encoded and never held. The second is why [`write`] exists
//! beside [`run`]: the peak of a job is then one batch and one chunk rather than the rows
//! and the whole document, and the ceiling on what a job may keep refuses at the byte that
//! passes it instead of once all of it is in memory.
//!
//! Nothing here knows it is inside a request future. The caller supplies the ceiling a route
//! allows and reads the answer out; the clock over the router is the sync route's alone.

use std::time::Instant;

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
use crate::output::{dsv, stream, votable};
use crate::storage::{self, Authorities, StorageOptions};
use crate::tap::jobs::Writing;
use crate::tap::schema;

/// What a null is written as in `csv` and `tsv` here.
///
/// Empty, which is `arrow-csv`'s own. TAP has no parameter for it, so there is nothing for a
/// caller to choose and no reason to answer differently from the writer's default.
const DSV_NULL: &str = "";

/// What one statement's answer turned out to be, beside the bytes of it.
///
/// The bytes are not here: they are the response body on one resource and a file on the
/// other, and both are large enough that carrying them through a struct nobody needs them
/// in would be a second copy of one answer. What is kept is the two numbers a log line
/// wants and the overflow flag a body cannot always carry.
#[derive(Debug)]
pub(super) struct Answered {
    pub content_type: &'static str,
    /// Whether the answer stopped at the row bound. Only a VOTable can say so in the
    /// document; the delimited formats have nowhere to put it.
    pub overflow: bool,
    pub num_rows: usize,
    pub data_bytes_read: u64,
    /// The tables the statement named, as it named them, for the log.
    pub tables: Vec<String>,
}

/// Run one request's statement and keep the whole answer, which is what a `/sync` body is.
pub(super) async fn run(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
) -> Result<(String, Answered), ApiError> {
    let prepared = prepare(service, parameters, ceiling).await?;
    let answer = adql::query::run(
        &prepared.translated,
        &prepared.tables,
        &prepared.data_files,
        prepared.limits,
    )
    .await?;
    let body = encode(&answer, answering)?;
    Ok((
        body,
        Answered {
            content_type: answering.content_type,
            overflow: answer.overflow,
            num_rows: answer.result.num_rows(),
            data_bytes_read: answer.result.data_bytes_read,
            tables: prepared
                .tables
                .iter()
                .map(|table| table.name.clone())
                .collect(),
        },
    ))
}

/// The same statement, written into a sink as its rows arrive.
///
/// Nothing between the first byte and the last is held: each batch becomes a piece of the
/// document, the piece goes to the sink, and both are dropped. What the sink does with a
/// piece — count it against a ceiling, refuse — is the sink's, and a refusal ends the
/// reading where it stands.
///
/// **A streamed document has no `nrows`.** The count is known only once the rows have run
/// out and the attribute sits at the head of the `TABLE`, so it is left out — which VOTable
/// allows, it being optional, and which the job record answers anyway.
pub(super) async fn write(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
    into: &mut Writing,
) -> Result<Answered, ApiError> {
    let started = Instant::now();
    let prepared = prepare(service, parameters, ceiling).await?;
    let mut encoder = encoder(answering)?;
    let mut execution = adql::query::plan(
        &prepared.translated,
        &prepared.tables,
        &prepared.data_files,
        prepared.limits,
    )
    .await?;

    into.write(&encoder.begin(&execution.schema)?).await?;
    while let Some(batch) = execution.next().await {
        into.write(&encoder.rows(&batch?)?).await?;
    }
    let ending = stream::Ending {
        rows: execution.num_rows(),
        data_bytes_read: execution.data_bytes_read(),
        elapsed: started.elapsed(),
        overflow: execution.overflow(),
        // A bound that stopped this part-way is the row bound, which `overflow` already
        // says; nothing else here refuses after the first byte.
        refused: None,
    };
    into.write(&encoder.end(ending)?).await?;
    Ok(Answered {
        content_type: answering.content_type,
        overflow: execution.overflow(),
        num_rows: execution.num_rows(),
        data_bytes_read: execution.data_bytes_read(),
        tables: prepared
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect(),
    })
}

/// A statement and the tables it names, opened and ready to be planned.
#[derive(Debug)]
struct Prepared {
    translated: adql::Translated,
    tables: Vec<Table>,
    data_files: DataFiles,
    limits: adql::query::Limits,
}

/// Read one request's parameters and open every table its statement names.
///
/// `ceiling` is what the route allows; `MAXREC` narrows it and never widens it. The row
/// bound truncates rather than refusing — on the TAP resources alone, `OVERFLOW` being the
/// in-band statement whose absence makes a cut answer indistinguishable from a whole one.
async fn prepare(
    service: &Service,
    parameters: &Parameters,
    ceiling: adql::query::Limits,
) -> Result<Prepared, ApiError> {
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
    Ok(Prepared {
        translated,
        tables,
        data_files,
        limits,
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

/// The format the request asked for, as something a batch at a time goes through.
///
/// The same encoders the collected writers are built on — [`encode`] drives them over rows
/// that are all in hand, and this drives them over rows that are not. Two ways of writing
/// one format would be two things to keep saying the same.
fn encoder(answering: Answering) -> Result<Box<dyn stream::Encoder>, ApiError> {
    Ok(match answering.format {
        Format::Votable => Box::new(votable::Document::new(None)),
        Format::Dsv(kind) => Box::new(dsv::Delimited::new(kind, DSV_NULL)),
        // The spelling table is the only source of a format here, and it holds these two.
        other => {
            return Err(ApiError::internal(format!(
                "{} is not a format this resource writes",
                other.name()
            )));
        }
    })
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
