//! One statement, from the parameters to the bytes of the answer.
//!
//! Both TAP query resources run this and differ only in where the bytes go: `/sync` puts
//! them in the response and `/async` puts them in a file. Everything up to the rows is one
//! callable, which is what makes the uploads, `MAXREC`, the `TAP_SCHEMA` tables and the
//! format table answer the same on both — by construction, rather than by a test that the
//! two have not drifted.
//!
//! **What differs after that is whether the answer has to exist all at once**, and three
//! drivers answer that three ways over one set of encoders.
//!
//! - [`run`] builds the whole body, which is what a `/sync` answer with a `Content-Length`
//!   is.
//! - [`write()`] puts each piece into a job's file: the peak is then one batch and one chunk
//!   rather than the rows and the whole document, and the ceiling on what a job may keep
//!   refuses at the byte that passes it instead of once all of it is in memory. Parquet is
//!   the one format whose chunk is a row group rather than a batch — the writer cannot emit
//!   a group before it is full — which is bounded and is still not the answer.
//! - [`stream()`] sends each piece as the rows arrive, which is `STREAMING=true`. The same
//!   peak as a job's, and nothing on disk.
//!
//! Nothing here knows it is inside a request future. The caller supplies the ceiling a route
//! allows and reads the answer out; the clock over the router is the sync route's alone —
//! which for a streamed body means the clock runs over the body too, and [`Streamed`] carries
//! the mark that says so.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::body::Body;
use datafusion::arrow::array::RecordBatch;
use futures::{Stream, StreamExt as _};

use crate::access::data::DataFiles;
use crate::adql;
use crate::adql::query::{Answer, Execution, Rows, Source, Table};
use crate::app::request::Format;
use crate::app::routes::tap::format::Answering;
use crate::app::routes::tap::parameters::Parameters;
use crate::app::routes::tap::published::describe;
use crate::app::routes::tap::upload::{Kind, UPLOAD_SCHEMA, Upload};
use crate::app::service::Service;
use crate::error::ApiError;
use crate::output::{dsv, parquet, stream, votable};
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
    /// document; the delimited formats and parquet have nowhere to put it.
    pub overflow: bool,
    pub num_rows: usize,
    pub data_bytes_read: u64,
    /// The tables the statement named, as it named them, for the log.
    pub tables: Vec<String>,
}

/// Run one request's statement and keep the whole answer, which is what a `/sync` body is.
///
/// The body is bytes rather than text: every format here but parquet writes UTF-8, and one
/// that does not is the reason nothing between this and the response may assume otherwise.
pub(super) async fn run(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
) -> Result<(Vec<u8>, Answered), ApiError> {
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
        // A statement's scan, whatever it read, is not a count this document has a place
        // for: VOTable and the delimited formats say nothing about either.
        partitions: None,
        elapsed: started.elapsed(),
        overflow: execution.overflow(),
        // A bound that stopped this part-way is the row bound, which `overflow` already
        // says; a read that fails leaves the job in error with nothing kept, so there is
        // no half-written document here to explain itself.
        stopped: None,
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

/// One statement, answered as its rows arrive.
///
/// There is no [`Answered`] beside this body and there cannot be: the counts and the overflow
/// flag are known once the last row is in, and by then the headers have gone. Everything a
/// collected answer says in a header a streamed one says in the document or not at all —
/// which is the whole of what `STREAMING=true` costs, and why it is off unless asked for.
#[derive(Debug)]
pub(super) struct Streamed {
    pub content_type: &'static str,
    /// The tables the statement named, as it named them, for the log. The counts are not
    /// among them: the log line is written before a row has been read.
    pub tables: Vec<String>,
    pub body: Body,
}

/// The same statement, sent as its rows arrive.
///
/// **The head is written here, while a status can still be chosen.** `csv`, `tsv` and VOTable
/// each refuse a nested column in `begin`, and a refusal after the `200` is a document that
/// stops rather than a `400` naming the column — so `begin` is called before the response is
/// built and its error is this function's.
///
/// **A truncation ends the document rather than closing it.** A collected answer says it was
/// cut in `x-hats-overflow` where the format cannot say it itself; a streamed one sent that
/// header before it knew. So the ending carries [`stream::Stopped::Bound`] where the row
/// bound was reached, which VOTable writes as `OVERFLOW` after the table and which `csv`,
/// `tsv` and parquet answer by ending without their terminator — no closing footer on a
/// parquet file, no last chunk on a delimited one. A reader refuses all three, which is what
/// it must do: a parquet file that closed over a truncation is one that looks whole and holds
/// fewer rows than the query matched, and nothing in it says so.
pub(super) async fn stream(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
) -> Result<Streamed, ApiError> {
    let started = Instant::now();
    let prepared = prepare(service, parameters, ceiling).await?;
    let tables = prepared
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect();
    let mut encoder = encoder(answering)?;
    let execution = adql::query::plan(
        &prepared.translated,
        &prepared.tables,
        &prepared.data_files,
        prepared.limits,
    )
    .await?;
    let schema = Arc::clone(&execution.schema);
    let head = encoder.begin(&schema)?;

    // What the read cost, which only the `Execution` knows and which the ending is built
    // from once the rows have run out. It is behind a lock because the two halves are apart:
    // the execution is owned by the batch stream, and the ending is a closure the stream
    // driver calls after that stream has ended.
    let counted = Arc::new(Mutex::new(Counted::default()));
    let batches = reading(execution, Arc::clone(&counted));
    // The driver's own count is the same number and is not read: the rows, the bytes and the
    // overflow all come off the `Execution`, and taking two of the three from one place and
    // the third from another is how they come to disagree.
    let chunks = stream::streamed(encoder, head, batches, move |_rows| {
        let counted = counted.lock().map(|held| *held).unwrap_or_default();
        stream::Ending {
            rows: counted.rows,
            data_bytes_read: counted.data_bytes_read,
            // A statement's scan, whatever it read, is not a count this document has a place
            // for: VOTable and the delimited formats say nothing about either.
            partitions: None,
            elapsed: started.elapsed(),
            overflow: counted.overflow,
            stopped: counted.overflow.then(|| {
                stream::Stopped::Bound(format!(
                    "this answer was cut at {} rows by the row bound this request was given",
                    counted.rows
                ))
            }),
        }
    });
    Ok(Streamed {
        content_type: answering.content_type,
        tables,
        body: Body::from_stream(chunks),
    })
}

/// What a read had cost by the time its rows ran out.
#[derive(Debug, Default, Clone, Copy)]
struct Counted {
    rows: usize,
    data_bytes_read: u64,
    overflow: bool,
}

/// One [`Execution`] as a stream of batches, leaving its counters where the ending can read
/// them.
///
/// **They are written after every batch rather than at the end**, because there are three
/// ways the rows stop and only one of them reaches a line after the loop: the batches run
/// out, the bound cuts them, or the read fails — and the last of those ends the stream at the
/// error, with the driver calling the ending straight after. Written each time, whatever
/// happened last is what the ending finds.
fn reading(
    execution: Execution,
    counted: Arc<Mutex<Counted>>,
) -> impl Stream<Item = Result<RecordBatch, ApiError>> + Send + 'static {
    futures::stream::unfold(Some(execution), move |state| {
        let counted = Arc::clone(&counted);
        async move {
            let mut execution = state?;
            let next = execution.next().await;
            if let Ok(mut held) = counted.lock() {
                *held = Counted {
                    rows: execution.num_rows(),
                    data_bytes_read: execution.data_bytes_read(),
                    overflow: execution.overflow(),
                };
            }
            next.map(|batch| (batch, Some(execution)))
        }
    })
    // An `unfold` panics when it is polled after returning `None`, and it panics on the
    // worker rather than failing the request, so nothing in the response says what happened.
    // `stream::streamed` does not poll this again — it moves to its own end phase on the
    // `None` and never reaches the batches after that — but that is an assumption about a
    // driver this does not own, and it is the assumption `StreamBody` held until a
    // compression layer was wrapped around it.
    .fuse()
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
/// **A job's parquet answer is not a streamed response.** The pieces go into a file that is
/// served afterwards with a length and ranged reads, which is what a parquet reader needs to
/// open one at a url.
///
/// The layout is [`parquet::SourceLayout::default`]: a statement reads a catalog, or several
/// tables it joined, and there is no one source file whose bloom filters or writer version
/// this answer could be said to inherit.
fn encoder(answering: Answering) -> Result<Box<dyn stream::Encoder>, ApiError> {
    Ok(match answering.format {
        Format::Votable => Box::new(votable::Document::new(None)),
        Format::Dsv(kind) => Box::new(dsv::Delimited::new(kind, DSV_NULL)),
        Format::Parquet => Box::new(parquet::Writing::new(parquet::SourceLayout::default())),
        // The spelling table is the only source of a format here, and it holds these three.
        other => {
            return Err(ApiError::internal(format!(
                "{} is not a format this resource writes",
                other.name()
            )));
        }
    })
}

/// The rows, in the format the request asked for.
///
/// **Only a VOTable is written differently when the rows were cut.** `OVERFLOW` is a VOTable
/// marker; `csv`, `tsv` and parquet have nowhere in the document to put one and say it in
/// `x-hats-overflow` instead, which is what `app::routes::tap::answer` is for. A parquet file
/// that carried the fact would have to carry it in key/value metadata no reader looks at,
/// which is the same as nowhere and worse for looking like somewhere.
fn encode(answer: &Answer, answering: Answering) -> Result<Vec<u8>, ApiError> {
    match (answering.format, answer.overflow) {
        (Format::Votable, false) => votable::encode(&answer.result).map(String::into_bytes),
        (Format::Votable, true) => {
            votable::encode_truncated(&answer.result).map(String::into_bytes)
        }
        (Format::Dsv(kind), _) => {
            dsv::encode(&answer.result, kind, DSV_NULL).map(String::into_bytes)
        }
        (Format::Parquet, _) => parquet::encode(&answer.result, parquet::SourceLayout::default()),
        // The spelling table is the only source of a format here, and it holds these three.
        (other, _) => Err(ApiError::internal(format!(
            "{} is not a format this resource writes",
            other.name()
        ))),
    }
}
