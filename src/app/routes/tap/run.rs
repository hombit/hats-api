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
//!   a group before it is full — which is bounded and is still not the answer. A VOTable
//!   whose head has to see every row first reads them into a spool beside the file and
//!   writes the document from there, which [`measured`] says more about.
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
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use futures::{Stream, StreamExt as _};
use url::Url;

use crate::access::data::DataFiles;
use crate::adql;
use crate::adql::query::{Answer, Execution, Rows, Source, Table};
use crate::app::request::Format;
use crate::app::routes::tap::format::Answering;
use crate::app::routes::tap::parameters::Parameters;
use crate::app::routes::tap::published::describe;
use crate::app::routes::tap::upload::{Kind, Part, Source as UploadSource, UPLOAD_SCHEMA, Upload};
use crate::app::service::Service;
use crate::app::uploaded::{self, Budget, Sniffed};
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
/// allows, it being optional, and which the job record answers anyway. A document written
/// from a spool by [`measured`] has counted its rows by then and carries it.
pub(super) async fn write(
    service: &Service,
    parameters: &Parameters,
    answering: Answering,
    ceiling: adql::query::Limits,
    into: &mut Writing,
) -> Result<Answered, ApiError> {
    let started = Instant::now();
    let prepared = prepare(service, parameters, ceiling).await?;
    let mut execution = adql::query::plan(
        &prepared.translated,
        &prepared.tables,
        &prepared.data_files,
        prepared.limits,
    )
    .await?;

    let mut encoder =
        if matches!(answering.format, Format::Votable) && votable::measures(&execution.schema) {
            measured(&mut execution, into).await?
        } else {
            let mut encoder = encoder(answering)?;
            into.write(&encoder.begin(&execution.schema)?).await?;
            while let Some(batch) = execution.next().await {
                into.write(&encoder.rows(&batch?)?).await?;
            }
            encoder
        };
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

/// A job's VOTable that has to see every row before its head: every row is read into a spool
/// beside the answer, measured on the way, and the document is then written from the spool.
///
/// **This is the one answer read twice, and it is the job's alone.** A list of strings is
/// written at one width that the `FIELD` declares ahead of the rows, so a document written as
/// they arrive refuses such a column; `/sync` measures the rows it has already collected. A
/// job's answer is a file whichever way it is written, so it can afford the same answer with
/// the peak still one batch — the rows wait on disk rather than in memory. A streamed `/sync`
/// body has nowhere to put them and keeps the refusal.
///
/// The spool is Arrow IPC, the rows' own layout, so what is read back is the batches as the
/// statement produced them. Reading it is blocking file I/O and goes on a blocking thread, a
/// batch at a time.
async fn measured(
    execution: &mut Execution,
    into: &mut Writing,
) -> Result<Box<dyn stream::Encoder>, ApiError> {
    let schema = Arc::clone(&execution.schema);
    let spooling = |error: datafusion::arrow::error::ArrowError| {
        ApiError::internal(format!("cannot spool a job's rows: {error}"))
    };
    let mut spool = into.spool();
    let mut measuring = votable::Measuring::new(&schema);
    // Written into memory a batch at a time and moved to the file from there, so the only
    // blocking writer is one over a buffer.
    let mut ipc = StreamWriter::try_new(Vec::new(), &schema).map_err(spooling)?;
    spool.write(&std::mem::take(ipc.get_mut())).await?;
    while let Some(batch) = execution.next().await {
        let batch = batch?;
        measuring.rows(&batch);
        ipc.write(&batch).map_err(spooling)?;
        spool.write(&std::mem::take(ipc.get_mut())).await?;
    }
    ipc.finish().map_err(spooling)?;
    spool.write(&std::mem::take(ipc.get_mut())).await?;

    let mut encoder: Box<dyn stream::Encoder> =
        Box::new(measuring.document(Some(execution.num_rows())));
    into.write(&encoder.begin(&schema)?).await?;
    let path = spool.finish().await?.to_path_buf();
    let (sender, mut batches) = tokio::sync::mpsc::channel(1);
    let reading = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)
            .map_err(|error| ApiError::internal(format!("cannot read a job's rows: {error}")))?;
        let reader =
            StreamReader::try_new(std::io::BufReader::new(file), None).map_err(spooling)?;
        for batch in reader {
            // A closed channel is the writer having stopped, whose error is its own.
            if sender.blocking_send(batch.map_err(spooling)).is_err() {
                break;
            }
        }
        Ok::<_, ApiError>(())
    });
    while let Some(batch) = batches.recv().await {
        into.write(&encoder.rows(&batch?)?).await?;
    }
    reading
        .await
        .map_err(|error| ApiError::internal(format!("a job's rows were not read: {error}")))??;
    drop(spool);
    Ok(encoder)
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
///
/// **A bound of no rows is the exception, and it is not a truncation.** `MAXREC=0` reaches the
/// bound by construction — the statement is planned and never run — so every request that
/// writes it sets `overflow`, and ending the document there would leave `csv`, `tsv` and
/// parquet unreadable. What that request asked for is the columns, which DALI §3.4.4 has come
/// back with the indicator beside them, and a body no reader opens is not the columns. It is
/// also the one bound a caller cannot be misled by: they wrote the zero.
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
    // Whether reaching the bound cuts anything away. `MAXREC=0` sets `overflow` on a request
    // that asked for no rows, and a document ended over that is a schema the caller cannot
    // read rather than a truncation they cannot see.
    let cuts = prepared.limits.max_rows > 0;
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
            stopped: (counted.overflow && cuts).then(|| {
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
    // Every table the request brings with it comes out of one allowance: the parts it sent,
    // which were counted as they arrived, and the VOTables its urls name, counted here.
    let mut budget = Budget::new(service.max_upload_bytes);
    for upload in parameters.uploads.iter() {
        if let UploadSource::Inline(part) = &upload.source {
            budget.spend(part.bytes.len() as u64)?;
        }
    }
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
                let (source, files) =
                    uploaded(service, upload, spelling, &mut authorities, &mut budget).await?;
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
                Source::Catalog(dir, service.catalogs_for(url))
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
async fn uploaded<'a>(
    service: &'a Service,
    upload: &'a Upload,
    spelling: &'a str,
    authorities: &mut Authorities<'a>,
    budget: &mut Budget,
) -> Result<(Source, Option<DataFiles>), ApiError> {
    match &upload.source {
        UploadSource::Inline(part) => Ok((inline(upload, part).await?, None)),
        UploadSource::Url(url) => at_url(service, upload, url, spelling, authorities, budget).await,
    }
}

/// A table sent as a part of the request.
///
/// **Where `UPLOAD_TYPE` said nothing, the part's media type decides, and then its bytes.**
/// `pyvo` sends a part with no type at all, so the bytes are the ordinary case: a parquet
/// file says so in its first four, and a VOTable is XML whose root is `VOTABLE`.
async fn inline(upload: &Upload, part: &Part) -> Result<Source, ApiError> {
    let kind = match upload.kind {
        Some(kind) => kind,
        None => match part
            .content_type
            .as_deref()
            .and_then(uploaded::of_media_type)
            .unwrap_or_else(|| uploaded::sniff(&part.bytes))
        {
            Sniffed::Votable => Kind::Votable,
            Sniffed::Parquet => Kind::Parquet,
            Sniffed::Other => {
                return Err(ApiError::bad_request(format!(
                    "UPLOAD {} is neither a VOTable nor a parquet file; say which with \
                     UPLOAD_TYPE={},votable or UPLOAD_TYPE={},parquet",
                    upload.name, upload.name, upload.name
                )));
            }
        },
    };
    let provider = match kind {
        Kind::Votable => uploaded::votable_table(part.bytes.clone(), &upload.name).await?,
        Kind::Parquet => uploaded::parquet_table(part.bytes.clone(), &upload.name).await?,
        // `Uploads::read` refuses the pairing, a catalog being a directory.
        Kind::Hats => {
            return Err(ApiError::bad_request(format!(
                "UPLOAD {} is sent inline, and a catalog is a directory; name it by url",
                upload.name
            )));
        }
    };
    Ok(Source::Memory(provider))
}

/// A table a url names.
///
/// **Where `UPLOAD_TYPE` said nothing, the url is worked out in two steps.** A name matching
/// the data-file globs is a parquet file, read in place. Anything else is asked for its
/// first bytes: a VOTable is fetched and read into memory, and a url naming no object —
/// which is how a store answers a directory — is a catalog, as is an `http(s)` url answered
/// with a page.
///
/// **A VOTable is not held to the data-file globs, and a parquet file is.** The globs say
/// which files are *queried in place*, and a VOTable never is: it is read whole, which is
/// what serving its bytes would hand over anyway. A directory that is not one fails where a
/// catalog is opened, which is the read that looks for `hats.properties`, `properties` and
/// `collection.properties` and says so by name. The question costs one small read, and only
/// for a request that did not say.
async fn at_url<'a>(
    service: &'a Service,
    upload: &'a Upload,
    url: &'a Url,
    spelling: &'a str,
    authorities: &mut Authorities<'a>,
    budget: &mut Budget,
) -> Result<(Source, Option<DataFiles>), ApiError> {
    let files = service.data_files_for(url);
    let kind = match upload.kind {
        Some(kind) => kind,
        None if files.matches_url(url) => Kind::Parquet,
        // A url with no name at its end is a directory by construction, and `open` would
        // refuse it for naming no object.
        None if url.path().ends_with('/') || url.path().is_empty() => Kind::Hats,
        None => {
            let file = storage::open(url, &upload.storage, &service.policy, &service.transfers)?;
            match uploaded::head_of(&file)
                .await
                .map(|head| uploaded::sniff(&head))
            {
                Some(Sniffed::Votable) => Kind::Votable,
                // Refused below, by name, the data-file globs being what says which files
                // are read as data whatever their bytes are.
                Some(Sniffed::Parquet) => Kind::Parquet,
                None => Kind::Hats,
                // A page is how an HTTP server answers a directory's url, so there it is
                // still a catalog. Anywhere else a file that is there and is not a table is
                // neither, and says so rather than failing as a catalog.
                Some(Sniffed::Other) if matches!(url.scheme(), "http" | "https") => Kind::Hats,
                Some(Sniffed::Other) => {
                    return Err(ApiError::bad_request(format!(
                        "UPLOAD {} names a file that is neither a VOTable nor a parquet \
                         file; say which it is with UPLOAD_TYPE",
                        upload.name
                    )));
                }
            }
        }
    };
    match kind {
        Kind::Votable => {
            // Read whole and held for the request, so it is never registered as a store
            // and shares nobody's authority.
            let file = storage::open(url, &upload.storage, &service.policy, &service.transfers)?;
            let bytes = uploaded::fetch(&file, url, budget).await?;
            let provider = uploaded::votable_table(bytes, &upload.name).await?;
            Ok((Source::Memory(provider), None))
        }
        Kind::Parquet => {
            if !files.matches_url(url) {
                return Err(ApiError::not_found(format!(
                    "UPLOAD {} names no data file; a parquet url ends in a name matching {}",
                    upload.name,
                    files.describe()
                )));
            }
            let file = storage::open(url, &upload.storage, &service.policy, &service.transfers)?;
            authorities.check(spelling, file.opened(&upload.storage))?;
            Ok((Source::File(file), None))
        }
        Kind::Hats => {
            let files = files.clone();
            let dir = storage::open_dir(url, &upload.storage, &service.policy, &service.transfers)?;
            authorities.check(spelling, dir.opened(&upload.storage))?;
            Ok((Source::Catalog(dir, service.catalogs_for(url)), Some(files)))
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
