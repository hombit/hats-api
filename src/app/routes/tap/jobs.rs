//! `{api.prefix}/tap/async`: the job list and everything under one job.
//!
//! A DALI-async resource, which TAP §2.2 makes a MUST alongside `/sync`. What runs here is
//! the same statement `/sync` runs, through the same [`super::run`], read by the same
//! [`super::parameters`] — the difference is only when, and where the answer is put.
//!
//! **A parameter is checked when the job runs, not when it is created.** TAP §2.7: the
//! requirements on them "must be satisfied (and errors returned if not) only when the query
//! is run (in the sense of UWS job execution)". So a `POST` with no `QUERY` creates a job
//! and redirects, and the refusal is the job's — `ERROR`, with the document at `/error`.
//! That is the one thing this resource does differently from `/sync`, and it is the
//! standard asking for it rather than a convenience.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::rejection::StringRejection;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, TimeDelta, Utc};
use tower_http::services::ServeFile;

use crate::adql;
use crate::app::routes::tap::answer::{OVERFLOW_HEADER, XML_CONTENT_TYPE, answered, base_url};
use crate::app::routes::tap::format;
use crate::app::routes::tap::parameters::{self, Parameters};
use crate::app::routes::tap::run;
use crate::app::service::Service;
use crate::config::{AsyncConfig, LimitsConfig};
use crate::error::ApiError;
use crate::output::votable;
use crate::tap::jobs::{
    Bounds, Change, Job, JobId, JobStore, MemoryJobStore, Rendered, Results, Runner, Slots, Work,
    Writing,
};
use crate::tap::uws;

/// Where this resource sits under the API's prefix.
const SEGMENT: &str = "tap/async";

/// The parameter whose value is a credential, and so is never stored with the rest.
const CREDENTIAL: &str = "UPLOAD_STORAGE_OPTION";

/// What a job may be given, and for how long.
#[derive(Debug, Clone, Copy)]
struct Allowance {
    default_execution: TimeDelta,
    max_execution: TimeDelta,
    default_destruction: TimeDelta,
    max_destruction: TimeDelta,
    max_wait: std::time::Duration,
}

/// Everything the job resource needs, built once at startup.
#[derive(Debug)]
pub struct Jobs {
    store: Arc<dyn JobStore>,
    runner: Arc<Runner>,
    results: Arc<Results>,
    allowance: Allowance,
}

impl Jobs {
    /// Build it, which is also where a deployment that cannot write results finds out.
    pub fn new(
        service: Service,
        limits: &LimitsConfig,
        config: &AsyncConfig,
    ) -> Result<Self, ApiError> {
        let results = Arc::new(Results::open(limits.scratch_dir.as_deref())?);
        let store: Arc<dyn JobStore> = Arc::new(MemoryJobStore::new(Bounds {
            max_jobs: config.max_jobs,
            max_result_bytes_total: config.max_result_bytes_total.as_u64(),
        }));
        let work: Arc<dyn Work> = Arc::new(Query {
            ceiling: ceiling(service.adql_limits, config),
            service,
        });
        let runner = Arc::new(Runner::new(
            Arc::clone(&store),
            Arc::clone(&results),
            work,
            Slots {
                max_running: config.max_running,
                max_result_bytes: config.max_result_bytes.as_u64(),
            },
        ));
        Ok(Self {
            store,
            runner,
            results,
            allowance: Allowance {
                default_execution: seconds(config.default_execution_seconds),
                max_execution: seconds(config.max_execution_seconds),
                default_destruction: seconds(config.default_destruction_seconds),
                max_destruction: seconds(config.max_destruction_seconds),
                max_wait: std::time::Duration::from_secs(config.max_wait_seconds),
            },
        })
    }

    /// Destroy whatever has reached its destruction time. Called on a timer by the service.
    pub async fn expire(&self) {
        self.runner.expire().await;
    }

    /// Where a finished job's rows are on disk.
    ///
    /// For a test that a destroyed job takes its file with it, which is the one thing about
    /// this resource no response can show: a record that went while its file stayed answers
    /// `404` either way.
    #[cfg(test)]
    pub(in crate::app) async fn result_path(&self, id: &str) -> Option<std::path::PathBuf> {
        let id: JobId = id.parse().ok()?;
        let job = self.store.get(&id).await.ok()??;
        job.product
            .as_ref()
            .map(|product| self.results.path(&product.file))
    }

    /// The job, or the `404` that an id naming none gets.
    async fn job(&self, id: &str) -> Result<Job, ApiError> {
        let missing = || ApiError::not_found("no job of that name");
        let id: JobId = id.parse()?;
        self.store.get(&id).await?.ok_or_else(missing)
    }
}

/// A saturating conversion, since a configured number of seconds is not a duration a
/// `TimeDelta` can fail to hold and there is nothing to report if it somehow were.
fn seconds(count: u64) -> TimeDelta {
    TimeDelta::try_seconds(count.min(i64::MAX as u64) as i64).unwrap_or(TimeDelta::MAX)
}

/// What a job's query may spend: `[limits]`, with `[tap.async.limits]` over the top.
///
/// Field by field rather than a second `Limits`, so a bound nobody overrode is the sync one
/// by construction and there is one list of names rather than two that drift.
fn ceiling(sync: adql::query::Limits, config: &AsyncConfig) -> adql::query::Limits {
    let over = &config.limits;
    adql::query::Limits {
        max_rows: over.max_rows.unwrap_or(sync.max_rows),
        max_memory_bytes: over
            .max_query_memory_bytes
            .map_or(sync.max_memory_bytes, |bytes| bytes.as_u64()),
        catalog: crate::hats::table::Limits {
            max_partitions: over.max_partitions.unwrap_or(sync.catalog.max_partitions),
            ..sync.catalog
        },
        ..sync
    }
}

/// Running one job's statement, which is running one statement.
#[derive(Debug)]
struct Query {
    service: Service,
    ceiling: adql::query::Limits,
}

#[async_trait]
impl Work for Query {
    async fn run(
        &self,
        job: Job,
        credentials: Vec<String>,
        into: &mut Writing,
    ) -> Result<Rendered, ApiError> {
        // The credentials go back where the caller wrote them, so that what is read here is
        // the request they sent rather than a second arrangement of it. They were taken out
        // on the way in, and only so that the store never holds one.
        let mut pairs = job.parameters.clone();
        pairs.extend(
            credentials
                .into_iter()
                .map(|value| (CREDENTIAL.to_owned(), value)),
        );
        let parameters = Parameters::read(&pairs)?;
        // Refused rather than ignored, and refused rather than accepted as a no-op. A job's
        // answer is already written as it is read — that is what `Writing` is — so what
        // `STREAMING` asks for on `/sync`, a job does by construction and cannot stop doing.
        // Accepting it would say the answer comes back differently, and it does not: the
        // result is a file either way, served with the length and the ranges a stream gives
        // up. A parameter this service acts on is honoured or refused, never dropped.
        if parameters.streaming {
            return Err(ApiError::bad_request(
                "STREAMING is the /sync resource's; a job's answer is written as it is read \
                 whatever this says, and is served from its result resource as a file with a \
                 length and ranged reads",
            ));
        }
        let answering = format::resolve(parameters.format.as_deref())?;
        // Into the file as it is encoded, rather than built whole and handed over: a job's
        // answer is a file at the end of it either way, and this way the rows and the
        // document are never both in memory.
        let answer = run::write(&self.service, &parameters, answering, self.ceiling, into).await?;
        tracing::info!(
            job = %job.id,
            tables = %answer.tables.join(","),
            format = answering.format.name(),
            runid = parameters.runid.as_deref().unwrap_or(""),
            num_rows = answer.num_rows,
            overflow = answer.overflow,
            data_bytes_read = answer.data_bytes_read,
            "tap async"
        );
        Ok(Rendered {
            content_type: answer.content_type.to_owned(),
            rows: answer.num_rows,
            overflow: answer.overflow,
        })
    }
}

// ----------------------------------------------------------------- the resources

/// `POST /async`: create a job.
pub(in crate::app) async fn create(
    State(state): State<crate::app::service::AppState>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    answered(created(&state, &headers, body).await)
}

async fn created(
    state: &crate::app::service::AppState,
    headers: &HeaderMap,
    body: Result<String, StringRejection>,
) -> Result<Response, ApiError> {
    let jobs = state.jobs()?;
    let pairs = parameters::posted(headers, body)?;
    // The one thing read at submission, and only so that it is not written down: everything
    // else about these parameters is the runner's to check when the query runs.
    let (kept, credentials) = split_credentials(pairs);
    let run_id = value(&kept, "RUNID").map(str::to_owned);

    let now = Utc::now();
    let id = JobId::new()?;
    let job = Job::new(
        id.clone(),
        kept.clone(),
        run_id,
        jobs.allowance.default_execution,
        now + jobs.allowance.default_destruction,
        now,
    );
    jobs.store.create(job).await?;
    jobs.runner.hold(&id, credentials);

    // UWS §2.2.3.1 notes the facility: a client may have the job "placed into a potentially
    // running state by adding ?PHASE=RUN".
    if value(&kept, "PHASE").is_some_and(|asked| asked.eq_ignore_ascii_case("RUN")) {
        jobs.runner.start(&id).await?;
    }
    Ok(see_other(&where_job(headers, &state.service, &id)))
}

/// `GET /async`: the job list.
pub(in crate::app) async fn list(State(state): State<crate::app::service::AppState>) -> Response {
    match state.jobs() {
        Ok(_) => document(uws::jobs(std::iter::empty())),
        Err(refused) => answered(Err(refused)),
    }
}

/// `GET /async/{id}`: the job, after blocking for as long as `WAIT` asked.
pub(in crate::app) async fn show(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    answered(shown(&state, &id, query.as_deref(), &headers).await)
}

async fn shown(
    state: &crate::app::service::AppState,
    id: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let jobs = state.jobs()?;
    let asked = parameters::pairs(query.unwrap_or_default());
    let job = waited(jobs, id, value(&asked, "WAIT")).await?;
    let base = where_job(headers, &state.service, &job.id);
    Ok(document(uws::job(&job, &base)))
}

/// Block while the job is in an active phase, for as long as the client asked and the
/// operator allows.
///
/// UWS 1.1: the blocking behaviour is "restricted to the `/{jobs}/{job-id}` endpoint", the
/// server "may block only when in one of the 'active' phases", and it "may impose a maximum
/// blocking time" — which is why the cap here is the standard's own allowance rather than a
/// shortfall. `WAIT=-1` means block indefinitely, so it becomes the cap.
async fn waited(jobs: &Jobs, id: &str, asked: Option<&str>) -> Result<Job, ApiError> {
    let job = jobs.job(id).await?;
    let Some(asked) = asked else {
        return Ok(job);
    };
    // Unreadable or zero is no wait at all. A number this service cannot parse is not worth
    // refusing a poll over — the answer without it is the same document, just sooner.
    let wanted = match asked.trim().parse::<i64>() {
        Ok(seconds) if seconds < 0 => jobs.allowance.max_wait,
        Ok(seconds) => {
            std::time::Duration::from_secs(seconds.unsigned_abs()).min(jobs.allowance.max_wait)
        }
        Err(_) => return Ok(job),
    };
    if !job.phase.is_active() || wanted.is_zero() {
        return Ok(job);
    }
    let was = job.phase;
    let until = tokio::time::Instant::now() + wanted;
    loop {
        tokio::time::sleep(POLL.min(until.saturating_duration_since(tokio::time::Instant::now())))
            .await;
        let job = jobs.job(id).await?;
        if job.phase != was || tokio::time::Instant::now() >= until {
            return Ok(job);
        }
    }
}

/// How often a blocking `GET` looks again. Short enough that a client's `WAIT` ends when the
/// phase does rather than a poll later, long enough that a held connection is not a spin.
const POLL: std::time::Duration = std::time::Duration::from_millis(250);

/// `DELETE /async/{id}`, and `POST /async/{id}` with `ACTION=DELETE`.
pub(in crate::app) async fn destroy(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    answered(destroyed(&state, &id, &headers).await)
}

pub(in crate::app) async fn act(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    let acted = async {
        let pairs = parameters::posted(&headers, body)?;
        match value(&pairs, "ACTION") {
            Some(action) if action.eq_ignore_ascii_case("DELETE") => {
                destroyed(&state, &id, &headers).await
            }
            // DALI is explicit that a DALI-async job url does not take job parameters; they
            // go to `/parameters`. So there is exactly one thing a POST here means.
            other => Err(ApiError::bad_request(format!(
                "ACTION {:?} is not something this resource does; it takes ACTION=DELETE, and \
                 a parameter goes to this job's parameters resource",
                other.unwrap_or("")
            ))),
        }
    };
    answered(acted.await)
}

async fn destroyed(
    state: &crate::app::service::AppState,
    id: &str,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let jobs = state.jobs()?;
    let id: JobId = id.parse()?;
    // Stop it before forgetting it: a job whose record is gone while its task runs on would
    // write a file nothing points at.
    jobs.runner.stop(&id);
    if let Some(job) = jobs.store.delete(&id).await?
        && let Some(product) = &job.product
    {
        jobs.results.remove(&product.file).await;
    }
    // §2.2.3.2 sends a client back to the job list, that being what is left.
    Ok(see_other(&where_list(headers, &state.service)))
}

/// `GET /async/{id}/phase`, and the other three that are one line of text.
pub(in crate::app) async fn phase(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    answered(text(&state, &id, |job| uws::phase(job.phase)).await)
}

pub(in crate::app) async fn quote(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    // Empty rather than a number: nothing here can estimate a query, and a guess is shown to
    // a user as a promise.
    answered(text(&state, &id, |_| String::new()).await)
}

pub(in crate::app) async fn owner(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    answered(text(&state, &id, |job| job.owner.clone().unwrap_or_default()).await)
}

pub(in crate::app) async fn execution_duration(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    answered(
        text(&state, &id, |job| {
            job.execution_duration.num_seconds().to_string()
        })
        .await,
    )
}

pub(in crate::app) async fn destruction(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    answered(text(&state, &id, |job| instant(job.destruction)).await)
}

async fn text(
    state: &crate::app::service::AppState,
    id: &str,
    say: impl Fn(&Job) -> String,
) -> Result<Response, ApiError> {
    let job = state.jobs()?.job(id).await?;
    Ok((
        [(header::CONTENT_TYPE, "text/plain;charset=UTF-8")],
        say(&job),
    )
        .into_response())
}

fn instant(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// `POST /async/{id}/phase`: `RUN` or `ABORT`.
pub(in crate::app) async fn set_phase(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    let acted = async {
        let jobs = state.jobs()?;
        let pairs = parameters::posted(&headers, body)?;
        let id: JobId = id.parse()?;
        match value(&pairs, "PHASE") {
            Some(asked) if asked.eq_ignore_ascii_case("RUN") => {
                jobs.runner.start(&id).await?;
            }
            Some(asked) if asked.eq_ignore_ascii_case("ABORT") => {
                jobs.runner.abort(&id).await?;
            }
            other => {
                return Err(ApiError::bad_request(format!(
                    "PHASE {:?} is not something a client sets; it sets RUN or ABORT",
                    other.unwrap_or("")
                )));
            }
        }
        Ok(see_other(&where_job(&headers, &state.service, &id)))
    };
    answered(acted.await)
}

/// `POST /async/{id}/destruction` and `…/executionduration`.
///
/// **Honoured, clamped, or refused — never taken and dropped.** UWS §2.1 lets a service
/// "forbid changes, or ... set limits"; what it does not allow is accepting a write and
/// leaving the value as it was, which a client reads as having set something it has not.
pub(in crate::app) async fn set_destruction(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    let acted = async {
        let jobs = state.jobs()?;
        let pairs = parameters::posted(&headers, body)?;
        let id: JobId = id.parse()?;
        let asked = value(&pairs, "DESTRUCTION").ok_or_else(|| {
            ApiError::bad_request("DESTRUCTION is the instant to destroy this job at")
        })?;
        let wanted = DateTime::parse_from_rfc3339(asked.trim())
            .map_err(|error| {
                ApiError::bad_request(format!(
                    "DESTRUCTION {asked:?} is not an ISO 8601 instant: {error}"
                ))
            })?
            .with_timezone(&Utc);
        // Clamped rather than refused: a client asking to keep a result longer than this
        // service keeps anything is asking for something reasonable, and the document it
        // reads back says what it really got.
        let ceiling = Utc::now() + jobs.allowance.max_destruction;
        jobs.store
            .apply(&id, Change::Destruction(wanted.min(ceiling)))
            .await?;
        Ok(see_other(&where_job(&headers, &state.service, &id)))
    };
    answered(acted.await)
}

pub(in crate::app) async fn set_execution_duration(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    let acted = async {
        let jobs = state.jobs()?;
        let pairs = parameters::posted(&headers, body)?;
        let id: JobId = id.parse()?;
        let asked = value(&pairs, "EXECUTIONDURATION").ok_or_else(|| {
            ApiError::bad_request("EXECUTIONDURATION is how many seconds this job may run")
        })?;
        let wanted = asked.trim().parse::<u64>().map_err(|_| {
            ApiError::bad_request(format!(
                "EXECUTIONDURATION {asked:?} is not a number of seconds"
            ))
        })?;
        // UWS makes 0 mean unlimited, which this service does not offer — so it means the
        // most it does offer, which is what the document then says.
        let wanted = match wanted {
            0 => jobs.allowance.max_execution,
            count => seconds(count).min(jobs.allowance.max_execution),
        };
        jobs.store
            .apply(&id, Change::ExecutionDuration(wanted))
            .await?;
        Ok(see_other(&where_job(&headers, &state.service, &id)))
    };
    answered(acted.await)
}

/// `GET /async/{id}/error`: the document that says why a job failed.
///
/// The same DALI §4.4 document every other refusal here is rendered as, which is what makes
/// a client's error handling one path rather than two.
pub(in crate::app) async fn error(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    let found = async {
        let job = state.jobs()?.job(&id).await?;
        let message = job.error.clone().ok_or_else(|| {
            ApiError::not_found(format!("this job is {} and has no error", job.phase))
        })?;
        Ok((
            [(header::CONTENT_TYPE, votable::CONTENT_TYPE)],
            votable::error(&message),
        )
            .into_response())
    };
    answered(found.await)
}

/// `GET /async/{id}/results`: what there is to collect.
pub(in crate::app) async fn results(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let found = async {
        let job = state.jobs()?.job(&id).await?;
        let base = where_job(&headers, &state.service, &job.id);
        Ok(document(uws::results(&job, &base)))
    };
    answered(found.await)
}

/// `GET /async/{id}/results/result`: the rows.
///
/// Served off disk by the same [`ServeFile`] a mounted file goes through, so a client
/// collecting a large answer gets the `Content-Length` and the ranged reads it would get
/// from the file server — which a body built in memory cannot offer.
pub(in crate::app) async fn result(
    State(state): State<crate::app::service::AppState>,
    Path((id, name)): Path<(String, String)>,
    request: Request,
) -> Response {
    let served = async {
        let jobs = state.jobs()?;
        let job = jobs.job(&id).await?;
        if name != uws::RESULT {
            return Err(ApiError::not_found(format!(
                "this job has no result named {name}; TAP names the one result {}",
                uws::RESULT
            )));
        }
        let product = job.product.as_ref().ok_or_else(|| {
            ApiError::not_found(format!(
                "this job is {} and has no result; a failed job says why at its error \
                 resource",
                job.phase
            ))
        })?;
        let path = jobs.results.path(&product.file);
        let mut response = ServeFile::new(&path)
            .try_call(request)
            .await
            .map_err(|error| {
                tracing::warn!(%error, job = %job.id, "serving a job result failed");
                ApiError::internal("cannot read this job's result")
            })?
            .into_response();
        let headers = response.headers_mut();
        // The format the request asked for, which `mime_guess` cannot know from a filename
        // that is a job id.
        if let Ok(value) = product.content_type.parse() {
            headers.insert(header::CONTENT_TYPE, value);
        }
        // The same thing `/sync` says, for the same reason: `csv`, `tsv` and `parquet` have
        // nowhere in the document to say they were cut, and this client did not see the
        // request.
        if let Ok(value) = product.overflow.to_string().parse() {
            headers.insert(header::HeaderName::from_static(OVERFLOW_HEADER), value);
        }
        Ok(response)
    };
    answered(served.await)
}

/// `GET /async/{id}/parameters`, and `POST` to add one while the job is `PENDING`.
pub(in crate::app) async fn job_parameters(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
) -> Response {
    let found = async {
        let job = state.jobs()?.job(&id).await?;
        Ok(document(uws::parameters(&job)))
    };
    answered(found.await)
}

pub(in crate::app) async fn set_job_parameters(
    State(state): State<crate::app::service::AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    let acted = async {
        let jobs = state.jobs()?;
        let pairs = parameters::posted(&headers, body)?;
        let id: JobId = id.parse()?;
        let (kept, credentials) = split_credentials(pairs);
        for (name, value) in kept {
            jobs.store
                .apply(&id, Change::Parameter { name, value })
                .await?;
        }
        if !credentials.is_empty() {
            jobs.runner.hold(&id, credentials);
        }
        Ok(see_other(&where_job(&headers, &state.service, &id)))
    };
    answered(acted.await)
}

// ---------------------------------------------------------------------- shared bits

/// Take the credential-bearing parameter out of the pairs.
///
/// **The one thing read at submission, and only so that it is not written down.** A job
/// outlives the request that made it, so the value has to be kept somewhere; the store is
/// the one place it must not be, a row-backed store being something that writes to disk and
/// a job document being something whoever holds the id can read.
fn split_credentials(pairs: Vec<(String, String)>) -> (Vec<(String, String)>, Vec<String>) {
    let mut kept = Vec::with_capacity(pairs.len());
    let mut credentials = Vec::new();
    for (name, value) in pairs {
        if name.eq_ignore_ascii_case(CREDENTIAL) {
            credentials.push(value);
        } else {
            kept.push((name, value));
        }
    }
    (kept, credentials)
}

/// One parameter's value, matched the way DALI §3.1 matches a name.
fn value<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(written, _)| written.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn where_list(headers: &HeaderMap, service: &Service) -> String {
    format!(
        "{}/{SEGMENT}",
        base_url(headers, service.api_prefix.as_deref().unwrap_or("/"))
    )
}

fn where_job(headers: &HeaderMap, service: &Service, id: &JobId) -> String {
    format!("{}/{id}", where_list(headers, service))
}

/// UWS §2.2.3.1: "The response when a job is accepted must have code 303 'See other' and the
/// Location header of the response must point to the created job."
fn see_other(url: &str) -> Response {
    (StatusCode::SEE_OTHER, [(header::LOCATION, url.to_owned())]).into_response()
}

fn document(body: String) -> Response {
    ([(header::CONTENT_TYPE, XML_CONTENT_TYPE)], body).into_response()
}
