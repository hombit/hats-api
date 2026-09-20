//! The UWS documents: a job, and a list of them.
//!
//! UWS 1.1, whose XML namespace is still `v1.0` — the schema moved on and the namespace
//! deliberately did not, so a reader written against 1.0 still recognises the elements. The
//! `version="1.1"` attribute is what says which this is.
//!
//! Rendered here rather than in the route for the reason `tap::metadata` is: this is what
//! the service publishes over the protocol, and the route is how it is published. What the
//! documents may say about a job is also not the route's to decide — `UPLOAD_STORAGE_OPTION`
//! is left out of the parameters below, and that has to be true of every document that
//! carries them rather than of whichever handler remembered.

use std::fmt::Write as _;

use chrono::{DateTime, SecondsFormat, Utc};

use crate::tap::jobs::{Job, Phase};

/// The namespace, which is `v1.0` for every 1.x version.
const NAMESPACE: &str = "http://www.ivoa.net/xml/UWS/v1.0";

/// The version this service writes, which is what `WAIT` and the job-list filters need.
const VERSION: &str = "1.1";

/// TAP §2.2: "the result must be named result".
pub const RESULT: &str = "result";

/// The parameter whose value is a credential, and so is in no document.
///
/// UWS §2.1.11 asks for "an enumeration of the Job parameters" and requires no completeness,
/// so leaving one out is within the standard and needs no masking spelling invented for it.
/// A job outlives the request that made it and is readable by whoever holds its id, so
/// echoing this would publish a caller's secret rather than return it to them.
const WITHHELD: &str = "UPLOAD_STORAGE_OPTION";

/// An instant as DALI §3.3.3 and UWS write one: ISO 8601, UTC, to the millisecond.
fn instant(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Escape text for an element's content or an attribute's value.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

/// One job, as `GET /async/{id}` answers it.
///
/// `base` is where this job is, so the result's `xlink:href` is one a client can follow.
pub fn job(job: &Job, base: &str) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<uws:job xmlns:uws=\"{NAMESPACE}\" \
         xmlns:xlink=\"http://www.w3.org/1999/xlink\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"{VERSION}\">"
    );
    let _ = writeln!(out, "<uws:jobId>{}</uws:jobId>", escape(job.id.as_str()));
    if let Some(run_id) = &job.run_id {
        let _ = writeln!(out, "<uws:runId>{}</uws:runId>", escape(run_id));
    }
    // Read off the record rather than decided here. It is `None` for every job this service
    // makes, there being no authentication, and UWS §2.1.8 has the object exist only "in
    // cases where the access to the service is authenticated" — so nil is right, and it is
    // right because the job says so rather than because this function assumes it.
    match &job.owner {
        Some(owner) => {
            let _ = writeln!(out, "<uws:ownerId>{}</uws:ownerId>", escape(owner));
        }
        None => out.push_str("<uws:ownerId xsi:nil=\"true\"/>\n"),
    }
    let _ = writeln!(out, "<uws:phase>{}</uws:phase>", job.phase);
    // Nothing here can estimate how long a query will take, and a number that is a guess is
    // worse than none: a client shows it to a user as a promise.
    out.push_str("<uws:quote xsi:nil=\"true\"/>\n");
    let _ = writeln!(
        out,
        "<uws:creationTime>{}</uws:creationTime>",
        instant(job.created)
    );
    match job.started {
        Some(at) => {
            let _ = writeln!(out, "<uws:startTime>{}</uws:startTime>", instant(at));
        }
        None => out.push_str("<uws:startTime xsi:nil=\"true\"/>\n"),
    }
    match job.ended {
        Some(at) => {
            let _ = writeln!(out, "<uws:endTime>{}</uws:endTime>", instant(at));
        }
        None => out.push_str("<uws:endTime xsi:nil=\"true\"/>\n"),
    }
    let _ = writeln!(
        out,
        "<uws:executionDuration>{}</uws:executionDuration>",
        job.execution_duration.num_seconds()
    );
    let _ = writeln!(
        out,
        "<uws:destruction>{}</uws:destruction>",
        instant(job.destruction)
    );

    parameter_list(&mut out, job);
    result_list(&mut out, job, base);

    if let Some(message) = &job.error {
        // `fatal`, never `transient`: nothing this service refuses becomes allowed by being
        // asked again, so a client told to retry would be told wrongly.
        let _ = writeln!(
            out,
            "<uws:errorSummary type=\"fatal\" hasDetail=\"true\">\n\
             <uws:message>{}</uws:message>\n</uws:errorSummary>",
            escape(message)
        );
    }
    out.push_str("</uws:job>\n");
    out
}

/// The parameters, as `GET /async/{id}/parameters` answers them.
///
/// The same element the job document carries, written by the same code — so the standalone
/// resource and the one inside the job cannot come to disagree about what was withheld.
pub fn parameters(job: &Job) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(out, "<uws:parameters xmlns:uws=\"{NAMESPACE}\">");
    for (name, value) in &job.parameters {
        if name.eq_ignore_ascii_case(WITHHELD) {
            continue;
        }
        let _ = writeln!(
            out,
            "<uws:parameter id=\"{}\">{}</uws:parameter>",
            escape(&name.to_lowercase()),
            escape(value)
        );
    }
    out.push_str("</uws:parameters>\n");
    out
}

/// The results, as `GET /async/{id}/results` answers them.
pub fn results(job: &Job, base: &str) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<uws:results xmlns:uws=\"{NAMESPACE}\" \
         xmlns:xlink=\"http://www.w3.org/1999/xlink\">"
    );
    if job.product.is_some() {
        let _ = writeln!(
            out,
            "<uws:result id=\"{RESULT}\" xlink:href=\"{}/results/{RESULT}\"/>",
            escape(base)
        );
    }
    out.push_str("</uws:results>\n");
    out
}

/// The `<uws:parameters>` block, inside a job document.
fn parameter_list(out: &mut String, job: &Job) {
    out.push_str("<uws:parameters>\n");
    for (name, value) in &job.parameters {
        if name.eq_ignore_ascii_case(WITHHELD) {
            continue;
        }
        let _ = writeln!(
            out,
            "<uws:parameter id=\"{}\">{}</uws:parameter>",
            escape(&name.to_lowercase()),
            escape(value)
        );
    }
    out.push_str("</uws:parameters>\n");
}

/// The `<uws:results>` block, inside a job document.
///
/// One result where there is one. TAP §2.2 requires the resource to exist for a query that
/// matched no rows — a completed job with an empty table — but a failed job has an error
/// document and no rows anywhere, so an href there would send a client to a `404`.
fn result_list(out: &mut String, job: &Job, base: &str) {
    out.push_str("<uws:results>\n");
    if job.product.is_some() {
        let _ = writeln!(
            out,
            "<uws:result id=\"{RESULT}\" xlink:href=\"{}/results/{RESULT}\"/>",
            escape(base)
        );
    }
    out.push_str("</uws:results>\n");
}

/// The job list, as `GET /async` answers it.
///
/// **Empty, and that is the visibility policy rather than a missing feature.** UWS §2.2.2.1
/// asks for "a list (which may be empty) of all the jobs … that the client can see in the
/// current security context", and §3 leaves what that means to the service. A job here is
/// visible to whoever holds its id — the id being the whole of the access control, there
/// being no authentication — so an anonymous caller's context holds nothing.
pub fn jobs<'a>(refs: impl Iterator<Item = (&'a Job, String)>) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<uws:jobs xmlns:uws=\"{NAMESPACE}\" \
         xmlns:xlink=\"http://www.w3.org/1999/xlink\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"{VERSION}\">"
    );
    for (job, href) in refs {
        // §2.2.2.1 leaves ARCHIVED out of the list "for backward compatibility"; this
        // service never enters that phase, so there is nothing to filter.
        let _ = writeln!(
            out,
            "<uws:jobref id=\"{}\" xlink:href=\"{}\">\n\
             <uws:phase>{}</uws:phase>\n\
             <uws:creationTime>{}</uws:creationTime>\n\
             </uws:jobref>",
            escape(job.id.as_str()),
            escape(&href),
            job.phase,
            instant(job.created)
        );
    }
    out.push_str("</uws:jobs>\n");
    out
}

/// What `/phase` answers, which UWS makes plain text rather than a document.
pub fn phase(phase: Phase) -> String {
    phase.as_str().to_owned()
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::tap::jobs::{Change, JobId, Product};

    const WHERE: &str = "http://h/tap/async/x";

    fn pending() -> Job {
        let now = DateTime::parse_from_rfc3339("2026-09-20T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        Job::new(
            JobId::new().unwrap(),
            vec![
                ("QUERY".to_owned(), "SELECT TOP 1 * FROM a".to_owned()),
                ("LANG".to_owned(), "ADQL".to_owned()),
            ],
            Some("run-7".to_owned()),
            TimeDelta::seconds(600),
            now + TimeDelta::days(1),
            now,
        )
    }

    /// Take a job to a terminal phase the way the runner would.
    fn finished(record: &mut Job, change: Change) {
        let now = Utc::now();
        record.apply(Change::Queue, now).unwrap();
        record.apply(Change::Start, now).unwrap();
        record.apply(change, now).unwrap();
    }

    #[test]
    fn a_pending_job_carries_what_uws_asks_for() {
        let document = job(&pending(), WHERE);
        for wanted in [
            "<uws:job ",
            "version=\"1.1\"",
            "http://www.ivoa.net/xml/UWS/v1.0",
            "<uws:phase>PENDING</uws:phase>",
            "<uws:runId>run-7</uws:runId>",
            "<uws:ownerId xsi:nil=\"true\"/>",
            "<uws:quote xsi:nil=\"true\"/>",
            "<uws:creationTime>2026-09-20T12:00:00.000Z</uws:creationTime>",
            "<uws:startTime xsi:nil=\"true\"/>",
            "<uws:executionDuration>600</uws:executionDuration>",
            "<uws:destruction>2026-09-21T12:00:00.000Z</uws:destruction>",
            "<uws:parameter id=\"query\">SELECT TOP 1 * FROM a</uws:parameter>",
        ] {
            assert!(
                document.contains(wanted),
                "{wanted} missing from {document}"
            );
        }
        // No result until there is one, and no errorSummary on a job that has not failed.
        assert!(
            document.contains("<uws:results>\n</uws:results>"),
            "{document}"
        );
        assert!(!document.contains("errorSummary"), "{document}");
    }

    /// The credential never reaches a document, whoever is reading it.
    #[test]
    fn the_storage_option_is_not_in_the_parameters() {
        let mut record = pending();
        record.parameters.push((
            "UPLOAD_STORAGE_OPTION".to_owned(),
            "t,secret_access_key,hunter2".to_owned(),
        ));
        record
            .parameters
            .push(("UPLOAD".to_owned(), "t,https://example.org/t".to_owned()));
        let document = job(&record, WHERE);
        assert!(!document.contains("hunter2"), "{document}");
        assert!(!document.contains("upload_storage_option"), "{document}");
        // The ones that are not a credential are still there.
        assert!(document.contains("https://example.org/t"), "{document}");
    }

    #[test]
    fn a_completed_job_points_at_its_result() {
        let mut record = pending();
        finished(
            &mut record,
            Change::Complete(Product {
                file: "f".to_owned(),
                content_type: "text/csv".to_owned(),
                bytes: 4,
                rows: 1,
                overflow: false,
            }),
        );
        let document = job(&record, WHERE);
        assert!(
            document.contains("<uws:phase>COMPLETED</uws:phase>"),
            "{document}"
        );
        assert!(
            document.contains(
                "<uws:result id=\"result\" xlink:href=\"http://h/tap/async/x/results/result\"/>"
            ),
            "{document}"
        );
    }

    /// A failed job has an errorSummary and no result: the rows are not somewhere else, they
    /// do not exist, and a `result` href would send a client to a 404.
    #[test]
    fn a_failed_job_says_why_and_points_at_nothing() {
        let mut record = pending();
        finished(&mut record, Change::Fail("no column <b> there".to_owned()));
        let document = job(&record, WHERE);
        assert!(
            document.contains("<uws:phase>ERROR</uws:phase>"),
            "{document}"
        );
        assert!(
            document.contains("type=\"fatal\"") && document.contains("no column &lt;b&gt; there"),
            "{document}"
        );
        assert!(!document.contains("<uws:result "), "{document}");
    }

    /// Well-formed and empty, which is what the visibility policy answers.
    #[test]
    fn the_job_list_is_a_document_even_with_nothing_in_it() {
        let document = jobs(std::iter::empty());
        assert!(document.contains("<uws:jobs "), "{document}");
        assert!(document.contains("</uws:jobs>"), "{document}");
        assert!(!document.contains("<uws:jobref"), "{document}");
    }

    /// A job's own text is somebody else's, so the document escapes it rather than letting
    /// a quote or an angle bracket end an element early.
    #[test]
    fn a_callers_text_cannot_break_the_document() {
        let mut record = pending();
        record.parameters = vec![(
            "QUERY".to_owned(),
            "SELECT * FROM a WHERE n = ']]></uws:parameter><x y=\"'".to_owned(),
        )];
        let document = job(&record, WHERE);
        assert!(!document.contains("<x y="), "{document}");
        assert_eq!(
            document.matches("</uws:parameter>").count(),
            1,
            "{document}"
        );
    }

    #[test]
    fn the_phase_resource_is_the_bare_word() {
        assert_eq!(phase(Phase::Executing), "EXECUTING");
    }
}
