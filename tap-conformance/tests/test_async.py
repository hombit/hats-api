"""The asynchronous query resource — TAP 1.1 section 2.2, UWS 1.1, DALI 1.1 section 2.1.

TAP requires it. A service that has no `/async` fails these, and that is all this file
says: whether some particular service has decided not to implement it is a fact about
that service's plans, and writing it down here would make the suite agree with whatever
was built — which is the one thing it is for not doing.

**What is here is what `taplint`'s UWS stage does not ask.** Its `JobStage` creates a
job, reads `/phase`, `/executionduration`, `/destruction` and `/quote`, checks that the
parameters it sent come back in the job document, POSTs a `runId` to `/parameters` and
`RUN` and `ABORT` to `/phase`, and deletes the job. It never fetches the job list, never
fetches `/error` or `/results`, never writes a destruction time or an execution
duration, and never asks what becomes of a job submitted without a `QUERY`. The
validator is stronger than anything hand-written wherever the two overlap, so nothing
below is a twin of one of those; every check names the clause it came from.

Two checks are about absence rather than presence. A feature that is absent has one
correct way to be absent — 404, not a hang and not a 500 — and a service with no async
at all must *fail* the rest rather than pass them vacuously, which is why every check
here submits a real job first and asserts the submission worked.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ElementTree
from datetime import UTC, datetime, timedelta

import pytest

from tap_conformance.taplint import assert_clean
from tap_conformance.votable import overflow, statuses

#: The UWS namespace, which is 1.0's even in UWS 1.1.
UWS = "http://www.ivoa.net/xml/UWS/v1.0"

#: The phases a job does not leave.
TERMINAL = frozenset({"COMPLETED", "ERROR", "ABORTED", "ARCHIVED"})

#: How long a check waits for a job it started to reach one of those.
SETTLE_SECONDS = 60.0


@pytest.fixture(scope="session")
def job_table(published) -> str:
    """A table these checks can run a job against.

    Not the `queryable` fixture the rest of the suite uses, which skips where the
    service publishes no catalog of its own. What is being measured here is the job
    resource and not the data, and TAP §4 requires every service to have `TAP_SCHEMA` —
    so falling back to it keeps the whole file asking something of a service with no
    catalogs published, rather than skipping into a report indistinguishable from one
    where the questions were put and answered.
    """
    for name in published:
        if not name.upper().startswith("TAP_SCHEMA"):
            return name
    return "TAP_SCHEMA.tables"


@pytest.fixture(scope="session")
def job_query(job_table):
    """`SELECT TOP n *` against that table."""

    def query(count: int = 5) -> str:
        return f"SELECT TOP {count} * FROM {job_table}"

    return query


@pytest.fixture(scope="session")
def session(tap):
    """The HTTP session `pyvo` itself uses.

    These checks read statuses, headers and bytes — a 303 and its `Location`, a 404, the
    root element of a document — which no client API exposes. Borrowing pyvo's own
    session rather than building a second one keeps every request one a client's stack
    really sends: the same headers, the same redirect handling, the same timeouts.
    """
    return tap._session  # noqa: SLF001 — the transport under the client, deliberately


def submitted(session, service, **parameters) -> str:
    """Create a job, and return where the service said it is.

    UWS §2.2.3.1: "The response when a job is accepted must have code 303 'See other'
    and the Location header of the response must point to the created job." DALI §2.1
    says the same of every DALI-async resource.

    Every check goes through here, so a service with no `/async` fails all of them
    rather than passing whichever ones a blanket 404 happens to satisfy.
    """
    answered = session.post(
        f"{service.base_url}/async", data=parameters, allow_redirects=False
    )
    assert answered.status_code == 303, (
        f"creating a job answered {answered.status_code} where UWS §2.2.3.1 requires "
        f"303: {answered.content[:200]!r}"
    )
    location = answered.headers.get("Location")
    assert location, "the 303 carries no Location, so nothing says where the job is"
    return urllib.parse.urljoin(f"{service.base_url}/async", location)


def document(session, url: str):
    """One UWS resource, parsed."""
    answered = session.get(url)
    assert answered.status_code == 200, (
        f"{url} answered {answered.status_code}: {answered.content[:200]!r}"
    )
    return ElementTree.fromstring(answered.content)


def phase_of(session, job: str) -> str:
    """The job's phase, read from the job document rather than from `/phase`.

    `taplint` already holds the two to each other; what is wanted here is one of them.
    """
    found = document(session, job).find(f"{{{UWS}}}phase")
    assert found is not None, "the job document carries no phase"
    return (found.text or "").strip().upper()


def settled(session, job: str, timeout: float = SETTLE_SECONDS) -> str:
    """Run the job and wait for it to stop, returning the phase it stopped in.

    `PHASE=RUN` goes to the phase resource, which is where UWS's REST binding puts the
    phase. Posting it to the job URL instead is not something the standard describes —
    UWS names that URL for *parameters*, and DALI then forbids even those there — so a
    service that declines it is within its rights and this helper would be testing a
    shape nobody specified.
    """
    session.post(f"{job}/phase", data={"PHASE": "RUN"}, allow_redirects=False)
    deadline = time.monotonic() + timeout
    phase = phase_of(session, job)
    while phase not in TERMINAL and time.monotonic() < deadline:
        time.sleep(0.5)
        phase = phase_of(session, job)
    assert phase in TERMINAL, f"the job was still {phase} after {timeout:.0f}s"
    return phase


def destroy(session, job: str) -> None:
    """Give the job back, so a run does not leave a service full of them."""
    try:
        session.delete(job, allow_redirects=False)
    except Exception:  # noqa: BLE001 — tidying up is not what is being measured
        pass


# ------------------------------------------------------------------ the job runs


def test_job_submission(tap, job_query, record_property):
    """A query submitted as a job runs and its rows can be collected."""
    found = tap.run_async(job_query(2)).to_table()
    record_property("detail", f"an async job returned {len(found)} rows")
    assert len(found) == 2


def test_the_lifecycle_a_client_drives(tap, job_query, record_property):
    """Submit, run, wait, fetch, delete — each step separately, as a client does.

    `run_async` does the whole thing in one call and hides which step failed. TOPCAT
    and any client showing a progress bar drive the steps themselves, so a service can
    answer the one-shot and still be one nobody can watch.
    """
    job = tap.submit_job(job_query(2))
    try:
        started = job.phase
        job.run()
        job.wait(phases={"COMPLETED", "ERROR", "ABORTED"}, timeout=SETTLE_SECONDS)
        record_property("detail", f"{started} → {job.phase}")
        assert job.phase == "COMPLETED", f"the job ended {job.phase}"
        assert len(job.fetch_result().to_table()) == 2
    finally:
        try:
            job.delete()
        except Exception:  # noqa: BLE001 — tidying up is not what is being measured
            pass


def test_the_result_is_named_result(session, service, job_query, record_property):
    """The result is reachable at `results/result`.

    TAP §2.2: "the result must be named result and thus clients can access it
    directly". It is what lets a client build the URL instead of reading the result
    list, and every client does.
    """
    job = submitted(session, service, QUERY=job_query(2), LANG="ADQL")
    try:
        assert settled(session, job) == "COMPLETED"
        answered = session.get(f"{job}/results/result")
        record_property(
            "detail",
            f"results/result → {answered.status_code}, "
            f"{answered.headers.get('Content-Type', '?')}",
        )
        assert answered.status_code == 200, (
            f"results/result answered {answered.status_code}, so the result is not "
            f"where TAP §2.2 says a client may look for it"
        )
        assert b"<VOTABLE" in answered.content[:8000].upper(), (
            f"the result is not a VOTable: {answered.content[:200]!r}"
        )
    finally:
        destroy(session, job)


def test_an_empty_result_still_exists(session, service, job_table, record_property):
    """A query matching nothing still has a result resource, with no rows in it.

    TAP §2.2: "If the query returned no rows, the result resource must exist and
    contain no data rows." A 404 there is indistinguishable, to a client, from a job
    whose result was never produced.
    """
    query = f"SELECT * FROM {job_table} WHERE 1 = 0"
    job = submitted(session, service, QUERY=query, LANG="ADQL")
    try:
        assert settled(session, job) == "COMPLETED"
        answered = session.get(f"{job}/results/result")
        record_property("detail", f"an empty result answered {answered.status_code}")
        assert answered.status_code == 200, (
            f"a query matching no rows answered {answered.status_code} at "
            f"results/result rather than an empty table"
        )
        assert b"<TR" not in answered.content.upper(), "the empty result carries rows"
    finally:
        destroy(session, job)


def test_maxrec_is_marked_on_a_job_result(
    session, service, job_table, record_property
):
    """A job result cut by MAXREC carries the overflow marker after the table.

    DALI §3.4.4 applies to async as it does to sync, and §4.4.1 puts the marker after
    the table. The sync half is checked elsewhere; this is the resource where a client
    comes back for the document later, having not seen the request that made it — so
    the document is the only thing that can tell it the rows are not all of them.
    """
    query = f"SELECT TOP 20 * FROM {job_table}"
    job = submitted(session, service, QUERY=query, LANG="ADQL", MAXREC="2")
    try:
        assert settled(session, job) == "COMPLETED"
        body = session.get(f"{job}/results/result").content
        marker = overflow(body)
        record_property("detail", f"markers: {[str(s) for s in statuses(body)] or 'none'}")
        assert marker is not None, (
            "a result cut at MAXREC carries no OVERFLOW, so a client cannot tell it "
            "from the whole answer"
        )
        assert marker.after_table, f"the marker is written {marker}, not after the table"
    finally:
        destroy(session, job)


# --------------------------------------------------------- when a job goes wrong


def test_a_failed_job_has_an_error_document(session, service, record_property):
    """A job that fails ends in ERROR with a DALI error document at `/error`.

    TAP §2.2: "Failed TAP queries produce an error document (see section 3.3) which
    must be accessible as the error resource". §3.3 defers to DALI §4.4, which is the
    INFO carrying QUERY_STATUS="ERROR". A job that fails with nothing at `/error`
    leaves a client with a phase and no way to tell a user what went wrong.
    """
    job = submitted(session, service, QUERY="SELECT FROM WHERE", LANG="ADQL")
    try:
        phase = settled(session, job)
        assert phase == "ERROR", f"a malformed query ended {phase} rather than ERROR"
        answered = session.get(f"{job}/error")
        said = [status.value for status in statuses(answered.content[:8000])]
        record_property("detail", f"error → {answered.status_code}, markers {said}")
        assert answered.status_code == 200, (
            f"the error resource answered {answered.status_code}, so the document TAP "
            f"§2.2 requires is not there"
        )
        assert "ERROR" in said, (
            f"the error document carries no QUERY_STATUS=ERROR: "
            f"{answered.content[:200]!r}"
        )
    finally:
        destroy(session, job)


def test_parameters_are_checked_when_the_job_runs(session, service, record_property):
    """A job with no QUERY is still created, and fails when it is run.

    TAP §2.7 is explicit and easy to implement the other way round: "Requirements on
    the presence and values of parameters described below are enforced only when the
    TAP request is executed (not when individual HTTP requests are handled). Thus, for
    asynchronous TAP queries, the parameter requirements must be satisfied (and errors
    returned if not) only when the query is run (in the sense of UWS job execution)."

    A service that refuses the submission instead breaks the one workflow the rule
    exists for: create a job PENDING, POST its parameters one at a time, then run it.
    """
    job = submitted(session, service, LANG="ADQL")
    try:
        record_property("detail", f"created without a QUERY, phase {phase_of(session, job)}")
        phase = settled(session, job)
        assert phase == "ERROR", (
            f"a job with no QUERY ended {phase}; the missing parameter has to be the "
            f"job's error, TAP §2.7 putting the check at run time"
        )
    finally:
        destroy(session, job)


def test_an_unknown_job_is_not_found(session, service, job_query, record_property):
    """An id that names no job answers 404.

    UWS §2.2: "If a request is made to a resource that does not exist (e.g. an
    non-existent job-id) then a 404 error should be returned."

    A real job is submitted first and nothing else. Without it this passes against a
    service with no TAP at all, every URL there being a 404 — which is the vacuous pass
    this file exists to avoid.
    """
    job = submitted(session, service, QUERY=job_query(1), LANG="ADQL")
    destroy(session, job)
    answered = session.get(f"{service.base_url}/async/no-such-job-identifier")
    record_property("detail", f"an unknown job answered {answered.status_code}")
    assert answered.status_code == 404, (
        f"an id naming no job answered {answered.status_code} rather than 404"
    )


# -------------------------------------------------------------- the job list


def test_the_job_list_is_a_jobs_document(session, service, job_query, record_property):
    """`GET /async` is a UWS job list.

    UWS §2.2.2.1: "The service should return a list (which may be empty) of all the
    jobs (except for jobs in the 'ARCHIVED' phase for backward compatibility) that the
    client can see in the current security context."

    Which jobs are in it is deliberately not checked. "May be empty" and "the current
    security context" put that with the service's authorization policy, and §3 says as
    much — a caller "might only obtain a restricted list of jobs within the joblist".
    A service where a job is visible only to whoever holds its id answers an empty list
    and is conforming; requiring the job to appear would be a check written against one
    policy rather than against the standard.
    """
    job = submitted(session, service, QUERY=job_query(1), LANG="ADQL")
    try:
        root = document(session, f"{service.base_url}/async")
        refs = root.findall(f"{{{UWS}}}jobref")
        record_property("detail", f"<{root.tag.split('}')[-1]}> with {len(refs)} jobref")
        assert root.tag == f"{{{UWS}}}jobs", (
            f"the job list root is {root.tag}, not a UWS jobs document"
        )
    finally:
        destroy(session, job)


def test_the_job_list_takes_the_uws_1_1_filters(
    session, service, job_query, record_property
):
    """`PHASE`, `AFTER` and `LAST` narrow the job list rather than breaking it.

    UWS 1.1 added all three, and `LAST` is what a client uses to show a user their
    recent jobs without pulling every job the service has ever kept. A service that
    refuses them answers nothing a 1.1 client asked for; one that returns more than
    `LAST` asked for has ignored a parameter it acted on the appearance of.
    """
    job = submitted(session, service, QUERY=job_query(1), LANG="ADQL")
    try:
        since = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        seen = {}
        for name, query in [
            ("PHASE", "PHASE=EXECUTING"),
            ("AFTER", f"AFTER={since}"),
            ("LAST", "LAST=1"),
        ]:
            answered = session.get(f"{service.base_url}/async?{query}")
            seen[name] = answered.status_code
            assert answered.status_code == 200, (
                f"{query} answered {answered.status_code}; UWS 1.1 defines it on the "
                f"job list"
            )
            if name == "LAST":
                root = ElementTree.fromstring(answered.content)
                assert len(root.findall(f"{{{UWS}}}jobref")) <= 1, (
                    "LAST=1 returned more than one job"
                )
        record_property("detail", ", ".join(f"{k} → {v}" for k, v in seen.items()))
    finally:
        destroy(session, job)


# ------------------------------------------------- what a client may change


@pytest.mark.parametrize(
    ("resource", "element", "parameter", "wanted"),
    [
        ("destruction", "destruction", "DESTRUCTION", None),
        # The resource is lower-case in the URL and camel-case in the document; UWS spells
        # them differently and a check that assumed one name found neither.
        ("executionduration", "executionDuration", "EXECUTIONDURATION", "30"),
    ],
    ids=["destruction", "executionduration"],
)
def test_a_writable_value_is_honoured_or_refused(
    session, service, job_query, resource, element, parameter, wanted, record_property
):
    """Writing one changes it, or is refused — never accepted and dropped.

    UWS §2.1 on the destruction time: "The client may write to the Destruction Time to
    try to change the life expectancy of the job. The service may forbid changes, or
    may set limits on the allowed destruction time." Both answers are conforming. What
    is not is a 2xx or a 303 that leaves the value as it was, which a client reads as
    having set something it has not set — and for a destruction time that means
    believing a result will still be there tomorrow.

    Each asks for *less* than the job already has, so a service imposing a maximum has
    no reason to clamp and a value that did not move really did not move.
    """
    job = submitted(session, service, QUERY=job_query(1), LANG="ADQL")
    try:
        before = (document(session, job).find(f"{{{UWS}}}{element}").text or "").strip()
        if wanted is None:
            wanted = (datetime.now(UTC) + timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        answered = session.post(
            f"{job}/{resource}", data={parameter: wanted}, allow_redirects=False
        )
        after = (document(session, job).find(f"{{{UWS}}}{element}").text or "").strip()
        record_property(
            "detail",
            f"{before!r} → asked {wanted!r} → {after!r} ({answered.status_code})",
        )
        if answered.status_code >= 400:
            assert before == after, (
                f"{resource} was refused with {answered.status_code} and changed anyway"
            )
            return
        assert after != before, (
            f"{resource} answered {answered.status_code} and stayed {after!r}, so the "
            f"value was accepted and dropped"
        )
    finally:
        destroy(session, job)


def test_wait_returns_within_its_bound(session, service, job_query, record_property):
    """`?WAIT=n` on a pending job blocks and then answers, rather than hanging.

    UWS 1.1 adds blocking, "restricted to the /{jobs}/{job-id} endpoint", and lets a
    service "impose a maximum blocking time" — so answering early is conforming and
    answering late is not. A service that ignores it answers at once, which is also
    conforming and costs a polling client nothing; what this catches is the one that
    blocks and never returns.
    """
    job = submitted(session, service, QUERY=job_query(1), LANG="ADQL")
    try:
        asked = 5
        started = time.monotonic()
        answered = session.get(f"{job}?WAIT={asked}", timeout=asked + 30)
        took = time.monotonic() - started
        record_property("detail", f"WAIT={asked} answered {answered.status_code} in {took:.1f}s")
        assert answered.status_code == 200, (
            f"WAIT={asked} answered {answered.status_code}"
        )
        assert took < asked + 20, (
            f"WAIT={asked} took {took:.1f}s, so the bound the client asked for is not "
            f"one the service keeps"
        )
    finally:
        destroy(session, job)


# ------------------------------------------------------------ absence, and the linter


def test_the_resource_is_cleanly_absent(service, tap, record_property):
    """Absent means 404, not a hang and not a 500.

    A resource that is not implemented has one correct way to say so: how a service
    declines to offer something is its own business, but declining has to be legible.

    It asks for the capabilities first, and skips if there are none. Otherwise this
    passes against a service that has no TAP whatsoever — everything is 404 there, this
    one included — and a suite that scores points against nothing is measuring nothing.
    """
    try:
        tap.capabilities
    except Exception:  # noqa: BLE001 — whether it is a TAP service at all
        pytest.skip("no capabilities document, so there is no TAP here to be missing a bit of")

    url = f"{service.base_url}/async"
    try:
        with urllib.request.urlopen(url, timeout=30) as answered:
            status = answered.status
    except urllib.error.HTTPError as answered:
        status = answered.code
    except OSError as unreachable:
        raise AssertionError(f"{url} could not be reached: {unreachable}") from None
    record_property("detail", f"{url} → {status}")
    assert status in (200, 404, 405), (
        f"/async answered {status}, which is neither offering the resource nor declining it"
    )


@pytest.mark.taplint("QAS")
def test_queries(stage, record_property):
    """Queries made in async mode are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("UWS")
def test_uws_job_model(stage, record_property):
    """The job's phases, its polling and its destruction are UWS's."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
