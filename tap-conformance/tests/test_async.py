"""The asynchronous query resource — TAP 1.1 section 2.2, UWS 1.1.

TAP requires it, and this service does not have it: a query too slow to answer inside
one request has nowhere to go here, and the answer is to make the query smaller. These
tests are marked as expected failures so that a report says so in as many words rather
than losing it among the things nobody decided.

They are not skipped, which is the difference that matters. A skip says nothing was
asked; an expected failure says it was asked and came back the way it was meant to —
and if async ever answers, the report says that instead.
"""

from __future__ import annotations

import urllib.error
import urllib.request

import pytest

from tap_conformance.taplint import assert_clean

ABSENT = "this service answers synchronously only"


@pytest.mark.xfail(reason=ABSENT, strict=False)
def test_job_submission(tap, rows_query, record_property):
    """A query submitted as a job runs and its rows can be collected."""
    found = tap.run_async(rows_query(2)).to_table()
    record_property("detail", f"an async job returned {len(found)} rows")
    assert len(found) == 2


def test_the_resource_is_cleanly_absent(service, record_property):
    """Absent means 404, not a hang and not a 500.

    A resource that is not implemented has one correct way to say so. This is the
    check that stays passing while the one above is an expected failure: how a service
    declines to offer something is its own business, but declining has to be legible.
    """
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
@pytest.mark.xfail(reason=ABSENT, strict=False)
def test_queries(stage, record_property):
    """Queries made in async mode are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("UWS")
@pytest.mark.xfail(reason=ABSENT, strict=False)
def test_uws_job_model(stage, record_property):
    """The job's phases, its polling and its destruction are UWS's."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
