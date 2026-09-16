"""MAXREC and overflow — TAP 1.1 section 2.7.4, DALI 1.1 section 4.4.

How many rows a client gets, and how it finds out whether that was all of them. The
second half is the one that matters: a truncated answer that does not say it was
truncated is a wrong answer, and nothing in the rows can tell a client which it has.
"""

from __future__ import annotations

import warnings

import pyvo
import pytest

from tap_conformance.votable import overflow, statuses


def test_zero(tap, rows_query, record_property):
    """MAXREC=0 returns the columns and no rows.

    It is how a client inspects a table without reading it — TOPCAT does this on every
    table it shows — so it has to come back with the metadata intact.

    The parameter is set by hand rather than through `run_sync(maxrec=0)`: pyvo tests
    the value for truth before sending it, so a zero there is dropped and the query
    runs unlimited. Sending it is the only way to ask this question.
    """
    found = tap.create_query(rows_query(10), MAXREC=0).execute().to_table()
    record_property("detail", f"{len(found)} rows, {len(found.colnames)} columns")
    assert len(found) == 0, f"MAXREC=0 returned {len(found)} rows"
    assert found.colnames, "MAXREC=0 returned no columns either"


def test_overrides_top(tap, queryable, record_property):
    """MAXREC wins over TOP, TAP 2.7.4 saying it is the smaller that applies."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pyvo.dal.DALOverflowWarning)
        found = tap.run_sync(f"SELECT TOP 20 * FROM {queryable}", maxrec=3).to_table()
    record_property("detail", f"{len(found)} rows")
    assert len(found) == 3, f"TOP 20 with MAXREC=3 returned {len(found)} rows"


def test_truncation_is_marked(raw, queryable, record_property):
    """A truncated answer says so, with an OVERFLOW marker after the table.

    Read off the document rather than from the client: pyvo treats truncation at the
    MAXREC the caller asked for as expected and says nothing about it, so a service
    that omits the marker entirely looks the same from up there. The marker is what
    the next client — one that did not set MAXREC and got the service's own default —
    depends on.
    """
    response = raw(f"SELECT TOP 20 * FROM {queryable}", MAXREC=2)
    marker = overflow(response.content)
    record_property(
        "detail", f"{response.status_code}, statuses: {[str(s) for s in statuses(response.content)]}"
    )
    assert marker is not None, (
        "the answer was truncated and carries no OVERFLOW marker, so a client cannot "
        "tell it from a complete one"
    )


def test_the_marker_follows_the_table(raw, queryable, record_property):
    """DALI 4.4 puts OVERFLOW after the TABLE, where OK goes before it.

    That is what lets a service start writing rows before it knows whether there will
    be too many. A marker written into the prologue is a service that counted first.
    """
    response = raw(f"SELECT TOP 20 * FROM {queryable}", MAXREC=2)
    marker = overflow(response.content)
    if marker is None:
        pytest.skip("no overflow marker to place")
    record_property("detail", str(marker))
    assert marker.after_table, "the OVERFLOW marker is written before the table"


def test_a_complete_answer_is_not_marked(raw, tap, queryable, record_property):
    """An answer that fits is not marked as truncated.

    The opposite failure, and the reason a service has to read one row more than it
    was asked for: exactly MAXREC rows is otherwise two different answers with one
    spelling.
    """
    total = int(tap.run_sync(f"SELECT COUNT(*) AS n FROM {queryable}").to_table()["n"][0])
    response = raw(f"SELECT TOP 2 * FROM {queryable}", MAXREC=min(total, 1000))
    marker = overflow(response.content)
    record_property("detail", f"statuses: {[str(s) for s in statuses(response.content)]}")
    assert marker is None, "a complete answer was marked OVERFLOW"


def test_maxrec_above_what_matches(tap, queryable, record_property):
    """MAXREC larger than the answer changes nothing about it."""
    found = tap.run_sync(f"SELECT TOP 3 * FROM {queryable}", maxrec=1000).to_table()
    record_property("detail", f"{len(found)} rows")
    assert len(found) == 3


def test_negative_maxrec_is_refused(raw, rows_query, record_property):
    """A MAXREC that is not a row count is an error (DALI 3.4)."""
    response = raw(rows_query(3), MAXREC="-1")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code >= 400 or b"ERROR" in response.content[:4000], (
        f"MAXREC=-1 was answered with {response.status_code}"
    )


def test_unparsable_maxrec_is_refused(raw, rows_query, record_property):
    """Neither is a MAXREC that is not a number."""
    response = raw(rows_query(3), MAXREC="lots")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code >= 400 or b"ERROR" in response.content[:4000], (
        f"MAXREC=lots was answered with {response.status_code}"
    )
