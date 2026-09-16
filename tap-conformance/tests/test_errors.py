"""Error documents — TAP 1.1 section 3.3, DALI 1.1 sections 4.2 and 4.4.

A failed query is answered with a document saying so: an INFO whose QUERY_STATUS is
ERROR, in the format the request asked for. The HTTP status is deliberately not part of
these checks — TAP §3.3 lets a synchronous service answer with "an appropriate HTTP
status code, including 200", so a 200 carrying a proper error document is conforming and
a check demanding a 4xx would fail a service for doing it right.

`taplint` sends deliberately broken queries too, and checks that they fail. These check
what the failure *is*, which is the half that lets a client tell a user anything: run
against the ESA archive, a query with an unclosed string literal comes back as an HTML
page, which is neither the requested format nor either of the two DALI §4.2 allows, and
the validator passed it.
"""

from __future__ import annotations

import pyvo
import pytest


def is_error_document(response) -> str:
    """What the error looks like, against what TAP 1.1 §3.3 asks it to look like.

    The status is not part of it. §3.3 lets a synchronous service answer with "an
    appropriate HTTP status code, including 200" — so a 200 carrying an error document
    is conforming, and an earlier version of these checks that demanded a 4xx would have
    failed a service doing it properly.

    What §3.3 does ask is that an error document "should be in a format that matches the
    requested format where possible". These queries ask for nothing, so the format is
    VOTable by default, and a VOTable error is what matches.
    """
    body = response.content[:8000]
    assert b"<VOTABLE" in body.upper(), (
        f"{response.status_code}, and the error is not a VOTable where a VOTable was "
        f"the requested format: {body[:200]!r}"
    )
    assert b'value="ERROR"' in body or b"value='ERROR'" in body, (
        'no INFO with QUERY_STATUS="ERROR"'
    )
    return f"{response.status_code}, VOTable with QUERY_STATUS=ERROR"


@pytest.mark.parametrize(
    ("what", "query"),
    [
        ("syntax", "SELECT FROM WHERE"),
        ("unknown-table", "SELECT * FROM no_such_schema.no_such_table"),
        ("unclosed-string", "SELECT * FROM TAP_SCHEMA.tables WHERE table_name = 'x"),
    ],
    ids=["syntax", "unknown_table", "unclosed_string"],
)
def test_bad_query(raw, what, query, record_property):
    """A query that cannot run comes back as an error document."""
    record_property("detail", is_error_document(raw(query)))


def test_unknown_column(raw, queryable, record_property):
    """A column the table has not got is an error rather than an empty answer.

    Dropping it and answering anyway returns rows the client did not ask for, which
    it has no way to tell from the ones it did.
    """
    record_property(
        "detail", is_error_document(raw(f"SELECT no_such_column FROM {queryable}"))
    )


def test_the_client_raises(tap, record_property):
    """pyvo turns the error document into an exception rather than an empty table.

    `DALQueryError` specifically, which is what pyvo raises when it has read a
    QUERY_STATUS of ERROR. A `DALServiceError` is the transport failing — a 404 from a
    service that has no `/sync` at all raises one, and accepting it here would make
    this check pass against a service implementing nothing, which is how the last
    version of it was found to be wrong.
    """
    try:
        tap.run_sync("SELECT FROM WHERE")
    except pyvo.dal.DALQueryError as error:
        record_property("detail", f"DALQueryError: {str(error)[:200]}")
        return
    except pyvo.dal.DALServiceError as error:
        raise AssertionError(
            f"the client could not reach the query resource at all: {str(error)[:200]}"
        ) from None
    raise AssertionError("a malformed query raised nothing in the client")


def test_the_message_says_something(raw, queryable, record_property):
    """The error names what was wrong with the query it was given.

    An error document with an empty INFO is a conforming document and a useless one.
    """
    response = raw(f"SELECT no_such_column FROM {queryable}")
    body = response.content[:8000].decode("utf-8", errors="replace")
    record_property("detail", " ".join(body.split())[:300])
    assert "no_such_column" in body, (
        "the error does not name the column the query was refused over"
    )
