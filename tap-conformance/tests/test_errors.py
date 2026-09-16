"""Error documents — DALI 1.1 section 4.4, TAP 1.1 section 2.9.

A failed query is answered with a VOTable carrying an INFO whose QUERY_STATUS is
ERROR, and an HTTP status that says it failed. Both halves matter: a client reads the
status to know it failed and the document to know why, and a service that answers 200
with an error inside makes every client's success path the one that runs.

`taplint` sends deliberately broken queries too, and checks that they fail. These check
what the failure *is*, which is the half that lets a client tell a user anything: run
against the ESA archive, a query with an unclosed string literal comes back as a 500
and an HTML page, and the validator passed it.
"""

from __future__ import annotations

import pyvo
import pytest


def is_error_document(response) -> str:
    """What DALI 4.4 asks an error to look like."""
    body = response.content[:8000]
    assert b"<VOTABLE" in body.upper(), f"the error is not a VOTable: {body[:200]!r}"
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
    """A query that cannot run comes back as an error document with a 4xx."""
    response = raw(query)
    assert response.status_code >= 400, (
        f"a {what} error was answered with {response.status_code}"
    )
    record_property("detail", is_error_document(response))


def test_unknown_column(raw, queryable, record_property):
    """A column the table has not got is an error rather than an empty answer.

    Dropping it and answering anyway returns rows the client did not ask for, which
    it has no way to tell from the ones it did.
    """
    response = raw(f"SELECT no_such_column FROM {queryable}")
    assert response.status_code >= 400, f"answered with {response.status_code}"
    record_property("detail", is_error_document(response))


def test_the_client_raises(tap, record_property):
    """pyvo turns the error document into an exception rather than an empty table."""
    try:
        tap.run_sync("SELECT FROM WHERE")
    except (pyvo.dal.DALQueryError, pyvo.dal.DALServiceError) as error:
        record_property("detail", f"{type(error).__name__}: {str(error)[:200]}")
        return
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
