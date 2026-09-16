"""The synchronous query resource and its parameters — TAP 1.1 sections 2.1 to 2.7.

What a client sends to /sync and what it is entitled to get back. The parameters are
DALI's as much as TAP's: case-insensitive names, one value each, and every one of them
either acted on or refused.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean
from tap_conformance.votable import refusal


def test_select_top(tap, rows_query, record_property):
    """A sync query returns the rows TOP asked for."""
    found = tap.run_sync(rows_query(5)).to_table()
    record_property("detail", f"{len(found)} rows, {len(found.colnames)} columns")
    assert len(found) == 5, f"TOP 5 returned {len(found)} rows"


def test_select_list_is_the_answer(tap, queryable, coordinates, record_property):
    """The columns come back in the number, order and name the SELECT wrote.

    TAP 3.2: the answer's columns are the select list's, and an alias is the name of
    the column it was written on.
    """
    ra, dec = coordinates
    found = tap.run_sync(
        f"SELECT TOP 3 {dec}, {ra}, {ra} + 0.0 AS shifted FROM {queryable}"
    ).to_table()
    expected = [dec.strip('"').lower(), ra.strip('"').lower(), "shifted"]
    record_property("detail", ", ".join(found.colnames))
    assert [name.lower() for name in found.colnames] == expected, (
        f"expected {expected}, got {found.colnames}"
    )


def test_empty_answer_still_has_columns(tap, queryable, coordinates, record_property):
    """A query matching nothing is an empty table, not an error and not no table."""
    ra, _ = coordinates
    found = tap.run_sync(
        f"SELECT TOP 5 {ra} FROM {queryable} WHERE {ra} < -999"
    ).to_table()
    record_property("detail", f"{len(found)} rows, columns {found.colnames}")
    assert len(found) == 0
    assert found.colnames, "an empty answer carried no column metadata"


def test_lang_adql(tap, rows_query, record_property):
    """LANG=ADQL is accepted, being the one language TAP makes mandatory."""
    found = tap.run_sync(rows_query(1), language="ADQL").to_table()
    record_property("detail", f"{len(found)} rows")
    assert len(found) == 1


def test_lang_unknown_is_refused(raw, rows_query, record_property):
    """A language the service does not implement is refused rather than guessed at.

    Answering it in ADQL anyway would give a client an answer to a question it did not
    ask, which is worse than the error it can act on.
    """
    record_property("detail", refusal(raw(rows_query(1), language="PQL")))


def test_request_doquery(raw, rows_query, record_property):
    """REQUEST=doQuery is accepted, a TAP 1.0 client still sending it."""
    response = raw(rows_query(1), REQUEST="doQuery")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code < 400, f"REQUEST=doQuery was refused with {response.status_code}"


def test_runid(raw, rows_query, record_property):
    """RUNID is accepted and changes nothing about the answer (DALI 3.4.6)."""
    response = raw(rows_query(1), RUNID="tap-conformance")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code < 400, f"RUNID was refused with {response.status_code}"


def test_parameter_names_are_case_insensitive(raw, rows_query, record_property):
    """DALI 3.1: a parameter name means the same however it is cased."""
    response = raw(rows_query(1), ReSpOnSeFoRmAt="votable")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code < 400, (
        f"a mixed-case parameter name was refused with {response.status_code}"
    )


@pytest.mark.taplint("QGE")
def test_get_mode(stage, record_property):
    """Queries sent as a GET are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("QPO")
def test_post_mode(stage, record_property):
    """Queries sent as a form-encoded POST are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("MDQ")
def test_results_match_declared_metadata(stage, record_property):
    """A result's columns are the ones the metadata said they would be."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
