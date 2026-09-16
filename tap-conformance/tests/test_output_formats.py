"""Output formats — TAP 1.1 section 2.7.3, DALI 1.1 section 3.4.

VOTable is the mandatory one and the default; CSV and TSV are a SHOULD. What a client
needs from all of them is that asking for one gets that one, that its media type says
which it is, and that asking for something the service has not got is an error rather
than a surprise.
"""

from __future__ import annotations

import pytest

from tap_conformance.votable import refusal


@pytest.fixture
def answer(raw, rows_query):
    def send(value: str, parameter: str = "RESPONSEFORMAT"):
        return raw(rows_query(3), **{parameter: value})

    return send


def test_default_is_votable(raw, rows_query, record_property):
    """A query naming no format is answered in VOTable."""
    response = raw(rows_query(3))
    media = response.headers.get("content-type", "none")
    record_property("detail", f"{response.status_code}, {media}")
    assert response.status_code == 200
    assert b"<VOTABLE" in response.content[:4000].upper(), (
        f"the default answer is not a VOTable: {response.content[:200]!r}"
    )


def test_votable(answer, record_property):
    """RESPONSEFORMAT=votable, the mandatory format."""
    response = answer("votable")
    media = response.headers.get("content-type", "none")
    record_property("detail", f"{response.status_code}, {media}")
    assert response.status_code == 200, f"status {response.status_code}"
    assert b"<VOTABLE" in response.content[:4000].upper()
    assert "votable" in media.lower(), f"content-type {media!r} does not say votable"


def test_csv(answer, record_property):
    """text/csv, a SHOULD of TAP 2.7.3."""
    response = answer("csv")
    media = response.headers.get("content-type", "none")
    record_property("detail", f"{response.status_code}, {media}")
    assert response.status_code == 200, f"status {response.status_code}"
    assert "csv" in media.lower(), f"content-type {media!r}"


def test_tsv(answer, record_property):
    """text/tab-separated-values, the other SHOULD of TAP 2.7.3."""
    response = answer("tsv")
    media = response.headers.get("content-type", "none")
    record_property("detail", f"{response.status_code}, {media}")
    assert response.status_code == 200, f"status {response.status_code}"
    assert "tab-separated" in media.lower() or "tsv" in media.lower(), (
        f"content-type {media!r}"
    )


def test_format_parameter_is_accepted(answer, record_property):
    """FORMAT is accepted as the equivalent of RESPONSEFORMAT (TAP 2.7.3)."""
    response = answer("votable", parameter="FORMAT")
    record_property("detail", f"status {response.status_code}")
    assert response.status_code == 200, f"status {response.status_code}"
    assert b"<VOTABLE" in response.content[:4000].upper(), (
        "FORMAT=votable did not produce a VOTable"
    )


def test_declared_formats_are_answered(tap, answer, record_property):
    """Every format the capabilities document declares can actually be asked for."""
    declared = [
        str(output.mime)
        for capability in tap.capabilities
        for output in getattr(capability, "outputformats", [])
    ]
    if not declared:
        pytest.skip("the capabilities document declares no output format")
    refused = []
    for media in declared:
        response = answer(media)
        if response.status_code >= 400:
            refused.append(f"{media} → {response.status_code}")
    record_property("detail", f"{len(declared)} declared: {', '.join(declared)}")
    assert not refused, f"declared but refused: {'; '.join(refused)}"


def test_unknown_format_is_refused(answer, record_property):
    """A format the service has not got is refused, not silently replaced.

    Answering in VOTable instead gives a client bytes it will try to parse as what it
    asked for, which fails somewhere further away from the cause.
    """
    record_property("detail", refusal(answer("application/x-nonsense")))
