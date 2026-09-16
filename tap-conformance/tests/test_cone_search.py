"""Simple Cone Search 1.03 — the 2008 Recommendation.

Three parameters and a VOTable: `RA`, `DEC` and `SR` in decimal degrees, ICRS, and the
rows inside that cone. There is no query language, no table parameter — a cone search
service *is* one table — and nothing it shares with TAP but the VOTable.

It predates DALI by a decade, which is the thing to keep in mind while reading these: an
error is an `INFO` named `Error` rather than a `QUERY_STATUS`, the required UCDs are
UCD1 rather than UCD1+, and there is no `MAXREC`. A check written by analogy with the TAP
ones would be testing the wrong standard.

Every question goes through `pyvo.dal.SCSService`, which is what `pyvo.conesearch` uses
and what an astronomer has.
"""

from __future__ import annotations

import pytest

#: The UCD1 words the response must mark its three mandatory columns with.
REQUIRED = {
    "id": "ID_MAIN",
    "ra": "POS_EQ_RA_MAIN",
    "dec": "POS_EQ_DEC_MAIN",
}


@pytest.fixture(scope="session")
def scs(pytestconfig):
    """The client, against the endpoint given on the command line."""
    import pyvo

    url = pytestconfig.getoption("--scs-url")
    if not url:
        pytest.skip("no --scs-url given, so there is no cone search to ask")
    service = pyvo.dal.SCSService(url)
    service._session.timeout = 120
    return service


@pytest.fixture(scope="session")
def answer(scs, center):
    """One cone, asked once and read by several checks."""
    ra, dec = center
    return scs.search(pos=(ra, dec), radius=0.1)


def ucds(result) -> dict[str, str]:
    """Each column's UCD, however the client spells the attribute."""
    found = {}
    for field in result.fielddescs:
        found[field.name] = (getattr(field, "ucd", "") or "").strip()
    return found


def test_answers(answer, record_property):
    """A cone comes back as a VOTable the client can read."""
    record_property("detail", f"{len(answer)} rows, {len(answer.fieldnames)} columns")
    assert len(answer) > 0, "the cone matched no rows where the fetched data says it should"


def test_required_columns(answer, record_property):
    """The three columns the standard makes mandatory, marked with their UCD1 words.

    `ID_MAIN`, `POS_EQ_RA_MAIN` and `POS_EQ_DEC_MAIN` — not the UCD1+ spellings the rest
    of this service publishes. A client of this protocol looks for these and has no other
    way to find the position in the table.
    """
    marked = ucds(answer)
    record_property("detail", "; ".join(f"{k}={v}" for k, v in marked.items() if v))
    for role, word in REQUIRED.items():
        assert any(word.upper() in value.upper() for value in marked.values()), (
            f"no column marked {word} for the {role}; the UCDs present are "
            f"{sorted(value for value in marked.values() if value)}"
        )


def test_rows_are_inside_the_cone(answer, center, record_property):
    """Every row returned is actually within the radius asked for."""
    import numpy as np

    marked = ucds(answer)
    names = {
        role: next(
            (name for name, value in marked.items() if word.upper() in value.upper()),
            None,
        )
        for role, word in REQUIRED.items()
    }
    if not names["ra"] or not names["dec"]:
        pytest.skip("no UCD-marked position to check against")

    table = answer.to_table()
    ra = np.asarray(table[names["ra"]], dtype=float)
    dec = np.asarray(table[names["dec"]], dtype=float)
    center_ra, center_dec = center
    # The same haversine the service uses, so this is a check of the rows rather than of
    # two different roundings.
    lat1, lat2 = np.radians(dec), np.radians(center_dec)
    delta = np.radians(ra - center_ra)
    separation = np.degrees(
        2
        * np.arcsin(
            np.sqrt(
                np.sin((lat1 - lat2) / 2) ** 2
                + np.cos(lat1) * np.cos(lat2) * np.sin(delta / 2) ** 2
            )
        )
    )
    worst = float(separation.max())
    record_property("detail", f"{len(table)} rows, farthest {worst:.4f}° of 0.1°")
    # A whisker over, for the rounding a service does on the way out.
    assert worst <= 0.1 + 1e-6, f"a row {worst:.4f}° away came back from a 0.1° cone"


def test_a_missing_parameter_is_refused(pytestconfig, record_property):
    """RA, DEC and SR are all mandatory, so a request without one is an error.

    The error is this protocol's own: a stubbed VOTable carrying an `INFO` (or `PARAM`)
    named `Error`. Not a `QUERY_STATUS`, which is DALI's and eight years younger.
    """
    import urllib.error
    import urllib.request

    url = pytestconfig.getoption("--scs-url")
    if not url:
        pytest.skip("no --scs-url given")
    asked = f"{url}{'&' if '?' in url else '?'}RA=45.0&DEC=0.0"
    try:
        with urllib.request.urlopen(asked, timeout=120) as answered:
            status, body = answered.status, answered.read(8000)
    except urllib.error.HTTPError as answered:
        status, body = answered.code, answered.read(8000)
    except OSError as unreachable:
        raise AssertionError(f"{asked} could not be reached: {unreachable}") from None

    text = body.decode("utf-8", errors="replace")
    record_property("detail", f"{status}: {' '.join(text.split())[:200]}")
    assert 'name="Error"' in text or "name='Error'" in text, (
        "a cone search missing SR did not come back with an INFO named Error"
    )
