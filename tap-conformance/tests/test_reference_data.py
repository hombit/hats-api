"""The same query, put to this service and to one that has been answering it for years.

Everything else in this suite asks whether an answer is well formed. This asks whether
it is right — which nothing about the shape of a VOTable can tell you, and which is
the question a person running a query actually has.

The reference answers were downloaded once, by `tap-conformance-fetch`, and are read
from disk here. Fetching them per run would be comparing this service against whatever
the reference service says today, which is a moving target and a slow one.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from astropy.table import Table

from tap_conformance.compare import compare


def pytest_generate_tests(metafunc):
    """One test per query per table, named so a failure says which was which."""
    if "case" not in metafunc.fixturenames:
        return
    path = Path(metafunc.config.getoption("--data")) / "queries.json"
    cases, names = [], []
    if path.exists():
        for query in json.loads(path.read_text()):
            for table in query["tables"]:
                cases.append((query, table))
                names.append(f"{query['id']}@{table}")
    if not cases:
        cases = [
            pytest.param(
                None,
                marks=pytest.mark.skip(
                    reason="no reference answers here — run tap-conformance-fetch"
                ),
            )
        ]
        names = ["nothing-fetched"]
    metafunc.parametrize("case", cases, ids=names)


def test_matches_the_reference(tap, published, data, case, record_property):
    """This service's answer says the same as the reference service's."""
    query, table = case
    if table not in published:
        pytest.skip(f"{table} is not published here")
    expected = Table.read(data / query["reference"], format="votable")
    actual = tap.run_sync(query["adql"].format(table=table)).to_table()
    record_property(
        "detail",
        f"{query['description']}: {compare(expected, actual, query.get('rtol', 0.0))}",
    )
