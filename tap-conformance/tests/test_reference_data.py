"""The same query, put to this service and to one that has been answering it for years.

Everything else in this suite asks whether an answer is well formed. This asks whether
it is right — which nothing about the shape of a VOTable can tell you, and which is the
question a person running a query actually has.

Every query is asked twice, once through `pyvo` and once through `stilts tapquery`, and
both answers are read against the reference. `tapquery` is STILTS as a user runs it —
the task TOPCAT uses underneath — rather than `taplint`, which is a linter and composes
its own queries. Two clients matter because a service can hand one of them the right
rows and the other something its parser refuses, and the half of the world using the
second client is the half that reports the service as broken.

The reference answers were downloaded once, by `tap-conformance-fetch`, and are read
from disk here. Fetching them per run would be comparing this service against whatever
the reference service says today, which is a moving target and a slow one.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from astropy.table import Table

from tap_conformance import tapquery
from tap_conformance.compare import compare


def pytest_generate_tests(metafunc):
    """One test per query, per table, per client, named so a failure says which."""
    if "case" not in metafunc.fixturenames:
        return
    path = Path(metafunc.config.getoption("--data")) / "queries.json"
    cases, names = [], []
    if path.exists():
        for query in json.loads(path.read_text()):
            for table in query["tables"]:
                for client in ("pyvo", "stilts"):
                    cases.append((query, table, client))
                    names.append(f"{query['id']}@{table}:{client}")
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


def test_matches_the_reference(
    tap, published, data, case, stilts_command, record_property
):
    """This service's answer says what the reference service's answer says."""
    query, table, client = case
    if table not in published:
        pytest.skip(f"{table} is not published here")

    expected = Table.read(data / query["reference"], format="votable")
    adql = query["adql"].format(table=table)
    if client == "pyvo":
        actual = tap.run_sync(adql).to_table()
    else:
        if stilts_command is None:
            pytest.skip("STILTS is not installed")
        try:
            actual = tapquery.query(stilts_command, tap.baseurl, adql)
        except tapquery.Unavailable as missing:
            pytest.skip(str(missing))
    record_property(
        "detail",
        f"{client}: {query['description']} — "
        f"{compare(expected, actual, query.get('rtol', 0.0))}",
    )
