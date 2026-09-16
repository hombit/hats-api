"""DALI examples — DALI 1.1 section 2.3.

A page of queries that run, marked up so a client can read them. TOPCAT puts them in
a menu, which is where most people's first query against a new service comes from.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean


def test_document(tap, record_property):
    """The examples endpoint answers, and has examples in it."""
    found = tap.examples
    record_property("detail", f"{len(found)} examples")
    assert found, "the examples document is empty"


#: How many examples are run. A service is free to publish a hundred, and each one is a
#: query against a real catalog — so without a cap this single check decides how long a
#: run takes, and a service with a long menu of slow examples stalls it for as long as
#: it likes. The first few answer the question either way: an examples document that is
#: broken is broken near the top of it.
MOST = 10


def test_examples_run(tap, record_property):
    """The published examples are queries that run.

    An example that fails is worse than no example: it is the first thing a new user
    tries, and what it teaches them is that the service is broken.

    pyvo hands back each example as a query ready to send, so what is run here is what
    a client would run from its menu rather than a reading of the markup.
    """
    published = tap.examples
    if not published:
        pytest.skip("no examples to run")
    found = published[:MOST]
    broken = []
    for number, example in enumerate(found, start=1):
        query = example.get("QUERY")
        if not query:
            broken.append(f"example {number}: carries no query")
            continue
        try:
            tap.run_sync(query, maxrec=5)
        except Exception as error:  # noqa: BLE001 — any failure is the finding
            broken.append(f"example {number}: {str(error)[:150]}")
    record_property("detail", f"{len(found) - len(broken)} of {len(found)} run")
    assert not broken, "; ".join(broken[:4])


@pytest.mark.taplint("EXA")
def test_content(stage, record_property):
    """The examples document is marked up the way DALI asks."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
