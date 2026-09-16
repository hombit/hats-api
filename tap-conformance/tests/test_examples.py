"""DALI examples — DALI 1.1 section 2.3.

A page of queries that run, marked up so a client can read them. TOPCAT puts them in
a menu, which is where most people's first query against a new service comes from.
"""

from __future__ import annotations

import time

import pytest

from tap_conformance.taplint import assert_clean


def test_document(tap, record_property):
    """The examples endpoint answers, and has examples in it."""
    found = tap.examples
    record_property("detail", f"{len(found)} examples")
    assert found, "the examples document is empty"


#: What this check may spend, in examples and in seconds.
#:
#: Both bounds are needed and the second is the one that matters. A service is free to
#: publish a hundred examples and each is a real query, so a count alone still lets one
#: service with a menu of slow examples decide how long a whole run takes — which is not
#: hypothetical: it stalled this suite twice, for forty minutes each time, before the
#: clock was put on it. Whatever is answered inside the budget answers the question; an
#: examples document that is broken is broken near the top of it.
MOST = 10
BUDGET = 90


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

    deadline = time.monotonic() + BUDGET
    broken, ran = [], 0
    for number, example in enumerate(published[:MOST], start=1):
        if time.monotonic() > deadline:
            break
        query = example.get("QUERY")
        if not query:
            broken.append(f"example {number}: carries no query")
            continue
        ran += 1
        try:
            tap.run_sync(query, maxrec=5)
        except Exception as error:  # noqa: BLE001 — any failure is the finding
            broken.append(f"example {number}: {str(error)[:150]}")
    record_property(
        "detail", f"{ran - len(broken)} of {ran} run, out of {len(published)} published"
    )
    assert not broken, "; ".join(broken[:4])


@pytest.mark.taplint("EXA")
def test_content(stage, record_property):
    """The examples document is marked up the way DALI asks."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
