"""VOSI availability — VOSI 1.1 section 3.

One resource, one question: is the service up. A client asks it before anything else,
and a registry asks it on a schedule.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean


def test_available(tap, record_property):
    """/availability answers, and says the service is up."""
    available = tap.available
    record_property("detail", f"available={available}")
    assert available, "the service reports itself unavailable"


@pytest.mark.taplint("AVV")
def test_schema(stage, record_property):
    """The availability document validates against its XML schema."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
