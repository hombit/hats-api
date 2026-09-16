"""Table upload — TAP 1.1 section 2.5.

A client sends a table with its query and joins against it as TAP_UPLOAD. It is an
optional feature: a service declares in its capabilities whether it has one, and one
that declares none is conforming without it.

So there are two questions here, and only the second is a conformance question: does
upload work, and does the service's answer about upload match what it declared.
"""

from __future__ import annotations

import numpy as np
import pytest
from astropy.table import Table

from tap_conformance.taplint import assert_clean

ABSENT = "this service declares no upload method"


def declared(tap) -> list[str]:
    return [str(method) for method in (tap.upload_methods or [])]


@pytest.mark.xfail(reason=ABSENT, strict=False)
def test_inline_upload(tap, record_property):
    """A table uploaded with the query is queryable as TAP_UPLOAD."""
    uploaded = Table({"id": np.arange(3), "x": np.arange(3) * 1.5})
    found = tap.run_sync(
        "SELECT * FROM TAP_UPLOAD.t1", uploads={"t1": uploaded}
    ).to_table()
    record_property("detail", f"{len(found)} rows came back from an upload")
    assert len(found) == 3


def test_capabilities_agree_with_behaviour(tap, record_property):
    """What the service says about upload is what it does.

    Declaring a method it has not got sends a client down a path that fails at the
    point of sending data; having one it does not declare means no client will try.
    """
    methods = declared(tap)
    record_property("detail", f"declared: {', '.join(methods) or 'none'}")
    uploaded = Table({"id": np.arange(2)})
    try:
        tap.run_sync("SELECT * FROM TAP_UPLOAD.t1", uploads={"t1": uploaded})
        works = True
    except Exception:  # noqa: BLE001 — whether it worked is the whole question
        works = False
    assert works == bool(methods), (
        f"upload {'works' if works else 'does not work'} "
        f"while the capabilities declare {methods or 'no method'}"
    )


@pytest.mark.taplint("UPL")
@pytest.mark.xfail(reason=ABSENT, strict=False)
def test_queries_with_uploads(stage, record_property):
    """Queries carrying an uploaded table are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
