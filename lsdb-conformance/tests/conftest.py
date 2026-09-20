"""A running service, and one question put to one catalog down both routes."""

from __future__ import annotations

from pathlib import Path

import lsdb
import pytest
from upath import UPath

from lsdb_conformance import service
from lsdb_conformance.catalogs import CATALOGS

@pytest.fixture(scope="session")
def base_url(request) -> str:
    """The file server to read through, started here unless one was given."""
    given = request.config.getoption("--base-url")
    if given:
        return given.rstrip("/")

    binary = Path(request.config.getoption("--server-binary"))
    if not binary.exists():
        pytest.fail(f"no service at {binary}; `cargo build`, or point --base-url at one")
    url, process = service.start(binary, Path(request.config.getoption("--report-dir")))
    request.addfinalizer(process.terminate)
    return url


@pytest.fixture
def both(base_url):
    """Ask one question twice, and hand back what each route said.

    The question is handed an `open_catalog` bound to one route rather than a catalog
    already opened, so a question naming two catalogs — a crossmatch — reads both through
    the same route. Keyword arguments reach `lsdb.open_catalog` untouched.

    Nothing here knows what the answer should be: these catalogs gain rows and get
    rebuilt, so what stays true is that two readers of one bucket have to agree.
    """

    def route(where):
        def open_catalog(slug: str, **kwargs):
            return lsdb.open_catalog(where(slug), **kwargs)

        return open_catalog

    # `anon=True` rides on the path rather than beside it: `storage_options` as a keyword
    # reaches `pyarrow.parquet.read_table`, which has no such argument.
    direct = route(lambda slug: UPath(CATALOGS[slug], anon=True))
    via_api = route(lambda slug: f"{base_url}/{slug}")
    return lambda question: (question(direct), question(via_api))
