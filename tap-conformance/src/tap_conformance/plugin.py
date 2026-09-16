"""The suite's own command-line options.

Here rather than in `tests/conftest.py`, and named by `addopts` in `pyproject.toml`, so
that they are registered before pytest parses anything.

A conftest is not early enough. pytest decides which conftests to load from the paths on
the command line, and at that point it does not yet know which options take a value — so
`--stilts-jar /home/you/stilts.jar` reads as a path to collect from, the directory it is
in has no conftest, `testpaths` is skipped because an argument that looks like a path was
given, and the options in `tests/conftest.py` are never registered. What comes out is
`unrecognized arguments: --stilts-jar` and exit code 4, for a command line that is
correct. Every option here points outside the project sooner or later — a binary in
`../target`, a jar in a cache, a report directory somewhere else — so this is the normal
case rather than a corner of it.
"""

from __future__ import annotations

from pathlib import Path

HERE = Path(__file__).resolve().parent.parent.parent
REPOSITORY = HERE.parent


def pytest_addoption(parser):
    group = parser.getgroup("tap-conformance")
    group.addoption(
        "--base-url",
        default=None,
        help="a TAP service that is already running; nothing is started",
    )
    group.addoption(
        "--server-binary",
        default=str(REPOSITORY / "target" / "debug" / "hats-api"),
        help="the service to start when --base-url is not given",
    )
    group.addoption(
        "--data",
        default=str(HERE / "data"),
        help="what tap-conformance-fetch downloaded",
    )
    group.addoption(
        "--report-dir",
        default=str(HERE / "report"),
        help="where the report and the tools' own output go",
    )
    # Cone search is not under the TAP base url and its url space is not decided here.
    # Given by hand rather than guessed, so that these checks never encode a shape this
    # service has not committed to; absent, they skip and say so. 1.03 only, that being
    # the version a client implements — the README says why 2.0 is not measured here.
    group.addoption(
        "--scs-url",
        default=None,
        help="a Simple Cone Search 1.03 endpoint, which is one table's",
    )
    group.addoption("--stilts", default=None, help="the stilts command")
    group.addoption("--stilts-jar", default=None, help="stilts.jar, run through java")
    group.addoption(
        "--skip-taplint", action="store_true", help="do not run the STILTS validator"
    )
    group.addoption(
        "--strict-conformance",
        action="store_true",
        help="exit non-zero when a check fails, rather than reporting it",
    )
