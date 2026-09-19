"""The suite's own options, registered as a pytest plugin by `addopts`.

Here rather than in `tests/conftest.py` because a conftest is loaded after the command
line is parsed, and `--server-binary ../target/debug/hats-api` is then an unrecognised
argument.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def pytest_addoption(parser):
    group = parser.getgroup("lsdb-conformance")
    group.addoption(
        "--base-url", default=None, help="a file server already running; nothing is started"
    )
    group.addoption(
        "--server-binary",
        default=str(ROOT.parent / "target" / "debug" / "hats-api"),
        help="the service to start when --base-url is not given",
    )
    group.addoption(
        "--report-dir",
        default=str(ROOT / "report"),
        help="where the service's configuration and output go",
    )
