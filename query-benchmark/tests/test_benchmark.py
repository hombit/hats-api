"""That the benchmark runs at all, which is the only thing CI is asked to say.

The numbers a CI runner produces are its network's rather than the code's, so nothing
here looks at a time. What it checks is that the whole path works: a configuration is
written, a service starts over it, a real catalog answers a cone, and rows come back.

One catalog, one run. `ps1` because its partitions are files and its columns are flat,
which makes it the cheapest of the four to ask and the one with least to go wrong that
is not this service's doing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from query_benchmark.run import DEFAULT_BINARY, chosen
from query_benchmark.service import configuration

#: Which build to measure. CI points this at the debug one, having built that.
BINARY = Path(os.environ.get("QUERY_BENCHMARK_BINARY", DEFAULT_BINARY))


def test_a_cone_over_ps1_answers_with_rows(tmp_path):
    """The whole path, end to end, against the real bucket."""
    assert BINARY.exists(), f"no service at {BINARY}; cargo build, or set QUERY_BENCHMARK_BINARY"
    written = tmp_path / "runs.json"
    finished = subprocess.run(
        [
            sys.executable,
            "-m",
            "query_benchmark",
            "--catalog",
            "ps1",
            "--runs",
            "1",
            "--binary",
            str(BINARY),
            "--json",
            str(written),
            "--report-dir",
            str(tmp_path / "report"),
        ],
        capture_output=True,
        text=True,
    )
    assert finished.returncode == 0, f"{finished.stdout}\n{finished.stderr}"

    measured = json.loads(written.read_text())["catalogs"]
    assert [one["catalog"] for one in measured] == ["ps1"]
    assert measured[0]["error"] is None
    assert len(measured[0]["seconds"]) == 1
    # A cone this service answered with nothing would pass every other assertion here.
    assert measured[0]["num_rows"] > 0
    assert measured[0]["data_bytes_read"] > 0


def test_a_location_replaces_the_catalogs_own_source():
    """`--catalog NAME=LOCATION` is what puts a local copy in the configuration."""
    assert chosen(["ps1=/data/ps1"]) == {"ps1": "/data/ps1"}
    assert chosen(["ps1"])["ps1"].startswith("s3://")
    written = configuration(8080, chosen(["ps1=/data/ps1", "tess"]))
    assert 'path = "/ps1"' in written
    assert 'source = "/data/ps1"' in written
    # The region is an S3 option, so it goes on the S3 mount and on no other.
    assert written.count("storage = ") == 0
    assert 'source = "https://data.lsdb.io/hats/tess/tess_lightcurve"' in written
