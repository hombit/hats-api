"""The configuration this benchmark writes, and the service it starts over it."""

from __future__ import annotations

import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from query_benchmark.catalogs import REGION

#: One store is opened per mount before the service listens, and no catalog is read.
STARTUP_SECONDS = 60


def toml_string(value: str) -> str:
    """A TOML basic string. JSON's escaping is a subset of it, so this is exact."""
    return json.dumps(value)


def configuration(port: int, mounts: dict[str, str]) -> str:
    """The API on, the file server off, and one mount per catalog being measured.

    Only the chosen catalogs are mounted, because mounting one opens a store: mounting
    four to ask one of them would put three round trips into the startup of every run.

    The bounds on bytes, rows and time are raised well past what this cone needs. They are
    there to stop a request running away, and a benchmark that tripped one would report the
    refusal's timing as though it were the query's.

    The partition bound is the service's own default and not raised: it is also what decides
    when a collection's index is asked, and an ID search timed with a bound raised past the
    catalog's size would be timing a scan of every partition instead.
    """
    lines = [
        "# Written by query-benchmark. Every run overwrites it.",
        "",
        "[server]",
        'address = "127.0.0.1"',
        f"port = {port}",
        "",
        "[api]",
        "enabled = true",
        "",
        "[limits]",
        "max_request_seconds = 600",
        'max_bytes_fetched = "50GiB"',
        "max_rows = 10000000",
        "max_partitions = 128",
        "",
    ]
    for slug, source in mounts.items():
        lines += [
            "[[mount]]",
            f"path = {toml_string('/' + slug)}",
            f"source = {toml_string(source)}",
            # API-only: the benchmark asks `POST /simple/hats`, and publishing the
            # directory as well would serve what nothing here reads.
            "serve = false",
        ]
        # The region is an S3 option and every other backend refuses it, so it goes on
        # the mounts that take it and nowhere else.
        if source.startswith("s3://"):
            lines.append(f"storage = {{ region = {toml_string(REGION)} }}")
        lines.append("")
    return "\n".join(lines)


def answers(url: str) -> bool:
    """Whether anything replies at all. A 404 is a reply: the service is up."""
    try:
        urllib.request.urlopen(url, timeout=2).read(1)
    except urllib.error.HTTPError:
        return True
    except OSError:
        return False
    return True


def start(
    binary: Path, report_dir: Path, mounts: dict[str, str]
) -> tuple[str, subprocess.Popen]:
    """Where the started service answers, and the process answering."""
    report_dir.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])

    config = report_dir / "hats-api.toml"
    config.write_text(configuration(port, mounts))
    log = report_dir / "hats-api.log"
    process = subprocess.Popen(
        [str(binary), "--config", str(config)],
        stdout=log.open("wb"),
        stderr=subprocess.STDOUT,
    )

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + STARTUP_SECONDS
    while time.monotonic() < deadline and process.poll() is None:
        if answers(f"{base_url}/"):
            return base_url, process
        time.sleep(0.2)

    process.kill()
    # The first lines and not the last: a service that refuses its configuration says why
    # and then prints its usage, so the tail is the usage.
    said = [line for line in log.read_text(errors="replace").splitlines() if line.strip()]
    raise SystemExit(
        f"the service would not start: {' / '.join(said[:5]) or 'it said nothing'}\n"
        f"its configuration and output are in {report_dir}"
    )
