"""Starting a file server over the public catalogs, and stopping it again."""

from __future__ import annotations

import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from lsdb_conformance.catalogs import CATALOGS, REGION

#: One store is opened per mount before the service listens, and no catalog is read.
STARTUP_SECONDS = 60


def configuration(port: int) -> str:
    """Every catalog mounted and served, with the API off.

    Mounting is cheap — a store opened at startup, no catalog read — so all of them are
    mounted and a check chooses which it pays for. The limits are raised because one
    partition of these catalogs is hundreds of megabytes, and a default that refused it
    would say nothing about whether LSDB can read it.
    """
    lines = [
        "[server]",
        'address = "127.0.0.1"',
        f"port = {port}",
        "",
        "[api]",
        "enabled = false",
        "",
        "[limits]",
        "max_request_seconds = 600",
        'max_bytes_fetched = "50GiB"',
        "max_rows = 10000000",
        "max_partitions = 4096",
        "",
    ]
    for slug, source in CATALOGS.items():
        lines += [
            "[[mount]]",
            f'path = "/{slug}"',
            f'source = "{source}"',
            "serve = true",
            f'storage = {{region = "{REGION}"}}',
            "",
        ]
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


def start(binary: Path, report_dir: Path) -> tuple[str, subprocess.Popen]:
    """Where the started service answers, and the process answering."""
    report_dir.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])

    config = report_dir / "hats-api.toml"
    config.write_text(configuration(port))
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
    raise RuntimeError(
        f"the service would not start: {' / '.join(said[:5]) or 'it said nothing'}\n"
        f"its configuration and output are in {report_dir}"
    )
