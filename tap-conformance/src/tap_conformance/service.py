"""The service under test: started, waited for, and taken down again.

Nothing here reads this repository's source. What it knows about the service is what
an operator knows — the command line, the shape of the configuration file, and that
it answers HTTP once it is up.
"""

from __future__ import annotations

import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

# Where the suite mounts the sample catalog. An address rather than a path: a `file://`
# url in a request names this, never the directory behind it.
MOUNT_PATH = "/hats"

# The url subtree the API answers under, and TAP below it. Both are the service's own
# defaults; the configuration written here says them anyway, so that a run reads the
# same whether or not a default moves.
API_PREFIX = "/api/v1"
TAP_PATH = "tap"

# How long the service is given to answer its first request.
STARTUP_SECONDS = 30


def free_port() -> int:
    """A port nothing is listening on, as far as anything can know."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


#: The region the published buckets are in. A mount naming a store takes the options a
#: request would carry beside its url, and a bucket needs its region.
REGION = "us-east-1"


@dataclass(frozen=True)
class Published:
    """One table the suite publishes, and where the service reaches it.

    A `[[tap.table]]` names a path in the service's own url space, so a catalog in a
    bucket is published by mounting it and naming the mount — `source` is that bucket, or
    `None` for one already under the sample mount.
    """

    name: str
    path: str
    source: str | None = None


def configuration(port: int, catalogs: Path | None, tables: list[Published]) -> str:
    """The configuration file the service is started with.

    The limits are raised well above their defaults on purpose. A validator asks for
    whole rows of whatever it finds, and one of the published catalogs is the real
    Gaia DR3 — a single partition of which is a few hundred megabytes. Leaving the
    defaults would make the report a page of timeouts, which says nothing about
    whether the protocol is implemented.
    """
    lines = [
        "[server]",
        'address = "127.0.0.1"',
        f"port = {port}",
        "",
        "[api]",
        "enabled = true",
        f'prefix = "{API_PREFIX}"',
        "",
        "[limits]",
        "max_request_seconds = 300",
        'max_bytes_fetched = "20GiB"',
        "max_rows = 5000000",
        "",
    ]
    if catalogs is not None:
        lines += [
            "[[mount]]",
            f'path = "{MOUNT_PATH}"',
            f'source = "{catalogs}"',
            "serve = false",
            "",
        ]
    # A catalog in a bucket is reached the same way a local one is: by a mount. What it
    # takes to read it is written there, once, and the table below names the address.
    for table in tables:
        if table.source is not None:
            lines += [
                "[[mount]]",
                f'path = "{table.path}"',
                f'source = "{table.source}"',
                "serve = false",
                f'storage = {{region = "{REGION}"}}',
                "",
            ]
    for table in tables:
        lines += ["[[tap.table]]", f'name = "{table.name}"', f'path = "{table.path}"', ""]
    return "\n".join(lines)


@dataclass(frozen=True)
class Note:
    """What the service was started on, where that is not what was asked for.

    `as_asked` is the difference between a finding and a broken run, and it is carried
    here rather than read out of the wording downstream. A service started on less than
    the suite configured still answers every question — about a service nobody asked
    for. That report reads exactly like a service missing the features, which is why the
    suite has to be told the two apart rather than guessing from a count.
    """

    id: str
    detail: str
    as_asked: bool


@dataclass
class Service:
    """A service the suite can put questions to, however it got there."""

    base_url: str
    #: What had to be given up to get it started. The report carries each as a check of
    #: its own, and one that is not [`Note.as_asked`] also makes the run unbelievable.
    notes: list[Note] = field(default_factory=list)
    process: subprocess.Popen | None = None
    log: Path | None = None

    def stop(self) -> None:
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()


def answers(url: str, timeout: float = 2.0) -> bool:
    """Whether anything at all replies. A 404 is a reply: the service is up."""
    try:
        urllib.request.urlopen(url, timeout=timeout).read(1)
        return True
    except urllib.error.HTTPError:
        return True
    except OSError:
        return False


def start(binary: Path, catalogs: Path | None, tables: list[tuple[str, str]], report_dir: Path) -> Service:
    """Start the service, and settle for less if it will not take the whole config.

    The tables are the part that may not be understood: publishing a table over TAP is
    the youngest thing in the configuration, and a service that refuses the key exits
    at startup rather than ignoring it. Rather than reporting that as every check
    failing for the same unexplained reason, it is retried without them and recorded
    once, as itself — and the note says the run is not one to believe.
    """
    report_dir.mkdir(parents=True, exist_ok=True)
    attempts = [("with its tables", tables)] if tables else []
    attempts.append(("with no tables", []))

    notes: list[Note] = []
    for number, (description, published) in enumerate(attempts, start=1):
        port = free_port()
        config = report_dir / f"hats-api.{number}.toml"
        config.write_text(configuration(port, catalogs, published))
        # One log per attempt: a second attempt writing over the first would take the
        # refusal that explains it with it.
        log = report_dir / f"hats-api.{number}.log"
        handle = log.open("wb")
        process = subprocess.Popen(
            [str(binary), "--config", str(config)],
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        root = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + STARTUP_SECONDS
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            if answers(f"{root}{API_PREFIX}/"):
                if published:
                    notes.append(
                        Note(
                            "service/published-tables",
                            f"{len(published)} tables published",
                            as_asked=True,
                        )
                    )
                return Service(
                    base_url=f"{root}{API_PREFIX}/{TAP_PATH}",
                    notes=notes,
                    process=process,
                    log=log,
                )
            time.sleep(0.2)

        process.kill()
        handle.close()
        # The first lines, not the last: a service that refuses its configuration says
        # why and then prints its usage, so the tail is the usage.
        said = [line for line in log.read_text(errors="replace").splitlines() if line.strip()]
        notes.append(
            Note(
                "service/published-tables",
                f"the service would not start {description}: "
                f"{' / '.join(said[:5]) or 'it said nothing'}",
                as_asked=False,
            )
        )

    raise RuntimeError(
        f"the service would not start at all; its output is in {report_dir}"
    )
