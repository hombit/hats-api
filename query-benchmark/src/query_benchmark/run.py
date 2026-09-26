"""One cone search and one ID search per catalog, timed, against a service this starts
itself.

What is measured is the whole request as a client sees it: the body goes out, the rows
come back, and the clock stops when the last byte is read. Two builds are compared by
running this against each and reading the tables — or the two `--json` files, which
carry every individual time rather than the summary.

**The cone goes through `/simple/hats` and the ID search through `/adql`**, since a
collection's index is asked only by a statement. Both read the same columns of the same
object; the ID search skips a catalog whose collection has no index.

**The two counts beside the times are what say the comparison is honest.** A build that
returns fewer rows, or reads fewer bytes, is not a faster build; it is a different
answer. They come off the service's own reply, so nothing here has to measure them.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from query_benchmark import service
from query_benchmark.catalogs import CATALOGS, DEC, RA, RADIUS_ARCSEC

#: This package's own directory; the repository is its parent.
ROOT = Path(__file__).resolve().parents[2]

#: What a build of this service is called when nobody says otherwise. Release, because a
#: debug build measures the compiler rather than the code.
DEFAULT_BINARY = ROOT.parent / "target" / "release" / "hats-api"

#: Longer than the service's own `max_request_seconds`, so a request that runs away is
#: ended by the service — which says why — rather than here, which cannot.
REQUEST_SECONDS = 660


@dataclass
class Measured:
    """What one catalog's runs came to."""

    catalog: str
    source: str
    seconds: list[float] = field(default_factory=list)
    num_rows: int | None = None
    data_bytes_read: int | None = None
    error: str | None = None


def chosen(values: list[str] | None) -> dict[str, str]:
    """`NAME`, or `NAME=LOCATION` for a copy somewhere else; all of them when none.

    A location is a local path or a url in any scheme the service reads — it becomes a
    mount's `source`, which takes either, so there is one thing to say rather than two.
    """
    if not values:
        return {name: catalog.source for name, catalog in CATALOGS.items()}
    picked: dict[str, str] = {}
    for value in values:
        name, _, location = value.partition("=")
        if name not in CATALOGS:
            raise SystemExit(
                f"no catalog named {name!r}; the names are {', '.join(CATALOGS)}"
            )
        picked[name] = location or CATALOGS[name].source
    return picked


def ask(base_url: str, name: str) -> tuple[float, dict]:
    """One cone search, and how long it took to read the whole answer."""
    body = json.dumps(
        {
            # A mount's `path` is the address an API request names it by, never the
            # source it was mounted from.
            "url": f"file:///{name}",
            "columns": list(CATALOGS[name].columns),
            "region": [
                {
                    "type": "circle",
                    "ra": RA,
                    "dec": DEC,
                    "radius_arcsec": RADIUS_ARCSEC,
                }
            ],
            # The rows leave as they are read, which is how a client that downloads the
            # whole answer asks for it. What it takes out of the measurement is the wait
            # while the service holds a finished answer it has not started sending.
            "streaming": True,
        }
    ).encode()
    return timed(f"{base_url}/api/v1/simple/hats", body)


def quoted(name: str) -> str:
    """A column as ADQL delimits it, segment by segment.

    `_healpix_29` is not a name ADQL's grammar admits bare, and a delimited name is matched
    exactly, which is what the catalog's own spelling wants. The dot into a nested column
    stays structure rather than becoming part of one name.
    """
    return ".".join(f'"{part}"' for part in name.split("."))


def id_query(name: str) -> str:
    """The ID search: the cone's own columns, of the object at its centre, by its ids."""
    catalog = CATALOGS[name]
    columns = ", ".join(quoted(column) for column in catalog.columns)
    values = ", ".join(str(value) for value in catalog.ids)
    return f"SELECT {columns} FROM c WHERE {quoted(catalog.id_column)} IN ({values})"


def ask_ids(base_url: str, name: str) -> tuple[float, dict]:
    """One ID search, and how long it took to read the whole answer."""
    body = json.dumps(
        {
            "query": id_query(name),
            "tables": {"c": {"type": "hats", "url": f"file:///{name}"}},
        }
    ).encode()
    return timed(f"{base_url}/api/v1/adql", body)


def timed(url: str, body: bytes) -> tuple[float, dict]:
    request = urllib.request.Request(
        url, data=body, headers={"content-type": "application/json"}
    )
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=REQUEST_SECONDS) as response:
        answer = json.loads(response.read())
    return time.perf_counter() - started, answer


def measure(
    base_url: str,
    name: str,
    source: str,
    runs: int,
    asking: Callable[[str, str], tuple[float, dict]] = ask,
) -> Measured:
    """Ask one catalog `runs` times, or stop at the first refusal.

    Stopping matters: a request that 400s comes back in milliseconds, and four more of
    them would fill a row of the table with times that look like a very fast query.

    **A streamed answer says it was cut in the body, not in the status.** The rows have
    gone by the time a bound is reached, so what a collected answer reports as a 422 comes
    back here as a 200 carrying `refused` — which is a partial answer, and timing one
    against a whole one is the comparison this module exists to prevent.
    """
    measured = Measured(catalog=name, source=source)
    for _ in range(runs):
        try:
            seconds, answer = asking(base_url, name)
        except urllib.error.HTTPError as refused:
            said = refused.read().decode(errors="replace").strip()
            measured.error = f"HTTP {refused.code}: {said[:300]}"
            return measured
        except OSError as unreachable:
            measured.error = str(unreachable)
            return measured
        if answer.get("refused") is not None:
            measured.error = f"stopped part-way: {str(answer['refused'])[:300]}"
            return measured
        measured.seconds.append(seconds)
        measured.num_rows = answer.get("num_rows")
        measured.data_bytes_read = answer.get("data_bytes_read")
    return measured


def duration(seconds: float) -> str:
    return f"{seconds * 1000:.0f}ms" if seconds < 1 else f"{seconds:.2f}s"


def size(count: int | None) -> str:
    if count is None:
        return "-"
    for unit in ("B", "KiB", "MiB", "GiB"):
        if count < 1024 or unit == "GiB":
            return f"{count:.0f} {unit}" if unit == "B" else f"{count:.1f} {unit}"
        count /= 1024
    return str(count)


def table(results: list[Measured]) -> str:
    """The summary, one line per catalog."""
    head = f"{'catalog':<10}{'runs':>5}  {'min':>8}{'median':>9}{'max':>9}  {'rows':>7}  {'bytes read':>11}"
    lines = [head, "-" * len(head)]
    for measured in results:
        if not measured.seconds:
            lines.append(f"{measured.catalog:<10}{0:>5}  {measured.error or 'no runs'}")
            continue
        lines.append(
            f"{measured.catalog:<10}{len(measured.seconds):>5}  "
            f"{duration(min(measured.seconds)):>8}"
            f"{duration(statistics.median(measured.seconds)):>9}"
            f"{duration(max(measured.seconds)):>9}  "
            f"{measured.num_rows if measured.num_rows is not None else '-':>7}  "
            f"{size(measured.data_bytes_read):>11}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="query-benchmark",
        description=(
            "Time one cone search per HATS catalog through /simple/hats, and one ID search "
            "per indexed catalog through /adql."
        ),
    )
    parser.add_argument(
        "--catalog",
        action="append",
        metavar="NAME[=LOCATION]",
        help=(
            "a catalog to measure, repeatable; every one of "
            f"{', '.join(CATALOGS)} when this is not given. LOCATION reads that catalog "
            "from somewhere else — a local directory, or a url in any scheme the service "
            "reads"
        ),
    )
    parser.add_argument(
        "--runs", type=int, default=5, metavar="N", help="requests per catalog (5)"
    )
    parser.add_argument(
        "--binary",
        type=Path,
        default=DEFAULT_BINARY,
        metavar="PATH",
        help=f"the service to start ({DEFAULT_BINARY})",
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=None,
        dest="json_path",
        metavar="FILE",
        help="also write every individual time here, for comparing two builds",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=ROOT / "report",
        metavar="DIR",
        help="where the generated configuration and the service's output go",
    )
    args = parser.parse_args(argv)

    if args.runs < 1:
        raise SystemExit("--runs has to be at least 1")
    picked = chosen(args.catalog)
    if not args.binary.exists():
        raise SystemExit(
            f"no service at {args.binary}; `cargo build --release`, or pass --binary"
        )

    base_url, process = service.start(args.binary, args.report_dir, picked)
    try:
        results = [
            measure(base_url, name, source, args.runs) for name, source in picked.items()
        ]
        by_id = [
            measure(base_url, name, source, args.runs, asking=ask_ids)
            for name, source in picked.items()
            if CATALOGS[name].ids
        ]
    finally:
        process.terminate()
        process.wait(timeout=10)

    print("cone search\n")
    print(table(results))
    if by_id:
        print("\nID search\n")
        print(table(by_id))
    if args.json_path:
        args.json_path.write_text(
            json.dumps(
                {
                    "binary": str(args.binary),
                    "cone": {"ra": RA, "dec": DEC, "radius_arcsec": RADIUS_ARCSEC},
                    "catalogs": [vars(measured) for measured in results],
                    "ids": [
                        {**vars(measured), "query": id_query(measured.catalog)}
                        for measured in by_id
                    ],
                },
                indent=2,
            )
            + "\n"
        )
    failed = [f"{one.catalog} (cone)" for one in results if one.error] + [
        f"{one.catalog} (ID)" for one in by_id if one.error
    ]
    if failed:
        print(f"\nthese did not answer: {', '.join(failed)}")
    return 1 if failed else 0
