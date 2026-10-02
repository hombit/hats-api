"""STILTS as a user runs it, rather than as a validator.

`taplint` lints: it decides whether a service conforms, composing its own queries out of
the metadata to do it. It is not how anybody gets data. `stilts tapquery` is — it is the
task TOPCAT runs underneath when a user types a query and presses go — and it is the
only way to ask the question this module exists for: given one query that a person
actually wrote, do both of the clients they might be using come back with the same rows.

A service can conform and answer one of them wrong. It can also conform and hand one of
them something its parser refuses, which looks like a broken service to half the world
and like a clean validator run to whoever is maintaining it.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from astropy.table import Table

#: One query, one client, one answer. Long enough for a cone over a real catalog on S3.
TIMEOUT = 600


class Unavailable(RuntimeError):
    """STILTS could not be run, which is not a fault of the service under test."""


def query(
    command: list[str],
    base_url: str,
    adql: str,
    maxrec: int | None = None,
    uploads: dict[str, Path] | None = None,
    upload_format: str | None = None,
    ofmt: str = "votable",
) -> Table:
    """Run one ADQL statement through STILTS and read back what it got.

    The answer comes back as a VOTable written to a file and parsed by astropy, so what
    is compared afterwards is rows and column metadata rather than two clients' ideas of
    how to print a float.

    `ofmt="ecsv"` has STILTS write what it read as ECSV instead, for an answer astropy's
    VOTable reader refuses and STILTS does not: read as a VOTable, the failure would be
    astropy's and be reported as STILTS'.

    `sync=true` because that is what this suite is about, and because a service without
    an async resource would otherwise be asked to submit a job.

    `uploads` maps a `TAP_UPLOAD` name to a table file, which STILTS reads and sends inline
    the way TOPCAT does — `nupload`, `upload<n>` and `upname<n>` are its own parameters —
    serialized as `upload_format` says, `TABLEDATA`, `BINARY` or `BINARY2`.
    """
    with tempfile.TemporaryDirectory() as scratch:
        out = Path(scratch) / f"answer.{ofmt}"
        arguments = [
            *command,
            "tapquery",
            f"tapurl={base_url}",
            f"adql={adql}",
            "sync=true",
            f"ofmt={ofmt}",
            f"out={out}",
        ]
        if maxrec is not None:
            arguments.append(f"maxrec={maxrec}")
        if uploads:
            arguments.append(f"nupload={len(uploads)}")
            for number, (name, location) in enumerate(uploads.items(), start=1):
                arguments += [f"upload{number}={location}", f"upname{number}={name}"]
        if upload_format is not None:
            arguments.append(f"upvotformat={upload_format}")
        try:
            spoken = subprocess.run(
                arguments, capture_output=True, text=True, timeout=TIMEOUT
            )
        except FileNotFoundError as missing:
            raise Unavailable(str(missing)) from missing
        except subprocess.TimeoutExpired as slow:
            raise AssertionError(f"STILTS got no answer in {TIMEOUT}s") from slow

        if not out.exists():
            # STILTS writes nothing when the service refused the query, and what it
            # says about why is on stderr.
            said = (spoken.stderr or spoken.stdout or "").strip().splitlines()
            raise AssertionError(
                f"STILTS got no table back (exit {spoken.returncode}): "
                f"{' / '.join(said[-3:]) or 'it said nothing'}"
            )
        return Table.read(out, format={"votable": "votable", "ecsv": "ascii.ecsv"}[ofmt])
