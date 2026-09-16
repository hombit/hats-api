"""STILTS `taplint`, run once and read by whichever tests are about its stages.

`taplint` is the validator TAP service operators are actually judged by — it is what
the IVOA's own validation pages run — so it is here whole rather than reimplemented in
pieces. Each of its stages lands in the test file for the feature it is about, so a
report reads as one list of features rather than as two lists from two tools.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

#: The stages asked for, and the file each is read in. Naming them rather than taking
#: the default set is what keeps the report's rows stable: a STILTS release that adds a
#: stage would otherwise add a row nothing here has decided the meaning of.
#:
#: ObsCore, ObsLocTAP and EPN-TAP are left out. They validate data models a service
#: chooses to publish, and one that publishes none of them is not less conforming for
#: it — their stages would report a service's whole surface as missing.
STAGES = {
    "TMV": "Validate table metadata against XML schema",
    "TME": "Check content of tables metadata from /tables",
    "TMS": "Check content of tables metadata from TAP_SCHEMA",
    "TMC": "Compare table metadata from /tables and TAP_SCHEMA",
    "UUC": "Check column units and UCDs are legal",
    "CPV": "Validate capabilities against XML schema",
    "CAP": "Check TAP and TAPRegExt content of capabilities document",
    "AVV": "Validate availability against XML schema",
    "QGE": "Make ADQL queries in sync GET mode",
    "QPO": "Make ADQL queries in sync POST mode",
    "QAS": "Make ADQL queries in async mode",
    "UWS": "Test asynchronous UWS/TAP behaviour",
    "MDQ": "Check table query result columns against declared metadata",
    "EXA": "Check content of examples document",
    "UPL": "Make queries with table uploads",
}

#: How long the whole validator is given. It asks a lot of questions, and a catalog on
#: S3 answers each of them over the network.
TIMEOUT = 1800


@dataclass
class Section:
    """One stage's findings."""

    code: str
    title: str
    reports: list[dict]

    def at(self, level: str) -> list[dict]:
        return [entry for entry in self.reports if entry.get("level") == level]

    @property
    def errors(self) -> list[dict]:
        """What the validator calls a violation of the standard."""
        return self.at("ERROR")

    @property
    def failures(self) -> list[dict]:
        """What the validator could not test, usually because of an error above."""
        return self.at("FAILURE")

    @property
    def warnings(self) -> list[dict]:
        """Questionable, or against a recommendation, but not a violation."""
        return self.at("WARNING")

    def summarize(self, limit: int = 3) -> str:
        """The sentence a report row carries."""
        counts = (
            f"{len(self.errors)} errors, {len(self.warnings)} warnings, "
            f"{len(self.failures)} untestable"
        )
        first = self.errors + self.failures + self.warnings
        if not first:
            return counts
        named = "; ".join(
            f"{entry.get('code', '?')}: {entry.get('text', '')}"[:200]
            for entry in first[:limit]
        )
        return f"{counts} — {named}"


@dataclass
class Run:
    """Everything one invocation of the validator said."""

    sections: dict[str, Section]
    version: str
    command: list[str]
    #: Set when the validator could not be run at all, in which case every stage test
    #: skips rather than failing: an absent validator is not a service's fault.
    unavailable: str | None = None

    def section(self, stage: str) -> Section:
        return self.sections.get(stage, Section(stage, STAGES.get(stage, stage), []))


#: Codes the validator uses to say there was nothing there to look at.
#:
#: These come back as warnings, because `taplint` is lenient about resources TAP 1.0
#: called optional — so a stage that validates a document reports "no document" and
#: passes. That is a stage which checked nothing being counted as a stage which found
#: nothing wrong, and it is how this suite came to score two points against a service
#: with no TAP at all.
NOTHING_THERE = {
    "GONO",  # optional resource not present
    "TBNF",  # /tables resource absent
    "NOTM",  # no table metadata available, so later stages did not run
    "GONE",  # table metadata absent
}


def assert_clean(section: Section) -> None:
    """A stage passes when the validator looked at something and found it sound.

    A warning is not a failure — the validator says so itself, warnings being for
    behaviour that is questionable rather than wrong — but it is carried into the
    report's detail, so a stage that passes with reservations says so. The exception is
    a warning that says the thing was not there at all, which is not a pass in anybody's
    reading.
    """
    if section.errors or section.failures:
        raise AssertionError(section.summarize())
    absent = [
        entry for entry in section.reports if entry.get("code") in NOTHING_THERE
    ]
    if absent:
        raise AssertionError(
            "nothing was there to validate — "
            + "; ".join(
                f"{entry.get('code')}: {entry.get('text', '')}"[:200] for entry in absent[:3]
            )
        )


def command(stilts: str | None, jar: Path | None) -> list[str] | None:
    """How to invoke STILTS here, or nothing if it is not installed."""
    if jar is not None:
        java = shutil.which("java")
        return [java, "-jar", str(jar)] if java else None
    name = stilts or "stilts"
    found = shutil.which(name)
    return [found] if found else None


def version(base: list[str]) -> str:
    try:
        spoken = subprocess.run(
            [*base, "-version"], capture_output=True, text=True, timeout=120
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    found = re.search(r"STILTS version (\S+)", spoken)
    return found.group(1) if found else "unknown"


def run(base_url: str, stilts: str | None, jar: Path | None, out: Path) -> Run:
    """Validate the service once, and keep what was said.

    `interface=tap1.1` rather than the default `cap`: the endpoints are then the
    standard ones below the base url, so a stage that fails fails about itself. Read
    from the capabilities document instead, a service whose capabilities are wrong
    reports every other stage as missing, which says nothing about any of them.
    """
    base = command(stilts, jar)
    if base is None:
        return Run({}, "absent", [], unavailable="STILTS is not installed")

    arguments = [
        *base,
        "taplint",
        f"tapurl={base_url}",
        f"stages={' '.join(STAGES)}",
        "format=json",
        "report=EWISF",
        "interface=tap1.1",
        # A service with several tables would otherwise have every one of them queried
        # through every stage, and one of these tables is the whole of Gaia DR3.
        "maxtable=2",
        "maxrepeat=4",
        "truncate=400",
    ]
    try:
        spoken = subprocess.run(
            arguments, capture_output=True, text=True, timeout=TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return Run({}, version(base), arguments, unavailable=f"no answer in {TIMEOUT}s")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(spoken.stdout or "")
    (out.parent / f"{out.stem}.stderr.txt").write_text(spoken.stderr or "")

    try:
        document = json.loads(spoken.stdout)
    except json.JSONDecodeError:
        tail = (spoken.stderr or spoken.stdout or "").strip().splitlines()[-3:]
        return Run(
            {},
            version(base),
            arguments,
            unavailable=f"the validator wrote no report: {' / '.join(tail)}",
        )

    sections = {
        entry["code"]: Section(
            entry["code"], entry.get("title", ""), entry.get("reports", [])
        )
        for entry in document.get("sections", [])
    }
    return Run(sections, version(base), arguments)
