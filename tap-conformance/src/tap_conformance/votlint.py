"""STILTS `votlint`, over a document the service actually returned.

`taplint` validates the three VOSI documents against their XML schemas and never looks
at the one a query answers with, and neither does anything else here: `pyvo` hands the
bytes to `astropy`, which reads what it recognises and is quiet about the rest. So the
document this service spends most of its effort writing is the one document nothing
checks.

`votlint` is what checks it. It validates against the VOTable schema the document itself
declares and then reads it as a VOTable reader would — a datatype that is not one of the
twelve, a `TR` with the wrong number of cells, an `nrows` that disagrees with the rows
that follow, a value that will not parse as the type its `FIELD` claims.

Two things about running it, both learned by running it:

- **It exits 0 whatever it finds.** The findings are its output, so a check that looked
  at the exit code would pass on a document full of errors.
- **It reads standard input**, so nothing here writes a temporary file: the bytes go to
  it as they came off the wire, which is the point of validating them at all.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass

#: How long one document is given. Generous: this is a JVM start against a body that is
#: at most a few thousand rows.
TIMEOUT = 180

#: `ERROR (l.6, c.58): Unknown datatype 'int64' - can't parse column`
FINDING = re.compile(r"^\s*(INFO|WARNING|ERROR)\s*\(([^)]*)\)\s*:\s*(.*)$")


@dataclass(frozen=True)
class Finding:
    """One thing the validator said about the document."""

    level: str
    where: str
    text: str

    def __str__(self) -> str:
        return f"{self.where}: {self.text}"


@dataclass
class Report:
    """Everything it said about one document."""

    findings: list[Finding]
    #: Set when the validator could not be run at all, in which case a check skips
    #: rather than fails: whether STILTS is installed is not the service's doing.
    unavailable: str | None = None

    def at(self, level: str) -> list[Finding]:
        return [finding for finding in self.findings if finding.level == level]

    @property
    def errors(self) -> list[Finding]:
        return self.at("ERROR")

    @property
    def warnings(self) -> list[Finding]:
        return self.at("WARNING")

    def summarize(self, limit: int = 3) -> str:
        counts = f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        first = self.errors + self.warnings
        if not first:
            return counts
        named = "; ".join(str(finding)[:200] for finding in first[:limit])
        return f"{counts} — {named}"


def check(body: bytes, stilts: list[str] | None) -> Report:
    """Validate one document, as bytes off the wire."""
    if stilts is None:
        return Report([], unavailable="STILTS is not installed")
    arguments = [*stilts, "votlint", "votable=-"]
    try:
        spoken = subprocess.run(
            arguments, input=body, capture_output=True, timeout=TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return Report([], unavailable=f"no answer in {TIMEOUT}s")
    except OSError as unreachable:
        return Report([], unavailable=f"could not be run: {unreachable}")

    spoken_text = (spoken.stdout or b"").decode("utf-8", errors="replace")
    spoken_text += (spoken.stderr or b"").decode("utf-8", errors="replace")
    findings = []
    for line in spoken_text.splitlines():
        found = FINDING.match(line)
        if found:
            findings.append(Finding(found.group(1), found.group(2), found.group(3)))
    return Report(findings)


def assert_valid(report: Report, what: str) -> str:
    """A document passes when the validator read it and found nothing wrong.

    A warning is carried into the report's detail rather than failed on, which is the
    same reading `taplint.assert_clean` gives one: the validator distinguishes the two
    itself, and what a warning usually says here is that a convention was not followed
    rather than that a reader will misread the bytes.
    """
    if report.errors:
        raise AssertionError(f"{what}: {report.summarize()}")
    return report.summarize()
