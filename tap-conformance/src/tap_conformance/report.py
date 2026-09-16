"""What a run found, in the two shapes it is written in.

JSON for anything that reads a run rather than a person, and Markdown for the pull
request comment and the terminal.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

#: The outcomes, in the order a summary line reads them.
#:
#: There is no "expected failure" among them, and there is no marking a check as one.
#: Whether a service has decided not to implement something is a fact about that
#: service's plans; a suite that knew about those decisions would be one written against
#: an implementation, which is the single thing this is for not being. A MUST that goes
#: unanswered is a failure here whoever is asked and whyever it is missing. What to do
#: about it — implement it, or write down that it is deliberate — is a decision made
#: somewhere a measurement cannot reach.
#:
#: `xpass` and `xfail` stay in the vocabulary because pytest can still produce them if
#: a check is ever marked, and a report that dropped them on the floor would be
#: reporting less than it was told. Nothing here marks any.
OUTCOMES = ["pass", "fail", "xpass", "xfail", "skip"]

WORDS = {
    "pass": "pass",
    "fail": "fail",
    "xpass": "answered despite being marked absent",
    "xfail": "marked absent",
    "skip": "skip",
}


#: The three questions a check can speak to. A standard answers none of them on its
#: own: it says what must be there, not whether this service does it, not whether the
#: clients can use what it does, and not whether the answers are right.
QUESTIONS = {
    "standard": "checks that read the standard",
    "clients": "checks that go through a client",
    "answers": "checks against a reference answer",
}


@dataclass
class Result:
    """One check."""

    id: str
    area: str
    outcome: str
    asks: list[str] = field(default_factory=lambda: ["standard"])
    description: str = ""
    detail: str = ""


@dataclass
class Report:
    target: str
    tools: dict[str, str] = field(default_factory=dict)
    provenance: str = ""
    results: list[Result] = field(default_factory=list)
    #: Why this run cannot be read as a score, if it cannot: the service fell over, or
    #: the suite did. Counts of failures mean nothing then, and the report has to say so
    #: before the numbers rather than after them.
    broken: list[str] = field(default_factory=list)
    generated: str = field(
        default_factory=lambda: dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    )

    def count(self, outcome: str, area: str | None = None) -> int:
        return sum(
            1
            for result in self.results
            if result.outcome == outcome and (area is None or result.area == area)
        )

    def asking(self, question: str, outcome: str) -> int:
        return sum(
            1
            for result in self.results
            if question in result.asks and result.outcome == outcome
        )

    def areas(self) -> list[str]:
        """In the order they were first reported, which is the order the suite runs."""
        seen: list[str] = []
        for result in self.results:
            if result.area not in seen:
                seen.append(result.area)
        return seen

    def summary(self) -> str:
        counts = [
            f"{self.count(outcome)} {WORDS[outcome]}"
            for outcome in OUTCOMES
            if self.count(outcome)
        ]
        if not counts:
            return "no checks ran"
        return f"{' · '.join(counts)} — of {len(self.results)} checks"

    def as_json(self) -> str:
        return (
            json.dumps(
                {
                    "generated": self.generated,
                    "target": self.target,
                    "tools": self.tools,
                    "provenance": self.provenance,
                    "broken": self.broken,
                    "totals": {
                        outcome: self.count(outcome) for outcome in OUTCOMES
                    },
                    "questions": {
                        question: {
                            outcome: self.asking(question, outcome)
                            for outcome in OUTCOMES
                        }
                        for question in QUESTIONS
                    },
                    "checks": [asdict(result) for result in self.results],
                },
                indent=2,
            )
            + "\n"
        )

    def as_markdown(self, full: bool = True) -> str:
        """`full` keeps the table of every check.

        A pull request comment has a size limit and a run with three hundred rows in
        it would reach it, so the comment drops that table and points at the artifact.
        """
        tools = " · ".join(f"{name} {version}" for name, version in sorted(self.tools.items()))
        out = ["## TAP conformance", ""]
        if self.broken:
            out += [
                "> [!WARNING]",
                "> **This run is not a conformance result.** Something below did not "
                "get as far as asking, so the counts say nothing about the service:",
                ">",
                *[f"> - {cell(reason, 400)}" for reason in self.broken],
                "",
            ]
        # The target is worth naming when it is somebody's service and worth nothing
        # when it is a port this run opened and closed. What always belongs here is
        # which clients produced the numbers, a suite run a year from now against newer
        # ones being a different measurement.
        where = f"`{self.target}` · " if full else ""
        out.append(f"{where}{tools} · {self.generated}")
        if self.provenance and full:
            out += ["", f"_{self.provenance}_"]
        # Only the outcomes this run produced get a column. An empty one is a column
        # every reader has to work out the meaning of to find it says nothing.
        shown = [
            outcome
            for outcome in OUTCOMES
            if outcome in ("pass", "fail", "skip") or self.count(outcome)
        ]
        header = "| " + " | ".join(WORDS[outcome] for outcome in shown) + " |"
        rule = "|---|" + "--:|" * len(shown)

        # The whole run, as the last row rather than as a sentence above the table. It
        # is not the sum of the rows above it: a check speaks to more than one question,
        # so those overlap, and this counts each check once.
        whole = " | ".join(f"**{self.count(outcome)}**" for outcome in shown)
        total = f"| **every check, counted once** | {whole} |"
        # The three rows above overlap — a check that goes through a client is usually
        # reading the standard too — so they do not add up to the last one, and a reader
        # who tries to add them deserves to be told why rather than left to wonder.
        note = (
            "_The first three overlap: most checks speak to more than one question. "
            "The last row counts each of the "
            f"{len(self.results)} checks once._"
        )

        out += ["", f"| |{header[1:]}", rule]
        for question, asked in QUESTIONS.items():
            counts = " | ".join(str(self.asking(question, outcome)) for outcome in shown)
            out.append(f"| {asked} | {counts} |")
        out += [total, "", note]
        if full:
            out += ["", f"| area |{header[1:]}", rule]
            for area in self.areas():
                counts = " | ".join(str(self.count(outcome, area)) for outcome in shown)
                out.append(f"| {area} | {counts} |")
            out.append(total)

        if not full:
            # A comment is read for the score and for which of the three questions
            # moved it. Everything else — every check with what it found, where the
            # reference answers came from — is in the run's artifact, and putting it
            # here buries the one table worth reading.
            return "\n".join(out) + "\n"

        failures = [result for result in self.results if result.outcome == "fail"]
        if failures:
            out += ["", "### What is not answered", "", "| check | area | what happened |", "|---|---|---|"]
            for result in failures:
                out.append(f"| `{result.id}` | {result.area} | {cell(result.detail)} |")

        news = [result for result in self.results if result.outcome == "xpass"]
        if news:
            out += ["", "### Answered after all", ""]
            out += [
                f"- `{result.id}` — {cell(result.description or result.detail, 200)}"
                for result in news
            ]

        out += [
            "",
            "<details><summary>Every check</summary>",
            "",
            "| check | area | outcome | detail |",
            "|---|---|---|---|",
        ]
        for result in self.results:
            out.append(
                f"| `{result.id}` | {result.area} | {WORDS[result.outcome]} "
                f"| {cell(result.detail)} |"
            )
        out += ["", "</details>"]
        return "\n".join(out) + "\n"


def cell(text: str, limit: int = 300) -> str:
    """One value, safe to put between two pipes.

    A detail is whatever a client said, which is a line of XML as often as not — so
    the pipe that would end the cell, the newline that would end the row and the
    backtick that would open a code span all have to go.
    """
    flattened = (
        (text or "")
        .replace("|", "/")
        .replace("`", "'")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
    ).strip()
    if len(flattened) <= limit:
        return flattened
    return flattened[:limit] + "…"


def write(report: Report, directory: Path) -> Path:
    """Both shapes, plus the short one a comment is posted from."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(report.as_json())
    markdown = directory / "report.md"
    markdown.write_text(report.as_markdown(full=True))
    (directory / "comment.md").write_text(report.as_markdown(full=False))
    return markdown
