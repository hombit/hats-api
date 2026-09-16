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
#: `xfail` is the one that needs saying out loud: a check of something this service is
#: known not to offer, which failed the way it was expected to. It is not a pass — the
#: standard still asks for the thing — and it is not a failure anyone has to act on.
#: Counting the two together would hide whichever is the smaller number. `xpass` is its
#: opposite and is news: something documented as absent has started answering.
OUTCOMES = ["pass", "fail", "xpass", "xfail", "skip"]

WORDS = {
    "pass": "pass",
    "fail": "fail",
    "xpass": "unexpectedly answered",
    "xfail": "expected fail",
    "skip": "skip",
}


#: The three questions a check can speak to. A standard answers none of them on its
#: own: it says what must be there, not whether this service does it, not whether the
#: clients can use what it does, and not whether the answers are right.
QUESTIONS = {
    "standard": "follows the standard",
    "clients": "works through pyvo and STILTS",
    "answers": "answers what a reference service answers",
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
        out = [
            "## TAP conformance",
            "",
            f"**{self.summary()}**",
            "",
            f"`{self.target}` · {tools} · {self.generated}",
        ]
        if self.provenance:
            out += ["", f"_{self.provenance}_"]
        out += [
            "",
            "| | " + " | ".join(WORDS[outcome] for outcome in OUTCOMES) + " |",
            "|---|" + "--:|" * len(OUTCOMES),
        ]
        for question, asked in QUESTIONS.items():
            counts = " | ".join(
                str(self.asking(question, outcome)) for outcome in OUTCOMES
            )
            out.append(f"| {asked} | {counts} |")
        out += [
            "",
            "| area | " + " | ".join(WORDS[outcome] for outcome in OUTCOMES) + " |",
            "|---|" + "--:|" * len(OUTCOMES),
        ]
        for area in self.areas():
            counts = " | ".join(
                str(self.count(outcome, area)) for outcome in OUTCOMES
            )
            out.append(f"| {area} | {counts} |")

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

        absent = [result for result in self.results if result.outcome == "xfail"]
        if absent:
            out += ["", "### Absent on purpose", ""]
            out += [
                f"- `{result.id}` — {cell(result.description or result.detail, 200)}"
                for result in absent
            ]

        if full:
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
    """Both shapes, plus the trimmed one a comment is posted from."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(report.as_json())
    markdown = directory / "report.md"
    markdown.write_text(report.as_markdown(full=True))
    comment = report.as_markdown(full=len(report.results) < 120)
    # GitHub refuses a comment body over 65536 characters, and a refused comment is a
    # run that reported nothing where it mattered most.
    if len(comment) > 60_000:
        comment = report.as_markdown(full=False)[:60_000]
    (directory / "comment.md").write_text(comment)
    return markdown
