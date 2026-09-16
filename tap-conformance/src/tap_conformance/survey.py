"""The suite, run here and read beside what other servers make of the same checks.

This is not a study of anyone else's service. What the reference columns are for is
deciding **what matters**, which a standard cannot tell you — it marks everything MUST
or SHOULD and stops there:

- **Which features matter.** A feature every established server implements is one
  clients depend on, whatever the standard calls it. A feature none of them implements
  is one to think twice about before spending a week on it — and, as likely, a check
  here reading the standard more strictly than anyone reads it in practice.
- **Which details matter.** A check this service passes as *standard* while the same
  area fails as *clients* is a detail that decides whether the feature can be used at
  all. Nothing in a specification marks those, and they are the ones to get right first.

Each service is a separate pytest run, because one session talks to one service. The
reference runs are **not** part of an ordinary run and never part of CI: they put a few
dozen questions to somebody else's service, which is not a thing to do on every push.
They are run by hand, what they found is committed under `references/`, and this reads
that. `--refresh-references` re-runs them.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent.parent

#: Where a reference run is kept once it has been made. Committed, because it is the
#: only way the matrix can be rendered without asking somebody else's service again —
#: and because a change in what a reference server answers is then a diff someone
#: reviews rather than a number that moved.
SNAPSHOTS = HERE / "references"

#: The services the checks are put to besides this one.
#:
#: Four different implementations on purpose: ESA's own stack, DaCHS — written by one of
#: the people who wrote the standards — IRSA's and MAST's. One service answering a check
#: one way is an anecdote about that service; four agreeing is what the standard turned
#: out to mean in practice, which is the only thing this list is for.
#:
#: Two serve Gaia DR3, so a disagreement can be read against the same rows, and IRSA
#: serves both of the catalogs published here — it is where the HATS copies come from —
#: so it is the one that can be asked the same question about the same data.
#:
#: Each is asked a few dozen questions per refresh, which is why the list is short, why
#: a run is kept rather than repeated, and why none of this happens in CI.
#:
#: Tried and left out: VizieR, whose table list runs to tens of thousands of entries —
#: `pyvo` reads it before it can ask anything and does not come back in any time worth
#: waiting, which is a fact about VOSI at that scale rather than about the service. The
#: GAVO data centre and CADC do not resolve from here and are worth another attempt from
#: a network that can see them.
REFERENCES = {
    "ESA Gaia": "https://gea.esac.esa.int/tap-server/tap",
    "ARI-Gaia": "https://gaia.ari.uni-heidelberg.de/tap",
    "IRSA": "https://irsa.ipac.caltech.edu/TAP",
    "MAST": "https://mast.stsci.edu/vo-tap/api/v0.1/caom/",
}

#: Statuses in the order a column reads them.
MARKS = {
    "pass": "yes",
    "fail": "**no**",
    "xfail": "absent",
    "xpass": "yes (documented absent)",
    "skip": "—",
}


def slug(name: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in name).strip("-")


def run(arguments: list[str], where: Path) -> int:
    """One pytest session. Its exit status is ignored: a failing check is the output."""
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-c", "pyproject.toml", *arguments],
        cwd=where,
    ).returncode


def read_report(path: Path) -> dict | None:
    try:
        return json.loads((path / "report.json").read_text())
    except (OSError, json.JSONDecodeError):
        return None


def matrix(reports: dict[str, dict]) -> str:
    """Every check, against every service that was asked."""
    services = list(reports)
    checks: dict[str, dict] = {}
    for service, report in reports.items():
        for check in report["checks"]:
            entry = checks.setdefault(
                check["id"],
                {"area": check["area"], "description": check.get("description", ""), "by": {}},
            )
            entry["by"][service] = check

    out = [
        "# TAP conformance across services",
        "",
        "The same checks, here and against services that have been answering TAP for "
        "years. Read it for what to do next rather than for what anybody else does: a "
        "column is one service, a row is one thing a standard asks for, and the "
        "sections below the tables are the ones worth acting on.",
        "",
        "| service | | result |",
        "|---|---|---|",
    ]
    for service, report in reports.items():
        out.append(
            f"| **{service}** | `{report['target']}` | {report['totals_line']} |"
        )
    out += ["", f"_generated {next(iter(reports.values()))['generated']}_", ""]

    areas: dict[str, list] = {}
    for identifier, entry in checks.items():
        areas.setdefault(entry["area"], []).append((identifier, entry))

    for area, rows in areas.items():
        out += [f"## {area}", "", "| check | " + " | ".join(services) + " |",
                "|---|" + "---|" * len(services)]
        for identifier, entry in sorted(rows):
            marks = [
                MARKS.get(entry["by"].get(service, {}).get("outcome", "skip"), "—")
                for service in services
            ]
            out.append(f"| `{identifier}` | " + " | ".join(marks) + " |")
        out.append("")

    gaps = [
        (identifier, entry)
        for identifier, entry in checks.items()
        if entry["by"].get(services[0], {}).get("outcome") == "fail"
        and any(
            entry["by"].get(service, {}).get("outcome") == "pass"
            for service in services[1:]
        )
    ]
    if gaps:
        out += [
            "## Worth doing: asked for, and everyone else does it",
            "",
            "Checks this service fails and a reference service passes. A feature here "
            "is one clients meet elsewhere and will expect to find here — which is the "
            "argument for doing it that the standard cannot make on its own.",
            "",
            "| check | what it asks |",
            "|---|---|",
        ]
        for identifier, entry in sorted(gaps):
            out.append(f"| `{identifier}` | {cell(entry['description'])} |")
        out.append("")

    unimplemented = [
        identifier
        for identifier, entry in checks.items()
        if all(
            entry["by"].get(service, {}).get("outcome") in ("fail", "xfail")
            for service in services
        )
        and len(entry["by"]) == len(services)
    ]
    if unimplemented:
        out += [
            "## Worth questioning: asked for, and nobody does it",
            "",
            "No service in this survey passes these. Either the check reads the "
            "standard more strictly than anyone implements it, or it is a corner every "
            "service cuts. Decide which before spending anything on them — and where "
            "the check is the thing that is wrong, it is the check that should go.",
            "",
        ]
        out += [f"- `{identifier}`" for identifier in sorted(unimplemented)]
        out.append("")

    out += details_that_decide(reports[services[0]])
    return "\n".join(out) + "\n"


def details_that_decide(report: dict) -> list[str]:
    """Areas this service conforms in and a client still cannot use.

    The other thing the survey is for. A standard marks every clause MUST or SHOULD and
    has no way to say which of them a client will fall over — so the way to find that
    out is to have both sorts of check in the same area and watch them disagree. Where
    the documents are right and `pyvo` or STILTS still fails, what is wrong is a detail
    nobody wrote down, and it is worth more than the next feature.
    """
    areas: dict[str, dict[str, int]] = {}
    for check in report["checks"]:
        counts = areas.setdefault(check["area"], {"standard": 0, "clients": 0})
        if check["outcome"] != "fail":
            continue
        for question in check.get("asks", []):
            if question in counts:
                counts[question] += 1

    disagreeing = [
        area
        for area, counts in areas.items()
        if counts["clients"] and not counts["standard"]
    ]
    if not disagreeing:
        return []
    return [
        "## Worth first: conforming here, and a client still cannot use it",
        "",
        "In these areas nothing fails the checks that read the standard, and something "
        "fails the checks that go through a client. Whatever is wrong is a detail no "
        "specification writes down, and it is what decides whether the feature is "
        "reachable at all.",
        "",
        *[f"- {area}" for area in sorted(disagreeing)],
        "",
    ]


def cell(text: str, limit: int = 240) -> str:
    flattened = " ".join((text or "").replace("|", "/").replace("`", "'").split())
    return flattened if len(flattened) <= limit else flattened[:limit] + "…"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-dir", type=Path, default=HERE / "report", help="where everything goes"
    )
    parser.add_argument(
        "--reference",
        action="append",
        default=[],
        metavar="NAME=URL",
        help="a service to compare against, in place of the built-in list",
    )
    parser.add_argument(
        "--refresh-references",
        action="store_true",
        help=(
            "put the checks to the reference services again and rewrite the committed "
            "snapshots. Talks to somebody else's service, so it is run by hand"
        ),
    )
    parser.add_argument(
        "--reference-taplint",
        action="store_true",
        help="run the validator against the reference services too, which is minutes each",
    )
    parser.add_argument(
        "--skip-this-service",
        action="store_true",
        help="refresh the snapshots and render the matrix without running anything here",
    )
    known, extra = parser.parse_known_args()

    references = (
        dict(entry.split("=", 1) for entry in known.reference)
        if known.reference
        else REFERENCES
    )
    reports: dict[str, dict] = {}

    if not known.skip_this_service:
        here = known.report_dir / "hats-api"
        run(["--report-dir", str(here), *extra], HERE)
        found = read_report(here)
        if found:
            reports["this service"] = found

    for name, url in references.items():
        snapshot = SNAPSHOTS / f"{slug(name)}.json"
        if known.refresh_references:
            where = known.report_dir / "references" / slug(name)
            arguments = ["--base-url", url, "--report-dir", str(where), *extra]
            # The validator against a reference service is minutes, and what it says
            # about one moves on their release schedule rather than on ours.
            if not known.reference_taplint:
                arguments.append("--skip-taplint")
            run(arguments, HERE)
            fresh = read_report(where)
            if fresh:
                SNAPSHOTS.mkdir(parents=True, exist_ok=True)
                snapshot.write_text(json.dumps(fresh, indent=2) + "\n")
        if snapshot.exists():
            reports[name] = json.loads(snapshot.read_text())
        else:
            print(
                f"no snapshot for {name}; run with --refresh-references to make one",
                file=sys.stderr,
            )

    if not reports:
        print("nothing to report", file=sys.stderr)
        return 1

    for report in reports.values():
        report["totals_line"] = " · ".join(
            f"{count} {name}" for name, count in report["totals"].items() if count
        )

    known.report_dir.mkdir(parents=True, exist_ok=True)
    written = known.report_dir / "matrix.md"
    written.write_text(matrix(reports))
    print(f"\n{written}")
    for name, report in reports.items():
        print(f"  {name}: {report['totals_line']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
