"""How a run is set up, and how it ends up as a report.

One command runs the suite: it starts the service, points `pyvo` and STILTS at it,
and writes what they made of it. What it does not do is decide whether the result is
acceptable — a conformance run is a report, so by default it exits zero however much
of the standard is unanswered. `--strict-conformance` is for whoever wants the other
behaviour.
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import pytest

from tap_conformance import report as reporting
from tap_conformance import service as under_test
from tap_conformance import taplint as validator

HERE = Path(__file__).parent.parent
REPOSITORY = HERE.parent

#: What each test file is about, for a test that names no area of its own.
AREAS = {
    "test_availability": "VOSI availability",
    "test_capabilities": "VOSI capabilities",
    "test_tables_metadata": "VOSI tables",
    "test_tap_schema": "TAP_SCHEMA",
    "test_sync_query": "sync query",
    "test_output_formats": "output formats",
    "test_maxrec": "MAXREC and overflow",
    "test_errors": "errors",
    "test_adql": "ADQL",
    "test_examples": "examples",
    "test_async": "async",
    "test_upload": "uploads",
    "test_reference_data": "against a reference service",
}


@pytest.fixture(scope="session")
def report_dir(pytestconfig) -> Path:
    directory = Path(pytestconfig.getoption("--report-dir"))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture(scope="session")
def data(pytestconfig) -> Path:
    return Path(pytestconfig.getoption("--data"))


@pytest.fixture(scope="session")
def manifest(data) -> dict | None:
    """What the fetch step published, or nothing if it has not run.

    A missing one is not an error: the suite still has everything it needs to ask a
    service about itself. What it loses is the half that compares an answer against a
    reference one, and those checks skip saying so.
    """
    path = data / "MANIFEST.json"
    return json.loads(path.read_text()) if path.exists() else None


@pytest.fixture(scope="session")
def queries(data) -> list[dict]:
    path = data / "queries.json"
    return json.loads(path.read_text()) if path.exists() else []


@pytest.fixture(scope="session")
def service(pytestconfig, data, manifest, report_dir):
    """The service every check is put to."""
    given = pytestconfig.getoption("--base-url")
    if given:
        started = under_test.Service(base_url=given.rstrip("/"))
        yield started
        return

    binary = Path(pytestconfig.getoption("--server-binary"))
    if not binary.exists():
        pytest.exit(
            f"{binary} is not there — build it first, or pass --base-url", returncode=2
        )
    tables = published_tables(data, manifest)
    catalogs = data / "hats"
    started = under_test.start(
        binary, catalogs if catalogs.is_dir() else None, tables, report_dir
    )
    yield started
    # Before stopping it: a service that is already gone went on its own, which is a
    # crash. Every check after that point answered nothing, so the report is not one to
    # read as a score — and the run says so by exiting non-zero.
    if started.process is not None and started.process.poll() is not None:
        BROKEN.append(
            f"the service exited during the run with status {started.process.returncode}; "
            f"its output is in {started.log}"
        )
    started.stop()


def published_tables(data: Path, manifest: dict | None) -> list[tuple[str, str]]:
    """Each table as the name and url a config entry is written from.

    A sample catalog that was never built is left out rather than named: the service
    checks a table's url at startup, so one missing directory would cost every other
    table its chance to be served.
    """
    if not manifest:
        return []
    tables = []
    for table in manifest["tables"]:
        if table.get("url"):
            tables.append((table["name"], table["url"]))
            continue
        directory = table.get("directory")
        if directory and (data / directory).is_dir():
            below = directory.removeprefix("hats/")
            tables.append((table["name"], f"file://{under_test.MOUNT_PATH}/{below}"))
    return tables


@pytest.fixture(scope="session")
def tap(service):
    """The `pyvo` client, which is how every check that is about rows asks."""
    import pyvo

    warnings.filterwarnings("ignore", category=UserWarning)
    client = pyvo.dal.TAPService(service.base_url)
    client._session.timeout = 300
    return client


@pytest.fixture(scope="session")
def published(tap) -> list[str]:
    """The table names the service says it has, VOSI being how a client asks.

    Empty rather than an exception when the resource is not there: a suite whose every
    test errored in setup would report nothing about the resources that do answer.
    """
    try:
        return list(tap.tables.keys())
    except Exception:  # noqa: BLE001 — a service with no table metadata is the finding
        return []


@pytest.fixture(scope="session")
def queryable(published, manifest) -> str:
    """A table to put a question to: the smallest of the suite's own, if it is there.

    The order matters for how long a run takes. The sample catalog is a few thousand
    rows and answers in milliseconds; the next one is the whole of Gaia DR3.
    """
    if manifest:
        for table in manifest["tables"]:
            if table["name"] in published:
                return table["name"]
    for name in published:
        if not name.upper().startswith("TAP_SCHEMA"):
            return name
    pytest.skip("the service publishes no table to query")


@pytest.fixture(scope="session")
def coordinates(manifest, queryable) -> tuple[str, str]:
    """Which columns of the queryable table hold a position."""
    for table in (manifest or {}).get("tables", []):
        if table["name"] == queryable:
            return table["ra_column"], table["dec_column"]
    pytest.skip(f"nothing here knows which columns of {queryable} are a position")


@pytest.fixture(scope="session")
def center(manifest) -> tuple[float, float]:
    where = (manifest or {}).get("center", {"ra": 45.0, "dec": 0.0})
    return where["ra"], where["dec"]


@pytest.fixture(scope="session")
def raw(tap):
    """The response to a query pyvo built, unparsed.

    For the checks that are about the bytes — a media type, a status, a format this
    client cannot read — rather than about the rows. Still pyvo's request: what is
    being tested is what a client sends, not what this suite can compose.
    """

    def send(query: str, **parameters):
        return tap.create_query(query, **parameters).submit()

    return send


@pytest.fixture(scope="session")
def rows_query(queryable):
    """`SELECT TOP n *` against the cheapest table the service publishes."""

    def query(count: int = 5) -> str:
        return f"SELECT TOP {count} * FROM {queryable}"

    return query


@pytest.fixture(scope="session")
def stilts_command(pytestconfig) -> list[str] | None:
    """How to invoke STILTS here, for the checks that use it as a user would."""
    jar = pytestconfig.getoption("--stilts-jar")
    return validator.command(pytestconfig.getoption("--stilts"), Path(jar) if jar else None)


@pytest.fixture(scope="session")
def taplint_run(pytestconfig, service, report_dir) -> validator.Run:
    """The validator, run once for the whole session."""
    if pytestconfig.getoption("--skip-taplint"):
        return validator.Run({}, "skipped", [], unavailable="asked not to run")
    jar = pytestconfig.getoption("--stilts-jar")
    return validator.run(
        service.base_url,
        pytestconfig.getoption("--stilts"),
        Path(jar) if jar else None,
        report_dir / "taplint.json",
    )


@pytest.fixture
def stage(request, taplint_run):
    """The findings of the taplint stage this test is marked with.

    A validator that could not run makes these skip rather than fail: whether STILTS
    is installed is not something the service under test has any say in.
    """
    marker = request.node.get_closest_marker("taplint")
    if marker is None:
        raise RuntimeError("the stage fixture needs a taplint marker")
    if taplint_run.unavailable:
        pytest.skip(f"the validator did not run: {taplint_run.unavailable}")
    return taplint_run.section(marker.args[0])


# --------------------------------------------------------------- what a run leaves

COLLECTED: dict[str, reporting.Result] = {}
AREA_OF: dict[str, str] = {}
ASKS_OF: dict[str, list[str]] = {}
DESCRIPTION_OF: dict[str, str] = {}

#: Why this run cannot be believed, if it cannot.
#:
#: A check that fails is the output of this suite and never a reason to go red — a
#: service is expected to be some way short of the whole of TAP. Two things are not
#: that, and both have to be loud: the service under test falling over, and the suite
#: itself failing to run. Either leaves a report that reads like a service missing
#: features when what happened is that nobody asked it anything.
BROKEN: list[str] = []


def asked_by(item) -> list[str]:
    """Which of the three questions a check answers.

    Derived from how it asks rather than declared on each of ninety tests, because how
    it asks *is* the difference:

    - a check that reads the returned bytes is about the **standard**, and nothing
      else: the document either carries what the specification asks for or it does not,
      whatever any client makes of it;
    - a check whose answer came through `pyvo`'s parser, or through a STILTS validator
      stage, also says whether the **clients** work against this service — which is a
      different question, and the one a user has. A service can carry every INFO the
      standard asks for and still hand astropy a byte it will not decode;
    - a check that compares an answer against a reference service's is about neither.
      It is about whether the **answers** are right, which no amount of well-formed
      XML establishes.

    `@pytest.mark.asks(...)` overrides it where that reading is wrong.
    """
    marker = item.get_closest_marker("asks")
    if marker:
        return list(marker.args)
    if item.module.__name__ == "test_reference_data":
        return ["answers", "clients"]
    fixtures = set(item.fixturenames)
    asked = ["standard"]
    if fixtures & {"tap", "published", "queryable", "stage"}:
        asked.append("clients")
    return asked


def pytest_collection_modifyitems(items):
    for item in items:
        marker = item.get_closest_marker("area")
        AREA_OF[item.nodeid] = (
            marker.args[0] if marker else AREAS.get(item.module.__name__, "other")
        )
        ASKS_OF[item.nodeid] = asked_by(item)
        DESCRIPTION_OF[item.nodeid] = (item.function.__doc__ or "").strip().split("\n")[0]


def outcome_of(entry) -> str | None:
    """pytest's own vocabulary, which is already the one a conformance run needs."""
    if hasattr(entry, "wasxfail"):
        return "xpass" if entry.outcome == "passed" else "xfail"
    if entry.when == "call":
        return {"passed": "pass", "failed": "fail", "skipped": "skip"}[entry.outcome]
    if entry.outcome == "failed":
        return "fail"
    if entry.when == "setup" and entry.outcome == "skipped":
        return "skip"
    return None


#: Exceptions that are this suite being wrong rather than a service being wrong.
#:
#: A check reports what it found by failing an assertion, and it is allowed to fail by
#: letting a client's own exception through — a `DALQueryError` is how pyvo says the
#: service refused something, which is a finding. None of these is: a name that does not
#: exist, a type that does not fit, a module that is not installed. They are bugs in the
#: suite, they say nothing about the service, and a run carrying one is not a score.
OUR_MISTAKES = (
    "NameError",
    "UnboundLocalError",
    "AttributeError",
    "TypeError",
    "IndexError",
    "ImportError",
    "ModuleNotFoundError",
    "SyntaxError",
    "IndentationError",
    "RecursionError",
    "NotImplementedError",
)


def wrote_this(entry) -> bool:
    """Whether a failure is the suite's own fault rather than a finding."""
    crash = getattr(entry.longrepr, "reprcrash", None)
    message = getattr(crash, "message", "") or ""
    return message.split(":", 1)[0].strip() in OUR_MISTAKES


def detail_of(entry) -> str:
    """What the check found, which for a failure is the whole of why it failed."""
    recorded = [value for name, value in entry.user_properties if name == "detail"]
    if recorded:
        return str(recorded[-1])
    if getattr(entry, "wasxfail", ""):
        return str(entry.wasxfail)
    if entry.outcome == "skipped" and isinstance(entry.longrepr, tuple):
        return entry.longrepr[2]
    if entry.longrepr is None:
        return ""
    text = entry.longreprtext
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    # The assertion is what says what happened; the frames above it are how the suite
    # is written, which is not a finding about the service.
    interesting = [line for line in lines if line.startswith(("E ", "AssertionError"))]
    return " ".join(interesting or lines[-2:])[:600]


def pytest_runtest_logreport(report):
    outcome = outcome_of(report)
    if outcome is None:
        return
    # An exception in a fixture rather than in a check. The suite could not put its
    # question, which says nothing about the service's conformance and everything about
    # something being broken — so it is kept apart from the failures and is what makes
    # the run exit non-zero.
    if report.when in ("setup", "teardown") and report.outcome == "failed":
        # By cause rather than by test: one fixture that raises errors every check that
        # wanted it, and ninety lines saying so is one fact written ninety times.
        reason = f"a fixture raised in {report.when}: {detail_of(report)}"[:400]
        if reason not in BROKEN:
            BROKEN.append(reason)
    if report.when == "call" and report.outcome == "failed" and wrote_this(report):
        reason = f"{short(report.nodeid)} raised: {detail_of(report)}"[:400]
        if reason not in BROKEN:
            BROKEN.append(reason)
    existing = COLLECTED.get(report.nodeid)
    # A test that failed in its call and again in teardown keeps the first word on it.
    if existing and existing.outcome == "fail":
        return
    COLLECTED[report.nodeid] = reporting.Result(
        id=short(report.nodeid),
        area=AREA_OF.get(report.nodeid, "other"),
        asks=ASKS_OF.get(report.nodeid, ["standard"]),
        outcome=outcome,
        description=DESCRIPTION_OF.get(report.nodeid, ""),
        detail=detail_of(report),
    )


def short(nodeid: str) -> str:
    """`tests/test_maxrec.py::test_zero` as `maxrec/zero`."""
    path, _, name = nodeid.partition("::")
    file = Path(path).stem.removeprefix("test_")
    return f"{file}/{name.removeprefix('test_')}"


#: pytest's own statuses for a session that did not get to ask its questions:
#: interrupted, an internal error, and a command line it could not use.
COULD_NOT_RUN = {2, 3, 4}


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if not COLLECTED:
        # Nothing was recorded at all, which is never a conformance result: either the
        # suite could not start or it was interrupted. Whatever pytest made of that
        # stands.
        return
    tools = {}
    try:
        import astropy
        import pyvo

        tools["pyvo"] = pyvo.__version__
        tools["astropy"] = astropy.__version__
    except ImportError:  # pragma: no cover — the environment is the suite's own
        pass
    tools["python"] = sys.version.split()[0]

    run = getattr(config, "_taplint_run", None)
    if run is not None:
        tools["stilts"] = run.version

    built = reporting.Report(
        target=getattr(config, "_target", "?"),
        tools=tools,
        provenance=getattr(config, "_provenance", ""),
        results=[COLLECTED[key] for key in sorted(COLLECTED)],
        broken=list(BROKEN),
    )
    written = reporting.write(built, Path(config.getoption("--report-dir")))
    config._report_summary = (built.summary(), written)
    config._broken = list(BROKEN)

    # A failing check is this suite's output, so the run is green however many of them
    # there are — nobody expects the whole of TAP, and a red mark that is always there
    # is one everybody learns to scroll past. What must go red is a run whose report
    # cannot be believed: the service fell over, a fixture raised, or pytest never got
    # to the questions at all. Those look identical to "implements nothing" in the
    # report, and telling them apart is the whole reason this is not just `|| true` in
    # the workflow.
    if BROKEN or exitstatus in COULD_NOT_RUN:
        session.exitstatus = 1
    elif not config.getoption("--strict-conformance"):
        session.exitstatus = 0


@pytest.fixture(scope="session", autouse=True)
def record_run(pytestconfig, service, manifest, taplint_run):
    """What the report says about what was run, gathered where the fixtures are."""
    pytestconfig._target = service.base_url
    pytestconfig._taplint_run = taplint_run
    if manifest:
        pytestconfig._provenance = (
            f"reference answers from {manifest.get('reference_service_name', '?')}, "
            f"fetched {manifest.get('fetched_utc', '?')}"
        )
    for identifier, note in service.notes:
        COLLECTED[identifier] = reporting.Result(
            id=identifier,
            area="the service under test",
            asks=["standard"],
            outcome="pass" if "published" in note else "fail",
            description="the service starts with the tables the suite publishes",
            detail=note,
        )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    summary = getattr(config, "_report_summary", None)
    if summary:
        terminalreporter.write_sep("=", "TAP conformance")
        terminalreporter.write_line(summary[0])
        terminalreporter.write_line(f"report: {summary[1]}")
    for broken in getattr(config, "_broken", []):
        terminalreporter.write_line(f"NOT A CONFORMANCE RESULT: {broken}", red=True)
