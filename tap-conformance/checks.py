#!/usr/bin/env python3
"""What a `pyvo` client can and cannot do against a TAP service.

Every check here goes through `pyvo`, because the question this suite asks is
whether the clients astronomers already have work — not whether a request this
repository composed itself gets a reply it recognises. Where a check needs the
bytes rather than the parsed answer it still asks `pyvo` to build and send the
request, and reads the response off it.

The results go out as JSON, one object per check, for the runner to fold into its
report. Nothing here decides whether a failure matters; it records what happened.

    uv run --project tap-conformance/python tap-conformance/python/checks.py \\
        --base-url http://127.0.0.1:8080/api/v1/tap \\
        --data tap-conformance/data --out checks.json
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
import warnings
from pathlib import Path

import numpy as np
import pyvo
from astropy.table import Table

# How long any one request may take before the check is abandoned. A service that
# has to be waited minutes for is a finding of its own, and without this a single
# hanging request stops the whole suite.
TIMEOUT = 120

CHECKS: list = []


def check(identifier: str, area: str, *, divergence: bool = False):
    """Register one check.

    `divergence` marks a check of something the service under test is known not to
    offer. Such a check reports `xfail` when it fails, which is the documented
    behaviour, and `fail` when it succeeds — because a service that answers here
    has stopped diverging and the report should say so rather than quietly pass.
    """

    def register(function):
        CHECKS.append(
            {
                "id": identifier,
                "area": area,
                "divergence": divergence,
                "run": function,
                "description": (function.__doc__ or "").strip().split("\n")[0],
            }
        )
        return function

    return register


class Skip(Exception):
    """The check had nothing to run against, which is not a failure of the service."""


class Context:
    """What every check is handed: the service, and the data the suite fetched."""

    def __init__(self, base_url: str, data: Path) -> None:
        self.base_url = base_url.rstrip("/")
        self.data = data
        self.service = pyvo.dal.TAPService(self.base_url)
        self.service._session.timeout = TIMEOUT
        manifest_path = data / "MANIFEST.json"
        self.manifest = (
            json.loads(manifest_path.read_text()) if manifest_path.exists() else None
        )
        queries_path = data / "queries.json"
        self.queries = (
            json.loads(queries_path.read_text()) if queries_path.exists() else []
        )
        self._published = None

    @property
    def published(self) -> list[str]:
        """The table names the service says it has, VOSI being how a client asks."""
        if self._published is None:
            self._published = [name for name in self.service.tables.keys()]
        return self._published

    def any_table(self) -> str:
        """A table to query. The suite's own if the service has it, else any."""
        names = self.published
        if self.manifest:
            for table in self.manifest["tables"]:
                if table["name"] in names:
                    return table["name"]
        for name in names:
            if not name.upper().startswith("TAP_SCHEMA"):
                return name
        raise Skip("the service publishes no table to query")

    def coordinates(self, table_name: str) -> tuple[str, str]:
        """Which columns hold a position, for a query that has to name them."""
        if self.manifest:
            for table in self.manifest["tables"]:
                if table["name"] == table_name:
                    return table["ra_column"], table["dec_column"]
        raise Skip(f"nothing here knows which columns of {table_name} are a position")

    def rows_query(self, count: int = 10) -> str:
        return f"SELECT TOP {count} * FROM {self.any_table()}"

    def raw(self, query: str, **parameters):
        """The response to a query pyvo built, unparsed.

        For the checks that are about the bytes — a media type, a status, a format
        this client cannot read — rather than about the rows.
        """
        request = self.service.create_query(query, **parameters)
        return request.submit()


# ---------------------------------------------------------------- VOSI resources


@check("vosi-availability", "VOSI availability")
def availability(context: Context) -> str:
    """/availability answers, and says the service is up."""
    if not context.service.available:
        raise AssertionError("the service reports itself unavailable")
    return "available=true"


@check("vosi-capabilities", "VOSI capabilities")
def capabilities(context: Context) -> str:
    """/capabilities declares the TAP standard id."""
    ids = [str(capability.standardid) for capability in context.service.capabilities]
    if "ivo://ivoa.net/std/TAP" not in ids:
        raise AssertionError(f"no TAP capability among {ids}")
    return ", ".join(ids)


@check("capabilities-tapregext", "VOSI capabilities")
def tapregext(context: Context) -> str:
    """The TAP capability carries the TAPRegExt detail clients read."""
    languages = [
        language.name
        for capability in context.service.capabilities
        for language in getattr(capability, "languages", [])
    ]
    formats = [
        str(output.mime)
        for capability in context.service.capabilities
        for output in getattr(capability, "outputformats", [])
    ]
    if not languages:
        raise AssertionError("the TAP capability declares no query language")
    if "ADQL" not in " ".join(languages).upper():
        raise AssertionError(f"ADQL is not among the declared languages: {languages}")
    if not formats:
        raise AssertionError("the TAP capability declares no output format")
    return f"languages={languages} formats={formats}"


@check("capabilities-limits", "VOSI capabilities")
def limits(context: Context) -> str:
    """The capability says what its row limits are, which is what MAXREC is read against."""
    default, hard = context.service.maxrec, context.service.hardlimit
    if default is None and hard is None:
        raise AssertionError("neither a default nor a hard output limit is declared")
    return f"default={default} hard={hard}"


@check("vosi-tables", "VOSI tables")
def tables(context: Context) -> str:
    """/tables lists the published tables."""
    names = context.published
    if not names:
        raise AssertionError("no table is published")
    return f"{len(names)} tables: {', '.join(sorted(names)[:8])}"


@check("vosi-tables-columns", "VOSI tables")
def table_columns(context: Context) -> str:
    """Every published table declares columns, with a datatype on each."""
    thin = []
    untyped = []
    for name, table in context.service.tables.items():
        columns = list(table.columns)
        if not columns:
            thin.append(name)
            continue
        untyped += [
            f"{name}.{column.name}" for column in columns if not column.datatype
        ]
    if thin:
        raise AssertionError(f"tables with no columns: {', '.join(thin[:5])}")
    if untyped:
        raise AssertionError(f"columns with no datatype: {', '.join(untyped[:5])}")
    return "every table has typed columns"


@check("vosi-tables-units-ucds", "VOSI tables")
def units_and_ucds(context: Context) -> str:
    """Positional columns carry the UCDs a client finds a position by."""
    if not context.manifest:
        raise Skip("no fetched data, so nothing knows which column is which")
    found = []
    for table in context.manifest["tables"]:
        if table["name"] not in context.published:
            continue
        columns = {
            column.name: column for column in context.service.tables[table["name"]].columns
        }
        for role, key in (("ra", "ra_column"), ("dec", "dec_column")):
            column = columns.get(table[key].strip('"'))
            if column is None:
                raise AssertionError(f"{table['name']} declares no {table[key]} column")
            found.append(f"{role}={column.ucd or 'no ucd'}/{column.unit or 'no unit'}")
    if not found:
        raise Skip("none of the suite's tables is published")
    if all("no ucd" in entry for entry in found):
        raise AssertionError(f"no positional UCD on any coordinate column: {found}")
    return ", ".join(found)


@check("vosi-tables-nested-columns", "VOSI tables")
def nested_columns(context: Context) -> str:
    """A table holding a nested column declares it by names a query can write.

    VOTable has no nesting, so whatever such a column is called in the metadata is
    what a client will put in a SELECT and expect an answer for. A table that
    declares nothing for it looks narrower than it is; one that declares a name no
    query can use is worse.
    """
    if not context.manifest:
        raise Skip("no fetched data, so nothing knows which table is nested")
    nested = [table for table in context.manifest["tables"] if table.get("nested")]
    if not nested:
        raise Skip("no table here is known to hold a nested column")
    published = [table for table in nested if table["name"] in context.published]
    if not published:
        raise Skip(f"{nested[0]['name']} is not published here")
    name = published[0]["name"]
    columns = list(context.service.tables[name].columns)
    dotted = [column for column in columns if "." in column.name]
    if not dotted:
        raise AssertionError(
            f"{name} declares {len(columns)} columns and none of them is a nested one"
        )
    untyped = [column.name for column in dotted if not column.datatype]
    if untyped:
        raise AssertionError(f"nested columns with no datatype: {untyped[:5]}")
    return f"{len(dotted)} nested columns, e.g. {dotted[0].name}"


# ------------------------------------------------------------------- TAP_SCHEMA


def tap_schema_rows(context: Context, table: str, columns: str = "*") -> Table:
    return context.service.run_sync(f"SELECT {columns} FROM TAP_SCHEMA.{table}").to_table()


@check("tap-schema-schemas", "TAP_SCHEMA")
def schema_schemas(context: Context) -> str:
    """TAP_SCHEMA.schemas is queryable and names at least TAP_SCHEMA itself."""
    rows = tap_schema_rows(context, "schemas", "schema_name")
    names = [str(name) for name in rows["schema_name"]]
    if not names:
        raise AssertionError("TAP_SCHEMA.schemas is empty")
    return f"{len(names)} schemas: {', '.join(names[:8])}"


@check("tap-schema-tables", "TAP_SCHEMA")
def schema_tables(context: Context) -> str:
    """TAP_SCHEMA.tables is queryable and describes itself."""
    rows = tap_schema_rows(context, "tables", "schema_name, table_name")
    names = {str(name).upper() for name in rows["table_name"]}
    missing = {"TAP_SCHEMA.TABLES", "TAP_SCHEMA.COLUMNS"} - names
    if missing:
        raise AssertionError(f"TAP_SCHEMA does not describe itself: {missing} absent")
    return f"{len(names)} tables"


@check("tap-schema-columns", "TAP_SCHEMA")
def schema_columns(context: Context) -> str:
    """TAP_SCHEMA.columns carries the not-null fields TAP 4.3 requires."""
    name = context.any_table()
    rows = context.service.run_sync(
        "SELECT column_name, datatype, indexed, principal, std "
        f"FROM TAP_SCHEMA.columns WHERE table_name = '{name}'"
    ).to_table()
    if len(rows) == 0:
        raise AssertionError(f"TAP_SCHEMA.columns has no row for {name}")
    for field in ("datatype", "indexed", "principal", "std"):
        if field not in rows.colnames:
            raise AssertionError(f"TAP_SCHEMA.columns has no {field} column")
        if hasattr(rows[field], "mask") and rows[field].mask.any():
            raise AssertionError(f"{field} is null for some column of {name}")
    return f"{len(rows)} columns of {name}"


@check("tap-schema-keys", "TAP_SCHEMA")
def schema_keys(context: Context) -> str:
    """TAP_SCHEMA.keys and key_columns exist, empty being a legal answer."""
    keys = tap_schema_rows(context, "keys")
    key_columns = tap_schema_rows(context, "key_columns")
    return f"keys={len(keys)} key_columns={len(key_columns)}"


@check("tap-schema-agrees-with-vosi", "TAP_SCHEMA")
def schema_agrees(context: Context) -> str:
    """The columns TAP_SCHEMA publishes are the ones /tables does."""
    name = context.any_table()
    rows = context.service.run_sync(
        f"SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name = '{name}'"
    ).to_table()
    from_schema = {str(value) for value in rows["column_name"]}
    from_vosi = {column.name for column in context.service.tables[name].columns}
    if from_schema != from_vosi:
        only_schema = sorted(from_schema - from_vosi)[:5]
        only_vosi = sorted(from_vosi - from_schema)[:5]
        raise AssertionError(
            f"TAP_SCHEMA only: {only_schema}; /tables only: {only_vosi}"
        )
    return f"{len(from_schema)} columns agree"


@check("tap-schema-case-insensitive", "TAP_SCHEMA")
def schema_case(context: Context) -> str:
    """TAP_SCHEMA's own name resolves however a client spells it.

    A client cannot have read this name off an answer it has not received yet, so
    the five fixed names of TAP 4 are the one thing it hardcodes.
    """
    rows = context.service.run_sync("SELECT table_name FROM tap_schema.tables").to_table()
    return f"lowercase spelling answered with {len(rows)} rows"


# ----------------------------------------------------------------------- queries


@check("sync-select-top", "sync query")
def select_top(context: Context) -> str:
    """A sync query returns the rows TOP asked for."""
    rows = context.service.run_sync(context.rows_query(5)).to_table()
    if len(rows) != 5:
        raise AssertionError(f"TOP 5 returned {len(rows)} rows")
    return f"5 rows, {len(rows.colnames)} columns"


@check("select-list-is-the-answer", "sync query")
def select_list(context: Context) -> str:
    """The columns come back in the number, order and name the SELECT wrote."""
    name = context.any_table()
    ra, dec = context.coordinates(name)
    rows = context.service.run_sync(
        f"SELECT TOP 3 {dec}, {ra}, {ra} + 0.0 AS shifted FROM {name}"
    ).to_table()
    expected = [dec.strip('"'), ra.strip('"'), "shifted"]
    if [name.lower() for name in rows.colnames] != [
        name.lower() for name in expected
    ]:
        raise AssertionError(f"expected {expected}, got {rows.colnames}")
    return ", ".join(rows.colnames)


@check("adql-geometry", "ADQL")
def geometry(context: Context) -> str:
    """A cone written as ADQL's CONTAINS is answered."""
    name = context.any_table()
    ra, dec = context.coordinates(name)
    center = context.manifest["center"] if context.manifest else {"ra": 45.0, "dec": 0.0}
    rows = context.service.run_sync(
        f"SELECT COUNT(*) AS n FROM {name} WHERE 1=CONTAINS("
        f"POINT('ICRS', {ra}, {dec}), "
        f"CIRCLE('ICRS', {center['ra']}, {center['dec']}, 0.2))"
    ).to_table()
    count = int(rows["n"][0])
    if count == 0:
        raise AssertionError("the cone matched no rows, where the data says it should")
    return f"{count} rows in the cone"


@check("maxrec-zero", "MAXREC")
def maxrec_zero(context: Context) -> str:
    """MAXREC=0 returns the columns and no rows, which is how a client inspects a table."""
    result = context.service.run_sync(context.rows_query(10), maxrec=0)
    rows = result.to_table()
    if len(rows) != 0:
        raise AssertionError(f"MAXREC=0 returned {len(rows)} rows")
    if not rows.colnames:
        raise AssertionError("MAXREC=0 returned no columns either")
    return f"0 rows, {len(rows.colnames)} columns"


@check("maxrec-overrides-top", "MAXREC")
def maxrec_over_top(context: Context) -> str:
    """MAXREC wins over TOP, TAP 2.7.4 saying it is the smaller that applies."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pyvo.dal.DALOverflowWarning)
        rows = context.service.run_sync(
            f"SELECT TOP 20 * FROM {context.any_table()}", maxrec=3
        ).to_table()
    if len(rows) != 3:
        raise AssertionError(f"TOP 20 with MAXREC=3 returned {len(rows)} rows")
    return "3 rows"


@check("maxrec-overflow-marked", "MAXREC")
def overflow(context: Context) -> str:
    """A truncated answer says so, with an OVERFLOW marker after the table."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rows = context.service.run_sync(
            f"SELECT TOP 20 * FROM {context.any_table()}", maxrec=2
        ).to_table()
    if len(rows) != 2:
        raise AssertionError(f"MAXREC=2 returned {len(rows)} rows")
    overflows = [
        entry for entry in caught if issubclass(entry.category, pyvo.dal.DALOverflowWarning)
    ]
    if not overflows:
        raise AssertionError(
            "the answer was truncated and carried no OVERFLOW marker, "
            "so a client cannot tell it from a complete one"
        )
    return str(overflows[0].message)


@check("maxrec-no-false-overflow", "MAXREC")
def no_false_overflow(context: Context) -> str:
    """An answer that fits is not marked as truncated."""
    name = context.any_table()
    total = int(
        context.service.run_sync(f"SELECT COUNT(*) AS n FROM {name}").to_table()["n"][0]
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        context.service.run_sync(f"SELECT TOP 2 * FROM {name}", maxrec=min(total, 1000))
    overflows = [
        entry for entry in caught if issubclass(entry.category, pyvo.dal.DALOverflowWarning)
    ]
    if overflows:
        raise AssertionError("a complete answer was marked OVERFLOW")
    return "no marker on a complete answer"


@check("lang-adql-required", "parameters")
def lang_adql(context: Context) -> str:
    """LANG=ADQL is accepted, being the one language TAP makes mandatory."""
    rows = context.service.run_sync(context.rows_query(1), language="ADQL").to_table()
    return f"{len(rows)} rows"


@check("lang-unknown-refused", "parameters")
def lang_unknown(context: Context) -> str:
    """A language the service does not implement is refused rather than guessed at."""
    response = context.raw(context.rows_query(1), language="PQL")
    if response.status_code < 400 and b"ERROR" not in response.content[:4000]:
        raise AssertionError(
            f"LANG=PQL was answered with {response.status_code} and no error marker"
        )
    return f"refused with {response.status_code}"


@check("request-doquery", "parameters")
def request_doquery(context: Context) -> str:
    """REQUEST=doQuery is accepted, a 1.0-era client still sending it."""
    response = context.raw(context.rows_query(1), REQUEST="doQuery")
    if response.status_code >= 400:
        raise AssertionError(f"REQUEST=doQuery was refused with {response.status_code}")
    return f"{response.status_code}"


# ----------------------------------------------------------------------- formats


def response_format(context: Context, value: str, parameter: str = "RESPONSEFORMAT"):
    return context.raw(context.rows_query(3), **{parameter: value})


@check("format-votable", "output formats")
def format_votable(context: Context) -> str:
    """VOTable is what a service answers with when nothing else is asked for."""
    response = response_format(context, "votable")
    if response.status_code != 200:
        raise AssertionError(f"status {response.status_code}")
    if b"<VOTABLE" not in response.content[:4000].upper():
        raise AssertionError(f"not a VOTable: {response.content[:200]!r}")
    return response.headers.get("content-type", "no content-type")


@check("format-csv", "output formats")
def format_csv(context: Context) -> str:
    """text/csv, a SHOULD of TAP 2.7.3."""
    response = response_format(context, "csv")
    if response.status_code != 200:
        raise AssertionError(f"status {response.status_code}")
    media = response.headers.get("content-type", "")
    if "csv" not in media:
        raise AssertionError(f"content-type {media!r}")
    return media


@check("format-tsv", "output formats")
def format_tsv(context: Context) -> str:
    """text/tab-separated-values, the other SHOULD of TAP 2.7.3."""
    response = response_format(context, "tsv")
    if response.status_code != 200:
        raise AssertionError(f"status {response.status_code}")
    media = response.headers.get("content-type", "")
    if "tab-separated" not in media and "tsv" not in media:
        raise AssertionError(f"content-type {media!r}")
    return media


@check("format-parameter-alias", "output formats")
def format_alias(context: Context) -> str:
    """FORMAT is accepted as the equivalent of RESPONSEFORMAT (TAP 2.7.3)."""
    response = response_format(context, "votable", parameter="FORMAT")
    if response.status_code != 200:
        raise AssertionError(f"status {response.status_code}")
    if b"<VOTABLE" not in response.content[:4000].upper():
        raise AssertionError("FORMAT=votable did not produce a VOTable")
    return "FORMAT=votable answered"


@check("format-unknown-refused", "output formats")
def format_unknown(context: Context) -> str:
    """A format the service does not have is refused, not silently replaced."""
    response = response_format(context, "application/x-nonsense")
    if response.status_code < 400:
        raise AssertionError(
            f"an unknown RESPONSEFORMAT was answered with {response.status_code}"
        )
    return f"refused with {response.status_code}"


# ------------------------------------------------------------------------ errors


def error_document(response) -> str:
    body = response.content[:8000]
    if b"<VOTABLE" not in body.upper():
        raise AssertionError(f"the error is not a VOTable: {body[:200]!r}")
    if b'value="ERROR"' not in body and b"value='ERROR'" not in body:
        raise AssertionError('no INFO with QUERY_STATUS="ERROR"')
    return f"{response.status_code}, VOTable with QUERY_STATUS=ERROR"


@check("error-bad-syntax", "errors")
def error_syntax(context: Context) -> str:
    """A statement that will not parse comes back as a VOTable error document."""
    response = context.raw("SELECT FROM WHERE")
    if response.status_code < 400:
        raise AssertionError(f"a malformed query was answered with {response.status_code}")
    return error_document(response)


@check("error-unknown-table", "errors")
def error_table(context: Context) -> str:
    """A table the service has not got is an error naming it."""
    response = context.raw("SELECT * FROM no_such_schema.no_such_table")
    if response.status_code < 400:
        raise AssertionError(f"answered with {response.status_code}")
    return error_document(response)


@check("error-unknown-column", "errors")
def error_column(context: Context) -> str:
    """A column the table has not got is an error rather than an empty answer."""
    response = context.raw(f"SELECT no_such_column FROM {context.any_table()}")
    if response.status_code < 400:
        raise AssertionError(f"answered with {response.status_code}")
    return error_document(response)


@check("error-raised-to-the-client", "errors")
def error_raised(context: Context) -> str:
    """pyvo turns the error document into an exception rather than an empty table."""
    try:
        context.service.run_sync("SELECT FROM WHERE")
    except pyvo.dal.DALQueryError as error:
        return f"DALQueryError: {str(error)[:160]}"
    except pyvo.dal.DALServiceError as error:
        return f"DALServiceError: {str(error)[:160]}"
    raise AssertionError("a malformed query raised nothing")


# ---------------------------------------------------------------------- examples


@check("examples-document", "examples")
def examples(context: Context) -> str:
    """The DALI examples endpoint answers, which is where TOPCAT's menu comes from."""
    found = context.service.examples
    if not found:
        raise AssertionError("the examples document is empty")
    return f"{len(found)} examples"


@check("examples-run", "examples")
def examples_run(context: Context) -> str:
    """Every published example is a query that runs."""
    found = context.service.examples
    if not found:
        raise Skip("no examples to run")
    broken = []
    for example in found:
        query = example.get("query")
        if not query:
            broken.append(f"{example.get('name', '?')}: no query")
            continue
        try:
            context.service.run_sync(query, maxrec=5)
        except Exception as error:  # noqa: BLE001 — any failure is the finding
            broken.append(f"{example.get('name', '?')}: {str(error)[:120]}")
    if broken:
        raise AssertionError("; ".join(broken[:4]))
    return f"{len(found)} examples run"


# --------------------------------------------------- what this service does not do


@check("async-job-submission", "async", divergence=True)
def async_absent(context: Context) -> str:
    """/async answers, which TAP 2.2 makes a MUST."""
    result = context.service.run_async(context.rows_query(2))
    return f"an async job returned {len(result.to_table())} rows"


@check("upload-inline", "uploads", divergence=True)
def upload_absent(context: Context) -> str:
    """A table uploaded with the query is queryable as TAP_UPLOAD."""
    uploaded = Table({"id": np.arange(3), "x": np.arange(3) * 1.5})
    rows = context.service.run_sync(
        "SELECT * FROM TAP_UPLOAD.t1", uploads={"t1": uploaded}
    ).to_table()
    return f"{len(rows)} rows came back from an upload"


# --------------------------------------------------------------- against real data


def compare(expected: Table, actual: Table, rtol: float) -> str:
    """Two answers to one query, from two services."""
    if len(expected) != len(actual):
        raise AssertionError(f"{len(expected)} rows expected, {len(actual)} returned")
    lowered_expected = [name.lower() for name in expected.colnames]
    lowered_actual = [name.lower() for name in actual.colnames]
    if lowered_expected != lowered_actual:
        raise AssertionError(
            f"columns {expected.colnames} expected, {actual.colnames} returned"
        )
    for expected_name, actual_name in zip(expected.colnames, actual.colnames):
        left, right = expected[expected_name], actual[actual_name]
        left_mask = np.asarray(getattr(left, "mask", np.zeros(len(left), bool)))
        right_mask = np.asarray(getattr(right, "mask", np.zeros(len(right), bool)))
        if not np.array_equal(left_mask, right_mask):
            raise AssertionError(
                f"{expected_name}: {int(left_mask.sum())} nulls expected, "
                f"{int(right_mask.sum())} returned"
            )
        present = ~left_mask
        left_values = np.asarray(left)[present]
        right_values = np.asarray(right)[present]
        if left_values.dtype.kind in "fc" and right_values.dtype.kind in "fc":
            close = np.isclose(
                left_values.astype(float),
                right_values.astype(float),
                rtol=rtol,
                atol=0.0,
                equal_nan=True,
            )
            if not close.all():
                first = int(np.argmax(~close))
                raise AssertionError(
                    f"{expected_name}: row {first} is {right_values[first]!r}, "
                    f"expected {left_values[first]!r}"
                )
        else:
            differing = left_values.astype(str) != right_values.astype(str)
            if differing.any():
                first = int(np.argmax(differing))
                raise AssertionError(
                    f"{expected_name}: row {first} is {right_values[first]!r}, "
                    f"expected {left_values[first]!r}"
                )
    return f"{len(actual)} rows, {len(actual.colnames)} columns match"


def register_reference_checks(context: Context) -> None:
    """One check per query the fetch step asked the reference service, per table.

    The same query goes to every table the fetch step said holds the same rows, which
    is what makes a disagreement readable: failing on the sample and on the whole
    catalog alike says something about this service, and failing on only one of them
    says something about that catalog.
    """
    for query in context.queries:
        for table_name in query["tables"]:

            def run(context: Context, query=query, table_name=table_name) -> str:
                if table_name not in context.published:
                    raise Skip(f"{table_name} is not published here")
                expected = Table.read(
                    context.data / query["reference"], format="votable"
                )
                actual = context.service.run_sync(
                    query["adql"].format(table=table_name)
                ).to_table()
                return compare(expected, actual, query.get("rtol", 0.0))

            run.__doc__ = query["description"]
            check(f"reference/{query['id']}@{table_name}", "against a reference service")(
                run
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="the TAP service to test")
    parser.add_argument("--data", type=Path, required=True, help="the fetched data")
    parser.add_argument("--out", type=Path, required=True, help="where the JSON goes")
    parser.add_argument(
        "--only", default=None, help="run only checks whose id contains this"
    )
    args = parser.parse_args()

    warnings.filterwarnings("ignore", category=UserWarning)
    context = Context(args.base_url, args.data)
    try:
        register_reference_checks(context)
    except Exception:  # noqa: BLE001 — a missing data directory is not a service fault
        pass

    results = []
    for entry in CHECKS:
        if args.only and args.only not in entry["id"]:
            continue
        record = {
            "id": entry["id"],
            "area": entry["area"],
            "description": entry["description"],
        }
        try:
            detail = entry["run"](context)
            record["status"] = "fail" if entry["divergence"] else "pass"
            record["detail"] = (
                f"this is offered after all: {detail}"
                if entry["divergence"]
                else detail
            )
        except Skip as skipped:
            record["status"] = "skip"
            record["detail"] = str(skipped)
        except Exception as error:  # noqa: BLE001 — every failure is a result
            record["status"] = "xfail" if entry["divergence"] else "fail"
            record["detail"] = f"{type(error).__name__}: {error}"[:600]
            record["traceback"] = traceback.format_exc()[-2000:]
        print(f"{record['status']:>5}  {record['id']}", flush=True)
        results.append(record)

    import astropy

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(
            {
                "tools": {
                    "pyvo": pyvo.__version__,
                    "astropy": astropy.__version__,
                    "python": sys.version.split()[0],
                },
                "checks": results,
            },
            indent=2,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
