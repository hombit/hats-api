"""TAP_SCHEMA — TAP 1.1 section 4.

The same metadata again, as tables a client can query. It is a second copy of what
/tables says, which is why the standard is specific about it: the two have to agree,
and a client that builds a query out of TAP_SCHEMA has to get names it can write.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean


def rows(tap, table: str, columns: str = "*"):
    return tap.run_sync(f"SELECT {columns} FROM TAP_SCHEMA.{table}").to_table()


def test_the_five_tables_answer(tap, record_property):
    """All five tables of TAP 4 can be queried, an empty one being a legal answer.

    The content of each is `taplint`'s TMS stage, which reads them against the
    standard column by column and is a better judge of them than anything here. What
    this asks is the thing that stage cannot: whether a Python client gets an answer
    at all, five queries being how `pyvo` and every notebook find out what exists.
    """
    counted = {}
    for table in ("schemas", "tables", "columns", "keys", "key_columns"):
        counted[table] = len(rows(tap, table))
    record_property("detail", ", ".join(f"{name}={count}" for name, count in counted.items()))
    assert counted["tables"] > 0, "TAP_SCHEMA.tables is empty"
    assert counted["columns"] > 0, "TAP_SCHEMA.columns is empty"


def test_published_names_are_queryable(tap, queryable, record_property):
    """A name TAP_SCHEMA publishes is a name a query can be built out of.

    TAP 4.3 says the published `column_name` is "the string that is recommended for
    use in querying", so whatever is in that column has to work in a SELECT as it
    stands. This is the check a client's own table browser amounts to.
    """
    found = tap.run_sync(
        f"SELECT TOP 8 column_name FROM TAP_SCHEMA.columns WHERE table_name = '{queryable}'"
    ).to_table()
    names = [str(value) for value in found["column_name"]]
    assert names, f"TAP_SCHEMA.columns has no row for {queryable}"
    selected = ", ".join(names)
    record_property("detail", f"selected {selected}")
    answer = tap.run_sync(f"SELECT TOP 1 {selected} FROM {queryable}").to_table()
    assert len(answer.colnames) == len(names), (
        f"asked for {len(names)} published names, got {answer.colnames}"
    )


def test_bootstrap_name_is_case_insensitive(tap, record_property):
    """TAP_SCHEMA's own name resolves however a client spells it.

    A client cannot have read this name off an answer it has not received yet, so the
    fixed names of TAP 4 are the one thing it hardcodes — and which case it hardcodes
    them in is the client's business.
    """
    found = tap.run_sync("SELECT table_name FROM tap_schema.tables").to_table()
    record_property("detail", f"the lowercase spelling answered with {len(found)} rows")


@pytest.mark.taplint("TMS")
def test_content(stage, record_property):
    """The content of TAP_SCHEMA is what TAP 4 asks for."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("TMC")
def test_consistent_with_tables_resource(stage, record_property):
    """TAP_SCHEMA and /tables describe the same tables the same way."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
