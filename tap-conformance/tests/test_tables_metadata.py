"""VOSI tables — VOSI 1.1 section 2.

The table metadata resource: what tables there are, what columns they have, and what
each column is. It is the document TOPCAT fills its table browser from, so a client
that cannot read it cannot show a user anything to query.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean


def test_tables_listed(tap, published, record_property):
    """/tables lists the published tables."""
    record_property("detail", f"{len(published)} tables: {', '.join(sorted(published)[:8])}")
    assert published, "no table is published"


def test_columns_are_readable_by_the_client(tap, published, record_property):
    """pyvo can read the column list of every table, and each column has a datatype.

    Whether the document is *correct* is `taplint`'s TME stage, which checks it
    against the schema and the standard. What this asks is whether the other client
    can read it at all — a table whose columns pyvo cannot parse is a table a Python
    user cannot see, however well formed the XML.
    """
    thin, untyped, counted = [], [], 0
    for name, table in tap.tables.items():
        columns = list(table.columns)
        counted += len(columns)
        if not columns:
            thin.append(name)
            continue
        untyped += [f"{name}.{column.name}" for column in columns if not column.datatype]
    record_property("detail", f"{counted} columns over {len(published)} tables")
    assert not thin, f"tables with no columns: {', '.join(thin[:5])}"
    assert not untyped, f"columns with no datatype: {', '.join(untyped[:5])}"


def test_positional_ucds(tap, manifest, published, record_property):
    """The coordinate columns carry the UCDs a client finds a position by.

    Which UCDs are *legal* is `taplint`'s UUC stage, which parses every one of them
    against the vocabulary. What that stage cannot know is which columns of this
    catalog are a position — so this is the half that needs the fetched data: it
    checks that `pos.eq.ra` and `pos.eq.dec` are on the columns that actually hold
    one, which is how a client that was told nothing works it out.
    """
    if not manifest:
        pytest.skip("no fetched data, so nothing knows which column is which")
    found = []
    for table in manifest["tables"]:
        if table["name"] not in published:
            continue
        columns = {column.name: column for column in tap.tables[table["name"]].columns}
        for role in ("ra_column", "dec_column"):
            column = columns.get(table[role].strip('"'))
            assert column is not None, f"{table['name']} declares no {table[role]} column"
            found.append(f"{table[role]}: {column.ucd or 'no ucd'} {column.unit or ''}".strip())
    if not found:
        pytest.skip("none of the suite's tables is published")
    record_property("detail", "; ".join(found))
    assert any("pos.eq" in entry for entry in found), (
        f"no positional UCD on any coordinate column: {found}"
    )


def test_nested_columns_declared(tap, manifest, published, record_property):
    """A table holding a nested column declares it by names a query can write.

    VOTable has no nesting, so whatever such a column is called in the metadata is
    what a client will put in a SELECT and expect an answer for. A table that declares
    nothing for it looks narrower than it is, and `SELECT *` then fails over columns
    the client was never told about.
    """
    nested = [table for table in (manifest or {}).get("tables", []) if table.get("nested")]
    if not nested:
        pytest.skip("no table here is known to hold a nested column")
    present = [table for table in nested if table["name"] in published]
    if not present:
        pytest.skip(f"{nested[0]['name']} is not published here")

    name = present[0]["name"]
    columns = list(tap.tables[name].columns)
    dotted = [column for column in columns if "." in column.name]
    record_property("detail", f"{name}: {len(dotted)} of {len(columns)} columns are nested")
    assert dotted, f"{name} declares {len(columns)} columns and none of them is nested"
    untyped = [column.name for column in dotted if not column.datatype]
    assert not untyped, f"nested columns with no datatype: {untyped[:5]}"


@pytest.mark.taplint("TMV")
def test_schema(stage, record_property):
    """The table metadata validates against its XML schema."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("TME")
def test_content(stage, record_property):
    """Its content is what VOSI asks for."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("UUC")
def test_units_and_ucds(stage, record_property):
    """The units and UCDs on the columns are ones the standards define."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
