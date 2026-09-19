# lsdb-conformance

[LSDB](https://lsdb.readthedocs.io/) reading a public catalog through this service, against
LSDB reading the same bucket directly. The catalog is the control: a difference between the
two answers is this service answering differently from S3, and a failure on both is a
finding about the catalog or about LSDB.

```
uv run pytest -c pyproject.toml
```

It starts its own service from `../target/debug/hats-api` — `--server-binary` points
somewhere else, `--base-url` at one already running.

[`catalogs.py`](src/lsdb_conformance/catalogs.py) is every `s3://` catalog
[data.lsdb.io](https://data.lsdb.io) publishes. All are mounted and few are read: the
service opens one store per mount at startup and reads no catalog, so which one a check
touches is the check's own choice, and that is where the time goes.

## Writing a check

Take the `both` fixture, which asks one catalog one question down both routes:

```python
def test_something(both):
    assert_same(*both("sdss_dr7_spectra", lambda c: c.head(5), columns=["RA", "DEC"]))
```

Nothing is compared against a number written down here — these catalogs gain rows and get
rebuilt. What stays true is that two readers of one bucket must agree, so `assert_same` is
the whole assertion: same values, types, index and row order, compared through pyarrow
because these columns hold lists.

**Name the columns.** Opened without `columns`, LSDB asks for every column of a partition
as a query on the file's own url, which re-encodes the whole partition rather than reading
a few column chunks — 807 MB and 289 s against SDSS DR7 spectra, where the projection is
six. A user reading three columns writes three columns, and so does a check.

Everything is read anonymously, both ways, so a bucket that stopped being public fails
both routes rather than quietly using a runner's credentials.
