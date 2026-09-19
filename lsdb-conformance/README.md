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

**Name the columns, and name `_healpix_29` among them.** Two separate reasons, and both
bite.

A read with no projection is answered by encoding the whole partition, and — since a range
is cut from the body that request generated — once per block the client asks for. Against
SDSS DR7 spectra, whose `spectra` column is a nested array per row, that is fifteen minutes
and then a client-side timeout, against two seconds off S3.

`_healpix_29` has to be named because LSDB builds the url it asks this service for *before*
it adds the index column to what it asks `pyarrow` for. Leave it out and the service is
asked for the other columns, honours exactly that, and `pyarrow` — holding the catalog's
schema — fills the column it did not get with nulls, which LSDB then makes an index of. The
frame that comes back has every value right and no position on the sky.

**Two catalogs are not read through this service at all.** Euclid Q1 and the ZTF catalogs
are written with `hats_npix_suffix = "/"`, so a partition is a directory; LSDB lists it at a
url carrying the query string, and `fsspec` keeps only links that start with that whole
string, query included. No href can match, so the listing is empty whatever is served —
there is no answer this service could give. `catalogs.py` names them and the `both` fixture
refuses them.

Everything is read anonymously, both ways, so a bucket that stopped being public fails
both routes rather than quietly using a runner's credentials.
