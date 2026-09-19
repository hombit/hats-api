"""What LSDB gets out of a catalog, served by this service and read off S3 directly.

Every question below is LSDB's own work — the cone search, the crossmatch and the
predicate are all computed in the client. The only thing that differs between the two
runs is where the parquet bytes came from, so what these compare is the file server.
"""

from __future__ import annotations

import lsdb
import pytest

from lsdb_conformance.compare import assert_same

#: The row's position on the sky, and the index of every frame LSDB hands back.
#:
#: **Named in every projection here on purpose.** LSDB builds the url it asks this service
#: for from the columns given to `open_catalog`, and then adds the index column to what it
#: asks `pyarrow` for — after the url is built. Where a check does not name it, the service
#: is asked for the other columns, honours exactly that, and `pyarrow`, holding the
#: catalog's schema, fills the column it did not get with nulls; LSDB then makes an index
#: of them. Naming it puts it in the url, which is the client-side gap closed from this
#: side rather than papered over inside the service.
INDEX = "_healpix_29"

#: Five of Gaia DR3's 152 columns, from the front, the middle and the end.
GAIA_COLUMNS = ["source_id", "ra", "dec", "phot_bp_mean_mag", "libname_gspphot", INDEX]
#: SDSS DR7 spectra is the smallest published catalog, at 1.6M rows over 231 partitions,
#: which is what makes it the one to read several of. `OBJID` is a list of five ints per
#: row, which is the column shape a comparison written with `==` gets wrong.
SDSS_COLUMNS = ["OBJID", "RA", "DEC", "MAG", "SFD_EBV", INDEX]

#: The default clock is set for a read of a few columns; these read whole partitions.
slow = pytest.mark.timeout(1800)

# **Every check here names its columns**, and not only to keep the suite quick. A read
# with no projection at all is answered by encoding the whole partition — and, because a
# slice is cut from the body this request generated, once per block the client asks for.
# For SDSS DR7 spectra, whose `spectra` column is a nested array per row, that is fifteen
# minutes and then a client-side timeout, against two seconds off S3. A projected read
# does not have that shape: the bodies are small and the re-encoding is cheap. What would
# close the gap is holding a generated answer between the requests that slice it, which is
# a decision about caching that nothing here makes.


def test_head(both):
    def read(open_catalog):
        return open_catalog("sdss_dr7_spectra", columns=["OBJID", "RA", "DEC"]).head(5)

    assert_same(*both(read))


@slow
def test_projection_too_large_to_read_in_one_block(both):
    """A partition of Gaia, projected — and still twenty megabytes of answer.

    This is the read that needs the answer to be seekable. `fsspec` fetches a body this
    size in blocks, and a query answer that refused ranges was one `pyarrow` could not
    open at all: it reported `partial: False`, handed back a streaming file, and the read
    ended at `Cannot seek streaming HTTP file`.
    """

    def read(open_catalog):
        return open_catalog("gaia_dr3", columns=GAIA_COLUMNS).partitions[0].compute()

    assert_same(*both(read))


@slow
def test_cone_search_on_gaia(both):
    """Ten arcminutes of sky, which LSDB reads as two partitions."""

    def read(open_catalog):
        return open_catalog("gaia_dr3", search_filter=lsdb.ConeSearch(10, 40, 600)).compute()

    assert_same(*both(read))


@slow
def test_crossmatch_gaia_with_sdss(both):
    """LSDB's crossmatch, over two catalogs read through the same route.

    One partition of the result: what varies is where the bytes of both catalogs came
    from, and Gaia is 2016 partitions deep.
    """

    def read(open_catalog):
        gaia = open_catalog("gaia_dr3", columns=GAIA_COLUMNS)
        sdss = open_catalog("sdss_dr7_spectra", columns=SDSS_COLUMNS)
        return gaia.crossmatch(sdss).partitions[0].compute()

    assert_same(*both(read))


@slow
def test_several_partitions_at_once(both, dask_client):
    """Three partitions read in parallel by two Dask workers.

    Every other check reads one partition at a time from one process, which is not how
    LSDB is used: a real read is a Dask graph whose tasks open the same catalog from
    several workers at once. What that exercises here is the service under concurrent
    requests for different objects of one mount — and, because each worker is its own
    process with its own HTTP session, without the client's connection pooling hiding a
    per-connection mistake.
    """

    def read(open_catalog):
        catalog = open_catalog("sdss_dr7_spectra", columns=SDSS_COLUMNS)
        return catalog.partitions[0:3].compute()

    assert_same(*both(read))
