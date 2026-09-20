"""Every `s3://` catalog data.lsdb.io publishes, by the slug this suite mounts it at.

So `s3://…` and `http://<service>/<slug>` are one catalog reached two ways, which is the
whole of what a check compares. All are public and read anonymously, by both routes.
"""

from __future__ import annotations

#: Every bucket here is in this region.
REGION = "us-east-1"

#: The ones whose partitions are directories — `hats_npix_suffix = "/"` — which no check
#: reads through this service.
#:
#: LSDB reads such a partition by listing it, and the url it lists carries the query
#: string it would have read the partition with. `fsspec`'s HTTP listing keeps only links
#: that start with the url it was given, query string and all, so no href can match and
#: the listing is empty whatever is served. There is no answer this service could give
#: that would work; the query has to be dropped before the listing, in `hats` or in
#: `fsspec`. Both routes are fine reading these off S3, which lists natively.
DIRECTORY_PARTITIONED = frozenset(
    {"euclid_q1", "ztf_dr23_lc", "ztf_dr23_objects", "ztf_dr24_lc", "ztf_dr24_objects"}
)

CATALOGS: dict[str, str] = {
    "delve_dr2": "s3://stpubdata/mast/public/delve/hats/delve_dr2",
    "delve_dr3_gold": "s3://stpubdata/mast/public/delve/hats/delve_dr3_gold",
    "des_dr2": "s3://stpubdata/mast/public/des/hats/des_dr2",
    "des_y6_gold": "s3://stpubdata/mast/public/des/hats/des_y6_gold",
    "desi_dr1_zcat": "s3://stpubdata/mast/public/desi/hats/desi_dr1_zcat",
    "euclid_q1": "s3://nasa-irsa-euclid-q1/contributed/q1/merged_objects/hats",
    "gaia_dr3": "s3://stpubdata/gaia/gaia_dr3/public/hats",
    "galex": "s3://stpubdata/galex/public/hats/galex",
    "ps1_detection": "s3://stpubdata/panstarrs/ps1/public/hats/detection",
    "ps1_forced_mean_object": "s3://stpubdata/panstarrs/ps1/public/hats/forced_mean_object",
    "ps1_otmo": "s3://stpubdata/panstarrs/ps1/public/hats/otmo",
    "ps1_stack_object": "s3://stpubdata/panstarrs/ps1/public/hats/stack_object",
    "sdss_dr7_spectra": "s3://stpubdata/sdss/public/hats/sdss_dr7_spectra",
    "tic": "s3://stpubdata/tess/public/hats/tic",
    "ztf_dr23_lc": "s3://ipac-irsa-ztf/contributed/dr23/lc/hats",
    "ztf_dr23_objects": "s3://ipac-irsa-ztf/contributed/dr23/objects/hats",
    "ztf_dr24_lc": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats",
    "ztf_dr24_objects": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/objects/hats",
}
