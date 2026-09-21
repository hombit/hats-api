# query-benchmark

Times one cone search per HATS catalog through `POST /api/v1/simple/hats`. It writes a
config mounting the catalogs, starts a build of this service over it, sends the same
request `--runs` times each, prints what they took, and stops the service.

```sh
cargo build --release
cd query-benchmark && uv run query-benchmark
```

```
catalog    runs       min   median      max     rows   bytes read
-----------------------------------------------------------------
tess          1    36.41s   36.41s   36.41s        2     66.6 MiB
gaia          1     7.30s    7.30s    7.30s        1     33.9 MiB
ztf           1    21.64s   21.64s   21.64s        4    299.0 MiB
ps1           1     1.41s    1.41s    1.41s       87     14.2 MiB
```

Those are over the public internet from a laptop, and they are mostly the network. `ztf`
reads its `lightcurve` column whole, which is where its 299 MiB goes.

## Choosing catalogs, and reading them from elsewhere

`--catalog` takes `NAME`, or `NAME=LOCATION` to read it from somewhere else, and repeats.
Without it, all four. A location is anything a `[[mount]] source` takes — a local path, or
a url in any scheme the service reads.

```sh
uv run query-benchmark --catalog ps1 --catalog ztf
uv run query-benchmark --catalog ps1=s3://my-bucket/ps1/detection

# lsdb-cmu
uv run query-benchmark \
  --catalog gaia=/mnt/data/hats/catalogs/gaiadr3_epoch_phot \
  --catalog tess=/mnt/data/hats/catalogs/tess/tess_lightcurve

# arnor
uv run query-benchmark \
  --catalog gaia=/astro/store/shire/hats/catalogs/gaiadr3_epoch_phot \
  --catalog tess=/astro/store/shire/hats/catalogs/tess/tess_lightcurve

# bridges2
uv run query-benchmark \
  --catalog ps1=/ocean/projects/phy210048p/shared/hats/catalogs/ps1/ps1_detection \
  --catalog ztf=/ocean/projects/phy210048p/shared/hats/catalogs/ztf_dr24/ztf_dr24_lc-pageidx
```

Naming a catalog is also choosing it, so those measure two of the four. Add `--catalog
ps1 --catalog ztf` to measure the rest from their published locations in the same run.

The cone and the column lists are in `src/query_benchmark/catalogs.py`.

## Comparing two builds

```sh
cargo build --release && uv run query-benchmark --json baseline.json
RUSTFLAGS="-C target-cpu=native" cargo build --release && uv run query-benchmark --json native.json
```

Read `rows` and `bytes read` before the times: a build that returned fewer rows or read
fewer bytes is a different answer, not a faster one. Against a remote catalog the times
are mostly the network's, so point `--catalog NAME=LOCATION` at local copies.

| option | default |
| --- | --- |
| `--catalog NAME[=LOCATION]` | all four |
| `--runs N` | 5 |
| `--binary PATH` | `../target/release/hats-api` |
| `--json FILE` | not written |
| `--report-dir DIR` | `report/`, where a run that would not start says why |

## Tests

```sh
uv run pytest -c pyproject.toml
```

One real cone over `ps1`, and what `--catalog NAME=LOCATION` puts in the config. Neither
asserts a time.
