# hats-api

A small web service for point lookups in HATS parquet catalogs.

Stateless by design for now: every request builds its own DataFusion session, opens the
remote file, reads its metadata, and throws all of it away. Nothing is cached.

## Endpoints

### `GET /api/v1/health`

```json
{"status": "ok"}
```

### `GET /api/v1/select`

Returns every row where `column == value`.

| parameter | meaning |
|---|---|
| `url` | where the data is: `s3://bucket/key.parquet` |
| `column` | column to filter on; must be a top-level, non-nested column |
| `value` | the value to match, parsed into the column's own type |
| `columns` | *optional* comma-separated columns to return; omit for all of them |
| `format` | *optional* `json` (default) or `parquet` |

`columns` entries are dotted paths into nested structs, e.g.
`objectid,lightcurve.mag,objra`. A nested path reads only that leaf of the parquet
file, not every field of the struct, and the JSON key is the path as written. Naming a
column or a struct field that does not exist is a 400 that lists what is there.

With `format=parquet` the same rows come back as a parquet file laid out like the file
they were read from — see [Parquet output](#parquet-output).

Storage-specific options travel in the `url`'s own query string, not as parameters of
this endpoint. Only `s3://` is supported so far; adding a scheme means adding a match
arm in `src/storage.rs` and nothing else.

| s3 option | meaning |
|---|---|
| `region` | the bucket's region, default `us-east-1` — S3 offers no way to discover it |
| `endpoint` | base URL of a non-AWS S3: MinIO, Ceph, R2, Wasabi. Omit for AWS |
| `allow_http` | send credentials to a plain-`http` endpoint; only meaningful with `endpoint` |
| `access_key_id`, `secret_access_key` | credentials; must be given together |
| `session_token` | for temporary credentials; needs the pair above |

```
s3://bucket/key.parquet?region=us-west-2
s3://bucket/key.parquet?access_key_id=AKIA...&secret_access_key=...&region=eu-west-1
s3://bucket/key.parquet?endpoint=https://s3.example.com&access_key_id=...&secret_access_key=...
s3://bucket/key.parquet?endpoint=http://127.0.0.1:9000                   # local MinIO, anonymous
```

With no credentials the request is made anonymously, unsigned. An unknown option is a
400 rather than a silent fallback to anonymous.

An `http://` endpoint works as-is for anonymous requests — there is no secret to expose,
and that is the usual local-MinIO case. Sending *credentials* to an `http://` endpoint
needs `allow_http=true`, so a typo cannot put them in cleartext by accident. Requests to
a custom endpoint are path-style (`endpoint/bucket/key`), which is what MinIO and Ceph
expect; virtual-hosted style is not exposed yet.

**On credentials in URLs.** The service never logs them and never puts them in an error
message: it strips the query string the moment the store is built, and everything
downstream — logs, error text, DataFusion — sees only `s3://bucket/key.parquet`. The
request span logs method and path, not the query string. What the service cannot do is
protect a secret in transit through someone else's infrastructure: a URL query string
lands in the access logs of any proxy, load balancer or CDN in front of this service,
and in shell history. Use TLS, and prefer short-lived credentials.

Example:

```bash
curl -sG http://127.0.0.1:8080/api/v1/select \
  --data-urlencode 'url=s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/Norder=5/Dir=10000/Npix=12240/part0.snappy.parquet' \
  --data-urlencode 'column=_healpix_29' \
  --data-urlencode 'value=3445524782181585918' \
  --data-urlencode 'columns=objectid,lightcurve.mag,objra,objdec'
```

```json
{
  "num_rows": 1,
  "elapsed_ms": 9877,
  "rows": [{
    "objectid": 1383212200036217,
    "lightcurve.mag": [22.03865, 22.190256, 22.104124],
    "objra": 320.65747,
    "objdec": -12.35315
  }]
}
```

## Parquet output

`format=parquet` returns a parquet file rather than JSON:

```bash
curl -sG http://127.0.0.1:8080/api/v1/select \
  --data-urlencode 'url=s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/Norder=5/Dir=10000/Npix=12240/part0.snappy.parquet' \
  --data-urlencode 'column=_healpix_29' \
  --data-urlencode 'value=3445524782181585918' \
  --data-urlencode 'columns=objectid,lightcurve.mag,objra,objdec' \
  --data-urlencode 'format=parquet' -o selection.parquet
```

```
content-type: application/vnd.apache.parquet
content-disposition: attachment; filename="part0.snappy.parquet"
x-hats-num-rows: 1
x-hats-elapsed-ms: 8173
```

The download is named after the source object, so a directory of these says which
partition each came from. `num_rows` and `elapsed_ms` have nowhere to live in a parquet
body, so they travel as headers; the JSON form keeps them in the body as before.

Nested columns keep their requested names, which is what makes the output a flat
`lightcurve.mag` column of lists rather than a `lightcurve` struct.

### The result is written the way the source is written

The answer to a point lookup is a few rows out of a file someone else wrote, so it is
written back with that file's own layout rather than with the writer's defaults. Reading
the source footer for that costs one extra request, made only when `format=parquet`.

Per leaf column, matched by its parquet path:

- the compression codec. Its *level* is not recorded anywhere in a parquet file, so the
  writer's default level for that codec is used.
- dictionary encoding, and the fallback encoding when the column is not
  dictionary-encoded. Parquet does not record which encoding held the *data* either —
  the chunk lists the encodings of its levels and its dictionary page alongside it — so
  this is read off the encoding list: RLE is a level encoding for everything but
  booleans, PLAIN is what a dictionary page uses, and what is left is the data encoding.
- statistics: page-level when the source column has a column index, chunk-level when it
  has only chunk statistics, off when it has neither.
- bloom filters, when the source column has one.

Per file: the largest row group row count, and the writer version — which parquet also
does not record, and which is taken as 2.0 when a v2-only encoding (`DELTA_*`,
`BYTE_STREAM_SPLIT`) is in use anywhere.

A column the source does not have under that path — and a nested column whose list or
map nesting is named differently there — simply keeps the writer's defaults.

The source file's key/value metadata is *not* copied. It describes the source's own
schema (`ARROW:schema`, pandas metadata), and a projection of a few rows is not that
file.

## Running

```bash
cargo run --release            # listens on 127.0.0.1:8080
HATS_API_LISTEN_ADDR=0.0.0.0:80 cargo run --release
RUST_LOG=hats_api=debug cargo run
```

## Docker

```bash
docker build -t hats-api .
docker run --rm -p 8080:80 hats-api
curl -s localhost:8080/api/v1/health
```

The image runs as a non-root user, listens on port 80, and carries a healthcheck on
`/api/v1/health`. `HATS_API_LISTEN_ADDR` and `RUST_LOG` work the same as outside the
container. Dependencies are built on their own layer, so editing `src/` rebuilds in
seconds rather than recompiling DataFusion.

## How the read is done

`src/query.rs` turns on everything that makes a parquet point lookup cheap, and assumes
nothing about the file:

- `pushdown_filters` — **off in DataFusion by default**, and worth more than everything
  else combined. It evaluates the predicate while decoding, so the data columns of
  non-matching row groups and pages are never fetched.
- `enable_page_index`, `bloom_filter_on_read`, `pruning` — used when the file happens to
  carry a page index or bloom filters, ignored when it does not.


The filter value is parsed into the column's own Arrow type. That is what makes
statistics, page index and bloom filter pruning possible at all: comparing as strings
would quietly read the whole file.

## Known costs

Ask for the columns you need. Without `columns` the endpoint fetches every column chunk
of every row group that survives pruning — on the ZTF DR24 file above, ~375 MB of a
363 MiB file. With `columns=objectid,lightcurve.mag,objra,objdec` it is ~37 MB.
Measured against that file from a home connection:

| | bytes | wall clock |
|---|---|---|
| `columns=objectid,lightcurve.mag,objra,objdec` | ~37 MB | 5.1, 5.4, 6.0, 6.6, 7.1 s |
| all columns | ~375 MB | 16.4 s, 141.8 s |

These are transfer-bound, not service-bound: raw S3 throughput measured at the same
time was 6.8 MB/s, and 37 MB ÷ 6.8 MB/s is 5.4 s. The numbers therefore say nothing
flattering about the service and everything about how much data each form moves — which
is the point. Both forms show occasional outliers several times the median when the link
stalls (one 48 s run on the four-column form).

That file also carries no page index and no bloom filters, so pruning has only row-group
statistics to work with, and its `_healpix_29` row groups overlap. None of that is
something the service can fix.
