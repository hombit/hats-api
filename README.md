# hats-api

A small web service for point lookups in HATS parquet catalogs.

Stateless by design for now: every request builds its own DataFusion session, opens the
remote file, reads its metadata, and throws all of it away. Nothing is cached.

## Endpoints

### `GET /api/v1/health`

```json
{"status": "ok"}
```

### `POST /api/v1/select`

Returns every row where `column == value`. The request is a JSON body:

```json
{
  "url": "s3://bucket/key.parquet",
  "storage": {"region": "us-west-2"},
  "column": "objectid",
  "value": "42",
  "columns": ["objectid", "lightcurve.mag"],
  "format": "json"
}
```

| field | meaning |
|---|---|
| `url` | where the data is: `s3://bucket/key.parquet`, or a local path |
| `storage` | *optional* how to reach the store — see [Storage options](#storage-options) |
| `column` | column to filter on; must be a top-level, non-nested column |
| `value` | the value to match, parsed into the column's own type |
| `columns` | *optional* list of columns to return; omit for all of them |
| `format` | *optional* `json` (default) or `parquet` |

**`POST`, because `storage` can carry credentials.** A query string lands in the access
log of every proxy, load balancer and CDN in front of this service, and in shell history;
a body does not. A body also has no URL-length limit, and lets `url` be an ordinary
string rather than a URL nested inside another URL's parameter.

`columns` entries are dotted paths into nested structs, e.g.
`["objectid", "lightcurve.mag", "objra"]`. A nested path reads only that leaf of the
parquet file, not every field of the struct, and the JSON key is the path as written.
Naming a column or a struct field that does not exist is a 400 that lists what is there.

With `"format": "parquet"` the same rows come back as a parquet file laid out like the file
they were read from — see [Parquet output](#parquet-output).

**Which URLs are allowed is a matter of configuration**, and by default that is any s3
endpoint except the loopback interface, and no local files at all. See
[Configuration](#configuration).

### Storage options

`storage` says how to reach the store; `url` says which object. They are kept apart
because a URL's own query string belongs to the origin — a presigned signature, a CDN
token — and there would be no way to tell one of those from one of ours. A `url` with a
query string is a 400, not a silent reinterpretation.

`s3://` and local files are supported so far; adding a scheme means adding a match arm
in `src/storage.rs` and a rule kind in `src/access.rs`.

| s3 option | meaning |
|---|---|
| `region` | the bucket's region, default `us-east-1` — S3 offers no way to discover it |
| `endpoint` | base URL of a non-AWS S3: MinIO, Ceph, R2, Wasabi. Omit for AWS |
| `allow_http` | send credentials to a plain-`http` endpoint; only meaningful with `endpoint` |
| `access_key_id`, `secret_access_key` | credentials; must be given together |
| `session_token` | for temporary credentials; needs the pair above |

```json
{"region": "us-west-2"}
{"access_key_id": "AKIA...", "secret_access_key": "...", "region": "eu-west-1"}
{"endpoint": "https://s3.example.com", "access_key_id": "...", "secret_access_key": "..."}
{"endpoint": "http://127.0.0.1:9000"}
```

That last one — an endpoint on the loopback interface — needs `allow_loopback` in the
config file, and is a 403 without it. The service can reach things on its own machine
that its callers are not meant to reach through it.

With no credentials the request is made anonymously, unsigned. An unknown option is a
400 rather than a silent fallback to anonymous, and so is a `storage` sent with a
`file://` url, which has no store to reach.

An `http://` endpoint works as-is for anonymous requests — there is no secret to expose,
and that is the usual local-MinIO case. Sending *credentials* to an `http://` endpoint
needs `"allow_http": true`, so a typo cannot put them in cleartext by accident. Requests to
a custom endpoint are path-style (`endpoint/bucket/key`), which is what MinIO and Ceph
expect; virtual-hosted style is not exposed yet.

### Local files

A `url` may also be a file on the machine the service runs on, written either way:

```
/srv/hats/ztf/Norder=5/Npix=12240/part0.parquet
file:///srv/hats/ztf/Norder=5/Npix=12240/part0.parquet
```

**No local directory is readable until one is named in the config file**, so this is a
403 out of the box. See [Configuration](#configuration) for `access.local.paths` and
`follow_symlinks`.

**On credentials.** The service never logs them and never puts them in an error message.
They are held in types that do not print, so everything downstream — logs, spans, error
text, DataFusion — sees `s3://bucket/key.parquet` and `***`. Credentials go in the
request body rather than a query string, which keeps them out of intermediaries' access
logs and shell history. What the service still cannot do is protect a secret in transit:
use TLS, and prefer short-lived credentials.

Example:

```bash
curl -s http://127.0.0.1:8080/api/v1/select -H 'content-type: application/json' -d '{
  "url": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/Norder=5/Dir=10000/Npix=12240/part0.snappy.parquet",
  "column": "_healpix_29",
  "value": "3445524782181585918",
  "columns": ["objectid", "lightcurve.mag", "objra", "objdec"]
}'
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

`"format": "parquet"` returns a parquet file rather than JSON:

```bash
curl -s http://127.0.0.1:8080/api/v1/select -H 'content-type: application/json' -o selection.parquet -d '{
  "url": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/Norder=5/Dir=10000/Npix=12240/part0.snappy.parquet",
  "column": "_healpix_29",
  "value": "3445524782181585918",
  "columns": ["objectid", "lightcurve.mag", "objra", "objdec"],
  "format": "parquet"
}'
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
the source footer for that costs one extra request, made only when `"format": "parquet"`.

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

## Configuration

A TOML file, passed with `--config` or `HATS_API_CONFIG`. Every key has a default and
the file itself is optional, so running with no config at all is the same as running
with the defaults below. `hats-api.example.toml` is that file, written out.

```toml
[server]
address = "0.0.0.0"      # every interface; "127.0.0.1" keeps it on the machine
port = 80

[access]
allow_loopback = false   # may a request reach 127.0.0.1?

[access.s3]
# endpoints = ["aws"]    # absent: any endpoint. See below.

[access.local]
paths = []               # no local files until a directory is listed
follow_symlinks = false

[log]
filter = "hats_api=info,tower_http=info"
format = "text"          # or "json", one object per event
ansi = true
```

An unknown key is a startup error rather than a silent default. This file decides what
the service will read, and a typo in it must not quietly widen or narrow that.

### `access.s3.endpoints`

For s3 the thing worth deciding is **which endpoint may be contacted, not which bucket
may be read**. A bucket name says nothing about the host: the request carries its own
`endpoint` option and can point the service anywhere, so `s3://allowed-bucket/key`
with a `storage.endpoint` of `https://elsewhere.example.com` would sail through a bucket allowlist
while the service talks to whatever that host is. So the rules are endpoints, and any
bucket at an allowed endpoint is readable.

| entry | means |
|---|---|
| `aws` | AWS S3 itself — a request with no `endpoint` option |
| `https://minio.example.com` | one S3-compatible server |

Three states, and the first two differ:

| `endpoints` | means |
|---|---|
| absent | any endpoint, still subject to `allow_loopback` |
| `[]` | s3 is off; the service serves no `s3://` urls at all |
| a list | exactly those endpoints, and nothing else |

A list that does not say `aws` does not allow AWS. Naming an endpoint in the list is
permission enough on its own — `allow_loopback` is not consulted for it, since writing
`http://127.0.0.1:9000` in a config file is already pointing at it deliberately.

### `access.local.paths`

Each entry is a directory, written as an absolute path (`/srv/hats`) or a `file://`
url (`file:///srv/hats`). Everything under it is readable. Empty, the default, is no
local file access at all; a directory that does not exist is a startup error, not a
rule that silently never matches.

Local paths get two checks an endpoint does not need, because a filesystem has ways of
pointing outside itself:

- a path is **resolved before it is matched**, so a symlink inside an allowed directory
  cannot lead out of one, and `..` cannot climb out either;
- unless `follow_symlinks` is on, a path that goes through a symlink at all is refused.

Turning `follow_symlinks` on therefore does not widen *which files* can be read — only
how they may be spelled.

A refused request is a 403 naming what the server does allow. A file that does not
exist inside an allowed directory is a 404; one outside every allowed directory is a
403 whether it exists or not, so the endpoint cannot be used to probe the filesystem.

## Running

```bash
cargo run --release                              # 0.0.0.0:80, any s3 endpoint, no local files
cargo run --release -- --config hats-api.toml
HATS_API_CONFIG=hats-api.toml cargo run --release
HATS_API_LISTEN_ADDR=127.0.0.1:8080 cargo run    # overrides [server]
RUST_LOG=hats_api=debug cargo run                # overrides [log].filter
```

## Docker

```bash
docker build -t hats-api .
docker run --rm -p 8080:80 hats-api
curl -s localhost:8080/api/v1/health
```

The image needs no config file: `0.0.0.0:80` and "any s3 endpoint, no local files" are
the built-in defaults. Mount one to narrow what the service may read, or to give it a
local directory:

```bash
docker run --rm -p 8080:80 \
  -v ./hats-api.toml:/etc/hats-api.toml:ro \
  -v /srv/hats:/data:ro \
  -e HATS_API_CONFIG=/etc/hats-api.toml \
  hats-api
```

with `paths = ["/data"]` under `[access.local]` in that file — the container's view of
the path, not the host's.

The image runs as a non-root user, listens on port 80, and carries a healthcheck on
`/api/v1/health`. `HATS_API_CONFIG`, `HATS_API_LISTEN_ADDR` and `RUST_LOG` work the
same as outside the container. Dependencies are built on their own layer, so editing
`src/` rebuilds in seconds rather than recompiling DataFusion.

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
