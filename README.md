# hats-api

A read-only HTTP service that serves parquet catalogs, and answers questions about one
without sending the whole thing.

Two interfaces:

- **File-server mode** publishes a local directory over HTTP. Without a query string it
  is an ordinary static file server; with one, a parquet file answers a question about
  itself.
- **API mode** filters a file the caller names in the request: a local one, or a remote
  one in S3, GCS, Azure Blob, a WebDAV server, or any HTTP server that honours `Range` —
  subject to rules the operator writes.

Both are off until configured, and both can run at once.

## Running it

```
cargo build --release
./target/release/hats-api --config hats-api.toml
```

```
usage: hats-api [--config <path>]

  -c, --config <path>  TOML configuration file; defaults to $HATS_API_CONFIG,
                       and to the built-in defaults when neither is given
  -h, --help           this message

environment:
  HATS_API_CONFIG        configuration file to read
  HATS_API_LISTEN_ADDR   address:port to listen on, overriding the config file
  RUST_LOG               tracing filter, overriding the config file
```

[`hats-api.example.toml`](hats-api.example.toml) is an example with every key written
out. The shortest useful file is one mount:

```toml
[server]
address = "127.0.0.1"
port = 8080

[api]
enabled = false

[[mount]]
path = "/"
source = "/srv/hats"
```

## File-server mode

Each `[[mount]]` publishes one directory under one url prefix. A request for a file
gets the file; a request for a directory gets its own `index.html` if it has one, and
otherwise a listing of every entry ordered by name.

A listing answers in whichever form the client asked for. `Accept: text/html` gets a
page; everything else, `*/*` included, gets JSON.

```json
{
  "path": "/dataset/Norder=1/Dir=0/",
  "parent": "/dataset/Norder=1/",
  "entries": [
    { "name": "Npix=44.parquet", "type": "file", "size": 4006,
      "modified": "2026-01-27T22:26:43Z", "url": "/dataset/Norder=1/Dir=0/Npix=44.parquet" }
  ]
}
```

Each entry carries its own `url`, already encoded, so a client walking the tree does not
have to encode names itself. `=` is left as it is, since HATS directories are called
`Norder=5` and `Npix=12240`.

### Asking a file for less of itself

Add a query string to a data file's own url:

```
GET /dataset/Norder=0/Dir=0/Npix=11.parquet?columns=id,ra,dec&filters=ra%20%3E%20300%20%26%26%20dec%20%3C%20-50
```

That `filters` reads `ra > 300 && dec < -50`, and it has to be percent-encoded. Let the
client do it:

```python
requests.get(
    "http://localhost:8080/dataset/Norder=0/Dir=0/Npix=11.parquet",
    params={"columns": "id,ra,dec", "filters": "ra > 300 && dec < -50"},
)
```

```
curl -G http://localhost:8080/dataset/Norder=0/Dir=0/Npix=11.parquet \
  --data-urlencode 'columns=id,ra,dec' \
  --data-urlencode 'filters=ra > 300 && dec < -50'
```

| parameter | means |
| --- | --- |
| `columns` | comma-separated column names. Absent returns every column. |
| `filters` | one row predicate; `&&` spells `AND`. Absent returns every row. |
| `limit` | most rows to return. |
| `format` | `parquet` (the default here) or `json`. |

This interface is made to be compatible with
[https://vizcat.cds.unistra.fr/hats/](https://vizcat.cds.unistra.fr/hats/).

The response is a parquet file laid out like the one it came from, with the row count and
the timing in `x-hats-num-rows` and `x-hats-elapsed-ms`. `format=json` returns the same
body shape as API mode.

Which files are data is one configured list of filename globs, `[data] filenames`,
defaulting to what a HATS catalog contains:

```toml
[data]
filenames = ["*.parq", "*.parquet", "*.pq", "_metadata", "_common_metadata"]
```

It is matched against a file's own name and never against the path above it. Anything
off the list — `properties`, `partition_info.csv`, an `index.html` — is served verbatim,
query string and all, and a directory takes no parameters either.

## API mode

The caller names the data in the request body. The route is under `[api] prefix`,
`/api/v1` by default:

```
POST /api/v1/parquet
GET  /api/v1/health
```

```json
{
  "url": "s3://survey-data/catalog/dataset/Norder=1/Dir=0/Npix=44.parquet",
  "storage": { "region": "us-east-1" },
  "select": "objectid, ra, dec, mag_g - 0.1 AS mag_g_corr",
  "where": "mag_g < 20 AND dec BETWEEN -30 AND -20",
  "region": [{ "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_deg": 0.01 }],
  "ra_column": "ra",
  "dec_column": "dec",
  "limit": 1000,
  "format": "json"
}
```

```json
{
  "num_rows": 2,
  "elapsed_ms": 15,
  "rows": [{ "objectid": 1383212200036217, "ra": 307.4, "dec": -24.9, "mag_g_corr": 18.6 }]
}
```

`POST` rather than `GET`: the request carries credentials, which a query string would
write to every proxy's access log, and a body has no url-length limit.

`region` and its two column names are the API's alone; a mounted file takes no spatial
parameter, since a request there selects a region by naming `Norder=k/Npix=p` in the path.

`select` and `where` take SQL expressions. `columns` and `filters` are also accepted and
mean what they mean in file-server mode; a request may use either pair and not both.
`format` defaults to `json` here and to `parquet` in file-server mode.

An error is a status code and a one-field body, `{"error": "…"}`.

### Selecting a region of the sky

`region` is a structured field rather than part of `where`. It is always an array, and
**the array is a union**: a row inside any of its shapes qualifies. The whole field is
then `AND`ed with `where`.

`ra_column` and `dec_column` say which columns of the file hold the position, and are
required alongside `region`. They resolve the same way any column name does: the file's
own spelling, or that spelling in lowercase.

Degrees throughout, and both ends of every range inclusive.

| `type` | fields |
|---|---|
| `circle` | `ra`, `dec`, and exactly one of `radius_deg` or `radius_arcsec` — the cone search, under ADQL's name for it |
| `box` | `ra: [from, to]`, `dec: [from, to]` |

```json
"region": [
  { "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_arcsec": 36 },
  { "type": "box", "ra": [349.5, 10.5], "dec": [-20, -10] }
]
```

A radius names its unit; a position is always degrees and takes no suffix.

A `box` is the product of two scalar ranges — what `ra BETWEEN … AND dec BETWEEN …` says,
with parallels for its north and south edges. It is `hats`'s and `lsdb`'s `box`, and the
numbers carry across from `box_search` unchanged.

`ra` in a box **runs eastward from the first value to the second**: `[350, 10]` is twenty
degrees across the origin and `[10, 350]` is the three hundred and forty the other way,
both legal and different boxes. A whole turn, `[0, 360]`, is every right ascension; the
two values naming the *same* point is refused. `dec` is ordered, so its first value may
not be the greater one.

Which numbers your file writes for a right ascension does not matter: a column running
from 0 to 360 and one running from -180 to 180 both work, and so does a shape crossing
the origin under either.

### The SQL

`select` and `where` are planned against the file's own schema, so
`objectid = 1383212200036217` is an `Int64` compared against row-group statistics, the
page index and a bloom filter.

A column answers to its own name — `Gmag`, `objectId`, whatever the file spells it — and
to that name in lowercase, which is what unquoted SQL means by a name.

Functions are judged by volatility, not by name: only immutable ones are callable, so
`now()` and `random()` are refused.

### Storage options

`storage` says how to reach the store; the url says which object. Leave it out for a
public object, which is read anonymously — no ambient credential of the deployment's own
is ever used to answer a request.

| option | for |
| --- | --- |
| `endpoint` | a server other than the provider's own: MinIO, Ceph, R2, Azurite |
| `region` | `s3://` |
| `access_key_id`, `secret_access_key`, `session_token` | `s3://` |
| `service_account_key`, `access_token` | `gs://` — the JSON Google issues, base64-encoded, or an OAuth2 token |
| `account`, `access_key`, `sas_token` | `az://` — `account` is required, since the url carries only the container |
| `headers` | `http(s)://` — a bearer token or an API key for a server that authenticates |
| `transport`, `username`, `password` | `webdav://` — `https` (default) or `http`, and a Basic credential |
| `allow_http` | permission to send the above to a cleartext endpoint |

Schemes: `s3`, `gs`, `az`, `http`, `https`, `webdav`, `file`. An option a scheme has no
use for is refused rather than ignored.

Credentials are never logged — not their values, and not their names either.

### What a request may reach

Two independent sets of rules, and both apply:

- **`[api.access.<backend>]`** decides which endpoint a request may name. Three states
  per backend: no `endpoints` key at all for any endpoint, an empty list to turn the
  scheme off, or a list for exactly those. `[api.access.local] paths` is empty by
  default, so no local file is readable until a directory is listed.
- **`[api.access.network]`** decides which addresses may be reached, whatever backend the
  request goes through. Loopback, private ranges — link-local included, where a cloud
  instance serves this machine's own IAM credentials — and network-internal names are all
  refused by default.

A name is judged before it is resolved, and every address it resolves to is judged again
inside the HTTP client's own resolver. Redirects are not followed.

Mounting a directory also lets API mode read it, scoped to that directory. The grant is
one way — `[api.access.local]` says nothing about what the mounts publish.

### Servers that ignore `Range`

A parquet read is tens of ranged requests, and a plain HTTP server may answer one with
the whole object and a `200`. Any backend whose host comes from the request is probed per
object, and an object on a server that will not serve ranges is copied to scratch once
and read from local disk after that. `[limits]` bounds that per object, in total, and in
flight at once.

## Development

`cargo test` passes with no network, no Docker and no credentials. Anything that needs a
real server is a separate test binary that skips when its environment variables are
absent.

```
pre-commit run --all-files    # cargo fmt, clippy -D warnings, and the tests
```

`CLAUDE.md` is the conventions this codebase is held to.
