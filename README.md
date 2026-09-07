# hats-api

A read-only HTTP service that serves parquet catalogs, and answers questions about one
without sending the whole thing.

A HATS catalog is a directory tree of parquet partitions, and the usual way to read one
column out of one partition over HTTP is to fetch the footer, then a dozen byte ranges,
then decode locally. That works, and it is what `lsdb` does. It stops working when the
client is a browser, a notebook on a slow link, or anything that wanted a hundred rows
out of a two-gigabyte file. This service does the read where the data is and sends back
the answer.

It does that behind two interfaces, and they execute the same way:

- **File-server mode** publishes a local directory over HTTP. Without a query string it
  is an ordinary static file server — ranged requests, `ETag`, conditional requests,
  directory listings — so an existing `lsdb` or `fsspec` client works against it with no
  knowledge that it is anything else. With a query string, a parquet file answers a
  question about itself.
- **API mode** takes the location of the data in the request instead, so one deployment
  can read a catalog in S3, GCS, Azure Blob, a WebDAV server, or any HTTP server that
  honours `Range` — subject to rules the operator writes, since a service that will fetch
  any url on request is a proxy into its own network.

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

`hats-api.example.toml` is the whole configuration surface with every default written
out, and it is the reference — every key is optional, and an unknown key is a startup
error rather than a silent default. The shortest useful file is one mount:

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
page; everything else, `*/*` included, gets JSON — because `*/*` is what every client
library sends and it is not a request for markup.

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

Each entry carries its own `url`, already encoded, so a client walking the tree never has
to know how a name becomes one — which is the part with the rules in it. A `/`, a `%` or
a `#` in a name has to be encoded or it stops being part of the name; `=` deliberately is
not, because HATS directories are called `Norder=5` and `Npix=12240`, and a listing that
spells those `Norder%3D5` is unreadable for no gain.

### Asking a file for less of itself

Add a query string to a data file's own url:

```
GET /dataset/Norder=0/Dir=0/Npix=11.parquet?columns=id,ra,dec&filters=ra%20%3E%20300%20%26%26%20dec%20%3C%20-50
```

That `filters` reads `ra > 300 && dec < -50`. It has to be percent-encoded, and not only
for the `&&`: `<` and `>` are not legal in a request line, so a client that sends them
raw gets a bodyless `400` from the HTTP layer before this service sees the request at
all.

| parameter | means |
| --- | --- |
| `columns` | comma-separated column names. Absent returns every column. |
| `filters` | one row predicate. `&&` spells `AND`, since a bare `&` separates parameters. Absent returns every row. |
| `limit` | most rows to return. |
| `format` | `parquet` (the default here) or `json`. |

`columns` and `filters` are [vizcat](https://vizcat.cds.unistra.fr/hats/)'s names, so a
client written against that service reads a mount here after changing the host. What they
mean is this service's own: a `filters` that does not parse, or that names a column the
file has not got, is a `400` rather than a request that silently returned every row.

The response is a parquet file laid out like the one it came from, with the row count and
the timing in `x-hats-num-rows` and `x-hats-elapsed-ms`. `format=json` returns the same
body shape as API mode.

Two things this deliberately does not do. It does not answer a query on a file it does
not read as data — `properties`, `partition_info.csv`, an `index.html` — those go out
verbatim, parameters and all, the way any file server ignores a parameter it has no use
for. And a directory takes no parameters at all: a listing is a listing.

Which files are data is one configured list of filename globs, `[data] filenames`,
defaulting to what a HATS catalog contains:

```toml
[data]
filenames = ["*.parq", "*.parquet", "*.pq", "_metadata", "_common_metadata"]
```

It is matched against a file's own name and never against the path above it, so a
directory called `catalog.parquet` does not make what is under it data. `_metadata` and
`_common_metadata` are why this is a list of names rather than a list of suffixes: they
are parquet files with no extension at all.

Being on the list is a claim about the name, not about the contents. A file called
`part0.parquet` that is not one is refused by the parquet reader, as the caller's mistake
rather than a fault here — and so is `_metadata`, whose footer describes the rows in the
partition files beside it rather than any of its own.

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

`POST` rather than `GET`, because the request carries credentials: a query string is
written to every proxy's access log and to the caller's shell history on the way. A body
also has no url-length limit, and a list of ten thousand object ids is an ordinary
request here.

`region` and its two column names are the API's alone; a mounted file takes no spatial
parameter, since a request there selects a region by naming `Norder=k/Npix=p` in the path.

`select` and `where` take SQL expressions. `columns` and `filters` are also accepted and
mean exactly what they mean in file-server mode, so client code can move between the two
interfaces; a request may use either pair and not both. `format` is `json` here and
`parquet` in file-server mode, so that adding a query string to a path does not change
what media type it answers with.

An error is a status code and a one-field body, `{"error": "…"}`, and it never quotes a
value out of the request back at you — the request is where the credentials are.

### Selecting a region of the sky

`region` is a structured field rather than part of `where`, because a spatial constraint
has to be *recognised* to be planned on — which is what will let it choose partitions
rather than scan them. It is always an array, and **the array is a union**: a row inside
any of its shapes qualifies. The whole field is then `AND`ed with `where`.

`ra_column` and `dec_column` say which columns of the file hold the position, and are
required alongside `region`. A parquet file carries nothing that says which of its columns
are coordinates, and guessing from names would answer a different question than the one
asked without saying so. They resolve the same way any column name does — the file's own
spelling, or that spelling in lowercase.

Degrees throughout, ICRS, and both ends of every range inclusive.

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

A radius names its unit because a bare `radius` cannot: `0.01` is a plausible cone in
either degrees or arcseconds, the two differ by a factor of 3600, and nothing in the
answer would say which reading you got. Positions need no such suffix — they are always
degrees.

**A `box` takes a range in each coordinate, not a centre and a size.** It is the product of
two scalar ranges — what `ra BETWEEN … AND dec BETWEEN …` already says — so its north and
south edges are parallels rather than great circles. This is `hats`'s and `lsdb`'s `box`,
so the numbers carry across from `box_search` unchanged. It is *not* ADQL's `BOX`, which
takes a centre with a width and a height, and which ADQL 2.1 deprecates.

`ra` in a box **runs eastward from the first value to the second**, so `[350, 10]` is
twenty degrees across the origin and `[10, 350]` is the three hundred and forty the other
way — both legal, and different boxes. There is no ordering on a circle for a `min`/`max`
pair to have meant. A whole turn, `[0, 360]`, is every right ascension; the two values
naming the *same* point is refused, since it reads equally as an empty box or as the whole
sky. `dec` does have an ordering, so its first value may not be the greater one.

Which numbers your file writes for a right ascension does not matter: `[0, 360)` and
`[-180, 180)` both work, and so does a shape crossing the origin under either.

### The SQL

Expressions, never statements. There is no `FROM` to write: the url says what is being
read. A join across two catalogs is a crossmatch and is `lsdb`'s job, and `GROUP BY` is
not a point lookup.

Both fields are planned against the file's own schema, which is what makes
`objectid = 1383212200036217` an `Int64` compared against row-group statistics, the page
index and a bloom filter — rather than a string comparison that reads the whole file and
returns nothing.

A column answers to its own name and to its name in lowercase, and to nothing else.
Astronomy column names are mixed-case as a matter of course — `Gmag`, `Norder`,
`objectId` — and a caller reads them off the file, so the file's spelling has to work;
lowercase has to work too, because that is what unquoted SQL means by a name.

Functions are judged by volatility, not by name: only immutable ones are callable.
`now()` and `random()` make one request's answer differ from the next's for the same
query.

### Storage options

`storage` says how to reach the store, never what the object is — that is the url, which
this service treats as opaque. Leave it out for a public object, which is read
anonymously: no ambient credential of the deployment's own is ever used to answer a
request.

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
use for is refused rather than ignored, so a misspelling is an error rather than a
credential that quietly went nowhere.

Credentials are never logged — not their values, and not their names either, since a
token typed into the wrong field is still a token in this service's log.

### What a request may reach

API mode is a service that makes outbound requests on a stranger's instruction, so
everything is off until an operator says otherwise. Two independent sets of rules, and
both apply:

- **`[api.access.<backend>]`** decides which endpoint a request may name. Three states
  per backend: no `endpoints` key at all for any endpoint, an empty list to turn the
  scheme off, or a list for exactly those. `[api.access.local] paths` is empty by
  default, so no local file is readable until a directory is listed.
- **`[api.access.network]`** decides which addresses may be reached, whatever backend the
  request goes through. Loopback, private ranges and network-internal names are all
  refused by default — link-local is where a cloud instance serves this machine's own IAM
  credentials.

A name is judged before it is resolved, and every address it resolves to is judged again
inside the HTTP client's own resolver, so a name that answers differently the second time
round does not help. Redirects are not followed: a `3xx` is the origin choosing where the
caller's credentials go next.

Mounting a directory also lets API mode read it, scoped to that directory. The bytes are
already served whole over the file-server route, so refusing to query them would withhold
nothing. The grant is one way — `[api.access.local]` says nothing about what the mounts
publish.

### Servers that ignore `Range`

A parquet read is tens of ranged requests. A plain HTTP server may answer a ranged
request with the whole object and a `200`, and the reader has no way to tell: it gets the
head of the file where it asked for the tail, which is a wrong answer rather than a
failed request. Any backend whose host comes from the request is probed per object, and
an object on a server that will not serve ranges is copied to scratch once and read from
local disk after that. `[limits]` bounds that — per object, in total, and in flight at
once — so it is a cost rather than a way to fill the disk.

## Development

`cargo test` passes with no network, no Docker and no credentials. Anything that needs a
real server is a separate test binary that skips when its environment variables are
absent.

```
pre-commit run --all-files    # cargo fmt, clippy -D warnings, and the tests
```

`CLAUDE.md` is the conventions this codebase is held to.
