# hats-api

A read-only HTTP service that serves parquet catalogs, and answers questions about one
without sending the whole thing.

Two interfaces:

- **File-server mode** publishes a local directory over HTTP. Without a query string it
  is an ordinary static file server; with one, a parquet file answers a question about
  itself and a HATS catalog answers a cone search over its partitions.
- **API mode** filters a file the caller names in the request: a local one, or a remote
  one in S3, GCS, Azure Blob, a WebDAV server, or any HTTP server that honours `Range` —
  subject to rules the operator writes.

Both are off until configured, and both can run at once.

A `[[mount]]` is the only way a local directory becomes readable, in either mode. Its
`path` is the address both modes use; `serve` publishes it as a directory as well.

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
serve = true
```

## Mounts

```toml
[[mount]]
path = "/hats"            # the address, in both modes
source = "/data/hats"     # where it actually is, which no caller sees
serve = true              # publish it as a directory; off is API-only
follow_symlinks = false
immutable = false
filenames = ["*.parquet"] # in place of [data] filenames, for this mount
```

`path` is the address in both modes. The file server publishes the directory there, and
an API request names a file in it by the same path — `file:///hats/dr1/x.parquet`, never
the `/data/hats` it lives in.

`serve` publishes the directory. With it off the mount is not served and not listed, and
a request for any path under it is a 404; an API request naming a file in it is answered
as usual.

Two mounts may not claim overlapping url prefixes, served or not. A mount's `filenames`
replaces `[data] filenames` for the files under it.

## File-server mode

A request for a file gets the file; a request for a directory gets its own `index.html`
if it has one, and otherwise a listing of every entry ordered by name.

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
| `ra`, `dec` | the centre of a cone, in degrees. |
| `radius_arcsec`, `radius_deg` | its radius; exactly one of the two. |
| `ra_column`, `dec_column` | which columns hold the position. Required with a cone against a file, refused against a catalog. |

`columns`, `filters`, `limit` and `format` are made to be compatible with
[https://vizcat.cds.unistra.fr/hats/](https://vizcat.cds.unistra.fr/hats/).

The response is a parquet file laid out like the one it came from, with the row count, the
bytes read out of the source file and the timing in `x-hats-num-rows`,
`x-hats-data-bytes-read` and `x-hats-elapsed-ms`. `format=json` returns the same body shape
as API mode.

Rows come back in the source file's order, so the same request twice gives the same rows
in the same places, and a `limit` is the front of the file rather than an arbitrary
selection of rows.

**A `limit` bounds the rows, never the bytes.** Parquet is fetched a column chunk at a
time, so a query reads every chunk holding a row it returns, however few rows that is —
and where a partition was written as a single row group, that is the whole of every column
named. Naming the columns is the thing that makes a query cheap: on a 335 MiB, 153-column
Gaia partition, ten rows of everything read 335 MiB and ten rows of one column read 3.8
MiB. `x-hats-data-bytes-read`, and `data_bytes_read` in a JSON answer, is where that shows.

### Querying a catalog by its own url

A HATS catalog's directory answers a query on its url. The catalog chooses which of its
partitions to read and names its own position columns, so the request is only what to
narrow it by:

```
GET /small_sky_order3_source?limit=10
GET /small_sky_order3_source?ra=348.077&dec=-29.339&radius_arcsec=30&columns=source_id,mag
```

The first is the front of the catalog, in the catalog's own order — which is HEALPix order,
so it is a coherent piece of sky rather than an arbitrary sample. **A `limit` stops the
read**: partitions are read in order until there are enough rows, so ten rows of a
thousand-partition catalog cost the first partition.

The second narrows it to a cone. The answer is the same body as the API's catalog route —
parquet by default here, with the partition count in `x-hats-num-partitions` — and
`format=json` gives the rows with `num_partitions` beside them.

**The radius is capped, at 600″ by default.** A url is followed rather than fanned out, so
what it asks for has to fit in one answer; a wider search is the API's, whose plan route
hands back the requests it takes. `[limits] max_query_radius_arcsec` is the operator's
knob, and `0` closes the circle surface entirely, leaving the plain `limit` request.

```toml
[limits]
max_query_radius_arcsec = 600
```

With neither a circle nor a limit the request is the whole catalog, and `max_partitions`
refuses it before anything is read. With no query string at all the url is the directory
listing it always was, and a directory that is not a catalog ignores the parameters
entirely — whether there is a query surface is decided by which directory this is, before
any parameter is looked at.

### The page

A browser gets a page for a directory, and a file this service reads as data gets a query
panel on it: the columns are fetched from the file when the panel is opened, and the query
runs against the same url a client would write by hand.

A directory that is a catalog — or that is inside one, `dataset/Norder=5/Dir=0` included —
gets the query over the whole catalog above it: the catalog's name, what it says about its
own size, its columns read from `dataset/_common_metadata`, the request written out for
`curl` and for the Python readers, and a **Plan** button. Preview asks for the first ten
rows and the circle is optional, so a catalog answers something the moment it is opened.

Both are additions to the markup rather than replacements for it: with the script blocked,
the listing and its links are exactly what they were, and what the page says about querying
it says as a url.

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
POST /api/v1/expr/parquet     POST /api/v1/simple/parquet
POST /api/v1/expr/hats        POST /api/v1/simple/hats
POST /api/v1/expr/hats/plan   POST /api/v1/simple/hats/plan
GET  /api/v1/health
```

Two segments, and each names one thing. The **second** is what the url names: `parquet`
takes a url naming one file, `hats` takes a url naming a catalog directory and chooses the
files itself, and `hats/plan` takes the same body and returns the work rather than doing it
— [below](#querying-a-whole-catalog).

The **first** is the vocabulary the body is written in:

| | projection | predicate |
|---|---|---|
| `expr` | `select` — a SQL select list, so `mag - 0.1 AS mag_corr` works | `where` — one boolean SQL expression |
| `simple` | `columns` — comma-separated names, never expressions | `filters` — one predicate, `&&` spelling `AND` |

Both lower to the same planned expression, so the two answer identically; they differ in
what a field may hold. `simple` is what a url query string can carry, which is why
file-server mode reads the same pair out of one, and it is what
[vizcat](https://vizcat.cds.unistra.fr/hats/) clients write.

`expr` is named for what a field holds and not for SQL, because a *statement* is refused:
each field is parsed on its own and the parser must reach the end of the string, so
`SELECT … FROM …` is a different thing rather than a longer form of this.

**A field of the other vocabulary is refused, not ignored** — `filters` sent to an `expr`
route is a `400` naming the field and the vocabulary it belongs to. Dropping it would return
every row, which a caller cannot tell from a predicate that matched them all.

Everything else — `url`, `storage`, `region`, `limit`, `format`, the column names — is the
same in both, and the body stays flat either way.

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
  "schema": [
    { "name": "objectid", "type": "Int64" },
    { "name": "ra", "type": "Float64" },
    { "name": "dec", "type": "Float64" },
    { "name": "mag_g_corr", "type": "Float64" }
  ],
  "data_bytes_read": 41238,
  "elapsed_ms": 15,
  "rows": [{ "objectid": 1383212200036217, "ra": 307.4, "dec": -24.9, "mag_g_corr": 18.6 }]
}
```

`schema` describes the answer — the projection where the request made one, the file's own
columns where it did not. It is there because rows do not describe themselves: an answer
that matched nothing looks like a file without the column, so `limit=0` is how to ask what
a file holds, and it reads no data at all.

A struct column also carries `fields`, its own fields one level down — a HATS catalog packs
a light curve into one, so the column is `sources` and what a reader wants is `sources.mjd`.
Both vocabularies plan that spelling. **Each name is its own, not the path**, so a part that
needs quoting is quoted on its own: `"sources"."mjd"` names the field, while
`"sources.mjd"` names a column no file has got and is refused.

```json
{ "name": "sources", "type": "Struct(...)",
  "fields": [{ "name": "mjd", "type": "List(Float64)" },
             { "name": "mag", "type": "List(Float64)" }] }
```

Only a struct: a list of structs holds the same names and a compound identifier does not
reach into one, so listing its fields would offer a name that does not answer. A scalar
column carries no `fields` key at all rather than an empty list.

**Every row carries every column, and a value JSON cannot spell is a string.** A null is
written as `null` rather than left out, so a row's keys are the answer's columns and not
whatever that row happened to have. `NaN`, `Infinity` and `-Infinity` come back as those
three strings, which `float()` in Python and `Number()` in JavaScript both read back — JSON
has no number for them, and writing `null` instead would report three values a file really
holds as a fourth it does not. In a photometric column they are ordinary, so a caller
reading `NaN` as "no measurement" would have a wrong answer rather than an error. The
`parquet` format carries all of them as themselves and needs none of this.

`POST` rather than `GET`: the request carries credentials, which a query string would
write to every proxy's access log, and a body has no url-length limit.

`region` and its two column names are the API's alone; a mounted file takes no spatial
parameter, since a request there selects a region by naming `Norder=k/Npix=p` in the path.

`format` defaults to `json` here and to `parquet` in file-server mode.

Rows come back in no particular order — unlike file-server mode, which preserves the
file's. A `limit` is still reproducible: the same request against the same file returns
the same rows, in whatever order they arrive.

An error is a status code and a one-field body, `{"error": "…"}`.

### Querying a whole catalog

`POST /api/v1/{expr,simple}/hats` takes the same body, with `url` naming a HATS catalog
directory rather than a file in it — here in the `simple` vocabulary:

```json
{
  "url": "s3://survey-data/catalog",
  "region": [{ "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_deg": 0.5 }],
  "columns": "objectid, ra, dec",
  "limit": 1000
}
```

**The catalog names its own columns, so this route refuses `ra_column`, `dec_column`,
`healpix_column` and `healpix_order`.** `hats_col_ra`, `hats_col_dec` and `hats_col_healpix`
are what the catalog says its files hold, and it can see more of them than a request can. A
caller who wants a different pair tested queries one of the files directly, where the
single-file route takes them. They are refused rather than ignored: rows tested against
columns the caller did not write are an answer they cannot tell from the one they asked for.

The answer is a `parquet`-route answer with one more field:

```json
{ "num_rows": 412, "num_partitions": 3, "schema": [], "data_bytes_read": 8241938,
  "elapsed_ms": 380, "rows": [] }
```

`num_partitions` is how many of the catalog's partitions were read. Next to
`data_bytes_read` it is what says the region pruned: a cone that touches three partitions
of a hundred thousand reads three.

**Rows come back in HEALPix order** — partition by partition, in the order the catalog's
cells fall on the sky, so neighbouring rows arrive near each other and a `limit` is a
coherent piece of sky. Within one partition nothing is promised, as everywhere else in API
mode.

**Omitting `region` reads the whole catalog**, which is what the limits below are for.

Partitions are read `limits.max_concurrent_partitions` at a time and still come back in the
catalog's order, so the parallelism costs nothing in reproducibility.

### What one request may spend

Three bounds, whichever is reached first, all in `[limits]`:

| | default | |
|---|---|---|
| `max_partitions` | 16 | checked before anything is read |
| `max_bytes_fetched` | `10GiB` | watched as partitions land |
| `max_rows` | 1000000 | watched as partitions land |

Only the first can act before work happens; the other two are counters, so a request
overshoots them by whatever the reads already in flight go on to fetch. Nothing is returned
part-way: a truncated answer is one a caller cannot tell from a complete one.

**A `limit` is the other bound that acts before work happens.** The read stops as soon as
enough rows are in, so a request that carries one is bounded by it rather than by the
partition list, and `max_partitions` is watched as the reads land instead of refusing the
list up front. That is what makes `?limit=10` against a thousand-partition catalog cost one
partition rather than being refused for naming a thousand.

**Over a limit is `413`, and the body is the plan.** So the answer to "that is more than I
will do at once" is the list of requests that would do it.

The three bind a catalog reached by its own url in file-server mode too, where a `413`
carries the sentence rather than the plan — a url has no way to express a fan-out, which is
also what `max_query_radius_arcsec` is about.

### Planning instead of running

`POST /api/v1/{expr,simple}/hats/plan` takes the same body and resolves it without reading
a row:

```json
{
  "catalog": "s3://survey-data/catalog",
  "num_partitions": 3,
  "estimated_bytes": 1140850688,
  "requires_credentials": true,
  "requests": [
    {
      "order": 3, "pixel": 264,
      "method": "POST",
      "path": "/api/v1/simple/parquet",
      "estimated_bytes": 380375000,
      "body": {
        "url": "s3://survey-data/catalog/dataset/Norder=3/Dir=0/Npix=264.parquet",
        "columns": "source_id, mag",
        "filters": "mag < 18",
        "region": [{ "type": "circle", "ra": 348.05, "dec": -29.28, "radius_deg": 3.0 }],
        "ra_column": "source_ra",
        "dec_column": "source_dec",
        "healpix_column": "_healpix_29",
        "healpix_order": 29
      }
    }
  ]
}
```

With `"return_storage": true` in the request, each `body` also carries the `storage` object
that request was sent with.

Each entry is a request against this service, written in the vocabulary the plan was asked
for in and naming that vocabulary's own `parquet` route in `path` — so an entry can be sent
back exactly as it stands. The client sends them with its own concurrency and retries and
concatenates the answers **in the order given**, which is the same rows the `hats` route
would have returned. A `limit` is carried on each entry, so the client takes the first
`limit` rows of the concatenation.

The column names the `hats` route refuses are written into each entry, because the
single-file route has no catalog to ask.

An entry carries `region` only where the region does not contain that partition whole; one
without it is a partition every row of which qualifies.

`estimated_bytes` is the size of the **whole** partition file, not what the query will
fetch — a projection with the predicate pruned reads a small fraction of it. What it is
good for is deciding how much to run at once. It comes from `_metadata` and is absent for a
catalog whose partitions were found any other way, omitted rather than guessed, at the top
too: a sum over only the entries that knew would read as a total.

**Credentials are not echoed unless asked for.** By default the entries carry the stripped
url and `requires_credentials` says whether the original request had any, so the client
re-attaches what it already holds.

Set `"return_storage": true` and each entry gains the request's own `storage`, credentials
included, ready to send as it stands. It is your secret coming back to you in a response to
your own request, so it discloses nothing — but the plan is then a document with a
credential in it, and plans get logged, cached and pasted into issues. Hence off by
default. Asking for it with no `storage` in the request writes no field at all, and the
rows route refuses the flag outright, having no plan to put it in.

Nothing bounds this route: the point of it is to answer a request too large to run.

The catalog's own files are read first: `hats.properties` or `properties`, then
`partition_info.csv`, `dataset/_metadata` or a listing of `dataset/`, whichever answers
first. Nothing is cached between requests yet, so that is two extra `GET`s per query.

A catalog whose partitions are directories of files (`hats_npix_suffix = "/"`) needs a
listing to read one, so such a catalog cannot be served over `http(s)://` — the names
inside a partition appear in none of its metadata.

### Selecting a region of the sky

Three shapes: `circle`, `box` and `moc`.

`region` is a structured field rather than part of `where`. It is always an array, and
**the array is a union**: a row inside any of its shapes qualifies. The whole field is
then `AND`ed with `where`.

`ra_column` and `dec_column` say which columns of the file hold the position, and are
required alongside `region` — except where every shape in it is a `moc`, which is cells and
reads no position. They resolve the same way any column name does: the file's own spelling,
or that spelling in lowercase.

**A file with a `_healpix_29` column is accelerated without being asked.** That is the one
column name that carries its own order, so it is the one that can be recognised: if the
schema has exactly one column called `_healpix_29`, of an integer type wide enough for an
order-29 cell, it is used. Nothing else is guessed at — any other index column is at some
order the name does not say, and reading it at the wrong one returns no rows.

`healpix_column` and `healpix_order` name a HEALPix cell column that is called something
else, or written at another order. They are optional, they travel together, and naming one
the file has not got is an error — unlike the discovered column, whose absence is simply a
file with no index. They change what a query costs rather than what it returns: the region is
covered by HEALPix cells, so a row whose cell the region does not reach is dropped without
the trigonometry, and one whose cell the region covers wholly is kept without it. Where the
file is sorted by that column — which HATS catalogs usually are and no file has to be —
those bounds also skip whole row groups, and `data_bytes_read` is where that shows.

Both are needed together because neither is fixed: HATS *recommends* the name `_healpix_29`
and recommends nothing about it beyond that, so a catalog may call its column anything and
write it at any order — and the order is what says which cell a value is. A column named
with the wrong order returns no rows rather than an error, which is why only the one name
that states its order is ever assumed.

```json
{
  "url": "s3://survey-data/catalog/dataset/Norder=1/Dir=0/Npix=44.parquet",
  "region": [{ "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_arcsec": 10 }],
  "ra_column": "ra",
  "dec_column": "dec",
  "healpix_column": "_healpix_29",
  "healpix_order": 29
}
```

Degrees throughout, and both ends of every range inclusive.

| `type` | fields |
|---|---|
| `circle` | `ra`, `dec`, and exactly one of `radius_deg` or `radius_arcsec` — the cone search, under ADQL's name for it |
| `box` | `ra: [from, to]`, `dec: [from, to]` |
| `moc` | exactly one of `ascii` or `json` — an IVOA MOC given directly |

```json
"region": [
  { "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_arcsec": 36 },
  { "type": "box", "ra": [349.5, 10.5], "dec": [-20, -10] },
  { "type": "moc", "ascii": "3/3 10 4/16-18 22" }
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

#### `moc`

A Multi-Order Coverage map, in either of IVOA's two text serializations — exactly one of:

```json
{ "type": "moc", "ascii": "3/3 10 4/16-18 22" }
{ "type": "moc", "json": { "3": [3, 10], "4": [16, 17, 18, 22] } }
```

These are what `mocpy`'s `serialize(format="str")` and `serialize(format="json")` write.
FITS is not accepted: it is binary, so it would have to arrive base64-encoded, which is
neither of the two things a caller already has. There is no `url` — fetching a MOC the
caller names is a request this service would make on their behalf, and that needs the
endpoint and network rules deciding it.

**It is used at the depth you wrote it at.** Every other shape is approximated by cells and
this service picks how finely; a MOC *is* cells, so re-covering it could only move the
answer. That also makes it the one exact shape — the inner and outer coverings are the same
set — so no row reaches any trigonometry.

**It needs a HEALPix column**, and is refused without one. The cells are the whole of the
test, so unlike every other shape there is no geometry to fall back on, and answering with
no rows would be indistinguishable from a MOC that holds none. For a catalog the column
comes from `hats_col_healpix`; for a single file, name `healpix_column` and `healpix_order`.

Since it reads no position, `ra_column` and `dec_column` are not required alongside a
region whose shapes are all `moc`.

A MOC that names no cells at all is refused rather than answered with nothing.

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
  scheme off, or a list for exactly those. There is no section for local files: a
  `[[mount]]` is the whole of what makes one readable.
- **`[api.access.network]`** decides which addresses may be reached, whatever backend the
  request goes through. Loopback, private ranges — link-local included, where a cloud
  instance serves this machine's own IAM credentials — and network-internal names are all
  refused by default.

A name is judged before it is resolved, and every address it resolves to is judged again
inside the HTTP client's own resolver. Redirects are not followed.

A `file://` url names a mount's `path`, not a place on the disk. A path under no mount is
refused whether or not anything is there.

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
pre-commit run --all-files    # cargo fmt, clippy -D warnings, the tests, and biome
```

The listing page's script and stylesheet are formatted and linted by
[Biome](https://biomejs.dev), configured in `biome.jsonc`.

`CLAUDE.md` is the conventions this codebase is held to.
