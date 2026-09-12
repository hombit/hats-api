# hats-api

Query HATS catalogs over HTTP.

A query narrows a catalog three ways:
- A **region** of the sky: a circle, a box, or a MOC.
- A **row predicate** written as a SQL expression.
- And the **columns** you want.

The answer is rows, as JSON, parquet or a VOTable. A query too large to run at once comes
back as a **plan** instead: the same work split into one request per partition, for the
client to fan out itself.

The catalog can sit on local disk or in S3, GCS, Azure Blob, WebDAV, or behind a plain
HTTP server. A single parquet file is queryable the same way.
Two interfaces, either or both:

- **[File-server mode](#file-server-mode)** publishes a local directory. Without a query
  string it is an ordinary static file server; with one, the catalog's URL is the
  query: `?ra=348&dec=-29&radius_arcsec=30&columns=source_id,mag&filters=mag<18`.
- **[API mode](#api-mode)** takes the location in the request body, so each request names
  its own catalog instead of one this server publishes.

## Running it

```
docker run -p 8080:80 ghcr.io/hombit/hats-api
```

With no configuration that is API mode on
`http://localhost:8080/api/v1`, reading any S3, GCS, Azure, WebDAV or HTTPS store on the
public internet. Publishing a local directory, or narrowing what may be reached, is
[Configuration](#configuration).

From source, with a [Rust toolchain](https://rustup.rs):

```
cargo build --release
./target/release/hats-api --config hats-api.toml
```

## File-server mode

Local directories are **mounted** onto http paths, much as a filesystem is mounted onto a
directory, e.g. `/mnt/data/gaia` on disk, mounted at `/gaia`, is served from
`https://example.com/gaia`.

With no query string a mount is an ordinary static file server. Add parameters and the
response is rows rather than bytes:

```
GET /gaia/dataset/Norder=1/Dir=0/Npix=44.parquet     # the file, as stored
GET /gaia/dataset/Norder=1/Dir=0/                    # a listing of that directory
GET /gaia?ra=348.077&dec=-29.339&radius_arcsec=30    # rows from the whole catalog
GET /gaia/…/Npix=44.parquet?columns=source_id        # rows from that one file
```

### Querying a catalog

Put a query string on the catalog's URL. The service picks the partitions and reads
the position columns out of the catalog's metadata, so the request is only what to narrow
it by:

```
GET /gaia?limit=10
GET /gaia?ra=348.077&dec=-29.339&radius_arcsec=30&columns=source_id,phot_g_mean_mag
```

| parameter | means |
| --- | --- |
| `columns` | comma-separated column names. Absent returns every column. |
| `filters` | one row predicate; `&&` spells `AND`. Absent returns every row. |
| `limit` | most rows to return. |
| `format` | `parquet` (the default here), `json`, or `votable`. |
| `ra`, `dec` | the centre of a cone, in degrees. |
| `radius_arcsec`, `radius_deg` | its radius; exactly one of the two. |
| `ra_column`, `dec_column` | which columns hold the position. Refused against a catalog, which names its own; required with a cone against a parquet file. |

A cone is the only shape a URL takes; the API's [`region`](#selecting-a-region-of-the-sky)
also has a box and a MOC.

The same from Python:

```python
requests.get(
    "https://example.com/gaia",
    params={"columns": "source_id,phot_g_mean_mag", "filters": "phot_g_mean_mag < 18",
            "ra": 348.077, "dec": -29.339, "radius_arcsec": 30},
)
```

The answer is the API catalog route's, with the partition count in
`x-hats-num-partitions`. Rows come back in the catalog's own order, which is usually the
[HEALPix](https://irsa.ipac.caltech.edu/healpix/index.html) order, so a `limit` is the
"front" of the catalog.

**The radius is capped**, at 600″ by default. A wide cone reaches a large part of the
catalog, and this service will not build an answer that size and send it in one response.
A search that large belongs to the [plan route](#a-plan-instead-of-the-rows).

### Querying one file

The parameters above work on a data file's URL too, whether it is a HATS catalog's
partition or a parquet file standing on its own:

```
GET /gaia/dataset/Norder=1/Dir=0/Npix=44.parquet?columns=source_id,ra,dec&limit=100
```

The response is a parquet file, with the row count, the bytes read out of the source file
and the timing in `x-hats-num-rows`, `x-hats-data-bytes-read` and `x-hats-elapsed-ms`.

Listing the columns you need, e.g. `columns=source_id,ra,dec`, is what makes a query cheap.
Parquet is fetched a column chunk at a time, so even a small `limit` reads every chunk
holding a row it returns: on a 335 MiB, 153-column Gaia partition, ten rows of everything
read 335 MiB, and ten rows of one column read 3.8 MiB.

### Directory listings

A request for a directory gets its own `index.html` if it has one, and otherwise a listing
of every entry ordered by name. Which form it takes depends on `Accept`: a browser asks
for `text/html` and gets a page, while most of the API clients send `*/*` by default and
get a JSON.

```json
{
  "path": "/gaia/dataset/Norder=1/Dir=0/",
  "parent": "/gaia/dataset/Norder=1/",
  "entries": [
    { "name": "Npix=44.parquet", "type": "file", "size": 4006,
      "modified": "2026-01-27T22:26:43Z", "url": "/gaia/dataset/Norder=1/Dir=0/Npix=44.parquet" }
  ]
}
```

### The page

A browser gets a page for a directory, and a file this service reads as data gets a query
panel on it: the columns are fetched from the file when the panel opens, and the query
runs against the same URL a client would write by hand.

A directory that is a catalog, or inside one (`dataset/Norder=5/Dir=0` included), gets the
query over the whole catalog above it: its name, what it says about its own size, its
columns from `dataset/_common_metadata`, the request written out for `curl` and for the
Python readers, and a **Plan** button. Preview asks for ten rows and the circle is
optional, so there are rows on screen the moment the page opens.

The markup is complete without the script. With it blocked, the listing and its links
still work, and what the page says about querying it says as a URL.

## API mode

A running server documents itself. `GET /api/v1/docs` is a reference page for every route,
with each request shown as a body you can edit and send from the page, and
`GET /api/v1/openapi.json` is the same thing as an OpenAPI document for generating a
client from. Both are built from the types the routes deserialize, so they describe the
server you are actually talking to.

A query has one required field, `url`, the resource to query: a parquet file for the
`parquet` routes, a HATS catalog or collection for the `hats` ones. Everything else in the
body narrows what comes back.

An error is a status code and a one-field body, `{"error": "…"}`.

### The routes

Every route is a pair: which kind of **target** it takes, and which **vocabulary** the
body is written in. Both go in the path, under `[api] prefix`, `/api/v1` by default.

| target | in `expr` | in `simple` |
|---|---|---|
| one parquet file | `POST /api/v1/expr/parquet` | `POST /api/v1/simple/parquet` |
| a whole catalog | `POST /api/v1/expr/hats` | `POST /api/v1/simple/hats` |
| a catalog query, resolved but not run | `POST /api/v1/expr/hats/plan` | `POST /api/v1/simple/hats/plan` |

Plus `GET /api/v1/health`.

The `hats` routes pick the partitions the query touches; `hats/plan` takes the same body
and returns the requests that query would take.

The two vocabularies differ in how the projection and the predicate are spelled:

| vocabulary | projection | predicate                                    |
|---|---|----------------------------------------------|
| `expr` | `select`: a SQL select list, so `mag - 0.1 AS mag_corr` works | `where`: single boolean SQL expression       |
| `simple` | `columns`: a list of names | `filters`: one row condition, in the same language as `where` |

Think of an `expr` query as `SELECT {select} FROM {url} WHERE {where}`: you write the two
fields, and `url` is the table. `simple` narrows the projection to a list of names — nothing
computed and no aliases — and keeps the same expression language for the condition. It is
also the pair a URL query string carries, which is why file-server mode reads it out of one;
there the names are one parameter separated by commas, a url having nowhere to put a list.

The rest of the body is the same in both vocabularies:

| field | means                                                                                                         |
|---|---------------------------------------------------------------------------------------------------------------|
| `url` | the resource to query. The only required field                                                                |
| `region` | [a shape on the sky](#selecting-a-region-of-the-sky): a circle, a box or a MOC                                |
| `ra_column`, `dec_column` | which columns hold the position. Required with a `region` for `/api/v1/*/parquet`, not for HATS, which names its own |
| `healpix_column`, `healpix_order` | [a HEALPix index column](#the-healpix-column), if the parquet file has one                                    |
| `limit` | most rows to return                                                                                           |
| `format` | [`json`](#the-three-formats), the default here, `parquet` or `votable`                                        |
| `storage` | [how to reach the store](#storage-options): endpoint, credentials, headers                                    |
| `return_storage` | write this request's own `storage` into each plan entry. `/api/v1/hats/plan` only                             |

### A whole catalog

One query across a whole HATS catalog. The service reads the catalog's metadata, works out
which partitions the region falls in, and reads those alone.

Gaia DR3 is public and readable anonymously, so this one runs as it stands:

```
curl -s https://example.com/api/v1/expr/hats -H 'content-type: application/json' -d '
{
  "url": "s3://stpubdata/gaia/gaia_dr3/public/hats",
  "select": "source_id, ra, dec, 1000 / parallax AS dist_pc",
  "where": "parallax > 1 AND phot_bp_rp_excess_factor < 1.3 + 0.06 * bp_rp * bp_rp",
  "region": [{ "type": "circle", "ra": 30.0, "dec": 5.0, "radius_arcsec": 300 }],
  "limit": 2
}'
```

Stars within a kiloparsec, with the usual BP/RP excess cut. `select` does arithmetic and
names the result, so `dist_pc` is a new column. `where` takes any expression, so a cut can
use columns on both sides.

```json
{
  "num_rows": 2,
  "num_partitions": 1,
  "schema": [
    { "name": "source_id", "type": "Int64" },
    { "name": "ra", "type": "Float64" },
    { "name": "dec", "type": "Float64" },
    { "name": "dist_pc", "type": "Float64" }
  ],
  "data_bytes_read": 10161744,
  "elapsed_ms": 2129,
  "rows": [
    { "source_id": 2518878678495119104, "ra": 29.974869483196265,
      "dec": 4.929369328907767, "dist_pc": 275.82239199631914 },
    { "source_id": 2518878717150249728, "ra": 29.98507241901302,
      "dec": 4.938764009556236, "dist_pc": 556.812777041018 }
  ]
}
```

`num_partitions` is how many of the catalog's partitions were read. Next to
`data_bytes_read` it is what says the region pruned: a cone touching three partitions of a
hundred thousand reads three.

**Rows come back in the catalog's order**, partition by partition. Within one partition
the order is not specified.

**Omitting `region` reads the whole catalog**, which is what
[the bounds in the configuration](#what-a-request-may-spend) are for.

### A plan instead of the rows

`…/hats/plan` takes the same body and resolves it without reading a row.

A fifteen-degree cone over Gaia DR3, too wide to run in one answer:

```
curl -s https://example.com/api/v1/simple/hats/plan -H 'content-type: application/json' -d '
{
  "url": "s3://stpubdata/gaia/gaia_dr3/public/hats",
  "columns": ["source_id", "ra", "dec"],
  "filters": "parallax > 1",
  "region": [{ "type": "circle", "ra": 30.0, "dec": 5.0, "radius_deg": 15.0 }],
  "limit": 1000
}'
```

```json
{
  "catalog": "s3://stpubdata/gaia/gaia_dr3/public/hats",
  "num_partitions": 7,
  "requires_credentials": false,
  "requests": [
    {
      "order": 2, "pixel": 0,
      "method": "POST",
      "path": "/api/v1/simple/parquet",
      "body": {
        "url": "s3://stpubdata/gaia/gaia_dr3/public/hats/gaia/dataset/Norder=2/Dir=0/Npix=0.parquet",
        "columns": ["source_id", "ra", "dec"],
        "filters": "parallax > 1",
        "region": [{ "type": "circle", "ra": 30.0, "dec": 5.0, "radius_deg": 15.0 }],
        "ra_column": "ra",
        "dec_column": "dec",
        "limit": 1000
      }
    },
    "… Npix=2, 68, 69, 70, 71, 143"
  ]
}
```

`ra_column` and `dec_column` are filled in from the catalog, since the single-file route
those entries go to has no catalog to ask.

Each entry is a request against this service, ready to send as it stands. A `limit`
is carried on every entry, so the client would need to aggregate them, e.g. take
`limit` of the concatenation of all the partitions.

`estimated_bytes` is the whole partition file, not what the query will fetch, so read it
as how much there is to get through rather than as a cost. It may be absent and not
guaranteed to be accurate.

By default, an entry carries the stripped URL, and `requires_credentials` says whether the
original request had any, so the client re-attaches what it already holds.
`"return_storage": true` writes this request's own `storage` into every entry, credentials
included, which makes the plan ready to go, but may lead to credential leakage on the
middleware or on the client.

### One parquet file

`…/parquet` takes the same body with `url` naming one file, a HATS partition or a
file standing on its own. There is no catalog to ask which columns hold a position, so a
`region` here comes with `ra_column` and `dec_column`, and `healpix_column` and
`healpix_order` are available where the file has such a column.

One partition of ZTF DR24, in the `simple` vocabulary, reaching into a nested column:

```
curl -s https://example.com/api/v1/simple/parquet -H 'content-type: application/json' -d '
{
  "url": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ztf_dr24_lc-hats/dataset/Norder=6/Dir=30000/Npix=34623/part0.snappy.parquet",
  "columns": ["objectid", "objra", "objdec", "lightcurve.mag"],
  "filters": "nepochs > 10",
  "limit": 2
}'
```

```json
{
  "num_rows": 2,
  "schema": [
    { "name": "objectid", "type": "Int64" },
    { "name": "objra", "type": "Float32" },
    { "name": "objdec", "type": "Float32" },
    { "name": "lightcurve", "type": "Struct(\"mag\": List(Float32, field: 'element'))",
      "fields": [{ "name": "mag", "type": "List(Float32, field: 'element')" }] }
  ],
  "data_bytes_read": 77991,
  "elapsed_ms": 418,
  "rows": [
    { "objectid": 1248202100005682, "objra": 67.294304, "objdec": -30.673319,
      "lightcurve": { "mag": [15.843088, 15.964036, 15.909784, "…"] } },
    "…"
  ]
}
```

`lightcurve.mag` asked for one field of a struct column, and it comes back as
`lightcurve` holding that field. The answer is the catalog answer without
`num_partitions`, and rows arrive in no particular order.

### What comes back

A JSON answer that returns rows always has the same fields: `num_rows`, `schema`,
`data_bytes_read`, `elapsed_ms` and `rows`, with `num_partitions` added for a catalog. A
`parquet` or `votable` answer is the rows alone, and carries those counts in the
`x-hats-*` headers instead.

Some HATS catalogs pack a whole light curve into a single struct column. Ask for part of
it with a dotted name, `sources.mjd`, and the `sources` column comes back holding the
fields you named, the way `pyarrow` reads a subset of a struct:

```
columns: ["id", "sources.mjd", "sources.mag"]  ->  id, sources{mjd, mag}
columns: ["sources"]                           ->  sources{every field}
columns: ["sources", "sources.mjd"]            ->  sources{every field}
select:  "sources.mjd AS mjd"                  ->  mjd   (an alias is your own column)
select:  "get_field(sources,'mjd') AS m"       ->  m     (an expression, not part of sources)
```

In a url the same names are one parameter separated by commas —
`?columns=id,sources.mjd,sources.mag` — which is the only difference between the two
carriers.

Fields come back in the order you named them, and the column keeps the place where you
first named it.

Quote each part on its own where it needs quoting: `"sources"."mjd"` names the field,
while `"sources.mjd"` names a column no file has got.

In `schema`, a struct column lists its own fields one level down, and a scalar column has
no `fields` key:

```json
{ "name": "sources", "type": "Struct(...)",
  "fields": [{ "name": "mjd", "type": "List(Float64)" },
             { "name": "mag", "type": "List(Float64)" }] }
```

#### The three formats

`format` is a body field in API mode and a query parameter in file-server mode, defaulting
to `json` and to `parquet` respectively. Anything but `json` carries its counts in the
`x-hats-*` headers, there being no room in the body.

**`json`.** Every row carries every column, a null included, so a row's keys are the
answer's columns. `NaN`, `Infinity` and `-Infinity` come back as those three strings,
which `float()` in Python and `Number()` in JavaScript both read back.

**`parquet`.** Laid out like the file it came from, with the same codec, encodings,
statistics and bloom filters per column, so the answer round-trips through anything that
reads the original. It carries the values above as themselves.

**`votable`.** A VOTable 1.4 document, `TABLEDATA` serialized, with `NaN`, `+Inf` and
`-Inf` written as themselves. Flat columns only: selection of a nested column fails
the request.

### Selecting a region of the sky

`region` is a list of shapes. A row comes back when it falls inside **any** of them and
also matches the predicate. `ra` and `dec` are in degrees; a circle radius names its unit
in the field name: `radius_deg` or `radius_arcsec`.

| `type` | fields |
|---|---|
| `circle` | `ra`, `dec`, and exactly one of `radius_deg` or `radius_arcsec` |
| `box` | `ra: [from, to]`, `dec: [from, to]` |
| `moc` | an IVOA MOC, in exactly one of `ascii` or `json` |

```json
"region": [
  { "type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_arcsec": 36 },
  { "type": "box", "ra": [349.5, 10.5], "dec": [-20, -10] },
  { "type": "moc", "ascii": "3/3 10 4/16-18 22" }
]
```

`ra_column` and `dec_column` name the position columns, which a `circle` or a `box` is
tested against; a `moc` is tested against the [HEALPix column](#the-healpix-column)
instead. So the pair is required with a `region`, unless every shape in it is a `moc`.
Either right ascension convention works, 0 to 360 or -180 to 180.

A `box` is two inclusive ranges. `ra` runs eastward from the first value to the second, so
`[350, 10]` is twenty degrees across the origin and `[10, 350]` is the three hundred and
forty the other way; `[0, 360]` is every right ascension, and the two values naming the
same point is refused. `dec` is ordered, so its first value may not be the greater one.

A `moc` is a Multi-Order Coverage map in either of IVOA's two text serializations,
`{"ascii": "3/3 10 4/16-18 22"}` or `{"json": {"3": [3, 10], "4": [16, 17, 18, 22]}}`,
which are what `mocpy`'s `serialize(format="str")` and `serialize(format="json")` write.

#### The HEALPix column

A `moc` is tested against this column, so it is required there. For a `circle` or a `box`
it is an accelerator: it changes what a query costs and never which rows come back.

`_healpix_29` is used automatically when a file has it. Any other column is named by
`healpix_column` and
`healpix_order` together, on the single-file route only; a catalog's comes from its
`hats_col_healpix`. Naming a column the file has not got is an error, and giving the wrong
order returns no rows.

### The SQL

`select` and `where` are planned against the file's own schema, so
`source_id = 1383212200036217` is an `Int64` compared against row-group statistics, the
page index and a bloom filter.

A column answers to its own name, `Gmag`, `objectId`, whatever the file spells it, and to
that name in lowercase, which is what unquoted SQL means by a name.

Functions are judged by volatility: only immutable ones are callable, so
`now()` and `random()` are refused.

### Storage options

`storage` says how to reach the store; the URL says which object. Leave it out for a
public object, which is read anonymously. No ambient credential of the deployment's own is
ever used to answer a request, and a `file://` URL takes no storage options at all.

| option | scheme | means |
| --- | --- | --- |
| `endpoint` | `s3`, `gs`, `az` | a server other than the provider's own: MinIO, Ceph, R2, Azurite |
| `region` | `s3` | the bucket's region |
| `access_key_id`, `secret_access_key`, `session_token` | `s3` | an AWS credential |
| `service_account_key`, `access_token` | `gs` | the JSON Google issues, base64-encoded, or an OAuth2 token |
| `account`, `access_key`, `sas_token` | `az` | `account` is required, the URL carrying only the container |
| `headers` | `http`, `https` | a bearer token or an API key for a server that authenticates |
| `transport`, `username`, `password` | `webdav` | `https` (default) or `http`, and a Basic credential |
| `allow_http` | any remote | permission to send a credential to a cleartext endpoint |

Schemes: `s3`, `gs`, `az`, `http`, `https`, `webdav`, `file`. An option a scheme has no
use for is refused rather than ignored. Credentials are never logged, neither their values
nor their names. A `file://` URL names a mount's `path`, and a path under no mount is
refused whether or not anything is there.

## What a request may spend

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
enough rows are in, so the limit bounds the request and `max_partitions` joins the
counters, watched as the reads land. That is what makes `?limit=10` against a
thousand-partition catalog cost one partition.

**Over a limit is `413`, and the body is the plan.** So the answer to "that is more than I
will do at once" is the list of requests that would do it. The same three bind a catalog
reached by its own URL in file-server mode, where a `413` carries the sentence alone. A
URL has no way to express a fan-out, which is also what `max_query_radius_arcsec` is about.

### Servers that ignore `Range`

A parquet read is tens of ranged requests, and a plain HTTP server may answer one with the
whole object and a `200`. Any backend whose host comes from the request is probed per
object, and an object on a server that will not serve ranges is copied to scratch once and
read from local disk after that. `[limits]` bounds that per object, in total, and in flight
at once.

## Configuration

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

With no file the API answers on `/api/v1` and the file server publishes nothing, there
being no mount to publish. [`hats-api.example.toml`](hats-api.example.toml) writes out
every key; the shortest useful file is one mount:

```toml
[server]
address = "127.0.0.1"
port = 8080

[api]
enabled = false

[[mount]]
path = "/gaia"
source = "/mnt/data/gaia"
serve = true
```

### Mounts

```toml
[[mount]]
path = "/gaia"               # the address, in both modes
source = "/mnt/data/gaia"    # where it actually is, which no caller sees
serve = true                 # publish it as a directory; off is API-only
follow_symlinks = false
immutable = false
filenames = ["*.parquet"]    # in place of [data] filenames, for this mount
```

`path` is the address in both modes. The file server publishes the directory there, and an
API request names a file in it by the same path, `file:///gaia/dataset/…`, never the
`/mnt/data/gaia` it lives in.

`serve` publishes the directory. With it off the mount is not served and not listed, and a
request for any path under it is a 404, while an API request naming a file in it is
answered as usual. Two mounts may not claim overlapping URL prefixes, served or not.

### Which files are data

One list of filename globs, matched against a file's own name and never against the path
above it:

```toml
[data]
filenames = ["*.parq", "*.parquet", "*.pq", "_metadata", "_common_metadata"]
```

Anything off the list, `properties` and `partition_info.csv` and an `index.html` among
them, is served verbatim, query string and all, and a directory takes no parameters
either. A mount's own `filenames` replaces this for the files under it.

### Bounds

```toml
[limits]
max_partitions = 16             # what one catalog query may spend
max_bytes_fetched = "10GiB"
max_rows = 1000000
max_concurrent_partitions = 4   # a performance setting, not a bound
max_query_radius_arcsec = 600   # file-server mode only; 0 closes the circle surface
max_materialize_bytes = "2GiB"  # copying an object off a server that ignores Range
```

`[limits]` also caps the catalog metadata fetched, the scratch copies resident and in
flight at once, and how much SQL one request may carry. The example file writes all of
them out with what each is for.

### What a request may reach

Two independent sets of rules, and both apply:

- **`[api.access.<backend>]`** decides which endpoint a request may name. Three states per
  backend: no `endpoints` key at all for any endpoint, an empty list to turn the scheme
  off, or a list for exactly those. There is no section for local files, a `[[mount]]`
  being the whole of what makes one readable.
- **`[api.access.network]`** decides which addresses may be reached, whatever backend the
  request goes through. Loopback, private ranges (link-local included, where a cloud
  instance serves this machine's own IAM credentials) and network-internal names are all
  refused by default.

A name is judged before it is resolved, and every address it resolves to is judged again
inside the HTTP client's own resolver. Redirects are not followed.

### In a container

The image listens on port 80 and holds the binary and a set of CA roots. A config file
reaches it as a bind mount, and so does any directory a `[[mount]]` names, at the `source`
it names:

```
docker run -p 8080:80 \
  -v ./hats-api.toml:/etc/hats-api.toml:ro -e HATS_API_CONFIG=/etc/hats-api.toml \
  -v /mnt/data/gaia:/mnt/data/gaia:ro \
  ghcr.io/hombit/hats-api
```

The image carries no `HEALTHCHECK`, the port and the API prefix both being configuration.
Probe `GET {api.prefix}/health`.

## Development

`cargo test` passes with no network, no Docker and no credentials. Anything that needs a
real server is a separate test binary that skips when its environment variables are absent.

```
pre-commit run --all-files    # cargo fmt, clippy -D warnings, the tests, and biome
```

The listing page's script and stylesheet are formatted and linted by
[Biome](https://biomejs.dev), configured in `biome.jsonc`.

`CLAUDE.md` is the conventions this codebase is held to.
