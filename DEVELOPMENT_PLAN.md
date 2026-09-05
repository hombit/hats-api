# Development plan

## Progress tracker

**This tracker is part of the plan. Whoever implements a step updates it in the same
commit as the code.** A step is `done` only when its tests pass, `cargo clippy
--all-targets` and `cargo fmt --check` are clean, and the invariants in §0 still hold.

**This document is a plan, not a log of the work.** It says what is still to be done and
what constrains it. Nothing here describes what was implemented, in what order, or by
whom — the code, the tests and `git log` already say that, and better. So when a step is
finished, do not write down what you built; change only what a later step now has to do
differently:

- a question the step settled, where the answer decides something later — a version, a
  limit, a measured number, a constraint every following backend inherits;
- a decision that turned out differently from what this document assumed, with the
  assumption corrected rather than annotated;
- work the step revealed but did not do, entered as its own row rather than a note.

If a step changed nothing about what comes next, its row moving to `done` is the whole
update. A note that could begin "we added" belongs in the commit message.

**Nothing outside this file may refer to it.** No section number, no filename, no "see
the development plan" — not in comments, doc comments, test names, config files or
workflows. This document is scaffolding and will be deleted once the work in it is
done; a reference to `§8.1` in a comment becomes a dangling pointer the moment that
happens, and the reader has no way to recover what it meant. When code needs a reason,
the comment states the reason. If that makes a comment longer, the comment was
depending on this file to finish its sentence.

Status values: `todo`, `in progress`, `done`, `dropped` (with the reason).

| § | step | status | notes |
|---|---|---|---|
| 2.1 | OpenDAL backend layer, s3 migrated first | done | `object_store_opendal` 0.58.0 → `object_store ^0.13.1`, matching DataFusion 55's 0.13.2, one copy in the lock file. The §2.1 gate is met; the phase is not blocked. See the notes below. |
| 2.2 | GCS and Azure | todo | |
| 8.3 | network policy | todo | prerequisite for §2.3, per §8.3 |
| 2.3 | HTTP/HTTPS, range probe, materialization | todo | needs §8.3 |
| 2.4 | WebDAV | todo | needs §8.3 |
| 2.5 | Hugging Face | todo | droppable; §2.1's gate was met, so no reason to drop it yet |
| 3.1 | two-mode configuration | todo | |
| 3.2 | routing | todo | |
| 3.3 | API request shape (`select`/`where`/`region`) | todo | needs DataFusion's `sql` feature. Only the query language is left: `POST` and the `storage` object are done |
| 3.4 | file-server request shape | todo | needs `docs/vizcat-compat.md` written from the live service first |
| 4 | file-server interface | todo | |
| 5.1 | HATS catalog metadata | todo | |
| 5.2 | spatial predicate | todo | order policy and range budget to be settled by measurement first |
| 5.3 | sync / plan / auto | todo | |
| 6.8 | request cost benchmark | todo | prerequisite for the rest of §6 — it ranks the layers |
| 6.1–6.7 | caching | todo | build in the order §6.8 ranks |
| 7 | operational surface | todo | |

Constraints §2.1 discovered that bind every backend after it:

- **Every remote backend must call `storage::install_http_transport` before building.**
  OpenDAL 0.58 has no HTTP client until one is installed process-wide, and a store
  built without it builds fine and fails on its first request — a failure no test that
  stops short of the wire will catch.
- **Ambient credential discovery is disabled per store, not globally.** s3 uses
  `disable_config_load` and `disable_ec2_metadata`; §2.2's GCS and Azure builders need
  their own equivalents, and §8.1 is not satisfied until each has one.
- **Addressing is per backend, not global**: path-style for a named S3-compatible
  endpoint, virtual-host for AWS itself.
- **`allow_http` stays ours.** OpenDAL follows the endpoint's own scheme without
  asking, so the cleartext decision has no backend half to defer to.
- **`file` stays on `object_store`'s `LocalFileSystem`.** It has no options and no
  credentials, so it is not the second option-and-credential surface §2.1 was avoiding.
  Revisit if §3.1's mounts want OpenDAL's listing.
- **OpenDAL's retry, timeout and concurrent-limit layers are available but unapplied** —
  enabling a Cargo feature only makes a layer constructible. §7.1 and §8.4 wire them.
- **`object_store` is now trait-only**: no `aws` feature, so it supplies the
  `ObjectStore` trait DataFusion consumes and `LocalFileSystem`, nothing else. Adding a
  backend means adding it to OpenDAL's side, never re-enabling one here.
- **`disable_config_load` is untested.** `skip_signature` is what makes an anonymous
  request unsigned, and that is covered; the config guard's other job — stopping
  `AWS_ENDPOINT_URL` from redirecting a request — is not observable through a url with
  no `endpoint` option, because virtual-host addressing turns a redirected endpoint
  into `bucket.<host>`, which does not resolve. Each new backend's equivalent guard
  inherits the same blind spot.

Written against `781ceac`: a stateless service with one endpoint, `GET /api/v1/select`,
doing point lookups in a single parquet file over `s3://` or `file://`, under an access
policy in a TOML config file.

§1 is the target shape, §2–§7 the phases in order, §8 the conditions every phase must
keep, §9 what is deferred.

## 0. Where we are

| piece | state |
|---|---|
| `src/access.rs` | endpoint- and directory-level policy; s3 + local |
| `src/storage.rs` | url → `ObjectStore`; `s3`, `file`; options passed beside the url |
| `src/query.rs` | DataFusion session per request, `column == value`, projection pushdown |
| `src/parquet_out.rs` | writes the answer with the source file's own layout |
| `src/app.rs` | axum router, `/api/v1/health`, `/api/v1/select` |
| `src/config.rs` | TOML, `deny_unknown_fields`, every key defaulted |
| `src/error.rs` | `ApiError` → status + message; credentials never reach it |
| `src/main.rs` | `--config` / `HATS_API_CONFIG`, logging setup, graceful shutdown |

Baseline to hold: 63 tests, `cargo clippy --all-targets` clean, `cargo fmt` clean.

Three invariants for every phase below:

1. **The service only ever reads.** No endpoint writes, no mount is writable. Mounts
   therefore carry no `readonly` flag. Adding writes is a new document, not a new flag.
2. **Stateless per request.** No session cache, no catalog registry across requests.
   §6's caches must be evictable, bounded, and correct when empty.
3. **Nothing reaches an object store without passing its mode's policy.** Today:
   `AccessPolicy::authorize` in `storage::open`, the only route to a store. §3 adds a
   second mode with its own table, leaving two tables, one resolver, and no route to
   bytes that consults neither.

## 1. The shape we are aiming at

One binary, two interfaces over shared storage and query layers.

```
  POST /api/v1/…      ┌───────────────────────────────────────┐
                      │ API mode: url-addressed               │
                      │  the caller names the location        │
                      │  any scheme, incl. local files        │
                      │  governed by [api.access]  ◄──┐       │
                      └───────────────┬───────────────┼───────┘
                                      │               │ each mount
  GET  /  /hats  …    ┌───────────────┴───────────────┼───────┐
                      │ File-server mode: path-addr.  │ grants│
                      │  the operator named it        │ its   │
                      │  local directories            │ prefix│
                      │  governed by [[mount]] ───────┘       │
                      └───────────────┬───────────────────────┘
                                      │
                      ┌───────────────┴───────────────────────┐
                      │  shared: storage → query → output     │
                      └───────────────────────────────────────┘
```

The modes differ in who names the location:

- **API mode** takes the location from the caller as a `url`, which may carry storage
  options and credentials. `[api.access]` governs where a caller may point the service.
  Every backend is reachable, local files included; local access is denied by default,
  not absent.
- **File-server mode** maps an operator-configured prefix onto a URL path (`/` →
  `/srv/data`, `/hats` → `/data/hats`). The caller never names a store and never supplies
  a credential. Outside a mount is a 404, not a policy refusal. A mount's source is a
  local directory.

The query engine, parquet writer, object stores and path-resolution primitives are one
implementation under both. The request shapes differ (§3.3, §3.4).

Both modes may be enabled at once, and either may be off.

## 2. Phase 1 — more storage backends

Adding a backend is a match arm in `storage::open`, a name in `SUPPORTED_SCHEMES`, an
option table, and a rule kind in `access.rs`. §2.1 settles the plumbing all four share.

### 2.1 The backend layer: OpenDAL

**Decision: OpenDAL for every backend, migrated in one step**, rather than
`object_store`'s own `gcp`/`azure`/`http` features alongside the existing native s3.
Two store-construction paths would mean two option and credential surfaces to strip and
audit, and §8.5's checklist written twice.

This replaces the backend layer, not the interface DataFusion sees: DataFusion consumes
`object_store::ObjectStore` trait objects, and `object_store_opendal` adapts an OpenDAL
`Operator` into that trait. `RemoteFile.store` does not change type.

**The adapter's `object_store` version must match DataFusion's.** A mismatch links two
copies of the crate, whose `ObjectStore` traits are distinct types, so the adapter's output
cannot be registered with DataFusion at all — a type error rather than a version warning.
Pick the adapter release that matches, and treat this as the constraint that can block the
phase.

**Migrate s3 first.** `storage::tests` is the specification for behaviour that already
works — unsigned-when-anonymous, path-style addressing against a custom endpoint,
`allow_http`, region defaulting — and the migration is done when those pass unchanged.
s3 has the most tests, so it answers whether OpenDAL can express current behaviour before
the other three depend on the answer.

If the versions cannot be matched, or a service cannot express what is needed, keep
`object_store` native and drop Hugging Face (§2.5) rather than run both layers.

### 2.2 GCS and Azure

`opendal`'s `services-gcs` and `services-azblob`, following the s3 precedent: credentials
and endpoint arrive beside the url as `StorageOptions`, never inside it, and are held in
types that do not print.

| scheme | options |
|---|---|
| `gs://bucket/key` | `service_account_key`, `service_account_path`, `application_credentials`, `endpoint` |
| `az://container/key`, `abfss://` | `account`, `access_key`, `sas_token`, `bearer_token`, `endpoint`, `use_emulator` |

Policy: `[api.access.gcs]` and `[api.access.azure]`, with the same three-state `endpoints`
key as s3 — absent means any, `[]` means off, a list means exactly those. The provider's
own endpoint is the `"gcp"` / `"azure"` sentinel, as `"aws"` is for s3.

**Anonymous access is the primary case.** Most data served here is public. `storage.rs`
already does this for s3: no credentials means `with_skip_signature(true)`, an unsigned
request. GCS and Azure must match.

**Ambient credential discovery must be explicitly disabled** on all three. GCS and Azure
builders otherwise pick up `GOOGLE_APPLICATION_CREDENTIALS`, `~/.config/gcloud`, workload
identity, the GCE metadata server, `AZURE_*` variables, or the managed-identity endpoint.
A caller sending no credentials would then get the service's own identity and every
private bucket the deployment can reach. No credentials in the url means unsigned. See
§8.1.

### 2.3 HTTP/HTTPS and range requests

`opendal`'s `services-http`: `GET` with `Range`. A plain HTTP server has no listing
operation, so an `http(s)://` catalog has only §5.1's tiers 1 and 2 for partition
discovery, and §4's directory pages cannot be served from one.

This is the first backend where the caller supplies the host, which makes §8.3's network
policy a prerequisite for it rather than a later addition. The same applies to §2.4
and §2.5.

**The problem.** A parquet read is many ranged reads: footer length, footer, page index,
then a chunk per column per surviving row group. Against a server that ignores `Range`
and returns `200` with the whole body, the reader gets the wrong bytes at the footer
offsets; passing the failure through instead would cost *N × filesize* per query, with N
in the tens.

**The fix** is to fetch such an object once and serve every subsequent range from that
copy:

1. **Probe per host.** On the first read of an `http(s)://` url, issue a `HEAD`;
   `Accept-Ranges: bytes` is the affirmation. Cache the verdict per (scheme, host, port)
   with a short TTL.
2. **Verify on the first ranged read.** A `200` where `206` was requested demotes the host
   to non-ranging, catching servers that advertise ranges without honouring them.
3. **Materialize to disk, not memory.** For a non-ranging host, wrap the store in a
   `MaterializingStore`: the first read streams the whole object into a temporary file and
   every `get_range` is a `pread` from it. Partitions run to gigabytes, so an in-memory
   buffer would OOM under concurrency.
4. **`max_materialize_bytes`, default 2 GiB** — multi-GiB partitions are ordinary, so a
   smaller cap would refuse ordinary data. `0` disables materialization.
5. **`max_materialize_total_bytes`** across in-flight materializations, plus a concurrency
   limit on them; over either, the request waits or gets a 503. Clean up temp files on
   every exit path including cancellation. Scratch directory is configurable.
6. **`Content-Length` is not required** — a service generating parquet on the fly (vizcat
   among them) answers chunked with no length. Discover the size in this order:

   | | source | when it works |
   |---|---|---|
   | a | `Content-Range` on a suffix range request (`Range: bytes=-8`) | any range-honouring server; returns the footer tail in the same request |
   | b | `Content-Length` on the `HEAD` | static objects |
   | c | counting bytes while streaming to disk | everything else |

   In case (c) the cap is enforced during transfer: abort and delete the temp file when
   the running count passes `max_materialize_bytes`.

7. **Dynamically generated responses are a separate case.** Their bytes may differ between
   requests, so ranged reads over them are meaningless even when offered. Always
   materialize whole, and **never enter them in the §6 object cache under a url key** —
   the url does not identify the content.

Errors must be distinguishable: 413 for over-cap, saying whether the size was declared or
exceeded while streaming, and 504 for timeout. At these sizes the timeout is reached before
the cap; §7.2 covers that.

Wire `MaterializingStore` to `[cache.object]` with `dir` set (§6.5), so the copy outlives
the request that made it.

Configuration: `[api.access.http]` with `hosts` (three-state), and `allow_plain_http`
distinct from `allow_loopback`.

### 2.4 WebDAV

`opendal`'s `services-webdav`, as `webdav://host/path`. WebDAV is HTTP plus `PROPFIND`,
which is the listing operation §2.3 lacks, so a WebDAV-hosted catalog gets all three of
§5.1's discovery tiers and can be served through §4's directory pages.

| option | meaning |
|---|---|
| `allow_http` | contact the host over plain `http`; `webdav://` is `https` otherwise |
| `username`, `password` | credentials, given together, and subject to §8.1 |

Policy: `[api.access.webdav]` with `hosts`, three-state as elsewhere. Separate from
`[api.access.http]` — a host that may be read as flat objects is not thereby a host whose
directory tree may be enumerated.

Everything in §2.3 applies unchanged: WebDAV is HTTP underneath, so the range probe,
materialization and size ladder are the same code, and §8.3's network policy governs the
host the same way.

### 2.5 Hugging Face

`opendal`'s `services-hf`, as `hf://namespace/name/path`. Lists through the repo tree API,
so §4's directory pages and §5.1's tier 3 work.

Options: `revision` (default `main`), and `token` for gated datasets — a credential,
handled as §8.1 requires.

**Drop this backend** if §2.1's version constraint cannot be met: it has the least
astronomy data behind it and its absence costs nothing structural.

**Deliverable.** `SUPPORTED_SCHEMES = ["s3", "gs", "az", "abfss", "http", "https", "webdav", "hf", "file"]`,
one policy section per backend, a matrix test that every scheme is refused by the default
config and allowed by the narrowest config that should allow it, and `storage::tests`
passing unchanged across the migration.

## 3. Phase 2 — two modes of operation

Separate configuration (§3.1), routing (§3.2) and request shapes (§3.3, §3.4), but one
internal query representation and one execution path. `columns`/`filters` and
`select`/`where` parse into the same thing; a semantic divergence between them is a bug.

### 3.1 Configuration

`[api.access]` governs API mode alone, so that `[api.access.local] paths = []` does not
disable a configured mount, and mounting `/srv/data` does not let a caller pass
`url=/srv/data/x.parquet` to the API.

```toml
[api]                      # API mode: the caller names the location
enabled = true
prefix = "/api/v1"

[api.access]               # caller-supplied urls only
allow_loopback = false
[api.access.s3]
# endpoints = ["aws"]
[api.access.local]
paths = []
follow_symlinks = false

[[mount]]                  # file-server mode: zero or more mounts
path = "/"                 # url prefix
source = "/srv/data"       # a local directory
follow_symlinks = false    # resolution rules are per-mount, not global
immutable = false          # never changes once published — skip revalidation, §6.2

[[mount]]
path = "/hats"
source = "/data/hats"
immutable = true
```

**A mount's `source` is a local directory.** A non-local `source` is a startup error.

**Mount prefixes must be non-overlapping** — a startup error, not first-match-wins.

**Two tables, one resolver.** `Target` grows a `Mount { mount, relative_path }` variant so
the existing local-path resolution — canonicalize before matching, no climbing out with
`..`, symlink policy — is reused. The tables stay per mode.

**Each mount grants API access to what it mounts**, unconditionally: the bytes are already
served whole over the file-server route, so refusing to query them through the API would
withhold nothing. The grant is one-way — the API table says nothing about mounts.

Two constraints on the derived rule:

- **Scoped to the mount's prefix**, which is the shape `[api.access.local]` already has,
  so no new rule kind is needed. A derived rule is never wider than the mount.
- **Inherits the mount's resolution rules**, `follow_symlinks` in particular, so both
  routes agree about the same file.

### 3.2 Routing

`api.prefix` claims its subtree, mounts claim theirs, and overlap is refused at startup,
so dispatch is static and a request belongs to exactly one mode. A mount at `/` with the
API at `/api/v1` is the expected arrangement; the API's prefix wins as the more specific
route.

**Deliverable.** With no `[[mount]]`, the API alone. With `api.enabled = false` and one
mount, a pure file server.

### 3.3 The API request shape

**The API is `POST` only**; `GET` belongs to the file-server mode (§3.4). What this step
still has to settle is the query language, not the transport.

1. **Length.** A long `IN` list, a wide `select` and later a full ADQL statement run into
   URL length limits — nginx's default header buffer is 8 KB.

`POST` responses are uncacheable by intermediaries, which costs nothing here: §6.5 does
not cache query results, and the caching that matters is service→store, keyed on the
object.

**The path names the target; the predicate never appears in it.** A spatial constraint is
one clause of a query, so a `{target}/{predicate}` path set would grow as the product of
the predicate kinds rather than their sum.

```
POST /api/v1/parquet     one parquet file
POST /api/v1/hats        a HATS catalog
```

```json
{
  "url": "s3://bucket/hats/ztf_dr24",
  "region": [{"type": "circle", "ra": 320.65747, "dec": -12.35315, "radius": 0.01}],
  "select": "objectid, lightcurve.mag AS mag, objra, objdec",
  "where": "filterid = 2 AND mag < 20 AND objectid IN (1383212200036217, 1383212200036218)",
  "format": "parquet",
  "limit": 1000
}
```

- `select` — the projection, a SQL select list, so `mag - 0.1 AS mag_corr` works. Absent
  means every column.
- `where` — one boolean SQL expression over this target's columns. Not a statement: no
  `FROM`, `JOIN`, subquery, aggregate or window function. DataFusion parses it to an
  `Expr`; walk the tree to reject those forms, and allowlist scalar functions (`random()`
  would break determinism and §6's caching).
- `region` — a structured field, not part of the expression (§3.5). Drives partition
  pruning in HATS mode (§5.2).
- `format` — `json` or `parquet`. `limit` — maximum rows.

**`region` is always an array; each element is an object with a `type`.** One region is an
array of one. Every shape lowers to a MOC, which is what the planner needs anyway.

```json
"region": [{"type": "circle", "ra": 320.65747, "dec": -12.35315, "radius": 0.01}]
"region": [{"type": "circle", …}, {"type": "moc", …}]
```

| `type` | fields |
|---|---|
| `circle` | `ra`, `dec`, `radius` — the cone search, under ADQL's name for it |
| `box` | `ra`, `dec`, `width`, `height` |
| `polygon` | `vertices: [[ra, dec], …]` |
| `moc` | `ascii`, or `url` — an IVOA MOC given directly |

Degrees throughout; a `frame` field defaults to `icrs`. Unknown fields are rejected.

- **The array is a union.** State it in the docs and in error text, since a reader coming
  from `where` may expect `AND`. Intersection and difference are cheap to add later as
  explicit combinators.
- **`moc: {url: …}` is a caller-named fetch** and goes through §8.3 like any other.

**Nested columns.** `lightcurve.mag` works in both `select` and `where` in DataFusion 55,
including list-typed sub-fields; no quoting rule is needed and nested filters need not be
forbidden. Two consequences:

- **Auto-alias bare field accesses**, so the output column is the dotted path. Unaliased,
  DataFusion names it `t.lightcurve[mag]`.
- **DataFusion's `sql` feature is a prerequisite.** The current `default-features = false`
  build has no `ctx.sql`.

### 3.4 The file-server request shape

`GET`, a query string on the file's own path, using vizcat's parameter names. Browsers,
`wget`, `lsdb` clients and §5.3 plan entries speak this one.

**We adopt vizcat's query syntax — not more, not less.** Parameter names and the grammar
inside them; not their URL structure, not their limits.

| vizcat parameter | meaning | ours |
|---|---|---|
| `columns` | comma-separated column names, projection (`${X}` escapes awkward names) | same name, same meaning, no count limit |
| `filters` | a SQL-`WHERE`-like row constraint, e.g. `Gmag>8.0 && o_Gmag>100` | same name, same meaning; accept `AND` as well as `&&` |

Never reuse one of their parameter names for different semantics. Extensions with no
vizcat equivalent — `format`, `limit` — take names of our own.

**No spatial parameter.** Spatial selection here is by path, addressing `Norder=k/Npix=p`
directly. A caller who wants the catalog to choose partitions uses the API (§5.2) against
the same data, which the mount's derived grant permits.

Before implementing, complete `docs/vizcat-compat.md` from the live service: the operator
set `filters` accepts, whether `AND`/`OR` work alongside `&&`, the `${X}` escaping rules,
and the response shapes.

### 3.5 How much SQL

**SQL expressions are accepted; SQL statements are not. The spatial constraint is a
structured field, not an expression, until ADQL (§9).**

- **Expressions in SQL.** A predicate language needs comparisons, `AND`/`OR`/`NOT`, `IN`,
  `BETWEEN`, `IS NULL` and arithmetic. DataFusion parses and plans these already, and
  astronomers know the syntax from ADQL.
- **No statements.** `FROM` admits joins — a join across two catalogs is a crossmatch,
  which is `lsdb`'s job — and `GROUP BY`, which breaks §5.3's plan mode and the
  pre-execution cost estimate §5.3 and §8.4 depend on. Keeping the target in the path and
  its identity in `url` leaves no `FROM` to abuse, so the restriction needs no enforcement
  machinery.
- **Spatial stays structured.** Partition pruning requires *recognising* the constraint. A
  named `region` field is recognised by construction. As `WHERE cone_contains(…)` it would
  be a UDF call to pattern-match inside a logical plan — implementable via a
  `TableProvider` with `supports_filters_pushdown`, but best-effort: an unexpected
  phrasing degrades silently to scanning every partition.

**With ADQL** (§9), the spatial functions are standardised — `CONTAINS`, `POINT`,
`CIRCLE`, `DISTANCE` — so the matcher targets specified spellings and can refuse
unrecognised spatial predicates rather than scanning everything. That is when the
constraint can move into the expression.

## 4. Phase 3 — the file-server interface

Modelled on <https://vizcat.cds.unistra.fr/hats/> and
<https://github.com/astronomy-commons/lsdb-server>, with the query surface from §3.4.

Behaviour, in order of precedence:

1. **A directory path** → a listing. HTML for a browser (`Accept: text/html`), JSON
   otherwise — a `readdir` over the mount. Cap entries and paginate: a HATS `Dir=` level
   holds ten thousand entries.
2. **A non-parquet file, or a parquet file with no query parameters** → the bytes,
   statically. This covers `properties`, `partition_info.csv`, `point_map.fits`,
   `_metadata` and `_common_metadata`, which `hats`/`lsdb` clients fetch verbatim. Serve
   with:
   - correct `Content-Type` (`application/vnd.apache.parquet` for parquet);
   - `Range` passed through to the store, so a remote `lsdb` can read a partition without
     downloading it;
   - `ETag` and `Last-Modified` from store metadata, with `If-None-Match` /
     `If-Modified-Since` handling;
   - `Content-Length`, streaming the body rather than buffering it.
3. **A parquet file with query parameters** → a query through `query.rs` +
   `parquet_out.rs`, with the file taken from the mount.

The static-serving path must not regress: an `lsdb` client pointed at a mount should work
with no knowledge of anything else this service does.

## 5. Phase 4 — the HATS interface

Removes the requirement that a caller know which partition file holds their object.

### 5.1 Catalog metadata

A HATS catalog is a directory with `properties`, `partition_info.csv`,
`_metadata`/`_common_metadata`, and `dataset/Norder=k/Dir=d/Npix=p/*.parquet`. Add
`src/hats/`:

- `properties.rs` — the `properties` file: catalog name, type, ra/dec column names,
  `hats_order`, row count.
- `partitions.rs` — the partition list, by the tiers below.
- `pixels.rs` — HEALPix nested-scheme arithmetic: region → covering pixel set, pixel →
  `_healpix_29` range, ancestor/descendant tests. Use `cdshealpix` rather than writing it.

**Partition discovery, in priority order.** Each tier is used only when the one above is
absent:

| | source | requests | gives |
|---|---|---|---|
| 1 | `partition_info.csv` | one small `GET` | `(Norder, Npix)` only |
| 2 | `_metadata` | one `GET`, footer-first | the pixel list, plus row counts, byte sizes and column statistics per partition |
| 3 | listing `dataset/` | one `LIST` per level, paginated | the pixel list, parsed from `Norder=k/Npix=p` path names |

`_metadata` is a parquet file whose footer carries the `FileMetaData` of every partition,
each row group tagged with its `file_path`, so one ranged footer read reconstructs the
partition list. It also supplies the per-partition sizes §5.3's estimates need, so it is
worth reading **even when tier 1 succeeded** — lazily, when a query needs sizing, not on
every catalog open.

- `_common_metadata` cannot substitute: schema only, no row groups, no `file_path`
  entries. It is useful for validating a projection before reading data.
- `_metadata` can reach hundreds of MB for a wide schema over many partitions. Size it by
  §2.3's ladder and cap it; fall through to tier 3 rather than blocking on a large
  download.
- Listing may be unavailable entirely: an `http(s)://` catalog has no listing operation
  (§2.3), leaving tiers 1 and 2. The same catalog served over WebDAV (§2.4) has all
  three.

If tiers 1 and 2 disagree, prefer `partition_info.csv` and log at `warn` — they disagree
when a catalog is malformed or being rewritten.

The parsed partition list goes in §6.1's HATS metadata cache; the `_metadata` footer goes
in the parquet metadata cache.

### 5.2 The spatial predicate

`region` (§3.3) is one field of an ordinary request, beside `where` and `select`. The
`mode` field — `sync`, `plan`, `auto` (default) — controls delivery (§5.3).

**Execution, HATS target:**

1. `region` → MOC → intersect with the partition list. This is free, needs no column in
   the data, and already separates two cases:
   - **partitions fully inside the region** — every row qualifies, no spatial test at all;
   - **partitions the region only partly covers** — rows need a per-row test.
2. Per surviving partition, push down the `where` clauses that map onto its columns, fused
   with whatever spatial predicate step 3 produces, so a partition whose statistics rule out
   `filterid = 2` is never read.
3. Row-level refinement, **for boundary partitions only**. Where the catalog has a spatial
   index column (`_healpix_29`, usual but not guaranteed), prefilter on it — see below;
   otherwise go straight to the geometric test against `ra`/`dec`, read into the scan and
   dropped unless `select` asked for them.
4. Union, project to `select`, return.

**Prefiltering on `_healpix_29` is required where the column exists**, but not as one exact
covering — a covering fine enough to be exact is enormous (at order 29 a one-degree circle's
boundary is of order 10⁷ cells), and testing membership against thousands of ranges costs
more per row than the trigonometry it replaces. Two things keep it cheap:

- **Two coarse range sets, not one.** An **inner** set of cells wholly inside the region:
  rows there are accepted with no geometric test. An **outer** set covering the region:
  rows outside it are rejected with no geometric test. Only rows between the two — the
  boundary shell — reach the trigonometry. Neither set has to be tight, so both stay small.
- **Computed per boundary partition, not for the whole region.** The covering order is
  chosen relative to that partition's own cell rather than to the region's size, so each
  boundary partition contributes a handful of ranges however large the region is, and for a
  large region most partitions are interior (step 1) and contribute none.

Size the range sets to what the page index can prune: the aim is skipping row groups and
pages on a column the partition is already sorted by, not per-row membership. The order
policy and the range budget are to be settled by measurement before implementing §5.2.

This reduces bytes read per row, not rows per query. A region over a dense catalog can
still select terabytes, which is what §5.3's `max_scanned_bytes` and plan mode are for.

**Parquet target:** the same, with step 1 skipped. Today's `column == value` behaviour is
`where` with no `region`.

`box` and `polygon` are the same machinery with a different covering step. Nearest-object
lookup is a `circle` plus ordering and `limit: 1`, not a predicate, and waits for ordering.
`crossmatch` is out of scope — `lsdb`'s job.

### 5.3 Small queries and large queries

A region over a dense catalog can touch hundreds of partitions of hundreds of MB.

- **`mode=sync`** — run it and return rows, bounded by `max_partitions`,
  `max_scanned_bytes` and `timeout`. Sizes come from `_metadata` (§5.1); without it, fall
  back to a `HEAD` per candidate partition or to counting partitions. Exceeding a limit is
  a 413 whose body is the plan below.
- **`mode=plan`** — resolve the catalog and return a work list without reading data:

  ```json
  {
    "catalog": "s3://bucket/hats/ztf_dr24",
    "num_partitions": 3,
    "estimated_bytes": 1140850688,
    "requires_credentials": true,
    "requests": [
      {
        "order": 5, "pixel": 12240,
        "method": "POST",
        "path": "/api/v1/parquet",
        "body": {"url": "…", "where": "_healpix_29 = …", "select": "…"},
        "estimated_bytes": 380375000
      }
    ]
  }
  ```

  Each entry is a request against this service. The client fans out with its own
  concurrency limit and concatenates; retries are per partition. `method` and `path` are
  separate fields so the file-server case, where entries are `GET`s under a mount, uses the
  same shape.

  **Credentials are never echoed into a plan** (§8.1). When the original request carried
  them, the entries carry the stripped url and `requires_credentials` is `true`; the client
  re-attaches the credentials it already holds before sending. Returning them would enable
  nothing the client cannot already do, while copying the secret into a response that gets
  logged, cached and pasted. This is not configurable: an option to echo them would be a
  footgun with no capability behind it. The flag exists so the client re-attaches
  deliberately rather than discovering the need through a 403.
- **`mode=auto`** — sync under the limits, plan over them. The response states which.

No job queue, job ids or polling: the plan is a list of stateless requests. See §7.2.

## 6. Phase 5 — caching

### 6.0 Invariants

1. **A cache is never a source of truth.** Empty it at any moment and every request still
   returns the same answer, slower.
2. **The access policy is consulted before the cache.** A lookup that short-circuits
   `AccessPolicy::authorize` would keep serving an object admitted under an older config,
   or reachable through a different mount.
3. **The key includes everything affecting the bytes, credentials included.** Two callers
   with different credentials for one `s3://` url may be entitled to different objects, so
   entries are keyed by credential identity — `HMAC(per-process key, credential set)`,
   never a bare hash of a possibly low-entropy secret, since a cache that outlives or lives
   outside the process holds that key at rest.
4. **Credentialed entries are always revalidated**, never served on TTL alone. The
   conditional request carries the caller's own credentials, so a revoked or narrowed
   credential fails at the origin and the entry is evicted. Keying by credential gives
   isolation between callers; only revalidation catches a credential that has stopped being
   valid, which is the one thing the key cannot express.
5. **Bound every layer twice**, by entries and by bytes.
6. **Every layer is emptiable at runtime** (§6.6).

### 6.1 The layers

| layer | holds | keyed by | bound | default |
|---|---|---|---|---|
| **HATS catalog metadata** | parsed `properties`, partition list, derived MOC | catalog url + validator | entries | on |
| **Parquet file metadata** | `FileMetaData` footer, page index, bloom filter headers | object url + validator | bytes | on |
| **Range-support verdict** (§2.3) | does this host honour `Range`? | scheme + host + port | entries | on |
| **Object bytes** | whole objects or ranges | object url + validator (+ range) | bytes, disk if enabled | off |
| **Directory listings** (§4) | one page of a listing | prefix + validator | entries | on, short TTL |
| **Negative results** | 404 for a missing object | url | entries | on, very short TTL |
| Query results | — | — | — | not cached, §6.5 |

The first two are the ones worth building. Parquet footers are read **twice per
`format=parquet` request** — once by `query.rs` to plan, once by `parquet_out.rs` to copy
the source layout; fix that directly as well as caching it.

Negative caching applies to absence only. A 403 is recomputed every time, since the policy
behind it can be reloaded.

### 6.2 Expiration and validation

| backend | validator | revalidation |
|---|---|---|
| s3 / gcs / azure | `ETag` | conditional `GET`/`HEAD` with `If-None-Match` → `304` |
| http(s) | `ETag`, else `Last-Modified` | `If-None-Match` / `If-Modified-Since` |
| local file | `(device, inode, mtime_nsec, size)` | `stat` |

Freshness, in order of preference:

1. **Declared immutable → never revalidate.** A mount or access rule sets
   `immutable = true`.
2. **Otherwise revalidate after `ttl`.** Within the TTL, serve the hit. After it,
   revalidate: a `304` or unchanged `stat` refreshes in place and keeps the parsed value;
   a changed validator evicts and refetches.
3. **No validator → TTL alone**, kept short, and reported in the metrics.

`ttl` is the staleness contract: an update is visible within `ttl`, or immediately when a
validator changes. Anything that can miss an update — including §6.7's watcher — is an
optimization on top of validation, so a missed event costs `ttl`-bounded staleness, never
a permanently wrong answer.

**Callers cannot set the TTL**; it is operator-only, in `[cache.*]` and per-mount. A
caller-set TTL would be shared state, abusable in both directions — short as a
cache-busting amplifier, long as a way to degrade another caller's freshness. Request-side
`Cache-Control` is honoured instead, being per-request by construction:

| request header | meaning |
|---|---|
| `no-cache` | revalidate before serving this caller; the `304` refreshes the shared entry |
| `max-age=N` | do not serve this caller an entry older than N; falls back to revalidation, never eviction |
| `no-store` | do not cache what this request produced |

Two guards:

- **Coalesce and floor revalidations.** Concurrent `no-cache` requests for one key collapse
  into a single conditional request, with a per-key minimum revalidation interval.
- **`immutable` wins over `no-cache`**, and the response says so in a header. The
  operator's escape hatch for a wrong `immutable` is `SIGHUP`/purge (§6.6).

### 6.3 Implementation

One `src/cache.rs` with a single generic bounded cache used by every layer. `moka` (async,
TTL + TTI, weight-based eviction) is the expected dependency; `quick_cache` if something
smaller is wanted. Each layer is an instance with its own weigher and limits.

Entries hold parsed values (`Arc<FileMetaData>`, a parsed partition list), shared by `Arc`
so a hit costs a clone.

Concurrent misses on one key must collapse into a single fetch (`moka`'s `try_get_with`).

### 6.4 Configuration

```toml
[cache]
enabled = true            # master switch; false makes every layer a no-op

[cache.hats]
max_entries = 256
ttl = "5m"
revalidate = true

[cache.parquet]
max_bytes = "512MiB"
ttl = "5m"
revalidate = true

[cache.object]
enabled = false           # §6.5
max_bytes = "1GiB"
# dir = "/var/cache/hats-api"   # absent: memory only

[cache.listing]
ttl = "30s"
```

Mounts and access rules carry `immutable` and `ttl` overrides, since freshness is a
property of the data.

### 6.5 What stays outside the process

**Object byte ranges belong in a reverse proxy.** nginx/Varnish `proxy_cache` with `slice`
solves it, and immutable objects make the key trivial. Ship a `docker-compose.yml` with
nginx in front, `proxy_cache_valid` tuned for immutable objects and `slice 1m`, plus
`docs/caching.md`. `[cache.object]` exists for deployments that cannot front the service
with a proxy, and stays off by default.

The exception is §2.3's materialized copy for non-ranging hosts, where there is no ranged
request for a proxy to cache. **Enable `[cache.object]` with `dir` set for that case**,
under the same `max_bytes` and eviction rules as every other layer.

**Query results are not cached.** The key space is the cross product of url, predicate,
projection and format, and the hit rate on point lookups is near zero.

### 6.6 Observability and control

- Per layer: hits, misses, evictions, entries, bytes, and revalidations split into `304`
  versus changed, through §7.1's `/api/v1/metrics`.
- `X-Cache: hit | miss | revalidated` on file-server responses.
- `POST /api/v1/cache/purge`, optionally scoped to a url prefix or one layer, behind
  whatever admin gate §7 settles on; absent that, bind it to loopback.
- **`SIGHUP` empties every cache**, and later reloads the config.

### 6.7 Later: invalidation from the filesystem

For local mounts, two levels:

1. **Signal-driven.** `SIGHUP` as above: a publishing pipeline ends with `kill -HUP` and
   the next request sees the new data. No watching, no per-path bookkeeping, no platform
   differences.
2. **Watch-driven.** `notify` over inotify and FSEvents, watching each mount and evicting
   only keys under a changed path. Constraints to handle:
   - inotify watches are per-directory and capped by `fs.inotify.max_user_watches`; a HATS
     tree is tens of thousands of directories, so a recursive watch can exhaust the limit
     and stop reporting silently.
   - events coalesce, arrive out of order, and are lost on `IN_Q_OVERFLOW` — treat an
     overflow as "empty this mount's entries".
   - a write in progress fires events before the file is complete: evict, and let the next
     request refetch, rather than reading on the event.
   - network filesystems report nothing for changes made on another host. Log what is
     actually being watched at startup.
   - remote stores are out of scope; there is no push channel short of bucket
     notifications.

   Validation and TTL stay on underneath, per §6.2.

### 6.8 Prerequisite

Add a benchmark measuring the cost breakdown of a request — footer read, metadata parse,
data read, and the second footer read for `format=parquet` — against a real file. Build the
layers it justifies, in the order it ranks them. If the numbers are transfer-bound, as the
README's "Known costs" suggests, the duplicate footer read in §6.1 is the cheaper fix.

## 7. Phase 6 — operational surface

### 7.1 The basics

- `/api/v1/metrics` — Prometheus text format: request counts and latencies by endpoint and
  status, bytes fetched from stores, cache hit rates, partitions scanned.
- Request limits: a global concurrency cap and a per-request timeout, configured, returning
  429/504.
- OpenAPI description of the API mode, generated rather than hand-written.
- `docker-compose.yml` for the realistic deployment: `hats-api` + nginx cache + MinIO.

### 7.2 Long requests

**No async job interface in this plan.** TAP's `/async` (§9) is what forces one, and UWS
specifies its shape, so it is built once rather than invented and then reconciled.

| slow case | decomposable? | answer |
|---|---|---|
| large region over many partitions | yes, by partition | §5.3 plan mode |
| all-columns read of one big partition | partly, by column | request fewer columns |
| materializing 2 GiB from a non-ranging host (§2.3) | no | prefetch, below |
| a slow or distant origin | no | timeout, reported clearly |

A job system would break §0's statelessness invariant and add job ids as an authorization
surface for §8. Raising timeouts is not an alternative: nginx's `proxy_read_timeout`
defaults to 60 seconds. Two things to build instead:

1. **Stream the response** — parquet in row-group chunks, JSON element by element. Keeps
   time-to-first-byte short, keeps bytes flowing so intermediaries do not drop the
   connection, and caps memory on large results.
2. **A prefetch primitive.** `POST /api/v1/prefetch` with a url returns `202` and warms
   `[cache.object]` in the background; `GET` on it reports residency. This is a cache
   operation, not a job: no result to store, no per-user state, nothing to expire beyond
   what the cache expires, idempotent, and a no-op if never called.

Document the limitation: against a non-ranging origin holding a huge object, the first
request after a cold start times out, and prefetch is the way around it.

## 8. Security requirements

Conditions every phase must keep.

**Threat model.** The operator is trusted — they wrote the config and run the process. The
caller is not: they supply a url, storage options, credentials, a projection and a
predicate, and the service makes network and filesystem requests on their behalf. The
policy is the only thing between a caller and the machine's own network.

### 8.1 No credential leakage

Held today by `errors_never_carry_the_credentials` and the tests around it; every new
backend in §2 is a chance to break it.

- Credentials are **stripped at the boundary**: everything downstream of `storage::open` —
  logs, spans, errors, DataFusion, metrics — sees only `scheme://host/key`. New backends
  add option names to that stripping.
- **No credential in a log line**, including at `debug` and `trace`. This is not only
  about the code here. A dependency may derive `Debug` on a struct holding a secret and
  log it, and the SigV4 signer under OpenDAL does exactly that at `DEBUG`, so
  `RUST_LOG=debug` would otherwise write every caller's secret to the log. The filter is
  therefore **not purely the operator's to choose**: `logging::CREDENTIAL_UNSAFE_TARGETS`
  is silenced after `RUST_LOG` is applied, where a more specific directive wins, so
  raising the log level cannot re-enable it. Silencing a target loses its diagnostics,
  which is why it is a list with a reason per entry rather than a wildcard.
- **No credential in an error message**, including errors raised inside `object_store` or
  DataFusion, which must only ever be handed the stripped url.
- **No credential in a response**, including §5.3's plan bodies.
- **No credential in a metric label.**
- **The request body is the only source of credentials.** The service reads them from
  nowhere else: not from environment variables, not from files on disk, not from ambient
  discovery by any backend SDK (§2.2), not from an instance profile or metadata server. A
  request with no credentials is unsigned, never the process's own identity. Supporting an
  operator-configured credential source is §9, and it must be explicit in the config when
  it arrives.
- A caller must not reach another's credentials through the cache — §6.0's keying rule.

### 8.2 No local filesystem until the config says so

- Default deny, and **an empty list means none**, not "unset, so allow".
- Every path is **canonicalized before matching**, so `..` cannot climb out and a symlink
  inside an allowed directory cannot lead out of one. Without `follow_symlinks`, a path
  traversing a symlink at all is refused.
- **Refusal must not be a filesystem oracle.** Outside every allowed directory is 403
  whether or not the path exists; only inside one does a missing file become 404. Preserve
  this when adding the file-server mode.
- Derived API grants (§3.1) are never wider than the mount they come from.
- Run as an unprivileged user, and document `ReadOnlyPaths=`/`ProtectSystem=` (systemd)
  and read-only bind mounts (Docker) in `docs/deployment.md`.

### 8.3 No local network until the config says so

**This is a real gap today, not a rule to preserve.** `allow_loopback` covers `127.0.0.0/8`
and `::1` only. With `[api.access.s3].endpoints` absent — meaning any endpoint — a caller
can aim the service at `169.254.169.254` (the EC2/GCE instance metadata service, a source
of IAM credentials), at RFC1918 space, or at any internal hostname resolving there. §2.3's
HTTP backend turns that from probing into direct reads, so **the network policy must land
with or before the HTTP backend**.

- A single **`[api.access.network]`** section governing every backend, since this is a
  property of the destination address rather than the protocol:

  ```toml
  [api.access.network]
  allow_loopback = false      # 127.0.0.0/8, ::1
  allow_private = false       # RFC1918, fc00::/7, link-local incl. 169.254.0.0/16
  allow_local_names = false   # single-label, .local, .internal, .cluster.local, …
  # allow_cidrs = ["10.1.2.0/24"]
  # allow_hosts = ["minio.internal"]
  ```

- **Decide on the resolved address**, which also handles literal-form tricks (decimal
  `2130706433`, octal, IPv4-mapped IPv6). **Every** resolved address must pass, not only
  the first.
- **Refuse local names before resolution**, since internal hosts on public IP space defeat
  the address rules: single-label names, `.local`, `.internal`, `.localhost`, `.home.arpa`,
  `.cluster.local`, `.svc`, plus an operator deny list. Names and addresses are two layers
  and both are needed.
- **Close the resolve-then-connect gap** with a connector that re-validates the socket
  address at connection time; checking a name and letting the client resolve it again is
  DNS rebinding.
- **Follow no redirects** on the HTTP backend by default; if ever needed, each hop is a
  fresh policy decision.
- **Never echo a remote response body into an error** — report the status code and the
  stripped url.
- Naming an endpoint in `[api.access.s3].endpoints` remains permission enough for it. The
  network rules govern what a caller may reach, not what the operator configured.

### 8.4 Bounded work per request

- Per request: a timeout, a cap on bytes fetched from the store, §2.3's
  `max_materialize_bytes`, §5.3's `max_partitions` and `max_scanned_bytes`.
- Per process: a concurrency limit (§7.1).
- `POST` body size limit, and caps on `where` expression depth and node count, rejected at
  parse time against the `Expr` tree. The projection needs no cap: it is bounded by the
  schema, and the byte and time limits govern the data it moves.
- Reject pathological parquet early — a footer claiming implausible row-group or column
  counts is a 400, not an allocation.

### 8.5 Obligations on every new backend

1. Its option names are on the stripping list, with a test that an error mentioning a bad
   option does not carry the credential.
2. Its destination goes through §8.3's network check, after resolution.
3. It is default-deny in the config, with the same three-state shape as `endpoints`.
4. Its refusals do not distinguish "does not exist" from "not allowed" outside an allowed
   scope.
5. There is a test that the default config refuses it, and one that the narrowest config
   that should allow it does.
6. **Its SDK is checked for logging credentials**, at `trace`, with a request that
   carries them — a new backend brings a new signer and a new chance at §8.1's
   dependency problem. Anything found goes in `CREDENTIAL_UNSAFE_TARGETS` with its
   reason, which the canary test then holds to being the complete list.

Run `cargo deny` (advisories + licences) in CI.

## 9. Future development

1. **Remote mount sources.** A `[[mount]]` fronting `s3://bucket/hats/` or an HTTP tree.
   Listings become paginated `LIST` calls, static serving becomes a proxied range read with
   the origin's `ETag` passed through, and §8.3's network policy starts applying to the
   file-server mode. `[api.access]` needs a prefix rule kind first: `source =
   "s3://bucket/hats/"` is not an endpoint entry, which would allow every bucket at that
   endpoint, and §3.1's derived grant must stay no wider than the mount. This is
   also the first mount that could need credentials, so it is where §8.1's rule is
   revisited: any operator-configured credential source — a config value, an environment
   variable, a file — is named explicitly in the config for that mount, never discovered
   from the process environment.
2. **SQL, then ADQL, as front ends.** Both parse into the structured query the service
   already executes (§3.5), rather than opening a second execution path. ADQL's `CONTAINS`,
   `POINT`, `CIRCLE`, `DISTANCE` map onto §5.2's spatial predicates. Plain SQL first: it
   settles the lowering and the rejection messages before the IVOA grammar.
3. **TAP protocol.** IVOA TAP over the ADQL layer: `/sync`, `/async`, VOSI endpoints,
   `VOTable` output, the UWS job model. `/async` is a real job system with state, and is
   where §5.3's and §7.2's no-job-queue decision is revisited.
4. **Filesystem-driven cache invalidation** (§6.7): `SIGHUP` first, then a `notify` watcher
   over local mounts.
5. **Separate crates, separate repos.** Once ADQL and TAP exist, split into `hats-query`,
   `adql` and `tap` so each is usable without the others.

Each phase leaves the service useful, and each is a prerequisite for the next rather than a
parallel track.
