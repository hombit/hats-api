# Conventions

## Where things go

Four places, four jobs. Writing something in the wrong one is what makes all four rot.

- **Commit messages** are the only place for history. What changed, why it changed, what
  it replaced, what was tried and rejected, what a decision turned on — all of it, and
  only there. Nothing else in the repository records what happened.
- **Code comments** explain the tricky part in front of them: a constraint, a failure
  mode, a reason a plausible alternative is wrong. Not what changed, not when.
- **`DEVELOPMENT_PLAN.md`** is for planning — what is still to be done and what
  constrains it. Never what was built. Finishing a step means moving its row to `done`
  and changing whatever a later step now has to do differently; if nothing later
  changed, the row is the whole update. A note that could begin "we added" is a commit
  message. Something the plan assumed and got wrong is corrected in place, not annotated.
  Work that was considered and not done is simply absent — the commit says why.
- **`CLAUDE.md`** is for instructions to whoever works here next. A rule a finished piece
  of work leaves behind belongs here, phrased as a rule rather than as a story about
  where it came from.

Nothing outside `DEVELOPMENT_PLAN.md` may refer to it — no section number, no filename,
not in code, comments, test names, config or workflows. It is scaffolding and gets
deleted when the work in it is done, and a reference to `§8.1` becomes a dangling
pointer the moment that happens. When code needs a reason, the comment states the
reason; if that makes the comment longer, it was leaning on the plan to finish its
sentence. The `no-plan-references` pre-commit hook greps for both spellings; this file
is exempt because it is where the rule is written down.

## Dependencies

Look for a public crate before writing one. Especially for anything security- or
performance-critical: a widely used crate has had far more eyes on its edge cases than
anything written here in an afternoon. `cargo search`, then `cargo info <name>` for the
licence and features.

Say what was picked and what it costs — added crates, licence, what it does not cover —
rather than adding it silently. Not every crate is worth it: a one-liner with a heavy
dependency tree is not, and neither is one that solves a different problem than the one
at hand.

Keep `object_store` matched to DataFusion's, and `reqwest` and
`opendal-http-transport-reqwest` matched to what `opendal` resolves to. Two copies of a
crate are two distinct types, so a mismatch is a type error rather than a version
warning — and one that surfaces several crates from the line that caused it.
`deny.toml` denies multiple versions of those three by name, so `cargo deny check bans`
says which crate went double before the compiler gets a chance to be unhelpful about it.

## Credentials

- **A credential is never printed, and neither is anything a caller wrote beside one.**
  `storage::Headers` prints its count and nothing else: both the name and the value of a
  header come from the caller, and a token typed into a name is still a token in this
  service's log. The same reasoning is why anything holding one — `MaterializingStore`,
  `WithHeaders` — has a hand-written `Debug`. A `HeaderMap`'s own `Debug` redacts values
  marked sensitive but prints every name in the clear, so a derive is not enough. Mark
  values `set_sensitive(true)` as well; it is the backstop, not the guarantee.
- A credential is never a `String`. Use `secrecy::SecretString`, and
  `storage::SourceUrl` for a caller-supplied url. Neither prints its value, so
  `#[derive(Debug)]` around them is safe. `StorageOptions::named` enforces this: a
  credential is registered with `Named::credential`, which takes a `SecretString` and
  nothing else, and a `SecretString` cannot be registered with `Named::plain`. Both
  mistakes are compile errors, and so is adding an option field and not registering it
  at all.
- `url::Url`'s own `Debug` prints its `password` field, so a struct holding one needs a
  hand-written `Debug` rather than a derive.
- A credential option is a value, never a path or a filename. Reading a credential off
  local disk at a caller's direction is both "the request is the only source" and the
  local-path rules, broken at once.
- A caller's string that ends up inside a hostname — a region, an account — goes through
  `storage::require_label` first, and what gets passed on is the `HostLabel` it returns,
  not the `&str` that went in. A `/` in it moves the host to whatever came before, which
  is a way past the endpoint policy rather than a cosmetic problem.
- Nothing downstream of `storage::open` sees a url with options on it.
- A dependency that logs credentials goes in `logging::CREDENTIAL_UNSAFE_TARGETS`, with
  a reason. `tests/credential_logging.rs` is what catches the next one.

## Adding a backend

Add the `Backend` variant and follow the compile errors. Every match on a `Backend` is
exhaustive, and the served schemes, the accepted options and the endpoint rules are all
derived from it rather than written out beside it, so there is no list to forget.

`Backend::provider` is the fork in that road. A backend that has a provider is addressed
by bucket: the url's host is a bucket name, the server is the `endpoint` option, and the
provider's own service is what a request naming no endpoint means. A backend that has
none is addressed by origin: the url is the server, so it takes no `endpoint` and has no
default to fall back to. That is all `provider` decides — it says nothing about
credentials, and an origin-addressed backend can still take `headers`. Ask `has_provider`
rather than matching on the variant, so a later backend of either shape lands on the
right side without this being rewritten.

A backend may serve more than one scheme — `Backend::schemes` returns a slice, and
`http`/`https` are one backend reached two ways.

Before committing to a service, check it offers both of these; one that does not cannot
be served here at all.

- **`skip_signature`, or whatever the service calls it.** It is what makes an anonymous
  request anonymous. Without it OpenDAL walks its ambient chain and answers with the
  deployment's identity, and nothing on this side can prevent that.
- **A switch for every ambient credential source**, disabled per store rather than
  globally — `disable_config_load`, `disable_ec2_metadata`, `disable_vm_metadata`.

A service with no ambient chain at all satisfies both by construction — OpenDAL's http
service sends an `Authorization` header only when the builder was handed one — but say so
in the backend function rather than leaving the absence of the calls to be read as an
oversight.

Then follow the shape the existing ones set:

- Addressing is per backend: path-style against a named S3-compatible endpoint,
  virtual-host against the provider itself.
- `allow_http` stays ours. OpenDAL follows the endpoint's own scheme without asking, so
  the cleartext decision has no backend half to defer to. It has two halves of its own,
  and both must pass:
  - **the operator's**, `access.<backend>.allow_plain_http`, for a backend whose url is
    its own address. What cleartext costs there is the assurance that the bytes came from
    the host the url named, and only the operator knows the network.
  - **the caller's**, the `allow_http` option, wherever the request can carry a
    credential. That is every backend: an origin-addressed one has no `endpoint` option
    but may still be given `headers`. Do not reason from "this backend has no endpoint"
    to "this backend has no secret to protect".
- `object_store` is trait-only here — the `ObjectStore` trait DataFusion consumes, plus
  `LocalFileSystem` for `file://`. Put a new backend on OpenDAL's side; never re-enable
  an `object_store` backend feature.
- **A credential that the probe also needs goes out as a header, not into the builder.**
  `MaterializingStore`'s probe is this crate's own request rather than OpenDAL's, so a
  credential configured on the builder reaches the reads and not the probe. The probe
  then gets a `401`, reads it as "not a `206`", concludes the server ranges, and hands
  the reader the head of the file at every offset — a wrong answer rather than an error.
  `webdav_builder` is the worked example, and `tests/webdav.rs` is what catches it.
- **Ask whether its servers honour `Range`.** A provider's does. A server the caller
  named may not, and OpenDAL accepts a `200` to a ranged read without complaint, so the
  reader gets the head of the file where it asked for the tail — a wrong answer, not an
  error. Any backend whose host comes from the request goes behind
  `materialize::MaterializingStore`, which decides per object and copies to scratch when
  it has to. Reading the code cannot tell the two servers apart; `tests/http_ranges.rs`
  serves both and checks the rows.

## The network

- A remote store is built through `storage::remote_store`, which is the one thing that
  turns a configured builder into something that can make a request — and the one place
  that puts the access policy's HTTP transport on it. A backend function returns its
  builder and never holds an `Operator`. `clippy.toml` disallows `Operator::new`,
  `reqwest::Client::new` and `reqwest::Client::builder` outside their single permitted
  call sites, each of which carries an `#[expect]` saying so; a new one needs a reason
  written down next to it. An `#[expect]` clippy reports as *unfulfilled* means the rule
  is not covering the call it was written for — check what the call actually resolves to
  rather than deleting the attribute.
- A request this crate makes itself, rather than through a store, goes through
  `NetworkPolicy::client`. It is the same built client the transport wraps, so it carries
  the same resolver and the same refusal to follow redirects. Building a second client
  would resolve names again with nothing checking the answer.
- The address check belongs in the resolver and nowhere else. Checking a host and then
  letting a client resolve it again is DNS rebinding: the answer that passed is not the
  answer that gets connected to.
- Do not follow redirects. A 3xx is the origin choosing the next destination, which
  would carry the caller's credentials to a host no endpoint rule named.
- A host named in an endpoint list is permission at both layers — the endpoint rules and
  the network rules. An operator should not have to say it twice.
- `access` decides which endpoint may be named; `network` decides which address may be
  reached. A new rule belongs in whichever of those it is actually about.

## The caller's SQL

`sql.rs` is the only way a caller's expression becomes something this service runs, and
everything about what SQL means here is decided there.

- **Expressions, never statements.** Each field is parsed on its own with `sqlparser`,
  and the parser must reach the end of the string. That is what makes it an expression
  rather than the head of something longer: without the end-of-input check, `1 UNION
  SELECT …` parses as `1` and the rest is dropped in silence. Never assemble a caller's
  text into a SQL statement and check the plan afterwards — the check would be the only
  thing standing between a select list and a join.
- **`allowed` is an allowlist over an exhaustive match**, not a list of what is refused.
  A DataFusion upgrade that adds an expression kind is then a compile error, and someone
  decides whether it belongs in a per-row expression rather than a caller discovering
  that it already did. Add the variant to the arm it belongs in; do not add a wildcard.
- **A function is judged by volatility, never by name.** Only `Immutable` passes: `now()`
  is `Stable` and `random()` is `Volatile`, and both make one request's answer differ
  from the next's for the same query, which is wrong to cache and wrong to reproduce from
  a plan. The rule holds for functions this crate has never compiled in, which is why it
  is not a list of names.
- **A column answers to its own name and to its name in lowercase, and to nothing else.**
  Astronomy column names are mixed-case as a matter of course — `Gmag`, `Norder`,
  `objectId` — and a caller reads them off the file, so the file's spelling has to work;
  lowercase has to work too, because that is what SQL says an unquoted name means. Every
  other casing is refused rather than resolved, so which names a column answers to never
  depends on what else is in the file. Two columns whose lowercase forms collide are each
  reachable by writing them out, and the form they share names neither.

  This is `resolve_identifiers`, and it needs `enable_ident_normalization` to stay off in
  `query::session_config` — with it on, DataFusion lowercases what the pass did not
  rewrite, and `OBJECTID` starts finding `objectid` again. Neither half works alone: a
  session config that turns normalization back on quietly widens the rule.
- **The schema is what types a literal.** Plan against the file's `DFSchema` so that
  `objectid = 1383212200036217` becomes an `Int64` literal, which row-group statistics,
  the page index and a bloom filter can all prune on. Compared as a string it reads the
  whole file and returns nothing — a slow wrong answer rather than an error.

- **Two vocabularies, one meaning.** `select`/`where` take expressions and
  `columns`/`filters` take the narrower forms a query string can carry, but both lower to
  the same planned expression and meet the same allowlist, and a request may use either
  pair and not both. A difference in what they mean is a bug, not a feature — so a change
  to one is a change to `sql.rs`, where they share the code, rather than a second path
  beside it. `columns` stays narrower: it takes names, and a caller who wants an
  expression writes `select`.
- **A parameter this service acts on is honoured or refused, never dropped.** A `filters`
  that does not parse, or that names a column the file has not got, is a 400. Ignoring it
  returns every row, which the caller cannot tell from a predicate that matched every row
  — a wrong answer rather than an error, and the same failure shape as a server that
  ignores `Range`. The service whose parameter names these are does exactly this, which is
  why the names were taken and the behaviour was not.

  A parameter on a path that has no query surface is a different thing and is ignored, the
  way any HTTP server ignores what it has no use for. `data::DataFiles` is what draws that
  line — one configured list of filename globs, consulted by both modes — so whether a
  request is a query at all is decided before any parameter is read, rather than by each
  parameter deciding for itself.

Adding a scalar function feature to the `datafusion` dependency adds everything it
registers to what a caller may call. That is the decision being made; make it
deliberately.

## The order of the rows

**The file-server mode returns rows in the source file's order. The API mode promises
nothing about order. Both answer a `limit` reproducibly.** `query::Order` is how a request
says which it is, and `query::reproducible` turns that plus the presence of a `limit` into
the one decision the rest of the code reads.

A `limit` is the part that is easy to get wrong. Under an unstable order it stops being
"how many rows" and becomes "which rows", so the same request returns a different subset
each time — and a caller cannot tell that from the data having changed. That is why
reproducibility does not follow the mode.

Three mechanisms, and all three are load-bearing:

- **`enable_file_stream_work_stealing` must be off.** A file scan hands byte-range morsels
  to whichever partition goes idle, so the partition holding the start of the file is not
  reliably partition 0. Each partition's own rows stay contiguous and ascending either
  way, which is what makes this dangerous: with stealing on the answer comes out *nearly*
  ordered, and often exactly ordered, so it survives a spot check.
- **`collect_partitioned`, not `collect`.** `collect` puts a `CoalescePartitionsExec` on
  top, which takes whichever batch is ready. `collect_partitioned` sorts by partition
  index, and with stealing off that index is the file's order. It keeps every partition
  working, so this is not a serial read — do not reach for `target_partitions = 1`.
- **A `limit` is applied while reading, never by the plan.** `DataFrame::limit` also
  inserts a `CoalescePartitionsExec`, so a plan-level limit chooses arbitrary rows by
  construction. `query::first_rows_in_order` walks the partitions in index order and
  stops, which is both correct and cheap: a stream that is never polled never reads its
  byte range.

`query::tests` crosses these against row counts, row-group sizes, and files written with
page statistics, chunk statistics and none — because the guarantee cannot depend on how a
file was written, and which importer wrote a caller's file is not something this service
gets to assume. Two things such a test needs to stay honest:

- **Assert the scan was actually split.** DataFusion does not split a file below
  `repartition_file_min_size`, which is 1 MiB, so a small fixture reads in one partition
  and every ordering assertion passes while checking nothing. `query::execute` returns the
  partition count for exactly this.
- **Defeat compression.** An ascending column ZSTDs down to nothing, so a fixture needs a
  scattered one to reach that 1 MiB at all.

## The region on the sky

`region.rs` is the only place a shape becomes a predicate. It is a structured field rather
than an expression because the constraint has to be *recognised* to be planned on, and it
lowers to a `datafusion` `Expr` directly rather than to SQL text — there is nothing to
quote and no second parser.

- **Positions are degrees and unsuffixed; an extent names its unit.** `radius_deg` and
  `radius_arcsec`, exactly one of them. A bare `radius` is a plausible cone under either
  reading, the two differ by 3600, and no validation can tell them apart — so the field
  name is the only thing that can.
- **Compare haversines, never angles.** Haversine is monotone in the separation over the
  whole range one can take, so an `asin` on the way out is one more function per row and
  one more rounding for the same rows. It also makes the right ascension need no wrapping:
  the difference enters as `sin²(Δ/2)`, which a whole turn leaves alone, so a file writing
  `[0, 360)` and one writing `[-180, 180)` both work with no arithmetic on the column.
- **A bound around the exact test may only ever be too wide.** The coordinate ranges beside
  the haversine exist to let statistics prune row groups; they are `AND`ed onto the answer,
  so one that is too tight does not fail — it drops rows near the edge and returns fewer
  than the formula asked for. Hence the pad, and hence giving the right-ascension bound up
  entirely where `asin` gets too steep to trust: near a pole its own rounding exceeds the
  pad. `the_ra_reach_of_a_disk_contains_its_boundary` is what holds this, and it has to
  evaluate the extreme at the position angle it is known to be at — `cos θ = tan dec₀ tan
  r`. A scan over position angle walks past the peak near a pole and then passes with the
  guard removed.
- **Which columns hold a position comes from the request, never from the column names.**
  `ra_column` and `dec_column` are required alongside `region`. A file says nothing about
  which of its columns are coordinates, and a guess from conventional names would answer a
  different question than the one asked without saying so. A catalog's `properties` is the
  one thing allowed to supply them.
- **A shape that reads two ways is refused, not resolved.** A `box`'s `ra` runs eastward
  from the first value to the second, so `[350, 10]` and `[10, 350]` are different boxes
  and both are legal; the two values naming the *same* point is refused, because it reads
  equally as an empty box and as the whole sky. `hats` reads that case as the whole sky —
  a deliberate divergence, since nothing in the answer would say which reading was used.
- Every literal in these expressions is an `f64`, so a `Float32` coordinate column is
  widened before the arithmetic rather than the trigonometry running at single precision.
  `region::tests` crosses both column types against both right-ascension conventions.
- A caller's shape is validated once, into a `region::Shape`, and everything downstream
  reads that. Two readings of one field — the predicate's and the covering's — is how the
  two come to disagree about what `dec: [10, 10]` meant.

## The HEALPix covering

`healpix.rs` turns a shape into cells: which partitions of a catalog it can touch, and
which rows of one it cannot. `cdshealpix` computes the coverings and `moc` holds them.

- **Two sets, and each may only be wrong one way.** The outer set contains the shape, so a
  row outside it really is outside; the inner set is contained by it, so a row inside it
  really is inside. Every widening is allowed on the outer set and every loss on the inner
  one; the reverse of either is a wrong row. `a_covering_brackets_its_shape` asks both
  questions of a lattice over the whole sky, which is what catches a covering that drifted.
- **Only the outer set is about being right.** It contains the region, so nothing it
  rejects was wanted, and `outer AND exact` alone would be a complete and correct filter.
  The inner set never removes a row — it says which rows need no geometry. Keep that
  distinction when changing either: a bug in the outer set loses rows, a bug in the inner
  set returns rows that are not in the region, and only the first of those is a question
  about correctness of the *filter*.
- The row predicate is `inner OR (outer AND exact)` and the partition test uses both. What
  the inner half is worth per row is **not measured**: it costs two comparisons per inner
  range on every row and saves the geometry only where DataFusion's `OR` short-circuits,
  which needs the left side true for four rows in five. A batch well inside the region, in a
  file sorted by the column, is that lopsided — which is the case it is kept for. If it is
  ever dropped or defended further, do it with a benchmark.
- **Never a covering at the order the column is written at.** A covering fine enough to be
  exact along a boundary is of order 10⁷ cells for a one-degree circle at order 29, and a
  row tested against thousands of ranges costs more than the trigonometry that was being
  saved. Every depth here is capped well short of 29.
- **Choosing partitions and testing rows are two coverings, not one at two scales.**
  `Detail` is which. Choosing partitions is answered once per partition, so the range count
  is free and the depth follows the *catalog's* order — at the catalog's own order a
  partition the region merely touches is one cell and can only come back as a boundary, so
  it is taken a couple of orders finer. Testing rows puts every range into an expression
  every row meets, so the count is budgeted and the depth follows the *shape* — with a
  floor at the partition's own order, since `Norder` and `Npix` give a partition's exact
  span of cells without reading anything. Without the floor a region far larger than a
  partition is covered at a scale that cannot tell one part of that partition from another,
  and what survives `within` is a single range no row can fail. A change that makes one of
  these better at the other's expense has made something worse.
- **A range set is not a cheap membership test.** It lowers to `h BETWEEN … OR h BETWEEN …`,
  which DataFusion evaluates as two comparison kernels and an `OR` per range over every
  batch — linear in the ranges, no tree and no search. Sixty of them are more arithmetic per
  row than the haversine they were meant to spare it. So the row-level ranges exist to
  **skip row groups**, and are sized to that: a partition holds tens of groups and no set of
  ranges can skip more than exist. Measured, the bytes read were identical at budgets of 8,
  64 and 256 — only the expression's length differed.
- **The covering goes on the left of the `AND`.** DataFusion's `AND` inspects the left side
  first: all false skips the right entirely, and under a fifth true switches to evaluating
  the right on the selected rows alone. That is the mechanism by which the covering spares
  the trigonometry — not the covering's mere presence. Reversed, every row pays the sines.
  The optimizer would put it back, classing `BETWEEN` as cheap and `sin` as expensive, but
  an expression that depends on being corrected is one nobody can read.
- What the alternatives to a range set cost, checked against DataFusion 55 rather than
  assumed, since the answer is the library's and not ours:
  - **A hash set exists and is blind to statistics.** `h >> shift IN (cells…)` becomes an
    `Int64StaticFilter` — constant time per row. But `PruningPredicate` needs a column, and
    a shifted column is not one, so it prunes nothing. It could only ever be an addition
    beside a small range set, and it needs measuring first.
  - **Nothing searches, and sortedness is used only for I/O.** There is no tree or binary
    search in the expression layer. A sorted column pays off through the page index, which
    the ranges already drive. A merge against a table of ranges would be the asymptotically
    right shape, but `PiecewiseMergeJoinExec` takes a single inequality, is experimental,
    and would mean giving up the one-scan-with-a-filter plan.
  - **`ScalarUDFImpl::preimage` is the sanctioned way to say this.** A UDF that declares
    the interval `f(x) = v` inverts to has `f(col) = v` rewritten into `col >= lo AND
    col < hi`, which prunes. It covers comparisons and not `IN`, so a set of cells lands
    back on the range expression — by the optimizer's hand rather than ours.
- **Budget the boundary, not the area.** The interior of a shape merges into few ranges
  whatever the depth — the whole sky is one — so the range count follows the boundary's
  length. Sizing by area instead is the same thing up to a constant for a round shape and
  wrong for a thin one, and a strip of declination is an ordinary request.
- **No cone wider than a quarter turn.** `cone_coverage_approx` stops being a superset as
  the radius approaches a half turn: at 179 degrees it comes back missing a tenth of the
  cells. A disk larger than a hemisphere is the complement of the disk opposite it, and a
  declination band is cut at the equator so each half is measured from its nearer pole.
- **`zone_coverage` is the inner covering's, never the outer's.** Its walk along an edge
  drops wedges of the cell beyond it when the edge lies on a seam between base cells and
  the box reaches into a polar cap — and a box's edges are exactly where a caller writes a
  round number. Dropping cells is what an inner covering is allowed to do. For the outer
  one a box is the intersection of two supersets built from cones: its declination band,
  and the cones enclosing the pieces of its arc.
- **A HEALPix column is a name *and* an order.** HATS recommends `_healpix_29` and
  recommends nothing else about it, so the column may be called anything and be written at
  any order, in any integer type wide enough for it — an order-13 catalog fits `Int32`. The
  order is what says which cell a value is, so it is never inferred from the name: read at
  the wrong order every bound is one no row satisfies, which returns nothing rather than
  failing. `SpatialIndex::resolve` refuses a type too narrow for the order it was given.
- **The column is an accelerator, and that is a claim about every answer.** Naming it
  changes what a query costs and never which rows come back, which is why
  `region::tests` runs every case both ways over one fixture and asserts they agree —
  and why one test measures `data_bytes_read` to show the prefilter ran at all. A file
  sorted by the column skips row groups; one that is not gets the same rows, having only
  saved the trigonometry. Nothing checks for the sorting, because nothing depends on it.

## What a caller's file is like

**Nothing here may rely on how a HATS catalog happens to be written today.** Not the row
group size, not one row group per file, not which of column statistics, the page index and
a bloom filter it carries, not the compression, not the column order, not the writer.
`hats-import` is not the only importer, importers change, and a caller's file may predate
or postdate anything measured here.

That applies to code and to conclusions equally. A setting kept because "today's files
have no page index" is a setting that breaks quietly the week an importer starts writing
one — and one dropped for the same reason is worse, because nothing in the answer would
say the query got slower. A measurement over one file shape is a measurement of that
shape: `query::tests` and `tests/engine.rs` both cross their cases over how the file was
written for this reason, and a finding that holds on one shape and not another is a
finding about the shape.

What a file is observed to carry is worth writing down — it says what is worth asking an
importer for. It is never worth depending on.

## What an answer can say

A value the caller cannot tell apart from a different value is the failure this service
keeps finding, in a new place each time. It reads like data rather than like a fault, so
nothing downstream reports it.

- **JSON has no number for `NaN` or either infinity, and arrow's writer spells all three
  `null`.** Three values a file holds, reported as a fourth it does not — and in a
  photometric column all three are ordinary. `query::to_json` installs an `EncoderFactory`
  that writes them as the strings `"NaN"`, `"Infinity"` and `"-Infinity"`, which `float()`
  and `Number()` both read back. It takes over only a column that actually holds one, so
  ordinary data keeps the writer's own faster formatting; the cost of the check is a pass
  over the values, and of the formatting about 3% on a column that needs it.
- **A null is written rather than omitted.** `explicit_nulls` is off by default, which
  makes a row's keys depend on that row's own values and a null indistinguishable from a
  column the projection never asked for.
- **A float is formatted at its own width.** Widening an `f32` to an `f64` first prints
  `1.1` as `1.100000023841858` — the same value, and not the same answer.
- **The page sets these apart from the numbers**, italic and muted, rather than leaving a
  blank cell that reads as nothing much. `null` is marked in any column; the three strings
  count only in a float column, since elsewhere a string is just a string.

## Directory listings

A directory is served the way `apache` and `nginx` serve one: its own `index.html` if it
has one, otherwise every entry, ordered by name. No paging, no cap, no sort parameters.

- **`text/html` gets a page; everything else gets JSON.** `*/*` is what every client
  library sends, and it is not a request for markup. This is also why none of the
  content-negotiation crates is used: they resolve a wildcard *to* `text/html`, which is
  the right default for a website and the wrong one for a data service.
- **A name is the filesystem's, and it is encoded twice.** Into a url — where `/`, `%`
  and the delimiters must not survive literally, and `=` must, because HATS directories
  are called `Norder=5` — and into HTML, where a name is markup until it is escaped.
  Both encodings live in `listing.rs`; nothing outside it builds a url out of a name.
- **The page is scraped, so every link on it is a claim about the directory.** `fsspec`'s
  HTTP filesystem — and the `lsdb` clients above it — reads a directory by pulling every
  `href` out of the markup and keeping the ones below the url it asked for. So each entry
  stays a plain `<a href>` an expression can find, rather than a link a script assembles;
  and nothing else on the page may point below the directory. The breadcrumb and the
  parent row point upwards and are dropped, but a link offering a query on an entry would
  arrive at a client as a file that does not exist. Say such a thing in prose.
- **The page carries everything it needs.** No CDN, no webfont, no framework: a page that
  is blank on the network this service is built for is worse than a plain one. Inline the
  CSS, and let any script be small enough to inline and optional enough that the markup is
  complete without it.
- **A listing describes only what the same mount would serve.** A mount that does not
  follow symlinks does not list them either, since listing one would only advertise a
  404. `DirEntry::metadata` is an `lstat` and answers "a symlink" a second time; a mount
  that does follow them needs `fs::metadata` on the resolved path to learn what is
  behind one.

## What a mount tells a caller

A mount publishes a directory, not the machine it is on. What is on disk — the source
path, the layout above it, whether a name exists outside what the mount serves — is the
operator's business, and none of it may appear in an answer.

- **A local path never reaches a caller, and an error message is where one gets out.** A
  store names the path it was reading, so the message a store or a reader raises about a
  mounted file is not repeatable as-is. `ApiError::from_mount` is where that is turned
  into a message of this crate's own; the original goes to the log. Anything reading a
  mounted file goes through it, and it is not needed in API mode — there the path in the
  message is the caller's own url.
- **No status may describe a store the caller never named.** A mount has no origin behind
  it, so `502` blames a gateway that does not exist. Neither is it a `500`: the bytes of
  that same file are served without complaint when the url carries no query, so a failure
  to read it *as parquet* is a statement about the file. It is a `400`.
- **A refusal says the same thing whether or not the file exists.** A directory that
  cannot be listed and one that is not published answer alike, since the difference is
  itself something about the disk.

Proving any of this needs a case that reaches the arm in question, and the arms are not
obvious: a file whose footer will not parse, one that is empty, and one whose footer
describes rows that are not in it are three different errors with three different
statuses. `a_data_file_that_is_not_parquet_is_the_callers_mistake` carries all three, and
the last one has to be built from a file with more data than footer — strip a ten-row
fixture and the offsets still land inside what is left, so nothing reads past the end and
the test passes without the guarantee.

## Comments

Focused and informative. Say what the code does and what a reader could not work out
from reading it — a constraint, a failure mode, a reason a plausible alternative is
wrong.

Not a changelog: "now", "no longer", "used to", "moved here" say nothing to someone
seeing the file for the first time. That story goes in the commit message.

Not thinking-out-loud: no reasoning towards the decision, no defending it to a reviewer,
no restating what the line below already says.

## Tests

`cargo test` must pass with no network, no Docker and no credentials. Anything needing a
real server is a separate test binary that skips when its env vars are absent.

Do not run MinIO or any other container locally; the MinIO tests are CI's.

Check a signer by running it, not by reading it. `tests/credential_logging_canary.rs`
asserts each backend actually logged something, because a backend whose signer never ran
reads exactly like one that leaked nothing. The same goes for the network rules: prove a
guarantee has teeth by removing it and watching the test fail.

Known blind spot to work around, not to trust: `disable_config_load`'s second job —
stopping `AWS_ENDPOINT_URL` from redirecting a request — is not observable through a url
with no `endpoint` option, because virtual-host addressing turns a redirected endpoint
into `bucket.<host>`, which does not resolve. Every backend's equivalent guard shares
it.

## Before committing

`cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test
--all-targets`. `pre-commit run --all-files` runs all three.

Also `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items`, which
pre-commit does not run. It is where a doc link to a private item turns up, and clippy
does not see those.
