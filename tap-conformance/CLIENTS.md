# What the clients do

A service is only as usable as the clients people point at it, and the clients do not
agree with the standards or with each other. Everything here was found by running this
suite against services known to be correct, so each is about the client rather than
about anyone's service.

Two of them are worked around in the suite. The rest are things to know before
publishing a TAP surface of our own.

| | what happens | what follows |
|---|---|---|
| **pyvo drops `maxrec=0`** | `run_sync(query, maxrec=0)` tests the value for truth before sending, so the query runs unlimited and the caller is not told | Anything asking about `MAXREC=0` sets the parameter by hand. A user asking pyvo to inspect a table gets the whole table |
| **pyvo is silent about a truncation you asked for** | Given `MAXREC=n` and exactly `n` rows back it treats the overflow as expected and warns nothing (`DALResults.check_overflow_warning`) | A service that omits the `OVERFLOW` marker entirely looks identical from up there, so the marker is read off the document instead. The marker still matters — it is what the *next* client, which set no MAXREC and met the service's own default, depends on |
| **The two clients disagree about a column's name** | One query, one service, one VOTable: pyvo says `source_id`, `stilts tapquery` says `SOURCE_ID` | A query written in TOPCAT and pasted into a notebook raises `KeyError`. The comparison here folds case. Worth making `name` and `ID` agree on every `FIELD` we write |
| **pyvo's `examples` are queries, not text** | `TAPService.examples` returns `TAPQuery` objects keyed `REQUEST`/`LANG`/`QUERY`, not dicts of the marked-up fields | An examples document is judged by whether its queries run, which is the right test anyway |
| **`taplint` is a linter, `tapquery` is the client** | `taplint` composes its own queries from the metadata and is nobody's way of getting data; `stilts tapquery` is what TOPCAT runs underneath | Both are used here, for different questions. A service can pass the linter and hand the client something it cannot parse |
| **A large table list stops pyvo dead** | `TAPService.tables` reads the whole VOSI document before anything else can be asked. Against VizieR, which publishes tens of thousands of tables, it did not return in forty minutes | A client's first act on meeting a service is to read this, so whatever we publish there is on the critical path of every session. VizieR is left out of the reference list for this reason |

## Versions

pyvo 1.9.1, astropy 8.0.1, STILTS 3.5-6, observed 2026-09-16. All three move; a finding
here is about the version beside it and is worth re-checking when Dependabot raises the
lock.
