# What the clients do

A service is only as usable as the clients people point at it, and the clients do not
always agree with the standards or with each other. Everything here was found by running
this suite against services known to be correct, so each is about the client rather than
about anyone's service.

| | what happens | what follows |
|---|---|---|
| **pyvo drops `maxrec=0`** | `run_sync(query, maxrec=0)` tests the value for truth before sending it, so the query runs unlimited and the caller is not told | A user asking pyvo to inspect a table gets the whole table. Anything asking about `MAXREC=0` has to set the parameter by hand |
| **pyvo is silent about a truncation you asked for** | Given `MAXREC=n` and exactly `n` rows back it treats the overflow as expected and warns nothing | A service that omits the `OVERFLOW` marker looks identical from up there, so the marker is read off the document here. It still matters: the next client, which set no MAXREC and met the service's own default, depends on it |
| **The two clients disagree about a column's name** | One query, one service, one VOTable: pyvo says `source_id`, `stilts tapquery` says `SOURCE_ID` | A query written in TOPCAT and pasted into a notebook raises `KeyError`. Worth making `name` and `ID` agree on every `FIELD` we write |
| **A large table list stops pyvo dead** | `TAPService.tables` reads the whole VOSI document before anything else can be asked. Against a service publishing tens of thousands of tables it did not return in forty minutes | A client's first act on meeting a service is to read this, so whatever we publish there is on the critical path of every session |

Observed with pyvo 1.9.1, astropy 8.0.1 and STILTS 3.5-6 on 2026-09-16. All three move;
each of these is worth re-checking when Dependabot raises the lock.
