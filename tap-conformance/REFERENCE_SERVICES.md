# What the reference services get wrong

The suite was pointed at services that have been answering TAP correctly for years,
which is how it was checked before this repository had any TAP to check. That turned up
things those services get wrong — worth writing down, because each one is a decision
waiting to be made here: either the check is too strict and goes, or the check is right
and this service should not copy what the reference does.

Nothing here is a bug report filed anywhere. It is a list to review.

**Observed** 2026-09-16, against the ESA Gaia Archive
(`https://gea.esac.esa.int/tap-server/tap`), with pyvo 1.9.1 and STILTS 3.5-6. A second
service was tried and could not be reached that day, so every finding below has one
source and none of them is corroborated.

Each row says what was seen and how confident the reading of the standard is. The
confidence is about the *standard*, not the observation — the observations are
reproducible with the commands at the foot of this file.

## Found by the pyvo checks

### 1. A malformed query is answered with an HTML page

`SELECT * FROM tap_schema.tables WHERE table_name = 'x` (an unclosed string literal)
comes back as **HTTP 500** with `Content-Type: text/html;charset=UTF-8` and a styled
error page.

DALI 1.1 §4.4 asks for a VOTable carrying `<INFO name="QUERY_STATUS" value="ERROR">`.
A client that gets HTML has nothing to show a user but a stack trace.

Two other malformed queries — `SELECT FROM WHERE` and an unknown table — *are* answered
with a correct error document, so this is one path through their parser rather than a
missing feature. STILTS `taplint` sends its own deliberately-broken queries (`DUFF`) and
passed the service, because it checks that the query failed rather than what the failure
was.

**Confidence: high.** DALI is explicit about the error document.

### 2. `MAXREC=-1` is answered rather than refused

`MAXREC=-1` returns **200** and the full result, marker-free.

DALI 1.1 §3.4 defines `MAXREC` as a non-negative integer. What a service must do with a
value outside that is the part worth checking in the text before treating this as a
violation — it is the general "a parameter is honoured or refused, never dropped" rule
rather than a sentence about `MAXREC`.

**Confidence: medium.** The observation is certain; the obligation needs the text.

### 3. `MAXREC=lots` is answered rather than refused

The same, for a value that is not a number at all. Returns **200** and the full result.

This one is harder to argue away than the negative case: there is no reading of `lots`
as a row count, so the parameter was dropped.

**Confidence: medium-high.**

### 4. An unknown `RESPONSEFORMAT` is answered in VOTable

`RESPONSEFORMAT=application/x-nonsense` returns **200** and a VOTable.

TAP 1.1 §2.7.3 is where the obligation would be. A client that asked for a format and
got a different one parses the wrong thing, and finds out somewhere further from the
cause than an error would have put it.

**Confidence: medium.** Needs the text.

### 5. `SELECT *` from `TAP_SCHEMA` breaks the Python client

```
pyvo.dal.exceptions.DALFormatError: UnicodeDecodeError:
'ascii' codec can't decode byte 0xa0 in position 404: ordinal not in range(128)
```

A `0xa0` — a non-breaking space, almost certainly inside a column description — in a
document astropy ends up decoding as ASCII. The same query through STILTS is fine, which
is exactly why both clients are in this suite: this is invisible from the validator's
side and fatal from the other one.

Whether the fault is the service's (a mis-declared encoding, or a `Content-Type` without
a charset) or astropy's (defaulting to ASCII where the XML declaration says otherwise) is
the thing to work out. It is the finding most worth chasing, being the only one here that
stops a user's session dead.

**Confidence: high that something is wrong; unclear whose.**

## Found by STILTS taplint

Counts from one run over two tables (`maxtable=2`), so these are a sample of a 248-table
service rather than a census.

| stage | | |
|---|---|---|
| `TME`, `TMS` | 5 × `TNTN` | Table names whose schema component is a reserved word — `external.apassdr9` and four siblings — published undelimited. taplint's advice is to publish them as `"external".apassdr9` |
| `UUC` | 3 × `UCDX` | UCDs that are not in the vocabulary |
| `UUC` | 5 × `VUNE`, 5 × `VUNR` | Units that do not parse as VOUnits, or are discouraged spellings |
| `EXA` | 1 × `EXVC`, 3 × `EXVL`, 4 × `YDNM` | Errors and warnings in the DALI examples markup |
| `TMV` | 1 × `UNSC` | A schema-validation warning on the table metadata |
| `CAP` | 1 × `SVRV` | A server-identification warning |

`TMC` — the stage that cross-checks `/tables` against `TAP_SCHEMA` — passed clean, as did
both capability stages and availability.

## Found about the clients themselves

Not service faults, and worked around in the suite rather than reported as findings:

- **pyvo 1.9.1 drops `maxrec=0`.** `run_sync(query, maxrec=0)` tests the value for truth
  before sending it, so the query runs unlimited and the caller is never told. Anything
  asking about `MAXREC=0` has to set the parameter by hand.
- **pyvo 1.9.1 does not warn about a truncation the caller asked for.** Given `MAXREC=n`
  and exactly `n` rows back it treats the overflow as expected and stays silent
  (`DALResults.check_overflow_warning`). A service that omits the `OVERFLOW` marker
  altogether is therefore indistinguishable from one that writes it, from up there.

## What ESA does that is worth copying

The list above is one-sided by construction. On everything else it was asked, the
service was right, including the two things hardest to get right:

- The `OVERFLOW` marker is written, and written **after** the `TABLE`, which is where
  DALI 1.1 §4.4 puts it.
- All three VOSI documents validate against their schemas, and `/tables` and
  `TAP_SCHEMA` agree with each other.

## Reproducing

```sh
cd tap-conformance
uv run pytest -c pyproject.toml --base-url https://gea.esac.esa.int/tap-server/tap \
    --skip-taplint --report-dir /tmp/esa                 # the five above, in minutes
uv run pytest -c pyproject.toml --base-url https://gea.esac.esa.int/tap-server/tap \
    -k stage --report-dir /tmp/esa-taplint               # the validator, in about two
```

The second writes `taplint.json` beside its report, which is the validator's own output
and holds every message these counts summarize.
