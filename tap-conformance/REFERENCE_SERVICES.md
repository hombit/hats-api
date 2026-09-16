# How other services read the standard

Where this suite's checks and a long-running TAP service disagree. Every row is a
question about **the check**, not a verdict on the service: a clause read one way by
people who had to ship something is a clause that has been decided in practice, and a
check that no established service passes is more likely to be reading the standard too
strictly than to have found five independent bugs.

So each row is here to be settled one way or the other — the check is wrong and goes, or
the check is right and this service should not follow the same reading. Neither has been
decided yet. Findings about the clients are in `CLIENTS.md`.

**One source each, which is the first thing to fix.** Observed 2026-09-16 against the
ESA Gaia Archive with pyvo 1.9.1 and STILTS 3.5-6.
`tap-conformance-survey --refresh-references` asks five services, and `report/matrix.md`
is where a check everybody fails can be told from one only this service fails. This file
predates that and has not been re-read against it.

| | what was observed | what the check reads it against | is the check right? |
|---|---|---|---|
| **Malformed query answered as HTML** | `… WHERE table_name = 'x` (unclosed literal) → **500**, `text/html`, a styled error page | DALI §4.4: a VOTable with `QUERY_STATUS="ERROR"`. Two other malformed queries do get one, so this is one path through a parser rather than a position | probably. DALI is explicit |
| **`MAXREC=-1` answered** | **200** and the full result | DALI §3.4 defines MAXREC as a non-negative integer | unclear — what a service owes an out-of-range value is not spelled out in one sentence |
| **`MAXREC=lots` answered** | **200** and the full result | The same, for a value that is not a number | more likely than the row above: there is no reading of `lots` as a row count |
| **Unknown `RESPONSEFORMAT` answered** | `application/x-nonsense` → **200** and a VOTable | TAP §2.7.3 | needs the text. The argument for it is that a client which asked for one format and got another parses the wrong thing far from the cause |
| **`SELECT *` from `TAP_SCHEMA` fails in the client** | `UnicodeDecodeError: 'ascii' codec can't decode byte 0xa0` — a non-breaking space in a document astropy decodes as ASCII; STILTS reads the same query | Not a clause at all. It may be a mis-declared encoding, a `Content-Type` with no charset, or astropy's default | the check is right that something is wrong; whose is open |

STILTS `taplint` adds these, from one run over two tables of a 248-table service. They
are the validator's readings rather than this suite's:

| stage | | |
|---|---|---|
| `TME`, `TMS` | 5 × `TNTN` | Table names whose schema part is a reserved word (`external.apassdr9` and four siblings), published undelimited |
| `UUC` | 3 × `UCDX`, 10 × unit warnings | UCDs outside the vocabulary; units that do not parse as VOUnits |
| `EXA` | 1 × `EXVC`, 7 warnings | The DALI examples markup |
| `TMV`, `CAP` | 1 warning each | Table-metadata schema validation, server identification |

## What the same run settles

Checks that passed are worth as much as the ones that did not: a reading confirmed by a
service with a decade of clients is a reading to follow here.

- `OVERFLOW` is written, and written **after** the `TABLE`, which is where DALI §4.4 puts
  it — so that is what this service should do.
- All three VOSI documents validate against their schemas, and `/tables` and
  `TAP_SCHEMA` agree with each other.
- `TMC`, both capability stages and availability pass clean.

## Reproducing

```sh
cd tap-conformance
uv run pytest -c pyproject.toml --base-url https://gea.esac.esa.int/tap-server/tap \
    --skip-taplint --report-dir /tmp/esa        # the client checks, minutes
uv run pytest -c pyproject.toml --base-url https://gea.esac.esa.int/tap-server/tap \
    -k stage --report-dir /tmp/esa-taplint      # the validator, about two
```

The second writes `taplint.json` beside its report — the validator's own output, holding
every message the counts above summarize.
