# What four TAP services actually implement

The suite run against four independent implementations. Not to grade them: to find out
what the standard means in practice, which is the thing a specification cannot tell you.
A capability all four have is one clients depend on and we will be measured against. A
check none of them passes is almost certainly a check reading the standard more strictly
than anyone implements it — and it is the check that should change, not four services.

Four rather than one because a single reference cannot tell a service's mistake from a
check's. They are four separate stacks, and two of them serve Gaia DR3, so a
disagreement can be read against the same rows:

| | |
|---|---|
| **ESA Gaia** | `https://gea.esac.esa.int/tap-server/tap` |
| **ARI-Gaia** | `https://gaia.ari.uni-heidelberg.de/tap` |
| **IRSA** | `https://irsa.ipac.caltech.edu/TAP` |
| **MAST** | `https://mast.stsci.edu/vo-tap/api/v0.1/caom/` |

VizieR (`https://tapvizier.cds.unistra.fr/TAPVizieR/tap`) was tried and left out: a
client reads the whole VOSI table list before it can ask anything, and tens of thousands
of tables do not arrive in any time worth waiting. The GAVO data centre and CADC do not
resolve from here and are worth another attempt from a network that can see them.

| capability | ESA Gaia | ARI-Gaia | IRSA | MAST |
|---|---|---|---|---|
| VOSI availability | yes | yes | yes | yes |
| VOSI capabilities | yes | yes | 5/6 | yes |
| VOSI tables | yes | yes | 1/2 | yes |
| TAP_SCHEMA | 2/3 | 2/3 | yes | yes |
| sync query | yes | 5/6 | 5/6 | yes |
| ADQL | yes | yes | 4/5 | yes |
| output formats | 6/7 | 4/7 | 6/7 | 3/7 |
| MAXREC and overflow | 6/8 | 7/8 | 5/8 | yes |
| error documents | 5/6 | yes | 1/6 | yes |
| examples | yes | yes | 1/2 | 1/2 |
| async | yes | yes | yes | yes |
| upload | yes | yes | no | 1/2 |

`n/m` is checks passed out of checks that ran. Totals: ESA 45 of 50, ARI-Gaia 44, MAST 44,
IRSA 34. Observed 2026-09-16 with pyvo 1.9.1, astropy 8.0.1 and STILTS 3.5-6; regenerate
with `tap-conformance-survey --refresh-references`.

## What this settles

**Everything core is universal.** Availability, capabilities, table metadata,
`TAP_SCHEMA`, synchronous queries, ADQL, VOTable output, `MAXREC`, error documents and
async are implemented by all four. There is no part of the mandatory surface that
established services treat as optional, so there is nothing here to argue our way out
of.

**`/async` is implemented by all four.** It is the one thing this service plans not to
have, and no reference service agrees. That is the cost of the decision stated in
numbers rather than in prose.

**Upload is genuinely optional.** IRSA has none and MAST half. It is declared in the
capabilities document and clients read it there, which is the shape a feature takes when
the ecosystem has not settled it — and the shape to copy for anything we do not
implement.

**CSV and TSV are not universal.** ESA and IRSA answer both; ARI-Gaia and MAST answer
neither. A SHOULD in the standard is a SHOULD in practice.

## Edge cases

Observed directly and reproducible; each is a decision rather than a verdict.

| service | what happens | why it matters |
|---|---|---|
| **all four** | an unknown `RESPONSEFORMAT` is answered in VOTable rather than refused | either TAP §2.7.3 does not require the refusal or nobody implements it. Asking it here would be stricter than the whole ecosystem, so that check should go or become a note |
| **ESA** | an unclosed string literal returns 500 and an HTML page | DALI §4.4 asks for an error document, and its other malformed queries do get one — so this is one path through a parser |
| **ESA** | `SELECT *` from `TAP_SCHEMA` breaks pyvo: a byte astropy decodes as ASCII | the only finding here that stops a session dead. STILTS reads the same query |
| **IRSA** | one of six error checks passes — most failures do not come back as DALI error documents | a client cannot read the reason a query failed |
| **MAST** | a VOTable is labelled `text/xml`, not `application/x-votable+xml` | a client choosing its parser by content type picks wrong |

One more, about this suite rather than anyone's service: `ABS`, `CEILING`, `FLOOR` and
`SQRT` failed against three of four services until the alias in the check was changed
from `value`, which is a reserved word. That is the clearest argument for asking several
services — with one reference the reading would have been "three services are broken".

Findings about the clients rather than the services are in `CLIENTS.md`.
