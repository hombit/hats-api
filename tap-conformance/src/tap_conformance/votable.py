"""Reading the parts of a VOTable that carry the protocol rather than the data.

`pyvo` parses the rows and `astropy` parses the document, and neither answers the
question DALI 4.4 asks: *where* a QUERY_STATUS sits. `OK` goes before the table and
`OVERFLOW` after it, and a service that writes the marker in the wrong place has
written a document a streaming client cannot read the way the standard intends.

So this is the one place the suite looks at the bytes. It is a few lines of text
searching rather than an XML parse, which is enough for a question about order.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

STATUS = re.compile(
    r"<INFO\b[^>]*\bname\s*=\s*[\"']QUERY_STATUS[\"'][^>]*>", re.IGNORECASE
)
VALUE = re.compile(r"\bvalue\s*=\s*[\"']([^\"']*)[\"']", re.IGNORECASE)
TABLE_END = re.compile(r"</TABLE\s*>", re.IGNORECASE)


@dataclass(frozen=True)
class Status:
    """One QUERY_STATUS, and which side of the table it was written on."""

    value: str
    after_table: bool

    def __str__(self) -> str:
        return f"{self.value} ({'after' if self.after_table else 'before'} the table)"


def statuses(body: str | bytes) -> list[Status]:
    """Every QUERY_STATUS in the document, in the order they appear."""
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    ends = [match.end() for match in TABLE_END.finditer(body)]
    last_table_end = ends[-1] if ends else len(body)
    found = []
    for match in STATUS.finditer(body):
        value = VALUE.search(match.group(0))
        found.append(
            Status(
                value=value.group(1).upper() if value else "",
                after_table=match.start() > last_table_end,
            )
        )
    return found


def overflow(body: str | bytes) -> Status | None:
    """The overflow marker, if the document carries one."""
    for status in statuses(body):
        if status.value == "OVERFLOW":
            return status
    return None


def refusal(response) -> str:
    """Raise unless this is a service refusing something, rather than a missing route.

    The difference matters more than it looks. A check that a bad parameter is refused
    is satisfied by any 4xx if it only reads the status — and a service with no TAP at
    all answers 404 to everything, so every such check passes against nothing. Which is
    how this was found: the suite scored points against a service that implements none
    of the protocol.

    DALI §4.4 says what a refusal is — a VOTable carrying `QUERY_STATUS="ERROR"` — so
    requiring it is both the stricter check and the correct one.
    """
    body = response.content[:8000]
    if response.status_code < 400:
        raise AssertionError(f"answered {response.status_code} rather than refusing")
    if b"<VOTABLE" not in body.upper():
        raise AssertionError(
            f"refused with {response.status_code} but not as a VOTable, so this is a "
            f"resource that is missing rather than a parameter that was read: "
            f"{body[:160]!r}"
        )
    found = [status.value for status in statuses(body)]
    if "ERROR" not in found:
        raise AssertionError(f'no QUERY_STATUS="ERROR"; found {found or "none"}')
    return f"refused with {response.status_code}, VOTable with QUERY_STATUS=ERROR"
