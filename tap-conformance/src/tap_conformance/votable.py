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
