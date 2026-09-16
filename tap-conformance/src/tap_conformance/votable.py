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
    """Raise unless the service refused, in either of the two ways it is allowed to.

    A refusal is legible one of two ways, and TAP 1.1 §3.3 permits both: an HTTP error
    status, or a document that says `QUERY_STATUS="ERROR"`. It is explicit that a
    synchronous service "may use an appropriate HTTP status code, including 200" — so
    demanding a 4xx would fail a service that answers 200 with a proper error document,
    which is conforming. An earlier version of this did exactly that.

    What both shapes exclude is the answer that matters: `200` carrying an ordinary
    successful result, which is a parameter read and thrown away. That is what the four
    reference services do with an unsupported `RESPONSEFORMAT`, against DALI 1.1 §3.4.3
    telling them to fail.

    The status alone is not enough either, or a service with no TAP in it passes every
    such check on its blanket 404s — which is how the vacuous passes were found. So a
    404 with no error document is read as a missing resource rather than a refusal.
    """
    body = response.content[:8000]
    said = [status.value for status in statuses(body)]
    if "ERROR" in said:
        return f"refused with {response.status_code} and QUERY_STATUS=ERROR"
    if response.status_code == 404:
        raise AssertionError(
            f"404 with no error document, so nothing read the parameter: {body[:160]!r}"
        )
    if response.status_code >= 400:
        return f"refused with {response.status_code}, no error document in the body"
    raise AssertionError(
        f"answered {response.status_code} with no error marker, so the parameter was "
        f"dropped rather than refused"
    )
