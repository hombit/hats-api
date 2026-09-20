"""Whether the two routes answered the same thing."""

from __future__ import annotations

from typing import Any

import pyarrow as pa


def assert_same(direct: Any, via_api: Any) -> None:
    """Fail unless both answers hold the same values, types, index and row order.

    Compared through pyarrow rather than numpy because these columns hold lists — ZTF
    carries a whole light curve per row — and a cell holding an array is one `==` has no
    answer for. The index comes with them, `_healpix_29` being where the row is on the
    sky. Nothing is sorted or reindexed: rows coming back rearranged is the failure worth
    catching most.
    """
    left, right = (pa.Table.from_pandas(f, preserve_index=True) for f in (direct, via_api))

    assert left.schema == right.schema, f"\ndirect:   {left.schema}\nhats_api: {right.schema}"
    assert left.num_rows == right.num_rows, f"{left.num_rows} rows, then {right.num_rows}"
    assert left.equals(right), (
        f"\ndirect:   {left.slice(0, 3).to_pydict()}"
        f"\nhats_api: {right.slice(0, 3).to_pydict()}"
    )
