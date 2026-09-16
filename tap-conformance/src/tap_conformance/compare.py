"""Two answers to one query, from two services.

What is compared is what a client can see: the number of rows, the names and order of
the columns, which values are null, and the values themselves. Not the XML — two
correct services write different VOTables for the same rows, and a difference there is
a difference in serialization rather than in the answer.
"""

from __future__ import annotations

import numpy as np
from astropy.table import Table


def masks(column) -> np.ndarray:
    found = getattr(column, "mask", None)
    if found is None:
        return np.zeros(len(column), dtype=bool)
    return np.asarray(found, dtype=bool)


def compare(expected: Table, actual: Table, rtol: float = 0.0) -> str:
    """Raise unless the two answers say the same thing.

    `rtol` is what a value may differ by: zero for anything a service copies out of its
    own storage, and loose for an aggregate, where the order of summation is the
    implementation's business and not something either service promises.

    Column names are compared case-insensitively. A service publishes the spelling it
    wants queried and the two here were written by different people; what would make a
    difference in case worth reporting is the metadata resources saying one thing and an
    answer saying another, which is a question for the tests about metadata.
    """
    if len(expected) != len(actual):
        raise AssertionError(
            f"{len(expected)} rows expected, {len(actual)} returned"
        )
    if [name.lower() for name in expected.colnames] != [
        name.lower() for name in actual.colnames
    ]:
        raise AssertionError(
            f"columns {expected.colnames} expected, {actual.colnames} returned"
        )

    for left_name, right_name in zip(expected.colnames, actual.colnames, strict=True):
        left, right = expected[left_name], actual[right_name]
        left_mask, right_mask = masks(left), masks(right)
        if not np.array_equal(left_mask, right_mask):
            raise AssertionError(
                f"{left_name}: {int(left_mask.sum())} nulls expected, "
                f"{int(right_mask.sum())} returned"
            )
        present = ~left_mask
        left_values = np.asarray(left)[present]
        right_values = np.asarray(right)[present]
        if left_values.dtype.kind in "fc" and right_values.dtype.kind in "fc":
            # NaN is a value this data holds, and it is equal to itself here: a
            # photometric column is full of them and they are not nulls.
            same = np.isclose(
                left_values.astype(float),
                right_values.astype(float),
                rtol=rtol,
                atol=0.0,
                equal_nan=True,
            )
        else:
            same = left_values.astype(str) == right_values.astype(str)
        if not np.all(same):
            first = int(np.argmax(~same))
            raise AssertionError(
                f"{left_name}: row {first} is {right_values[first]!r}, "
                f"expected {left_values[first]!r} "
                f"({int((~same).sum())} of {len(same)} rows differ)"
            )
    return f"{len(actual)} rows, {len(actual.colnames)} columns match"
