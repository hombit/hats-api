"""`python -m query_benchmark`, which is what the tests drive."""

from __future__ import annotations

import sys

from query_benchmark.run import main

if __name__ == "__main__":
    sys.exit(main())
