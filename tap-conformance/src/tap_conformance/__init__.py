"""What a TAP conformance run needs beside the checks themselves.

The checks are in `tests/`, one file per part of the standards. Here is what they are
run against and what a run leaves behind: starting the service (`service`), the STILTS
validator (`taplint`), comparing an answer against a reference one (`compare`), the
report (`report`), and the download the comparisons read (`fetch`).
"""
