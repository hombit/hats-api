"""VOSI capabilities and TAPRegExt — TAP 1.1 section 2.4, TAPRegExt 1.0.

What a service says it can do. A client chooses its interface, its language and its
output format from this document, so anything wrong in it is wrong before a query is
ever sent — and anything *missing* from it a client will not try.
"""

from __future__ import annotations

import pytest

from tap_conformance.taplint import assert_clean

TAP_STANDARD_ID = "ivo://ivoa.net/std/tap"


def standard_ids(tap) -> list[str]:
    return [str(capability.standardid).lower() for capability in tap.capabilities]


def test_tap_capability(tap, record_property):
    """The TAP standard id is among the declared capabilities."""
    declared = standard_ids(tap)
    record_property("detail", ", ".join(declared) or "none")
    assert any(found.startswith(TAP_STANDARD_ID) for found in declared), (
        f"no TAP capability among {declared}"
    )


def test_vosi_capabilities(tap, record_property):
    """The three VOSI resources are declared alongside it."""
    declared = standard_ids(tap)
    wanted = {
        "capabilities": "ivo://ivoa.net/std/vosi#capabilities",
        "availability": "ivo://ivoa.net/std/vosi#availability",
        "tables": "ivo://ivoa.net/std/vosi#tables",
    }
    missing = [
        name
        for name, identifier in wanted.items()
        if not any(found.startswith(identifier) for found in declared)
    ]
    record_property("detail", f"declared: {', '.join(declared) or 'none'}")
    assert not missing, f"no capability declared for {', '.join(missing)}"


def test_query_language(tap, record_property):
    """ADQL is declared as a supported language, which TAP makes mandatory."""
    languages = [
        language.name
        for capability in tap.capabilities
        for language in getattr(capability, "languages", [])
    ]
    record_property("detail", f"languages={languages}")
    assert languages, "the TAP capability declares no query language"
    assert any("ADQL" in str(name).upper() for name in languages), (
        f"ADQL is not among {languages}"
    )


def test_output_formats(tap, record_property):
    """Every format the service will answer in is declared, VOTable among them."""
    formats = [
        str(output.mime)
        for capability in tap.capabilities
        for output in getattr(capability, "outputformats", [])
    ]
    record_property("detail", f"formats={formats}")
    assert formats, "the TAP capability declares no output format"
    assert any("votable" in media.lower() for media in formats), (
        f"VOTable is not among the declared formats: {formats}"
    )


def test_output_limits(tap, record_property):
    """The row limits are declared, which is what a client reads MAXREC against."""
    default, hard = tap.maxrec, tap.hardlimit
    record_property("detail", f"default={default} hard={hard}")
    assert default is not None or hard is not None, (
        "neither a default nor a hard output limit is declared"
    )


def test_declared_interfaces_exist(tap, service, record_property):
    """Nothing is advertised that is not there.

    A client picks its interface out of this document and has no way back: told about
    an endpoint that answers 404, it fails at the point of submitting a job rather
    than at the point of choosing. So an interface declared here is a promise, and a
    service that does not implement one is better off declaring nothing.
    """
    urls = [
        str(url)
        for capability in tap.capabilities
        for interface in getattr(capability, "interfaces", [])
        for url in getattr(interface, "accessurls", [])
    ]
    record_property("detail", ", ".join(urls) or "no access url declared")
    missing = []
    for url in urls:
        if not str(url).startswith(service.base_url.rsplit("/", 1)[0]):
            continue
        import urllib.error
        import urllib.request

        try:
            urllib.request.urlopen(url, timeout=30).read(1)
        except urllib.error.HTTPError as answered:
            if answered.code == 404:
                missing.append(f"{url} → 404")
        except OSError as unreachable:
            missing.append(f"{url} → {unreachable}")
    assert not missing, f"declared but absent: {'; '.join(missing)}"


@pytest.mark.taplint("CPV")
def test_schema(stage, record_property):
    """The capabilities document validates against its XML schema."""
    record_property("detail", stage.summarize())
    assert_clean(stage)


@pytest.mark.taplint("CAP")
def test_content(stage, record_property):
    """Its TAP and TAPRegExt content is what those standards ask for."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
