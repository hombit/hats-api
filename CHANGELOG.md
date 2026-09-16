# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Release dates are in the UTC time zone.

## [Unreleased]

### Added

- `GET /robots.txt`, and `[server] serve_mounted_robots_txt` to answer with a mounted one instead.
- `hf://` urls, with the `token` storage option and `[api.access.hf]`.
- `[[tap.table]]`, the HATS catalogs published over IVOA's Table Access Protocol. [#68](https://github.com/hombit/hats-api/pull/68)
- `{api.prefix}/tap/sync`, `GET` and `POST`, taking `QUERY`, `LANG`, `RESPONSEFORMAT`, `FORMAT`, `MAXREC`, `RUNID` and `REQUEST`. [#68](https://github.com/hombit/hats-api/pull/68)
- `{api.prefix}/tap/capabilities`, `/tap/availability`, `/tap/tables` and `/tap/tables/{name}`. [#68](https://github.com/hombit/hats-api/pull/68)
- `TAP_SCHEMA.schemas`, `.tables`, `.columns`, `.keys` and `.key_columns`, queryable through `/tap/sync`. [#68](https://github.com/hombit/hats-api/pull/68)
- ADQL 2.0's coordinate system argument, `POINT('ICRS', ra, dec)`. [#68](https://github.com/hombit/hats-api/pull/68)

### Changed

- **Breaking**: `[server] serve_index_html` renamed to `[server] serve_mounted_index_html`.
- An unquoted name in `/api/v1/adql` is matched case-insensitively, as ADQL has it. [#68](https://github.com/hombit/hats-api/pull/68)
- A VOTable `FIELD` carries its name as `ID` as well. [#68](https://github.com/hombit/hats-api/pull/68)

### Deprecated

--

### Removed

--

### Fixed

--

### Security

--

## [0.0.7] - 2026-09-16

### Added

- `[server] user_agent`, the `User-Agent` on every request this service makes. [#65](https://github.com/hombit/hats-api/pull/65)
- `[server] contact`, written after the name in `User-Agent`, in `Server`, at the foot of a generated listing, and as `info.contact` in the API description. [#65](https://github.com/hombit/hats-api/pull/65)
- A `Server` response header, under `[server] show_version`. [#65](https://github.com/hombit/hats-api/pull/65)

### Changed

- The `headers` storage option refuses `User-Agent`. [#65](https://github.com/hombit/hats-api/pull/65)

### Removed

- **Breaking** `POST {api.prefix}/expr/parquet`, `/expr/hats` and `/expr/hats/plan`, with their `select` and `where` fields; use the `simple` routes or `POST {api.prefix}/adql`. [#57](https://github.com/hombit/hats-api/pull/57)

## [0.0.6] - 2026-09-15

### Added

- `format=csv` and `format=tsv` on every query route, as `text/csv;header=present` and `text/tab-separated-values`. [#52](https://github.com/hombit/hats-api/pull/52)
- `dsv_null_value`, what a null is written as under `format=csv` and `format=tsv`. [#53](https://github.com/hombit/hats-api/pull/53)

## [0.0.5] - 2026-09-15

### Added

- `POST {api.prefix}/adql`, taking an ADQL statement over tables the request declares, each a parquet file or a HATS catalog. [#36](https://github.com/hombit/hats-api/pull/36), [#37](https://github.com/hombit/hats-api/pull/37), [#41](https://github.com/hombit/hats-api/pull/41), [#47](https://github.com/hombit/hats-api/pull/47)
- A crossmatch, `1 = CONTAINS(POINT(b.ra, b.dec), CIRCLE(a.ra, a.dec, r))`, and `DISTANCE(...)` as a value in degrees. [#47](https://github.com/hombit/hats-api/pull/47)
- `RAND()`, on the ADQL route and refused everywhere else. [#41](https://github.com/hombit/hats-api/pull/41)
- The numeric functions — `sqrt`, `log10`, `power`, `abs`, `degrees`, the trigonometric ones — are callable in `select` and `where`. `log` is refused as ambiguous. [#30](https://github.com/hombit/hats-api/pull/30), [#33](https://github.com/hombit/hats-api/pull/33)
- `[limits] max_query_memory_bytes`, `1GiB` by default. [#41](https://github.com/hombit/hats-api/pull/41)

### Changed

- **Breaking** The `box` region is now `zone`, ADQL spelling a different shape `BOX`. [#28](https://github.com/hombit/hats-api/pull/28)
- **Breaking** `[limits] max_request_body_bytes` is `2MiB` by default, down from `16MiB`. [#44](https://github.com/hombit/hats-api/pull/44)
- **Breaking** A query over `max_partitions`, `max_bytes_fetched` or `max_rows` answers `422` instead of `413`. [#42](https://github.com/hombit/hats-api/pull/42)

### Fixed

- A `region` of a few hundred shapes, or a `moc` of a few hundred ranges, aborted the process with a stack overflow instead of answering. [#43](https://github.com/hombit/hats-api/pull/43)
- A `zone` whose declination band lies near a pole dropped the connection instead of answering. [#45](https://github.com/hombit/hats-api/pull/45), [cds-healpix-rust#27](https://github.com/cds-astro/cds-healpix-rust/issues/27)

## [0.0.4] - 2026-09-13

### Added

- `[limits] max_request_body_bytes`, `16MiB` by default. [#24](https://github.com/hombit/hats-api/pull/24)
- Request bodies sent with `Content-Encoding: gzip`, `br` or `zstd`. [#25](https://github.com/hombit/hats-api/pull/25)

## [0.0.3] - 2026-09-13

### Added

- `[server] serve_index_html`, on by default. [#21](https://github.com/hombit/hats-api/pull/21)

### Changed

- The page's astropy snippets ask for a VOTable. [#19](https://github.com/hombit/hats-api/pull/19)

## [0.0.2] - 2026-09-12

### Added

- Request timeout, `[limits] max_request_seconds`, 90 by default. [#15](https://github.com/hombit/hats-api/pull/15)

## [0.0.1] - 2026-09-12

### Added

Initial release.

[Unreleased]: https://github.com/hombit/hats-api/compare/v0.0.7...HEAD
[0.0.7]: https://github.com/hombit/hats-api/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/hombit/hats-api/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/hombit/hats-api/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/hombit/hats-api/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/hombit/hats-api/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/hombit/hats-api/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/hombit/hats-api/releases/tag/v0.0.1
