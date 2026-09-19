# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Release dates are in the UTC time zone.

## [Unreleased]

### Added

- `[[mount]] source` takes a url in any scheme this service reads — `s3://`, `gs://`, `az://`, `https://`, `webdav://`, `hf://` — as well as a local path.
- `[[mount]] storage`, the options that reach a `source` in a store: the same names a request writes beside its url.
- A served mount over a store answers `Range`, `If-None-Match` and `HEAD` against the origin, and lists a directory one level at a time.

### Changed

- **Breaking** `[[tap.table]]` takes `path`, a path under a `[[mount]]`, in place of `url`; a published catalog may now need a credential, which the mount carries.

### Deprecated

--

### Removed

--

### Fixed

--

### Security

--

## [0.0.9] - 2026-09-18

### Added

- `UPLOAD` on `/tap/sync`, naming a HATS catalog or parquet file by url, with `UPLOAD_STORAGE_OPTION` and `UPLOAD_TYPE`. [#74](https://github.com/hombit/hats-api/pull/74)
- A date or timestamp column is answered in `votable`, as `xtype="timestamp"`, and published in `TAP_SCHEMA.columns` and `/tap/tables`. [#79](https://github.com/hombit/hats-api/pull/79)

### Changed

- A date or timestamp column in `csv` and `tsv` is written in UTC in DALI's form, instead of carrying the column's own UTC offset. [#79](https://github.com/hombit/hats-api/pull/79)
- A catalog's `limit=0` answers from `dataset/_common_metadata` where it has one, instead of
  opening a partition to describe zero rows of it. [#77](https://github.com/hombit/hats-api/pull/77)
- A plan for `limit=0` lists no partitions, instead of one entry per partition a region reached. [#77](https://github.com/hombit/hats-api/pull/77)
- A request naming two tables at one authority with different `storage` options is refused, instead of running under whichever table's credentials registered first. [#78](https://github.com/hombit/hats-api/pull/78)

### Fixed

- A query's own answer now sends `Accept-Ranges: none` instead of silently ignoring `Range`. [#73](https://github.com/hombit/hats-api/pull/73)

## [0.0.8] - 2026-09-17

### Added

- `GET /robots.txt`, and `[server] serve_mounted_robots_txt` to answer with a mounted one instead. [#71](https://github.com/hombit/hats-api/pull/71)
- `hf://` urls, with the `token` storage option and `[api.access.hf]`. [#70](https://github.com/hombit/hats-api/pull/70)
- TAP sync queries at `{api.prefix}/tap`, over catalogs listed as `[[tap.table]]`. [#68](https://github.com/hombit/hats-api/pull/68)
- `lang` in the `/api/v1/adql` body, and `POINT('ICRS', ra, dec)`. [#68](https://github.com/hombit/hats-api/pull/68)
- ADQL `TOP` over a catalog with no region. [#68](https://github.com/hombit/hats-api/pull/68)

### Changed

- **Breaking** `[server] serve_index_html` renamed to `[server] serve_mounted_index_html`. [#71](https://github.com/hombit/hats-api/pull/71)
- ADQL names are matched case-insensitively unless quoted. [#68](https://github.com/hombit/hats-api/pull/68)
- `[limits] max_partitions` defaults to 128, from 16. [#68](https://github.com/hombit/hats-api/pull/68)

### Fixed

- ADQL regions outside a catalog or over `Float32` coordinates, and `TOP` with `OFFSET`. [#68](https://github.com/hombit/hats-api/pull/68)

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

[Unreleased]: https://github.com/hombit/hats-api/compare/v0.0.9...HEAD
[0.0.9]: https://github.com/hombit/hats-api/compare/v0.0.8...v0.0.9
[0.0.8]: https://github.com/hombit/hats-api/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/hombit/hats-api/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/hombit/hats-api/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/hombit/hats-api/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/hombit/hats-api/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/hombit/hats-api/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/hombit/hats-api/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/hombit/hats-api/releases/tag/v0.0.1
