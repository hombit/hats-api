# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Release dates are in the UTC time zone.

## [Unreleased]

### Added

- The numeric functions — `sqrt`, `log10`, `power`, `abs`, `degrees`, the trigonometric ones — are callable in `select` and `where`. `log` is refused as ambiguous.
- `POST {api.prefix}/adql`, taking an ADQL statement over parquet tables the request declares.
- `[limits] max_query_memory_bytes`, `1GiB` by default.

### Changed

- **Breaking** The `box` region is now `zone`, ADQL spelling a different shape `BOX`. [#28](https://github.com/hombit/hats-api/pull/28)

### Deprecated

--

### Removed

--

### Fixed

--

### Security

--

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

[Unreleased]: https://github.com/hombit/hats-api/compare/v0.0.4...HEAD
[0.0.4]: https://github.com/hombit/hats-api/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/hombit/hats-api/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/hombit/hats-api/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/hombit/hats-api/releases/tag/v0.0.1
