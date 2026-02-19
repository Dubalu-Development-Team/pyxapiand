# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [2.1.0] - 2026-02-19

### Added

- **Decimal and datetime serialization**: Outbound request bodies automatically serialize `Decimal` to `float` and `datetime`/`date`/`time` to ISO 8601 strings via `_serialize_default`.
- **Decimal and datetime deserialization**: Inbound JSON/msgpack responses automatically convert floats to `Decimal` (using `parse_float=Decimal` for JSON, `str()` conversion for msgpack) and ISO 8601 strings to `datetime`/`date`/`time` (via regex matching and `fromisoformat`).
- New `_deserialize_value` function for recursive type conversion of response values.
- New `_deserialize_object_pairs_hook` function used as `object_pairs_hook` for `json.loads` and `msgpack.loads`.

### Changed

- Removed Django and `dfw.core` dependencies; configuration now uses environment variables only (`XAPIAND_HOST`, `XAPIAND_PORT`, `XAPIAND_COMMIT`, `XAPIAND_PREFIX`).

## [2.0.0] - 2026-02-17

### Breaking Changes

- **Async API**: All client methods (`search`, `get`, `post`, `put`, `patch`, `merge`, `delete`, `store`, `stats`, `head`, `count`, `update`, `index`) are now `async def` and must be called with `await`.
- **Dependency change**: Replaced `requests` with `httpx`. Install with `pip install pyxapiand` (httpx is pulled automatically).
- **`Session` class removed**: The custom `Session` subclass of `requests.Session` has been removed. `httpx.AsyncClient` handles custom HTTP methods (`MERGE`, `STORE`) natively.
- **`TransportError`**: Now aliases `httpx.HTTPStatusError` instead of `requests.HTTPError`.
- **`allow_redirects` kwarg removed**: Redirects are disabled at the `httpx.AsyncClient` level (`follow_redirects=False`), no longer passed per-request.
- **`data` kwarg**: Internal serialization now uses httpx's `content` parameter instead of `data` for raw payloads.
- **Renamed Python import**: `import pyxapiand` is now `import xapiand` (the PyPI package name remains `pyxapiand`).

### Added

- `pytest-asyncio` added as a test dependency (`pip install pyxapiand[test]`).
- Code conventions enforced: Google-style docstrings, `__all__` exports, and PEP 695 type hints on all public APIs.
- Comprehensive test suite with full coverage.

## [1.0.0] - 2026-02-12

### Added

- Initial release with synchronous `requests`-based client.
- Support for all Xapiand REST operations: `search`, `get`, `post`, `put`, `patch`, `merge`, `delete`, `store`, `stats`, `head`, `count`, `update`, and `index`.
- `DictObject` and `OrderedDictObject` for attribute-style access to response data.
- JSON and optional msgpack serialization.
- Xapian-compatible binary serialization utilities.
- Predefined Xapian term constants for date accuracy, HTM geospatial levels, and numeric accuracy.

[2.1.0]: https://github.com/Dubalu-Development-Team/pyxapiand/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/Dubalu-Development-Team/pyxapiand/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/Dubalu-Development-Team/pyxapiand/releases/tag/v1.0.0
