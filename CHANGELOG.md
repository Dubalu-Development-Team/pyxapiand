# Changelog

## 2.0.0

### Breaking Changes

- **Async API**: All client methods (`search`, `get`, `post`, `put`, `patch`, `merge`, `delete`, `store`, `stats`, `head`, `count`, `update`, `index`) are now `async def` and must be called with `await`.
- **Dependency change**: Replaced `requests` with `httpx`. Install with `pip install pyxapiand` (httpx is pulled automatically).
- **`Session` class removed**: The custom `Session` subclass of `requests.Session` has been removed. `httpx.AsyncClient` handles custom HTTP methods (`MERGE`, `STORE`) natively.
- **`TransportError`**: Now aliases `httpx.HTTPStatusError` instead of `requests.HTTPError`.
- **`allow_redirects` kwarg removed**: Redirects are disabled at the `httpx.AsyncClient` level (`follow_redirects=False`), no longer passed per-request.
- **`data` kwarg**: Internal serialization now uses httpx's `content` parameter instead of `data` for raw payloads.

### Added

- `pytest-asyncio` added as a test dependency (`pip install pyxapiand[test]`).

## 1.0.0

- Initial release with synchronous `requests`-based client.
