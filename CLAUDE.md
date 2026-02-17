# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pyxapiand` — Python client library for [Xapiand](https://github.com/pber/xapiand), a RESTful search engine built on Xapian. Published by Dubalu LLC under the MIT license.

**Important**: This codebase requires Python 3.12+.

## Build & Install

The package is configured via `pyproject.toml` (setuptools backend) with a `src/` layout.

```bash
pip install pyxapiand            # from PyPI
pip install pyxapiand[msgpack]   # with optional msgpack support
pip install -e .                 # editable install for development
```

## Dependencies

- **Required**: `requests`
- **Optional**: `msgpack` (preferred serialization when available), Django (for settings integration), `dfw` (Dubalu Framework utilities)

## Testing

Tests live in `tests/` and use `pytest`:

```bash
pytest              # run all 169 tests
pytest -v           # verbose output
pytest tests/test_client.py  # run a single test module
```

## Architecture

The package (`src/xapiand/`) has four modules:

- **`__init__.py`** — Core client. The `Xapiand` class wraps all HTTP communication with the Xapiand server. A `Session` subclass of `requests.Session` adds custom HTTP methods `MERGE` and `STORE`. A module-level `client` singleton is instantiated at import time.
- **`collections.py`** — `DictObject` and `OrderedDictObject`: dict subclasses that allow attribute-style access (`obj.key` instead of `obj['key']`). `DictObject` is used as `object_pairs_hook` when deserializing JSON/msgpack responses.
- **`constants.py`** — Predefined Xapian term constants for date accuracy ranges (hour→millennium), HTM geospatial levels (0→20), and numeric accuracy levels.
- **`utils.py`** — Xapian-compatible binary serialization/deserialization for lengths, strings, and chars.

## Configuration

The client reads config from environment variables, with Django settings as override:

| Env Var | Django Setting | Default |
|---------|---------------|---------|
| `XAPIAND_HOST` | `settings.XAPIAND_HOST` | `127.0.0.1` |
| `XAPIAND_PORT` | `settings.XAPIAND_PORT` | `8880` |
| `XAPIAND_COMMIT` | `settings.XAPIAND_COMMIT` | `False` |
| `XAPIAND_PREFIX` | `settings.XAPIAND_PREFIX` | `default` |

## Docstring Conventions

All modules, classes, and public functions **must** have docstrings following the
[Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) format:

- **Modules**: Summary line, extended description, and optional `Example:` section.
- **Classes**: Summary line, extended description, `Attributes:` section for public attributes, and optional `Example:`.
- **Functions/Methods**: Summary line, `Args:`, `Returns:`, and `Raises:` sections as applicable.

```python
def example_function(arg1, arg2):
    """Summary line describing what this function does.

    Extended description if needed.

    Args:
        arg1: Description of arg1.
        arg2: Description of arg2.

    Returns:
        Description of the return value.

    Raises:
        ValueError: If arg1 is invalid.
    """
```

## Key Patterns

- All API methods (`search`, `get`, `post`, `put`, `patch`, `merge`, `delete`, `store`, `stats`, `head`) route through `_send_request`, which builds URLs, handles serialization (JSON or msgpack), and deserializes responses.
- Search responses are restructured: `#query` → top-level with `#hits` → `hits`, `#total_count` → `count`, `#matches_estimated` → `total`; `#aggregations` is extracted separately.
- URL scheme: `http://{host}:{port}/{prefix}{index}/{id}{@nodename}{:command}`
- 404 responses on `patch`/`merge`/`delete`/`get` raise `NotFoundError` (subclass of Django's `ObjectDoesNotExist` when available) unless a `default` value is provided.
