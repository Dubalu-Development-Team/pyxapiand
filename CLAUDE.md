# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python client library for [Xapiand](https://github.com/pber/xapiand), a RESTful search engine built on Xapian. Published by Dubalu LLC under the MIT license.

**Important**: This codebase targets Python 2/3 compatibility using `six` and `from __future__ import absolute_import`. Note the Python 2-style `print` statement in `__init__.py:263`.

## Dependencies

- **Required**: `requests`, `six`
- **Optional**: `msgpack` (preferred serialization when available), Django (for settings integration), `dfw` (Dubalu Framework utilities)

No `setup.py`, `pyproject.toml`, or `requirements.txt` exists yet. No test suite is present.

## Architecture

The package has four modules:

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

## Key Patterns

- All API methods (`search`, `get`, `post`, `put`, `patch`, `merge`, `delete`, `store`, `stats`, `head`) route through `_send_request`, which builds URLs, handles serialization (JSON or msgpack), and deserializes responses.
- Search responses are restructured: `#query` → top-level with `#hits` → `hits`, `#total_count` → `count`, `#matches_estimated` → `total`; `#aggregations` is extracted separately.
- URL scheme: `http://{host}:{port}/{prefix}{index}/{id}{@nodename}{:command}`
- 404 responses on `patch`/`merge`/`delete`/`get` raise `NotFoundError` (subclass of Django's `ObjectDoesNotExist` when available) unless a `default` value is provided.
