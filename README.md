# Xapiand Python Client

Python client library for [Xapiand](https://github.com/pber/xapiand), a RESTful search engine built on top of [Xapian](https://xapian.org/).

## Features

- Full coverage of Xapiand REST operations: search, get, post, put, patch, merge, delete, store, stats, and head.
- Automatic serialization/deserialization with JSON and [msgpack](https://msgpack.org/) (preferred when available).
- Django settings integration for seamless configuration in Django projects.
- Attribute-style access on response objects (`result.hits` instead of `result['hits']`).
- Custom HTTP methods (`MERGE`, `STORE`) supported via an extended `requests.Session`.

## Requirements

- Python 3.12+
- [`requests`](https://docs.python-requests.org/)

## Installation

```bash
pip install pyxapiand
```

With optional msgpack support (recommended for better performance):

```bash
pip install pyxapiand[msgpack]
```

For development (editable install):

```bash
git clone https://github.com/Dubalu-Development-Team/pyxapiand.git
cd pyxapiand
pip install -e .[msgpack]
```

### Python version with pyenv

This project includes a `.python-version` file that pins Python 3.12. If you use [pyenv](https://github.com/pyenv/pyenv), make sure you have a 3.12.x version installed:

```bash
pyenv install 3.12
```

This installs the latest available 3.12.x release. pyenv will then automatically select it when you enter the project directory.

## Quick Start

```python
from xapiand import Xapiand

client = Xapiand(host="localhost", port=8880)

# Index a document
client.put("books", body={"title": "The Art of Search", "year": 2024}, id="book1")

# Retrieve a document
doc = client.get("books", id="book1")
print(doc.title)  # "The Art of Search"

# Search
results = client.search("books", query="search")
for hit in results.hits:
    print(hit.title)

# Delete a document
client.delete("books", id="book1")
```

A pre-configured module-level `client` singleton is also available:

```python
from xapiand import client

results = client.search("myindex", query="hello world")
print(results.count)  # total count
print(results.total)  # estimated matches
```

## Configuration

The client reads configuration from environment variables, with optional overrides from Django settings:

| Environment Variable | Django Setting          | Default     | Description                          |
|----------------------|-------------------------|-------------|--------------------------------------|
| `XAPIAND_HOST`      | `settings.XAPIAND_HOST` | `127.0.0.1` | Server hostname                      |
| `XAPIAND_PORT`      | `settings.XAPIAND_PORT` | `8880`      | Server port                          |
| `XAPIAND_COMMIT`    | `settings.XAPIAND_COMMIT` | `False`   | Auto-commit write operations         |
| `XAPIAND_PREFIX`    | `settings.XAPIAND_PREFIX` | `default` | URL prefix prepended to index paths  |

Django settings take precedence over environment variables when Django is available.

### Client initialization

You can also pass configuration directly when creating a client:

```python
client = Xapiand(
    host="192.168.1.100",
    port=8880,
    commit=True,          # auto-commit writes
    prefix="production",  # URL prefix for index paths
)
```

The `host` parameter accepts a `host:port` format (`"192.168.1.100:9000"`), in which case the port part overrides the `port` parameter.

## API Reference

All API methods return `DictObject` instances, which are dictionaries with attribute-style access.

### Search

```python
results = client.search(
    "myindex",
    query="hello world",    # query string
    partial="hel",          # partial query for autocomplete
    terms="tag:python",     # term filters
    offset=0,               # starting offset
    limit=10,               # max results
    sort="date",            # sort field
    language="en",          # query language
    check_at_least=100,     # minimum documents to check
)

results.hits    # list of matching documents
results.count   # total count
results.total   # estimated matches
```

Search with a request body (uses POST internally):

```python
results = client.search("myindex", body={
    "_query": {"title": "search engine"},
})
```

### Count

```python
results = client.count("myindex", query="hello")
print(results.count)
```

### Get

```python
doc = client.get("myindex", id="doc1")

# With a default value (returns it instead of raising on 404)
doc = client.get("myindex", id="doc1", default=None)
```

### Create (POST)

```python
# Server-assigned ID
result = client.post("myindex", body={"title": "New Document"})
```

### Create or Replace (PUT)

```python
result = client.put("myindex", body={"title": "My Document"}, id="doc1")

# Alias
result = client.index("myindex", body={"title": "My Document"}, id="doc1")
```

### Partial Update (PATCH)

```python
result = client.patch("myindex", id="doc1", body={"title": "Updated Title"})
```

### Deep Merge (MERGE)

Uses Xapiand's custom `MERGE` HTTP method for deep-merging fields:

```python
result = client.merge("myindex", id="doc1", body={"metadata": {"tags": ["new"]}})
```

### Update

Chooses strategy based on content type:

```python
# Partial merge (default)
client.update("myindex", id="doc1", body={"title": "Updated"})

# Full replacement when content_type is specified
client.update("myindex", id="doc1", body=data, content_type="application/json")
```

### Delete

```python
client.delete("myindex", id="doc1")
```

### Store

Store binary content using Xapiand's custom `STORE` HTTP method:

```python
client.store("myindex", id="doc1", body="/path/to/file.bin")
```

### Head

Check if a document exists:

```python
client.head("myindex", id="doc1")
```

### Stats

```python
stats = client.stats("myindex")
```

### Common Parameters

Most methods accept these optional parameters:

| Parameter | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `commit`  | `bool` | Commit changes immediately (write ops)   |
| `pretty`  | `bool` | Request pretty-printed response          |
| `volatile`| `bool` | Bypass caches, read from disk            |
| `kwargs`  | `dict` | Additional arguments passed to requests  |

## Error Handling

```python
from xapiand import Xapiand, TransportError

client = Xapiand()

# NotFoundError on missing documents
try:
    doc = client.get("myindex", id="nonexistent")
except client.NotFoundError:
    print("Document not found")

# TransportError (requests.HTTPError) on other HTTP errors
try:
    client.search("myindex", query="test")
except TransportError as e:
    print(f"HTTP error: {e}")
```

When Django is installed, `NotFoundError` is a subclass of `django.core.exceptions.ObjectDoesNotExist`, making it compatible with Django's error handling patterns.

## Utilities

### `xapiand.collections`

Dict subclasses with attribute-style access:

```python
from xapiand.collections import DictObject, OrderedDictObject

obj = DictObject(name="test", value=42)
obj.name      # "test"
obj["value"]  # 42
```

### `xapiand.constants`

Predefined Xapian term constants for configuring index schema accuracy:

```python
from xapiand.constants import (
    # Date accuracy
    HOUR_TERM, DAY_TERM, MONTH_TERM, YEAR_TERM,
    DAY_TO_YEAR_ACCURACY,     # [day, month, year]
    HOUR_TO_YEAR_ACCURACY,    # [hour, day, month, year]

    # Geospatial accuracy (HTM levels)
    LEVEL_0_TERM,             # ~10,000 km
    LEVEL_10_TERM,            # ~10 km
    LEVEL_20_TERM,            # ~10 m
    STATE_TO_BLOCK_ACCURACY,  # [level_5, level_10, level_15]

    # Numeric accuracy
    TENS_TO_TEN_THOUSANDS_ACCURACY,
    HUDREDS_TO_MILLIONS_ACCURACY,
)
```

### `xapiand.utils`

Xapian-compatible binary serialization for lengths, strings, and characters:

```python
from xapiand.utils import serialise_length, unserialise_length

encoded = serialise_length(42)
length, remaining = unserialise_length(encoded)
assert length == 42
```

## License

MIT License - Copyright (c) 2015-2026 [Dubalu LLC](https://dubalu.com/)

See [LICENSE](LICENSE) for details.
