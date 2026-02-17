# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2026 Dubalu LLC. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
"""Xapiand Python client library.

Provides the ``Xapiand`` async client class for communicating with a Xapiand
search engine server over HTTP. Supports JSON and msgpack serialization,
Django settings integration, and all Xapiand REST operations.

Configuration is read from environment variables (``XAPIAND_HOST``,
``XAPIAND_PORT``, ``XAPIAND_COMMIT``, ``XAPIAND_PREFIX``), with optional
overrides from Django settings. A module-level ``client`` singleton is
created at import time using these defaults.

Example:
    >>> from xapiand import client
    >>> results = await client.search('myindex', query='hello')
    >>> results['hits']
    [...]
"""
from __future__ import annotations

import os
import logging
from typing import Any

try:
    from django.core.exceptions import ObjectDoesNotExist
except ImportError:
    ObjectDoesNotExist = Exception

try:
    from dfw.core.utils.datastructures.nested import NestedDict
except ImportError:
    NestedDict = dict

try:
    from dfw.core.utils import json
except ImportError:
    import json

try:
    from dfw.core.utils import msgpack
except ImportError:
    try:
        import msgpack
    except ImportError:
        msgpack = None

try:
    import httpx
except ImportError:
    raise ImportError("Xapiand requires the installation of the httpx module.")

from .collections import DictObject


__version__ = '2.0.0'
__all__ = [
    'Xapiand',
    'NotFoundError',
    'TransportError',
    'NA',
    'client',
    'IndexSpec',
    'XAPIAND_HOST',
    'XAPIAND_PORT',
    'XAPIAND_COMMIT',
    'XAPIAND_PREFIX',
]

logger = logging.getLogger('xapiand')

OFFSET_LIMIT = 100000  # LIMIT TO AVOID SLOWDOWN XAPIAND WITH HIGH OFFSET

RESPONSE_QUERY = '#query'
RESPONSE_AGGREGATIONS = '#aggregations'
RESPONSE_TOOK = '#took'
COMMAND_PREFIX = ':'

XAPIAND_HOST = os.environ.get('XAPIAND_HOST', '127.0.0.1')
XAPIAND_PORT = os.environ.get('XAPIAND_PORT', 8880)
XAPIAND_COMMIT = os.environ.get('XAPIAND_COMMIT', False)
XAPIAND_PREFIX = os.environ.get('XAPIAND_PREFIX', 'default')

try:
    from django.conf import settings
    XAPIAND_HOST = getattr(settings, 'XAPIAND_HOST', XAPIAND_HOST)
    XAPIAND_PORT = getattr(settings, 'XAPIAND_PORT', XAPIAND_PORT)
    XAPIAND_COMMIT = getattr(settings, 'XAPIAND_COMMIT', XAPIAND_COMMIT)
    XAPIAND_PREFIX = getattr(settings, 'XAPIAND_PREFIX', getattr(settings, 'PROJECT_SUFFIX', XAPIAND_PREFIX))
except Exception:
    settings = None


type IndexSpec = str | tuple[str, ...] | list[str] | set[str]


class NotFoundError(ObjectDoesNotExist):
    """Raised when a requested document is not found (HTTP 404).

    Inherits from Django's ``ObjectDoesNotExist`` when Django is available,
    otherwise falls back to the base ``Exception`` class.
    """


TransportError = httpx.HTTPStatusError


NA = object()


class Xapiand:
    """Async client for communicating with a Xapiand search engine server.

    Manages HTTP connections and provides async methods for all Xapiand REST
    operations: search, get, post, put, patch, merge, delete, store,
    stats, and head.

    All API methods route through ``_send_request``, which builds URLs,
    handles serialization (JSON or msgpack), and deserializes responses.

    Attributes:
        host: Xapiand server hostname.
        port: Xapiand server port.
        commit: Whether to commit changes immediately by default.
        prefix: URL prefix prepended to all index paths.
        default_accept: Default ``Accept`` header for requests.
        default_accept_encoding: Default ``Accept-Encoding`` header.
        NotFoundError: Reference to the ``NotFoundError`` exception class.
        NA: Sentinel object indicating no default value was provided.

    Example:
        >>> client = Xapiand(host='localhost', port=8880)
        >>> results = await client.search('myindex', query='hello world')
        >>> doc = await client.get('myindex', id='doc1')
    """

    NotFoundError = NotFoundError
    NA = NA

    session = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
        trust_env=False,
        follow_redirects=False,
    )
    _methods = dict(
        search=('GET', 'results'),
        stats=('GET', 'result'),
        get=('GET', 'result'),
        delete=('DELETE', 'result'),
        head=('HEAD', 'result'),
        post=('POST', 'result'),
        put=('PUT', 'result'),
        patch=('PATCH', 'result'),
        merge=('MERGE', 'result'),
        store=('STORE', 'result'),
    )

    def __init__(self, host: str | None = None, port: str | int | None = None,
            commit: bool | None = None, prefix: str | None = None,
            default_accept: str | None = None,
            default_accept_encoding: str | None = None,
            *args, **kwargs) -> None:
        """Initialize the Xapiand client.

        Args:
            host: Server hostname. If it contains a colon, the part after
                it is used as the port. Defaults to the ``XAPIAND_HOST``
                environment variable or ``'127.0.0.1'``.
            port: Server port. Defaults to the ``XAPIAND_PORT`` environment
                variable or ``8880``.
            commit: Whether to auto-commit write operations. Defaults to the
                ``XAPIAND_COMMIT`` environment variable or ``False``.
            prefix: URL prefix for index paths. Defaults to ``None`` (no
                prefix).
            default_accept: Default ``Accept`` header. Defaults to
                ``'application/x-msgpack'`` if msgpack is available,
                otherwise ``'application/json'``.
            default_accept_encoding: Default ``Accept-Encoding`` header.
                Defaults to ``'deflate, gzip, identity'``.
            *args: Additional positional arguments (unused).
            **kwargs: Additional keyword arguments (unused).
        """
        if host is None:
            host = XAPIAND_HOST
        if port is None:
            port = XAPIAND_PORT
        if commit is None:
            commit = XAPIAND_COMMIT
        if host and ':' in host:
            host, _, port = host.partition(':')
        self.host = host
        self.port = port
        self.commit = commit
        self.prefix = f'{prefix}/' if prefix else ''
        if default_accept is None:
            default_accept = 'application/json' if msgpack is None else 'application/x-msgpack'
        self.default_accept = default_accept
        if default_accept_encoding is None:
            default_accept_encoding = 'deflate, gzip, identity'
        self.default_accept_encoding = default_accept_encoding

        self.DoesNotExist = NotFoundError

    def _build_url(self, action_request: str, index: IndexSpec,
            host: str | None, port: str | int | None,
            nodename: str | None, id: str | None) -> str:
        """Build the full URL for a Xapiand API request.

        Constructs a URL following the scheme:
        ``http://{host}:{port}/{prefix}{index}/{id}{@nodename}{:command}``

        Args:
            action_request: The action type (e.g., ``'search'``, ``'get'``,
                ``'stats'``). Actions ``'search'`` and ``'stats'`` are
                appended as ``:command`` suffixes.
            index: Index name or comma-separated index names. Can also be
                a list, tuple, or set of index names.
            host: Server hostname override. Falls back to ``self.host``.
            port: Server port override. Falls back to ``self.port``.
            nodename: Optional node name to route the request to.
            id: Optional document ID.

        Returns:
            str: The fully constructed URL.
        """
        if host and ':' in host:
            host, _, port = host.partition(':')
        if not host:
            host = self.host
        if not port:
            port = self.port
        host = f'{host}:{port}'

        if not isinstance(index, (tuple, list, set)):
            index = index.split(',')

        indexes = [f'{self.prefix}{i.strip("/")}' for i in set(index)]
        index = ','.join(['/'.join((i, id or '')) for i in indexes])

        nodename = f'@{nodename}' if nodename else ''

        if action_request in ('search', 'stats',):
            action_request = f'{COMMAND_PREFIX}{action_request}'
        else:
            action_request = ''

        return f'http://{host}/{index}{nodename}{action_request}'

    async def _send_request(self, action_request: str, index: IndexSpec,
            host: str | None = None, port: str | int | None = None,
            nodename: str | None = None, id: str | None = None,
            body: dict | list | str | None = None, default: Any = NA,
            **kwargs) -> DictObject | bytes | Any:
        """Send an HTTP request to the Xapiand server.

        Central method through which all API operations are routed. Handles
        URL construction, content serialization (JSON or msgpack), request
        dispatch, error handling, and response deserialization.

        Args:
            action_request: The API action to perform. Must be one of
                ``'search'``, ``'stats'``, ``'get'``, ``'delete'``,
                ``'head'``, ``'post'``, ``'put'``, ``'patch'``,
                ``'merge'``, or ``'store'``.
            index: Index name or comma-separated index names.
            host: Server hostname override.
            port: Server port override.
            nodename: Node name to route the request to.
            id: Document ID for the request.
            body: Request body. Can be a dict, list, or file path string.
            default: Default value to return on 404 for ``patch``,
                ``merge``, ``delete``, and ``get`` actions. If not
                provided (``NA``), a ``NotFoundError`` is raised instead.
            **kwargs: Additional keyword arguments passed to the underlying
                HTTP request (e.g., ``params``, ``headers``, ``json``,
                ``msgpack``).

        Returns:
            DictObject: Deserialized response content. For search responses,
                the ``#query`` structure is flattened with ``#hits`` renamed
                to ``hits``, ``#total_count`` to ``count``, and
                ``#matches_estimated`` to ``total``.

        Raises:
            NotFoundError: If the response status is 404 and no ``default``
                was provided (only for ``patch``, ``merge``, ``delete``,
                ``get`` actions).
            httpx.HTTPStatusError: If the response status indicates an error
                (other than handled 404s).
        """

        http_method, key = self._methods[action_request]
        url = self._build_url(action_request, index, host, port, nodename, id)

        if action_request == 'search' and body is not None:
            http_method, key = self._methods['post']

        params = kwargs.pop('params', None)
        if params is not None:
            kwargs['params'] = {
                k.replace('__', '.'): (v and 1 or 0) if isinstance(v, bool) else v
                for k, v in params.items()
                if k not in ('commit', 'volatile', 'pretty', 'indent') or v
            }

        headers = kwargs.setdefault('headers', {})
        accept = headers.setdefault('accept', self.default_accept)
        headers.setdefault('accept-encoding', self.default_accept_encoding)

        if 'json' in kwargs:
            body = kwargs.pop('json')
            headers['content-type'] = 'application/json'
            is_msgpack = False
            is_json = True
        elif 'msgpack' in kwargs:
            body = kwargs.pop('msgpack')
            headers['content-type'] = 'application/x-msgpack'
            is_msgpack = True
            is_json = False
        else:
            content_type = headers.setdefault('content-type', accept)
            is_msgpack = 'application/x-msgpack' in content_type
            is_json = 'application/json' in content_type

        if body is not None:
            if isinstance(body, dict):
                if '_schema' in body:
                    body = body.copy()
                    schema = body['_schema']
                    if isinstance(schema, dict):
                        schema['_foreign'] = f"{self.prefix}{schema['_foreign'].strip('/')}"
                    else:
                        schema = f"{self.prefix}{schema.strip('/')}"
                    body['_schema'] = schema
            if logger.isEnabledFor(logging.DEBUG):
                try:
                    verb_body = json.dumps(body, ensure_ascii=True)
                except Exception:
                    verb_body = body
                logger.debug(f"@@@>> URL: {url}  ::  BODY: {verb_body}  ::  KWARGS: {kwargs}")
            if isinstance(body, (dict, list)):
                if is_msgpack:
                    body = msgpack.dumps(body)
                elif is_json:
                    body = json.dumps(body, ensure_ascii=True)
            elif os.path.isfile(body):
                body = open(body, 'r')
            res = await self.session.request(http_method, url, content=body, **kwargs)
        else:
            data = kwargs.pop('data', None)
            if data:
                if is_msgpack:
                    kwargs['content'] = msgpack.dumps(data)
                elif is_json:
                    kwargs['content'] = json.dumps(data, ensure_ascii=True)
            logger.debug(f"@@@>> URL: {url}  ::  KWARGS: {kwargs}")
            res = await self.session.request(http_method, url, **kwargs)

        if res.status_code == 404 and action_request in ('patch', 'merge', 'delete', 'get'):
            if default is NA:
                raise self.NotFoundError("Matching query does not exist.")
            return default
        else:
            try:
                res.raise_for_status()
            except Exception as exc:
                print(f"@@@RRES>> {exc} :: {res.content}")
                logger.debug(f"@@@RES>> {exc}")
                raise

        content_type = res.headers.get('content-type', '')
        is_msgpack = 'application/x-msgpack' in content_type
        is_json = 'application/json' in content_type

        if is_msgpack:
            content = msgpack.loads(res.content, object_pairs_hook=DictObject)
        elif is_json:
            content = json.loads(res.content, object_pairs_hook=DictObject)
        else:
            return res.content

        results = content.pop(RESPONSE_QUERY, DictObject())
        agg = content.pop(RESPONSE_AGGREGATIONS, DictObject())

        if results:
            results['hits'] = results.pop('#hits')
            results['count'] = results.pop('#total_count')
            results['total'] = results.pop('#matches_estimated')

        if agg:
            results['aggregations'] = agg

        if results:
            return results

        return content

    async def search(self, index: IndexSpec, query: str | None = None,
            partial: str | None = None, terms: str | None = None,
            offset: int | str | None = None,
            check_at_least: int | None = None, limit: int | None = None,
            sort: str | None = None, language: str | None = None,
            pretty: bool = False, volatile: bool = False,
            kwargs: dict | None = None, **kw) -> DictObject:
        """Search an index for matching documents.

        Args:
            index: Index name or comma-separated index names to search.
            query: Query string to search for.
            partial: Partial query string for autocomplete-style searches.
            terms: Terms to filter by.
            offset: Starting offset for results. Values exceeding
                ``OFFSET_LIMIT`` (100,000) are reset to 0.
            check_at_least: Minimum number of documents to check.
            limit: Maximum number of results to return.
            sort: Field or fields to sort results by.
            language: Language for query parsing.
            pretty: If ``True``, request pretty-printed response.
            volatile: If ``True``, bypass caches and read from disk.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.
            **kw: Extra keyword arguments merged into ``kwargs``.

        Returns:
            DictObject: Search results with keys ``hits`` (list of matching
                documents), ``count`` (total count), ``total`` (estimated
                matches), and optionally ``aggregations``.
        """
        kwargs = kwargs or {}
        kwargs.update(kw)
        kwargs['params'] = dict(
            pretty=pretty,
            volatile=volatile,
        )
        if query is not None:
            kwargs['params']['query'] = query
        if partial is not None:
            kwargs['params']['partial'] = partial
        if terms is not None:
            kwargs['params']['terms'] = terms
        if limit is not None:
            kwargs['params']['limit'] = limit
        if check_at_least is not None:
            kwargs['params']['check_at_least'] = check_at_least
        if sort is not None:
            kwargs['params']['sort'] = sort
        if language is not None:
            kwargs['params']['language'] = language
        if offset is not None:
            try:
                offset = int(offset)
            except ValueError:
                logger.debug(f"@@@>> INVALID OFFSET: {offset} (type: {type(offset)})")
                kwargs['params']['offset'] = 0
            else:
                if offset > OFFSET_LIMIT:  # the offset was probably sent wrong in this case
                    logger.debug(
                        f"@@@>> PROBABLY ERR OFFSET: {offset} (type: {type(offset)})"
                        f" :: INDEX: {index} :: KWARGS: {kwargs}"
                    )
                    kwargs['params']['offset'] = 0
                else:
                    kwargs['params']['offset'] = offset
        return await self._send_request('search', index, **kwargs)

    async def stats(self, index: IndexSpec, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Retrieve statistics for an index.

        Args:
            index: Index name to retrieve statistics for.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Index statistics from the server.
        """
        kwargs = kwargs or {}
        kwargs['params'] = dict(
            pretty=pretty,
        )
        return await self._send_request('stats', index, **kwargs)

    async def head(self, index: IndexSpec, id: str, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Check if a document exists (HEAD request).

        Args:
            index: Index name containing the document.
            id: Document ID to check.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Response headers/metadata from the server.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['params'] = dict(
            pretty=pretty,
        )
        return await self._send_request('head', index, **kwargs)

    async def count(self, index: IndexSpec, body: dict | list | str | None = None,
            query: str | None = None, commit: bool | None = None,
            pretty: bool = False, volatile: bool = False,
            kwargs: dict | None = None, **kw) -> DictObject:
        """Count documents matching a query in an index.

        Internally uses the search endpoint to retrieve counts.

        Args:
            index: Index name to count documents in.
            body: Optional request body with search criteria.
            query: Query string to filter counted documents.
            commit: Unused (present for API consistency).
            pretty: If ``True``, request pretty-printed response.
            volatile: If ``True``, bypass caches and read from disk.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.
            **kw: Extra keyword arguments added to request params.

        Returns:
            DictObject: Search results containing the document count.
        """
        kwargs = kwargs or {}
        kwargs['params'] = dict(
            pretty=pretty,
            volatile=volatile,
        )
        kwargs['params'].update(kw)
        if query is not None:
            kwargs['params']['query'] = query
        if body is not None:
            kwargs['body'] = body
        return await self._send_request('search', index, **kwargs)

    async def get(self, index: IndexSpec, id: str, accept: str | None = None,
            default: Any = NA, pretty: bool = False, volatile: bool = False,
            kwargs: dict | None = None) -> DictObject | Any:
        """Retrieve a document by ID from an index.

        Args:
            index: Index name containing the document.
            id: Document ID to retrieve.
            accept: Override ``Accept`` header for the request (e.g.,
                ``'application/json'``).
            default: Value to return if the document is not found. If not
                provided, a ``NotFoundError`` is raised on 404.
            pretty: If ``True``, request pretty-printed response.
            volatile: If ``True``, bypass caches and read from disk.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: The retrieved document, or ``default`` if provided
                and the document was not found.

        Raises:
            NotFoundError: If the document is not found and no ``default``
                was provided.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        if accept is not None:
            kwargs['headers'] = {}
            kwargs['headers']['accept'] = accept

        kwargs['params'] = dict(
            pretty=pretty,
            volatile=volatile,
        )
        kwargs['default'] = default
        return await self._send_request('get', index, **kwargs)

    async def delete(self, index: IndexSpec, id: str, commit: bool | None = None,
            pretty: bool = False, kwargs: dict | None = None) -> DictObject:
        """Delete a document by ID from an index.

        Args:
            index: Index name containing the document.
            id: Document ID to delete.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response confirming deletion.

        Raises:
            NotFoundError: If the document is not found.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('delete', index, **kwargs)

    async def post(self, index: IndexSpec, body: dict | list | str,
            commit: bool | None = None, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Create a new document in an index (server-assigned ID).

        Args:
            index: Index name to create the document in.
            body: Document body as a dict, list, or file path.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the created document metadata.
        """
        kwargs = kwargs or {}
        kwargs['body'] = body
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('post', index, **kwargs)

    async def put(self, index: IndexSpec, body: dict | list | str, id: str,
            commit: bool | None = None, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Create or replace a document with a specific ID.

        Args:
            index: Index name for the document.
            body: Document body as a dict, list, or file path.
            id: Document ID to assign.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the document metadata.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['body'] = body
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('put', index, **kwargs)

    async def index(self, index: IndexSpec, body: dict | list | str, id: str,
            commit: bool | None = None, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Create or replace a document (alias for ``put``).

        Args:
            index: Index name for the document.
            body: Document body as a dict, list, or file path.
            id: Document ID to assign.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the document metadata.
        """
        return await self.put(index, body, id, commit, pretty, kwargs)

    async def patch(self, index: IndexSpec, id: str, body: dict | list | str,
            commit: bool | None = None, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Partially update a document by ID.

        Sends a PATCH request, replacing only the fields present in
        ``body``.

        Args:
            index: Index name containing the document.
            id: Document ID to update.
            body: Partial document body with fields to update.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the updated document metadata.

        Raises:
            NotFoundError: If the document is not found.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['body'] = body
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('patch', index, **kwargs)

    async def update(self, index: IndexSpec, id: str, body: dict | list | str,
            content_type: str | None = None, commit: bool | None = None,
            pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Update a document, choosing strategy based on content type.

        If ``content_type`` is specified, performs a full replacement via
        ``put``. Otherwise, performs a partial merge via ``merge``.

        Args:
            index: Index name containing the document.
            id: Document ID to update.
            body: Document body (full or partial, depending on strategy).
            content_type: If provided, sets the ``Content-Type`` header and
                uses ``put`` for a full replacement. If ``None``, uses
                ``merge`` for a partial update.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the document metadata.
        """
        kwargs = kwargs or {}
        if content_type is not None:
            kwargs.setdefault('headers', {})
            kwargs['headers']['content-type'] = content_type
            return await self.put(index, body, id, commit, pretty, kwargs)
        return await self.merge(
            index=index,
            id=id,
            body=body,
            content_type=content_type,
            commit=commit,
            pretty=pretty,
            kwargs=kwargs,
        )

    async def merge(self, index: IndexSpec, id: str, body: dict | list | str,
            content_type: str | None = None, commit: bool | None = None,
            pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Partially update a document using the MERGE HTTP method.

        Unlike ``patch``, which uses standard HTTP PATCH, this method uses
        Xapiand's custom MERGE method for deep-merging document fields.

        Args:
            index: Index name containing the document.
            id: Document ID to merge into.
            body: Partial document body with fields to merge.
            content_type: Optional ``Content-Type`` header override.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response with the updated document metadata.

        Raises:
            NotFoundError: If the document is not found.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['body'] = body
        if content_type is not None:
            kwargs.setdefault('headers', {})
            kwargs['headers']['content-type'] = content_type
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('merge', index, **kwargs)

    async def store(self, index: IndexSpec, id: str, body: dict | list | str,
            commit: bool | None = None, pretty: bool = False,
            kwargs: dict | None = None) -> DictObject:
        """Store binary content for a document using the STORE HTTP method.

        Args:
            index: Index name for the document.
            id: Document ID to store content for.
            body: Binary content or file path to store.
            commit: Whether to commit immediately. Defaults to
                ``self.commit``.
            pretty: If ``True``, request pretty-printed response.
            kwargs: Additional keyword arguments dict passed to
                ``_send_request``.

        Returns:
            DictObject: Server response confirming storage.
        """
        kwargs = kwargs or {}
        kwargs['id'] = id
        kwargs['body'] = body
        kwargs['params'] = dict(
            commit=self.commit if commit is None else commit,
            pretty=pretty,
        )
        return await self._send_request('store', index, **kwargs)


client = Xapiand(host=XAPIAND_HOST, port=XAPIAND_PORT, commit=XAPIAND_COMMIT, prefix=XAPIAND_PREFIX)
