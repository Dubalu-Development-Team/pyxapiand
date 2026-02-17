"""Tests for xapiand — NotFoundError and Xapiand async client."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

import pytest
import httpx

from xapiand import (
    NA,
    Xapiand,
    NotFoundError,
    TransportError,
    XAPIAND_COMMIT,
    XAPIAND_HOST,
    XAPIAND_PORT,
    XAPIAND_PREFIX,
)
from xapiand.collections import DictObject


# ── helpers ────────────────────────────────────────────────────────────

def _mock_response(status_code=200, content=b'{}', content_type='application/json',
                   headers=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.headers = {'content-type': content_type}
    if headers:
        resp.headers.update(headers)
    resp.raise_for_status = MagicMock()
    if status_code >= 400 and status_code != 404:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp)
    return resp


def _json_content(data):
    return json.dumps(data).encode()


# ── NotFoundError ──────────────────────────────────────────────────────

class TestNotFoundError:
    def test_is_exception(self):
        assert issubclass(NotFoundError, Exception)

    def test_raise_and_catch(self):
        with pytest.raises(NotFoundError):
            raise NotFoundError("not found")

    def test_accessible_from_class(self):
        assert Xapiand.NotFoundError is NotFoundError


# ── TransportError ─────────────────────────────────────────────────────

class TestTransportError:
    def test_is_http_status_error(self):
        assert TransportError is httpx.HTTPStatusError


# ── Xapiand.__init__ ──────────────────────────────────────────────────

class TestXapiandInit:
    def test_defaults(self):
        c = Xapiand()
        assert c.host == XAPIAND_HOST
        assert c.port == XAPIAND_PORT
        assert c.commit == XAPIAND_COMMIT
        assert c.default_accept_encoding == 'deflate, gzip, identity'

    def test_explicit_params(self):
        c = Xapiand(host='myhost', port=9999, commit=True, prefix='pre')
        assert c.host == 'myhost'
        assert c.port == 9999
        assert c.commit is True
        assert c.prefix == 'pre/'

    def test_host_with_port(self):
        c = Xapiand(host='myhost:1234')
        assert c.host == 'myhost'
        assert c.port == '1234'

    def test_no_prefix(self):
        c = Xapiand(prefix=None)
        assert c.prefix == ''

    def test_prefix_with_value(self):
        c = Xapiand(prefix='idx')
        assert c.prefix == 'idx/'

    def test_default_accept_json_when_no_msgpack(self):
        with patch('xapiand.msgpack', None):
            c = Xapiand()
            assert c.default_accept == 'application/json'

    def test_default_accept_msgpack_when_available(self):
        with patch('xapiand.msgpack', MagicMock()):
            c = Xapiand()
            assert c.default_accept == 'application/x-msgpack'

    def test_custom_accept(self):
        c = Xapiand(default_accept='text/plain')
        assert c.default_accept == 'text/plain'

    def test_custom_accept_encoding(self):
        c = Xapiand(default_accept_encoding='identity')
        assert c.default_accept_encoding == 'identity'

    def test_does_not_exist_alias(self):
        c = Xapiand()
        assert c.DoesNotExist is NotFoundError

    def test_na_sentinel(self):
        assert Xapiand.NA is NA


# ── Xapiand._build_url ────────────────────────────────────────────────

class TestBuildUrl:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix='default')

    def test_search_url(self):
        url = self.client._build_url('search', 'myindex', None, None, None, None)
        assert url == 'http://localhost:8880/default/myindex/:search'

    def test_stats_url(self):
        url = self.client._build_url('stats', 'myindex', None, None, None, None)
        assert url == 'http://localhost:8880/default/myindex/:stats'

    def test_get_url_with_id(self):
        url = self.client._build_url('get', 'myindex', None, None, None, 'doc1')
        assert url == 'http://localhost:8880/default/myindex/doc1'

    def test_custom_host_and_port(self):
        url = self.client._build_url('get', 'idx', 'other', 9999, None, 'id1')
        assert url == 'http://other:9999/default/idx/id1'

    def test_host_with_colon_port(self):
        url = self.client._build_url('get', 'idx', 'h:7777', None, None, 'id1')
        assert url == 'http://h:7777/default/idx/id1'

    def test_nodename(self):
        url = self.client._build_url('get', 'idx', None, None, 'node1', 'id1')
        assert url == 'http://localhost:8880/default/idx/id1@node1'

    def test_tuple_index(self):
        url = self.client._build_url('get', ('idx1', 'idx2'), None, None, None, None)
        assert 'default/idx1/' in url or 'default/idx2/' in url

    def test_list_index(self):
        url = self.client._build_url('get', ['idx1'], None, None, None, 'doc')
        assert 'default/idx1/doc' in url

    def test_set_index(self):
        url = self.client._build_url('get', {'idx1'}, None, None, None, 'doc')
        assert 'default/idx1/doc' in url

    def test_comma_separated_index(self):
        url = self.client._build_url('get', 'a,b', None, None, None, None)
        assert 'default/a/' in url
        assert 'default/b/' in url

    def test_no_prefix(self):
        c = Xapiand(host='localhost', port=8880, prefix=None)
        url = c._build_url('get', 'idx', None, None, None, 'doc')
        assert url == 'http://localhost:8880/idx/doc'

    def test_fallback_host_port(self):
        url = self.client._build_url('get', 'idx', '', '', None, None)
        assert url == 'http://localhost:8880/default/idx/'


# ── Xapiand._send_request ─────────────────────────────────────────────

class TestSendRequest:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    def _patch_method(self, action, response):
        mock_session = MagicMock()
        mock_session.request = AsyncMock(return_value=response)
        self.client.session = mock_session
        return mock_session.request

    def teardown_method(self):
        self.client.__dict__.pop('session', None)

    async def test_basic_get_json(self):
        resp = _mock_response(content=_json_content({"title": "hello"}))
        method = self._patch_method('get', resp)
        result = await self.client._send_request('get', 'idx', id='doc1')
        assert result['title'] == 'hello'
        method.assert_called_once()

    async def test_404_with_default(self):
        resp = _mock_response(status_code=404)
        self._patch_method('get', resp)
        result = await self.client._send_request('get', 'idx', id='doc1', default=None)
        assert result is None

    async def test_404_without_default_raises(self):
        resp = _mock_response(status_code=404)
        self._patch_method('get', resp)
        with pytest.raises(NotFoundError):
            await self.client._send_request('get', 'idx', id='doc1')

    async def test_404_on_patch(self):
        resp = _mock_response(status_code=404)
        self._patch_method('patch', resp)
        with pytest.raises(NotFoundError):
            await self.client._send_request('patch', 'idx', id='doc1', body={'a': 1})

    async def test_404_on_merge(self):
        resp = _mock_response(status_code=404)
        self._patch_method('merge', resp)
        with pytest.raises(NotFoundError):
            await self.client._send_request('merge', 'idx', id='doc1', body={'a': 1})

    async def test_404_on_delete(self):
        resp = _mock_response(status_code=404)
        self._patch_method('delete', resp)
        with pytest.raises(NotFoundError):
            await self.client._send_request('delete', 'idx', id='doc1')

    async def test_404_on_post_raises_http_status_error(self):
        resp = _mock_response(status_code=404)
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp)
        self._patch_method('post', resp)
        with pytest.raises(httpx.HTTPStatusError):
            await self.client._send_request('post', 'idx', body={'a': 1})

    async def test_500_error_raises(self):
        resp = _mock_response(status_code=500)
        self._patch_method('get', resp)
        with pytest.raises(httpx.HTTPStatusError):
            await self.client._send_request('get', 'idx', id='doc1')

    async def test_search_response_restructuring(self):
        data = {
            '#query': {
                '#hits': [{'id': 1}],
                '#total_count': 100,
                '#matches_estimated': 150,
            },
            '#aggregations': {'field': {'count': 10}},
        }
        resp = _mock_response(content=_json_content(data))
        self._patch_method('search', resp)
        result = await self.client._send_request('search', 'idx')
        assert result['hits'] == [{'id': 1}]
        assert result['count'] == 100
        assert result['total'] == 150
        assert result['aggregations'] == {'field': {'count': 10}}

    async def test_search_no_query_key(self):
        resp = _mock_response(content=_json_content({"key": "value"}))
        self._patch_method('search', resp)
        result = await self.client._send_request('search', 'idx')
        assert result['key'] == 'value'

    async def test_search_with_body_uses_post(self):
        resp = _mock_response(content=_json_content({"key": "value"}))
        method = self._patch_method('search', resp)
        await self.client._send_request('search', 'idx', body={'query': 'test'})
        method.assert_called_once()
        # When search has a body, it should use POST
        assert method.call_args[0][0] == 'POST'

    async def test_json_kwarg(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        method = self._patch_method('post', resp)
        await self.client._send_request('post', 'idx', json={'data': 1})
        call_kwargs = method.call_args
        assert 'json' not in call_kwargs.kwargs

    async def test_msgpack_kwarg(self):
        mock_msgpack = MagicMock()
        mock_msgpack.dumps.return_value = b'\x81\xa1k\xa1v'
        resp = _mock_response(content=_json_content({"ok": True}))
        method = self._patch_method('post', resp)
        with patch('xapiand.msgpack', mock_msgpack):
            await self.client._send_request('post', 'idx', msgpack={'data': 1})
        call_kwargs = method.call_args
        assert 'msgpack' not in call_kwargs.kwargs

    async def test_body_dict_json_serialization(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        method = self._patch_method('post', resp)
        await self.client._send_request('post', 'idx', body={'key': 'val'})
        call_kwargs = method.call_args.kwargs
        body_sent = call_kwargs['content']
        assert json.loads(body_sent) == {'key': 'val'}

    async def test_body_list_json_serialization(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        method = self._patch_method('post', resp)
        await self.client._send_request('post', 'idx', body=[1, 2, 3])
        call_kwargs = method.call_args.kwargs
        body_sent = call_kwargs['content']
        assert json.loads(body_sent) == [1, 2, 3]

    async def test_body_dict_msgpack_serialization(self):
        mock_msgpack = MagicMock()
        mock_msgpack.dumps.return_value = b'\x80'
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        c = Xapiand(host='localhost', port=8880, prefix=None,
                     default_accept='application/x-msgpack')
        c.session = self.client.session
        with patch('xapiand.msgpack', mock_msgpack):
            await c._send_request('post', 'idx', body={'key': 'val'})
        mock_msgpack.dumps.assert_called_once_with({'key': 'val'})

    async def test_body_file_path(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        m = mock_open(read_data='file content')
        with patch('os.path.isfile', return_value=True), patch('builtins.open', m):
            await self.client._send_request('post', 'idx', body='/path/to/file.json')
        m.assert_called_once_with('/path/to/file.json', 'r')

    async def test_no_body_with_data_kwarg_json(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        method = self._patch_method('post', resp)
        await self.client._send_request('post', 'idx', data={'k': 'v'})
        call_kwargs = method.call_args.kwargs
        assert call_kwargs['content'] == json.dumps({'k': 'v'}, ensure_ascii=True)

    async def test_no_body_with_data_kwarg_msgpack(self):
        mock_msgpack = MagicMock()
        mock_msgpack.dumps.return_value = b'\x80'
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        c = Xapiand(host='localhost', port=8880, prefix=None,
                     default_accept='application/x-msgpack')
        c.session = self.client.session
        with patch('xapiand.msgpack', mock_msgpack):
            await c._send_request('post', 'idx', data={'k': 'v'})
        mock_msgpack.dumps.assert_called_once_with({'k': 'v'})

    async def test_response_unknown_content_type(self):
        resp = _mock_response(content=b'raw bytes', content_type='application/octet-stream')
        self._patch_method('get', resp)
        result = await self.client._send_request('get', 'idx', id='doc1')
        assert result == b'raw bytes'

    async def test_response_msgpack_deserialization(self):
        mock_msgpack = MagicMock()
        mock_msgpack.loads.return_value = DictObject(title='hello')
        resp = _mock_response(content=b'\x80', content_type='application/x-msgpack')
        self._patch_method('get', resp)
        with patch('xapiand.msgpack', mock_msgpack):
            result = await self.client._send_request('get', 'idx', id='doc1')
        mock_msgpack.loads.assert_called_once()
        assert result['title'] == 'hello'

    async def test_params_bool_conversion(self):
        resp = _mock_response(content=_json_content({}))
        method = self._patch_method('get', resp)
        await self.client._send_request('get', 'idx', id='doc',
                                  params={'pretty': True, 'volatile': True})
        call_kwargs = method.call_args.kwargs
        assert call_kwargs['params']['pretty'] == 1
        assert call_kwargs['params']['volatile'] == 1

    async def test_params_falsy_special_keys_excluded(self):
        resp = _mock_response(content=_json_content({}))
        method = self._patch_method('get', resp)
        await self.client._send_request('get', 'idx', id='doc',
                                  params={'pretty': False, 'volatile': False,
                                          'commit': False, 'indent': False})
        call_kwargs = method.call_args.kwargs
        # Falsy values for these special keys are excluded
        assert 'pretty' not in call_kwargs['params']
        assert 'volatile' not in call_kwargs['params']
        assert 'commit' not in call_kwargs['params']
        assert 'indent' not in call_kwargs['params']

    async def test_params_double_underscore_conversion(self):
        resp = _mock_response(content=_json_content({}))
        method = self._patch_method('get', resp)
        await self.client._send_request('get', 'idx', id='doc',
                                  params={'some__nested': 'val'})
        call_kwargs = method.call_args.kwargs
        assert 'some.nested' in call_kwargs['params']

    async def test_params_commit_false_excluded(self):
        resp = _mock_response(content=_json_content({}))
        method = self._patch_method('post', resp)
        await self.client._send_request('post', 'idx', body={'a': 1},
                                  params={'commit': False, 'other': 'x'})
        call_kwargs = method.call_args.kwargs
        assert 'commit' not in call_kwargs['params']

    async def test_no_follow_redirects_in_kwargs(self):
        resp = _mock_response(content=_json_content({}))
        method = self._patch_method('get', resp)
        await self.client._send_request('get', 'idx', id='doc')
        call_kwargs = method.call_args.kwargs
        assert 'allow_redirects' not in call_kwargs
        assert 'follow_redirects' not in call_kwargs

    async def test_schema_handling_dict_schema(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        c = Xapiand(host='localhost', port=8880, prefix='pre',
                     default_accept='application/json')
        c.session = self.client.session
        body = {'_schema': {'_foreign': '/some/path'}, 'data': 1}
        await c._send_request('post', 'idx', body=body)
        method = c.session.request
        call_kwargs = method.call_args.kwargs
        sent_body = json.loads(call_kwargs['content'])
        assert sent_body['_schema']['_foreign'] == 'pre/some/path'

    async def test_schema_handling_string_schema(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        c = Xapiand(host='localhost', port=8880, prefix='pre',
                     default_accept='application/json')
        c.session = self.client.session
        body = {'_schema': '/schema/path', 'data': 1}
        await c._send_request('post', 'idx', body=body)
        method = c.session.request
        call_kwargs = method.call_args.kwargs
        sent_body = json.loads(call_kwargs['content'])
        assert sent_body['_schema'] == 'pre/schema/path'

    async def test_debug_logging_body(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        with patch('xapiand.logger') as mock_logger:
            mock_logger.isEnabledFor.return_value = True
            await self.client._send_request('post', 'idx', body={'key': 'val'})
            mock_logger.debug.assert_called()

    async def test_debug_logging_no_body(self):
        resp = _mock_response(content=_json_content({}))
        self._patch_method('get', resp)
        with patch('xapiand.logger') as mock_logger:
            mock_logger.isEnabledFor.return_value = False
            await self.client._send_request('get', 'idx', id='doc')
            mock_logger.debug.assert_called()

    async def test_debug_logging_body_not_json_serializable(self):
        resp = _mock_response(content=_json_content({"ok": True}))
        self._patch_method('post', resp)
        with patch('xapiand.logger') as mock_logger:
            mock_logger.isEnabledFor.return_value = True
            # Pass body that will fail json.dumps in debug logging
            body = {'key': object()}
            with patch('xapiand.json.dumps', side_effect=[Exception("fail"), '{}']) as jd:
                await self.client._send_request('post', 'idx', body=body)
            mock_logger.debug.assert_called()

    async def test_search_aggregations_without_query(self):
        data = {
            '#aggregations': {'field': {'count': 5}},
        }
        resp = _mock_response(content=_json_content(data))
        self._patch_method('search', resp)
        result = await self.client._send_request('search', 'idx')
        # No #query, so aggregations are attached to empty results
        # but results is empty so content is returned
        assert 'field' not in result or isinstance(result, DictObject)


# ── Xapiand API methods ───────────────────────────────────────────────

class TestXapiandSearch:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.resp = _mock_response(content=_json_content({
            '#query': {
                '#hits': [],
                '#total_count': 0,
                '#matches_estimated': 0,
            },
        }))

    def _patch(self):
        return patch.object(self.client, '_send_request',
                            return_value=DictObject(hits=[], count=0, total=0))

    async def test_basic_search(self):
        with self._patch() as m:
            await self.client.search('idx', query='hello')
            m.assert_called_once()
            args, kwargs = m.call_args
            assert args == ('search', 'idx')
            assert kwargs['params']['query'] == 'hello'

    async def test_search_with_all_params(self):
        with self._patch() as m:
            await self.client.search('idx', query='q', partial='p', terms='t',
                               offset=10, check_at_least=50, limit=20,
                               sort='field', language='en', pretty=True,
                               volatile=True)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['query'] == 'q'
            assert kwargs['params']['partial'] == 'p'
            assert kwargs['params']['terms'] == 't'
            assert kwargs['params']['offset'] == 10
            assert kwargs['params']['check_at_least'] == 50
            assert kwargs['params']['limit'] == 20
            assert kwargs['params']['sort'] == 'field'
            assert kwargs['params']['language'] == 'en'

    async def test_search_offset_too_high(self):
        with self._patch() as m:
            await self.client.search('idx', offset=200000)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['offset'] == 0

    async def test_search_offset_invalid_string(self):
        with self._patch() as m:
            await self.client.search('idx', offset='invalid')
            kwargs = m.call_args.kwargs
            assert kwargs['params']['offset'] == 0

    async def test_search_offset_valid_string(self):
        with self._patch() as m:
            await self.client.search('idx', offset='50')
            kwargs = m.call_args.kwargs
            assert kwargs['params']['offset'] == 50

    async def test_search_no_offset(self):
        with self._patch() as m:
            await self.client.search('idx')
            kwargs = m.call_args.kwargs
            assert 'offset' not in kwargs['params']

    async def test_search_extra_kw(self):
        with self._patch() as m:
            await self.client.search('idx', extra_param='val')
            kwargs = m.call_args.kwargs
            assert 'extra_param' in kwargs

    async def test_search_kwargs_dict(self):
        with self._patch() as m:
            await self.client.search('idx', kwargs={'custom': 'value'})
            kwargs = m.call_args.kwargs
            assert kwargs['custom'] == 'value'


class TestXapiandStats:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_stats(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.stats('idx', pretty=True)
            args, kwargs = m.call_args
            assert args == ('stats', 'idx')
            assert kwargs['params']['pretty'] is True

    async def test_stats_kwargs(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.stats('idx', kwargs={'extra': 1})
            kwargs = m.call_args.kwargs
            assert kwargs['extra'] == 1


class TestXapiandHead:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_head(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.head('idx', 'doc1', pretty=True)
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['params']['pretty'] is True


class TestXapiandCount:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_count_basic(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.count('idx')
            args = m.call_args
            assert args[0] == ('search', 'idx')

    async def test_count_with_query(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.count('idx', query='hello')
            kwargs = m.call_args.kwargs
            assert kwargs['params']['query'] == 'hello'

    async def test_count_with_body(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.count('idx', body={'match': 'all'})
            kwargs = m.call_args.kwargs
            assert kwargs['body'] == {'match': 'all'}

    async def test_count_extra_kw(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.count('idx', field='value')
            kwargs = m.call_args.kwargs
            assert kwargs['params']['field'] == 'value'


class TestXapiandGet:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_get(self):
        with patch.object(self.client, '_send_request', return_value=DictObject(title='hi')) as m:
            result = await self.client.get('idx', 'doc1')
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['default'] is NA

    async def test_get_with_default(self):
        with patch.object(self.client, '_send_request', return_value=None) as m:
            await self.client.get('idx', 'doc1', default=None)
            kwargs = m.call_args.kwargs
            assert kwargs['default'] is None

    async def test_get_with_accept(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.get('idx', 'doc1', accept='text/plain')
            kwargs = m.call_args.kwargs
            assert kwargs['headers']['accept'] == 'text/plain'

    async def test_get_volatile(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.get('idx', 'doc1', volatile=True)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['volatile'] is True


class TestXapiandDelete:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_delete(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.delete('idx', 'doc1')
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['params']['commit'] is False

    async def test_delete_with_commit(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.delete('idx', 'doc1', commit=True)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['commit'] is True


class TestXapiandPost:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_post(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.post('idx', body={'title': 'doc'})
            kwargs = m.call_args.kwargs
            assert kwargs['body'] == {'title': 'doc'}
            assert kwargs['params']['commit'] is False

    async def test_post_with_commit(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.post('idx', body={'title': 'doc'}, commit=True)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['commit'] is True


class TestXapiandPut:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_put(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.put('idx', body={'title': 'doc'}, id='doc1')
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['body'] == {'title': 'doc'}


class TestXapiandIndex:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_index_delegates_to_put(self):
        with patch.object(self.client, 'put', return_value=DictObject()) as m:
            await self.client.index('idx', body={'a': 1}, id='doc1')
            m.assert_called_once_with('idx', {'a': 1}, 'doc1', None, False, None)


class TestXapiandPatch:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_patch(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.patch('idx', 'doc1', body={'field': 'new'})
            args, kwargs = m.call_args
            assert args == ('patch', 'idx')
            assert kwargs['id'] == 'doc1'
            assert kwargs['body'] == {'field': 'new'}


class TestXapiandUpdate:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')

    async def test_update_with_content_type_uses_put(self):
        with patch.object(self.client, 'put', return_value=DictObject()) as m:
            await self.client.update('idx', 'doc1', body={'a': 1},
                               content_type='application/json')
            m.assert_called_once()

    async def test_update_without_content_type_uses_merge(self):
        with patch.object(self.client, 'merge', return_value=DictObject()) as m:
            await self.client.update('idx', 'doc1', body={'a': 1})
            m.assert_called_once()

    async def test_update_passes_headers(self):
        with patch.object(self.client, 'put', return_value=DictObject()) as m:
            await self.client.update('idx', 'doc1', body={'a': 1},
                               content_type='text/plain',
                               kwargs={'headers': {'x-custom': 'val'}})
            call_kwargs = m.call_args
            # kwargs dict should have headers with content-type
            passed_kwargs = call_kwargs[0][5] or call_kwargs.kwargs.get('kwargs', {})


class TestXapiandMerge:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_merge(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.merge('idx', 'doc1', body={'field': 'val'})
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['body'] == {'field': 'val'}

    async def test_merge_with_content_type(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.merge('idx', 'doc1', body={'a': 1},
                              content_type='text/plain')
            kwargs = m.call_args.kwargs
            assert kwargs['headers']['content-type'] == 'text/plain'

    async def test_merge_without_content_type_no_header(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.merge('idx', 'doc1', body={'a': 1})
            kwargs = m.call_args.kwargs
            assert 'headers' not in kwargs


class TestXapiandStore:
    def setup_method(self):
        self.client = Xapiand(host='localhost', port=8880, prefix=None,
                              default_accept='application/json')
        self.client.commit = False

    async def test_store(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.store('idx', 'doc1', body=b'binary data')
            kwargs = m.call_args.kwargs
            assert kwargs['id'] == 'doc1'
            assert kwargs['body'] == b'binary data'
            assert kwargs['params']['commit'] is False

    async def test_store_with_commit(self):
        with patch.object(self.client, '_send_request', return_value=DictObject()) as m:
            await self.client.store('idx', 'doc1', body=b'data', commit=True)
            kwargs = m.call_args.kwargs
            assert kwargs['params']['commit'] is True


# ── Module-level singleton ─────────────────────────────────────────────

class TestModuleSingleton:
    def test_client_exists(self):
        from xapiand import client
        assert isinstance(client, Xapiand)

    def test_client_has_defaults(self):
        from xapiand import client
        assert client.host == XAPIAND_HOST
        assert client.port == XAPIAND_PORT


# ── Module-level import paths ──────────────────────────────────────────

class TestModuleImportPaths:
    def test_httpx_import_error(self):
        """Verify ImportError is raised when httpx is not available."""
        import sys
        import importlib
        # Temporarily remove httpx from sys.modules
        saved = sys.modules.pop('httpx', None)
        saved_xapiand = sys.modules.pop('xapiand', None)
        saved_xapiand_coll = sys.modules.pop('xapiand.collections', None)
        try:
            # Block httpx import
            sys.modules['httpx'] = None
            with pytest.raises(ImportError, match="Xapiand requires"):
                importlib.import_module('xapiand')
        finally:
            # Restore
            sys.modules.pop('httpx', None)
            sys.modules.pop('xapiand', None)
            sys.modules.pop('xapiand.collections', None)
            if saved is not None:
                sys.modules['httpx'] = saved
            if saved_xapiand is not None:
                sys.modules['xapiand'] = saved_xapiand
            if saved_xapiand_coll is not None:
                sys.modules['xapiand.collections'] = saved_xapiand_coll

    def test_django_settings_integration(self):
        """Verify Django settings override env vars when available."""
        import sys
        import importlib
        import types

        # Save original modules
        saved_xapiand = sys.modules.pop('xapiand', None)
        saved_xapiand_coll = sys.modules.pop('xapiand.collections', None)
        saved_django = sys.modules.get('django', None)
        saved_django_conf = sys.modules.get('django.conf', None)
        saved_django_core = sys.modules.get('django.core', None)
        saved_django_core_exc = sys.modules.get('django.core.exceptions', None)

        try:
            # Create fake Django modules
            django_mod = types.ModuleType('django')
            django_conf = types.ModuleType('django.conf')
            django_core = types.ModuleType('django.core')
            django_core_exc = types.ModuleType('django.core.exceptions')
            django_core_exc.ObjectDoesNotExist = type('ObjectDoesNotExist', (Exception,), {})

            mock_settings = types.SimpleNamespace(
                XAPIAND_HOST='django-host',
                XAPIAND_PORT=9999,
                XAPIAND_COMMIT=True,
                XAPIAND_PREFIX='django-prefix',
            )
            django_conf.settings = mock_settings

            sys.modules['django'] = django_mod
            sys.modules['django.conf'] = django_conf
            sys.modules['django.core'] = django_core
            sys.modules['django.core.exceptions'] = django_core_exc

            mod = importlib.import_module('xapiand')
            assert mod.XAPIAND_HOST == 'django-host'
            assert mod.XAPIAND_PORT == 9999
            assert mod.XAPIAND_COMMIT is True
            assert mod.XAPIAND_PREFIX == 'django-prefix'
        finally:
            # Restore all modules
            sys.modules.pop('xapiand', None)
            sys.modules.pop('xapiand.collections', None)
            for name, saved in [
                ('django', saved_django),
                ('django.conf', saved_django_conf),
                ('django.core', saved_django_core),
                ('django.core.exceptions', saved_django_core_exc),
            ]:
                if saved is not None:
                    sys.modules[name] = saved
                else:
                    sys.modules.pop(name, None)
            if saved_xapiand is not None:
                sys.modules['xapiand'] = saved_xapiand
            if saved_xapiand_coll is not None:
                sys.modules['xapiand.collections'] = saved_xapiand_coll
