# Copyright (c) 2015-2019 Dubalu LLC
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
"""Dict subclasses with attribute-style access.

Provides ``DictObject`` and ``OrderedDictObject``, which allow accessing
dictionary keys as attributes (``obj.key`` instead of ``obj['key']``).
``DictObject`` is used as the ``object_pairs_hook`` when deserializing
JSON and msgpack responses from the Xapiand server.

Example:
    >>> obj = DictObject(name='test', value=42)
    >>> obj.name
    'test'
    >>> obj['value']
    42
"""
from __future__ import annotations

from collections import OrderedDict
from typing import Any


class OrderedDictObject(OrderedDict):
    """Ordered dictionary with attribute-style access.

    Extends ``collections.OrderedDict`` so that keys can be accessed,
    set, and deleted as object attributes while preserving insertion
    order.

    The implementation uses ``__new__`` to wire the instance's
    ``__dict__`` to the dict storage itself, enabling transparent
    attribute access.

    Example:
        >>> obj = OrderedDictObject([('a', 1), ('b', 2)])
        >>> obj.a
        1
        >>> obj.c = 3
        >>> list(obj.keys())
        ['a', 'b', 'c']
    """

    def __new__(cls, *args, **kwargs):
        """Create a new ``OrderedDictObject`` instance.

        Wires the instance's ``__dict__`` to the underlying dict storage
        so that attribute access maps directly to dict key access.

        Args:
            *args: Positional arguments passed to ``OrderedDict``.
            **kwargs: Keyword arguments passed to ``OrderedDict``.

        Returns:
            OrderedDictObject: A new instance with attribute-style access.
        """
        attrs = getattr(OrderedDict(), '__dict__', {})
        attrs.update(cls.__dict__)
        obj = type(cls.__name__, cls.__bases__, attrs)
        self = OrderedDict.__new__(obj, *args, **kwargs)
        OrderedDict.__setattr__(self, '__dict__', self)
        OrderedDict.__init__(self, *args, **kwargs)
        return self

    def __setattr__(self, name: str, value: Any) -> None:
        """Set an attribute by delegating to ``__setitem__``.

        Args:
            name: Attribute/key name.
            value: Value to assign.
        """
        self.__setitem__(name, value)

    def __delattr__(self, name: str) -> None:
        """Delete an attribute by delegating to ``__delitem__``.

        Args:
            name: Attribute/key name to delete.

        Raises:
            KeyError: If the key does not exist.
        """
        self.__delitem__(name)


class DictObject(dict):
    """Dictionary with attribute-style access.

    A simple ``dict`` subclass that maps its internal ``__dict__`` to
    itself, allowing keys to be accessed as attributes.

    Used as ``object_pairs_hook`` for JSON and msgpack deserialization
    to provide convenient dot-notation access to response fields.

    Example:
        >>> obj = DictObject(name='test', value=42)
        >>> obj.name
        'test'
        >>> obj['name']
        'test'
    """

    def __init__(self, *args, **kwargs):
        """Initialize the ``DictObject``.

        Args:
            *args: Positional arguments passed to ``dict``.
            **kwargs: Keyword arguments passed to ``dict``.
        """
        dict.__init__(self, *args, **kwargs)
        self.__dict__ = self
