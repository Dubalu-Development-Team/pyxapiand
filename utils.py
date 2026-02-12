# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2018 Dubalu LLC. All rights reserved.
# Copyright (C) 2006,2007,2008,2009,2010,2011,2012 Olly Betts
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
"""Xapian-compatible binary serialization utilities.

Provides functions for serializing and deserializing lengths, strings,
and single characters using Xapian's variable-length encoding format.

The length encoding scheme uses a single byte for values below 255,
and a variable-length encoding (7 bits per byte with continuation bit)
for larger values.

These utilities are compatible with the encoding used by the Xapian
search engine library.
"""
from __future__ import annotations


def serialise_length(length: int) -> str:
    """Serialize an integer length using Xapian's variable-length encoding.

    Values below 255 are encoded as a single byte. Values of 255 or
    greater are encoded as ``0xff`` followed by ``(length - 255)`` in a
    7-bit variable-length format (little-endian, high bit set on the
    last byte).

    Args:
        length: Non-negative integer to encode.

    Returns:
        str: The encoded length as a string of characters.

    Example:
        >>> serialise_length(42)
        '*'
        >>> serialise_length(300)
        '\\xff\\xad'
    """
    if length < 255:
        return chr(length)
    result = chr(0xff)
    length -= 255
    while True:
        b = length & 0x7f
        length >>= 7
        if not length:
            result += chr(b | 0x80)
            break
        result += chr(b)
    return result


def unserialise_length(data: str, check_remaining: bool = False) -> tuple[int, str]:
    """Deserialize a length from Xapian's variable-length encoding.

    Inverse of ``serialise_length``. Reads the encoded length from the
    beginning of ``data`` and returns both the decoded length and the
    remaining unconsumed data.

    Args:
        data: String containing the encoded length at the beginning.
        check_remaining: If ``True``, raises ``ValueError`` when the
            decoded length exceeds the remaining data size.

    Returns:
        tuple[int, str]: A tuple of ``(length, remaining_data)`` where
            ``length`` is the decoded integer and ``remaining_data``
            is the unconsumed portion of the input.

    Raises:
        ValueError: If the data is empty, the encoding is incomplete,
            or ``check_remaining`` is ``True`` and the length exceeds
            the remaining data.
    """
    if not data:
        raise ValueError("Bad encoded length: no data")
    length = ord(data[0])
    if length == 0xff:
        length = 0
        shift = 0
        for i, ch in enumerate(data[1:], 1):
            b = ord(ch)
            length |= (b & 0x7f) << shift
            shift += 7
            if b & 0x80:
                break
        else:
            raise ValueError("Bad encoded length: insufficient data")
        length += 255
        data = data[i + 1:]
    else:
        data = data[1:]
    if check_remaining and length > len(data):
        raise ValueError("Bad encoded length: length greater than data")
    return length, data


def serialise_string(s: str) -> str:
    """Serialize a string by prepending its encoded length.

    Encodes the string length using ``serialise_length`` and prepends
    it to the string itself.

    Args:
        s: String to serialize.

    Returns:
        str: The length-prefixed encoded string.
    """
    return serialise_length(len(s)) + s


def unserialise_string(data: str) -> tuple[str, str]:
    """Deserialize a length-prefixed string.

    Reads the encoded length from the beginning of ``data``, extracts
    that many characters as the string, and returns the string along
    with any remaining data.

    Args:
        data: String containing a length-prefixed encoded string.

    Returns:
        tuple[str, str]: A tuple of ``(decoded_string, remaining_data)``.

    Raises:
        ValueError: If the data is malformed or insufficient.
    """
    length, data = unserialise_length(data, True)
    return data[:length], data[length:]


def serialise_char(c: str) -> str:
    """Serialize a single character.

    Validates that the input is exactly one character and returns it
    unchanged.

    Args:
        c: Single character string to serialize.

    Returns:
        str: The character itself.

    Raises:
        ValueError: If ``c`` is not exactly one character long.
    """
    if len(c) != 1:
        raise ValueError("Serialisation error: Cannot serialise empty char")
    return c


def unserialise_char(data: str) -> tuple[str, str]:
    """Deserialize a single character from the beginning of data.

    Extracts the first character and returns it along with the
    remaining data.

    Args:
        data: String to extract the first character from.

    Returns:
        tuple[str, str]: A tuple of ``(character, remaining_data)``.

    Raises:
        ValueError: If ``data`` is empty.
    """
    if len(data) < 1:
        raise ValueError("Bad encoded length: insufficient data")
    return data[:1], data[1:]
