"""Tests for xapiand.utils — Xapian binary serialization utilities."""
from __future__ import annotations

import pytest

from xapiand.utils import (
    serialise_char,
    serialise_length,
    serialise_string,
    unserialise_char,
    unserialise_length,
    unserialise_string,
)


# ── serialise_length / unserialise_length ──────────────────────────────

class TestSerialiseLength:
    def test_zero(self):
        assert serialise_length(0) == chr(0)

    def test_small_value(self):
        assert serialise_length(42) == chr(42)

    def test_boundary_254(self):
        assert serialise_length(254) == chr(254)

    def test_boundary_255(self):
        encoded = serialise_length(255)
        assert encoded[0] == chr(0xff)

    def test_large_value(self):
        encoded = serialise_length(1000)
        assert encoded[0] == chr(0xff)
        assert len(encoded) > 1


class TestUnserialiseLength:
    def test_empty_data_raises(self):
        with pytest.raises(ValueError, match="no data"):
            unserialise_length("")

    def test_small_value(self):
        length, remaining = unserialise_length(chr(42) + "extra")
        assert length == 42
        assert remaining == "extra"

    def test_insufficient_data_raises(self):
        # 0xff followed by no continuation bytes
        with pytest.raises(ValueError, match="insufficient data"):
            unserialise_length(chr(0xff))

    def test_check_remaining_raises(self):
        # Encode length 10 but provide only 2 chars of remaining data
        encoded = serialise_length(10)
        with pytest.raises(ValueError, match="length greater than data"):
            unserialise_length(encoded + "ab", check_remaining=True)

    def test_check_remaining_ok(self):
        encoded = serialise_length(3)
        length, remaining = unserialise_length(encoded + "abc", check_remaining=True)
        assert length == 3
        assert remaining == "abc"


class TestRoundtripLength:
    @pytest.mark.parametrize("value", [0, 1, 127, 254, 255, 256, 500, 1000, 16383, 16384, 100000])
    def test_roundtrip(self, value):
        encoded = serialise_length(value)
        decoded, remaining = unserialise_length(encoded)
        assert decoded == value
        assert remaining == ""

    def test_roundtrip_with_trailing_data(self):
        encoded = serialise_length(300) + "tail"
        decoded, remaining = unserialise_length(encoded)
        assert decoded == 300
        assert remaining == "tail"


# ── serialise_string / unserialise_string ──────────────────────────────

class TestSerialiseString:
    def test_empty_string(self):
        result = serialise_string("")
        assert result == chr(0)

    def test_short_string(self):
        result = serialise_string("hello")
        length, remaining = unserialise_length(result)
        assert length == 5
        assert remaining == "hello"


class TestUnserialiseString:
    def test_roundtrip(self):
        original = "hello world"
        encoded = serialise_string(original)
        decoded, remaining = unserialise_string(encoded)
        assert decoded == original
        assert remaining == ""

    def test_roundtrip_with_trailing(self):
        encoded = serialise_string("abc") + "XYZ"
        decoded, remaining = unserialise_string(encoded)
        assert decoded == "abc"
        assert remaining == "XYZ"

    def test_empty_string_roundtrip(self):
        encoded = serialise_string("")
        decoded, remaining = unserialise_string(encoded)
        assert decoded == ""
        assert remaining == ""

    def test_insufficient_data_raises(self):
        # Encode length 10 but only provide 2 chars
        encoded = serialise_length(10) + "ab"
        with pytest.raises(ValueError, match="length greater than data"):
            unserialise_string(encoded)


# ── serialise_char / unserialise_char ──────────────────────────────────

class TestSerialiseChar:
    def test_single_char(self):
        assert serialise_char("A") == "A"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot serialise empty char"):
            serialise_char("")

    def test_multi_char_raises(self):
        with pytest.raises(ValueError, match="Cannot serialise empty char"):
            serialise_char("AB")


class TestUnserialiseChar:
    def test_single_char(self):
        char, remaining = unserialise_char("Ahello")
        assert char == "A"
        assert remaining == "hello"

    def test_exactly_one_char(self):
        char, remaining = unserialise_char("X")
        assert char == "X"
        assert remaining == ""

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="insufficient data"):
            unserialise_char("")
