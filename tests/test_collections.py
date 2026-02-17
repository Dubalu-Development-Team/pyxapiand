"""Tests for xapiand.collections — DictObject and OrderedDictObject."""
from __future__ import annotations

from xapiand.collections import DictObject, OrderedDictObject


# ── DictObject ──────────────────────────────────────────────────────────────────────────────────────────────

class TestDictObject:
    """Tests for DictObject attribute-style dict access."""

    def test_init_kwargs(self):
        obj = DictObject(a=1, b=2)
        assert obj["a"] == 1
        assert obj.b == 2

    def test_init_dict(self):
        obj = DictObject({"x": 10})
        assert obj.x == 10

    def test_init_pairs(self):
        obj = DictObject([("k1", "v1"), ("k2", "v2")])
        assert obj.k1 == "v1"
        assert obj["k2"] == "v2"

    def test_init_empty(self):
        obj = DictObject()
        assert len(obj) == 0

    def test_set_via_dict(self):
        obj = DictObject()
        obj["key"] = "val"
        assert obj.key == "val"

    def test_set_via_attr(self):
        obj = DictObject()
        obj.key = "val"
        assert obj["key"] == "val"

    def test_delete_via_dict(self):
        obj = DictObject(a=1)
        del obj["a"]
        assert "a" not in obj

    def test_delete_via_attr(self):
        obj = DictObject(a=1)
        del obj.a
        assert "a" not in obj

    def test_dict_identity(self):
        obj = DictObject(a=1)
        assert obj.__dict__ is obj


# ── OrderedDictObject ───────────────────────────────────────────────────────────────────────────────────────

class TestOrderedDictObject:
    """Tests for OrderedDictObject attribute-style access with insertion order."""

    def test_init_pairs(self):
        obj = OrderedDictObject([("a", 1), ("b", 2), ("c", 3)])
        assert obj.a == 1
        assert obj["b"] == 2
        assert list(obj.keys()) == ["a", "b", "c"]

    def test_init_empty(self):
        obj = OrderedDictObject()
        assert len(obj) == 0

    def test_set_via_attr(self):
        obj = OrderedDictObject()
        obj.x = 99
        assert obj["x"] == 99

    def test_set_via_dict(self):
        obj = OrderedDictObject()
        obj["y"] = 100
        assert obj.y == 100

    def test_delete_via_attr(self):
        obj = OrderedDictObject([("a", 1)])
        del obj.a
        assert "a" not in obj

    def test_delete_via_dict(self):
        obj = OrderedDictObject([("a", 1)])
        del obj["a"]
        assert "a" not in obj

    def test_delete_missing_raises(self):
        obj = OrderedDictObject()
        try:
            del obj.nonexistent
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_preserves_order(self):
        obj = OrderedDictObject()
        obj["z"] = 1
        obj["a"] = 2
        obj["m"] = 3
        assert list(obj.keys()) == ["z", "a", "m"]

    def test_dict_is_self(self):
        obj = OrderedDictObject([("a", 1)])
        assert obj.__dict__ is obj

    def test_init_kwargs(self):
        obj = OrderedDictObject(x=10, y=20)
        assert obj.x == 10
        assert obj["y"] == 20
