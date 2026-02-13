"""Tests for pyxapiand.constants — Xapian term constants."""
from __future__ import annotations

from pyxapiand import constants


class TestDateConstants:
    def test_term_values(self):
        assert constants.HOUR_TERM == "hour"
        assert constants.DAY_TERM == "day"
        assert constants.MONTH_TERM == "month"
        assert constants.YEAR_TERM == "year"
        assert constants.DECADE_TERM == "decade"
        assert constants.CENTURY_TERM == "century"
        assert constants.MILLENIUM_TERM == "millenium"

    def test_day_to_year_accuracy(self):
        assert constants.DAY_TO_YEAR_ACCURACY == ["day", "month", "year"]

    def test_year_accuracy(self):
        assert constants.YEAR_ACCURACY == ["year"]

    def test_hour_to_year_accuracy(self):
        assert constants.HOUR_TO_YEAR_ACCURACY == ["hour", "day", "month", "year"]


class TestGeopointConstants:
    def test_level_terms(self):
        assert constants.LEVEL_0_TERM == 0
        assert constants.LEVEL_5_TERM == 5

    def test_state_to_block(self):
        assert constants.STATE_TO_BLOCK_ACCURACY == [5, 10, 15]

    def test_area_to_block(self):
        assert constants.AREA_TO_BLOCK_ACCURACY == [10, 15]


class TestNumericConstants:
    def test_level_terms(self):
        assert constants.LEVEL_10_TERM == 10
        assert constants.LEVEL_100_TERM == 100
        assert constants.LEVEL_1000_TERM == 1000
        assert constants.LEVEL_10000_TERM == 10000
        assert constants.LEVEL_100000_TERM == 100000
        assert constants.LEVEL_1000000_TERM == 1000000
        assert constants.LEVEL_10000000_TERM == 10000000

    def test_tens_to_ten_thousands(self):
        assert constants.TENS_TO_TEN_THOUSANDS_ACCURACY == [10, 100, 1000, 10000]

    def test_tens(self):
        assert constants.TENS_ACCURACY == [10]

    def test_hundreds_to_millions(self):
        assert constants.HUDREDS_TO_MILLIONS_ACCURACY == [100, 1000, 10000, 100000, 1000000]

    def test_hundreds(self):
        assert constants.HUDREDS_ACCURACY == [100]

    def test_hundreds_to_thousands(self):
        assert constants.HUDREDS_TO_THOUSANDS_ACCURACY == [100, 1000]

    def test_thousands(self):
        assert constants.THOUSANDS_ACCURACY == [1000]

    def test_hundreds_to_ten_thousands(self):
        assert constants.HUDREDS_TO_TEN_THOUSANDS_ACCURACY == [100, 1000, 10000]
