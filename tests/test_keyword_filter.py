"""Unit tests for the keyword pre-filter.

Run:  python tests/test_keyword_filter.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.keyword_filter import passes_keyword_filter

KW = ["red sea", "hormuz", "houthi", "blank sailing", "port congestion"]


def test_no_keywords_passes_everything():
    assert passes_keyword_filter("anything", "at all", []) is True
    assert passes_keyword_filter("x", "y", None) is True


def test_match_in_title():
    assert passes_keyword_filter("Houthi strike hits tanker", "", KW) is True


def test_match_in_summary():
    assert passes_keyword_filter("Shipping update", "Vessels avoid the Red Sea route", KW) is True


def test_case_insensitive():
    assert passes_keyword_filter("BLANK SAILING announced by MSC", "", KW) is True


def test_no_match_filtered_out():
    assert passes_keyword_filter("Company posts quarterly profit", "routine earnings", KW) is False


def test_multiword_phrase():
    assert passes_keyword_filter("Severe port congestion at Rotterdam", "", KW) is True
    assert passes_keyword_filter("port improvements", "congestion easing elsewhere", KW) is False


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")
            passed += 1
    print(f"\n{passed} tests passed")
