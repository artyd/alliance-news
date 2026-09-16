"""Unit tests for the department subscription logic.

Run:  python tests/test_subscriptions.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.subscriptions import (
    all_topic_codes, is_subscribed, toggle_topic, toggle_department,
    normalize, build_department_keyboard,
)

DEPTS = [
    {"code": "procurement", "name": {"ua": "Закупівля", "en": "Procurement"},
     "topics": [("api", {"ua": "API", "en": "API"}),
                ("food", {"ua": "Харчова", "en": "Food"})]},
    {"code": "laws", "name": {"ua": "Закони", "en": "Laws"},
     "topics": [("apteka", {"ua": "Аптека", "en": "Apteka"}),
                ("kmu", {"ua": "КМУ", "en": "KMU"})]},
]
ALL = all_topic_codes(DEPTS)  # ['api','food','apteka','kmu']


def test_all_topic_codes_order():
    assert ALL == ["api", "food", "apteka", "kmu"]


def test_all_sentinel_means_everything():
    assert is_subscribed("all", "kmu", ALL) is True
    assert is_subscribed(None, "api", ALL) is True


def test_none_sentinel_means_nothing():
    assert is_subscribed("none", "api", ALL) is False


def test_toggle_from_all_removes_one():
    # 'all' → toggle off 'api' → explicit set of the rest
    res = toggle_topic("all", "api", ALL)
    assert res == "food,apteka,kmu"
    assert is_subscribed(res, "api", ALL) is False
    assert is_subscribed(res, "food", ALL) is True


def test_toggle_back_to_full_collapses_to_all():
    res = toggle_topic("food,apteka,kmu", "api", ALL)
    assert res == "all"


def test_toggle_last_off_becomes_none():
    res = toggle_topic("api", "api", ALL)  # only api was on
    # normalize of empty → 'none'
    assert res == "none"


def test_toggle_department_all_on_then_off():
    dept_codes = ["apteka", "kmu"]
    on = toggle_department("none", dept_codes, ALL)
    assert set(on.split(",")) == {"apteka", "kmu"}
    off = toggle_department(on, dept_codes, ALL)
    assert off == "none"


def test_normalize_stable_order():
    assert normalize({"kmu", "api"}, ALL) == "api,kmu"


def test_keyboard_structure_and_checkmarks():
    kb = build_department_keyboard(DEPTS, 0, "api", lang="ua")
    rows = kb["inline_keyboard"]
    # dept-all row + 2 topic rows + nav row + lang row + footer row
    assert len(rows) == 6
    # api is on (✅), food is off (☐)
    api_btn = rows[1][0]
    food_btn = rows[2][0]
    assert api_btn["text"].startswith("✅")
    assert api_btn["callback_data"] == "dtog:0:api"
    assert food_btn["text"].startswith("☐")
    # nav row wraps: prev of idx 0 is last dept (1)
    nav = rows[3]
    assert nav[0]["callback_data"] == "dnav:1"
    assert nav[2]["callback_data"] == "dnav:1"
    assert "1/2" in nav[1]["text"]
    # language row toggles to the other language
    lang_btn = rows[4][0]
    assert lang_btn["callback_data"] == "dlang:0"
    assert "English" in lang_btn["text"]  # currently ua → offers en


def test_keyboard_lang_button_en():
    kb = build_department_keyboard(DEPTS, 0, "all", lang="en")
    lang_btn = kb["inline_keyboard"][4][0]
    assert "Українська" in lang_btn["text"]  # currently en → offers ua


def test_keyboard_wraps_index():
    kb = build_department_keyboard(DEPTS, 5, "none")  # 5 % 2 == 1 → laws
    center = kb["inline_keyboard"][3][1]["text"]
    assert "Закони" in center and "2/2" in center


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")
            passed += 1
    print(f"\n{passed} tests passed")
