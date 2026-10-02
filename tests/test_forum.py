from app.forum import (REPORTS_KEY, category_to_department, missing_topics,
                       topic_plan)

DT = [
    {"code": "procurement", "name": {"ua": "Закупівля", "en": "Procurement"},
     "topics": [("api", {}), ("pvc", {})]},
    {"code": "logistics", "name": {"ua": "Логістика", "en": "Logistics"},
     "topics": [("maritime", {})]},
]


def test_plan_creates_bottom_tab_first_and_reports_last():
    # Telegram shows newest topics first → displayed: Звіти, Закупівля, Логістика.
    plan = topic_plan(DT)
    assert [p["key"] for p in plan] == ["logistics", "procurement", REPORTS_KEY]
    assert [p["name"] for p in plan] == ["Логістика", "Закупівля", "📋 Звіти"]
    assert all(isinstance(p["icon_color"], int) for p in plan)


def test_plan_uses_user_language_with_ua_fallback():
    assert [p["name"] for p in topic_plan(DT, "en")] == ["Logistics", "Procurement", "📋 Reports"]
    assert topic_plan(DT, "ru")[-1]["name"] == "📋 Звіти"


def test_category_maps_to_owning_department():
    m = category_to_department(DT)
    assert m == {"api": "procurement", "pvc": "procurement", "maritime": "logistics"}


def test_missing_topics_keeps_plan_order():
    plan = topic_plan(DT)
    assert [p["key"] for p in missing_topics(plan, {"procurement": 5})] == ["logistics", REPORTS_KEY]
    assert missing_topics(plan, {REPORTS_KEY: 1, "procurement": 5, "logistics": 7}) == []
