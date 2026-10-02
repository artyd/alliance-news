from app.forum import (REPORTS_KEY, category_to_department, missing_topics,
                       topic_plan)

DT = [
    {"code": "procurement", "name": {"ua": "Закупівля", "en": "Procurement"},
     "topics": [("api", {}), ("pvc", {})]},
    {"code": "logistics", "name": {"ua": "Логістика", "en": "Logistics"},
     "topics": [("maritime", {})]},
]


def test_plan_puts_reports_first_then_departments():
    plan = topic_plan(DT)
    assert [p["key"] for p in plan] == [REPORTS_KEY, "procurement", "logistics"]
    assert [p["name"] for p in plan] == ["📋 Звіти", "Закупівля", "Логістика"]
    assert all(isinstance(p["icon_color"], int) for p in plan)


def test_plan_uses_user_language_with_ua_fallback():
    assert [p["name"] for p in topic_plan(DT, "en")] == ["📋 Reports", "Procurement", "Logistics"]
    assert topic_plan(DT, "ru")[0]["name"] == "📋 Звіти"


def test_category_maps_to_owning_department():
    m = category_to_department(DT)
    assert m == {"api": "procurement", "pvc": "procurement", "maritime": "logistics"}


def test_missing_topics_keeps_plan_order():
    plan = topic_plan(DT)
    assert [p["key"] for p in missing_topics(plan, {"procurement": 5})] == [REPORTS_KEY, "logistics"]
    assert missing_topics(plan, {REPORTS_KEY: 1, "procurement": 5, "logistics": 7}) == []
