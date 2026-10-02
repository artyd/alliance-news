from app.forum import (category_to_department, department_topic_plan,
                       missing_topics)

DT = [
    {"code": "procurement", "name": {"ua": "Закупівля", "en": "Procurement"},
     "topics": [("api", {}), ("pvc", {})]},
    {"code": "logistics", "name": {"ua": "Логістика", "en": "Logistics"},
     "topics": [("maritime", {})]},
]


def test_plan_follows_menu_order_with_ua_names():
    plan = department_topic_plan(DT)
    assert [p["key"] for p in plan] == ["procurement", "logistics"]
    assert [p["name"] for p in plan] == ["Закупівля", "Логістика"]
    assert all(isinstance(p["icon_color"], int) for p in plan)


def test_category_maps_to_owning_department():
    m = category_to_department(DT)
    assert m == {"api": "procurement", "pvc": "procurement", "maritime": "logistics"}
    assert "unknown" not in m


def test_missing_topics_skips_known():
    plan = department_topic_plan(DT)
    assert [p["key"] for p in missing_topics(plan, {"procurement": 5})] == ["logistics"]
    assert missing_topics(plan, {"procurement": 5, "logistics": 7}) == []
