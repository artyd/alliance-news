import pytest

from app import miniapp

DEPTS = [
    {"code": "procurement", "emoji": "🛒", "name": {"ua": "Закупівля"},
     "topics": [("api", {}), ("food", {})]},
    {"code": "logistics", "emoji": "🚢", "name": {"ua": "Логістика"},
     "topics": [("logistics", {}), ("maritime", {})]},
    {"code": "strikes", "emoji": "💥", "name": {"ua": "Обстріли"},
     "topics": [("strikes", {})]},
]


def test_toolbar_normalized():
    assert miniapp.normalize_toolbar(["feed", "me", "bogus", "feed", "reports"]) == ["feed", "reports"]
    # at most 4 + pinned "Моє" = 5 tabs
    assert miniapp.normalize_toolbar(miniapp.TOOLBAR_SECTIONS) == miniapp.TOOLBAR_SECTIONS[:4]
    assert miniapp.normalize_toolbar([]) == miniapp.DEFAULT_TOOLBAR
    assert miniapp.normalize_toolbar(None) == miniapp.DEFAULT_TOOLBAR
    assert miniapp.normalize_toolbar(["reports"]) == ["reports"]


def test_theme():
    assert miniapp.normalize_theme("dark") == "dark"
    assert miniapp.normalize_theme("neon") == "auto"


def test_department_states():
    st = miniapp.department_states(DEPTS, "api,food,strikes")
    assert [(d["code"], d["on"]) for d in st] == [("procurement", True), ("logistics", False), ("strikes", True)]
    assert all(d["on"] for d in miniapp.department_states(DEPTS, "all"))
    assert not any(d["on"] for d in miniapp.department_states(DEPTS, "none"))
    # partially enabled department counts as off
    assert miniapp.department_states(DEPTS, "api")[0]["on"] is False


def test_set_department():
    assert miniapp.set_department(DEPTS, "all", "logistics", False) == "api,food,strikes"
    assert miniapp.set_department(DEPTS, "api,food,strikes", "logistics", True) == "all"
    assert miniapp.set_department(DEPTS, "strikes", "strikes", False) == "none"
    assert miniapp.set_department(DEPTS, "none", "strikes", True) == "strikes"
    with pytest.raises(ValueError):
        miniapp.set_department(DEPTS, "all", "nope", True)


def test_question_keywords():
    kw = miniapp.question_keywords("Що сталося з цінами на парацетамол цього тижня?")
    assert "цінами" in kw and "параце" in kw
    assert "що" not in kw and "тижня" not in kw
    assert miniapp.question_keywords("") == []


def test_build_context_numbers_sources():
    news = [{"id": 5, "title": "T", "title_ua": "Заголовок", "link": "https://n/5",
             "published": "2026-10-09 10:00:00", "category": "api", "summary_ua": "Опис"}]
    strikes = [{"id": 2, "card": {"headline": "Удар по заводу", "company": "Фармак"},
                "sent_at": "2026-10-09T08:00:00+00:00", "first_link": "https://s/2"}]
    markets = [{"label": "Нафта", "current": 80.5, "unit": "$", "change_pct": 1.234}]
    fx = [{"code": "USD", "rate_uah": 41.2}]
    text, sources = miniapp.build_context(news, strikes, markets, fx)
    assert [s["kind"] for s in sources] == ["strike", "news"]
    assert sources[0] == {"n": 1, "kind": "strike", "id": 2, "title": "Удар по заводу", "link": "https://s/2"}
    assert "[1] УДАР (2026-10-09): Удар по заводу | Фармак" in text
    assert "[2] НОВИНА (2026-10-09, api): Заголовок. Опис" in text
    assert "Нафта: 80.5 $ (+1.2% за день)" in text
    assert "USD 41.20 грн" in text


def test_prompt_language():
    assert "Ukrainian" in miniapp.build_ask_prompt("ua")
    assert "English" in miniapp.build_ask_prompt("en")
