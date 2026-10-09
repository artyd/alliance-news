import datetime

from app import strikes
from app.telegram_articles import bucket_facts_by_department


def test_candidate_needs_attack_and_enterprise():
    assert strikes.is_candidate("Унаслідок удару дронів пошкоджено склад у Києві")
    assert strikes.is_candidate("Росіяни атакували підприємство на Харківщині")
    # attack without an enterprise-type object
    assert not strikes.is_candidate("Ракетна атака: пошкоджено житловий будинок")
    # enterprise without an attack
    assert not strikes.is_candidate("Фармак відкрив новий склад у Київській області")


def test_candidate_by_company_name():
    assert strikes.is_candidate("Ворог влучив у Лубнифарм")


def test_cab_is_not_cabinet():
    # "каб" must not fire on "кабмін" — only full attack words count
    assert not strikes.is_candidate("Кабмін ухвалив рішення щодо підприємств")


def test_watchlist_hits_whole_words_and_apostrophes():
    assert strikes.watchlist_hits("Пошкоджено склад компанії «Фармак»") == ["Фармак"]
    assert "Бажаємо здоров'я" in strikes.watchlist_hits("аптека «Бажаємо здоровʼя» постраждала")
    # Kyiv district, not the pharma company
    assert strikes.watchlist_hits("удар по Дарницькому району, Дарниця") == []
    assert strikes.watchlist_hits("ФФ «Дарниця» призупинила роботу") == ["Дарниця"]
    # short alias must not match inside other words
    assert strikes.watchlist_hits("Франція передала допомогу") == []


_TG_HTML = """
<div class="tgme_widget_message_wrap"><div class="tgme_widget_message js-widget_message"
  data-post="kharkivoda/100">
  <div class="tgme_widget_message_text js-message_text">⚡️ Удар по підприємству<br/>
  У Харкові пошкоджено склад.</div>
  <time datetime="2026-10-09T06:44:56+00:00" class="time">09:44</time>
</div></div>
<div class="tgme_widget_message_wrap"><div class="tgme_widget_message js-widget_message"
  data-post="kharkivoda/101">
  <time datetime="2026-10-09T07:00:00+00:00" class="time">10:00</time>
</div></div>
"""


def test_parse_tg_channel_html():
    items = strikes.parse_tg_channel_html(_TG_HTML, "kharkivoda")
    assert len(items) == 1                      # photo-only post skipped
    it = items[0]
    assert it["link"] == "https://t.me/kharkivoda/100"
    assert it["title"] == "⚡️ Удар по підприємству"
    assert "пошкоджено склад" in it["text"]
    assert it["published"] == datetime.datetime(2026, 10, 9, 6, 44, 56,
                                                tzinfo=datetime.timezone.utc)
    assert it["source"] == "Харківська ОВА"


def test_google_news_url_is_ukrainian_edition():
    url = strikes.google_news_url('"фармак" (удар OR обстріл)', days=2)
    assert url.startswith("https://news.google.com/rss/search?q=")
    assert "when%3A2d" in url and url.endswith("hl=uk&gl=UA&ceid=UA:uk")


def test_every_watchlist_company_is_queried():
    joined = " ".join(strikes.GOOGLE_NEWS_QUERIES)
    for c in strikes.WATCHLIST:
        assert f'"{strikes.search_name(c)}"' in joined
        assert "re:" not in strikes.search_name(c)


def test_parse_json_lenient():
    assert strikes.parse_json('{"relevant": true}') == {"relevant": True}
    assert strikes.parse_json('text {"a": 1} tail') == {"a": 1}
    assert strikes.parse_json("garbage") == {}
    assert strikes.parse_json("[1, 2]") == {}


def _card(**kw):
    card = {"headline": "Удар по складу у Харкові", "company": None,
            "object_type": "склад", "city": "Харків", "region": "Харківська обл.",
            "damage": "пошкоджено дах", "production": "невідомо",
            "market_impact": "null", "summary": "Опис події."}
    card.update(kw)
    return card


def test_format_card_fields_and_escaping():
    html = strikes.format_card(_card(company="Фармак <test>"),
                               [{"source": "Укрінформ", "link": "https://x/1?a=1&b=2"},
                                {"source": "Укрінформ", "link": "https://x/2"}])
    assert html.startswith("💥 <b>Удар по складу у Харкові</b>")
    assert "Фармак &lt;test&gt;" in html
    assert "📍 <b>Місце:</b> Харків, Харківська обл." in html
    assert "Вплив на ринок" not in html                 # 'null' dropped
    assert html.count("Укрінформ</a>") == 1             # one link per source
    assert 'href="https://x/1?a=1&amp;b=2"' in html
    assert "не називається" not in html


def test_format_card_unnamed_company_and_limit():
    html = strikes.format_card(_card(summary="слово " * 2000),
                               [{"source": f"S{i}", "link": f"https://x/{i}"} for i in range(20)])
    assert "🏭 <b>Компанія:</b> не називається" in html
    assert len(html) <= strikes.TELEGRAM_LIMIT
    assert html.endswith("#обстріли_підприємств")


def test_format_place_dedups_city_in_region():
    assert strikes.format_place("Київ", "м. Київ") == "м. Київ"
    assert strikes.format_place("Лубни", "Полтавська обл.") == "Лубни, Полтавська обл."
    assert strikes.format_place(None, "null") == ""


def test_normalize_region():
    assert strikes.normalize_region("Київська область", "Київ") == "м. Київ"
    assert strikes.normalize_region("Київська область", "Бровари") == "Київська обл."
    assert strikes.normalize_region("Харківщина") == "Харківська обл."
    assert strikes.normalize_region("Одещина") == "Одеська обл."
    assert strikes.normalize_region("Полтавська обл.") == "Полтавська обл."
    assert strikes.normalize_region("null", None) == ""


def test_reportable_rule():
    base = {"is_strike": True, "in_ukraine": True}
    assert strikes.is_reportable(dict(base, category="pharma"))
    assert strikes.is_reportable(dict(base, category="unnamed"))
    assert not strikes.is_reportable(dict(base, category="other_business"))
    assert strikes.is_reportable(dict(base, category="other_business", canonical="Фармак"))
    # a tracked name never overrides housing / energy
    assert not strikes.is_reportable(dict(base, category="not_enterprise", canonical="Фармак"))
    assert not strikes.is_reportable(dict(base, category="pharma", in_ukraine=False))
    assert not strikes.is_reportable({"is_strike": False, "in_ukraine": True, "category": "pharma"})


def test_borshchahivka_place_is_not_the_plant():
    # Regression: houses in "Борщагівська громада" were filed as Борщагівський ХФЗ.
    text = "У Борщагівський громаді фіксують пошкодження приватних будинків"
    assert strikes.watchlist_hits(text) == []
    cls = {"company": "Борщагівський ХФЗ", "watchlist": True}
    assert strikes.canonical_company(cls, text) is None
    for t in ("Удар по Борщагівському хіміко-фармацевтичному заводу",
              "Росія атакувала Борщагівський ХФЗ", "дрони вдарили по БХФЗ",
              "пошкоджено цех Борщагівського заводу", "Борщагівський фармацевтичний завод"):
        assert strikes.watchlist_hits(t) == ["Борщагівський ХФЗ"], t
        assert strikes.canonical_company({"company": "Борщагівський завод"}, t) == "Борщагівський ХФЗ"


def test_canonical_requires_name_in_text():
    assert strikes.canonical_company({"company": "Фармак"}, "Удар по складу в Києві") is None
    assert strikes.canonical_company({"company": "АТ «Фармак»"}, "Удар по складу Фармак") == "Фармак"


def test_format_update():
    html = strikes.format_update(_card(), "Це був завод «Фармак».",
                                 [{"source": "Суспільне", "link": "https://s/1"}])
    assert html.startswith("🔄 <b>Оновлення:</b> Удар по складу у Харкові")
    assert "Це був завод «Фармак»." in html and "Суспільне</a>" in html


def test_event_as_fact_lands_in_strikes_department():
    ev = {"id": 7, "card": _card(company="Фармак", production="зупинено"),
          "first_link": "https://x/1", "first_source": "Укрінформ"}
    fact = strikes.event_as_fact(ev)
    assert fact["id"] == "strike-7" and fact["event_type"] == "strike"
    assert fact["who"] == "Фармак" and "зупинено" in fact["magnitude"]
    depts = [{"code": "laws", "sectors": [], "event_types": ["regulation"]},
             {"code": "strikes", "sectors": [], "event_types": ["strike"]}]
    out = bucket_facts_by_department([fact], depts)
    assert out["strikes"] == [fact] and out["laws"] == []
