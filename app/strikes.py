"""«💥 Обстріли підприємств» — strikes on Ukrainian pharma & adjacent sites.

Pure, testable logic for the strikes monitor:
  * the watchlist of companies and the source list (Google News queries,
    Ukrainian news RSS, public Telegram channels);
  * a cheap keyword pre-filter applied before any LLM call;
  * the Telegram channel web-preview parser (t.me/s/<channel>);
  * LLM prompts (classify one report, match it to a known event, compose the
    event card, decide whether a new report adds significant details);
  * HTML formatting of the event card and of the follow-up updates.

One *event* (a strike on one site) is reported by many *items* (news articles,
Telegram posts). The orchestration in main.py classifies each item, attaches it
to an event, sends ONE card per event and posts significant updates as a reply
to that card.
"""

from __future__ import annotations

import datetime
import html as _html
import json
import re
import urllib.parse

from bs4 import BeautifulSoup

# ── Watchlist ───────────────────────────────────────────────────────────────
# Companies we track by name. `aliases` are matched case-insensitively as whole
# words (Cyrillic-aware). Ambiguous names (Дарниця is also a Kyiv district,
# Здоров'я / Технолог are common words) are listed with company-specific forms
# only, so the pre-filter does not fire on every Kyiv shelling. The LLM gets the
# canonical names and resolves the company itself.
WATCHLIST: list[dict] = [
    # Manufacturers
    {"name": "Фармак", "kind": "manufacturer",
     "aliases": ["фармак", "farmak"]},
    {"name": "Дарниця", "kind": "manufacturer",
     "aliases": ["фармацевтична фірма «дарниця»", "фармацевтична фірма дарниця",
                 "фф «дарниця»", "фф дарниця", "завод дарниця", "darnitsa"]},
    {"name": "Артеріум", "kind": "manufacturer",
     "aliases": ["артеріум", "arterium"]},
    {"name": "Київмедпрепарат", "kind": "manufacturer",
     "aliases": ["київмедпрепарат", "kyivmedpreparat"]},
    {"name": "Галичфарм", "kind": "manufacturer",
     "aliases": ["галичфарм", "galychpharm"]},
    # Not the bare adjective: "Борщагівська громада" / "Софіївська Борщагівка"
    # are places. Any case form of the adjective + a plant word.
    {"name": "Борщагівський ХФЗ", "kind": "manufacturer",
     "query": "борщагівський хіміко-фармацевтичний",
     "aliases": [r"re:борщагівськ\w*\s+(?:хіміко|хфз|завод|фармзавод|фармацевтичн)\w*",
                 "бхфз", "borshchahivskiy"]},
    {"name": "Здоров'я (Харків)", "kind": "manufacturer",
     "aliases": ["фармацевтична компанія «здоров'я»", "фармацевтична компанія здоров'я",
                 "фк «здоров'я»", "фк здоров'я", "фк «здоровʼя»", "фк здоровʼя",
                 "zdravo", "pharmaceutical company zdorovye"]},
    {"name": "Лубнифарм", "kind": "manufacturer",
     "aliases": ["лубнифарм", "лубни фарм", "lubnypharm"]},
    {"name": "Юрія-Фарм", "kind": "manufacturer",
     "aliases": ["юрія-фарм", "юрія фарм", "yuria-pharm", "yuria pharm"]},
    {"name": "Інфузія", "kind": "manufacturer",
     "aliases": ["інфузія", "infusia"]},
    {"name": "Біофарма", "kind": "manufacturer",
     "aliases": ["біофарма", "biopharma"]},
    {"name": "Мікрохім", "kind": "manufacturer",
     "aliases": ["мікрохім", "microkhim", "microchem"]},
    {"name": "Технолог (Умань)", "kind": "manufacturer",
     "aliases": ["пат «технолог»", "пат технолог", "фармацевтична компанія «технолог»",
                 "фармацевтична компанія технолог", "фк «технолог»", "фк технолог"]},
    {"name": "Фармстандарт-Біолік", "kind": "manufacturer",
     "aliases": ["біолік", "фармстандарт-біолік", "biolik"]},
    {"name": "Індар", "kind": "manufacturer",
     "aliases": ["індар", "indar"]},
    {"name": "Київський вітамінний завод", "kind": "manufacturer",
     "aliases": ["київський вітамінний завод", "вітамінний завод", "kyiv vitamin plant"]},
    # Distributors
    {"name": "БаДМ", "kind": "distributor", "aliases": ["бадм", "badm"]},
    {"name": "Оптіма-Фарм", "kind": "distributor",
     "aliases": ["оптіма-фарм", "оптіма фарм", "optima-pharm", "optima pharm"]},
    {"name": "Вента", "kind": "distributor",
     "aliases": ["вента. лтд", "вента лтд", "вента.лтд", "компанія «вента»", "venta ltd"]},
    # Pharmacy chains & their warehouses
    {"name": "АНЦ", "kind": "pharmacy", "aliases": ["анц", "аптека низьких цін"]},
    {"name": "Подорожник", "kind": "pharmacy", "aliases": ["подорожник"]},
    {"name": "Бажаємо здоров'я", "kind": "pharmacy",
     "aliases": ["бажаємо здоров'я", "бажаємо здоровʼя", "бажаємо здоров’я"]},
    {"name": "D.S.", "kind": "pharmacy", "aliases": ["аптека d.s.", "аптеки d.s.", "аптек d.s."]},
    {"name": "9-1-1", "kind": "pharmacy", "aliases": ["аптека 9-1-1", "аптеки 9-1-1", "аптек 9-1-1"]},
]

_KIND_UA = {"manufacturer": "виробник ліків", "distributor": "фармдистриб'ютор",
            "pharmacy": "аптечна мережа"}


def _word_re(words: list[str]) -> re.Pattern:
    """Whole-word, case-insensitive alternation (\\w is Unicode → Cyrillic-aware).
    An alias starting with 're:' is a raw regex (for case forms)."""
    alts = sorted({w[3:] if w.startswith("re:") else re.escape(w.lower()) for w in words},
                  key=len, reverse=True)
    return re.compile(r"(?<!\w)(?:" + "|".join(alts) + r")(?!\w)", re.IGNORECASE)


_WATCH_RE = [(c["name"], _word_re(c["aliases"])) for c in WATCHLIST]


def watchlist_hits(text: str) -> list[str]:
    """Canonical names of watchlist companies mentioned in the text."""
    t = _norm(text)
    return [name for name, rx in _WATCH_RE if rx.search(t)]


# ── Keyword pre-filter ──────────────────────────────────────────────────────
# A report must mention an attack AND an enterprise-type object (or a tracked
# company). Stems, matched as word prefixes. Kept broad on purpose: the LLM is
# the real judge, this only avoids paying for obviously unrelated news.
_ATTACK_STEMS = [
    "обстріл", "удар", "атак", "влучан", "влучил", "влучив", "приліт", "прилет",
    "шахед", "дрон", "бпла", "ракет", "авіабомб", "керован", "вибух", "пошкодж", "зруйнов",
    "руйнуван", "пожеж", "загорання", "уламк",
    "strike", "struck", "shelling", "attack", "missile", "drone", "damaged", "destroyed",
]
_OBJECT_STEMS = [
    "підприєм", "завод", "фабрик", "склад", "логістичн", "цех", "виробнич",
    "виробництв", "офіс", "фарм", "аптек", "ліки", "лікар", "медикамент",
    "термінал", "ангар", "комбінат", "промислов", "промзон", "інфраструктур",
    "enterprise", "plant", "factory", "warehouse", "pharma", "facility",
]
_ATTACK_RE = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, _ATTACK_STEMS)) + r")",
                        re.IGNORECASE)
_OBJECT_RE = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, _OBJECT_STEMS)) + r")",
                        re.IGNORECASE)


def _norm(text: str) -> str:
    # Unify apostrophes so "здоров'я" / "здоровʼя" / "здоров’я" all match.
    return (text or "").replace("ʼ", "'").replace("’", "'").lower()


def is_candidate(text: str) -> bool:
    """Cheap pre-filter: attack word AND (enterprise word OR tracked company)."""
    t = _norm(text)
    if not _ATTACK_RE.search(t):
        return False
    return bool(_OBJECT_RE.search(t)) or bool(watchlist_hits(t))


# ── Sources ─────────────────────────────────────────────────────────────────
_ATTACK_Q = "(обстріл OR удар OR атака OR влучання OR пошкоджено OR зруйновано OR пожежа)"


def google_news_url(query: str, days: int = 2) -> str:
    """Google News RSS search URL (Ukrainian edition) for a raw query string."""
    q = urllib.parse.quote_plus(f"{query} when:{days}d")
    return f"https://news.google.com/rss/search?q={q}&hl=uk&gl=UA&ceid=UA:uk"


def search_name(company: dict) -> str:
    """Spelling used in the Google News query: an explicit "query", else the
    first (most specific) alias."""
    return company.get("query") or company["aliases"][0]


def _company_queries(batch: int = 6) -> list[str]:
    names = []
    for c in WATCHLIST:
        names.append(f'"{search_name(c)}"')
    out = []
    for i in range(0, len(names), batch):
        out.append("(" + " OR ".join(names[i:i + batch]) + ") " + _ATTACK_Q)
    return out


GOOGLE_NEWS_QUERIES: list[str] = [
    # Enterprises in general (filtered down by the LLM to pharma & adjacent)
    _ATTACK_Q + " (підприємство OR завод OR склад OR \"логістичний центр\" OR цех)",
    # Pharma specifically
    "(фармацевтичний OR фармзавод OR \"аптечний склад\" OR фармкомпанія OR \"склад ліків\") "
    + _ATTACK_Q,
    # Outlets whose own RSS blocks us (403) — reach them through Google News
    "(site:suspilne.media OR site:epravda.com.ua OR site:mind.ua) " + _ATTACK_Q
    + " (підприємство OR завод OR склад)",
    # Pharma trade press
    "(site:apteka.ua OR site:pharma.net.ua OR site:pharmencyclopedia.com.ua) " + _ATTACK_Q,
] + _company_queries()

# Ukrainian news RSS feeds that answer from our server.
NEWS_RSS: dict[str, str] = {
    "Укрінформ": "https://www.ukrinform.ua/rss/block-lastnews",
    "Українська правда": "https://www.pravda.com.ua/rss/view_news/",
    "УНІАН": "https://www.unian.ua/rss",
    "LB.ua": "https://lb.ua/rss/ukr/news.xml",
    "Аптека.ua": "https://www.apteka.ua/category/rss",
}

# Public Telegram channels read through the web preview (no account needed).
# Oblast military administrations first report strikes on enterprises; the
# national newsrooms aggregate them.
TELEGRAM_CHANNELS: dict[str, str] = {
    "suspilnenews": "Суспільне Новини",
    "ukrinform_news": "Укрінформ",
    "ukrpravda_news": "Українська правда",
    "kharkivoda": "Харківська ОВА",
    "synegubov": "Харківська ОВА (Синєгубов)",
    "dnipropetrovskaODA": "Дніпропетровська ОВА",
    "zoda_gov_ua": "Запорізька ОВА",
    "ivan_fedorov_zp": "Запорізька обл. (Федоров)",
    "odeskaODA": "Одеська ОВА",
    "kyivoda": "Київська ОВА",
    "KyivCityOfficial": "КМДА",
    "poltavskaODA": "Полтавська ОВА",
    "cherkaskaODA": "Черкаська ОВА",
    "chernigivskaODA": "Чернігівська ОВА",
    "kozytskyy_maksym_official": "Львівська ОВА",
    "zhytomyrskaODA": "Житомирська ОВА",
    "khmelnytskaODA": "Хмельницька ОВА",
    "volynskaODA": "Волинська ОВА",
    "kirovogradskaODA": "Кіровоградська ОВА",
    "mykolaivskaODA": "Миколаївська ОВА",
    "dsns_telegram": "ДСНС України",
}


def parse_tg_channel_html(html: str, channel: str) -> list[dict]:
    """Parse a t.me/s/<channel> web preview into
    [{link, title, text, published (aware datetime|None), source}].
    Posts without text (photo-only) are skipped."""
    soup = BeautifulSoup(html or "", "html.parser")
    out: list[dict] = []
    for msg in soup.select(".tgme_widget_message[data-post]"):
        body = msg.select_one(".tgme_widget_message_text")
        if body is None:
            continue
        for br in body.find_all("br"):
            br.replace_with("\n")
        text = body.get_text("", strip=False).strip()
        if not text:
            continue
        post = msg["data-post"]                     # "channel/12345"
        published = None
        t = msg.select_one("time[datetime]")
        if t is not None:
            try:
                published = datetime.datetime.fromisoformat(t["datetime"])
            except ValueError:
                published = None
        first_line = text.split("\n", 1)[0].strip()
        out.append({
            "link": f"https://t.me/{post}",
            "title": first_line[:200],
            "text": text[:5000],
            "published": published,
            "source": TELEGRAM_CHANNELS.get(channel, "@" + channel),
        })
    return out


def strip_html(text: str) -> str:
    """RSS descriptions often carry HTML — reduce to plain text."""
    if not text:
        return ""
    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)


# ── LLM prompts ─────────────────────────────────────────────────────────────
OBJECT_TYPES = ["завод", "цех", "склад", "логістичний центр", "офіс", "аптека",
                "лабораторія", "інше"]


def _watchlist_block() -> str:
    return "\n".join(f"- {c['name']} ({_KIND_UA[c['kind']]})" for c in WATCHLIST)


# What the classifier may answer in "category" and which of those we report.
# The relevance rule lives here (in code), not in the model's judgement:
# pharma + adjacent sectors + unnamed enterprises/warehouses are reported;
# named businesses outside our sectors, energy etc. are not.
CATEGORIES = {
    "pharma": "фармацевтика: фармзавод, офіс/склад фармкомпанії, фармдистриб'ютора, "
              "аптечної мережі, аптека, склад ліків, фармлабораторія",
    "adjacent": "суміжне: виробник чи склад упаковки, субстанцій, хімії, медвиробів, "
                "косметики, харчової сировини, ветпрепаратів, кормів; логістичний центр, "
                "великий склад чи розподільчий центр (зокрема рітейлу/дистриб'ютора), "
                "вантажний термінал",
    "unnamed": "підприємство / завод / склад / цех / виробниче приміщення, галузь і назву "
               "якого не вказано",
    "other_business": "інший НАЗВАНИЙ бізнес поза переліченими галузями (одяг, меблі, "
                      "інструменти, ІТ, магазин, кафе, АЗС, автосервіс тощо)",
    "energy_infra": "енергетика (ТЕС, ТЕЦ, ГЕС, підстанції), газ, нафта, залізниця, порти",
    "not_enterprise": "житло, лікарні, школи, адмінбудівлі, авто, військові об'єкти, або "
                      "зведення без пошкодженого підприємства",
}
REPORTED_CATEGORIES = {"pharma", "adjacent", "unnamed"}
NEVER_REPORTED = {"not_enterprise", "energy_infra"}


def is_reportable(cls: dict) -> bool:
    """Our rule on top of the classifier output: a strike (not an old story),
    in Ukraine, on a site in a reported category — or on a tracked company."""
    if not cls.get("is_strike") or not cls.get("in_ukraine"):
        return False
    if cls.get("category") in REPORTED_CATEGORIES:
        return True
    # A tracked company counts even if the model filed it under "other
    # business" — but never housing / energy (e.g. "Борщагівська громада").
    return bool(cls.get("canonical")) and cls.get("category") not in NEVER_REPORTED


def build_classify_prompt(examples: list[tuple[str, str]] | None = None) -> str:
    cats = "\n".join(f'  "{k}": {v}' for k, v in CATEGORIES.items())
    return (
        "Ти аналітик фармацевтичного ринку України. Тобі дають одне повідомлення "
        "(новина або пост Telegram-каналу). Опиши, чи йдеться в ньому про УДАР / "
        "ОБСТРІЛ / АТАКУ (ракети, дрони, КАБ, артилерія), унаслідок якого "
        "ПОШКОДЖЕНО чи ЗРУЙНОВАНО об'єкт, і що це за об'єкт.\n\n"
        "is_strike=false для: аналітики, інтерв'ю, новин про відновлення давніх "
        "руйнувань, збитих цілей без пошкоджень.\n"
        "in_ukraine=false для ударів по території росії чи окупованих територіях.\n"
        "Якщо в одному повідомленні кілька об'єктів — опиши НАЙВАЖЛИВІШИЙ для "
        "фармацевтичного ринку (фарма > суміжне > непойменоване підприємство).\n\n"
        "category — ОДНЕ з:\n"
        f"{cats}\n\n"
        "Компанії зі списку відстеження (використовуй ці назви, якщо згадано):\n"
        f"{_watchlist_block()}\n\n"
        "Відповідай ЛИШЕ JSON:\n"
        "{\n"
        '  "is_strike": true|false,\n'
        '  "in_ukraine": true|false,\n'
        f'  "category": одне з {list(CATEGORIES)},\n'
        '  "company": "назва компанії або null, якщо не названо",\n'
        '  "watchlist": true|false,            // компанія зі списку відстеження\n'
        f'  "object_type": одне з {OBJECT_TYPES},\n'
        '  "is_pharma": true|false,            // фарма/аптеки/ліки напряму\n'
        '  "city": "населений пункт або null",\n'
        '  "region": "область у форматі «Харківська обл.» (Київ = «м. Київ») або null",\n'
        '  "attack_date": "YYYY-MM-DD або null",\n'
        '  "damage": "що пошкоджено/зруйновано, пожежа — коротко або null",\n'
        '  "casualties": "загиблі/поранені або null",\n'
        '  "production": "зупинено / працює / невідомо",\n'
        '  "summary": "1-2 речення українською: що сталося"\n'
        "}\n"
        "Не вигадуй фактів, яких немає в тексті."
        + team_examples_block(examples)
    )


def team_examples_block(examples: list[tuple[str, str]] | None) -> str:
    """The team's own decisions on borderline reports (Mini App «🎯 Перевірка
    ударів»): 'show' = should be reported, 'hide' = should not. They override
    the general rules above for similar cases."""
    if not examples:
        return ""
    lines = [f"- {'ПОКАЗУВАТИ' if v == 'show' else 'НЕ показувати'}: {t[:160]}" for t, v in examples[:20]]
    return ("\n\nРІШЕННЯ КОМАНДИ щодо схожих повідомлень (вони важливіші за загальні правила; "
            "для схожого випадку став category/is_strike так, щоб результат збігся):\n" + "\n".join(lines))


def is_borderline(status: str, cls: dict) -> bool:
    """Reports worth a human look: dropped as «інший бізнес» though it was a
    strike in Ukraine, or reported only as an unnamed / adjacent site."""
    if not cls or not cls.get("is_strike") or not cls.get("in_ukraine"):
        return False
    cat = cls.get("category")
    if status == "irrelevant":
        return cat == "other_business"
    if status == "matched":
        return cat in ("unnamed", "adjacent") and not cls.get("is_pharma")
    return False


def build_classify_input(item: dict) -> str:
    when = item.get("published")
    when_s = when.strftime("%Y-%m-%d %H:%M") if isinstance(when, datetime.datetime) else ""
    return (f"Джерело: {item.get('source', '')}\nДата публікації: {when_s}\n"
            f"Заголовок: {item.get('title', '')}\n\nТекст:\n{(item.get('text') or '')[:4000]}")


def build_match_prompt() -> str:
    return (
        "Тобі дають НОВЕ повідомлення про удар по підприємству та список ВІДОМИХ "
        "подій (id, дата, місце, об'єкт, компанія, опис). Визнач, чи описує нове "
        "повідомлення ту саму подію (той самий удар по тому самому об'єкту), що й "
        "одна з відомих.\n"
        "ТА САМА подія, якщо:\n"
        "- той самий об'єкт / та сама компанія в тому ж місті — навіть якщо назву "
        "написано інакше (повна / скорочена / «завод ліків» замість «ХФЗ»);\n"
        "- дати відрізняються не більше ніж на 1 день (нічна атака, а новину "
        "опубліковано вранці чи наступного дня);\n"
        "- нове повідомлення не називає компанію, але галузь, тип об'єкта і місто "
        "збігаються з відомою подією (напр. «фармзавод у Києві» і «Борщагівський "
        "ХФЗ у Києві»), або воно уточнює раніше анонімне «підприємство» в тому ж місці.\n"
        "РІЗНІ події: явно інший об'єкт (інша компанія, інша галузь) — навіть у тому ж "
        "місті й тієї ж ночі; або той самий об'єкт, але удари в різні дні з проміжком "
        "понад 1 день.\n"
        'Відповідай ЛИШЕ JSON: {"event_id": <id або null>}'
    )


def canonical_company(cls: dict, text: str = "") -> str | None:
    """Watchlist name for the company the classifier found (or that the text
    mentions), so every report about e.g. Борщагівський ХФЗ carries the same
    name and events can be merged without the LLM."""
    # The name must actually occur in the report: the model sometimes copies a
    # watchlist name onto an unrelated place with a similar name.
    in_text = watchlist_hits(text)
    names = watchlist_hits(_val(cls.get("company")))
    if not names and cls.get("watchlist"):
        names = in_text
    return next((n for n in names if n in in_text), None)


def build_match_input(cls: dict, events: list[dict]) -> str:
    lines = [f"НОВЕ: дата={cls.get('attack_date')}, місце={cls.get('city')}, "
             f"{cls.get('region')}, об'єкт={cls.get('object_type')}, "
             f"компанія={cls.get('company')}; {cls.get('summary')}", "", "ВІДОМІ:"]
    for e in events:
        lines.append(f"- id={e['id']}: дата={e.get('attack_date')}, місце={e.get('city')}, "
                     f"{e.get('region')}, об'єкт={e.get('object_type')}, "
                     f"компанія={e.get('company')}; {e.get('summary') or e.get('headline')}")
    return "\n".join(lines)


_CARD_FIELDS = (
    '{\n'
    '  "headline": "заголовок до 120 символів: хто/що, де",\n'
    '  "company": "назва або null",\n'
    f'  "object_type": одне з {OBJECT_TYPES},\n'
    '  "city": "населений пункт або null",\n'
    '  "region": "область у форматі «Харківська обл.» (місто Київ = «м. Київ») або null",\n'
    '  "attack_time": "коли був удар, по-людськи: «у ніч на 9 жовтня», «7 жовтня близько 17:00» — або null",\n'
    '  "weapon": "чим атакували (дрони/ракети/КАБ) або null",\n'
    '  "damage": "масштаб ушкоджень: що пошкоджено/зруйновано, площа пожежі тощо",\n'
    '  "casualties": "загиблі/поранені або null",\n'
    '  "production": "статус виробництва/роботи: зупинено / частково / працює / невідомо",\n'
    '  "market_impact": "вплив на ринок або null",\n'
    '  "summary": "повний виклад події українською"\n'
    '}\n'
)


def build_card_prompt() -> str:
    return (
        "Ти аналітик фармацевтичного ринку України. Тобі дають УСІ доступні "
        "повідомлення з різних джерел про ОДИН удар по підприємству. Збери з них "
        "повну картину: усі деталі, цифри, назви, наслідки. Суперечності вкажи явно "
        "(«за даними ОВА …, за даними ЗМІ …»).\n"
        "Поле market_impact заповнюй, лише якщо об'єкт стосується фарми, аптек, "
        "сировини чи логістики: які препарати/категорії можуть зникнути або "
        "подорожчати, чи є ризик дефіциту, хто може замінити обсяги. Пиши це як "
        "оцінку («ймовірно», «можливий»), не як факт. Якщо даних мало — null.\n"
        "summary — 3-8 речень, без обмеження на деталі, але без води і повторів. "
        "НЕ вигадуй фактів, яких немає в повідомленнях.\n"
        "Відповідай ЛИШЕ JSON:\n" + _CARD_FIELDS
    )


def build_sources_input(items: list[dict], per_item: int = 3000, max_items: int = 10) -> str:
    parts = []
    for i, it in enumerate(items[:max_items], 1):
        parts.append(f"[{i}] {it.get('source', '')} — {it.get('title', '')}\n"
                     f"{(it.get('text') or '')[:per_item]}")
    return "\n\n".join(parts)


def build_update_prompt() -> str:
    return (
        "Ти ведеш картку події «удар по підприємству». Тобі дають ПОТОЧНУ картку "
        "(JSON) і НОВІ повідомлення. Визнач, чи є в нових повідомленнях СУТТЄВІ нові "
        "деталі: назва компанії чи об'єкта, яких не було; зупинка/відновлення "
        "виробництва; нові дані про загиблих/поранених; масштаб руйнувань; офіційна "
        "заява компанії; вплив на постачання. Перефразування, ті самі факти іншими "
        "словами, дрібні уточнення — НЕ суттєві.\n"
        "Відповідай ЛИШЕ JSON:\n"
        "{\n"
        '  "significant": true|false,\n'
        '  "update_text": "2-5 речень українською: ЩО САМЕ нового (лише нове)" або null,\n'
        '  "card": <оновлена повна картка в тому ж форматі, що й поточна>\n'
        "}\n"
        "Формат картки:\n" + _CARD_FIELDS
    )


def parse_json(text: str) -> dict:
    """Lenient JSON object parse for LLM output ({} on failure)."""
    if not text:
        return {}
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else {}
    except ValueError:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return {}
        try:
            v = json.loads(m.group(0))
            return v if isinstance(v, dict) else {}
        except ValueError:
            return {}


# ── Formatting ──────────────────────────────────────────────────────────────
TELEGRAM_LIMIT = 4096
_HASHTAG = "#обстріли_підприємств"


def _e(s) -> str:
    return _html.escape(str(s), quote=False) if s else ""


def _val(v) -> str:
    """Normalise an LLM field: None / 'null' / 'невідомо' → ''."""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("null", "none", "n/a", "-", "", "невідомо", "unknown") else s


_KYIV_NAMES = {"київ", "м. київ", "м.київ", "місто київ", "kyiv", "kiev"}


def normalize_region(region, city=None) -> str:
    """One spelling per oblast for the registry filters and event matching:
    'Харківська область' / 'Харківщина' → 'Харківська обл.'; the city of Kyiv
    (whatever the model wrote as its region) → 'м. Київ'."""
    region, city = _val(region), _val(city)
    if city.lower() in _KYIV_NAMES or region.lower() in _KYIV_NAMES:
        return "м. Київ"
    if not region:
        return ""
    r = re.sub(r"\s+", " ", region).strip()
    r = re.sub(r"(?i)\s*област[ьі]\.?$|\s*обл\.?$", "", r)
    if r.lower().endswith("щина"):        # Харківщина → Харківська
        r = r[:-4] + "ська"
    return f"{r[:1].upper()}{r[1:]} обл." if r.lower().endswith("ська") else region


def format_place(city: str, region: str) -> str:
    city, region = _val(city), _val(region)
    # "Київ" + "м. Київ" → just the region; "Харків" + "Харківська обл." → both.
    if city and region and not ("обл" not in region.lower() and city.lower() in region.lower()):
        return f"{city}, {region}"
    return region or city


def format_sources(sources: list[dict], limit: int = 8) -> str:
    """'<a>Укрінформ</a> · <a>Суспільне</a> …' — one link per distinct source."""
    seen, links = set(), []
    for s in sources:
        name = (s.get("source") or "").strip() or "джерело"
        if name in seen or not s.get("link"):
            continue
        seen.add(name)
        links.append(f'<a href="{_html.escape(s["link"], quote=True)}">{_e(name)}</a>')
        if len(links) >= limit:
            break
    return " · ".join(links)


def format_card(card: dict, sources: list[dict]) -> str:
    """HTML card for Telegram (≤ 4096 chars)."""
    rows = []
    company = _val(card.get("company"))
    rows.append(f"🏭 <b>Компанія:</b> {_e(company) if company else 'не називається'}")
    if _val(card.get("object_type")):
        rows.append(f"🏷 <b>Об'єкт:</b> {_e(_val(card['object_type']))}")
    place = format_place(card.get("city"), card.get("region"))
    if place:
        rows.append(f"📍 <b>Місце:</b> {_e(place)}")
    if _val(card.get("attack_time")):
        rows.append(f"🕒 <b>Коли:</b> {_e(_val(card['attack_time']))}")
    if _val(card.get("weapon")):
        rows.append(f"🎯 <b>Засоби ураження:</b> {_e(_val(card['weapon']))}")
    if _val(card.get("damage")):
        rows.append(f"💔 <b>Наслідки:</b> {_e(_val(card['damage']))}")
    if _val(card.get("casualties")):
        rows.append(f"🩹 <b>Постраждалі:</b> {_e(_val(card['casualties']))}")
    if _val(card.get("production")):
        rows.append(f"⚙️ <b>Виробництво:</b> {_e(_val(card['production']))}")
    if _val(card.get("market_impact")):
        rows.append(f"📈 <b>Вплив на ринок:</b> {_e(_val(card['market_impact']))}")

    head = f"💥 <b>{_e(_val(card.get('headline')) or 'Удар по підприємству')}</b>"
    src = format_sources(sources)
    tail = (f"\n\n🔗 <b>Джерела ({len(sources)}):</b> {src}" if src else "") + f"\n{_HASHTAG}"
    summary = _val(card.get("summary"))
    fixed = head + "\n\n" + "\n".join(rows)
    budget = TELEGRAM_LIMIT - len(fixed) - len(tail) - 10
    body = _e(summary)
    if len(body) > budget:
        body = body[:max(0, budget - 1)].rsplit(" ", 1)[0] + "…"
    return fixed + (f"\n\n{body}" if body else "") + tail


def format_update(card: dict, update_text: str, new_sources: list[dict]) -> str:
    """HTML follow-up posted as a reply to the original card."""
    head = "🔄 <b>Оновлення:</b> " + _e(_val(card.get("headline")) or "удар по підприємству")
    src = format_sources(new_sources)
    tail = (f"\n\n🔗 {src}" if src else "") + f"\n{_HASHTAG}"
    body = _e(update_text or "")
    budget = TELEGRAM_LIMIT - len(head) - len(tail) - 10
    if len(body) > budget:
        body = body[:max(0, budget - 1)].rsplit(" ", 1)[0] + "…"
    return f"{head}\n\n{body}{tail}"


def event_as_fact(ev: dict) -> dict:
    """Shape a strike event like an article_facts row for the digest pipeline."""
    card = ev.get("card") or {}
    what = _val(card.get("headline")) or _val(ev.get("headline"))
    summary = _val(card.get("summary"))
    return {
        "id": f"strike-{ev['id']}",
        "event_type": "strike",
        "what_happened": f"{what}. {summary}".strip(". ") if summary else what,
        "who": _val(card.get("company")),
        "where_loc": format_place(card.get("city"), card.get("region")),
        "magnitude": "; ".join(filter(None, [_val(card.get("damage")),
                                             _val(card.get("production"))])),
        "supply_chain_impact": _val(card.get("market_impact")),
        "ukraine_relevance": "high",
        "affected_sectors": "",
        "source_url": ev.get("first_link") or "",
        "source_publisher": ev.get("first_source") or "",
    }
