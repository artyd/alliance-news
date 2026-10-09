"""Mini App (webapp.html) server-side helpers — pure, testable logic.

* the bottom toolbar: which sections exist and how a user's choice is
  normalised (max 5 tabs, "Моє" always pinned last);
* "Спитати Харві" — the AI chat: keyword extraction for retrieval, the context
  block handed to the model and the system prompt;
* department-level subscription toggles for the settings screen.

DB access, the LLM call and HTTP live in main.py.
"""

from __future__ import annotations

import re

# ── Toolbar ─────────────────────────────────────────────────────────────────
# Sections a user may pin to the bottom bar ("me" is always there, last).
TOOLBAR_SECTIONS = ["today", "feed", "strikes", "markets", "currencies",
                    "reports", "tracking", "weather", "warehouse"]
PINNED = "me"
MAX_TABS = 5                       # including the pinned "Моє"
DEFAULT_TOOLBAR = ["today", "feed", "strikes", "markets"]


def normalize_toolbar(keys) -> list[str]:
    """User's chosen sections → valid, unique, at most MAX_TABS-1, in order.
    "me" is stripped (it is pinned by the client). Empty → the default."""
    out: list[str] = []
    for k in keys or []:
        k = str(k).strip()
        if k in TOOLBAR_SECTIONS and k not in out:
            out.append(k)
    out = out[:MAX_TABS - 1]
    return out or list(DEFAULT_TOOLBAR)


THEMES = {"auto", "light", "dark"}


def normalize_theme(theme) -> str:
    return theme if theme in THEMES else "auto"


# ── Subscriptions (department level) ────────────────────────────────────────
def department_states(department_topics: list[dict], subs: str | None) -> list[dict]:
    """[{code, emoji, name, on}] — a department is on when all its topics are."""
    if subs in (None, "", "all"):
        enabled = None
    elif subs == "none":
        enabled = set()
    else:
        enabled = {c for c in subs.split(",") if c}
    out = []
    for d in department_topics:
        codes = [c for c, _ in d["topics"]]
        on = True if enabled is None else all(c in enabled for c in codes)
        out.append({"code": d["code"], "emoji": d.get("emoji", ""),
                    "name": d["name"], "on": on})
    return out


def set_department(department_topics: list[dict], subs: str | None,
                   dept_code: str, on: bool) -> str:
    """Switch every topic of one department on/off; returns the storage value
    ('all' / 'none' / CSV in menu order)."""
    all_codes = [c for d in department_topics for c, _ in d["topics"]]
    if subs in (None, "", "all"):
        enabled = set(all_codes)
    elif subs == "none":
        enabled = set()
    else:
        enabled = {c for c in subs.split(",") if c}
    dept = next((d for d in department_topics if d["code"] == dept_code), None)
    if dept is None:
        raise ValueError(f"unknown department {dept_code!r}")
    codes = {c for c, _ in dept["topics"]}
    enabled = (enabled | codes) if on else (enabled - codes)
    if not enabled:
        return "none"
    if enabled >= set(all_codes):
        return "all"
    return ",".join(c for c in all_codes if c in enabled)


# ── "Спитати Харві" ─────────────────────────────────────────────────────────
_STOP = {
    "що", "як", "який", "яка", "які", "яке", "чи", "де", "коли", "чому", "хто", "про",
    "для", "від", "або", "але", "щодо", "цього", "цей", "ця", "ці", "тиждень", "тижня",
    "сьогодні", "вчора", "новини", "новина", "розкажи", "покажи", "поясни", "будь",
    "ласка", "мені", "нам", "наш", "наша", "наші", "було", "буде", "є", "та", "і", "в",
    "у", "на", "з", "із", "до", "по", "за", "не", "так", "ще", "вже", "все", "усі", "всі",
    "що", "сталося", "відбувається", "останні", "останній", "зараз", "ситуація",
    "what", "how", "why", "when", "where", "the", "and", "for", "with", "about", "news",
    "what's", "is", "are", "was", "this", "that", "week", "today",
}


def question_keywords(question: str, limit: int = 6) -> list[str]:
    """Search stems from a question: lower-case words ≥ 4 letters, stop-words
    dropped, cut to a 6-letter stem so Ukrainian case endings still match
    («парацетамолу» → «парацe…»). Order kept, duplicates removed."""
    words = re.findall(r"[a-zа-яіїєґ0-9'’-]+", (question or "").lower())
    out: list[str] = []
    for w in words:
        w = w.strip("'’-")
        if len(w) < 4 or w in _STOP:
            continue
        stem = w[:6] if len(w) > 6 else w
        if stem not in out:
            out.append(stem)
        if len(out) >= limit:
            break
    return out


def build_context(news: list[dict], strikes: list[dict], markets: list[dict],
                  fx: list[dict]) -> tuple[str, list[dict]]:
    """Numbered context block for the model + the source list the client shows.
    Each source: {n, kind ('news'|'strike'), id, title, link}."""
    sources: list[dict] = []
    lines: list[str] = []

    def add(kind, id_, title, link, body):
        n = len(sources) + 1
        sources.append({"n": n, "kind": kind, "id": id_, "title": title, "link": link})
        lines.append(f"[{n}] {body}")

    for e in strikes:
        card = e.get("card") or {}
        title = card.get("headline") or e.get("headline") or ""
        bits = [title, card.get("company") or e.get("company") or "",
                card.get("damage") or "", card.get("production") or "",
                card.get("market_impact") or ""]
        when = (e.get("sent_at") or "")[:10]
        add("strike", e["id"], title, e.get("first_link") or "",
            f"УДАР ({when}): " + " | ".join(b for b in bits if b and b != "null"))
    for a in news:
        title = a.get("title_ua") or a.get("title") or ""
        summary = (a.get("summary_ua") or a.get("summary_en") or "")[:600]
        add("news", a["id"], title, a.get("link") or "",
            f"НОВИНА ({(a.get('published') or '')[:10]}, {a.get('category')}): {title}. {summary}")

    if markets:
        mk = "; ".join(f"{m['label']}: {m['current']} {m.get('unit', '')} "
                       f"({m['change_pct']:+.1f}% за день)" for m in markets if m.get("current"))
        lines.append(f"РИНКИ зараз: {mk}")
    if fx:
        lines.append("КУРСИ НБУ: " + "; ".join(
            f"{c['code']} {c.get('rate_uah', c.get('rate', '')):.2f} грн"
            if isinstance(c.get('rate_uah', c.get('rate')), (int, float))
            else f"{c['code']}" for c in fx))
    return "\n".join(lines), sources


def build_ask_prompt(lang: str = "ua") -> str:
    language = "English" if lang == "en" else "Ukrainian"
    return (
        "Ти — Харві, заєць-аналітик у застосунку Alliance News. Допомагаєш команді "
        "українського імпортера фармацевтичної та хімічної сировини (Китай, Індія) "
        "розібратися в новинах, ринках, логістиці, регуляціях і ударах по "
        "підприємствах.\n"
        f"Відповідай мовою: {language}. Коротко й по суті: 2–6 речень або короткий "
        "список. Якщо доречно — додай один практичний висновок для закупівлі чи "
        "логістики.\n"
        "ПРАВИЛА:\n"
        "- Спирайся ЛИШЕ на наданий КОНТЕКСТ. Посилайся на джерела номерами в "
        "квадратних дужках, напр. [2] або [1][4].\n"
        "- Якщо в контексті немає відповіді — чесно скажи, що в базі бота цього немає, "
        "і запропонуй, що можна запитати інакше. Нічого не вигадуй: ні цифр, ні дат.\n"
        "- Можна виділяти **жирним** ключові цифри. Без заголовків і таблиць.\n"
        "- Тон дружній і професійний; можна зрідка легкий жарт у стилі зайця-аналітика, "
        "але не в темі обстрілів і загиблих."
    )
