"""Department-paginated topic subscription menu — pure, testable logic.

A user's `telegram_users.subscriptions` is a CSV of enabled category codes,
with two sentinels: 'all' (everything) and 'none' (nothing). The Telegram menu
shows one department at a time; the user pages between departments with ◀ ▶ and
toggles individual topics (checkboxes). All selection math lives here so it can
be unit-tested without Telegram/DB; main.py only wires it to callbacks + DB.

department_topics shape (defined in main.py):
    [
      {"code": "procurement", "name": {"ua": "...", "en": "..."},
       "topics": [("api", {"ua": "...", "en": "..."}), ...]},
      ...
    ]
"""

from __future__ import annotations

_UI = {
    "ua": {"dept_all": "Увесь відділ", "all_on": "✅ Усі теми",
           "done": "Готово", "saved": "Збережено"},
    "en": {"dept_all": "Whole department", "all_on": "✅ All topics",
           "done": "Done", "saved": "Saved"},
}


def all_topic_codes(department_topics: list[dict]) -> list[str]:
    """Every subscribable code across all departments, in display order."""
    return [code for d in department_topics for code, _ in d["topics"]]


def _expand(current_subs: str | None, all_codes: list[str]) -> set:
    """Resolve the stored value into a concrete set of enabled codes."""
    if current_subs in ("all", None, ""):
        return set(all_codes)
    if current_subs == "none":
        return set()
    return {c for c in current_subs.split(",") if c}


def normalize(subset, all_codes: list[str]) -> str:
    """Collapse a set back to storage form: 'all' / 'none' / stable CSV."""
    s = set(subset)
    if not s:
        return "none"
    if s >= set(all_codes):
        return "all"
    return ",".join(c for c in all_codes if c in s)  # stable, deduped order


def is_subscribed(current_subs: str | None, code: str, all_codes: list[str]) -> bool:
    return code in _expand(current_subs, all_codes)


def toggle_topic(current_subs: str | None, code: str, all_codes: list[str]) -> str:
    s = _expand(current_subs, all_codes)
    s.discard(code) if code in s else s.add(code)
    return normalize(s, all_codes)


def toggle_department(current_subs: str | None, dept_codes: list[str],
                      all_codes: list[str]) -> str:
    """If every topic in the department is on, turn them all off; else turn all on."""
    s = _expand(current_subs, all_codes)
    if set(dept_codes) <= s:
        s -= set(dept_codes)
    else:
        s |= set(dept_codes)
    return normalize(s, all_codes)


def build_department_keyboard(department_topics: list[dict], dept_idx: int,
                              current_subs: str | None, lang: str = "ua") -> dict:
    """Build the inline keyboard for one department page."""
    total = len(department_topics)
    if total == 0:
        return {"inline_keyboard": []}
    dept_idx %= total
    dept = department_topics[dept_idx]
    all_codes = all_topic_codes(department_topics)
    subset = _expand(current_subs, all_codes)
    L = _UI.get(lang, _UI["ua"])

    def nm(names: dict) -> str:
        return names.get(lang) or names.get("ua") or ""

    dept_codes = [code for code, _ in dept["topics"]]
    dept_all_on = bool(dept_codes) and set(dept_codes) <= subset

    rows: list = [[{
        "text": ("✅ " if dept_all_on else "◻️ ") + L["dept_all"],
        "callback_data": f"dall:{dept_idx}",
    }]]
    for code, names in dept["topics"]:
        on = code in subset
        rows.append([{
            "text": ("✅ " if on else "☐ ") + nm(names),
            "callback_data": f"dtog:{dept_idx}:{code}",
        }])

    prev_idx = (dept_idx - 1) % total
    next_idx = (dept_idx + 1) % total
    rows.append([
        {"text": "◀", "callback_data": f"dnav:{prev_idx}"},
        {"text": f"{nm(dept['name'])} · {dept_idx + 1}/{total}", "callback_data": "noop"},
        {"text": "▶", "callback_data": f"dnav:{next_idx}"},
    ])
    # Language toggle — button shows the language it switches TO.
    other_lang = "en" if lang == "ua" else "ua"
    lang_label = "🇬🇧 English" if other_lang == "en" else "🇺🇦 Українська"
    rows.append([{"text": lang_label, "callback_data": f"dlang:{dept_idx}"}])
    rows.append([
        {"text": L["all_on"], "callback_data": "dsub:all"},
        {"text": L["done"], "callback_data": "ddone"},
    ])
    return {"inline_keyboard": rows}
