"""Topics ("гілки") inside the bot's private chat with each user — pure logic.

Since Bot API 9.3/9.4 a bot with "Threaded mode" enabled in @BotFather can
create forum topics in its private chat with a user (createForumTopic with the
user's chat_id) and send into them via message_thread_id. Each user gets:
  * "📋 Звіти" — shown first — receives only the daily/midday/weekly reports;
  * one topic per business department — receives only that department's news.

Thread ids are created once per user and persisted by main.py; this module only
decides names/order and which topic a news category belongs to.
"""

from __future__ import annotations

REPORTS_KEY = "reports"
_REPORTS_NAME = {"ua": "📋 Звіти", "en": "📋 Reports"}

# Telegram allows only these six icon colours for createForumTopic.
_ICON_COLORS = [0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F]


def topic_plan(department_topics: list[dict], lang: str = "ua") -> list[dict]:
    """Topics in CREATION order. Telegram lists a chat's topics newest-first, so
    the bottom tab is created first: departments in reverse menu order, then
    "📋 Звіти" last → shown as Звіти, Закупівля, Логістика, ... (menu order)."""
    lang = lang if lang in _REPORTS_NAME else "ua"
    depts = [{"key": d["code"],
              "name": " ".join(filter(None, [d.get("emoji"),
                                             d["name"].get(lang) or d["code"]])),
              "icon_color": _ICON_COLORS[i % len(_ICON_COLORS)]}
             for i, d in enumerate(department_topics, start=1)]
    return depts[::-1] + [{"key": REPORTS_KEY, "name": _REPORTS_NAME[lang],
                           "icon_color": _ICON_COLORS[0]}]


def category_to_department(department_topics: list[dict]) -> dict[str, str]:
    """Map every news category code → the department code that owns it."""
    return {code: d["code"] for d in department_topics for code, _ in d["topics"]}


def missing_topics(plan: list[dict], known: dict[str, int]) -> list[dict]:
    """Plan entries (in plan order) that don't have a stored thread id yet."""
    return [p for p in plan if p["key"] not in known]
