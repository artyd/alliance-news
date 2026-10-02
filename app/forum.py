"""Forum-group routing — pure, testable logic.

A Telegram forum supergroup (FORUM_CHAT_ID) mirrors the bot's output split into
topics ("гілки"):
  * the General topic (always first in the list, can't be moved) is renamed to
    REPORTS_TOPIC_NAME and receives ONLY the daily/midday/weekly reports;
  * one topic per business department receives ONLY that department's live news.

Thread ids are created once via createForumTopic and persisted by main.py;
this module only decides names and which topic a news category belongs to.
"""

from __future__ import annotations

REPORTS_KEY = "reports"
REPORTS_TOPIC_NAME = "📋 Звіти"

# Telegram allows only these six icon colours for createForumTopic.
_ICON_COLORS = [0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F]


def department_topic_plan(department_topics: list[dict], lang: str = "ua") -> list[dict]:
    """One {key, name, icon_color} per department, in menu order."""
    return [
        {"key": d["code"],
         "name": d["name"].get(lang) or d["code"],
         "icon_color": _ICON_COLORS[i % len(_ICON_COLORS)]}
        for i, d in enumerate(department_topics)
    ]


def category_to_department(department_topics: list[dict]) -> dict[str, str]:
    """Map every news category code → the department code that owns it."""
    return {code: d["code"] for d in department_topics for code, _ in d["topics"]}


def missing_topics(plan: list[dict], known: dict[str, int]) -> list[dict]:
    """Plan entries that don't have a stored thread id yet."""
    return [p for p in plan if p["key"] not in known]
