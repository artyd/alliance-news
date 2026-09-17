"""Unit tests for the pure Telegra.ph node builders and digest message."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.telegraph import (
    plain_body_to_nodes,
    sources_to_nodes,
    build_page_content,
)
from app.telegram_articles import build_digest_message


def test_paragraphs_and_bullets():
    body = "Intro paragraph.\n\n• first point\n• second point\n\nClosing line."
    nodes = plain_body_to_nodes(body)
    tags = [n["tag"] if isinstance(n, dict) else "str" for n in nodes]
    assert tags == ["p", "ul", "p"]
    ul = nodes[1]
    assert len(ul["children"]) == 2
    # marker stripped
    assert ul["children"][0]["children"][0] == "first point"


def test_bullet_markers_variants():
    body = "- dash\n* star\n– endash"
    nodes = plain_body_to_nodes(body)
    assert len(nodes) == 1 and nodes[0]["tag"] == "ul"
    texts = [li["children"][0] for li in nodes[0]["children"]]
    assert texts == ["dash", "star", "endash"]


def test_empty_body():
    assert plain_body_to_nodes("") == []
    assert plain_body_to_nodes(None) == []


def test_sources_dedup_and_link():
    src = [("A", "https://x/1"), ("B", "https://x/2"), ("Dup", "https://x/1"),
           ("NoUrl", "")]
    nodes = sources_to_nodes(src)
    assert nodes[0]["tag"] == "h4"
    ul = nodes[1]
    assert len(ul["children"]) == 2  # dup + empty dropped
    a = ul["children"][0]["children"][0]
    assert a["tag"] == "a" and a["attrs"]["href"] == "https://x/1"


def test_sources_empty_returns_nothing():
    assert sources_to_nodes([]) == []


def test_build_page_content_has_body_hr_sources():
    content = build_page_content("Hello.\n\n• one", [("T", "https://u")],
                                 footer="Alliance News · 01.01.2026")
    tags = [n["tag"] for n in content]
    assert tags[0] == "p"
    assert "hr" in tags and "h4" in tags
    assert tags[-1] == "p"  # footer paragraph


def test_build_page_content_never_empty():
    assert build_page_content("", []) == [{"tag": "p", "children": ["—"]}]


def test_digest_message_and_keyboard():
    items = [
        {"name": "Logistics", "teaser": "Ports congested.", "url": "https://telegra.ph/a"},
        {"name": "Wars", "teaser": "New tariffs.", "url": "https://telegra.ph/b"},
    ]
    text, keyboard = build_digest_message(items, "17.09.2026")
    assert "<b>" in text and "17.09.2026" in text
    assert "Logistics" in text and "Ports congested." in text
    assert len(keyboard) == 2
    assert keyboard[0][0]["url"] == "https://telegra.ph/a"
    assert keyboard[0][0]["text"].startswith("Logistics")


def test_digest_message_skips_button_without_url():
    items = [{"name": "X", "teaser": "t", "url": None}]
    text, keyboard = build_digest_message(items, "01.01.2026")
    assert keyboard == []
