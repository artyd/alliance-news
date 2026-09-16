"""Unit tests for the pure Telegram-article helpers.

Run:  python tests/test_telegram_articles.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.telegram_articles import (
    escape_html,
    build_facts_payload,
    telegram_chunks,
    format_article_html,
    collect_sources,
    TELEGRAM_MSG_LIMIT,
)


def test_escape_html():
    assert escape_html("a < b & c > d") == "a &lt; b &amp; c &gt; d"
    assert escape_html("") == ""


def test_build_facts_payload_caps_and_formats():
    facts = [{"what_happened": f"event {i}", "who": "X", "ukraine_relevance": "high"}
             for i in range(60)]
    payload = build_facts_payload(facts, max_facts=40)
    assert "1. event 0" in payload
    assert "40. event 39" in payload
    assert "event 40" not in payload  # capped at 40


def test_build_facts_payload_skips_empty():
    facts = [{"what_happened": ""}, {"what_happened": "real"}]
    payload = build_facts_payload(facts)
    assert "real" in payload
    assert payload.count("1.") == 1


def test_telegram_chunks_short_passthrough():
    assert telegram_chunks("hi") == ["hi"]
    assert telegram_chunks("") == []
    assert telegram_chunks("   ") == []


def test_telegram_chunks_splits_under_limit():
    text = "\n".join(f"line number {i}" for i in range(2000))
    chunks = telegram_chunks(text)
    assert len(chunks) > 1
    for c in chunks:
        assert len(c) <= TELEGRAM_MSG_LIMIT


def test_telegram_chunks_hard_splits_monster_line():
    text = "x" * 10000
    chunks = telegram_chunks(text)
    assert len(chunks) >= 3
    for c in chunks:
        assert len(c) <= TELEGRAM_MSG_LIMIT


def test_format_article_html_has_header_and_sources():
    html = format_article_html(
        "Фарм API", "• price up 5%", "01.02.2026",
        sources=[("Reuters piece", "https://reuters.com/x")],
    )
    assert "<b>Фарм API</b>" in html
    assert "01.02.2026" in html
    assert "Джерела" in html
    assert 'href="https://reuters.com/x"' in html


def test_format_article_html_escapes_body():
    html = format_article_html("Dept", "a < b & c", "01.02.2026")
    assert "a &lt; b &amp; c" in html


def test_collect_sources_dedup():
    facts = [
        {"title": "t1", "link": "u1"},
        {"title": "t2", "link": "u1"},   # dup url
        {"title": "t3", "link": "u2"},
    ]
    srcs = collect_sources(facts)
    assert srcs == [("t1", "u1"), ("t3", "u2")]


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")
            passed += 1
    print(f"\n{passed} tests passed")
