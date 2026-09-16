"""Keyword pre-filter for broad RSS feeds.

Some sources (gCaptain, Splash247, Hellenic Shipping News, …) are broad maritime
feeds: most items are irrelevant to a Ukrainian raw-materials importer. Running
every item through full-text extraction + LLM summary + fact extraction would
waste API quota and flood subscribers.

This module decides — cheaply, before any AI call — whether an item is worth
keeping, by matching the title+summary against a category's keyword list. Only
categories that have a keyword list are filtered; everything else passes through
unchanged (targeted Google News queries are already pre-filtered by their query).
"""

from __future__ import annotations


def passes_keyword_filter(title: str, summary: str, keywords: list[str]) -> bool:
    """True if the item is relevant (or the category has no keyword list).

    Case-insensitive substring match on the combined title + summary.
    An empty/None keyword list means "no filter" → always True.
    """
    if not keywords:
        return True
    haystack = f"{title or ''} {summary or ''}".lower()
    return any(k.lower() in haystack for k in keywords)
