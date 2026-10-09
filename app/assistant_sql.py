"""Read-only SQL for the AI assistant — validation only (pure, testable).

The assistant may run its own SELECT over the bot's business data. This module
decides whether a query is acceptable: one SELECT/WITH statement, touching only
whitelisted tables, no comments, no data-changing keywords. Execution (inside
a READ ONLY transaction with a statement timeout and a row cap) is in main.py.
"""

from __future__ import annotations

import re

# Business data the assistant may read. Personal data (notes, favorites,
# follows, subscriptions, user prefs, personal parcels) is deliberately absent.
ALLOWED_TABLES = {
    "articles": "id, title, title_ua, link, published (TEXT 'YYYY-MM-DD HH:MM:SS' Kyiv), category, summary_ua, summary_en, full_text",
    "article_facts": "id, article_id → articles.id, event_type, what_happened, who, where_loc, magnitude, affected_sectors (CSV), supply_chain_impact, ukraine_relevance, created_at",
    "strike_events": "id, hidden (TRUE = out of scope, ignore), sent_at, attack_date, company, object_type, city, region, is_pharma, watchlist, headline, summary, card_json (JSON text), update_count",
    "strike_items": "id, link, source, title, text, published, status, event_id → strike_events.id",
    "corp_shipments": "key, data_json (JSON text: product, agent, qty, line, container, number, mode, eta, departed, origin, dest, comment, customs, warehouse, stage, sheet_no), number, in_sheet, live_status, live_carrier, last_checked",
    "digest_issues": "id, mode, date_str, items_json (JSON text), created_at",
}
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum|analyze|"
    r"call|do|execute|prepare|listen|notify|lock|set|reset|comment|security|pg_sleep|"
    r"pg_read_file|pg_ls_dir|lo_import|lo_export|dblink|into)\b", re.I)
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.I)
_CTE_NAMES = re.compile(r"(?:\bwith\b|,)\s*([a-zA-Z_]\w*)\s+as\s*\(", re.I)


def check_sql(sql: str) -> str | None:
    """None if the query is acceptable, otherwise the reason (shown to the model)."""
    q = (sql or "").strip().rstrip(";").strip()
    if not q:
        return "empty query"
    if ";" in q:
        return "only one statement is allowed"
    if "--" in q or "/*" in q:
        return "comments are not allowed"
    if not re.match(r"(?is)^\s*(select|with)\b", q):
        return "only SELECT / WITH queries are allowed"
    if _FORBIDDEN.search(q):
        return "the query contains a forbidden keyword"
    ctes = {m.lower() for m in _CTE_NAMES.findall(q)}
    for t in _TABLE_REF.findall(q):
        name = t.lower().split(".")[-1]
        if "." in t and not t.lower().startswith("public."):
            return f"schema-qualified table '{t}' is not allowed"
        if name not in ALLOWED_TABLES and name not in ctes:
            return f"table '{name}' is not available; allowed: {', '.join(sorted(ALLOWED_TABLES))}"
    return None


def schema_hint() -> str:
    return "\n".join(f"- {t}({cols})" for t, cols in ALLOWED_TABLES.items())
