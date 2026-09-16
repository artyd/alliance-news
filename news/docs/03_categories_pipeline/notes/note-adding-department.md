# How to add a new department / parsing source

## Two layers

1. **Sectors** (`REPORT_CATEGORIES` / `RSS_FEEDS`) — fine-grained topics with
   their own news feed and fact extraction. These drive the PDF report.
2. **Departments** (`DEPARTMENTS`) — the 5 business groupings that each get one
   **Telegram article**. A department collects facts by matching either a
   sector OR an `event_type` (both are already on every fact).

Current departments: `procurement` (закупівля — all raw-material sectors),
`logistics`, `world` (весь світ), `wars` (війни — geopolitical), `laws`
(закони — regulation/sanction/tariff). The morning (09:00) and midday (14:00)
schedulers now send these Telegram articles; the PDF is manual-only.

### Adding a department

Append to `DEPARTMENTS` in `main.py`:
```python
{"code": "finance", "name": "Фінанси та валюта",
 "sectors": [], "event_types": ["investment"]},   # sector OR event_type match
```
Facts flow in automatically — no extractor change needed if you route by an
existing `event_type` (regulation/sanction/tariff/geopolitical/price_move/
supply_disruption/market_trend/investment/corporate/other).

### Adding a parsing source (feed)

Everything below is about the sector layer that feeds the departments.

## 1. Add the news source (`RSS_FEEDS`)

Use the `google_news_rss()` helper so you don't hand-encode URLs:

```python
RSS_FEEDS["excipients"] = google_news_rss(
    phrases=[
        "pharmaceutical excipients",
        "excipient shortage",
        "microcrystalline cellulose price",
    ],
    sites=["pharmaexcipients.com"],   # optional site: filters
    days=5,                            # freshness window
)
```

- Use **specific** phrases. Broad single words pull noise (see the `api`
  category note: `"API price"` was removed because it matched OpenAI pricing).
- Prefer 3–6 phrases OR-ed together plus 1–2 trade-press `sites`.

## 2. Make it a report department (`REPORT_CATEGORIES`)

Add a `(code, "Ukrainian display name")` tuple. The code MUST match the
`RSS_FEEDS` key. This name is the department title in the PDF **and** the
Telegram article header.

```python
REPORT_CATEGORIES = [
    ...
    ("excipients", "Допоміжні речовини (ексципієнти)"),
]
```

## 3. (Optional) Tune the AI summary prompt (`CAT_SYSTEM_PROMPTS`)

If the default B2B prompt isn't specific enough for the topic, add a
per-category system prompt:

```python
CAT_SYSTEM_PROMPTS["excipients"] = "You are an analyst covering pharma excipients ..."
```

Otherwise `_DEFAULT_SYSTEM_PROMPT` is used.

## Category visibility flags

- `INTERNAL_CATEGORIES` — fetched into the DB and used in reports, but NOT
  offered to Telegram users as a subscription option (e.g. `middle_east`).
- `NON_REPORT_CATEGORIES` — user-visible pushes that skip the report / fact
  pipeline (e.g. `good_news`, `market_alerts`).

A normal new department goes in **neither** set: it is user-visible AND feeds
the report/articles.

## What happens automatically after that

- `fetch_and_store_news()` iterates `RSS_FEEDS`, so the new feed is polled.
- Full-text + fact extraction run for it (it's not in `NON_REPORT_CATEGORIES`).
- `fetch_facts_for_report()` buckets its facts under the new code.
- `generate_department_articles()` produces a Telegram article for it.
- The PDF report renders it as a Block-1 section.

No other code changes required.

## Telegram articles (PDF → messages)

Preview without sending (admin token required):

```
GET /generate_telegram_articles?preview=1&mode=daily_brief&token=YOUR_ADMIN_TOKEN
GET /generate_telegram_articles?preview=1&dept=excipients&token=...   # one department
```

Send to all subscribers:

```
GET /generate_telegram_articles?mode=daily_brief&token=YOUR_ADMIN_TOKEN
```

`mode` = `daily_brief` (yesterday) | `midday` (today so far) | `weekly` (7 days).
This is additive — the PDF report still runs. To schedule the articles instead
of (or alongside) the PDF, add a job in the `lifespan` scheduler that calls
`send_department_articles_to_users(mode=...)`.
