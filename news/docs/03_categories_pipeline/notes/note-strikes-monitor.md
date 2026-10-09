# 💥 Обстріли підприємств — strikes monitor

A separate pipeline (not `RSS_FEEDS` / `article_facts`) that tracks strikes on
Ukrainian pharma and adjacent enterprises: plants, warehouses, offices,
distributors, pharmacy chains, logistics centres.

- Pure logic: `app/strikes.py` (watchlist, sources, pre-filter, prompts, card
  formatting). Tests: `tests/test_strikes.py`.
- Orchestration: `main.py`, section "STRIKES MONITOR" (`run_strikes_cycle`,
  `monitor_strikes`, started in `lifespan`).

## Flow (every 5 min)

1. **Collect.** Sources:
   - Google News UA queries (`GOOGLE_NEWS_QUERIES`, including one query per
     batch of watchlist companies);
   - news RSS (`NEWS_RSS`);
   - public Telegram channels via `t.me/s/<channel>` (`TELEGRAM_CHANNELS`:
     oblast administrations, Суспільне, Укрінформ, УП, ДСНС).
2. **Dedup.** Every report is stored in `strike_items`, keyed by link.
   Reports older than 36 h are skipped; on the very first run only reports
   younger than 6 h are processed.
3. **Pre-filter** (`is_candidate`). The report must mention an attack word,
   and also an enterprise word or a watchlist company.
4. **Classify** (gpt-4o-mini). The model returns a `category`. Whether the
   report is shown is decided in code by `is_reportable`, not by the model:
   - shown: `pharma`, `adjacent` and `unnamed` enterprises in Ukraine, plus
     any watchlist company;
   - not shown: named businesses from other sectors, energy, housing, and
     strikes on Russian territory.
5. **Attach to an event.**
   - A watchlist company is matched by its canonical name.
   - Anything else is matched by the LLM against the events of the last 72 h.
6. **Publish.**
   - A new event gets one card, composed from all its reports, in the
     `strikes` topic. The card's `message_id` is stored per chat in
     `strike_messages`.
   - Later reports go through an "is this significant?" LLM check. If yes,
     an update is sent as a reply (`reply_parameters`) to the card.

## Where it shows up

- **Bot topic.** `DEPARTMENT_TOPICS` code `strikes` (a virtual pushable
  category). It is on by default: a one-shot migration (`schema_flags`
  `strikes_default_on`) appends it to users with custom subscriptions.
- **Digest 9:00/14:00.** `DEPARTMENTS` code `strikes`. `_collect_department_facts`
  adds events as `event_type='strike'` facts via `strikes.event_as_fact`.
- **Mini App.** The "💥 Обстріли" widget (registry with company / region /
  period filters) is served by `GET /api/webapp/strikes`.
- **Admin trigger.** `POST /admin/strikes/run` (header `X-Admin-Token`).
  Add `?push=0` to fill the registry without sending to Telegram.

## Tuning

- **Add a company:** append it to `WATCHLIST`. The first alias is used in the
  Google News query, so make it the most specific spelling. Ambiguous names
  (Дарниця is also a Kyiv district) should only get company-specific aliases.
- **Add a Telegram channel:** add it to `TELEGRAM_CHANNELS`, then check that
  `https://t.me/s/<name>` shows posts.
- **Change what is reported:** edit `CATEGORIES` / `REPORTED_CATEGORIES`.
