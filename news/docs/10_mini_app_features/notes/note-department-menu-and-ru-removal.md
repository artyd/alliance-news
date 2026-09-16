# Department subscription menu, law sources, Russian removal

## Department subscription menu (live feed)

Telegram users now pick which news they receive through a **paginated
department menu** (`/start` → language → menu, or the "menu_topics" button):

- One department shown at a time; ◀ ▶ page between the 5 departments
  (Закупівля, Логістика, Світ, Війни, Закони).
- Each topic has a checkbox (✅ / ☐); tap to toggle. "Увесь відділ" toggles a
  whole department. "Усі теми" subscribes to everything; "Готово" closes.
- Selection is saved per user in `telegram_users.subscriptions` (CSV of codes,
  or sentinels `all` / `none`) and filters the live per-article push.

Config lives in `main.py` → `DEPARTMENT_TOPICS`; all selection math is in the
pure, unit-tested `app/subscriptions.py` (see `tests/test_subscriptions.py`).

This is separate from `DEPARTMENTS` (the daily digest article grouping).
`DEPARTMENT_TOPICS` = the live-feed menu; `DEPARTMENTS` = the digest synthesis.

### Adding a topic to the menu
Append `(code, {"ua": "...", "en": "..."})` to the relevant department's
`topics` list. The `code` must be a pushable category (in `RSS_FEEDS` or a
virtual one like `market_alerts`) and NOT in `INTERNAL_CATEGORIES`.

## Law sources (Закони department)

- `apteka` — real RSS: `https://www.apteka.ua/category/rss`.
- `dls` — Держлікслужба has no RSS; Google News restricted to `dls.gov.ua` (uk).
- `kmu` — Кабінет Міністрів НПА; Google News restricted to `kmu.gov.ua` (uk).

`dls`/`kmu` are a stopgap until dedicated HTML scrapers are written (the sites
are not RSS). All three are normal pushable categories.

`INTERNAL_CATEGORIES` is now empty — every category (incl. wars/laws) is
user-selectable and pushed to its subscribers.

## Russian removed (UA + EN only)

- `generate_summary` no longer asks the model for `summary_ru` / `title_ru`
  (token saving). The legacy `*_ru` DB columns remain and are mirrored from the
  UA text so inserts and any residual reader keep working — no schema change.
- Language pickers (bot `/start`, `menu_lang`, and the Mini App) dropped the RU
  option; existing `ru` users are migrated to `ua` in `init_db()`.
