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
- `dls` — Держлікслужба (`dls.gov.ua/for_subject/`) — HTML-scraped (no RSS).
- `kmu` — Кабінет Міністрів НПА (`kmu.gov.ua/npasearch`) — HTML-scraped.

`dls`/`kmu` now use real BeautifulSoup scrapers in `app/scrapers.py`, wired via
`CUSTOM_SCRAPERS` in `main.py`: fetch_and_store_news calls the scraper (which
returns a feedparser-like object) instead of feedparser for those categories.
Scrapers fail soft (return no entries) so a markup change never crashes the
news loop — but selectors may then need a tweak (see tests/test_scrapers.py).
All three are normal pushable categories.

## Language toggle in the menu

The department keyboard has a language button (🇬🇧 English / 🇺🇦 Українська)
that flips `telegram_users.language` between ua/en and re-renders the menu in
place (callback `dlang:<idx>`).

`INTERNAL_CATEGORIES` is now empty — every category (incl. wars/laws) is
user-selectable and pushed to its subscribers.

## Russian removed (UA + EN only)

- `generate_summary` no longer asks the model for `summary_ru` / `title_ru`
  (token saving). The legacy `*_ru` DB columns remain and are mirrored from the
  UA text so inserts and any residual reader keep working — no schema change.
- Language pickers (bot `/start`, `menu_lang`, and the Mini App) dropped the RU
  option; existing `ru` users are migrated to `ua` in `init_db()`.
