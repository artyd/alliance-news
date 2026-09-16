# MacroHarvey — Запуск і Архітектура

## Запуск проекту

```bash
# Встановлення залежностей
pip install -r requirements.txt

# Dev
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Prod
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Lifespan-послідовність (при старті FastAPI)

`@asynccontextmanager lifespan`:

1. `init_db()` — створює/мігрує таблиці (ідемпотентно), скидає `failed` → `pending` за 7 днів
2. `fetch_and_store_news()` — фоновий task loop, кожні 15 хв
3. `poll_telegram_updates()` — long-poll Telegram loop
4. `cleanup_old_news()` — loop, щодня (видалення > 30 днів)
5. `backfill_missing_full_text(150)` — Stage 1 backfill (fire-and-forget)
6. `backfill_missing_facts(100)` — Stage 2 backfill (fire-and-forget)
7. `monitor_market_alerts()` — ринковий моніторинг loop, кожні 30 хв
8. APScheduler — всі cron-задачі (Kyiv TZ)

## Деплой на сервер

```bash
# З Windows — копіюємо файли на Hetzner
scp C:/Users/Артем/Desktop/news/main.py root@178.104.96.245:/path/to/app/
scp C:/Users/Артем/Desktop/news/.env    root@178.104.96.245:/path/to/app/
scp C:/Users/Артем/Desktop/news/logo.png root@178.104.96.245:/path/to/app/

# На сервері — рестарт
systemctl restart your-app-service
# або якщо вручну:
pkill -f "uvicorn main:app"
uvicorn main:app --host 0.0.0.0 --port 8000 &
```

## Ключові константи в коді (приблизні рядки)

| Константа | Рядок | Опис |
|---|---|---|
| `CAT_SYSTEM_PROMPTS` | ~200 | Промпти GPT по кожній категорії |
| `RSS_FEEDS` | ~350 | RSS-стрічки по категоріях |
| `CHART_TICKERS` | ~500 | 10 ринкових інструментів |
| `INTERNAL_CATEGORIES` | ~600 | Категорії що не показуються в UI |
| `NON_REPORT_CATEGORIES` | ~610 | Категорії без звітів |
| `WEBAPP_URL` | ~4808 | URL Mini App |
| `_WEBAPP_HTML` | ~4862 | Весь HTML Mini App (raw string) |

## HTTPS налаштування (Hetzner + Caddy)

```
IP: 178.104.96.245
DNS: 178-104-96-245.sslip.io (wildcard, без купівлі домену)
SSL: Let's Encrypt (автоматично Caddy)
```

```
# /etc/caddy/Caddyfile
178-104-96-245.sslip.io {
    reverse_proxy localhost:8000
}
```

## Ключові архітектурні рішення

### PostgreSQL замість SQLite
Стартував з SQLite, мігрував на **Supabase PostgreSQL** для продакшн-деплою. `database.py` — легасі, не використовується.

### Two-Stage Pipeline
- **Stage 1** (trafilatura): замінює RSS-сніпети на реальний текст
- **Stage 2** (gpt-4o-mini): витягує атомарні JSON-факти з full_text
- **Синтез** (gpt-4o): будує звіт виключно з фактів → усуває "cross-category fact bleeding"
- **Fallback**: якщо facts порожні → LLM читає full_text/RSS

### Google News URL Decoding
З EU IP HTTP-редирект блокується `consent.google.com`. Рішення: **`googlenewsdecoder`** — офлайн через `asyncio.to_thread()`.

### Facts-First з multi-day fallback
- Primary window: вчора (`daily_brief`) / тиждень (`weekly`) / сьогодні до зараз (`midday`)
- Якщо фактів < 15 → multi-day fallback (3 дні) — тільки для `daily_brief`
- `midday` та `weekly` — explicit window, без fallback

### OpenAI Rate Limit Protection (30k TPM)
- Block 1: `batch_size=2` (≤2 категорії за запит), `asyncio.sleep(65)` між батчами
- Block 2: окремий isolated API call + свій sleep(65) перед ним
- Ліміт: 7 фактів/категорія, 10 для Близького Сходу

### Market Alerts Architecture
Окремий asyncio loop, незалежний від fetch-циклу. Дедублікація через `telegram_sent`. `link='alert://<ticker>'` — псевдо-URL.

### Mini App Self-Contained
Весь HTML Mini App у `_WEBAPP_HTML` (Python raw string у `main.py`). Ніяких зовнішніх файлів → нема проблем з деплоєм на сервері.
