# Project structure

```
news/                         repo root
├── main.py                   FastAPI app, Telegram bot, scheduler, all routes
├── webapp.html               Telegram Mini App (was a 3,200-line string inside main.py)
├── index.html                public landing page
├── logo.png                  served at /logo.png, used in PDF headers
├── requirements.txt          pinned dependencies
├── .env / .env.example       secrets (real .env is gitignored)
│
├── app/                      extracted, testable helper modules
│   ├── __init__.py
│   ├── security.py           Telegram initData HMAC verification
│   ├── telegram_articles.py  pure helpers for PDF->per-department Telegram articles
│   ├── subscriptions.py      department-paginated topic subscription menu logic
│   └── scrapers.py           HTML scrapers for gov sources w/o RSS (dls, kmu)
│
├── assets/
│   └── fonts/DejaVuSans.ttf  bundled PDF font fallback
│
├── migrations/
│   ├── schema.sql            canonical DB schema (reference / manual bootstrap)
│   └── README.md             migration strategy (path to Alembic)
│
├── tests/
│   └── test_security.py      unit tests for initData verification
│
├── deploy/
│   ├── Caddyfile             reverse proxy + auto TLS for the sslip.io host
│   ├── macroharvey.service   systemd unit
│   ├── deploy.sh             pull + install + restart
│   └── README.md             full deployment guide
│
├── .github/workflows/
│   └── deploy.yml            autodeploy on push to main (SSH)
│
├── news/docs/                extensive project documentation (BUG_LOG, sessions, ...)
└── archive/                  historical/dead files (old .bak, legacy database.py)
```

## Next: splitting `main.py`

`main.py` is still ~7,000 lines. It stays as one file for now because it wires
together shared module-level state (the FastAPI `app`, the httpx client, dozens
of config constants and prompt strings) and cannot be safely split without
running the app to verify imports — which needs the live Postgres + API keys on
the server.

Recommended split (do it on the server / with the app runnable, one module at a
time, verifying after each):

| New module            | Moves out of main.py                                    |
|-----------------------|--------------------------------------------------------|
| `app/config.py`       | env vars, `CAT_SYSTEM_PROMPTS`, tickers, currency meta  |
| `app/db.py`           | `get_db_connection`, `init_db`, `db_fetchone/all`       |
| `app/news_pipeline.py`| fetch/extract/summary/facts functions                   |
| `app/reports.py`      | PDF builders (`make_pdf_base`, `generate_daily_pdf_...`)|
| `app/charts.py`       | matplotlib/yfinance chart helpers                       |
| `app/telegram_bot.py` | `poll_telegram_updates` + handlers, send helpers        |
| `app/tracking.py`     | Nova Poshta / 17track functions                         |
| `app/api/webapp.py`   | `/api/webapp/*` routes (an `APIRouter`)                 |

`main.py` then shrinks to app creation, lifespan wiring, and router includes.
