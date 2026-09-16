# MacroHarvey — Навігатор документації

> Головний індекс. Замінює монолітний PROJECT_OVERVIEW.md (~1500 рядків).

## Файли документації

| Файл | Що всередині |
|---|---|
| [01_intro.md](01_intro.md) | Що таке проект, структура файлів, змінні .env |
| [02_database.md](02_database.md) | Таблиці БД, схеми, індекси |
| [03_categories_pipeline.md](03_categories_pipeline.md) | RSS-категорії, Pipeline Stage 1 + Stage 2, paywall, семафори |
| [04_scheduler.md](04_scheduler.md) | Розклад APScheduler, всі cron-задачі, логіка вибору режиму |
| [05_pdf_reports.md](05_pdf_reports.md) | PDF-звіт: 3 режими, 3 блоки, рендеринг, facts-first + fallback |
| [06_market_telegram.md](06_market_telegram.md) | Market alerts (yfinance ±7%), Telegram-бот (команди, UX, розсилка) |
| [07_api_frontend_deps.md](07_api_frontend_deps.md) | REST API ендпоінти, фронтенд index.html, Python-залежності |
| [08_startup_architecture.md](08_startup_architecture.md) | Запуск проекту, lifespan-послідовність, ключові архітектурні рішення |
| [09_changelog_bugs.md](09_changelog_bugs.md) | Хронологія сесій 1–19, відомі баги та виправлення |
| [10_mini_app_features.md](10_mini_app_features.md) | Mini App: сесії 20–21, вкладки, нові таблиці БД, ендпоінти |
| [11_design_freeze.md](11_design_freeze.md) | DESIGN FREEZE — CSS-еталон Mini App (незмінний) |
| [SESSION_LOG.md](SESSION_LOG.md) | Журнал сесій: що зроблено, які баги виправлено |

## Швидкий пошук

- **Нова функція?** → спочатку `03`, `04`, `05` — щоб не зламати pipeline і scheduler
- **Баг у звіті?** → `05_pdf_reports.md` + `09_changelog_bugs.md`
- **Telegram-бот?** → `06_market_telegram.md`
- **DB міграція?** → `02_database.md`
- **Mini App CSS?** → `11_design_freeze.md` (ТІЛЬКИ там — не змінювати без команди власника)
- **Що робилось раніше?** → `SESSION_LOG.md` → `09_changelog_bugs.md`
