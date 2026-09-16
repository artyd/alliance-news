# MacroHarvey — Market Alerts + Telegram-бот

## Market Alerts (yfinance ±7%)

Функція `monitor_market_alerts()` — окремий asyncio loop.

| Параметр | Значення |
|---|---|
| Перевірка | Кожні 30 хвилин у торгові години (08:00–23:00 Kyiv) |
| Тригер | ±7% intraday зміна відстежуваного тикера |
| Дедублікація | Один алерт на тикер/напрямок/день (через `telegram_sent`) |
| Зберігання | `articles` з `category='market_alerts'`, `link='alert://<ticker>'` (псевдо-URL) |
| Розсилка | Telegram всім підписникам (якщо `market_alerts` в підписках) |
| LLM | GPT-4o-mini генерує коротке пояснення причини руху |

## Telegram-бот — Команди

| Команда | mode | UX-повідомлення |
|---|---|---|
| `/start` | — | Реєстрація + inline вибір мови |
| `/settings` або `/menu` | — | Меню налаштувань |
| `/generate_report` | `daily_brief` | ⏳ Генерую щоденний звіт (Блок 2 + Блок 3)... |
| `/generate_weekly` | `weekly` | ⏳ Генерую тижневий звіт (Блок 1 + 2 + 3, 7 днів)... Це займає 1-2 хвилини. |
| `/generate_middle` | `midday` | ⏳ Генерую полуденне оновлення... |
| `/app` | — | Кнопка відкриття Mini App |

Всі `/generate_*` команди: 1) надсилають ⏳ одразу, 2) генерують PDF, 3) надсилають + пінують, 4) при помилці — ❌.

## Inline Keyboard Flow

1. **Вибір мови** → `lang_ru` / `lang_ua` / `lang_en` → `telegram_users.language`
2. **Вибір тем** → `topic_<category>` (toggle on/off) або `topic_all`
3. **Daily-Only режим** → `toggle_daily_mode` — вимикає live push, лише PDF

## Розсилка новин (`fetch_and_store_news`)

- `INTERNAL_CATEGORIES` (`middle_east`) — **НЕ** надсилаються
- `only_daily_mode=True` — **НЕ** отримують live push
- Підписки: `subs == 'all'` або `category in subs.split(',')`
- Дублі: `telegram_sent` (chat_id, article_link) → UNIQUE constraint
- Admin list: `TELEGRAM_CHAT_ID` (comma-separated) → завжди UA summary
- DB users: мова відповідно до налаштувань

## Формат Telegram-повідомлення

```
💊 Фармацевтичні субстанції  #api #фарм
━━━━━━━━━━━━━━━━━━━━━━━━

📰 <b>Заголовок статті</b>

✍️ <i>Саммарі 40-50 слів...</i>

🔗 <a href="...">Читати повністю</a>
```

## Mini App — Кнопка у Telegram

- `WEBAPP_URL` з `.env` → кнопка `web_app` у `/start`, `/menu`, `/settings`, `/app`
- При старті → `setChatMenuButton` → кнопка "📱 Додаток" у рядку вводу Telegram
- Mini App self-contained у `_WEBAPP_HTML` (Python raw string у `main.py`)
