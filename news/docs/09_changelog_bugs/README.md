# MacroHarvey — Хронологія сесій + Відомі баги

## Хронологія розробки

| Сесія | Що зроблено |
|---|---|
| Старт | Базовий FastAPI, SQLite, feedparser, HTML-фронтенд |
| Сесія 2 | Категорії новин, фільтрація на фронтенді |
| Сесія 3 | Telegram-бот, push-розсилка новин |
| Сесія 4 | Міграція SQLite → PostgreSQL (Supabase) |
| Сесія 5 | Gemini AI саммарі, i18n (EN/UA/RU) |
| Сесія 6 | Ticker-рядок, Dark/Light mode, фінансовий дизайн |
| Сесія 7 | Stage 1: trafilatura + googlenewsdecoder |
| Сесія 8 | Stage 2: GPT-4o-mini facts extraction, `article_facts` таблиця |
| Сесія 9 | PDF-звіт (fpdf2), APScheduler, yfinance графіки |
| Сесія 10 | Блок 2 (Близький Схід), 3 режими звіту, `DAILY_REPORT_SYSTEM_PROMPT` |
| Сесія 11 | Market alerts (yfinance ±7%), `monitor_market_alerts()` |
| Сесія 12 | Stability: multi-user `TELEGRAM_CHAT_ID`, Gemini timeout 120s, PostgreSQL |
| Сесія 13 | Дедублікація URL+title, ISO-8601 дати, unfiltered API |
| Сесія 14 | PDF redesign: white/dark palette, TradingView charts, markdown parser |
| Сесія 15 | `good_news` категорія, `NON_REPORT_CATEGORIES`, `only_daily_mode` |
| Сесія 16 | `fetch_facts_for_report()` multi-day fallback, Middle East keyword matching |
| **Сесія 17** | Bug fixes: `_memo_b2_structure` NameError, scheduler `day_of_week='mon-fri'`, активовано `/generate_weekly` та `/generate_middle` з UX-feedback |
| **Сесія 18** | Block 2 overhaul: безумовний fetch `_me_raw_rows`, комбінований контекст (факти + повні тексти), 4 правила anti-hallucination |
| **Сесія 19** | API Rate Limit Fix: chunking (batch_size=2), sleep(65), ліміт 7 фактів/категорія, видалено нумерацію FACT X |
| **Сесія 20** | Telegram Mini App v1: splash + 3 вкладки + чорно-білий дизайн. `CAT_SYSTEM_PROMPTS`. 10 ринкових інструментів. `digest_reports` таблиця. Вкладка Ринки: lazy графіки + деталь-вид. |
| **Сесія 21** | Вкладка Трекінг: `tracked_shipments` таблиця, 4 нових API ендпоінти, Нова Пошта + 17track |

---

## Bug #1 — `NameError: _memo_b2_structure` (Сесія 17, критичний)

**Симптом:** Тижневий звіт щоп'ятниці не генерувався, тихо падав без повідомлення.

**Причина:** У гілці `weekly` + facts-path використовувалась змінна `_memo_b2_structure`, яка **ніде не була визначена**. Python генерував `NameError`, asyncio-task завершувався тихо.

**Виправлення:** Визначено локально перед використанням. Вміст — повна 9-секційна структура memo з «Ключові події тижня».

---

## Bug #2 — Scheduler спрацьовував у вихідні (Сесія 17)

**Симптом:** `send_daily_report_to_users` і `send_midday_report_to_users` могли запускатись у суботу/неділю.

**Причина:** `scheduler.add_job(..., 'cron', hour=9, minute=0)` без `day_of_week` — APScheduler за замовчуванням = щодня.

**Виправлення:** Додано `day_of_week='mon-fri'` до обох cron-задач.

---

## Bug #3 — `/generate_weekly` та `/generate_middle` не працювали (Сесія 17)

**Симптом:** Команди існували в задумі, але не були зареєстровані в `poll_telegram_updates()`.

**Виправлення:** Обидва обробники додані у блок `elif text.startswith(...)`. Кожен: 1) надсилає ⏳, 2) генерує PDF, 3) надсилає+пінує, 4) при помилці → ❌.

---

## Bug #4 — Block 2 галюцинації (Сесія 18)

**Симптом:** Блок 2 щодня генерував **однаковий шаблонний текст**, не прив'язаний до реальних подій.

**Причина:** LLM отримував тільки атомарні факти Stage 2 і спирався на загальні знання.

**Виправлення:**
1. Додано безумовний fetch `_me_raw_rows` (SQL `SELECT ... WHERE category = 'middle_east'`, LIMIT 25, full_text або summary_en)
2. Контекст = Секція А (факти Stage 2) + Секція Б (повні тексти статей)
3. 4 правила `_b2_hard_rules`: тільки факти зі статей, нуль галюцинацій, глибокий аналіз України, достатній обсяг
4. Аналогічні 4 правила для fallback path

---

## Bug #5 — OpenAI 429 Rate Limit при тижневому звіті (Сесія 19)

**Симптом:** ~89 000 токенів в одному запиті → `429 Too Many Requests` (OpenAI ліміт 30k TPM).

**Виправлення:**
- `batch_size=2` — 2 категорії за запит (~12–18k токенів)
- `await asyncio.sleep(65)` між батчами
- Block 2 — окремий API call + свій sleep(65)
- Ліміт: 7 фактів/категорія, 10 для Middle East
- Видалено `FACT {idx}` нумерацію → LLM генерує зв'язний текст, не список

---

## Bug #6 — "Webapp not found" на сервері (Сесія 20)

**Причина:** `FileResponse("webapp.html")` — файл існував локально на Windows, але не на Hetzner.

**Виправлення:** Весь HTML вбудовано у `_WEBAPP_HTML` (raw string у `main.py`), ендпоінт → `HTMLResponse(content=_WEBAPP_HTML)`.

---

## Bug #7 — Splash зависав (Сесія 20)

**Причина:** `window.addEventListener('load', ...)` чекає **всіх** зовнішніх ресурсів (Chart.js CDN).

**Виправлення:** `setTimeout(hideSplash, 1200)` запускається одразу при парсингу скрипта, незалежно від CDN.

---

## Bug #8 — `Request` відсутній у FastAPI import (Сесія 21)

**Причина:** `from fastapi import FastAPI, HTTPException` — `Request` не був імпортований, Pyrefly давав `unknown-name`.

**Виправлення:** `from fastapi import FastAPI, HTTPException, Request`
