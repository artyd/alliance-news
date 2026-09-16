# MacroHarvey — REST API, Фронтенд, Залежності

## REST API (FastAPI)

### Основні ендпоінти

| Endpoint | Метод | Опис |
|---|---|---|
| `GET /` | GET | Повертає `index.html` (SPA) |
| `GET /news` | GET | Всі статті (без `INTERNAL_CATEGORIES`), LIMIT 1000, DESC |
| `GET /news/{category}` | GET | Статті за категорією, LIMIT 15 |
| `GET /alerts` | GET | 5 останніх статей з display_time (`"5 mins ago"` / `"HH:MM"`) |
| `GET /generate_report` | GET | Ручний trigger `daily_brief` → повертає PDF файл |
| `GET /generate_weekly` | GET | Ручний trigger `weekly` → повертає PDF файл |
| `GET /generate_midday` | GET | Ручний trigger `midday` → повертає PDF файл |
| `GET /logo.png` | GET | Роздає лого для Mini App |

### Mini App API ендпоінти

| Endpoint | Метод | Опис |
|---|---|---|
| `GET /webapp` | GET | Повертає `_WEBAPP_HTML` як HTMLResponse |
| `GET /api/webapp/news?category=&limit=` | GET | Новини (підтримує comma-separated категорії) |
| `GET /api/webapp/digest_reports?limit=10` | GET | Список звітів (без pdf_data) |
| `GET /api/webapp/digest_reports/{id}/pdf` | GET | Скачати PDF по ID |
| `GET /api/webapp/track?number=&carrier=` | GET | Уніфікований трекінг |
| `POST /api/webapp/track/save` | POST | Зберегти відправлення |
| `GET /api/webapp/track/list?user_id=` | GET | Список відправлень (активні ≤15, архів ≤15) |
| `DELETE /api/webapp/track/remove?user_id=&number=` | DELETE | Видалити відправлення |

**Трекінг логіка:**
- ISO 6346 (4 літери + 7 цифр) → морський контейнер → 17track
- `carrier == "nova"` або Нова Пошта шаблон → НП API
- DHL / EMS / Meest → 17track з кодом перевізника
- Без API ключа → `no_api: true` + `tracking_url`

CORS: `allow_origins=["*"]`.

## Фронтенд (index.html)

Single-Page Application (~50KB), без фреймворків. Ключові фічі:

- **Теми:** Dark / Light mode (localStorage)
- **Мови:** EN / UA / RU (JS i18n-словник)
- **Фільтрація:** кнопки категорій (All + тематичні)
- **Ticker-рядок:** авто-прокрутка заголовків
- **Картки новин:** зображення, заголовок, саммарі, категорія-бейдж, час
- **Design:** фінансовий термінал-стиль (темна тема, монохромна з акцентами)
- **Fetch:** `GET /news` при завантаженні, `GET /alerts` кожні 60 секунд

## Python-залежності

| Пакет | Призначення |
|---|---|
| `fastapi` + `uvicorn` | Web-фреймворк та ASGI-сервер |
| `psycopg2-binary` | PostgreSQL клієнт |
| `python-dotenv` | Читання `.env` |
| `google-generativeai` | Gemini API (fallback саммарі) |
| `openai` | GPT-4o-mini (саммарі, факти, звіт) |
| `feedparser` | RSS парсинг |
| `httpx` | Async HTTP клієнт |
| `trafilatura` | Витяг повного тексту (Stage 1) |
| `googlenewsdecoder` | Офлайн декодування Google News URLs |
| `fpdf2` | Генерація PDF |
| `apscheduler` | Планувальник (cron + interval) |
| `pytz` | Timezone (Kyiv) |
| `yfinance` | Ціни ф'ючерсів (графіки + market alerts) |
| `matplotlib` | TradingView-style charts |
| `numpy` | Gradient fill для matplotlib |
| `beautifulsoup4` | HTML парсинг (резервний) |

### Graceful Fallbacks при старті

| Залежність | Flag | Якщо відсутня |
|---|---|---|
| matplotlib/yfinance | `CHARTS_AVAILABLE` | Графіки вимкнені |
| trafilatura | `TRAFILATURA_AVAILABLE` | Stage 1 пропускається |
| googlenewsdecoder | `GNEWSDECODER_AVAILABLE` | Google URLs не декодуються |
