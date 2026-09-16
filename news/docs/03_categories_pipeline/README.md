# MacroHarvey — Категорії та Pipeline обробки

## Категорії новин (RSS-джерела)

Всі джерела — Google News RSS (7 днів, US English).

| Код | Назва | Тип |
|---|---|---|
| `api` | Фармацевтичні субстанції | Публічний |
| `cosmetic` | Косметичні субстанції | Публічний |
| `herbal` | Трави та рослинна сировина | Публічний |
| `veterinary` | Ветеринарні субстанції | Публічний |
| `food` | Харчова сировина | Публічний |
| `feed` | Кормові амінокислоти | Публічний |
| `capsules` | Капсули | Публічний |
| `pvc` | ПВХ-плівка та пакування | Публічний |
| `logistics` | Логістика та постачання | Публічний |
| `global_sources` | Глобальна економіка та торгівля | Публічний (tier-1 + site: filter) |
| `middle_east` | Близький Схід | **INTERNAL** (тільки для звіту, не в Telegram) |
| `good_news` | Позитивні новини | Публічний (Telegram, не в звіті) |
| `market_alerts` | Ринкові алерти (yfinance) | Virtual (без RSS, генеруються локально) |

**`INTERNAL_CATEGORIES`** = `{"middle_east"}` — зберігаються у БД для звіту, але **не** в Telegram-підписці та API.

**`NON_REPORT_CATEGORIES`** = `{"market_alerts", "good_news"}` — видимі в Telegram, але не в PDF-звіт і не в facts pipeline.

## B2B RSS-видання (оновлено Сесія 20)

| Категорія | Видання |
|---|---|
| `cosmetic` | cosmeticsdesign.com, cosmeticsbusiness.com |
| `herbal` | nutraceuticalsworld.com, herbalgram.org |
| `food` | foodingredientsfirst.com, foodnavigator.com |
| `feed` | feednavigator.com, worldgrain.com |
| `logistics` | theloadstar.com, freightwaves.com (`when:3d`) |
| `api` | pharmiweb.com, fiercepharma.com |

## Pipeline обробки статей

```
RSS Feed (кожні 15 хв)
    │
    ▼
feedparser → новий заголовок/URL?
    │  Подвійна перевірка: UNIQUE link + UNIQUE title
    │
    ▼
GPT-4o-mini → summary_en / summary_ua / summary_ru (40-50 слів, B2B)
    │  (fallback → Gemini 2.5 Flash якщо OpenAI недоступний)
    │
    ▼
INSERT INTO articles (extraction_status='pending')
    │
    ├──► Telegram push → підписники (за мовою, підписками, only_daily_mode)
    │
    ▼
[STAGE 1] extract_and_store()
    ├── _resolve_google_news_url() — googlenewsdecoder (офлайн декодування base64)
    ├── HTTP GET з Chrome UA (httpx, timeout 15s)
    ├── trafilatura.extract() → full_text (до 20 000 символів)
    ├── _detect_paywall() → перевірка пейволу
    └── UPDATE articles SET full_text, extraction_status, final_url
              │
              ▼
        [STAGE 2] extract_facts_and_store()  (fire-and-forget)
            ├── gpt-4o-mini → JSON {"facts": [...]}  (до 4 фактів)
            ├── Валідація секторів (тільки з _FACT_SECTOR_CODES)
            └── INSERT INTO article_facts
```

## Паралелізм та Rate Limiting

| Параметр | Значення |
|---|---|
| Stage 1 семафор | `asyncio.Semaphore(5)` — 5 паралельних HTTP-запитів |
| Stage 2 семафор | `asyncio.Semaphore(3)` — 3 паралельних OpenAI-запити |
| Таймаут витягу | 15 секунд на статтю |
| Мінімум тексту | 300 символів (нижче → `failed`); Stage 2 — 200 символів |
| Backfill вікно | Статті за **останні 7 днів** зі статусом `pending` |
| Макс. backfill | 150 статей Stage 1 / 100 статей Stage 2 за запуск |
| Таймаут batch | 180 секунд на всі tasks за один цикл fetch |

## Paywall Detection

Евристика: текст коротший за 800 символів **ТА** HTML містить один із маркерів:
`"subscribe to read"`, `"sign in to read"`, `"this article is for subscribers"` та ін.

## Google News URL Decoding

Google News RSS містить base64-кодовані URLs. З EU IP (Hetzner, Німеччина) HTTP-редирект блокується `consent.google.com`.

**Рішення:** `googlenewsdecoder` — офлайн декодування через `asyncio.to_thread()` без мережевих запитів.

## Промпти для саммарі (`CAT_SYSTEM_PROMPTS`)

Словник з 12 окремими промптами — по одному на кожну категорію. Структура промпту (4–5 речень):
1. Подія — що відбулось і де
2. Причина — чому це сталось
3. Глобальний ринковий вплив
4. Вплив на українського B2B-імпортера
5. Рекомендована дія

Категорія `good_news` — cheerful/uplifting промпт без B2B-контексту.
