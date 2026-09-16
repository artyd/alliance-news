# MacroHarvey — База даних (PostgreSQL)

> Всі міграції — через `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (ідемпотентно, без даунтайму).

## Таблиця `articles`

Основна таблиця новин.

| Колонка | Тип | Опис |
|---|---|---|
| `id` | SERIAL PK | Авто-ID |
| `title` | TEXT | Заголовок |
| `link` | TEXT UNIQUE | URL статті (ключ дедублікації) |
| `published` | TEXT | Дата `YYYY-MM-DD HH:MM:SS` (Kyiv TZ) |
| `category` | TEXT | Категорія (api, cosmetic, herbal, ...) |
| `image_url` | TEXT | URL зображення |
| `summary_en/ua/ru` | TEXT | Саммарі трьома мовами |
| `full_text` | TEXT | Повний текст (trafilatura, до 20 000 символів) |
| `extraction_status` | TEXT | `pending` / `ok` / `failed` / `paywalled` / `skipped` |
| `extraction_attempted_at` | TIMESTAMP | Коли остання спроба Stage 1 |
| `final_url` | TEXT | URL після розкодування Google News redirect |
| `facts_status` | TEXT | `pending` / `ok` / `failed` / `skipped` |
| `facts_attempted_at` | TIMESTAMP | Коли остання спроба Stage 2 |

## Таблиця `article_facts`

Структуровані факти (Stage 2), витягнуті GPT-4o-mini.

| Колонка | Тип | Опис |
|---|---|---|
| `id` | SERIAL PK | — |
| `article_id` | INT FK → articles | — |
| `event_type` | TEXT | regulation / tariff / price_move / supply_disruption / corporate / ... |
| `what_happened` | TEXT | Один конкретний факт (subject-verb-object) |
| `who` | TEXT | Актори події |
| `where_loc` | TEXT | Країна/регіон |
| `magnitude` | TEXT | Числовий показник (+15%, $2.3B, ...) |
| `affected_sectors` | TEXT | CSV із кодів секторів (api,logistics,...) |
| `supply_chain_impact` | TEXT | Вплив на ланцюг постачання |
| `ukraine_relevance` | TEXT | `high` / `medium` / `low` / `none` |
| `confidence` | TEXT | `high` / `medium` / `low` |
| `source_url` | TEXT | Фінальний URL |
| `source_publisher` | TEXT | Домен видавця |
| `created_at` | TIMESTAMP | — |

## Таблиця `telegram_users`

| Колонка | Тип | Опис |
|---|---|---|
| `chat_id` | BIGINT PK | — |
| `language` | TEXT | `en` / `ua` / `ru` |
| `subscriptions` | TEXT | `all` або CSV-список категорій |
| `only_daily_mode` | BOOLEAN | TRUE → тільки PDF, без live push |

## Таблиця `telegram_sent`

Журнал надісланих повідомлень (для уникнення дублів).

| Колонка | Тип | Опис |
|---|---|---|
| `id` | SERIAL PK | — |
| `chat_id` | BIGINT | — |
| `article_link` | TEXT | URL статті |
| `sent_at` | TIMESTAMP | — |

## Таблиця `digest_reports`

PDF-звіти, збережені в БД (для Mini App вкладки "Звіти").

```sql
CREATE TABLE IF NOT EXISTS digest_reports (
    id          SERIAL PRIMARY KEY,
    report_type TEXT NOT NULL,   -- daily_brief / midday / weekly
    title       TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    pdf_data    BYTEA
)
```

## Таблиця `tracked_shipments`

Збережені відправлення для вкладки "Трекінг".

```sql
CREATE TABLE IF NOT EXISTS tracked_shipments (
    id           SERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    number       TEXT   NOT NULL,
    carrier      TEXT   DEFAULT 'auto',
    type         TEXT   DEFAULT 'parcel',  -- 'parcel' | 'container'
    carrier_name TEXT   DEFAULT '',
    status_text  TEXT   DEFAULT '',
    tracking_url TEXT   DEFAULT '',
    added_at     TIMESTAMP DEFAULT NOW(),
    last_checked TIMESTAMP DEFAULT NOW(),
    is_delivered BOOLEAN DEFAULT FALSE,
    delivered_at TIMESTAMP,
    UNIQUE (user_id, number)
)
```

## Індекси

```sql
idx_articles_extraction_status    ON articles(extraction_status)
idx_articles_category_published   ON articles(category, published DESC)
idx_articles_facts_status         ON articles(facts_status)
idx_articles_title                ON articles(title)
idx_facts_article_id              ON article_facts(article_id)
idx_facts_sectors                 ON article_facts(affected_sectors)
idx_facts_created                 ON article_facts(created_at DESC)
idx_sent_link                     ON telegram_sent(article_link)
```
