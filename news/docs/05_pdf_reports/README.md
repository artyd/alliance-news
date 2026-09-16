# MacroHarvey — PDF-звіти

## 3 режими генерації

| Режим | Коли | Що включає | Вікно даних |
|---|---|---|---|
| `daily_brief` | Пн–Чт 09:00 | Block 2 + Block 3 | Вчора 00:00–23:59 Kyiv |
| `weekly` | Пт 09:00 | Block 1 + Block 2 + Block 3 | 7 днів (Пт попер. тижня–Чт вчора) |
| `midday` | Пн–Пт 14:00 | Block 2 + Block 3 | Сьогодні 00:00–зараз |

`generate_daily_pdf_report(mode)` — main entrypoint. Legacy alias: `mode="daily"` → `"daily_brief"`.

## Блок 1 (тільки `weekly`)

**Огляд за 10 категоріями.** GPT-4o синтезує `article_facts` (Stage 2) в аналітичний огляд.

Формат на категорію: **Тренд** (↑/↓/→) → Що сталося → Вплив на глобальний ринок → Вплив на нашу компанію → Прогноз 2–3 тижні.

**Rate limit захист (OpenAI 30k TPM):**
- `batch_size=2` — максимум 2 категорії за один API-запит (~12–18k токенів)
- `await asyncio.sleep(65)` між батчами (скидання 60-секундного вікна)
- Ліміт фактів: 7 на категорію (High→Low), для Близького Сходу — 10
- Пропущені факти: `(+ ще X фактів...)`

## Блок 2 (Близький Схід, ~500–700 слів)

Аналізує факти `middle_east` + keyword-matches з інших категорій.

**Структура:** Заголовок → Короткий висновок → Що сталося → Вплив на нафту → Вплив на логістику → Вплив на укр. фармкомпанію (мін. 4–5 речень) → Практичні рекомендації → Прогноз → Джерела.

**Контекст для LLM (anti-hallucination):**
```
Секція А: СТРУКТУРОВАНІ ФАКТИ (Stage 2 pipeline)
Секція Б: ПОВНІ ТЕКСТИ СТАТЕЙ (middle_east, з БД, LIMIT 25)
```

**4 правила `_b2_hard_rules`:**
1. Тільки факти з наданих статей (загальні знання — заборонено)
2. Нуль галюцинацій (цифри/події/компанії без підтвердження → помилка)
3. Глибокий аналіз впливу на Україну (мін. 4–5 речень, конкретні АФІ/ціни/дії)
4. Обсяг і деталі (поверхневий саммарі = помилка)

## Блок 3 (Товарні ринки)

Генерується **без LLM** — TradingView-style line charts (matplotlib + yfinance).

10 інструментів (`CHART_TICKERS`):

| Ключ | Інструмент | Тікер |
|---|---|---|
| НАФТА | Brent Crude | BZ=F |
| ГАЗ | Natural Gas | NG=F |
| КУКУРУДЗА | Corn | ZC=F |
| ПШЕНИЦЯ | Wheat | ZW=F |
| СОЄВІ_БОБИ | Soybeans | ZS=F |
| СОЄВА_ОЛІЯ | Soybean Oil | ZL=F |
| ПАЛЬМОВА | Palm Oil | POO=F |
| ЦУКОР | Sugar | SB=F |
| ЄВРО | EUR/USD | EURUSD=X |
| ЮАНЬ | USD/CNY | CNY=X |

Кожен графік: 45-денне вікно, лінія + gradient fill + volume bars, OHLC у заголовку, change badge.

## PDF рендеринг

| Параметр | Деталь |
|---|---|
| Бібліотека | **fpdf2** (FPDF2) |
| Шрифт | **DejaVuSans** (кирилиця, regular + bold) |
| Колірна схема | Professional white/dark: `COLOR_ACCENT=(27,79,216)`, `COLOR_BODY=(25,25,35)` |
| Markdown | `**bold**`, `[text](url)` clickable links, bare URLs → активні посилання |
| Header | Лого + назва + дата; тонка роздільна лінія |
| Footer | `"MacroHarvey · Ринковий звіт за DD.MM.YYYY · Стор. N"` + конфіденційно |
| Параграф | First-line indent 7mm, justified, line height 6.5mm |

## Facts-First vs Fallback

```
fetch_facts_for_report()      → article_facts (Stage 2)
fetch_recent_news_for_report() → articles (full_text або RSS snippet)

якщо facts_count > 0  → LLM синтезує виключно з фактів
якщо facts_count == 0 → fallback: LLM читає full_text / RSS snippets
```

Теги: `[FULLTEXT]` — реальна стаття, `[RSS_SNIPPET]` — лише RSS-заголовок + 1–2 речення.

**Multi-day fallback** (тільки `daily_brief`): якщо фактів < 15 → розширення вікна на 3 дні. `midday` та `weekly` — без fallback (explicit window).

## Збереження PDF в БД

Кожен надісланий звіт зберігається в `digest_reports` (BYTEA) перед `os.remove(pdf_path)`. Доступний через Mini App вкладку "Звіти".
