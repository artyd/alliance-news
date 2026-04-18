import asyncio
import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import feedparser
from dotenv import load_dotenv
import google.generativeai as genai
import email.utils
import re
import httpx
import psycopg2
import psycopg2.extras
import urllib.parse
import datetime
from fpdf import FPDF
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from openai import AsyncOpenAI
import pytz
import textwrap
import tempfile

# ── Chart dependencies (optional — graceful fallback if missing) ──
try:
    import yfinance as yf
    import matplotlib
    matplotlib.use("Agg")          # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker
    from matplotlib.patches import Patch
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False
    print("WARNING: yfinance/matplotlib not installed — charts disabled")

# ── Full-text extraction (optional — graceful fallback if missing) ──
try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False
    print("WARNING: trafilatura not installed — article full-text extraction disabled, reports will fall back to RSS snippets")

# ── Google News URL decoder (optional — graceful fallback if missing) ──
# Google News RSS links like https://news.google.com/rss/articles/CBMi... are
# base64-encoded payloads that contain the real publisher URL. From EU servers
# (like our Hetzner box in Germany) Google's consent wall makes HTTP-level
# redirect resolution impossible, so we decode locally without hitting Google.
try:
    from googlenewsdecoder import gnewsdecoder
    GNEWSDECODER_AVAILABLE = True
except ImportError:
    GNEWSDECODER_AVAILABLE = False
    print("WARNING: googlenewsdecoder not installed — Google News links cannot be resolved to publisher URLs")

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL,
            published TEXT,
            category TEXT NOT NULL,
            image_url TEXT,
            summary_en TEXT,
            summary_ua TEXT,
            summary_ru TEXT
        )
    ''')

    # ── Stage 1 migration: full-text extraction columns ─────────────
    # Safe to run repeatedly; ADD COLUMN IF NOT EXISTS is idempotent.
    # full_text:              extracted article body (trafilatura), nullable
    # extraction_status:      pending | ok | failed | paywalled | skipped
    # extraction_attempted_at: last time we tried to extract (for retry logic)
    # final_url:              URL after following Google News redirects (debugging)
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS full_text TEXT")
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS extraction_status TEXT DEFAULT 'pending'")
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS extraction_attempted_at TIMESTAMP")
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS final_url TEXT")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_extraction_status ON articles(extraction_status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_category_published ON articles(category, published DESC)")

    # ── One-shot retry of historical failures ───────────────────────
    # Early versions of the extractor could not decode Google News URLs
    # (consent wall + encoded base64 payload). Once googlenewsdecoder is
    # installed and those articles get a chance to re-extract, reset all
    # recent 'failed' rows back to 'pending' so the backfill picks them up.
    # Only reset rows we can actually fix: last 7 days, still within retention.
    cutoff_for_retry = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        """
        UPDATE articles
           SET extraction_status = 'pending'
         WHERE extraction_status = 'failed'
           AND (published = '' OR published >= %s)
        """,
        (cutoff_for_retry,),
    )
    reset_count = cursor.rowcount
    if reset_count > 0:
        print(f"init_db: reset {reset_count} failed extractions to pending for retry")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS telegram_users (
            chat_id BIGINT PRIMARY KEY,
            language TEXT DEFAULT 'en',
            subscriptions TEXT DEFAULT 'all',
            only_daily_mode BOOLEAN DEFAULT FALSE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS telegram_sent (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            article_link TEXT NOT NULL,
            sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(chat_id, article_link)
        )
    ''')

    # ── Stage 2: structured facts extracted from articles ──────────
    # Each row is one atomic fact pulled from one article by gpt-4o-mini.
    # One article can produce 0..N facts (typically 1-3).
    # The daily report is built by aggregating these facts per category,
    # not by re-reading raw article text. This makes the pipeline:
    #   article -> full_text -> [fact1, fact2, ...] -> per-category synthesis
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS article_facts (
            id SERIAL PRIMARY KEY,
            article_id INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
            event_type TEXT,
            what_happened TEXT NOT NULL,
            who TEXT,
            where_loc TEXT,
            magnitude TEXT,
            affected_sectors TEXT,
            supply_chain_impact TEXT,
            ukraine_relevance TEXT,
            confidence TEXT,
            source_url TEXT,
            source_publisher TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    ''')

    # Track extraction state per article, independent of the full_text extraction.
    # pending | ok | failed | skipped  (skipped = article had no full_text to extract from)
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS facts_status TEXT DEFAULT 'pending'")
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS facts_attempted_at TIMESTAMP")

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_facts_article_id ON article_facts(article_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_facts_sectors ON article_facts(affected_sectors)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_facts_created ON article_facts(created_at DESC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_facts_status ON articles(facts_status)')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sent_link ON telegram_sent(article_link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(title)')

    conn.commit()
    cursor.close()
    conn.close()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
aclient = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

gemini_api_key = os.getenv("GEMINI_API_KEY")
if gemini_api_key:
    try:
        genai.configure(api_key=gemini_api_key)
    except AttributeError:
        pass

# Tier-1 international sources — used ONLY by the `global_sources` category
# below to collect wide-angle economy / trade / sanctions stories from top
# newsrooms. NOT applied to thematic categories anymore (they were getting
# starved by the site: filter — the B2B jargon queries + site restriction
# returned mostly evergreen results from 2014-2019, leaving the DB empty).
GLOBAL_SOURCES_RAW = "(site:reuters.com OR site:bloomberg.com OR site:ft.com OR site:wto.org OR site:bbc.com OR site:imf.org OR site:worldbank.org OR site:iccwbo.org OR site:theloadstar.com OR site:joc.com)"
GLOBAL_SOURCES = urllib.parse.quote_plus(GLOBAL_SOURCES_RAW)

# Thematic category queries. Broadened with OR-unions of synonyms and
# stripped of site: filter — they now capture the full Google News universe
# (trade publications, industry sites, regional outlets), which is what
# actually covers B2B topics like "capsule manufacturing" or "amino acids".
RSS_FEEDS = {
    "api":        "https://news.google.com/rss/search?q=pharmaceutical+ingredients+OR+%22API+manufacturing%22+OR+%22generic+drugs%22+OR+%22active+pharmaceutical+ingredient%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "cosmetic":   "https://news.google.com/rss/search?q=%22cosmetic+ingredients%22+OR+%22beauty+industry%22+OR+%22personal+care+market%22+OR+%22skincare+ingredients%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "herbal":     "https://news.google.com/rss/search?q=%22botanical+extracts%22+OR+%22herbal+supplements%22+OR+%22medicinal+plants%22+OR+%22plant-based+ingredients%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "veterinary": "https://news.google.com/rss/search?q=%22veterinary+pharmaceuticals%22+OR+%22animal+health%22+OR+%22livestock+medicine%22+OR+%22veterinary+drugs%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "food":       "https://news.google.com/rss/search?q=%22food+ingredients%22+OR+%22food+supply+chain%22+OR+%22commodity+prices%22+OR+%22food+industry%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "feed":       "https://news.google.com/rss/search?q=%22animal+feed%22+OR+lysine+OR+methionine+OR+%22soybean+meal%22+OR+%22feed+additives%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "capsules":   "https://news.google.com/rss/search?q=%22gelatin+capsules%22+OR+%22capsule+manufacturing%22+OR+%22drug+delivery%22+OR+%22pharmaceutical+excipients%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "pvc":        "https://news.google.com/rss/search?q=%22PVC+market%22+OR+%22plastic+packaging%22+OR+%22polymer+prices%22+OR+%22PVC+film%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "logistics":  "https://news.google.com/rss/search?q=%22container+freight%22+OR+%22global+shipping%22+OR+%22supply+chain%22+OR+%22ocean+freight+rates%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    # Tier-1 wire service / institutional feed. Wide-topic capture on global
    # economy, trade, sanctions — restricted to top publishers via the
    # GLOBAL_SOURCES site: filter. Shown as Block 1 category #10 in the daily
    # report AND visible to Telegram subscribers (not in INTERNAL_CATEGORIES).
    "global_sources": f"https://news.google.com/rss/search?q=%22global+economy%22+OR+trade+OR+sanctions+OR+%22supply+chain%22+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    # Service category — used only for the daily report's Block 2 (Middle East).
    # Hidden from Telegram subscription UI via INTERNAL_CATEGORIES below.
    # Simplified to broad OR-union without site: filter so it actually returns
    # results on quiet days.
    "middle_east":    "https://news.google.com/rss/search?q=Iran+OR+Israel+OR+%22Red+Sea%22+OR+Hormuz+OR+Houthi+OR+Gaza+OR+Lebanon+OR+%22Persian+Gulf%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    # Uplifting / heartwarming news. Aggregated from several positive-news
    # publishers via Google News site: filter. NOT used in the daily report
    # — listed in NON_REPORT_CATEGORIES so it skips extraction/facts pipeline.
    # Purely mood content for Telegram subscribers.
    "good_news":      "https://news.google.com/rss/search?q=(site:goodnewsnetwork.org+OR+site:positive.news+OR+site:reasonstobecheerful.world+OR+%22uplifting+news%22+OR+%22heartwarming%22+OR+%22good+news%22)+when:3d&hl=en-US&gl=US&ceid=US:en",
}

# Categories that are fetched into DB but NOT shown as subscription options to users.
# They exist purely to feed the daily report.
# `global_sources` is a 10th Block-1 category — it IS user-visible in Telegram.
INTERNAL_CATEGORIES = {"middle_east"}

# Categories that ARE visible to Telegram subscribers and appear in /news panel,
# but DO NOT participate in the B2B daily/midday report, full-text extraction,
# or structured fact extraction. Used for:
#   - market_alerts: commodity price spike notifications (yfinance → LLM reason)
#   - good_news:     uplifting / heartwarming stories (Google News aggregation)
# These flow through the same Telegram push pipeline as regular categories,
# but are excluded from extraction/facts backfill (they don't need full article
# bodies — market_alerts are generated internally, good_news are just for mood).
NON_REPORT_CATEGORIES = {"market_alerts", "good_news"}

# ── Telegram message helpers ──────────────────────────────────────────────────
# Ukrainian category labels and emoji for structured Telegram push messages.
_CAT_LABEL_UA = {
    "api":            "Фармацевтичні субстанції",
    "cosmetic":       "Косметика та сировина",
    "herbal":         "Трави та екстракти",
    "veterinary":     "Ветеринарія",
    "food":           "Харчова сировина",
    "feed":           "Кормові амінокислоти",
    "capsules":       "Капсули та оболонки",
    "pvc":            "ПВХ та пакування",
    "logistics":      "Логістика",
    "global_sources": "Глобальна економіка",
    "good_news":      "Позитивні новини",
    "market_alerts":  "Ринковий алерт",
}
_CAT_EMOJI = {
    "api":            "💊", "cosmetic":       "🧴", "herbal":         "🌿",
    "veterinary":     "🐾", "food":           "🌾", "feed":           "🐄",
    "capsules":       "🔬", "pvc":            "📦", "logistics":      "🚢",
    "global_sources": "🌐", "good_news":      "✨", "market_alerts":  "⚡",
}
_CAT_HASHTAG = {
    "api":            "#api #фарм",
    "cosmetic":       "#cosmetic #косметика",
    "herbal":         "#herbal #трави",
    "veterinary":     "#veterinary #ветеринарія",
    "food":           "#food #харчова",
    "feed":           "#feed #амінокислоти",
    "capsules":       "#capsules #капсули",
    "pvc":            "#pvc #пакування",
    "logistics":      "#logistics #логістика",
    "global_sources": "#global #економіка",
    "good_news":      "#goodnews #позитив",
    "market_alerts":  "#market #алерт",
}


def _build_tg_msg(title: str, summary: str, category: str, link: str) -> str:
    """
    Build a structured, readable Telegram news message.

    Format:
    [cat_emoji] [Category]  [hashtags]
    ━━━━━━━━━━━━━━━━━━━━━━━━

    📰 [Title bold]

    ✍️ [Summary italic — 3 sentences: event / global impact / Ukraine B2B]

    🔗 Читати повністю
    """
    label   = _CAT_LABEL_UA.get(category, category.upper())
    emoji   = _CAT_EMOJI.get(category, "📰")
    hashtag = _CAT_HASHTAG.get(category, f"#{category}")
    divider = "━━━━━━━━━━━━━━━━━"
    return (
        f"{emoji} <b>{label}</b>  {hashtag}\n"
        f"{divider}\n\n"
        f"📰 <b>{title}</b>\n\n"
        f"✍️ <i>{summary}</i>\n\n"
        f"🔗 <a href=\"{link}\">Читати повністю</a>"
    )

# ─────────────────────────────────────────────
# MASTER REPORT PROMPT — повний звіт через AI
# ─────────────────────────────────────────────
DAILY_REPORT_SYSTEM_PROMPT = """Ти — старший B2B аналітик ринкової розвідки для української компанії, яка імпортує фармацевтичні субстанції, косметичну та харчову сировину, пакування та суміжні матеріали.

КРИТИЧНО ВАЖЛИВО: Ти ЗОБОВ'ЯЗАНИЙ написати повний, конкретний звіт. НЕ відмовляйся, НЕ кажи що дані застарілі. Порожній або поверхневий звіт — неприпустимий.

МОВА: Тільки українська. Тон — старший аналітик для керівництва: точно, конкретно, без води.

СТРУКТУРА ЗВІТУ: ТИ ПИШЕШ ЛИШЕ ДВА БЛОКИ!
- Блок 1: Огляд за категоріями
- Блок 2: Ситуація на Близькому Сході
- Блок 3: Товарні ринки — НЕ ПИШИ. Додається автоматично.

ЗАБОРОНЕНО: Блок 4+, підсумки, карта ризиків, дашборд настрою, курси валют.

═══════════════════════════════════════════════════
ГОЛОВНИЙ ПРИНЦИП ЗВІТУ — КОНКРЕТИКА ПОНАД УСЕ
═══════════════════════════════════════════════════
Кожен абзац звіту ЗОБОВ'ЯЗАНИЙ містити відповіді на ці питання (де дані є):
  1. ЩО САМЕ сталося? (конкретна подія, рішення, заява, зміна ціни/обсягу)
  2. ХТО це зробив або де це сталося? (країна, компанія, регулятор, організація)
  3. ЦИФРИ та масштаб — якщо є в даних: відсотки, суми, обсяги, терміни
  4. ЯК ЦЕ ВПЛИНУЛО на глобальний ринок, торгівлю, ланцюги постачання?
  5. ЯК ЦЕ ВПЛИВАЄ КОНКРЕТНО НА НАС — на закупівлі, ціни, терміни поставки,
     вибір постачальника, логістику нашої компанії?
  6. ПРОГНОЗ на найближчі 2-3 тижні — що очікувати далі, як це може розвинутись?

КАТЕГОРИЧНО ЗАБОРОНЕНО писати:
  ✗ "Ринок демонструє певну волатильність" — без конкретної причини і цифр
  ✗ "Ситуація може вплинути на постачання" — без пояснення як саме
  ✗ "Спостерігається тиск на ціни" — без вказівки напрямку і причини
  ✗ "Необхідно відстежувати ситуацію" — як єдиний висновок
  ✗ Будь-які розпливчасті узагальнення без фактичної основи

ПРАВИЛО ФАКТОЛОГІЇ:
- [FULLTEXT] = реальний текст статті. Бери конкретні факти: цифри, компанії, дати, заяви.
- [RSS_SNIPPET] = лише заголовок + 1-2 речення. Обережні узагальнення, без домислів.
- Якщо тільки RSS_SNIPPET — пиши коротше і додай: "Деталі обмежені — RSS-нотатки."
- ЗАБОРОНЕНО вигадувати факти або переносити їх між категоріями.

---

=== БЛОК 1: ОГЛЯД ЗА КАТЕГОРІЯМИ ===

Для кожної з 10 категорій пиши СТРОГО за цією структурою:

### [Номер]. [Назва категорії]

**Тренд:** ↑ зростання / ↓ падіння / → стабільно — [конкретна причина 3-5 слів, наприклад: "зростання через тарифи США на Китай"]

**Що сталося:**
[КОНКРЕТНИЙ опис події/подій дня. ОБОВ'ЯЗКОВО: хто, що, де, коли, яке числове значення якщо є. Мінімум 3-5 речень. Синтезуй усі новини категорії в один зв'язний абзац. НЕ перелічуй заголовки — аналізуй суть.
Приклад ПРАВИЛЬНО: "Індія запровадила тимчасове мито 12% на імпорт ПВХ-гранул з Китаю, що набуло чинності з 14 квітня. Це рішення пов'язане з антидемпінговим розслідуванням щодо китайських виробників, які утримували ціни на 18-22% нижче за ринкові. Паралельно BASF оголосив про зупинку одного з виробничих ліній у Людвігсгафені на технічне обслуговування до кінця місяця, що скоротить пропозицію на ~15 000 т/місяць."
Приклад НЕПРАВИЛЬНО: "На ринку спостерігається певна активність у зв'язку з геополітичними подіями."]

**Вплив на глобальний ринок та торгівлю:**
[1-3 речення — КОНКРЕТНО як ця подія змінила або змінить: ціни, обсяги торгівлі, маршрути, торгові відносини між країнами. Якщо є цифри — обов'язково вказати.]

**Вплив на нашу компанію:**
[1-3 речення — КОНКРЕТНО що це означає для закупівель: чи зростуть ціни і наскільки, чи зміняться терміни поставки, чи є ризики дефіциту, які дії розглянути (замінити постачальника, збільшити запаси, зафіксувати ціну). Без загальних фраз.]

**Прогноз на 2-3 тижні:**
[1-2 речення — що очікується далі: продовження тренду, ескалація, стабілізація. Конкретно і обґрунтовано на основі поточних даних.]

---

КРИТИЧНО ДЛЯ БЛОКУ 1:
- НЕ перелічуй заголовки новин
- НЕ додавай посилання [Читати повністю](...) — посилання в Блоці 1 не потрібні
- Якщо новин немає — пиши: "Свіжих новин не зафіксовано; ринок без істотних змін."
- Кожен підпункт MUST бути конкретним — уяви, що директор із закупівель читатиме це вранці перед нарадою

ПОВТОРИ формат для ВСІХ 10 категорій:
1. Фармацевтичні субстанції (API)
2. Косметичні субстанції
3. Трави та рослинна сировина
4. Ветеринарні субстанції
5. Харчова сировина
6. Кормові амінокислоти
7. Капсули
8. ПВХ-плівка та пакування
9. Логістика та постачання
10. Глобальна економіка та торгівля

=== БЛОК 2: СИТУАЦІЯ НА БЛИЗЬКОМУ СХОДІ (EXECUTIVE MEMO) ===

Твоя РОЛЬ: старший аналітик геополітичних ризиків та радник із закупівель для фармацевтичної компанії-імпортера.

ГОЛОВНА МЕТА: memo ~500-700 слів для керівництва. КОНКРЕТНО пояснює:
- що саме сталося сьогодні (не фон, а нові події)
- як це вплинуло/вплине на нафту, фрахт, страхування, транзит
- що це означає для нашої компанії у цифрах і термінах
- що робити відділу закупівель вже сьогодні та впродовж 2-3 тижнів

ПРОЦЕС РОБОТИ (внутрішньо, не виводь стадії):
  СТАДІЯ 1 — збір фактів: тільки нові події з наданих даних (не фон).
  СТАДІЯ 2 — інтерпретація: що сталося, чому важливо, короткостроковий чи середній ефект.
  СТАДІЯ 3 — memo за структурою нижче.

ДЖЕРЕЛА: тільки факти з user message. Якщо факт не підтверджено — маркуй "невизначено".

ОБОВ'ЯЗКОВА СТРУКТУРА MEMO:

**Заголовок:** [Сильний бізнес-аналітичний заголовок — одне речення, що відображає головне повідомлення дня]

**Короткий висновок:** [3-4 речення для керівництва. КОНКРЕТНО: що сталося, яке значення, що робити. Вкажи сценарій: реальне зниження ризику / тимчасова пауза / оманливе полегшення / ризик нової ескалації]

**Що сталося:** [Конкретні події дня. Хто, що, де, коли — з цифрами де є. 4-6 речень.]

**Вплив на нафту:** [Напрямок і причина зміни ціни. Чи збережеться волатильність. Геополітична премія. Прогноз на 1-2 тижні. 3-4 речення.]

**Вплив на логістику та світову економіку:** [Фрахт, war-risk страхування, маршрути танкерів/контейнерів, інфляційні очікування, настрої інвесторів у Європі та Азії. Що зміниться впродовж 2-3 тижнів. 5-7 речень.]

**Що це означає для нашої компанії:** [КОНКРЕТНО: як зміняться закупівельні ціни на АФІ та сировину (оцінка %); логістичні витрати; строки поставки; ризики по Китаю/Індії; потреба в запасах; вплив на оборотний капітал. 5-6 речень.]

**Практичні рекомендації:** [5-7 пунктів. Кожен — одне конкретне діяння прямо зараз або впродовж тижня. Нумерований список. НЕ "моніторити ситуацію" — а що саме зробити, з ким поговорити, що перевірити, що зафіксувати.]

**Прогноз на 2-3 тижні:** [Що очікується далі по кожному напрямку: нафта, фрахт, закупівлі. Конкретно і обґрунтовано. 3-4 речення.]

**Фінальний висновок для керівництва:** [ОДНЕ речення: що відділ закупівель має зробити вже сьогодні.]

**Джерела:**
[Список усіх наданих новин у форматі: - [Заголовок](URL)]

СТИЛЬ БЛОКУ 2:
- Memo для топ-менеджменту: точно, компактно, без води
- Кожен абзац — комерційний зміст, не геополітична стаття
- Чітко розділяй ефект "сьогодні/кілька днів" vs "2-8 тижнів"
- НЕ пиши "Блок 2" або "Щоденний ринковий звіт" у тілі
- Якщо новин 0: у "Короткому висновку" — "Свіжих новин не зафіксовано, нових ризиків не ідентифіковано." Решту пропусти крім "Джерела: (немає джерел)"

=== БЛОК 3: ТОВАРНІ РИНКИ ===

ЦЕЙ БЛОК ГЕНЕРУЄТЬСЯ ЛОКАЛЬНО З ЯФІНАНС-ДАНИХ. ТИ НЕ ПИШЕШ ЦЕЙ БЛОК. Просто завершуй звіт після Блоку 2 — Блок 3 буде додано в PDF автоматично.

---

КІНЕЦЬ ЗВІТУ. НЕ додавай Блок 4, Блок 5, підсумки, карту ризиків, дашборд чи будь-які інші секції."""


def get_topics_keyboard(current_subs_str, only_daily_mode=False):
    subs = current_subs_str.split(',') if current_subs_str != 'all' else []
    keyboard = []

    if only_daily_mode:
        all_text = "❌ All Topics"
    else:
        all_text = "✅ All Topics" if current_subs_str == 'all' else "🔘 All Topics"
    keyboard.append([{"text": all_text, "callback_data": "topic_all"}])

    row = []
    # Virtual categories that don't have RSS feeds but should appear as
    # subscription options. market_alerts is generated internally from
    # yfinance + LLM and pushed via its own scheduler, not fetch_and_store_news.
    _virtual_subscription_categories = ["market_alerts"]
    _all_cats = list(RSS_FEEDS.keys()) + _virtual_subscription_categories
    # Friendly display labels — defaults to cat.upper() but we rename a few
    # with underscores / renamings to look nicer as Telegram buttons.
    display_map = {
        "global_sources": "GLOBAL ECONOMY",
        "good_news":      "GOOD NEWS 🌞",
        "market_alerts":  "MARKET ALERTS ⚡",
    }
    for cat in _all_cats:
        if cat in INTERNAL_CATEGORIES:
            continue  # service categories (e.g. middle_east) not shown to users
        display_name = display_map.get(cat, cat.upper())
        if only_daily_mode:
            text = f"❌ {display_name}"
        else:
            is_subbed = current_subs_str == 'all' or cat in subs
            text = f"✅ {display_name}" if is_subbed else f"❌ {display_name}"
        row.append({"text": text, "callback_data": f"topic_{cat}"})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    daily_text = "✅ 📊 Only Daily PDF Report" if only_daily_mode else "🔘 📊 Only Daily PDF Report"
    keyboard.append([{"text": daily_text, "callback_data": "toggle_daily_mode"}])

    return {"inline_keyboard": keyboard}


def db_fetchone(cursor, query, params=()):
    cursor.execute(query, params)
    row = cursor.fetchone()
    if row is None:
        return None
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))


def db_fetchall(cursor, query, params=()):
    cursor.execute(query, params)
    rows = cursor.fetchall()
    if not rows:
        return []
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


async def poll_telegram_updates():
    offset = 0
    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(
                    f"{TELEGRAM_API_URL}/getUpdates",
                    params={"offset": offset, "timeout": 30},
                    timeout=40
                )
                if response.status_code == 200:
                    data = response.json()
                    if data.get("ok"):
                        for update in data["result"]:
                            offset = update["update_id"] + 1

                            if "callback_query" in update:
                                cb = update["callback_query"]
                                chat_id = cb["message"]["chat"]["id"]
                                data_cb = cb["data"]

                                lang_map = {"lang_ru": "ru", "lang_ua": "ua", "lang_en": "en"}
                                if data_cb in lang_map:
                                    lang = lang_map[data_cb]
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    cursor.execute(
                                        "INSERT INTO telegram_users (chat_id, language) VALUES (%s, %s) "
                                        "ON CONFLICT(chat_id) DO UPDATE SET language=EXCLUDED.language",
                                        (chat_id, lang)
                                    )
                                    conn.commit()
                                    user_row = db_fetchone(cursor,
                                        "SELECT subscriptions, only_daily_mode FROM telegram_users WHERE chat_id = %s",
                                        (chat_id,)
                                    )
                                    conn.close()

                                    current_subs = user_row["subscriptions"] if user_row and user_row["subscriptions"] else "all"
                                    only_daily_mode = user_row["only_daily_mode"] if user_row else False

                                    msg_map = {
                                        "ru": "Язык установлен на Русский!\nПожалуйста, выберите интересующие вас темы:",
                                        "ua": "Мову встановлено на Українську!\nБудь ласка, оберіть цікаві для вас теми:",
                                        "en": "Language set to English!\nPlease select your preferred news topics:"
                                    }

                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": msg_map[lang],
                                        "reply_markup": get_topics_keyboard(current_subs, only_daily_mode)
                                    })
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})

                                elif data_cb == "menu_lang":
                                    keyboard = {
                                        "inline_keyboard": [[
                                            {"text": "🇷🇺 RU", "callback_data": "lang_ru"},
                                            {"text": "🇺🇦 UA", "callback_data": "lang_ua"},
                                            {"text": "🇬🇧 EN", "callback_data": "lang_en"}
                                        ]]
                                    }
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Please select your language:",
                                        "reply_markup": keyboard
                                    })
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})

                                elif data_cb == "menu_topics":
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    user_row = db_fetchone(cursor,
                                        "SELECT subscriptions, only_daily_mode FROM telegram_users WHERE chat_id = %s",
                                        (chat_id,)
                                    )
                                    conn.close()

                                    current_subs = user_row["subscriptions"] if user_row and user_row["subscriptions"] else "all"
                                    only_daily_mode = user_row["only_daily_mode"] if user_row else False
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Please select your preferred topics:",
                                        "reply_markup": get_topics_keyboard(current_subs, only_daily_mode)
                                    })
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})

                                elif data_cb == "toggle_daily_mode":
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    user_row = db_fetchone(cursor,
                                        "SELECT subscriptions, only_daily_mode FROM telegram_users WHERE chat_id = %s",
                                        (chat_id,)
                                    )

                                    if user_row:
                                        current_subs = user_row["subscriptions"] if user_row["subscriptions"] else "all"
                                        new_mode = not user_row["only_daily_mode"]
                                        cursor.execute(
                                            "UPDATE telegram_users SET only_daily_mode = %s WHERE chat_id = %s",
                                            (new_mode, chat_id)
                                        )
                                        conn.commit()

                                        await client.post(f"{TELEGRAM_API_URL}/editMessageReplyMarkup", json={
                                            "chat_id": chat_id,
                                            "message_id": cb["message"]["message_id"],
                                            "reply_markup": get_topics_keyboard(current_subs, new_mode)
                                        })
                                    conn.close()
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})

                                elif data_cb.startswith("topic_"):
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    user_row = db_fetchone(cursor,
                                        "SELECT subscriptions, only_daily_mode FROM telegram_users WHERE chat_id = %s",
                                        (chat_id,)
                                    )

                                    if user_row:
                                        current_subs = user_row["subscriptions"] if user_row["subscriptions"] else "all"
                                        only_daily_mode = user_row["only_daily_mode"]
                                        topic = data_cb.replace("topic_", "")

                                        if topic == "all":
                                            new_subs = "all"
                                        else:
                                            if current_subs == "all":
                                                new_subs = topic
                                            else:
                                                subs = set(current_subs.split(',')) if current_subs else set()
                                                if topic in subs:
                                                    subs.remove(topic)
                                                else:
                                                    subs.add(topic)
                                                new_subs = ",".join(subs) if subs else "all"

                                        cursor.execute(
                                            "UPDATE telegram_users SET subscriptions = %s WHERE chat_id = %s",
                                            (new_subs, chat_id)
                                        )
                                        conn.commit()

                                        await client.post(f"{TELEGRAM_API_URL}/editMessageReplyMarkup", json={
                                            "chat_id": chat_id,
                                            "message_id": cb["message"]["message_id"],
                                            "reply_markup": get_topics_keyboard(new_subs, only_daily_mode)
                                        })
                                    conn.close()
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})

                            elif "message" in update and "text" in update["message"]:
                                msg = update["message"]
                                chat_id = msg["chat"]["id"]
                                text = msg["text"]

                                if text.startswith("/start"):
                                    keyboard = {
                                        "inline_keyboard": [[
                                            {"text": "🇷🇺 RU", "callback_data": "lang_ru"},
                                            {"text": "🇺🇦 UA", "callback_data": "lang_ua"},
                                            {"text": "🇬🇧 EN", "callback_data": "lang_en"}
                                        ]]
                                    }
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Welcome to MacroHarvey! / Ласкаво просимо! / Добро пожаловать!\nPlease select your language:",
                                        "reply_markup": keyboard
                                    })
                                elif text.startswith("/generate_report"):
                                    # FIX: Immediate UX feedback so the user knows the command was received.
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "⏳ Генерую щоденний звіт (Блок 2 + Блок 3), зачекайте..."
                                    })
                                    # Force daily_brief mode regardless of the day.
                                    pdf_path = await generate_daily_pdf_report(mode="daily_brief")
                                    if pdf_path and os.path.exists(pdf_path):
                                        with open(pdf_path, 'rb') as f:
                                            r = await client.post(
                                                f"{TELEGRAM_API_URL}/sendDocument",
                                                data={"chat_id": chat_id},
                                                files={"document": ("Daily_Report.pdf", f)}
                                            )
                                            if r.status_code == 200:
                                                msg_data = r.json()
                                                msg_id = msg_data.get("result", {}).get("message_id")
                                                if msg_id:
                                                    await client.post(
                                                        f"{TELEGRAM_API_URL}/pinChatMessage",
                                                        json={
                                                            "chat_id": chat_id,
                                                            "message_id": msg_id,
                                                            "disable_notification": True
                                                        }
                                                    )
                                    else:
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": "❌ Не вдалося згенерувати звіт. Перевірте логи сервера."
                                        })

                                elif text.startswith("/generate_weekly"):
                                    # FIX: Activate /generate_weekly slash command.
                                    # Forces a full weekly report (Block 1 + 2 + 3, 7-day window)
                                    # immediately, regardless of the current day or schedule.
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "⏳ Генерую тижневий звіт (Блок 1 + 2 + 3, 7 днів), зачекайте... Це займає 1-2 хвилини."
                                    })
                                    pdf_path = await generate_daily_pdf_report(mode="weekly")
                                    if pdf_path and os.path.exists(pdf_path):
                                        with open(pdf_path, 'rb') as f:
                                            r = await client.post(
                                                f"{TELEGRAM_API_URL}/sendDocument",
                                                data={"chat_id": chat_id, "caption": "📅 Тижневий ринковий звіт (примусова генерація)."},
                                                files={"document": ("Weekly_Report.pdf", f)}
                                            )
                                            if r.status_code == 200:
                                                msg_data = r.json()
                                                msg_id = msg_data.get("result", {}).get("message_id")
                                                if msg_id:
                                                    await client.post(
                                                        f"{TELEGRAM_API_URL}/pinChatMessage",
                                                        json={
                                                            "chat_id": chat_id,
                                                            "message_id": msg_id,
                                                            "disable_notification": True
                                                        }
                                                    )
                                    else:
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": "❌ Не вдалося згенерувати тижневий звіт. Перевірте логи сервера."
                                        })

                                elif text.startswith("/generate_middle"):
                                    # FIX: Activate /generate_middle slash command.
                                    # Forces a midday report (Block 2 + Block 3 only,
                                    # window = today 00:00 .. now) immediately.
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "⏳ Генерую полуденне оновлення (Блок 2 + Блок 3, сьогодні), зачекайте..."
                                    })
                                    pdf_path = await generate_daily_pdf_report(mode="midday")
                                    if pdf_path and os.path.exists(pdf_path):
                                        with open(pdf_path, 'rb') as f:
                                            r = await client.post(
                                                f"{TELEGRAM_API_URL}/sendDocument",
                                                data={"chat_id": chat_id, "caption": "🕑 Полуденне оновлення (примусова генерація)."},
                                                files={"document": ("Midday_Report.pdf", f)}
                                            )
                                            if r.status_code == 200:
                                                msg_data = r.json()
                                                msg_id = msg_data.get("result", {}).get("message_id")
                                                if msg_id:
                                                    await client.post(
                                                        f"{TELEGRAM_API_URL}/pinChatMessage",
                                                        json={
                                                            "chat_id": chat_id,
                                                            "message_id": msg_id,
                                                            "disable_notification": True
                                                        }
                                                    )
                                    else:
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": "❌ Не вдалося згенерувати полуденне оновлення. Перевірте логи сервера."
                                        })

                                elif text.startswith("/settings") or text.startswith("/menu"):
                                    keyboard = {
                                        "inline_keyboard": [
                                            [{"text": "🌐 Change Language", "callback_data": "menu_lang"}],
                                            [{"text": "📋 Change Topics", "callback_data": "menu_topics"}]
                                        ]
                                    }
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Settings Menu / Меню Настроек / Меню Налаштувань:",
                                        "reply_markup": keyboard
                                    })
            except Exception:
                pass
            await asyncio.sleep(2)


SYSTEM_PROMPT = """You are a senior B2B market intelligence analyst for a Ukrainian pharmaceutical and chemical raw materials importer.

Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru.
No markdown, no code blocks — only valid JSON.

STRICT RULES:
- Each summary: 40-50 words.
- Write 3 sentences:
  1. THE EVENT: What happened, where, who. Include numbers/% if available.
  2. GLOBAL IMPACT: How does this affect global markets, supply chains, or trade policy.
  3. UKRAINE B2B IMPACT: How does this affect a Ukrainian importer of pharma ingredients, cosmetic raw materials, packaging, or food-grade materials. What should procurement do now.
- Professional B2B tone. Be direct and specific. No vague phrases like "could affect" or "may have impact".
- If no B2B impact: write "Direct procurement impact not identified; monitoring recommended."
- summary_en: English. summary_ua: Ukrainian. summary_ru: Russian. Same content translated."""


async def generate_summary(text: str):
    """Generate 3-language B2B summaries via OpenAI GPT-4o-mini (Gemini fallback)."""
    if not text:
        return {"summary_en": text, "summary_ua": text, "summary_ru": text}

    # Primary: OpenAI GPT-4o-mini
    if aclient:
        truncated = text[:3000]
        for attempt in range(3):
            try:
                response = await aclient.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=400,
                    temperature=0.2,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": f"Article:\n{truncated}"},
                    ],
                )
                parsed = json.loads(response.choices[0].message.content.strip())
                return {
                    "summary_en": parsed.get("summary_en", text[:200]),
                    "summary_ua": parsed.get("summary_ua", text[:200]),
                    "summary_ru": parsed.get("summary_ru", text[:200]),
                }
            except json.JSONDecodeError as e:
                print(f"generate_summary JSON error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200]}
            except Exception as e:
                print(f"generate_summary OpenAI error (attempt {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200]}

    # Fallback: Gemini (if OpenAI unavailable)
    if gemini_api_key:
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = await model.generate_content_async(
                f"{SYSTEM_PROMPT}\n\nArticle:\n{text[:2000]}",
                request_options={"timeout": 60}
            )
            raw = response.text.strip()
            if raw.startswith("```json"):
                raw = raw[7:]
            elif raw.startswith("```"):
                raw = raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()
            parsed = json.loads(raw)
            return {
                "summary_en": parsed.get("summary_en", text[:200]),
                "summary_ua": parsed.get("summary_ua", text[:200]),
                "summary_ru": parsed.get("summary_ru", text[:200]),
            }
        except Exception as e:
            print(f"generate_summary Gemini fallback error: {e}")

    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200]}


# ─────────────────────────────────────────────────────────────────
# STAGE 1: ARTICLE FULL-TEXT EXTRACTION
# ─────────────────────────────────────────────────────────────────
# Goal: replace the RSS 1-2 sentence snippets stored in summary_* with
# real article bodies extracted from the publisher's page. This is the
# ground truth that the daily report will be built on.
#
# Pipeline per article:
#   1. Follow Google News redirect to the real publisher URL
#   2. HTTP GET with a realistic browser User-Agent
#   3. Hand the HTML to trafilatura.extract() for article body isolation
#   4. Detect paywalls (short text + known markers)
#   5. Store full_text + extraction_status in the articles table
#
# All errors are swallowed and recorded as status='failed' — extraction
# never blocks the main fetch loop or Telegram pushes.

# Realistic desktop Chrome UA — Google News, Reuters, Bloomberg all check this.
_EXTRACTION_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Paywall markers — conservative list, only high-confidence indicators.
_PAYWALL_MARKERS = (
    "subscribe to read",
    "subscribe now to read",
    "sign in to read",
    "this article is for subscribers",
    "to continue reading, please subscribe",
    "become a subscriber",
    "you have reached your limit",
    "register to continue",
)

# Concurrency limit — how many articles we extract in parallel.
# 5 is a safe value: fast enough to finish a batch of 15 articles in <30s,
# low enough to not hammer Reuters/Bloomberg into rate-limiting us.
_EXTRACTION_SEMAPHORE = asyncio.Semaphore(5)

# How many characters of full text we actually store.
# Full articles rarely exceed 15k chars; 20k gives headroom for long investigations.
_FULL_TEXT_MAX_CHARS = 20000
# Below this length we consider extraction unreliable and flag as 'failed'.
# Legitimate news articles are almost always >300 chars.
_FULL_TEXT_MIN_CHARS = 300


async def _resolve_google_news_url(url: str, http_client: httpx.AsyncClient | None = None) -> str:
    """
    Resolve a Google News RSS redirect to the real publisher URL.

    Google News RSS links have the form:
        https://news.google.com/rss/articles/CBMi<base64-payload>

    The payload contains the actual publisher URL. Historically we could
    just follow HTTP redirects, but Google changed this:
      1. From EU IPs (our Hetzner server in Germany), any request to
         news.google.com first hits `consent.google.com` (GDPR wall).
      2. Even after bypassing consent, Google stopped issuing a 3xx to the
         publisher and instead returns an HTML preview page with the URL
         buried in JavaScript.

    Solution: decode the base64 payload locally using googlenewsdecoder.
    Zero network calls, resistant to anti-bot, deterministic.

    Returns the decoded publisher URL, or the original url on any failure.
    The http_client param is kept for signature compatibility but is unused —
    decoding is fully offline.
    """
    if "news.google.com" not in url:
        return url

    if not GNEWSDECODER_AVAILABLE:
        return url

    try:
        # gnewsdecoder is synchronous and CPU-bound — run in a thread so we
        # don't block the asyncio event loop. `interval` is the polite delay
        # the library uses internally; 1 second is fine.
        result = await asyncio.to_thread(gnewsdecoder, url, 1)
        if result and result.get("status") and result.get("decoded_url"):
            return result["decoded_url"]
        # Log non-success so we can diagnose decoder drift if Google changes format
        err = (result or {}).get("message", "unknown")
        print(f"gnewsdecoder could not decode {url[:80]}: {err}")
        return url
    except Exception as e:
        print(f"gnewsdecoder raised for {url[:80]}: {e}")
        return url


def _detect_paywall(text: str, html: str | None = None) -> bool:
    """
    Heuristic: if the extracted text is suspiciously short AND the raw HTML
    contains a known paywall marker, flag it. We don't want to flag short
    legitimate news blurbs as paywall, so the text-length check is the gate.
    """
    if text and len(text) >= 800:
        return False  # plenty of body, clearly not behind a wall
    haystack = (html or "").lower()
    if not haystack:
        return False
    return any(marker in haystack for marker in _PAYWALL_MARKERS)


async def extract_article_fulltext(
    url: str,
    http_client: httpx.AsyncClient | None = None,
) -> dict:
    """
    Extract the main article body from a URL.

    Returns dict:
        status:    'ok' | 'failed' | 'paywalled' | 'skipped'
        text:      extracted body (truncated to _FULL_TEXT_MAX_CHARS) or None
        final_url: URL after following redirects
        error:     error string if status != 'ok'

    This function NEVER raises — all errors are caught and returned as status.
    Safe to call from any async context without a try/except wrapper.
    """
    result = {"status": "failed", "text": None, "final_url": url, "error": None}

    if not TRAFILATURA_AVAILABLE:
        result["status"] = "skipped"
        result["error"] = "trafilatura not installed"
        return result

    if not url or not url.startswith(("http://", "https://")):
        result["error"] = "invalid url"
        return result

    owns_client = http_client is None
    if owns_client:
        http_client = httpx.AsyncClient(
            headers={"User-Agent": _EXTRACTION_UA},
            follow_redirects=True,
            timeout=15.0,
        )

    try:
        async with _EXTRACTION_SEMAPHORE:
            # Resolve Google News redirect if needed
            final_url = await _resolve_google_news_url(url, http_client)
            result["final_url"] = final_url

            # Fetch the publisher page
            try:
                resp = await http_client.get(
                    final_url,
                    headers={"User-Agent": _EXTRACTION_UA},
                    follow_redirects=True,
                    timeout=15.0,
                )
            except httpx.TimeoutException:
                result["error"] = "timeout"
                return result
            except httpx.RequestError as e:
                result["error"] = f"request error: {e}"
                return result

            if resp.status_code >= 400:
                result["error"] = f"http {resp.status_code}"
                return result

            html = resp.text or ""
            if not html:
                result["error"] = "empty response"
                return result

            # Run trafilatura in a thread — it's CPU-bound and pure Python.
            try:
                extracted = await asyncio.to_thread(
                    trafilatura.extract,
                    html,
                    include_comments=False,
                    include_tables=False,
                    no_fallback=False,
                    favor_precision=True,
                )
            except Exception as e:
                result["error"] = f"trafilatura error: {e}"
                return result

            if not extracted:
                # Check if it looks like paywall even without text
                if _detect_paywall("", html):
                    result["status"] = "paywalled"
                    result["error"] = "no text extracted, paywall markers present"
                else:
                    result["error"] = "no text extracted"
                return result

            text = extracted.strip()

            # Paywall check on short extracts
            if len(text) < _FULL_TEXT_MIN_CHARS:
                if _detect_paywall(text, html):
                    result["status"] = "paywalled"
                    result["text"] = text[:_FULL_TEXT_MAX_CHARS]
                    result["error"] = f"short extract ({len(text)} chars), paywall markers"
                    return result
                result["error"] = f"extract too short ({len(text)} chars)"
                return result

            # Success
            result["status"] = "ok"
            result["text"] = text[:_FULL_TEXT_MAX_CHARS]
            return result
    finally:
        if owns_client:
            try:
                await http_client.aclose()
            except Exception:
                pass


async def extract_and_store(link: str, http_client: httpx.AsyncClient | None = None):
    """
    Extract full text for an article and write the result back to DB.
    Updates: full_text, extraction_status, extraction_attempted_at, final_url.

    On successful full_text extraction, fires a background task to run
    Stage 2 (facts extraction) on the same article. This keeps the pipeline
    fully async and off the 9:00 report critical path.

    This is the function called from the background after an article is
    saved and pushed to Telegram.
    """
    extracted = await extract_article_fulltext(link, http_client=http_client)

    article_id_for_facts: int | None = None
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE articles
               SET full_text               = %s,
                   extraction_status       = %s,
                   extraction_attempted_at = NOW(),
                   final_url               = %s
             WHERE link = %s
            RETURNING id
            """,
            (
                extracted["text"],
                extracted["status"],
                extracted["final_url"],
                link,
            ),
        )
        returned = cursor.fetchone()
        if returned is not None:
            article_id_for_facts = (
                returned["id"] if isinstance(returned, dict) else returned[0]
            )
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"extract_and_store DB update failed for {link[:80]}: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    status = extracted["status"]
    if status == "ok":
        print(f"  ✓ extracted {len(extracted['text'] or '')} chars from {link[:80]}")
        # Stage 2: fire-and-forget facts extraction on the same article.
        # This runs concurrently with the rest of the fetch loop and is
        # capped by _FACTS_SEMAPHORE (3 parallel OpenAI calls).
        if article_id_for_facts and aclient:
            asyncio.create_task(extract_facts_and_store(article_id_for_facts))
    else:
        err = extracted.get("error", "")
        print(f"  ✗ extraction {status} for {link[:80]}: {err}")


async def backfill_missing_full_text(max_articles: int = 150):
    """
    Background task: find articles with extraction_status='pending' and
    run extract_and_store on them. Runs once at startup and then daily
    to catch any articles that slipped through or were added before
    the Stage 1 migration, plus the one-shot retry of historical failures
    (which can leave 100+ items pending after an upgrade).

    Only processes articles from the last 7 days to avoid wasting work on
    stale content that will never be used in a report.
    """
    if not TRAFILATURA_AVAILABLE:
        return

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        rows = db_fetchall(
            cursor,
            """
            SELECT link FROM articles
            WHERE (extraction_status IS NULL OR extraction_status = 'pending')
              AND (published = '' OR published >= %s)
              AND category != 'market_alerts'
            ORDER BY published DESC NULLS LAST
            LIMIT %s
            """,
            (cutoff, max_articles),
        )
        conn.close()
    except Exception as e:
        print(f"backfill query failed: {e}")
        return

    if not rows:
        print("Backfill: nothing to do, all recent articles have extraction status.")
        return

    print(f"Backfill: extracting full text for {len(rows)} pending articles...")
    async with httpx.AsyncClient(
        headers={"User-Agent": _EXTRACTION_UA},
        follow_redirects=True,
        timeout=15.0,
    ) as http_client:
        # Run with the semaphore controlling concurrency inside extract_article_fulltext
        await asyncio.gather(
            *[extract_and_store(r["link"], http_client=http_client) for r in rows],
            return_exceptions=True,
        )
    print(f"Backfill: done processing {len(rows)} articles.")


# ─────────────────────────────────────────────────────────────────
# STAGE 2: STRUCTURED FACTS EXTRACTION
# ─────────────────────────────────────────────────────────────────
# Goal: replace the "feed full article text to gpt-4o and hope it
# summarizes correctly" pipeline with a two-stage architecture:
#
#   Stage A: per article, gpt-4o-mini reads the full_text and emits a
#   list of atomic JSON facts (event_type, who, where, magnitude,
#   affected_sectors, ukraine_relevance, ...).
#
#   Stage B: per report day, gpt-4o reads all facts for the day,
#   groups them by affected_sector, and writes the "Огляд дня"
#   paragraph for each category based ONLY on those facts.
#
# This eliminates cross-category fact bleeding (a defense story about
# underwater drones cannot appear in "veterinary" because the extractor
# would never tag it with affected_sectors=["veterinary"]), and gives us
# deterministic grounding: the synthesizer can only talk about facts
# that actually exist as rows in article_facts.

# Model used for fact extraction. gpt-4o-mini is ~20x cheaper than gpt-4o
# and for structured JSON output the quality difference is negligible.
_FACTS_EXTRACTION_MODEL = "gpt-4o-mini"

# Concurrency for fact extraction (lower than full_text because each call
# is already a ~2-5s OpenAI API request).
_FACTS_SEMAPHORE = asyncio.Semaphore(3)

# The 10 sector codes the fact extractor is allowed to use for affected_sectors.
# Must match REPORT_CATEGORIES exactly. Any "other" / off-topic fact is tagged
# with [] and dropped from the daily report.
_FACT_SECTOR_CODES = ["api", "cosmetic", "herbal", "veterinary", "food",
                      "feed", "capsules", "pvc", "logistics", "global_sources"]

_FACTS_SYSTEM_PROMPT = """You are a B2B market intelligence analyst extracting atomic facts from a news article.

Your output MUST be a JSON object with a single key "facts" whose value is an array of fact objects. Return ONLY the JSON, no prose, no markdown fences.

Each fact object MUST have exactly these fields:
- event_type: one of ["regulation", "sanction", "tariff", "price_move", "supply_disruption", "corporate", "geopolitical", "market_trend", "investment", "other"]
- what_happened: ONE sentence, subject-verb-object, what concretely happened. No adjectives, no speculation.
- who: short string listing the key actors (companies, countries, organizations) separated by commas. Max 100 chars.
- where: country or region where the event occurred, or "global" if worldwide. Max 50 chars.
- magnitude: concrete numeric value with unit if mentioned in the article ("+15%", "$2.3B", "500k tons", "10pp"). Use null if no numeric value is in the article. NEVER fabricate numbers.
- affected_sectors: array of sector codes from this CLOSED list, pick ALL that apply:
    ["api", "cosmetic", "herbal", "veterinary", "food", "feed", "capsules", "pvc", "logistics", "global_sources"]
  Sector meanings for a Ukrainian B2B importer:
    api             - pharmaceutical active ingredients, generics, APIs, drugmakers
    cosmetic        - cosmetic ingredients, personal care raw materials, skincare
    herbal          - herbal extracts, botanical raw materials, plant medicines
    veterinary      - veterinary drugs and their ingredients, animal pharma
    food            - food ingredients, food additives, commodities as food raw material
    feed            - feed additives, amino acids, protein sources for animal feed
    capsules        - pharmaceutical capsules, hard/soft gel capsules, dosage forms
    pvc             - PVC film, blister packaging, plastic packaging materials
    logistics       - shipping, freight, ports, trade routes, customs, supply chains
    global_sources  - wide-angle global economy / trade / sanctions stories from tier-1 outlets
                      (Reuters, Bloomberg, FT, BBC, WTO, IMF, World Bank). Use for macro news
                      that doesn't fit a specific sector but still matters for a B2B importer.
                      Tag this IN ADDITION TO the specific sector when both apply.
  If the article is NOT about any of these sectors (e.g. crypto, consumer electronics, sports), return an empty array [].
- supply_chain_impact: ONE sentence describing the concrete effect on imports/logistics/raw material availability for a Ukrainian company buying globally. Use null if not applicable.
- ukraine_relevance: one of ["high", "medium", "low", "none"]
    high   - direct impact on sourcing, pricing, or delivery for a Ukrainian importer (new tariff on their category, supplier outage, logistics route closure)
    medium - indirect but relevant (new supplier country opening, alternative sources, upstream raw material movement)
    low    - general industry news with unclear short-term impact
    none   - not relevant to a Ukrainian B2B importer (domestic political news of another country, consumer-only story)
- confidence: one of ["high", "medium", "low"]
    high   - article states the fact directly with specifics (who, when, how much)
    medium - article implies the fact or attributes it to unnamed sources
    low    - analyst speculation, forecasts, opinion pieces

STRICT RULES:
1. Extract 1 to 4 facts per article. Prefer fewer high-quality facts over many weak ones.
2. If the article has no concrete B2B-relevant facts, return {"facts": []}.
3. NEVER invent numbers, names, or dates that are not in the source text.
4. NEVER tag a sector that is not explicitly mentioned or clearly implied (e.g. don't tag "veterinary" just because the article mentions "pharma").
5. The same fact can tag multiple sectors if it genuinely affects all of them (e.g. a shipping disruption affects both "logistics" and any cargo type mentioned).
6. Output MUST be valid JSON. Escape inner quotes as \\".
"""


async def extract_facts_from_article(
    article_id: int,
    full_text: str,
    title: str,
    source_url: str,
    source_publisher: str,
) -> list[dict]:
    """
    Call gpt-4o-mini with the facts extraction prompt. Returns a list of
    fact dicts ready to be inserted into article_facts, or [] on any failure.

    Never raises — all errors are logged and swallowed.
    """
    if not aclient or not full_text or len(full_text) < 200:
        return []

    # Truncate very long articles — gpt-4o-mini has 128k context but we don't
    # need more than ~6k chars of body to extract facts, and shorter = cheaper.
    body = full_text[:6000]
    user_content = (
        f"TITLE: {title}\n"
        f"PUBLISHER: {source_publisher}\n"
        f"URL: {source_url}\n\n"
        f"ARTICLE BODY:\n{body}"
    )

    async with _FACTS_SEMAPHORE:
        try:
            response = await aclient.chat.completions.create(
                model=_FACTS_EXTRACTION_MODEL,
                temperature=0.0,  # deterministic for structured extraction
                max_tokens=1500,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": _FACTS_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_content},
                ],
            )
            raw = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"  facts extraction API error for article {article_id}: {e}")
            return []

    # Parse JSON
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  facts JSON parse error for article {article_id}: {e}")
        return []

    facts = parsed.get("facts", [])
    if not isinstance(facts, list):
        return []

    # Normalize and validate each fact
    clean_facts = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        what = (f.get("what_happened") or "").strip()
        if not what:
            continue  # required field
        sectors = f.get("affected_sectors") or []
        if not isinstance(sectors, list):
            sectors = []
        # Filter sectors to our allowed list only — model can't smuggle in "defense"
        sectors = [s for s in sectors if s in _FACT_SECTOR_CODES]

        clean_facts.append({
            "event_type":          (f.get("event_type") or "other")[:50],
            "what_happened":       what[:500],
            "who":                 (f.get("who") or "")[:200],
            "where_loc":           (f.get("where") or "")[:100],
            "magnitude":           (f.get("magnitude") or "")[:100] if f.get("magnitude") else None,
            "affected_sectors":    ",".join(sectors),  # stored as CSV for simplicity
            "supply_chain_impact": (f.get("supply_chain_impact") or "")[:500] if f.get("supply_chain_impact") else None,
            "ukraine_relevance":   (f.get("ukraine_relevance") or "low")[:10],
            "confidence":          (f.get("confidence") or "medium")[:10],
            "source_url":          source_url[:500],
            "source_publisher":    source_publisher[:100],
        })

    return clean_facts


async def extract_facts_and_store(article_id: int):
    """
    Load an article's full_text, extract facts via gpt-4o-mini, insert rows
    into article_facts, update articles.facts_status. Never raises.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, title, link, full_text, extraction_status, final_url
              FROM articles WHERE id = %s
            """,
            (article_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return

        # psycopg2 with RealDictCursor returns dict; without — tuple. Handle both.
        if isinstance(row, dict):
            art_id    = row["id"]
            title     = row.get("title") or ""
            link      = row.get("link") or ""
            full_text = row.get("full_text") or ""
            ex_status = row.get("extraction_status") or ""
            final_url = row.get("final_url") or link
        else:
            art_id, title, link, full_text, ex_status, final_url = row
            title = title or ""
            link = link or ""
            full_text = full_text or ""
            ex_status = ex_status or ""
            final_url = final_url or link

        # Skip if no usable full_text
        if ex_status != "ok" or not full_text or len(full_text) < 200:
            cursor.execute(
                "UPDATE articles SET facts_status='skipped', facts_attempted_at=NOW() WHERE id=%s",
                (art_id,),
            )
            conn.commit()
            return

        # Infer publisher from final_url domain
        publisher = "unknown"
        try:
            from urllib.parse import urlparse
            host = urlparse(final_url).netloc or ""
            publisher = host.replace("www.", "").split(".")[0].title() if host else "unknown"
        except Exception:
            pass

        facts = await extract_facts_from_article(
            art_id, full_text, title, final_url, publisher
        )

        if not facts:
            cursor.execute(
                "UPDATE articles SET facts_status='ok', facts_attempted_at=NOW() WHERE id=%s",
                (art_id,),
            )
            conn.commit()
            print(f"  ℹ no facts extracted for article {art_id} ({title[:60]})")
            return

        # Insert all facts
        for f in facts:
            cursor.execute(
                """
                INSERT INTO article_facts
                    (article_id, event_type, what_happened, who, where_loc,
                     magnitude, affected_sectors, supply_chain_impact,
                     ukraine_relevance, confidence, source_url, source_publisher)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    art_id,
                    f["event_type"], f["what_happened"], f["who"], f["where_loc"],
                    f["magnitude"], f["affected_sectors"], f["supply_chain_impact"],
                    f["ukraine_relevance"], f["confidence"],
                    f["source_url"], f["source_publisher"],
                ),
            )
        cursor.execute(
            "UPDATE articles SET facts_status='ok', facts_attempted_at=NOW() WHERE id=%s",
            (art_id,),
        )
        conn.commit()

        sectors_summary = set()
        for f in facts:
            if f["affected_sectors"]:
                sectors_summary.update(f["affected_sectors"].split(","))
        print(f"  ✓ {len(facts)} facts from article {art_id} → sectors: {sorted(sectors_summary) or 'none'}")

    except Exception as e:
        print(f"extract_facts_and_store error for article {article_id}: {e}")
        try:
            if conn:
                conn.rollback()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE articles SET facts_status='failed', facts_attempted_at=NOW() WHERE id=%s",
                    (article_id,),
                )
                conn.commit()
        except Exception:
            pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


async def backfill_missing_facts(max_articles: int = 100):
    """
    Find articles with extraction_status='ok' (have full_text) but
    facts_status='pending' (facts not yet extracted), and run extraction.

    Runs at startup + every 2.5 hours + at 08:45 before daily report.
    """
    if not aclient:
        return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        rows = db_fetchall(
            cursor,
            """
            SELECT id FROM articles
            WHERE extraction_status = 'ok'
              AND (facts_status IS NULL OR facts_status = 'pending')
              AND (published = '' OR published >= %s)
              AND category != 'market_alerts'
            ORDER BY published DESC NULLS LAST
            LIMIT %s
            """,
            (cutoff, max_articles),
        )
        conn.close()
    except Exception as e:
        print(f"facts backfill query failed: {e}")
        return

    if not rows:
        print("Facts backfill: nothing to do.")
        return

    print(f"Facts backfill: extracting facts for {len(rows)} articles...")
    await asyncio.gather(
        *[extract_facts_and_store(r["id"] if isinstance(r, dict) else r[0]) for r in rows],
        return_exceptions=True,
    )
    print(f"Facts backfill: done processing {len(rows)} articles.")


# ─────────────────────────────────────────────────────────────────
# PDF RENDERING HELPERS
# ─────────────────────────────────────────────────────────────────

FONT_REGULAR = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
FONT_BOLD    = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'

# ── Colour system — clean white/dark professional palette ──────────
COLOR_ACCENT      = (27, 79, 216)    # Blue accent for section left-border
COLOR_ACCENT_DARK = (15, 45, 130)    # Deep blue for section title text
COLOR_LIGHT       = (245, 247, 250)  # Light blue-grey for table rows
COLOR_DIVIDER     = (215, 222, 235)  # Subtle horizontal divider
COLOR_BODY        = (25, 25, 35)     # Near-black body text
COLOR_MUTED       = (110, 115, 130)  # Grey for metadata / captions
# Risk badge colours (unchanged)
RISK_COLORS   = {
    "Високий": (220, 53, 69),
    "Середній": (255, 165, 0),
    "Низький":  (40, 167, 69),
}


def make_pdf_base() -> FPDF:
    pdf = FPDF()
    pdf.add_font("DejaVu",          fname=FONT_REGULAR)
    pdf.add_font("DejaVu", style="B", fname=FONT_BOLD)
    pdf.set_margins(20, 20, 20)           # wider margins — professional standard
    pdf.set_auto_page_break(auto=True, margin=22)
    return pdf


def draw_header_bar(pdf: FPDF, report_date: str, base_dir: str):
    """White header: logo + text + subtle light divider line at bottom."""
    # White background
    pdf.set_fill_color(255, 255, 255)
    pdf.rect(0, 0, 210, 44, style="F")

    # Subtle light grey rule at the bottom of the header
    pdf.set_draw_color(*COLOR_DIVIDER)
    pdf.set_line_width(0.3)
    pdf.line(0, 43, 210, 43)

    logo_path = os.path.join(base_dir, "logo.png")
    if os.path.exists(logo_path):
        pdf.image(logo_path, x=8, y=8, h=26)
        text_x = 54
    else:
        text_x = 14

    remaining_w = 210 - text_x - 8

    # Report title — bold black
    pdf.set_xy(text_x, 9)
    pdf.set_font("DejaVu", style="B", size=14)
    pdf.set_text_color(20, 20, 20)
    pdf.cell(remaining_w, 9, "Щоденний ринковий звіт", ln=True, align="C")

    # Subtitle — medium grey
    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=8.5)
    pdf.set_text_color(90, 90, 90)
    pdf.cell(remaining_w, 6,
             f"Для B2B-компанії в Україні  ·  Огляд за {report_date}",
             ln=True, align="C")

    # Categories line — light grey
    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=7.5)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(remaining_w, 5,
             "Сировина  ·  Субстанції  ·  Логістика  ·  Близький Схід  ·  Товарні ринки",
             ln=True, align="C")

    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(14)


def section_title(pdf: FPDF, title: str):
    """Section header — clean minimalist style, no bars or fills.
    Title in uppercase bold, thin divider line below."""
    pdf.ln(2)
    pdf.set_x(pdf.l_margin)
    pdf.set_font("DejaVu", style="B", size=11)
    pdf.set_text_color(20, 20, 20)
    pdf.cell(0, 8, title.upper(), ln=True, align="L")

    # Thin light divider under the title
    pdf.set_draw_color(*COLOR_DIVIDER)
    pdf.set_line_width(0.3)
    pdf.line(pdf.l_margin, pdf.get_y(), 210 - pdf.r_margin, pdf.get_y())
    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(5)


def sub_title(pdf: FPDF, title: str):
    """Bold dark sub-heading with top spacing."""
    pdf.ln(2)
    pdf.set_x(pdf.l_margin)
    pdf.set_font("DejaVu", style="B", size=10.5)
    pdf.set_text_color(25, 25, 35)
    pdf.multi_cell(0, 6.5, title)
    pdf.set_text_color(*COLOR_BODY)
    pdf.set_x(pdf.l_margin)
    pdf.ln(1)


# Regex for inline markdown links: [text](url)
_MD_LINK_RE = re.compile(r'\[([^\]]+)\]\((https?://[^\s\)]+)\)')
# Regex for bare URLs (http/https)
_BARE_URL_RE = re.compile(r'(?<!\()(?<!\])(https?://[^\s\)\]]+)')
# Regex for **bold** — non-greedy, must have content between markers
_BOLD_RE = re.compile(r'\*\*(.+?)\*\*')


def _tokenize_line(text: str) -> list[tuple[str, str, str | None]]:
    """Split a line into tokens: (kind, content, url_or_none).
    kind is one of: 'text', 'bold', 'link', 'boldlink'.
    Handles **bold**, [text](url), and bare URLs.
    Stray unmatched '**' markers are silently dropped.
    """
    # First, mask markdown links so their URLs don't get confused with bare URLs.
    # We'll tokenize in two passes: first bold, then within each text span — links.

    segments: list[tuple[str, str]] = []  # list of (kind: 'text'|'bold', content)
    pos = 0
    for m in _BOLD_RE.finditer(text):
        if m.start() > pos:
            segments.append(("text", text[pos:m.start()]))
        segments.append(("bold", m.group(1)))
        pos = m.end()
    if pos < len(text):
        segments.append(("text", text[pos:]))

    # Remove any leftover stray '**' that wasn't part of a matched pair
    segments = [(k, c.replace("**", "")) for k, c in segments]

    # Now expand links inside each segment
    tokens: list[tuple[str, str, str | None]] = []
    for kind, content in segments:
        if not content:
            continue
        sub_pos = 0
        matches = []
        for m in _MD_LINK_RE.finditer(content):
            matches.append(("link", m.start(), m.end(), m.group(1), m.group(2)))
        for m in _BARE_URL_RE.finditer(content):
            if any(s <= m.start() < e for (_, s, e, _, _) in matches):
                continue
            matches.append(("link", m.start(), m.end(), m.group(1), m.group(1)))
        matches.sort(key=lambda x: x[1])

        for _, start, end, label, url in matches:
            if start > sub_pos:
                tokens.append((kind, content[sub_pos:start], None))
            link_kind = "boldlink" if kind == "bold" else "link"
            tokens.append((link_kind, label, url))
            sub_pos = end
        if sub_pos < len(content):
            tokens.append((kind, content[sub_pos:], None))

    return tokens


def _render_line_tokens(pdf: FPDF, tokens: list[tuple[str, str, str | None]], size: int):
    """Render a tokenized line using pdf.write()."""
    link_color = (0, 102, 204)

    for kind, content, url in tokens:
        if not content:
            continue
        is_bold = kind in ("bold", "boldlink")
        is_link = kind in ("link", "boldlink")

        style = "B" if is_bold else ""
        pdf.set_font("DejaVu", style=style, size=size)

        if is_link:
            pdf.set_text_color(*link_color)
            try:
                pdf.write(5.5, content, link=url)
            except Exception:
                try:
                    pdf.write(5.5, content)
                except Exception:
                    pass
            pdf.set_text_color(*COLOR_BODY)
        else:
            pdf.set_text_color(*COLOR_BODY)
            try:
                pdf.write(5.5, content)
            except Exception:
                pass


def _justify_line(pdf: "FPDF", words: list, width: float, line_h: float,
                  is_last: bool, color: tuple):
    """
    Render one line of already-wrapped words at current X position.
    If is_last (or single word) → left-aligned cell.
    Otherwise → space widths expanded to fill `width` exactly.
    """
    from fpdf.enums import XPos, YPos
    pdf.set_text_color(*color)
    if not words:
        pdf.ln(line_h)
        return
    if is_last or len(words) == 1:
        pdf.cell(width, line_h, " ".join(words),
                 new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        return
    total_word_w = sum(pdf.get_string_width(w) for w in words)
    gap = (width - total_word_w) / (len(words) - 1)
    for i, word in enumerate(words):
        pdf.cell(pdf.get_string_width(word), line_h, word,
                 new_x=XPos.RIGHT, new_y=YPos.TOP)
        if i < len(words) - 1:
            pdf.cell(gap, line_h, "", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.ln(line_h)


def _wrap_words(pdf: "FPDF", text: str, first_budget: float,
                full_w: float) -> list:
    """
    Word-wrap `text` into lines.
    first_budget: available width on the first line (may be less if bold prefix used).
    full_w:       available width on subsequent lines.
    Returns list of (words_list, available_width).
    """
    words = text.split()
    lines = []
    current: list = []
    current_w = 0.0
    budget = first_budget
    for word in words:
        ww = pdf.get_string_width(word + " ")
        if current and current_w + ww > budget + 0.3:
            lines.append((current, budget))
            current = [word]
            current_w = ww
            budget = full_w
        else:
            current.append(word)
            current_w += ww
    if current:
        lines.append((current, budget))
    return lines


def body_text(pdf: FPDF, text: str, size: int = 10):
    """
    Renders body text to match the reference PDF format exactly:

    • **Bold label:** body text on same line, all continuation lines start
      from the LEFT margin (no indent), right-edge justified.
    • Pure paragraphs: fully justified, last line left-aligned.
    • Numbered list items: bold number, body justified from same line.
    • Standalone **Bold heading:** (no body on same line): left-aligned bold.
    • Empty input lines → small vertical gap between paragraphs.
    • Strips leading # markdown heading markers.
    • Supports [text](url) clickable links via _render_line_tokens fallback.
    """
    from fpdf.enums import XPos, YPos

    _LINE_H    = 6.5  # mm
    _PARA_GAP  = 3    # mm — gap for explicit empty lines in source text
    _BLOCK_GAP = 4    # mm — gap added automatically after every bold-label paragraph
    _INDENT    = 7    # mm — first-line indent for every paragraph (like book/newspaper)
    _FULL_W    = 210 - pdf.l_margin - pdf.r_margin

    pdf.set_text_color(*COLOR_BODY)

    for raw in text.split("\n"):
        clean = raw.strip()
        while clean.startswith("#"):
            clean = clean[1:]
        clean = clean.strip()

        # ── Empty line → small gap ────────────────────────────────
        if not clean:
            pdf.ln(_PARA_GAP)
            continue

        # ── Numbered list  "1. body text" ────────────────────────
        lm = re.match(r'^(\d+)\.\s+(.+)$', clean)
        if lm:
            num_str = lm.group(1) + ".  "
            body    = lm.group(2)
            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", style="B", size=size)
            pdf.set_text_color(*COLOR_BODY)
            num_w = pdf.get_string_width(num_str)
            pdf.cell(num_w, _LINE_H, num_str,
                     new_x=XPos.RIGHT, new_y=YPos.TOP)
            pdf.set_font("DejaVu", size=size)
            body_budget = _FULL_W - num_w
            wrapped = _wrap_words(pdf, body, body_budget, _FULL_W)
            for i, (wds, avail) in enumerate(wrapped):
                if i > 0:
                    pdf.set_x(pdf.l_margin)
                is_last = (i == len(wrapped) - 1)
                _justify_line(pdf, wds, avail, _LINE_H, is_last, COLOR_BODY)
            pdf.set_x(pdf.l_margin)
            pdf.ln(0.5)
            continue

        tokens = _tokenize_line(clean)

        # ── Bold label + body on same line ────────────────────────
        # e.g.  **Заголовок:** body text…
        #       **Короткий висновок:** body text…
        if tokens and tokens[0][0] == "bold":
            bold_raw  = tokens[0][1].rstrip(":")
            rest_text = "".join(t[1] for t in tokens[1:]).lstrip()

            if rest_text:
                # Inline bold label → justified body, first line indented
                label_str = bold_raw + ": "
                pdf.set_font("DejaVu", style="B", size=size)
                pdf.set_text_color(20, 20, 20)
                # First line starts at l_margin + _INDENT
                # so available width on first line = _FULL_W - _INDENT - label_w
                label_w = pdf.get_string_width(label_str)

                pdf.set_font("DejaVu", size=size)
                first_budget = _FULL_W - _INDENT - label_w
                wrapped = _wrap_words(pdf, rest_text, first_budget, _FULL_W)

                for i, (wds, avail) in enumerate(wrapped):
                    is_last = (i == len(wrapped) - 1)
                    if i == 0:
                        # First line: indent + bold label + body words
                        pdf.set_x(pdf.l_margin + _INDENT)
                        pdf.set_font("DejaVu", style="B", size=size)
                        pdf.set_text_color(20, 20, 20)
                        pdf.cell(label_w, _LINE_H, label_str,
                                 new_x=XPos.RIGHT, new_y=YPos.TOP)
                        pdf.set_font("DejaVu", size=size)
                        _justify_line(pdf, wds, avail, _LINE_H, is_last, COLOR_BODY)
                    else:
                        # Continuation lines: full width from left margin
                        pdf.set_x(pdf.l_margin)
                        pdf.set_font("DejaVu", size=size)
                        _justify_line(pdf, wds, _FULL_W, _LINE_H, is_last, COLOR_BODY)
                pdf.set_x(pdf.l_margin)
                pdf.ln(_BLOCK_GAP)

            else:
                # Standalone bold heading (no body text on same line)
                # e.g. "**Практичні рекомендації:**"
                pdf.set_x(pdf.l_margin + _INDENT)
                pdf.set_font("DejaVu", style="B", size=size)
                pdf.set_text_color(20, 20, 20)
                pdf.cell(_FULL_W - _INDENT, _LINE_H, bold_raw + ":",
                         new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf.set_text_color(*COLOR_BODY)
                pdf.ln(_BLOCK_GAP)
            continue

        # ── Plain paragraph ───────────────────────────────────────
        # Also handles lines with inline links/bold via token check
        if len(tokens) == 1 and tokens[0][0] == "text":
            pdf.set_font("DejaVu", size=size)
            # First line indented, rest full width
            first_budget = _FULL_W - _INDENT
            wrapped = _wrap_words(pdf, clean, first_budget, _FULL_W)
            for i, (wds, avail) in enumerate(wrapped):
                is_last = (i == len(wrapped) - 1)
                if i == 0:
                    pdf.set_x(pdf.l_margin + _INDENT)
                    _justify_line(pdf, wds, avail, _LINE_H, is_last, COLOR_BODY)
                else:
                    pdf.set_x(pdf.l_margin)
                    _justify_line(pdf, wds, _FULL_W, _LINE_H, is_last, COLOR_BODY)
            pdf.set_x(pdf.l_margin)
        else:
            # Fallback: mixed inline bold/links — render with write(), no justify
            pdf.set_x(pdf.l_margin)
            _render_line_tokens(pdf, tokens, size)
            pdf.ln(_LINE_H)
            pdf.set_font("DejaVu", size=size)
            pdf.set_text_color(*COLOR_BODY)
            pdf.set_x(pdf.l_margin)

    pdf.ln(1)


def draw_divider(pdf: FPDF):
    """Thin subtle divider between sections."""
    pdf.ln(3)
    pdf.set_draw_color(*COLOR_DIVIDER)
    pdf.set_line_width(0.25)
    pdf.line(pdf.l_margin, pdf.get_y(), 210 - pdf.r_margin, pdf.get_y())
    pdf.ln(4)


def draw_risk_table(pdf: FPDF, lines: list[str]):
    """Красива таблиця ризиків: Категорія | Сигнал | Тип | Рівень | Дія"""
    headers  = ["Категорія", "Сигнал", "Тип ризику", "Рівень", "Дія"]
    col_w    = [35, 42, 25, 22, 56]   # сума = 180 = ширина тексту A4

    # ── шапка ──────────────────────────────────────────────────────
    pdf.set_font("DejaVu", style="B", size=8.5)
    pdf.set_fill_color(*COLOR_ACCENT)
    pdf.set_text_color(255, 255, 255)
    pdf.set_x(pdf.l_margin)
    for i, h in enumerate(headers):
        pdf.cell(col_w[i], 7, h, border=0, fill=True, align="L")
    pdf.ln()

    # ── рядки ──────────────────────────────────────────────────────
    alternate = False
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # пропускаємо роздільники ---
        if set(line.replace("|","").replace("-","").strip()) == set():
            continue
        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 2:
            continue
        # пропускаємо повтор заголовка
        if cells[0].lower() in ("категорія", "category"):
            continue

        # доповнюємо до 5 клітинок якщо менше
        while len(cells) < 5:
            cells.append("")

        pdf.set_fill_color(*(COLOR_LIGHT if alternate else (255, 255, 255)))
        alternate = not alternate
        pdf.set_x(pdf.l_margin)

        # колір рівня ризику (4-та колонка, індекс 3)
        level = cells[3] if len(cells) > 3 else ""
        risk_color = RISK_COLORS.get(level, COLOR_BODY)

        for i, cell_text in enumerate(cells[:5]):
            if i == 3:
                pdf.set_font("DejaVu", style="B", size=8)
                pdf.set_text_color(*risk_color)
            else:
                pdf.set_font("DejaVu", size=8)
                pdf.set_text_color(*COLOR_BODY)
            # обрізаємо щоб не вилізти за межу
            max_chars = int(col_w[i] / 2.1)
            pdf.cell(col_w[i], 6, cell_text[:max_chars], border=0, fill=True)
        pdf.ln()

    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(4)


def draw_footer(pdf: FPDF, report_date: str):
    """Footer with thin rule, page info, and confidentiality notice."""
    pdf.set_y(-18)
    pdf.set_draw_color(*COLOR_DIVIDER)
    pdf.set_line_width(0.25)
    pdf.line(pdf.l_margin, pdf.get_y(), 210 - pdf.r_margin, pdf.get_y())
    pdf.ln(2)
    pdf.set_font("DejaVu", size=7)
    pdf.set_text_color(*COLOR_MUTED)
    pdf.cell(0, 4.5,
             f"MacroHarvey  ·  Ринковий звіт за {report_date}  ·  Стор. {pdf.page_no()}",
             align="C")
    pdf.ln(4.5)
    pdf.set_font("DejaVu", size=6.5)
    pdf.set_text_color(170, 175, 185)
    pdf.cell(0, 4,
             "Конфіденційно. Призначено виключно для внутрішнього використання.",
             align="C")


# ─────────────────────────────────────────────────────────────────
# CHART GENERATION  (yfinance → matplotlib → PNG → PDF)
# ─────────────────────────────────────────────────────────────────

# Ticker map: key → (yfinance ticker(s), display label, unit, TradingEconomics URL, TradingView URL)
# The first ticker is primary; the rest are fallbacks (yfinance sometimes returns empty).
CHART_TICKERS = {
    "КУКУРУДЗА": {
        "tickers": ("ZC=F",),
        "label": "Кукурудза (CBOT Corn Futures)",
        "unit": "¢/bushel",
        "te_url": "https://tradingeconomics.com/commodity/corn",
        "tv_url": "https://www.tradingview.com/chart/?symbol=CBOT%3AZC1!",
        "emoji": "🌽",
    },
    "НАФТА": {
        "tickers": ("BZ=F",),
        "label": "Нафта Brent (ICE Brent Crude Futures)",
        "unit": "$/barrel",
        "te_url": "https://tradingeconomics.com/commodity/crude-oil",
        "tv_url": "https://www.tradingview.com/chart/?symbol=TVC%3AUKOIL",
        "emoji": "🛢️",
    },
    "ПАЛЬМОВА": {
        # Palm oil has spotty yfinance coverage; try multiple tickers.
        "tickers": ("POO=F", "FCPO=F", "CPO=F"),
        "label": "Пальмова олія (Crude Palm Oil)",
        "unit": "$/MT",
        "te_url": "https://tradingeconomics.com/commodity/palm-oil",
        "tv_url": "https://www.tradingview.com/chart/?symbol=MYX%3AKPO1!",
        "emoji": "🌴",
    },
}


def _make_candle_chart(tickers: tuple[str, ...], label: str, unit: str,
                       date_from: datetime.date, date_to: datetime.date,
                       out_path: str) -> tuple[bool, dict | None]:
    """
    Try multiple tickers in order until one returns data. Draw a candlestick
    chart with volume bars for a ~45-day window ending at date_to, save to out_path.
    Returns (success, price_info). price_info is a dict with close/open/high/low/change_pct
    for the report day, or None on failure.
    """
    if not CHARTS_AVAILABLE:
        return False, None

    # Try each ticker until we get non-empty data
    df = None
    used_ticker = None
    for ticker_sym in tickers:
        try:
            start = date_from - datetime.timedelta(days=45)
            end   = date_to   + datetime.timedelta(days=2)
            tk = yf.Ticker(ticker_sym)
            candidate = tk.history(start=start.isoformat(), end=end.isoformat(), interval="1d")
            if candidate is not None and not candidate.empty:
                df = candidate
                used_ticker = ticker_sym
                break
        except Exception as e:
            print(f"Chart ticker {ticker_sym} failed: {e}")
            continue

    if df is None or df.empty:
        print(f"Chart: no data for any ticker in {tickers}")
        return False, None

    try:
        df.index = df.index.tz_localize(None) if df.index.tzinfo else df.index
        # keep only up to report date
        df = df[df.index.date <= date_to]
        if df.empty:
            return False, None

        # ── figure layout ──────────────────────────────────────────
        fig, (ax_price, ax_vol) = plt.subplots(
            2, 1, figsize=(9, 4.2),
            gridspec_kw={"height_ratios": [3, 1]},
            facecolor="#FFFFFF"
        )
        for ax in (ax_price, ax_vol):
            ax.set_facecolor("#F8F9FC")
            ax.tick_params(colors="#555555", labelsize=7)
            for spine in ax.spines.values():
                spine.set_edgecolor("#DDDDDD")

        # ── line chart (TradingView style) ────────────────────────
        closes    = df["Close"].values
        x_indices = range(len(df))
        last_i     = len(df) - 1
        last_close = closes[-1]
        last_open  = df["Open"].iloc[-1]

        # Determine overall trend colour: teal if today closed above open, red if below
        overall_color = "#2962ff"   # TradingView blue — neutral line colour

        # Draw main price line
        ax_price.plot(x_indices, closes, color=overall_color,
                      linewidth=1.3, zorder=3)

        # Gradient fill under the line using LinearSegmentedColormap
        from matplotlib.colors import LinearSegmentedColormap
        import numpy as np
        y_min = closes.min()
        y_max = closes.max()
        y_range = y_max - y_min if y_max != y_min else 1.0

        # Build a vertical gradient fill: blue at line, transparent at bottom
        grad_cmap = LinearSegmentedColormap.from_list(
            "tv_fill", [(0, (0.16, 0.38, 1.0, 0.0)),
                        (1, (0.16, 0.38, 1.0, 0.18))]
        )
        # Polygon fill
        ax_price.fill_between(x_indices, closes, y_min * 0.999,
                              color="#2962ff", alpha=0.12, zorder=2)

        # ── price label on last point ──────────────────────────────
        ax_price.annotate(
            f"{last_close:.2f}",
            xy=(last_i, last_close),
            xytext=(last_i - 1.5, last_close),
            fontsize=7.5, color="#f5a623", fontweight="bold",
            ha="right", va="center",
        )

        # Horizontal dotted price line at last close
        ax_price.axhline(y=last_close, color="#f5a623",
                         linewidth=0.6, linestyle=":", alpha=0.7, zorder=1)

        # ── volume bars ────────────────────────────────────────────
        w_vol = 0.6
        vol_colors = ["#26a69a" if df["Close"].iloc[i] >= df["Open"].iloc[i]
                      else "#ef5350" for i in range(len(df))]
        ax_vol.bar(range(len(df)), df["Volume"], color=vol_colors,
                   width=w_vol, linewidth=0, alpha=0.7)
        ax_vol.set_ylabel("Обсяг", color="#666666", fontsize=6)
        ax_vol.yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda x, _: f"{x/1e6:.0f}M" if x >= 1e6 else f"{x/1e3:.0f}K"
            )
        )

        # ── x-axis: show only ~6 date labels ──────────────────────
        step = max(1, len(df) // 6)
        tick_positions = list(range(0, len(df), step))
        tick_labels    = [df.index[i].strftime("%d.%m") for i in tick_positions]
        ax_price.set_xticks([])
        ax_vol.set_xticks(tick_positions)
        ax_vol.set_xticklabels(tick_labels, color="#666666", fontsize=6)

        # ── y-axis formatting ──────────────────────────────────────
        ax_price.yaxis.tick_right()
        ax_price.yaxis.set_label_position("right")
        ax_price.set_ylabel(unit, color="#666666", fontsize=6)

        # ── title & OHLC info ──────────────────────────────────────
        last_row = df.iloc[-1]
        prev_close = df["Close"].iloc[-2] if len(df) > 1 else last_row["Close"]
        chg   = last_row["Close"] - prev_close
        chg_p = chg / prev_close * 100 if prev_close else 0
        chg_color = "#26a69a" if chg >= 0 else "#ef5350"
        chg_sign  = "+" if chg >= 0 else ""

        title_str = (
            f"{label}   "
            f"O:{last_row['Open']:.2f}  "
            f"H:{last_row['High']:.2f}  "
            f"L:{last_row['Low']:.2f}  "
            f"C:{last_row['Close']:.2f}  "
        )
        ax_price.set_title(title_str, color="#333333", fontsize=7.5,
                           loc="left", pad=4)
        # change badge in top-right
        ax_price.annotate(
            f"{chg_sign}{chg:.2f} ({chg_sign}{chg_p:.2f}%)",
            xy=(1, 1), xycoords="axes fraction",
            xytext=(-4, -4), textcoords="offset points",
            fontsize=7.5, color=chg_color, fontweight="bold",
            ha="right", va="top",
        )

        # ── report-day dot marker on the line ─────────────────────
        report_idx = len(df) - 1
        ax_price.plot(report_idx, last_close, "o",
                      color="#f5a623", markersize=5, zorder=6)

        fig.tight_layout(pad=0.4)
        fig.savefig(out_path, dpi=130, bbox_inches="tight",
                    facecolor="#FFFFFF")
        plt.close(fig)

        price_info = {
            "close":      round(float(last_row["Close"]), 2),
            "open":       round(float(last_row["Open"]),  2),
            "high":       round(float(last_row["High"]),  2),
            "low":        round(float(last_row["Low"]),   2),
            "change_abs": round(float(chg), 2),
            "change_pct": round(float(chg_p), 2),
            "unit":       unit,
            "date":       df.index[-1].strftime("%d.%m.%Y"),
            "ticker":     used_ticker,
        }
        return True, price_info
    except Exception as e:
        print(f"Chart render error: {e}")
        try:
            plt.close("all")
        except Exception:
            pass
        return False, None


def generate_all_charts(report_date: datetime.date, tmp_dir: str) -> dict[str, dict]:
    """
    Generate PNG charts for all configured commodities.
    Returns dict: key → {png_path, price_info, meta} for successfully generated charts.
    For failed generations the entry still exists but without png_path/price_info
    (so the PDF section can still render a title+link without the image).
    """
    result: dict[str, dict] = {}
    for key, cfg in CHART_TICKERS.items():
        entry = {
            "png_path":   None,
            "price_info": None,
            "label":      cfg["label"],
            "unit":       cfg["unit"],
            "te_url":     cfg["te_url"],
            "tv_url":     cfg["tv_url"],
            "emoji":      cfg["emoji"],
        }
        if CHARTS_AVAILABLE:
            out = os.path.join(tmp_dir, f"chart_{key}.png")
            ok, price_info = _make_candle_chart(
                cfg["tickers"], cfg["label"], cfg["unit"],
                report_date, report_date, out
            )
            if ok:
                entry["png_path"]   = out
                entry["price_info"] = price_info
                print(f"Chart generated: {key} → {out}  close={price_info['close']}")
            else:
                print(f"Chart skipped: {key} (no data)")
        result[key] = entry
    return result


# ─────────────────────────────────────────────────────────────────
# NEWS FETCHING FROM DB FOR REPORT
# ─────────────────────────────────────────────────────────────────

# Mapping: RSS category code → Ukrainian report category name
REPORT_CATEGORIES = [
    ("api",            "Фармацевтичні субстанції (API)"),
    ("cosmetic",       "Косметичні субстанції"),
    ("herbal",         "Трави та рослинна сировина"),
    ("veterinary",     "Ветеринарні субстанції"),
    ("food",           "Харчова сировина"),
    ("feed",           "Кормові амінокислоти"),
    ("capsules",       "Капсули"),
    ("pvc",            "ПВХ-плівка та пакування"),
    ("logistics",      "Логістика та постачання"),
    ("global_sources", "Глобальна економіка та торгівля"),
]

# Keywords to identify Middle East news in title/summary
MIDDLE_EAST_KEYWORDS = [
    "iran", "israel", "israeli", "iranian", "tehran", "middle east",
    "red sea", "hormuz", "houthi", "gaza", "lebanon", "hezbollah",
    "saudi", "syria", "iraq", "yemen", "persian gulf",
]


def fetch_facts_for_report(report_date: datetime.date, days_back: int = 3,
                           end_time: datetime.datetime | None = None,
                           window_start: datetime.date | None = None) -> dict:
    """
    Stage 2: fetch structured facts grouped by affected sector for the report day.

    Returns:
      {
        "by_category": { "api": [fact_dict, ...], "cosmetic": [...], ... },
        "middle_east": [fact_dict, ...],
        "stats": { "total": N, "by_relevance": {...}, "by_category_counts": {...} }
      }

    Each fact_dict has all the fields from article_facts plus the parent article's
    title and link (for "Джерела" section in Block 2).

    Filtering rules:
      - Primary window: articles published on the report day
        ([report_date 00:00 .. report_date 23:59] Kyiv).
        When `end_time` is provided (midday report mode), the upper bound
        becomes `end_time` instead of end-of-day.
        When `window_start` is provided (weekly mode), the lower bound is
        [window_start 00:00] instead of report_date 00:00, giving a 7-day window.
      - Multi-day fallback is disabled when end_time or window_start is set
        (the caller already defines an explicit window).
      - Drop facts with ukraine_relevance='none' — they are noise.
      - A fact tagged with multiple sectors appears in each of them (that's the point).
    """
    result = {
        "by_category": {cat: [] for cat, _ in REPORT_CATEGORIES},
        "middle_east": [],
        "stats": {"total": 0, "by_relevance": {}, "by_category_counts": {}},
    }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # window_start overrides day_start for weekly mode (7-day window)
        if window_start is not None:
            day_start = datetime.datetime.combine(window_start, datetime.time.min).strftime("%Y-%m-%d %H:%M:%S")
        else:
            day_start = datetime.datetime.combine(report_date, datetime.time.min).strftime("%Y-%m-%d %H:%M:%S")
        if end_time is not None:
            # Midday mode: clamp upper bound to "now" instead of 23:59
            day_end = end_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            day_end = datetime.datetime.combine(report_date, datetime.time.max).strftime("%Y-%m-%d %H:%M:%S")
        fallback_cutoff = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
        # Disable multi-day fallback when caller provides an explicit window
        _use_multi_day_fallback = end_time is None and window_start is None

        # Columns we need from both article_facts and articles
        select_cols = (
            "f.id, f.article_id, f.event_type, f.what_happened, f.who, f.where_loc, "
            "f.magnitude, f.affected_sectors, f.supply_chain_impact, "
            "f.ukraine_relevance, f.confidence, f.source_url, f.source_publisher, "
            "a.title, a.link, a.category AS article_category"
        )

        def run_facts_query(where_clause: str, params: tuple) -> list[dict]:
            q = (
                f"SELECT {select_cols} "
                f"FROM article_facts f "
                f"JOIN articles a ON a.id = f.article_id "
                f"WHERE {where_clause} "
                f"  AND f.ukraine_relevance != 'none' "
                f"  AND f.affected_sectors IS NOT NULL "
                f"  AND f.affected_sectors != '' "
                f"ORDER BY "
                f"  CASE f.ukraine_relevance "
                f"    WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, "
                f"  CASE f.confidence "
                f"    WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, "
                f"  f.created_at DESC "
            )
            return db_fetchall(cursor, q, params) or []

        # Primary query: facts from articles published on the report day
        primary = run_facts_query(
            "a.published != '' AND a.published >= %s AND a.published <= %s",
            (day_start, day_end),
        )

        # If the report day is sparse, fall back to last `days_back` days.
        # In midday mode the user explicitly wants "today 00:00 .. now" only,
        # and in weekly mode the window is already 7 days — no fallback needed.
        if _use_multi_day_fallback and len(primary) < 15:
            fallback = run_facts_query(
                "(a.published = '' OR a.published >= %s)",
                (fallback_cutoff,),
            )
            # Merge: primary first (priority), then fallback items not already present
            seen_ids = {f["id"] for f in primary}
            for f in fallback:
                if f["id"] not in seen_ids:
                    primary.append(f)
                    seen_ids.add(f["id"])

        # Bucket each fact into every sector it tags. A fact tagged "api,logistics"
        # appears once in "api" and once in "logistics" — that's correct: it does
        # affect both categories and the synthesizer needs to see it in both.
        seen_per_cat: dict[str, set] = {cat: set() for cat, _ in REPORT_CATEGORIES}
        for f in primary:
            sectors_str = (f.get("affected_sectors") or "").strip()
            if not sectors_str:
                continue
            sectors = [s.strip() for s in sectors_str.split(",") if s.strip()]
            for sector in sectors:
                if sector in result["by_category"]:
                    if f["id"] not in seen_per_cat[sector]:
                        result["by_category"][sector].append(f)
                        seen_per_cat[sector].add(f["id"])

        # ── Middle East facts ─────────────────────────────────────
        # Facts are collected from TWO sources:
        #   1) Facts from articles in the dedicated 'middle_east' RSS category
        #   2) Facts whose what_happened/who/where_loc mention ME keywords
        # Deduplicated by fact id, ordered by relevance+confidence+recency.
        me_facts: list[dict] = []
        me_seen: set = set()

        def add_me(rows: list[dict]):
            for r in rows:
                if r["id"] in me_seen:
                    continue
                me_seen.add(r["id"])
                me_facts.append(r)

        # 1) From dedicated middle_east category, report day
        add_me(run_facts_query(
            "a.category = 'middle_east' AND a.published != '' "
            "AND a.published >= %s AND a.published <= %s",
            (day_start, day_end),
        ))

        # 2) Same category, fallback window (daily_brief mode only — midday/weekly
        # want strictly their explicit window, no multi-day widening)
        if _use_multi_day_fallback and len(me_facts) < 5:
            add_me(run_facts_query(
                "a.category = 'middle_east' AND (a.published = '' OR a.published >= %s)",
                (fallback_cutoff,),
            ))

        # 3) Keyword matches in fact text, report day window
        if len(me_facts) < 12:
            like_conds = []
            like_params = []
            for kw in MIDDLE_EAST_KEYWORDS:
                like_conds.append(
                    "(LOWER(f.what_happened) LIKE %s OR LOWER(COALESCE(f.who,'')) LIKE %s "
                    "OR LOWER(COALESCE(f.where_loc,'')) LIKE %s)"
                )
                like_params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%"])
            kw_clause = "(" + " OR ".join(like_conds) + ") "
            kw_clause += "AND a.published != '' AND a.published >= %s AND a.published <= %s"
            like_params.extend([day_start, day_end])
            add_me(run_facts_query(kw_clause, tuple(like_params)))

        result["middle_east"] = me_facts[:15]

        # Stats for the prompt
        total = sum(len(v) for v in result["by_category"].values())
        result["stats"]["total"] = total
        result["stats"]["by_category_counts"] = {
            cat: len(result["by_category"][cat]) for cat, _ in REPORT_CATEGORIES
        }
        by_rel: dict[str, int] = {}
        for facts in result["by_category"].values():
            for f in facts:
                rel = f.get("ukraine_relevance") or "low"
                by_rel[rel] = by_rel.get(rel, 0) + 1
        result["stats"]["by_relevance"] = by_rel

        conn.close()
    except Exception as e:
        print(f"fetch_facts_for_report error: {e}")
    return result


def fetch_recent_news_for_report(report_date: datetime.date, days_back: int = 3,
                                 end_time: datetime.datetime | None = None,
                                 window_start: datetime.date | None = None) -> dict:
    """
    Fetch news from DB:
    - by_category: ALL articles per category for the report day (yesterday Kyiv).
      Fallback: up to 10 latest per category if nothing found for that day.
    - middle_east: articles mentioning Middle East keywords (any recent).

    Each article row includes full_text and extraction_status so the report
    generator can prefer real article bodies over RSS snippets.

    When `end_time` is provided (midday report mode), the primary window becomes
    [report_date 00:00 .. end_time] instead of full-day, and the per-category
    fallbacks are disabled so the report strictly reflects "today until now".
    """
    result = {"by_category": {cat: [] for cat, _ in REPORT_CATEGORIES}, "middle_east": []}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Day-of-report window: 00:00 to 23:59 of the report date
        # (or 00:00 to end_time in midday mode, or window_start..report_date for weekly)
        if window_start is not None:
            day_start = datetime.datetime.combine(window_start, datetime.time.min).strftime("%Y-%m-%d %H:%M:%S")
        else:
            day_start = datetime.datetime.combine(report_date, datetime.time.min).strftime("%Y-%m-%d %H:%M:%S")
        if end_time is not None:
            day_end = end_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            day_end = datetime.datetime.combine(report_date, datetime.time.max).strftime("%Y-%m-%d %H:%M:%S")
        # Fallback window: last `days_back` days
        fallback_cutoff = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
        # When caller provides an explicit window (midday or weekly), disable fallbacks
        _fixed_window = end_time is not None or window_start is not None

        # Columns we always want for report-building.
        # full_text and extraction_status are added by the Stage 1 migration.
        cols = "title, link, summary_en, summary_ua, full_text, extraction_status"

        # Per category — ALL articles from the report day (or fallback)
        for cat, _ in REPORT_CATEGORIES:
            # Primary: articles from exactly the report day
            rows = db_fetchall(cursor,
                f"SELECT {cols} FROM articles "
                "WHERE category = %s AND published != '' "
                "AND published >= %s AND published <= %s "
                "ORDER BY published DESC",
                (cat, day_start, day_end)
            )
            # Fallbacks are DAILY-ONLY. Midday mode explicitly wants "today 00:00 .. now";
            # if today is quiet, the category comes back empty and the synthesizer
            # will write "no new events" — which is the correct behavior.
            if not rows and not _fixed_window:
                # Fallback 1: last `days_back` days
                rows = db_fetchall(cursor,
                    f"SELECT {cols} FROM articles "
                    "WHERE category = %s AND (published = '' OR published >= %s) "
                    "ORDER BY published DESC NULLS LAST LIMIT 10",
                    (cat, fallback_cutoff)
                )
            if not rows and not _fixed_window:
                # Fallback 2: 10 latest regardless of date
                rows = db_fetchall(cursor,
                    f"SELECT {cols} FROM articles "
                    "WHERE category = %s ORDER BY id DESC LIMIT 10",
                    (cat,)
                )
            result["by_category"][cat] = rows or []

        # ── Middle East news ─────────────────────────────────────
        # Strategy (priority order):
        #   1) Articles from the dedicated `middle_east` RSS category for the report day.
        #   2) Fallback: `middle_east` category from last `days_back` days.
        #   3) Also supplement with keyword matches from other categories (same day window)
        #      to catch relevant Hormuz/Iran logistics stories indexed under `logistics` etc.
        # Results are deduplicated by link and capped at 12 items.
        me_rows: list[dict] = []
        seen_links: set[str] = set()

        def add_me_rows(rows: list[dict]):
            for r in rows or []:
                lk = (r.get("link") or "").strip()
                if not lk or lk in seen_links:
                    continue
                seen_links.add(lk)
                me_rows.append(r)

        # 1) Dedicated middle_east category — report day window
        add_me_rows(db_fetchall(cursor,
            f"SELECT {cols} FROM articles "
            "WHERE category = 'middle_east' AND published != '' "
            "AND published >= %s AND published <= %s "
            "ORDER BY published DESC",
            (day_start, day_end)
        ))

        # 2) Fallback: middle_east category — last `days_back` days (daily_brief mode only)
        if len(me_rows) < 4 and not _fixed_window:
            add_me_rows(db_fetchall(cursor,
                f"SELECT {cols} FROM articles "
                "WHERE category = 'middle_east' AND (published = '' OR published >= %s) "
                "ORDER BY published DESC NULLS LAST LIMIT 15",
                (fallback_cutoff,)
            ))

        # 3) Supplement with keyword matches from any category (report day window)
        if len(me_rows) < 12:
            like_patterns = " OR ".join(
                ["LOWER(title) LIKE %s OR LOWER(COALESCE(summary_en,'')) LIKE %s"] * len(MIDDLE_EAST_KEYWORDS)
            )
            params: list = []
            for kw in MIDDLE_EAST_KEYWORDS:
                params.extend([f"%{kw}%", f"%{kw}%"])
            # Prefer report-day results; widen to fallback window if sparse
            params_with_window = params + [day_start, day_end]
            kw_query_day = (
                f"SELECT {cols} FROM articles "
                f"WHERE ({like_patterns}) "
                f"AND published != '' AND published >= %s AND published <= %s "
                f"ORDER BY published DESC LIMIT 15"
            )
            add_me_rows(db_fetchall(cursor, kw_query_day, tuple(params_with_window)))

            if len(me_rows) < 4 and not _fixed_window:
                # Last-resort: keyword matches from last days_back days (daily_brief only)
                params_fallback = params + [fallback_cutoff]
                kw_query_fallback = (
                    f"SELECT {cols} FROM articles "
                    f"WHERE ({like_patterns}) "
                    f"AND (published = '' OR published >= %s) "
                    f"ORDER BY published DESC NULLS LAST LIMIT 15"
                )
                add_me_rows(db_fetchall(cursor, kw_query_fallback, tuple(params_fallback)))

        result["middle_east"] = me_rows[:12]

        conn.close()
    except Exception as e:
        print(f"Error fetching news for report: {e}")
    return result


# ─────────────────────────────────────────────────────────────────
# MAIN REPORT GENERATION  (prompt-based, no MapReduce)
# ─────────────────────────────────────────────────────────────────

async def generate_daily_pdf_report(mode: str = "daily_brief") -> str | None:
    """
    Generate the market intelligence PDF report.

    mode="daily_brief"  (default, 09:00 Kyiv Mon-Thu)
        Short report (Block 2 + Block 3 only, no Block 1) covering *yesterday*
        from 00:00 to 23:59 Kyiv. Uses multi-day fallbacks if yesterday is sparse.
        Uses full memo format for Block 2 (400-600 words).

    mode="midday" (14:00 Kyiv Mon-Fri)
        Short report (Block 2 + Block 3 only, no Block 1) covering *today*
        from 00:00 to the current moment. No multi-day fallback — if today
        is quiet, Block 2 explicitly says so. Uses full memo format for Block 2.

    mode="weekly" (09:00 Kyiv Fridays)
        Full report (Block 1 + Block 2 + Block 3) covering the past 7 days
        (last Friday 00:00 .. yesterday Thursday 23:59 Kyiv).
        Block 1 covers all 10 categories over the week. Block 2 is a weekly memo.

    Legacy: mode="daily" is silently aliased to "daily_brief".
    """
    if mode == "daily":
        print("generate_daily_pdf_report: legacy mode='daily' aliased to 'daily_brief'")
        mode = "daily_brief"
    if mode not in ("daily_brief", "midday", "weekly"):
        print(f"generate_daily_pdf_report: invalid mode={mode!r}, defaulting to 'daily_brief'")
        mode = "daily_brief"

    if not aclient:
        print("OpenAI API key missing")
        return None

    kyiv_tz   = pytz.timezone("Europe/Kyiv")
    now_kyiv  = datetime.datetime.now(kyiv_tz)
    yesterday = now_kyiv - datetime.timedelta(days=1)
    weekdays_ua = ["понеділок","вівторок","середа","четвер","п'ятниця","субота","неділя"]
    today_weekday_ua = weekdays_ua[now_kyiv.weekday()]

    # ── Mode-aware window parameters ──────────────────────────────
    # daily_brief: subject=yesterday, window=[yesterday 00:00..23:59], no end_time
    # midday:      subject=today,     window=[today 00:00..now],       end_time=now_naive
    # weekly:      subject=thursday,  window=[last_friday 00:00..thursday 23:59], window_start set
    fetch_end_time: datetime.datetime | None = None
    fetch_window_start: datetime.date | None = None

    if mode == "daily_brief":
        report_subject_date = yesterday
        report_date = yesterday.strftime("%d.%m.%Y")
        weekday_ua  = weekdays_ua[yesterday.weekday()]
        # fetch_end_time=None, fetch_window_start=None → normal single-day with fallback

    elif mode == "midday":
        report_subject_date = now_kyiv
        report_date = now_kyiv.strftime("%d.%m.%Y")
        weekday_ua  = weekdays_ua[now_kyiv.weekday()]
        fetch_end_time = now_kyiv.replace(tzinfo=None)

    else:  # weekly — runs on Friday, covers last Friday..Thursday
        # "Yesterday" on Friday = Thursday. Window: last Friday (7 days ago) → Thursday 23:59
        report_subject_date = yesterday  # = Thursday (last day of the week)
        last_friday = (yesterday - datetime.timedelta(days=6)).date()  # 7 days back from Thursday
        fetch_window_start = last_friday
        weekday_ua = weekdays_ua[yesterday.weekday()]
        # Display date range in header: "dd.mm – dd.mm.yyyy"
        report_date = f"{last_friday.strftime('%d.%m')} – {yesterday.strftime('%d.%m.%Y')}"

    # ── Fetch structured facts from DB (Stage 2) ──────────────────
    facts_data = fetch_facts_for_report(
        report_subject_date.date(), days_back=3,
        end_time=fetch_end_time, window_start=fetch_window_start
    )

    # Safety net: if the facts table is empty (first days after deploy, or
    # pipeline broken), fall back to the old full_text pipeline so the
    # report never comes out blank.
    news_data = None
    use_facts_path = facts_data["stats"]["total"] > 0
    if not use_facts_path:
        print("Stage 2: facts table empty for report day — falling back to Stage 1 (full_text) pipeline")
        news_data = fetch_recent_news_for_report(
            report_subject_date.date(), days_back=3,
            end_time=fetch_end_time, window_start=fetch_window_start
        )

    # ─── Format helpers ──────────────────────────────────────────

    def fmt_fact(idx: int, f: dict) -> str:
        """
        Format one fact for the LLM prompt. Compact vertical layout —
        easy for the model to parse, cheap in tokens.
        """
        et    = (f.get("event_type") or "other").strip()
        rel   = (f.get("ukraine_relevance") or "low").strip()
        conf  = (f.get("confidence") or "medium").strip()
        what  = (f.get("what_happened") or "").strip()
        who   = (f.get("who") or "").strip()
        where = (f.get("where_loc") or "").strip()
        mag   = (f.get("magnitude") or "").strip() if f.get("magnitude") else ""
        impact = (f.get("supply_chain_impact") or "").strip() if f.get("supply_chain_impact") else ""
        publisher = (f.get("source_publisher") or "").strip()

        lines = [f"  FACT {idx} [type={et} | relevance={rel} | confidence={conf} | source={publisher}]"]
        lines.append(f"    what:   {what}")
        if who:
            lines.append(f"    who:    {who}")
        if where:
            lines.append(f"    where:  {where}")
        if mag:
            lines.append(f"    magnitude: {mag}")
        if impact:
            lines.append(f"    impact: {impact}")
        return "\n".join(lines)

    # ─── Build payload: facts path (primary) or full_text path (fallback) ──

    if use_facts_path:
        # ── Block 1: facts grouped by category (WEEKLY MODE ONLY) ──
        # In daily_brief and midday modes Block 1 is suppressed entirely.
        b1_news_text = ""
        if mode == "weekly":
            b1_parts = []
            for cat_code, cat_name in REPORT_CATEGORIES:
                facts = facts_data["by_category"].get(cat_code, [])
                n_high   = sum(1 for f in facts if f.get("ukraine_relevance") == "high")
                n_medium = sum(1 for f in facts if f.get("ukraine_relevance") == "medium")
                n_low    = sum(1 for f in facts if f.get("ukraine_relevance") == "low")
                b1_parts.append(
                    f"\n[КАТЕГОРІЯ: {cat_name}] — {len(facts)} фактів "
                    f"(high:{n_high}, medium:{n_medium}, low:{n_low}):"
                )
                if facts:
                    for i, f in enumerate(facts, 1):
                        b1_parts.append(fmt_fact(i, f))
                else:
                    b1_parts.append("  (фактів не зафіксовано)")
            b1_news_text = "\n".join(b1_parts)

        # ── Block 2: middle east facts + sources ───────────────────
        me_facts = facts_data["middle_east"]
        if me_facts:
            b2_parts = [fmt_fact(i, f) for i, f in enumerate(me_facts, 1)]
            b2_news_text = "\n".join(b2_parts)

            # Deduplicate sources by parent article link
            seen_links: set = set()
            sources_lines = []
            for f in me_facts:
                link = (f.get("link") or "").strip()
                title = (f.get("title") or "").strip()
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                sources_lines.append(f"- [{title}]({link})")
            b2_sources_text = "\n".join(sources_lines) if sources_lines else "(немає джерел)"
        else:
            b2_news_text = "(Свіжих фактів про Близький Схід не знайдено)"
            b2_sources_text = "(немає джерел)"

        stats = facts_data["stats"]
        stats_line = (
            f"Всього фактів за вказаний період: {stats['total']}. "
            f"По релевантності: {stats.get('by_relevance', {})}. "
            f"По категоріях: {stats.get('by_category_counts', {})}"
        )

        # ── Hard rules for facts path — rewritten: anti-hallucination + Ukraine focus ──
        # CHANGED: stricter rules that require citing fetched article text and
        # demand deep Ukraine-specific supply-chain business impact analysis.
        _b2_hard_rules = (
            "ЖОРСТКІ ПРАВИЛА ДЛЯ БЛОКУ 2 — ПОРУШЕННЯ = НЕДІЙСНИЙ ЗВІТ:\n"
            "\n"
            "ПРАВИЛО №1 — ТІЛЬКИ ФАКТИ З НАДАНИХ СТАТЕЙ:\n"
            "  Кожне твердження повинне бути підтверджене конкретним фрагментом\n"
            "  з розділів 'СТРУКТУРОВАНІ ФАКТИ' або 'ПОВНІ ТЕКСТИ СТАТЕЙ' нижче.\n"
            "  СУВОРО ЗАБОРОНЕНО: загальні знання, шаблонні фрази без підтвердження в тексті.\n"
            "  Якщо факт або цифра відсутня — напиши 'деталі не уточнені в джерелах'. Не вигадуй.\n"
            "\n"
            "ПРАВИЛО №2 — НУЛЬ ГАЛЮЦИНАЦІЙ:\n"
            "  ГАЛЮЦИНАЦІЯ = будь-яка цифра, подія, компанія або заява, якої немає в наданих статтях.\n"
            "  Якщо вчора була одна подія, а сьогодні інша — аналіз МУСИТЬ бути різним.\n"
            "\n"
            "ПРАВИЛО №3 — ГЛИБОКИЙ АНАЛІЗ ВПЛИВУ НА УКРАЇНУ (ГОЛОВНИЙ ФОКУС):\n"
            "  Твоя аудиторія — закупівельний директор УКРАЇНСЬКОГО фармацевтичного імпортера.\n"
            "  Секція 'Що це означає конкретно для нашої компанії' МІНІМУМ 4-5 речень.\n"
            "  Вона МУСИТЬ відповідати: які АФІ/сировина під ризиком? з яких країн?\n"
            "  наскільки зросте ціна/термін? що конкретно зробити закупівлям ЗАРАЗ?\n"
            "  Не абстрактний вплив — конкретні наслідки для відділу закупівель.\n"
            "\n"
            "ПРАВИЛО №4 — ОБСЯГ І ДЕТАЛІ:\n"
            "  Блок 2 МОЖЕ БУТИ ДОВГИМ, якщо аналіз бізнес-впливу цього вимагає.\n"
            "  Поверхневий саммарі — це помилка. Пріоритет — конкретність і глибина.\n"
            "\n"
            "СТРУКТУРА MEMO (обов'язкова якщо є новини):\n"
            "  **Заголовок:** [конкретна подія з наданих статей + головний наслідок для України/ЄС]\n"
            "  **Короткий висновок (3-4 речення):** [що сталося → механізм впливу → що робити]\n"
            "  **Що сталося (4-6 речень):** [хто, що, де, коли, цифри — виключно з наданих статей]\n"
            "  **Вплив на нафту та енергетику:** [напрямок ціни + причина з джерел + вплив на собівартість АФІ]\n"
            "  **Вплив на торгівлю та логістику ЄС і України:** [фрахт, страхування, маршрути, затримки]\n"
            "  **Що це означає конкретно для нашої компанії (4-5 речень):** [які АФІ/сировина під ризиком, з яких країн, ціни, терміни, дії зараз]\n"
            "  **Практичні рекомендації (7-9 пунктів):** [конкретні дії з назвами категорій/постачальників/термінів]\n"
            "  **Прогноз на 2-3 тижні:** [конкретні сценарії на основі трендів з джерел]\n"
            "  **Фінальний висновок:** [одне речення: пріоритет дії для відділу закупівель]\n"
            "  **Джерела:** [список як надано]\n"
        )

        if mode == "daily_brief":
            user_message = (
                f"Дата звіту: {report_date} ({weekday_ua}). "
                f"Поточна дата: {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), Київ.\n"
                f"Це РАНКОВИЙ ЗВІТ — охоплює ВЧОРА з 00:00 до 23:59.\n\n"
                f"{_b2_hard_rules}\n"
                f"=== НОВИНИ З БАЗИ ДАНИХ (Близький Схід, вчора) ===\n"
                f"Всього фактів: {stats['total']}. Нижче — повний список:\n\n"
                f"{b2_news_text}\n\n"
                f"--- ДЖЕРЕЛА (скопіюй дослівно в секцію Джерела:) ---\n"
                f"{b2_sources_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши Блок 2 ВИКЛЮЧНО на основі наведених новин.\n"
                f"НЕ пиши Блок 1, Блок 3. Після Блоку 2 звіт завершується."
            )

        elif mode == "midday":
            time_str = now_kyiv.strftime("%H:%M")
            user_message = (
                f"Дата звіту: {report_date} ({today_weekday_ua}), станом на {time_str} Київ.\n"
                f"Це ПОЛУДЕННЕ ОНОВЛЕННЯ — охоплює СЬОГОДНІ з 00:00 до {time_str}.\n\n"
                f"{_b2_hard_rules}\n"
                f"=== НОВИНИ З БАЗИ ДАНИХ (Близький Схід, сьогодні до {time_str}) ===\n"
                f"Всього фактів: {stats['total']}.\n\n"
                f"{b2_news_text}\n\n"
                f"--- ДЖЕРЕЛА ---\n"
                f"{b2_sources_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши Блок 2 ВИКЛЮЧНО на основі наведених новин.\n"
                f"Якщо фактів 0 — в 'Короткому висновку': 'Станом на полудень нових подій не зафіксовано.' Решту пропусти крім 'Джерела'.\n"
                f"НЕ пиши Блок 1, Блок 3. Після Блоку 2 звіт завершується."
            )

        else:  # weekly
            # ── Facts-path B2 memo structure (used only in weekly mode, facts path) ──
            # FIX: _memo_b2_structure was previously undefined, causing a NameError
            # that silently crashed every weekly report. Defined here as a local
            # constant identical in style to _memo_b2_structure_fb below.
            _memo_b2_structure = (
                "ЖОРСТКІ ПРАВИЛА ДЛЯ БЛОКУ 2 — ПОРУШЕННЯ НЕПРИПУСТИМЕ:\n"
                "1. ВИКОРИСТОВУЙ ВИКЛЮЧНО факти з розділу ФАКТИ нижче.\n"
                "   ЗАБОРОНЕНО будь-що, чого немає в цих фактах.\n"
                "   ЗАБОРОНЕНО покладатися на загальні знання про регіон.\n"
                "2. КОЖНЕ речення в секціях 'Що сталося', 'Вплив на енергетику',\n"
                "   'Вплив на логістику', 'Що це означає для компанії' МУСИТЬ\n"
                "   спиратися на конкретний факт з наданих даних.\n"
                "   Якщо факту немає — речення не пиши взагалі.\n"
                "3. ЦИФРИ — тільки ті, що є в фактах (magnitude).\n"
                "   ЗАБОРОНЕНО вигадувати або екстраполювати будь-які цифри.\n"
                "4. АУДИТОРІЯ — українські та європейські B2B компанії-імпортери.\n"
                "   Кожен висновок МУСИТЬ відповідати на питання:\n"
                "   'Як ця конкретна подія впливає на роботу нашої компанії в Україні/ЄС?'\n"
                "5. Структура memo (всі секції обов'язкові якщо є факти):\n"
                "   **Заголовок:** [ключова подія тижня + наслідок для ЄС/України]\n"
                "   **Короткий висновок:** [що сталося за тиждень → вплив на ЄС/Україну → дія]\n"
                "   **Ключові події тижня:** [тільки факти з даних: хто, що, коли, цифри]\n"
                "   **Вплив на енергетику Європи:** [як ці події змінюють енергоринок ЄС]\n"
                "   **Вплив на торгівлю та логістику ЄС і України:** [фрахт, маршрути, затримки]\n"
                "   **Що це означає конкретно для нашої компанії:** [ціни АФІ/сировини, терміни, ризики]\n"
                "   **Практичні рекомендації:** [5-7 пунктів — конкретні дії ЗАРАЗ]\n"
                "   **Прогноз на 2-3 тижні:** [на основі трендів з наданих фактів]\n"
                "   **Фінальний висновок для керівництва:** [одне речення]\n"
                "   **Джерела:** [список як надано]\n"
            )
            user_message = (
                f"Дата звіту: {report_date} ({weekday_ua} — п'ятниця, тижневий звіт). "
                f"Поточна дата складання: {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), Київ.\n"
                f"Це ТИЖНЕВИЙ ЗВІТ — охоплює повний тиждень: {report_date}.\n\n"
                f"=== СТАТИСТИКА ПО ФАКТАХ ЗА ТИЖДЕНЬ ===\n"
                f"{stats_line}\n\n"
                f"=== СТРУКТУРОВАНІ ФАКТИ ДЛЯ БЛОКУ 1 (за 10 категоріями, огляд ТИЖНЯ) ===\n"
                f"Нижче — список АТОМАРНИХ ФАКТІВ за весь тиждень, витягнутих з реальних статей. "
                f"Кожен факт містить: що сталося, хто учасники, де, величина ефекту, вплив на ланцюги постачання, релевантність.\n"
                f"Твоє завдання — для кожної з 10 категорій написати 'Огляд тижня' (4-7 речень) — "
                f"природним аналітичним текстом українською, синтез найважливіших подій за тиждень. "
                f"НЕ виводь факти списком, НЕ згадуй 'FACT', 'relevance', 'confidence'.\n"
                f"\nКРИТИЧНО:\n"
                f"  • Використовуй ТІЛЬКИ факти з цієї категорії. Не переноси між категоріями.\n"
                f"  • НЕ додумуй деталей яких немає в фактах. Цифри і дати — тільки з поля magnitude.\n"
                f"  • Пріоритет: relevance=high > medium > low. Факти low — обережно.\n"
                f"  • Якщо для категорії 0 фактів — 'Суттєвих змін за тиждень не зафіксовано; ринок стабільний.'\n"
                f"  • Стиль — природний зв'язний абзац, агрегуй тижневий тренд, не перераховуй дні.\n"
                f"\n{b1_news_text}\n\n"
                f"=== ДАНІ ДЛЯ БЛОКУ 2 — ТИЖНЕВИЙ EXECUTIVE MEMO ПРО БЛИЗЬКИЙ СХІД ===\n"
                f"Нижче — структуровані факти про Близький Схід за весь тиждень ({report_date}). "
                f"Це ТВОЄ ЄДИНЕ ДЖЕРЕЛО ФАКТІВ для memo.\n\n"
                f"{_memo_b2_structure}\n\n"
                f"У тижневому memo замість 'Що сталося сьогодні' пиши 'Ключові події тижня'. "
                f"Узагальнюй тижневий тренд, не перераховуй дні по одному.\n\n"
                f"ФАКТИ ДЛЯ АНАЛІЗУ:\n"
                f"{b2_news_text}\n\n"
                f"--- СПИСОК ДЖЕРЕЛ ДЛЯ СЕКЦІЇ 'Джерела' ---\n"
                f"Скопіюй цей список ДОСЛІВНО в секцію 'Джерела:' Блоку 2:\n"
                f"{b2_sources_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши ТИЖНЕВИЙ ринковий звіт строго за двома блоками.\n\n"
                f"ОБОВ'ЯЗКОВО:\n"
                f"- У БЛОЦІ 1 — 10 категорій. Для кожної: Тренд тижня (одна строка), "
                f"Огляд тижня (4-7 речень природного тексту), Геополітика та торгівля (1-2 речення), "
                f"Специфіка для України (1-2 речення). БЕЗ списків фактів, БЕЗ посилань.\n"
                f"- У БЛОЦІ 2 — ТИЖНЕВИЙ EXECUTIVE MEMO (≈400-600 слів) за 9-секційною структурою.\n"
                f"- Розділяй блоки МАРКЕРАМИ 'БЛОК 1:' та 'БЛОК 2:' на окремих рядках.\n"
                f"- Блок 3 НЕ ПИШИ — додається автоматично.\n"
                f"- НЕ додавай Блок 4+, підсумки, валюти.\n"
                f"- НЕ пиши слова 'FACT', 'relevance', 'confidence', 'magnitude'.\n"
                f"Після Блоку 2 звіт завершується."
            )

    else:
        # ── FALLBACK: old full_text path (used only when facts table is empty) ──
        _B1_FULLTEXT_BUDGET = 2500
        _B1_SNIPPET_BUDGET  = 400
        _B2_FULLTEXT_BUDGET = 3000
        _B2_SNIPPET_BUDGET  = 500

        def _pick_best_body(item: dict, fulltext_budget: int, snippet_budget: int) -> tuple[str, str]:
            full   = (item.get("full_text") or "").strip()
            status = (item.get("extraction_status") or "").strip()
            if full and status == "ok":
                body = full[:fulltext_budget]
                if len(full) > fulltext_budget:
                    body += "..."
                return body, "FULLTEXT"
            snippet = (item.get("summary_en") or item.get("summary_ua") or "").strip()
            if len(snippet) > snippet_budget:
                snippet = snippet[:snippet_budget] + "..."
            return snippet, "RSS_SNIPPET"

        def fmt_news_item(idx: int, item: dict) -> str:
            title = (item.get("title") or "").strip()
            link  = (item.get("link")  or "").strip()
            body, source_tag = _pick_best_body(item, _B2_FULLTEXT_BUDGET, _B2_SNIPPET_BUDGET)
            return (
                f"  {idx}. [{source_tag}] TITLE: {title}\n"
                f"     BODY: {body}\n"
                f"     URL: {link}"
            )

        def fmt_news_item_b1(idx: int, item: dict) -> str:
            title = (item.get("title") or "").strip()
            body, source_tag = _pick_best_body(item, _B1_FULLTEXT_BUDGET, _B1_SNIPPET_BUDGET)
            return f"  - [{source_tag}] {title}\n    {body}"

        b1_news_text = ""
        if mode == "weekly":
            b1_news_parts = []
            for cat_code, cat_name in REPORT_CATEGORIES:
                items = news_data["by_category"].get(cat_code, [])
                n_full = sum(1 for it in items if (it.get("extraction_status") == "ok" and it.get("full_text")))
                n_snip = len(items) - n_full
                b1_news_parts.append(
                    f"\n[КАТЕГОРІЯ: {cat_name}] — {len(items)} новин "
                    f"({n_full} з повним текстом, {n_snip} лише RSS):"
                )
                if items:
                    for i, it in enumerate(items, 1):
                        b1_news_parts.append(fmt_news_item_b1(i, it))
                else:
                    b1_news_parts.append("  (новин не зафіксовано)")
            b1_news_text = "\n".join(b1_news_parts)

        me_items = news_data["middle_east"]
        if me_items:
            b2_news_parts = [fmt_news_item(i, it) for i, it in enumerate(me_items, 1)]
            b2_news_text = "\n".join(b2_news_parts)
        else:
            b2_news_text = "(Свіжих новин про Близький Схід не знайдено)"

        # ── Hard rules for fallback path — rewritten: upgraded anti-hallucination ──
        # CHANGED: now matches the facts-path rule strength — zero tolerance for
        # outside knowledge, mandatory 4-5 sentence Ukraine business impact block.
        _memo_b2_structure_fb = (
            "ЖОРСТКІ ПРАВИЛА ДЛЯ БЛОКУ 2 — ПОРУШЕННЯ = НЕДІЙСНИЙ ЗВІТ:\n"
            "\n"
            "ПРАВИЛО №1 — ТІЛЬКИ ФАКТИ З НАДАНИХ СТАТЕЙ:\n"
            "  Кожне твердження повинне бути підтверджене конкретним фрагментом з розділу НОВИНИ нижче.\n"
            "  СУВОРО ЗАБОРОНЕНО: загальні знання, шаблонні фрази без підтвердження в тексті.\n"
            "  Якщо факт або цифра відсутня — напиши 'деталі не уточнені в джерелах'. Не вигадуй.\n"
            "\n"
            "ПРАВИЛО №2 — НУЛЬ ГАЛЮЦИНАЦІЙ:\n"
            "  ГАЛЮЦИНАЦІЯ = будь-яка цифра, подія, компанія або заява, якої немає в наданих статтях.\n"
            "  Якщо сьогодні інші новини ніж вчора — аналіз МУСИТЬ бути різним.\n"
            "\n"
            "ПРАВИЛО №3 — ГЛИБОКИЙ АНАЛІЗ ВПЛИВУ НА УКРАЇНУ (ГОЛОВНИЙ ФОКУС):\n"
            "  Твоя аудиторія — закупівельний директор УКРАЇНСЬКОГО фармацевтичного імпортера.\n"
            "  Секція 'Що це означає конкретно для нашої компанії' МІНІМУМ 4-5 речень.\n"
            "  Вона МУСИТЬ відповідати: які АФІ/сировина під ризиком? з яких країн?\n"
            "  наскільки зросте ціна/термін? що конкретно зробити закупівлям ЗАРАЗ?\n"
            "\n"
            "ПРАВИЛО №4 — ОБСЯГ:\n"
            "  Блок 2 МОЖЕ БУТИ ДОВГИМ, якщо аналіз бізнес-впливу цього вимагає.\n"
            "  Поверхневий саммарі — це помилка. Пріоритет — конкретність і глибина.\n"
            "\n"
            "СТРУКТУРА MEMO (обов'язкова якщо є новини):\n"
            "  **Заголовок:** [конкретна подія + головний наслідок для України/ЄС]\n"
            "  **Короткий висновок (3-4 реч.):** [що сталося → вплив на ринок → дія]\n"
            "  **Що сталося (4-6 реч.):** [хто, що, де, коли, цифри — виключно з наданих статей]\n"
            "  **Вплив на нафту та енергетику:** [напрямок ціни + причина з джерел + вплив на собівартість АФІ]\n"
            "  **Вплив на торгівлю та логістику ЄС і України:** [фрахт, страхування, маршрути, затримки]\n"
            "  **Що це означає конкретно для нашої компанії (4-5 реч.):** [АФІ/сировина під ризиком, країни, ціни, терміни, дії зараз]\n"
            "  **Практичні рекомендації (7-9 пунктів):** [конкретні дії з назвами категорій/постачальників/термінів]\n"
            "  **Прогноз на 2-3 тижні:** [конкретні сценарії на основі трендів з джерел]\n"
            "  **Фінальний висновок:** [одне речення: пріоритет для відділу закупівель]\n"
            "  **Джерела:** [список як надано]\n"
        )

        if mode == "daily_brief":
            user_message = (
                f"Дата звіту: {report_date} ({weekday_ua}). "
                f"Поточна дата: {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), Київ.\n"
                f"Це РАНКОВИЙ ЗВІТ — охоплює ВЧОРА з 00:00 до 23:59.\n\n"
                f"{_memo_b2_structure_fb}\n"
                f"=== НОВИНИ З БАЗИ ДАНИХ (Близький Схід, вчора) ===\n\n"
                f"{b2_news_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши Блок 2 ВИКЛЮЧНО на основі наведених новин вище.\n"
                f"НЕ пиши Блок 1, Блок 3. Після Блоку 2 звіт завершується."
            )

        elif mode == "midday":
            time_str = now_kyiv.strftime("%H:%M")
            user_message = (
                f"Дата звіту: {report_date} ({today_weekday_ua}), станом на {time_str} Київ.\n"
                f"Це ПОЛУДЕННЕ ОНОВЛЕННЯ — охоплює СЬОГОДНІ з 00:00 до {time_str}.\n\n"
                f"{_memo_b2_structure_fb}\n"
                f"=== НОВИНИ З БАЗИ ДАНИХ (Близький Схід, сьогодні до {time_str}) ===\n\n"
                f"{b2_news_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши Блок 2 ВИКЛЮЧНО на основі наведених новин.\n"
                f"Якщо новин 0 — в 'Короткому висновку': 'Станом на полудень нових подій не зафіксовано.' Решту пропусти крім 'Джерела'.\n"
                f"НЕ пиши Блок 1, Блок 3. Після Блоку 2 звіт завершується."
            )

        else:  # weekly fallback
            user_message = (
                f"Дата звіту: {report_date} ({weekday_ua} — тижневий звіт). "
                f"Поточна дата складання: {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), Київ.\n"
                f"Це ТИЖНЕВИЙ ЗВІТ — охоплює повний тиждень: {report_date}.\n"
                f"[FALLBACK MODE: facts table empty, using raw full_text pipeline]\n\n"
                f"=== РЕАЛЬНІ НОВИНИ ЗА ТИЖДЕНЬ ДЛЯ БЛОКУ 1 (за 10 категоріями) ===\n"
                f"Твоє завдання — СИНТЕЗУВАТИ їх у єдиний аналітичний абзац 'Огляд тижня' (4-7 речень) для кожної категорії.\n"
                f"{b1_news_text}\n\n"
                f"=== НОВИНИ ДЛЯ БЛОКУ 2 — ТИЖНЕВИЙ EXECUTIVE MEMO ПРО БЛИЗЬКИЙ СХІД ===\n"
                f"Це список новин за тиждень. Це ТВОЄ ЄДИНЕ ДЖЕРЕЛО ФАКТІВ для memo.\n\n"
                f"{_memo_b2_structure_fb}\n\n"
                f"У тижневому memo 'Що сталося сьогодні' → 'Ключові події тижня'.\n\n"
                f"НОВИНИ ДЛЯ АНАЛІЗУ:\n"
                f"{b2_news_text}\n\n"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши ТИЖНЕВИЙ ринковий звіт за двома блоками.\n"
                f"БЛОК 1 — 10 категорій (Тренд тижня + Огляд тижня 4-7 речень + Геополітика + Специфіка для України).\n"
                f"БЛОК 2 — тижневий executive memo (≈400-600 слів, 9 секцій).\n"
                f"Розділяй блоки МАРКЕРАМИ 'БЛОК 1:' та 'БЛОК 2:'. Блок 3 НЕ ПИШИ.\n"
                f"Після Блоку 2 звіт завершується."
            )

    print(f"Generating prompt-based daily report for {report_date}...")

    try:
        response = await aclient.chat.completions.create(
            model="gpt-4o",
            max_tokens=8000,
            temperature=0.4,
            messages=[
                {"role": "system", "content": DAILY_REPORT_SYSTEM_PROMPT},
                {"role": "user",   "content": user_message}
            ]
        )
        report_text = response.choices[0].message.content.strip()
    except Exception as e:
        print(f"OpenAI report generation error: {e}")
        return None

    # ── Parse the blocks from model output ───────────────────────
    # In daily mode the model writes both Block 1 and Block 2; extract_block finds each.
    # In midday mode the model was explicitly told to write ONLY Block 2, so after
    # the regular extraction we override: block1 = "", block2 = full text (minus
    # any stray "=== БЛОК 2:" header the model may have prepended).
    #
    # Parser is regex-based to tolerate ALL the ways gpt-4o likes to write the
    # block header: "=== БЛОК 1 ===", "## БЛОК 1", "**Блок 1: Огляд...**",
    # "Блок 1: Огляд за категоріями", even just "БЛОК 1". Case-insensitive.
    # The old parser required an exact string match and failed silently when
    # the model used markdown-bold syntax, which caused block1 to swallow the
    # entire block2 content (Block 2 then rendered "Даних не знайдено" on a
    # fresh page while the actual Block 2 text was already printed at the
    # bottom of Block 1's last page).
    import re as _re_parse

    def _block_header_pattern(num: int) -> "_re_parse.Pattern":
        # Matches any of:
        #   === БЛОК 2 === / === БЛОК 2 / === БЛОК 2:
        #   ## БЛОК 2 / ## БЛОК 2:
        #   **Блок 2** / **Блок 2:** / **БЛОК 2: ...**
        #   Блок 2: / БЛОК 2:
        # with optional trailing " === " / ":" / title text on the same line.
        # Captures the whole line so we can remove it cleanly.
        return _re_parse.compile(
            rf"(?im)^[\s>]*"                    # start-of-line, optional whitespace
            rf"(?:={{2,4}}\s*|##\s*|\*\*\s*)?"  # optional === or ## or **
            rf"блок\s*{num}"                     # "БЛОК 2" case-insensitive
            rf"[^\n]*$"                          # rest of the line (":", "===", title, **)
        )

    def extract_block(text: str, block_num: int, next_block_num: int | None) -> str:
        """
        Return the text between "БЛОК {block_num}" header and the next-block
        header (or end of text). Tolerates any header formatting style.
        """
        start_pat = _block_header_pattern(block_num)
        m = start_pat.search(text)
        if not m:
            return ""
        chunk = text[m.end():]

        if next_block_num is not None:
            end_pat = _block_header_pattern(next_block_num)
            em = end_pat.search(chunk)
            if em:
                chunk = chunk[:em.start()]

        return chunk.strip()

    if mode in ("midday", "daily_brief"):
        # No Block 1 in these modes — strip optional block-2 header if model added it
        block1 = ""
        b2_pat = _block_header_pattern(2)
        m = b2_pat.search(report_text)
        if m:
            block2 = report_text[m.end():].strip()
        else:
            block2 = report_text.strip()
    else:  # weekly: parse both block1 and block2
        block1 = extract_block(report_text, 1, 2)
        block2 = extract_block(report_text, 2, 3)

    # Clean up leftover section headers like ": ОГЛЯД ЗА КАТЕГОРІЯМИ ==="
    # and the model-emitted section titles ("Блок 1: Огляд за категоріями",
    # "**Блок 2: Ситуація на Близькому Сході**", "Щоденний ринковий звіт")
    # that otherwise end up duplicated below the real header bar in the PDF.
    def clean_block_header(chunk: str) -> str:
        if not chunk:
            return chunk
        lines = chunk.split("\n")

        def is_redundant_title_line(line: str) -> bool:
            """True if this line should be dropped as a duplicate of the header."""
            s = line.strip()
            # Strip markdown emphasis (**, *, __, _) and outer punctuation
            s_clean = s.strip("*_ ").strip().rstrip(":").strip().rstrip("=").strip()
            if not s_clean:
                return True
            # Drop common fully-redundant headings the model likes to emit
            redundant_patterns = [
                r"^щоденний\s+ринковий\s+звіт$",
                r"^блок\s*\d+\s*[:·—-]?\s*(огляд\s+за\s+категоріями|ситуація\s+на\s+близькому\s+сході|товарні\s+ринки)?\s*$",
                r"^(огляд\s+за\s+категоріями|ситуація\s+на\s+близькому\s+сході|товарні\s+ринки)$",
            ]
            s_lower = s_clean.lower()
            for pat in redundant_patterns:
                if _re_parse.fullmatch(pat, s_lower):
                    return True
            # Drop legacy separator/heading markers
            if s.startswith((":", "===", "---")) or s in ("===", "---"):
                return True
            return False

        # Drop redundant lines from the TOP — but only contiguously: stop at
        # first real content line. We also drop a single blank line AFTER the
        # dropped header so we don't leave a gap.
        while lines and is_redundant_title_line(lines[0]):
            lines.pop(0)
        # Drop trailing "КІНЕЦЬ ЗВІТУ..." lines and empty tails
        while lines:
            last = lines[-1].strip().upper()
            if ("КІНЕЦЬ ЗВІТУ" in last) or last in ("", "---", "==="):
                lines.pop()
                continue
            break
        return "\n".join(lines).strip()

    block1 = clean_block_header(block1)
    block2 = clean_block_header(block2)

    # Log what the parser extracted so we can spot silent regressions
    # (e.g. model switched to a new header format we don't handle yet).
    print(
        f"Parser extracted: block1={len(block1)} chars, block2={len(block2)} chars "
        f"(mode={mode}, raw={len(report_text)} chars)"
    )
    if mode in ("daily_brief", "weekly") and not block2:
        print(
            "⚠ Parser warning: block2 is empty in daily mode. "
            "Raw model output first 300 chars:\n  " + report_text[:300].replace("\n", " | ")
        )

    # If markers not present — use full text as fallback content
    if not any([block1, block2]):
        if mode in ("midday", "daily_brief"):
            block2 = report_text
        else:  # weekly
            block1 = report_text

    # ── Build PDF ─────────────────────────────────────────────────
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pdf = make_pdf_base()

    # Page 1
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)

    # ── BLOCK 1: Секції по 10 категоріях (WEEKLY MODE ONLY) ────────
    # In daily_brief and midday modes Block 1 is suppressed — the first page
    # starts directly with Block 2 (Middle East memo).
    if mode == "weekly":
        section_title(pdf, "БЛОК 1  ·  Огляд тижня за категоріями")

    def render_block1_sections(pdf: FPDF, text: str):
        """Render Block 1 as per-category sections, splitting on ### headings.
        Handles bold, links, automatic page breaks. No truncation."""
        if not text:
            body_text(pdf, "Дані відсутні.")
            return

        lines = text.split("\n")

        # Defensive: drop any stray "БЛОК 2" / "БЛОК 3" header lines that may
        # have leaked into block1 (happens when the model writes the header in
        # a form the parser didn't cut on). Without this, the previous day's
        # report showed "**Блок 2: Ситуація...**" in the middle of Block 1's
        # last page, with real Block 2 content bleeding into Block 1 territory.
        _b2_hdr = _block_header_pattern(2)
        _b3_hdr = _block_header_pattern(3)
        lines = [
            ln for ln in lines
            if not _b2_hdr.fullmatch(ln.strip()) and not _b3_hdr.fullmatch(ln.strip())
        ]

        # Group lines into sections by ### headings
        sections = []
        current_title = ""
        current_body: list[str] = []

        def is_category_heading(ln: str) -> bool:
            s = ln.strip()
            if s.startswith("###"):
                return True
            # Also detect "1. Name", "2. Name" at line start (without ###)
            if len(s) >= 3 and s[0].isdigit() and s[1] in (".", ")") and s[2] == " ":
                return True
            return False

        for raw in lines:
            if is_category_heading(raw):
                if current_title or current_body:
                    sections.append((current_title, current_body))
                current_title = raw.strip().lstrip("#").strip()
                current_body = []
            else:
                current_body.append(raw)
        if current_title or current_body:
            sections.append((current_title, current_body))

        if not sections:
            body_text(pdf, text)
            return

        for title, body_lines in sections:
            # Estimate if we need a page break — rough check
            remaining = 287 - pdf.get_y()
            if remaining < 40:  # less than ~40mm left — new page
                pdf.add_page()
                draw_header_bar(pdf, report_date, base_dir)
                section_title(pdf, "БЛОК 1  ·  Огляд тижня за категоріями (продовження)")

            if title:
                sub_title(pdf, title)
            body_text(pdf, "\n".join(body_lines))
            draw_divider(pdf)

    if mode == "weekly":
        if block1:
            render_block1_sections(pdf, block1)
        else:
            body_text(pdf, "Дані відсутні.")

        # ── BLOCK 2: new page after Block 1 in weekly ─────────────
        pdf.add_page()
        draw_header_bar(pdf, report_date, base_dir)
    # In daily_brief / midday Block 2 starts on the same first page.
    section_title(pdf, "БЛОК 2  ·  Ситуація на Близькому Сході")

    if block2:
        body_text(pdf, block2)
    else:
        body_text(pdf, "Даних по Близькому Сходу не знайдено.")
        draw_divider(pdf)

    # ── BLOCK 3: Товарні ринки — графіки з yfinance + посилання ──
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)
    section_title(pdf, "БЛОК 3  ·  Товарні ринки")

    # Generate chart PNGs into a temp dir; they live until we close the PDF.
    # In midday mode we pass "today" so _make_candle_chart's upper bound is today,
    # which means Brent/Palm oil can show today's candle (if already available),
    # while CBOT Corn — which opens at 15:00 Kyiv — will still show yesterday's
    # as its last available candle. The card's price line prints the actual
    # candle date (from price_info['date']) so the user sees the real window.
    charts_tmp_dir = tempfile.mkdtemp(prefix="charts_")
    try:
        charts = generate_all_charts(report_subject_date.date(), charts_tmp_dir)
    except Exception as e:
        print(f"Chart generation failed: {e}")
        charts = {}

    # Layout constants for each commodity card
    page_w        = 210
    left_margin   = pdf.l_margin
    right_margin  = pdf.r_margin
    content_w     = page_w - left_margin - right_margin
    img_w         = min(content_w, 170)   # chart image width in mm
    # keep 9:4.2 aspect ratio from matplotlib figsize
    img_h         = img_w * (4.2 / 9.0)
    card_gap      = 4                      # vertical gap after each card

    def render_commodity_card(key: str, data: dict):
        """Render one commodity: emoji + bold name + price line + chart image + links."""
        emoji      = data.get("emoji", "")
        label      = data.get("label", key)
        unit       = data.get("unit", "")
        te_url     = data.get("te_url", "")
        tv_url     = data.get("tv_url", "")
        png_path   = data.get("png_path")
        price_info = data.get("price_info")

        # Estimate total height needed for the card
        needed_h = 8 + (img_h + 4 if png_path else 0) + 8 + card_gap
        if pdf.get_y() + needed_h > 287:
            pdf.add_page()
            draw_header_bar(pdf, report_date, base_dir)
            section_title(pdf, "БЛОК 3  ·  Товарні ринки (продовження)")

        # ── Title line: emoji + bold label ─────────────────────────
        pdf.set_x(left_margin)
        pdf.set_font("DejaVu", style="B", size=10)
        pdf.set_text_color(*COLOR_BODY)
        try:
            pdf.cell(0, 6, f"{emoji}  {label}", ln=True)
        except Exception:
            pdf.cell(0, 6, label, ln=True)

        # ── Price summary line (if data available) ─────────────────
        if price_info:
            chg_abs = price_info["change_abs"]
            chg_pct = price_info["change_pct"]
            sign    = "+" if chg_abs >= 0 else ""
            color   = (34, 139, 34) if chg_abs >= 0 else (200, 40, 40)
            pdf.set_x(left_margin)
            pdf.set_font("DejaVu", size=8.5)
            pdf.set_text_color(*color)
            summary = (
                f"Ціна закриття за {price_info['date']}: "
                f"{price_info['close']} {unit}  "
                f"({sign}{chg_abs} / {sign}{chg_pct}%)  "
                f"| O: {price_info['open']}  H: {price_info['high']}  L: {price_info['low']}"
            )
            try:
                pdf.multi_cell(0, 5, summary)
            except Exception:
                pass
            pdf.set_text_color(*COLOR_BODY)
        else:
            pdf.set_x(left_margin)
            pdf.set_font("DejaVu", size=8.5)
            pdf.set_text_color(140, 140, 140)
            try:
                pdf.multi_cell(0, 5, "Ціна: дані yfinance недоступні.")
            except Exception:
                pass
            pdf.set_text_color(*COLOR_BODY)

        # ── Chart image ────────────────────────────────────────────
        if png_path and os.path.exists(png_path):
            try:
                x = left_margin + (content_w - img_w) / 2
                y = pdf.get_y() + 1
                pdf.image(png_path, x=x, y=y, w=img_w, h=img_h)
                pdf.set_y(y + img_h + 2)
            except Exception as e:
                print(f"Failed to embed chart for {key}: {e}")

        # ── Clickable links row ────────────────────────────────────
        pdf.set_x(left_margin)
        pdf.set_font("DejaVu", size=9)
        link_color = (0, 102, 204)

        # "Повний графік на TradingEconomics"
        pdf.set_text_color(*link_color)
        try:
            pdf.write(5.5, "📊 Повний графік: ")
            pdf.write(5.5, "TradingEconomics", link=te_url)
            pdf.set_text_color(*COLOR_BODY)
            pdf.write(5.5, "   •   ")
            pdf.set_text_color(*link_color)
            pdf.write(5.5, "TradingView", link=tv_url)
        except Exception:
            pass
        pdf.set_text_color(*COLOR_BODY)
        pdf.ln(7)

        # ── Divider before next card ───────────────────────────────
        draw_divider(pdf)
        pdf.ln(card_gap - 3 if card_gap > 3 else 0)

    if charts:
        # Render in the fixed order: Corn, Brent, Palm oil
        for key in ("КУКУРУДЗА", "НАФТА", "ПАЛЬМОВА"):
            if key in charts:
                render_commodity_card(key, charts[key])
    else:
        body_text(pdf, "Дані по товарних ринках недоступні.")

    # ── Save PDF ──────────────────────────────────────────────────
    # daily:  daily_report_YYYYMMDD.pdf        (subject = yesterday)
    # midday: daily_report_YYYYMMDD_midday.pdf (subject = today)
    if mode == "midday":
        filename_suffix = "_midday"
    elif mode == "weekly":
        filename_suffix = "_weekly"
    else:
        filename_suffix = ""
    pdf_path = os.path.join(
        base_dir,
        f"daily_report_{report_subject_date.strftime('%Y%m%d')}{filename_suffix}.pdf"
    )
    pdf.output(pdf_path)
    print(f"Report saved ({mode}): {pdf_path}")

    # Clean up chart temp files (PDF is already written, safe to delete)
    try:
        import shutil
        shutil.rmtree(charts_tmp_dir, ignore_errors=True)
    except Exception:
        pass

    return pdf_path


# ─────────────────────────────────────────────────────────────────
# SEND DAILY REPORT
# ─────────────────────────────────────────────────────────────────

async def send_daily_report_to_users():
    """
    09:00 Kyiv dispatcher.
    Friday → weekly report (Block 1 + Block 2 + Block 3, 7-day window).
    Mon-Thu → daily_brief report (Block 2 + Block 3, yesterday window).
    All report types pin the message for all users (pin accumulation — option 3).
    """
    now_kyiv = datetime.datetime.now(pytz.timezone("Europe/Kyiv"))
    is_friday = now_kyiv.weekday() == 4  # 0=Mon, 4=Fri

    if is_friday:
        report_mode = "weekly"
        doc_filename = "Weekly_Report.pdf"
    else:
        report_mode = "daily_brief"
        doc_filename = "Daily_Report.pdf"

    pdf_path = await generate_daily_pdf_report(mode=report_mode)
    if not pdf_path or not os.path.exists(pdf_path):
        print(f"{report_mode} report generation skipped or failed.")
        return

    today_str = now_kyiv.strftime("%d.%m.%Y")
    if is_friday:
        caption = f"📅 Тижневий ринковий звіт за тиждень до {today_str} готовий."
    else:
        caption = f"📊 Ранковий ринковий звіт за {today_str} готовий."

    conn = get_db_connection()
    cursor = conn.cursor()
    users = db_fetchall(cursor, "SELECT chat_id FROM telegram_users")
    conn.close()

    async with httpx.AsyncClient() as client:
        for user in users:
            try:
                chat_id = user["chat_id"]
                with open(pdf_path, 'rb') as f:
                    r = await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": chat_id, "caption": caption},
                        files={"document": (doc_filename, f)}
                    )
                    if r.status_code == 200:
                        msg_data = r.json()
                        msg_id = msg_data.get("result", {}).get("message_id")
                        if msg_id:
                            await client.post(
                                f"{TELEGRAM_API_URL}/pinChatMessage",
                                json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "disable_notification": True
                                }
                            )
            except Exception as e:
                print(f"Error sending PDF to {chat_id}: {e}")

    # Also send to static admin chat IDs from env
    chat_ids = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()]
    async with httpx.AsyncClient() as client:
        for admin_chat_id in chat_ids:
            try:
                with open(pdf_path, 'rb') as f:
                    r = await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": admin_chat_id, "caption": caption},
                        files={"document": (doc_filename, f)}
                    )
                    if r.status_code == 200:
                        msg_data = r.json()
                        msg_id = msg_data.get("result", {}).get("message_id")
                        if msg_id:
                            await client.post(
                                f"{TELEGRAM_API_URL}/pinChatMessage",
                                json={
                                    "chat_id": admin_chat_id,
                                    "message_id": msg_id,
                                    "disable_notification": True
                                }
                            )
            except Exception as e:
                print(f"Error sending PDF to admin {admin_chat_id}: {e}")

    try:
        os.remove(pdf_path)
    except Exception as e:
        print(f"Failed to delete {pdf_path}: {e}")


async def send_midday_report_to_users():
    """
    14:00 Kyiv intraday update. Generates a short Block2+Block3 report
    covering "today from 00:00 to now", sends it to the same user list as
    the morning 9:00 daily report. Does NOT pin the message (unlike morning),
    since the morning report is already pinned and stays the "anchor" of the day.
    """
    pdf_path = await generate_daily_pdf_report(mode="midday")
    if not pdf_path or not os.path.exists(pdf_path):
        print("Midday report generation skipped or failed.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    users = db_fetchall(cursor, "SELECT chat_id FROM telegram_users")
    conn.close()

    now_kyiv = datetime.datetime.now(pytz.timezone("Europe/Kyiv"))
    caption = f"🕑 Полуденне оновлення станом на {now_kyiv.strftime('%H:%M')} (Блок 2 + Блок 3)."

    async with httpx.AsyncClient() as client:
        for user in users:
            try:
                chat_id = user["chat_id"]
                with open(pdf_path, 'rb') as f:
                    r = await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": chat_id, "caption": caption},
                        files={"document": ("Midday_Report.pdf", f)}
                    )
                    if r.status_code == 200:
                        msg_data = r.json()
                        msg_id = msg_data.get("result", {}).get("message_id")
                        if msg_id:
                            await client.post(
                                f"{TELEGRAM_API_URL}/pinChatMessage",
                                json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "disable_notification": True
                                }
                            )
            except Exception as e:
                print(f"Error sending midday PDF to {chat_id}: {e}")

    # Also send to static admin chat IDs from env
    chat_ids = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()]
    async with httpx.AsyncClient() as client:
        for admin_chat_id in chat_ids:
            try:
                with open(pdf_path, 'rb') as f:
                    r = await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": admin_chat_id, "caption": caption},
                        files={"document": ("Midday_Report.pdf", f)}
                    )
                    if r.status_code == 200:
                        msg_data = r.json()
                        msg_id = msg_data.get("result", {}).get("message_id")
                        if msg_id:
                            await client.post(
                                f"{TELEGRAM_API_URL}/pinChatMessage",
                                json={
                                    "chat_id": admin_chat_id,
                                    "message_id": msg_id,
                                    "disable_notification": True
                                }
                            )
            except Exception as e:
                print(f"Error sending midday PDF to admin {admin_chat_id}: {e}")

    try:
        os.remove(pdf_path)
    except Exception as e:
        print(f"Failed to delete {pdf_path}: {e}")


# ─────────────────────────────────────────────────────────────────
# MARKET ALERTS — commodity price spike notifications
# ─────────────────────────────────────────────────────────────────
# Monitors Corn / Brent / Palm Oil via yfinance every 30 minutes during
# market hours (08:00–23:00 Kyiv). When intraday move exceeds ±7%, asks
# gpt-4o-mini to explain the move using today's news in our DB, stores
# the result as a pseudo-article with category='market_alerts', and pushes
# it to Telegram subscribers.
#
# Deduplication: one alert per commodity per direction per day. A second
# spike the same day in the same direction will not retrigger (the link
# is a unique composite key). An opposite-direction spike the same day
# WILL trigger a fresh alert.

_MARKET_ALERT_THRESHOLD_PCT = 7.0  # absolute percent, either direction
_MARKET_ALERT_INTERVAL_SECONDS = 30 * 60  # check every 30 minutes
_MARKET_ALERT_NEWS_LOOKBACK_CATEGORIES = (
    "global_sources", "middle_east", "food", "logistics",
    "feed", "api",
)


def _get_intraday_price_info(tickers: tuple[str, ...]) -> dict | None:
    """
    Fetch today's open + latest price for a commodity ticker chain.
    Returns dict {open, current, change_pct, ticker, as_of} or None.

    Strategy: ask yfinance for 2 recent daily candles. The *latest* row
    is considered "today" (even if the candle hasn't closed yet — yfinance
    updates it intraday). If the market is closed, latest row is last
    close and change_pct is that row's own daily change.
    """
    if not CHARTS_AVAILABLE:
        return None

    for ticker_sym in tickers:
        try:
            tk = yf.Ticker(ticker_sym)
            df = tk.history(period="5d", interval="1d")
            if df is None or df.empty or len(df) < 1:
                continue
            last_row = df.iloc[-1]
            open_price    = float(last_row["Open"])
            current_price = float(last_row["Close"])
            if open_price <= 0:
                continue
            change_pct = (current_price - open_price) / open_price * 100
            return {
                "open":       round(open_price, 2),
                "current":    round(current_price, 2),
                "change_pct": round(change_pct, 2),
                "ticker":     ticker_sym,
                "as_of":      df.index[-1].strftime("%Y-%m-%d"),
            }
        except Exception as e:
            print(f"Intraday price fetch failed for {ticker_sym}: {e}")
            continue
    return None


async def _explain_market_move(commodity_label: str, change_pct: float,
                                current_price: float, unit: str) -> str:
    """
    Ask gpt-4o-mini to explain a commodity spike using today's news from DB.
    Returns 2-3 sentence explanation in Ukrainian, or a fallback line.
    """
    if not aclient:
        return "Причина не ідентифікована (OpenAI недоступний)."

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Use Kyiv-local "today 00:00" because published is stored as naive
        # Kyiv-time string in the DB. Server runs in UTC (Hetzner), so
        # datetime.now() without tzinfo would give the wrong window.
        kyiv_tz = pytz.timezone("Europe/Kyiv")
        today_start = datetime.datetime.now(kyiv_tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).strftime("%Y-%m-%d %H:%M:%S")
        placeholders = ",".join(["%s"] * len(_MARKET_ALERT_NEWS_LOOKBACK_CATEGORIES))
        rows = db_fetchall(
            cursor,
            f"""
            SELECT title, COALESCE(summary_en, '') AS summary_en, category
            FROM articles
            WHERE category IN ({placeholders})
              AND published >= %s
            ORDER BY published DESC
            LIMIT 30
            """,
            (*_MARKET_ALERT_NEWS_LOOKBACK_CATEGORIES, today_start),
        )
        conn.close()
    except Exception as e:
        print(f"Market alert news query failed: {e}")
        rows = []

    direction_ua = "зросла" if change_pct >= 0 else "впала"
    sign = "+" if change_pct >= 0 else ""

    if not rows:
        return (
            f"{commodity_label} {direction_ua} на {sign}{change_pct:.1f}% "
            f"(поточна {current_price} {unit}). "
            f"Свіжих новин за сьогодні в нашій базі немає, причина не ідентифікована."
        )

    news_block = "\n".join(
        f"- [{r['category']}] {r['title'][:120]}"
        + (f" — {r['summary_en'][:150]}" if r['summary_en'] else "")
        for r in rows
    )

    prompt = (
        f"Ціна {commodity_label} сьогодні {direction_ua} на {sign}{change_pct:.1f}% "
        f"(поточна {current_price} {unit}).\n\n"
        f"Нижче — новини за сьогодні з нашої бази даних:\n{news_block}\n\n"
        f"Напиши 2-3 коротких речення українською мовою про те, які з цих новин "
        f"можуть пояснити такий рух ціни. Посилайся на конкретні події. "
        f"Якщо жодна новина не пояснює рух — напиши одне речення: "
        f"'Прямої причини у новинах сьогодні не знайдено, ймовірно технічний рух ринку.'\n"
        f"НЕ вигадуй факти. НЕ цитуй новини які не в списку. Пиши природно і коротко."
    )

    try:
        response = await aclient.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=250,
            temperature=0.3,
            messages=[
                {"role": "system", "content": "Ти — короткий фінансовий аналітик. Пояснюєш рухи цін на сировину."},
                {"role": "user",   "content": prompt},
            ],
        )
        reason = response.choices[0].message.content.strip()
        return (
            f"{commodity_label} {direction_ua} на {sign}{change_pct:.1f}% "
            f"(поточна {current_price} {unit}).\n\n{reason}"
        )
    except Exception as e:
        print(f"Market alert LLM call failed: {e}")
        return (
            f"{commodity_label} {direction_ua} на {sign}{change_pct:.1f}% "
            f"(поточна {current_price} {unit})."
        )


async def _push_market_alert(title: str, body: str, link: str, emoji: str):
    """
    Store a market_alerts pseudo-article in DB and push to subscribers.
    Mirrors the push logic from fetch_and_store_news but inline, so we
    don't have to wait for the next 15-minute fetch cycle.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        now_str = datetime.datetime.now(pytz.timezone("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M:%S")
        placeholder_image = "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?q=80&w=1200&auto=format&fit=crop"
        try:
            cursor.execute(
                """
                INSERT INTO articles
                    (title, link, published, category,
                     summary_en, summary_ua, summary_ru,
                     image_url, extraction_status, facts_status)
                VALUES (%s, %s, %s, 'market_alerts', %s, %s, %s, %s, 'skipped', 'skipped')
                ON CONFLICT(link) DO NOTHING
                RETURNING id
                """,
                (title, link, now_str, body, body, body, placeholder_image),
            )
            inserted_row = cursor.fetchone()
            conn.commit()
        except Exception as e:
            print(f"Market alert INSERT failed: {e}")
            return

        if inserted_row is None:
            # Link already exists → dedup hit, do not push again
            return

        # Push to subscribed users
        try:
            users = db_fetchall(cursor,
                "SELECT chat_id, language, subscriptions, only_daily_mode FROM telegram_users"
            )
        except Exception as e:
            print(f"Market alert users query failed: {e}")
            users = []

        async with httpx.AsyncClient() as http_client:
            for user in users:
                try:
                    if user["only_daily_mode"]:
                        continue
                    chat_id = user["chat_id"]
                    subs = user["subscriptions"] or "all"
                    if subs != "all" and "market_alerts" not in subs.split(","):
                        continue

                    msg = f"{emoji} <b>{title}</b>\n\n{body}"
                    resp = await http_client.post(
                        f"{TELEGRAM_API_URL}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": msg,
                            "parse_mode": "HTML",
                        },
                    )
                    if resp.status_code == 200:
                        try:
                            cursor.execute(
                                "INSERT INTO telegram_sent (chat_id, article_link) "
                                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                                (chat_id, link),
                            )
                            conn.commit()
                        except Exception as e:
                            print(f"Market alert telegram_sent record failed: {e}")
                except Exception as e:
                    print(f"Market alert push to {user.get('chat_id')} failed: {e}")

            # Admin fan-out (same content, not tracked in telegram_sent)
            admin_chat_ids = [
                cid.strip()
                for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",")
                if cid.strip()
            ]
            for admin_chat_id in admin_chat_ids:
                try:
                    msg = f"{emoji} <b>{title}</b>\n\n{body}"
                    await http_client.post(
                        f"{TELEGRAM_API_URL}/sendMessage",
                        json={"chat_id": admin_chat_id, "text": msg, "parse_mode": "HTML"},
                    )
                except Exception as e:
                    print(f"Market alert push to admin {admin_chat_id} failed: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


async def monitor_market_alerts():
    """
    Long-running loop that checks commodity prices every 30 minutes during
    Kyiv market hours (08:00–23:00). Sends a Telegram alert when any of
    Corn / Brent / Palm Oil moves by ±7% intraday. One alert per commodity
    per direction per day (dedup by composite link).
    """
    if not CHARTS_AVAILABLE:
        print("monitor_market_alerts: yfinance unavailable, exiting")
        return

    print("monitor_market_alerts: started")
    kyiv_tz = pytz.timezone("Europe/Kyiv")

    while True:
        try:
            now_kyiv = datetime.datetime.now(kyiv_tz)
            # Only run during extended market hours: 08:00-23:00 Kyiv
            if not (8 <= now_kyiv.hour < 23):
                await asyncio.sleep(_MARKET_ALERT_INTERVAL_SECONDS)
                continue

            for key, cfg in CHART_TICKERS.items():
                try:
                    info = await asyncio.to_thread(
                        _get_intraday_price_info, cfg["tickers"]
                    )
                except Exception as e:
                    print(f"monitor_market_alerts: fetch {key} failed: {e}")
                    continue
                if info is None:
                    continue
                change_pct = info["change_pct"]
                if abs(change_pct) < _MARKET_ALERT_THRESHOLD_PCT:
                    continue

                # Dedup key: one alert per commodity per direction per day
                direction = "up" if change_pct >= 0 else "down"
                day_str = now_kyiv.strftime("%Y%m%d")
                link = f"alert://commodity/{key}/{day_str}/{direction}"

                # Quick dedup check before we spend money on LLM
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT 1 FROM articles WHERE link = %s", (link,))
                    already = cur.fetchone() is not None
                    conn.close()
                except Exception as e:
                    print(f"Market alert dedup check failed: {e}")
                    already = False
                if already:
                    continue

                label   = cfg["label"]
                unit    = cfg["unit"]
                emoji   = cfg["emoji"]
                sign    = "+" if change_pct >= 0 else ""
                title   = f"{label}: {sign}{change_pct:.1f}% сьогодні"
                body    = await _explain_market_move(label, change_pct, info["current"], unit)

                await _push_market_alert(title, body, link, emoji)
                print(f"monitor_market_alerts: pushed {key} {sign}{change_pct:.1f}%")

        except asyncio.CancelledError:
            print("monitor_market_alerts: cancelled")
            raise
        except Exception as e:
            print(f"monitor_market_alerts loop error: {e}")

        await asyncio.sleep(_MARKET_ALERT_INTERVAL_SECONDS)


# ─────────────────────────────────────────────────────────────────
# BACKGROUND TASKS (news fetching unchanged)
# ─────────────────────────────────────────────────────────────────

async def fetch_and_store_news():
    while True:
        conn = None
        # Background extraction tasks created during this iteration.
        # We gather() them at the end so we don't pile up between 15-min cycles.
        extraction_tasks: list[asyncio.Task] = []
        try:
            print("Running background task: Fetching latest news and summarizing...")
            conn = get_db_connection()
            cursor = conn.cursor()

            for category, url in RSS_FEEDS.items():
                feed = await asyncio.to_thread(feedparser.parse, url)

                # ── Freshness filter ────────────────────────────────────
                # Google News frequently ignores the `when:7d` URL param for
                # narrow queries and returns evergreen matches from 2014-2019.
                # We filter those out HERE, before dedup/insert, so stale stuff
                # never pollutes the DB and we don't waste extraction/facts quota.
                # Widened slice from 15 → 50 because the fresh stuff may be
                # buried in the middle of the feed after topical-relevance sort.
                _MAX_ARTICLE_AGE_DAYS = 7
                _now_utc = datetime.datetime.now(datetime.timezone.utc)
                _fresh_entries = []
                _stale_count = 0
                _unparseable_count = 0
                for _entry in feed.entries[:50]:
                    _pub_raw = getattr(_entry, "published", "")
                    if not _pub_raw:
                        _unparseable_count += 1
                        continue
                    try:
                        _pub_dt = email.utils.parsedate_to_datetime(_pub_raw)
                        if _pub_dt.tzinfo is None:
                            _pub_dt = _pub_dt.replace(tzinfo=datetime.timezone.utc)
                        _age_days = (_now_utc - _pub_dt).days
                    except Exception:
                        _unparseable_count += 1
                        continue
                    if _age_days > _MAX_ARTICLE_AGE_DAYS:
                        _stale_count += 1
                        continue
                    _fresh_entries.append(_entry)

                print(
                    f"  {category}: feed={len(feed.entries)} fresh={len(_fresh_entries)} "
                    f"stale={_stale_count} no_date={_unparseable_count}"
                )

                for entry in _fresh_entries:
                    title    = getattr(entry, "title", "")
                    raw_link = getattr(entry, "link", "")
                    link     = raw_link.split('?')[0] if raw_link else ""
                    link     = link.strip()

                    if not link or not title:
                        continue

                    try:
                        cursor.execute("SELECT 1 FROM articles WHERE link = %s", (link,))
                        if cursor.fetchone() is not None:
                            continue
                        cursor.execute("SELECT 1 FROM articles WHERE title = %s", (title,))
                        if cursor.fetchone() is not None:
                            continue
                    except Exception as e:
                        print(f"DB check error: {e}")
                        continue

                    raw_published = getattr(entry, "published", "")
                    try:
                        if raw_published:
                            dt = email.utils.parsedate_to_datetime(raw_published)
                            try:
                                from zoneinfo import ZoneInfo
                                dt = dt.astimezone(ZoneInfo("Europe/Kyiv"))
                            except ImportError:
                                from datetime import timezone, timedelta
                                dt = dt.astimezone(timezone(timedelta(hours=2)))
                            published = dt.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            raise ValueError("Missing published date")
                    except Exception:
                        try:
                            from zoneinfo import ZoneInfo
                            tz = ZoneInfo("Europe/Kyiv")
                        except ImportError:
                            from datetime import timezone, timedelta
                            tz = timezone(timedelta(hours=2))
                        published = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

                    description = getattr(entry, "summary", "") or getattr(entry, "description", "") or title

                    image_url = None
                    try:
                        if hasattr(entry, 'media_content') and entry.media_content:
                            image_url = entry.media_content[0].get('url')
                        elif hasattr(entry, 'enclosures') and entry.enclosures:
                            for enc in entry.enclosures:
                                if 'image' in enc.get('type', ''):
                                    image_url = enc.get('href')
                                    break
                        if not image_url and description:
                            match = re.search(r'<img[^>]+src=[\'"]([^\'"]+)[\'"]', description, re.IGNORECASE)
                            if match:
                                image_url = match.group(1)
                    except Exception as e:
                        print(f"Error parsing image: {e}")

                    if not image_url:
                        image_url = "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?q=80&w=1200&auto=format&fit=crop"

                    summaries = await generate_summary(description)
                    sum_en = summaries.get("summary_en", description)
                    sum_ua = summaries.get("summary_ua", description)
                    sum_ru = summaries.get("summary_ru", description)

                    cursor.execute('''
                        INSERT INTO articles (title, link, published, category, summary_en, summary_ua, summary_ru, image_url, extraction_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                        ON CONFLICT(link) DO NOTHING
                    ''', (title, link, published, category, sum_en, sum_ua, sum_ru, image_url))
                    conn.commit()

                    # ── Schedule full-text extraction in the background ──
                    # Fire-and-forget: extract_and_store opens its own DB connection,
                    # so it's safe even if this iteration's `conn` is closed later.
                    # The _EXTRACTION_SEMAPHORE inside extract_article_fulltext
                    # caps real parallelism at 5 concurrent fetches.
                    # NOTE: market_alerts pseudo-articles are internally generated
                    # (link is alert://, no real URL to fetch) — defensive skip,
                    # though they shouldn't reach this loop since they're not in
                    # RSS_FEEDS. good_news articles go through normal extraction.
                    if TRAFILATURA_AVAILABLE and category != "market_alerts":
                        extraction_tasks.append(
                            asyncio.create_task(extract_and_store(link))
                        )

                    try:
                        async with httpx.AsyncClient() as http_client:
                            # Internal categories (e.g. middle_east) are stored in DB
                            # for the daily report only — do not push them to Telegram subscribers.
                            if category in INTERNAL_CATEGORIES:
                                continue

                            users = db_fetchall(cursor,
                                "SELECT chat_id, language, subscriptions, only_daily_mode FROM telegram_users"
                            )
                            for user in users:
                                try:
                                    if user["only_daily_mode"]:
                                        continue
                                    chat_id = user["chat_id"]
                                    lang    = user["language"]
                                    subs    = user["subscriptions"] if user["subscriptions"] else "all"

                                    if subs != "all":
                                        if category not in subs.split(","):
                                            continue

                                    cursor.execute(
                                        "SELECT 1 FROM telegram_sent WHERE chat_id = %s AND article_link = %s",
                                        (chat_id, link)
                                    )
                                    if cursor.fetchone() is not None:
                                        continue

                                    summary_text = summaries.get(f"summary_{lang}", sum_en)
                                    msg = _build_tg_msg(title, summary_text, category, link)

                                    resp = await http_client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": msg,
                                        "parse_mode": "HTML"
                                    })

                                    if resp.status_code == 200:
                                        cursor.execute(
                                            "INSERT INTO telegram_sent (chat_id, article_link) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                                            (chat_id, link)
                                        )
                                        conn.commit()
                                except Exception as e:
                                    print(f"Error sending to DB user {user['chat_id']}: {e}")

                            chat_ids = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()]
                            for admin_chat_id in chat_ids:
                                try:
                                    cursor.execute(
                                        "SELECT 1 FROM telegram_sent WHERE chat_id = %s AND article_link = %s",
                                        (int(admin_chat_id), link)
                                    )
                                    if cursor.fetchone() is not None:
                                        continue

                                    msg = _build_tg_msg(title, sum_ua, category, link)
                                    resp = await http_client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": admin_chat_id,
                                        "text": msg,
                                        "parse_mode": "HTML"
                                    })
                                    if resp.status_code == 200:
                                        cursor.execute(
                                            "INSERT INTO telegram_sent (chat_id, article_link) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                                            (int(admin_chat_id), link)
                                        )
                                        conn.commit()
                                except Exception as e:
                                    print(f"Error sending to static chat_id {admin_chat_id}: {e}")
                    except Exception as e:
                        print(f"Error broadcasting to Telegram: {e}")

            print("Successfully updated news database.")
        except Exception as e:
            print(f"Error fetching news: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as ce:
                    print(f"Error closing DB connection: {ce}")

        # ── Wait for background extraction tasks to complete ────────
        # They run concurrently (capped at 5 by _EXTRACTION_SEMAPHORE)
        # so this typically adds 10-60 seconds, not minutes.
        # Use wait_for with a hard timeout to prevent runaway hangs.
        if extraction_tasks:
            print(f"Waiting for {len(extraction_tasks)} extraction tasks to finish...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*extraction_tasks, return_exceptions=True),
                    timeout=180.0,  # 3 minutes hard cap
                )
            except asyncio.TimeoutError:
                print("Extraction tasks exceeded 180s timeout — cancelling stragglers")
                for t in extraction_tasks:
                    if not t.done():
                        t.cancel()
            print("Extraction batch complete.")

        await asyncio.sleep(900)


async def cleanup_old_news():
    while True:
        try:
            print("Running cleanup_old_news: Deleting articles older than 30 days...")
            cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM articles WHERE published != '' AND published < %s", (cutoff_date,))
            deleted_count = cursor.rowcount
            cursor.execute("DELETE FROM telegram_sent WHERE sent_at < %s", (cutoff_date,))
            sent_deleted = cursor.rowcount
            conn.commit()
            conn.close()
            print(f"Cleanup finished. Deleted {deleted_count} old articles, {sent_deleted} sent records.")
        except Exception as e:
            print(f"Error during cleanup_old_news: {e}")

        await asyncio.sleep(86400)


# ─────────────────────────────────────────────────────────────────
# APP STARTUP
# ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task_news    = asyncio.create_task(fetch_and_store_news())
    task_tg      = asyncio.create_task(poll_telegram_updates())
    task_cleanup = asyncio.create_task(cleanup_old_news())

    # Stage 1: backfill full_text for any articles left in extraction_status='pending'
    # (e.g. articles added before the Stage 1 migration, previous extraction failures
    # that were just reset by init_db, or overnight additions). Runs once at startup
    # without blocking — fire-and-forget.
    task_backfill = asyncio.create_task(backfill_missing_full_text(max_articles=150))

    # Stage 2: backfill facts extraction for any articles with full_text but no facts yet.
    # Same fire-and-forget pattern.
    task_backfill_facts = asyncio.create_task(backfill_missing_facts(max_articles=100))

    # Market alerts monitor: long-running loop that checks commodity prices
    # every 30 minutes during Kyiv market hours and pushes Telegram alerts
    # on ±7% intraday moves. Dedup is per-commodity per-direction per-day.
    task_market_alerts = asyncio.create_task(monitor_market_alerts())

    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Europe/Kyiv'))
    # FIX: Added day_of_week='mon-fri' to prevent spurious Saturday/Sunday triggers.
    # send_daily_report_to_users already switches to 'weekly' mode on Fridays
    # (is_friday check) and 'daily_brief' Mon-Thu, so one cron rule is sufficient.
    scheduler.add_job(
        send_daily_report_to_users, 'cron',
        day_of_week='mon-fri', hour=9, minute=0,
        id='morning_report'
    )
    # Midday intraday update at 14:00 Kyiv time (Block 2 + Block 3 only,
    # window = today 00:00 .. now). Same recipients as the morning report.
    # Restricted to Mon-Fri — no need for weekend midday updates.
    scheduler.add_job(
        send_midday_report_to_users, 'cron',
        day_of_week='mon-fri', hour=14, minute=0,
        id='midday_report'
    )
    # Full-text backfill runs every 2 hours so a fresh batch of 150 articles
    # gets processed continuously. At 8:30 specifically we still want it to
    # run right before the report, regardless of the 2h cadence.
    scheduler.add_job(backfill_missing_full_text, 'interval', hours=2, id='backfill_full_text')
    scheduler.add_job(backfill_missing_full_text, 'cron', hour=8, minute=30, id='backfill_full_text_morning')
    # Same pre-report backfill before the midday report: 13:30 gives
    # extraction 30 minutes to catch up before facts-extraction at 13:45.
    scheduler.add_job(backfill_missing_full_text, 'cron', hour=13, minute=30, id='backfill_full_text_midday')
    # Stage 2: facts extraction backfill — runs every 2 hours offset by 1h from
    # the full_text backfill, and at 8:45 (15 min before report) to catch any
    # facts for articles whose full_text just landed.
    scheduler.add_job(backfill_missing_facts, 'interval', hours=2, minutes=30, id='backfill_facts')
    scheduler.add_job(backfill_missing_facts, 'cron', hour=8, minute=45, id='backfill_facts_morning')
    # Same pattern before the midday report: 13:45 = 15 min before 14:00.
    scheduler.add_job(backfill_missing_facts, 'cron', hour=13, minute=45, id='backfill_facts_midday')
    scheduler.start()

    yield

    scheduler.shutdown()
    task_news.cancel()
    task_tg.cancel()
    task_cleanup.cancel()
    task_backfill.cancel()
    task_backfill_facts.cancel()
    task_market_alerts.cancel()


app = FastAPI(title="Alliance News API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=FileResponse)
async def read_index():
    base_dir   = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, "index.html")
    return FileResponse(index_path)


@app.get("/news")
def get_all_news():
    conn   = get_db_connection()
    cursor = conn.cursor()
    # Exclude internal/service categories from the public feed
    internal_list = list(INTERNAL_CATEGORIES)
    if internal_list:
        placeholders = ",".join(["%s"] * len(internal_list))
        rows = db_fetchall(cursor,
            f"SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url "
            f"FROM articles WHERE category NOT IN ({placeholders}) "
            f"ORDER BY published DESC LIMIT 1000",
            tuple(internal_list)
        )
    else:
        rows = db_fetchall(cursor,
            "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url "
            "FROM articles ORDER BY published DESC LIMIT 1000"
        )
    conn.close()
    return rows


@app.get("/alerts")
def get_latest_alerts():
    conn   = get_db_connection()
    cursor = conn.cursor()
    internal_list = list(INTERNAL_CATEGORIES)
    if internal_list:
        placeholders = ",".join(["%s"] * len(internal_list))
        rows = db_fetchall(cursor,
            f"SELECT title, link, published FROM articles "
            f"WHERE category NOT IN ({placeholders}) "
            f"ORDER BY published DESC LIMIT 5",
            tuple(internal_list)
        )
    else:
        rows = db_fetchall(cursor,
            "SELECT title, link, published FROM articles ORDER BY published DESC LIMIT 5"
        )
    conn.close()

    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Kyiv")
    except ImportError:
        from datetime import timezone, timedelta
        tz = timezone(timedelta(hours=2))

    now     = datetime.datetime.now(tz)
    results = []
    for r in rows:
        pub_str = r["published"]
        dt_obj  = None
        if pub_str:
            try:
                dt_obj = datetime.datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
            except ValueError:
                try:
                    dt_obj = email.utils.parsedate_to_datetime(pub_str).astimezone(tz)
                except Exception:
                    pass

        if not dt_obj:
            dt_obj = now

        diff = (now - dt_obj).total_seconds()
        if 0 <= diff < 3600:
            mins = int(diff / 60)
            display_time = "Just now" if mins <= 1 else f"{mins} mins ago"
        else:
            display_time = dt_obj.strftime("%H:%M")

        r["display_time"] = display_time
        r["published"]    = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        results.append(r)

    return results


@app.get("/news/{category}")
def get_category_news(category: str):
    if category not in RSS_FEEDS:
        raise HTTPException(status_code=404, detail="Category not found")

    conn   = get_db_connection()
    cursor = conn.cursor()
    rows   = db_fetchall(cursor,
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url "
        "FROM articles WHERE category = %s ORDER BY published DESC LIMIT 15",
        (category,)
    )
    conn.close()
    return rows


@app.get("/generate_report")
async def trigger_report():
    """HTTP endpoint to manually trigger morning daily_brief (Mon-Thu) report."""
    pdf_path = await generate_daily_pdf_report(mode="daily_brief")
    if pdf_path and os.path.exists(pdf_path):
        return FileResponse(pdf_path, media_type="application/pdf", filename="Daily_Report.pdf")
    raise HTTPException(status_code=500, detail="Report generation failed")


@app.get("/generate_weekly")
async def trigger_weekly_report():
    """HTTP endpoint to manually trigger the weekly Friday report (Block 1+2+3, 7-day window)."""
    pdf_path = await generate_daily_pdf_report(mode="weekly")
    if pdf_path and os.path.exists(pdf_path):
        return FileResponse(pdf_path, media_type="application/pdf", filename="Weekly_Report.pdf")
    raise HTTPException(status_code=500, detail="Weekly report generation failed")


@app.get("/generate_midday")
async def trigger_midday_report():
    """
    HTTP endpoint to manually trigger the 14:00 midday report.
    Generates a Block2+Block3 PDF covering today 00:00 .. now.
    """
    pdf_path = await generate_daily_pdf_report(mode="midday")
    if pdf_path and os.path.exists(pdf_path):
        return FileResponse(pdf_path, media_type="application/pdf", filename="Midday_Report.pdf")
    raise HTTPException(status_code=500, detail="Midday report generation failed")