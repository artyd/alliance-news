import asyncio
import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
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
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS title_ua TEXT")
    cursor.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS title_ru TEXT")
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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS digest_reports (
            id SERIAL PRIMARY KEY,
            report_type TEXT NOT NULL,
            title TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            pdf_data BYTEA
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_digest_reports_created ON digest_reports(created_at DESC)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracked_shipments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            number VARCHAR(60) NOT NULL,
            carrier VARCHAR(50) NOT NULL DEFAULT 'auto',
            type VARCHAR(20) NOT NULL DEFAULT 'parcel',
            carrier_name VARCHAR(150) DEFAULT '',
            status_text TEXT DEFAULT '',
            tracking_url TEXT DEFAULT '',
            steps_json TEXT DEFAULT '',
            is_delivered BOOLEAN DEFAULT FALSE,
            added_at TIMESTAMPTZ DEFAULT NOW(),
            delivered_at TIMESTAMPTZ,
            last_checked TIMESTAMPTZ,
            UNIQUE(user_id, number)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tsv_user ON tracked_shipments(user_id, is_delivered)')
    # Migration: add steps_json if missing (safe on existing DBs)
    try:
        cursor.execute("ALTER TABLE tracked_shipments ADD COLUMN IF NOT EXISTS steps_json TEXT DEFAULT ''")
        conn.commit()
    except Exception:
        conn.rollback()

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
    # Pharma active ingredients: price moves, shortages, API manufacturing news.
    # Sources include pharma trade press (pharmiweb, fiercepharma, icis) + Google News.
    # NOTE: "API price" removed — it matches OpenAI/tech API pricing. Use specific pharma terms only.
    "api": (
        "https://news.google.com/rss/search?q=%22active+pharmaceutical+ingredient%22+OR+"
        "%22pharma+raw+material%22+OR+%22pharmaceutical+raw+material+price%22+OR+"
        "%22drug+substance+supply%22+OR+%22drug+shortage%22+OR+%22generic+drug+supply+chain%22+OR+"
        "%22CDMO%22+OR+%22pharmaceutical+manufacturer%22+OR+%22bulk+drug+substance%22+OR+"
        "(site:pharmiweb.com)+OR+(site:fiercepharma.com)+OR+(site:drugchannels.net)"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Cosmetic ingredients: raw material prices, new regulations (EU Cosmetics), brand launches.
    "cosmetic": (
        "https://news.google.com/rss/search?q=%22cosmetic+ingredients%22+OR+"
        "%22personal+care+raw+materials%22+OR+%22cosmetic+regulation%22+OR+"
        "%22skincare+ingredients%22+OR+(site:cosmeticsdesign.com)+OR+(site:cosmeticsandtoiletries.com)"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Herbal extracts: harvest outlooks, export restrictions, demand from nutraceuticals.
    "herbal": (
        "https://news.google.com/rss/search?q=%22botanical+extracts%22+OR+"
        "%22herbal+extract+price%22+OR+%22plant+extract+supply%22+OR+"
        "%22medicinal+herbs%22+OR+(site:nutraceuticalsworld.com)+OR+(site:naturalproductsinsider.com)"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Veterinary pharma: regulatory approvals, API availability, disease outbreaks.
    "veterinary": (
        "https://news.google.com/rss/search?q=%22veterinary+pharmaceuticals%22+OR+"
        "%22animal+health+ingredients%22+OR+%22veterinary+API%22+OR+"
        "%22livestock+medicine%22+OR+(site:vetscite.co)+OR+(site:animalhealthmedia.com)"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Food ingredients: commodity price moves, food-grade additives, supply disruptions.
    # Focused on B2B ingredient sourcing — NOT consumer food/restaurant/waste news.
    "food": (
        "https://news.google.com/rss/search?q=%22food+ingredients%22+OR+"
        "%22food+additive+supply%22+OR+%22food+grade+ingredient%22+OR+"
        "%22food+ingredient+price%22+OR+%22food+additive+manufacturer%22+OR+"
        "%22citric+acid+price%22+OR+%22ascorbic+acid+price%22+OR+%22food+ingredient+shortage%22+OR+"
        "(site:foodingredientsfirst.com)+OR+(site:foodnavigator.com)+OR+(site:ingredients-network.com)"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Feed amino acids: lysine, methionine, threonine price and supply from China/EU.
    "feed": (
        "https://news.google.com/rss/search?q=lysine+price+OR+methionine+price+OR+"
        "threonine+price+OR+%22feed+amino+acids%22+OR+%22soybean+meal+price%22+OR+"
        "%22feed+additives+supply%22+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Pharmaceutical capsules: gelatin prices, HPMC capacity, excipient supply.
    "capsules": (
        "https://news.google.com/rss/search?q=%22hard+gelatin+capsule%22+OR+"
        "%22HPMC+capsule%22+OR+%22pharmaceutical+excipients%22+OR+"
        "%22gelatin+price%22+OR+%22capsule+manufacturer%22"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # PVC film & blister packaging: polymer prices, packaging regulations, supply.
    "pvc": (
        "https://news.google.com/rss/search?q=%22PVC+film%22+OR+"
        "%22blister+packaging%22+OR+%22pharmaceutical+packaging+material%22+OR+"
        "%22PVC+price%22+OR+%22polymer+packaging%22"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Logistics: freight rates, port congestion, route disruptions — very fresh (3d).
    "logistics": (
        "https://news.google.com/rss/search?q=%22ocean+freight+rates%22+OR+"
        "%22container+shipping%22+OR+%22supply+chain+disruption%22+OR+"
        "%22port+congestion%22+OR+%22air+cargo+rates%22+OR+(site:theloadstar.com)"
        "+when:3d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Tier-1 macro/trade news from Reuters, Bloomberg, FT, WTO, IMF etc.
    "global_sources": (
        f"https://news.google.com/rss/search?q=%22global+trade%22+OR+tariffs+OR+"
        f"sanctions+OR+%22supply+chain%22+OR+%22trade+war%22+{GLOBAL_SOURCES}"
        f"+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Middle East geopolitics — internal category for daily report Block 2 only.
    "middle_east": (
        "https://news.google.com/rss/search?q=Iran+OR+Israel+OR+%22Red+Sea%22+OR+"
        "Hormuz+OR+Houthi+OR+Gaza+OR+%22Persian+Gulf%22"
        "+when:5d&hl=en-US&gl=US&ceid=US:en"
    ),
    # Good news — uplifting stories to boost morale. Freshest possible (2d).
    "good_news": (
        "https://news.google.com/rss/search?q=(site:goodnewsnetwork.org+OR+"
        "site:positive.news+OR+site:reasonstobecheerful.world+OR+"
        "site:goodnews.com+OR+%22rescued%22+OR+%22breakthrough%22+OR+"
        "%22record+achievement%22+OR+%22uplifting+story%22)"
        "+when:2d&hl=en-US&gl=US&ceid=US:en"
    ),
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
_CAT_LABEL_RU = {
    "api":            "Фармацевтические субстанции",
    "cosmetic":       "Косметика и сырьё",
    "herbal":         "Травы и экстракты",
    "veterinary":     "Ветеринария",
    "food":           "Пищевое сырьё",
    "feed":           "Кормовые аминокислоты",
    "capsules":       "Капсулы и оболочки",
    "pvc":            "ПВХ и упаковка",
    "logistics":      "Логистика",
    "global_sources": "Глобальная экономика",
    "good_news":      "Позитивные новости",
    "market_alerts":  "Рыночный алерт",
}
_CAT_LABEL_EN = {
    "api":            "Pharma API",
    "cosmetic":       "Cosmetics & Raw Materials",
    "herbal":         "Herbal & Extracts",
    "veterinary":     "Veterinary",
    "food":           "Food Ingredients",
    "feed":           "Feed Amino Acids",
    "capsules":       "Capsules & Shells",
    "pvc":            "PVC & Packaging",
    "logistics":      "Logistics",
    "global_sources": "Global Economy",
    "good_news":      "Good News",
    "market_alerts":  "Market Alert",
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


def _build_tg_msg(title: str, summary: str, category: str, link: str,
                  lang: str = "ua", title_ua: str = "", title_ru: str = "") -> str:
    """Build a structured Telegram news message with language-aware title and category label."""
    _cat_labels = {"ua": _CAT_LABEL_UA, "ru": _CAT_LABEL_RU, "en": _CAT_LABEL_EN}
    label   = _cat_labels.get(lang, _CAT_LABEL_UA).get(category, category.upper())
    emoji   = _CAT_EMOJI.get(category, "📰")
    hashtag = _CAT_HASHTAG.get(category, f"#{category}")
    divider = "━━━━━━━━━━━━━━━━━"
    if lang == "ru":
        display_title = title_ru or title
        read_more = "Читать полностью"
    elif lang == "en":
        display_title = title
        read_more = "Read more"
    else:
        display_title = title_ua or title
        read_more = "Читати повністю"
    return (
        f"{emoji} <b>{label}</b>  {hashtag}\n"
        f"{divider}\n\n"
        f"📰 <b>{display_title}</b>\n\n"
        f"✍️ <i>{summary}</i>\n\n"
        f"🔗 <a href=\"{link}\">{read_more}</a>"
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
                                    lang_row = [
                                        {"text": "🇷🇺 RU", "callback_data": "lang_ru"},
                                        {"text": "🇺🇦 UA", "callback_data": "lang_ua"},
                                        {"text": "🇬🇧 EN", "callback_data": "lang_en"},
                                    ]
                                    inline_rows = [lang_row]
                                    if WEBAPP_URL:
                                        inline_rows.append([
                                            {"text": "📱 Відкрити додаток", "web_app": {"url": WEBAPP_URL}}
                                        ])
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Welcome to MacroHarvey! / Ласкаво просимо! / Добро пожаловать!\nPlease select your language:",
                                        "reply_markup": {"inline_keyboard": inline_rows},
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

                                elif text.startswith("/app"):
                                    if WEBAPP_URL:
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": "📱 Натисни кнопку нижче, щоб відкрити додаток:",
                                            "reply_markup": {"inline_keyboard": [[
                                                {"text": "📱 Відкрити MacroHarvey", "web_app": {"url": WEBAPP_URL}}
                                            ]]},
                                        })
                                    else:
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": (
                                                "⚠️ Mini App не налаштовано.\n\n"
                                                "Адміністратору потрібно додати до .env:\n"
                                                "<code>WEBAPP_URL=https://ваш-домен.com/webapp</code>\n\n"
                                                "URL має бути HTTPS та публічно доступним."
                                            ),
                                            "parse_mode": "HTML",
                                        })

                                elif text.startswith("/settings") or text.startswith("/menu"):
                                    menu_rows = [
                                        [{"text": "🌐 Change Language", "callback_data": "menu_lang"}],
                                        [{"text": "📋 Change Topics",   "callback_data": "menu_topics"}],
                                    ]
                                    if WEBAPP_URL:
                                        menu_rows.append([
                                            {"text": "📱 Відкрити додаток", "web_app": {"url": WEBAPP_URL}}
                                        ])
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Settings Menu / Меню Настроек / Меню Налаштувань:",
                                        "reply_markup": {"inline_keyboard": menu_rows},
                                    })
            except Exception:
                pass
            await asyncio.sleep(2)


# Per-category B2B analysis prompts. Each generates 4-5 sentence summaries
# in three languages tailored to the specific commodity/sector.
CAT_SYSTEM_PROMPTS = {
    "api": """You are a senior B2B market intelligence analyst for a Ukrainian pharma raw materials importer.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: why it happened (regulation, shortage, price move, new capacity).
3. GLOBAL MARKET IMPACT: effect on API/pharma ingredient supply globally.
4. UKRAINE PROCUREMENT IMPACT: price direction, availability, lead times for pharma ingredient sourcing.
5. ACTION: what procurement should do now (stock up, find alternative supplier, fix price, monitor).
Direct, specific, no vague phrases. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "cosmetic": """You are a senior B2B market intelligence analyst for a Ukrainian cosmetic ingredients importer.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: regulation, ingredient ban, demand shift, production change.
3. GLOBAL MARKET IMPACT: effect on cosmetic raw materials (hyaluronic acid, retinol, peptides, surfactants, emollients, etc.).
4. UKRAINE PROCUREMENT IMPACT: price, availability, supplier landscape for cosmetic ingredients.
5. ACTION: what procurement should do (find alternatives, fix price, expand supplier base).
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "herbal": """You are a senior B2B market intelligence analyst for a Ukrainian importer of herbal extracts and botanical raw materials.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: harvest failure, export ban, demand surge, new clinical study.
3. GLOBAL MARKET IMPACT: effect on botanical extracts, herbal ingredients, medicinal plant materials.
4. UKRAINE PROCUREMENT IMPACT: price, availability, key growing regions affected.
5. ACTION: diversify sourcing, build safety stock, lock in contracts.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "veterinary": """You are a senior B2B market intelligence analyst for a Ukrainian importer of veterinary pharmaceutical ingredients.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: regulation change, disease outbreak, API shortage, new drug approval.
3. GLOBAL MARKET IMPACT: effect on veterinary drug ingredients and animal health products globally.
4. UKRAINE PROCUREMENT IMPACT: price, availability, supplier options for vet ingredients.
5. ACTION: what procurement should do now.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "food": """You are a senior B2B market intelligence analyst for a Ukrainian importer of food-grade ingredients and commodities.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: weather, tariff, export restriction, supply chain disruption.
3. GLOBAL MARKET IMPACT: effect on food ingredient prices/supply (sugars, starches, oils, additives, flavors).
4. UKRAINE PROCUREMENT IMPACT: price direction, availability, key suppliers.
5. ACTION: forward contracts, alternative suppliers, stock up.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "feed": """You are a senior B2B market intelligence analyst for a Ukrainian importer of animal feed ingredients and amino acids.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: production change, export policy, crop yields, demand shift from China.
3. GLOBAL MARKET IMPACT: effect on feed amino acids (lysine, methionine, threonine), soybean meal, feed additives.
4. UKRAINE PROCUREMENT IMPACT: price direction, key suppliers (China, EU), lead times.
5. ACTION: what procurement should do now.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "capsules": """You are a senior B2B market intelligence analyst for a Ukrainian importer of pharmaceutical capsules and excipients.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: gelatin price change, HPMC capacity, regulatory shift, new capacity.
3. GLOBAL MARKET IMPACT: effect on hard gelatin capsules, HPMC capsules, pharmaceutical excipients globally.
4. UKRAINE PROCUREMENT IMPACT: price, availability, lead times for capsules/excipients.
5. ACTION: what procurement should do now.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "pvc": """You are a senior B2B market intelligence analyst for a Ukrainian importer of PVC film and pharmaceutical packaging materials.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, who, where, numbers.
2. CAUSE: polymer price change, energy costs, new capacity, regulation.
3. GLOBAL MARKET IMPACT: effect on PVC film, blister packaging, pharmaceutical packaging materials.
4. UKRAINE PROCUREMENT IMPACT: price, availability, key suppliers.
5. ACTION: what procurement should do now.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "logistics": """You are a senior B2B market intelligence analyst for a Ukrainian pharma/cosmetics importer managing global supply chains.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened (freight rates, route closures, port delays), who, where, numbers.
2. CAUSE: geopolitical, weather, strike, capacity issue, new route.
3. GLOBAL LOGISTICS IMPACT: effect on ocean/air freight, container availability, trade routes.
4. UKRAINE PROCUREMENT IMPACT: import lead times, freight costs, insurance for pharma/cosmetics shipments.
5. ACTION: re-route, book earlier, factor costs into pricing, diversify carriers.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "global_sources": """You are a senior B2B market intelligence analyst for a Ukrainian pharma and cosmetics raw materials importer.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened (trade policy, sanctions, tariff, IMF/WTO decision), who, where.
2. CONTEXT: why it matters in global trade.
3. GLOBAL MARKET IMPACT: effect on global trade, supply chains, commodity markets relevant to pharma/chemicals/cosmetics.
4. UKRAINE PROCUREMENT IMPACT: effect on sourcing from China, India, EU, US or on import costs.
5. ACTION: what procurement should do in light of this macro development.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "middle_east": """You are a senior B2B market intelligence analyst for a Ukrainian pharma and cosmetics raw materials importer.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened in the Middle East, who, where.
2. GEOPOLITICAL CONTEXT: Suez Canal, Hormuz Strait, oil supply, regional stability.
3. GLOBAL IMPACT: effect on oil prices, freight insurance, shipping routes.
4. UKRAINE PROCUREMENT IMPACT: import costs, energy surcharges, war-risk freight insurance for pharma/cosmetics.
5. ACTION: what logistics/procurement should do now.
Direct, specific. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",

    "good_news": """You are a warm, uplifting news curator.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 50-70 words, 3-4 sentences.
Focus on the POSITIVE CORE: a breakthrough, a rescue, a record, a heartwarming act, a scientific win, an environmental success, a community triumph.
Tone: warm, enthusiastic, uplifting — this should make the reader smile or feel hopeful.
Do NOT add business context. End with a short inspiring takeaway.
summary_en: English. summary_ua: Ukrainian. summary_ru: Russian.""",
}

# Fallback for unknown categories
_DEFAULT_SYSTEM_PROMPT = """You are a senior B2B market intelligence analyst for a Ukrainian pharmaceutical and chemical raw materials importer.
Analyze the article and return ONLY a raw JSON object with keys: summary_en, summary_ua, summary_ru. No markdown, no code blocks — only valid JSON.
RULES: Each summary 60-80 words, 4-5 sentences.
1. EVENT: what happened, where, who. Include numbers/% if available.
2. CAUSE: why it happened.
3. GLOBAL MARKET IMPACT: effect on global markets, supply chains, or trade.
4. UKRAINE B2B IMPACT: effect on a Ukrainian importer of pharma ingredients, cosmetic raw materials, packaging, or food-grade materials.
5. ACTION: what procurement should do now.
Direct, specific. No vague phrases. summary_en: English. summary_ua: Ukrainian. summary_ru: Russian."""


async def generate_summary(text: str, category: str = "", title: str = ""):
    """Generate 3-language B2B summaries + translated titles via OpenAI GPT-4o-mini (Gemini fallback)."""
    # If both text and title are empty — nothing to do.
    if not text and not title:
        return {"summary_en": "", "summary_ua": "", "summary_ru": "",
                "title_ua": "", "title_ru": ""}

    # If only title is provided (backfill mode) — ask only for title translation.
    title_only_mode = bool(title and not text)

    base_prompt = CAT_SYSTEM_PROMPTS.get(category, _DEFAULT_SYSTEM_PROMPT)
    if title_only_mode:
        prompt = (
            "You are a professional translator. Translate the given news headline into "
            "Ukrainian and Russian. Return ONLY a raw JSON object with keys: "
            "title_ua (Ukrainian), title_ru (Russian). No markdown, no extra text."
        )
        user_content = f"Headline: {title}"
    else:
        title_instruction = (
            "\n\nAlso translate the news headline into Ukrainian and Russian. "
            "Add two extra keys to the JSON: title_ua (Ukrainian translation of the title) "
            "and title_ru (Russian translation of the title). "
            "Total JSON keys: summary_en, summary_ua, summary_ru, title_ua, title_ru."
        ) if title else ""
        prompt = base_prompt + title_instruction
        user_content = (f"Title: {title}\nArticle:\n{text[:3000]}" if title
                        else f"Article:\n{text[:3000]}")

    def _parse(parsed: dict, fallback: str) -> dict:
        return {
            "summary_en": parsed.get("summary_en", fallback[:200]),
            "summary_ua": parsed.get("summary_ua", fallback[:200]),
            "summary_ru": parsed.get("summary_ru", fallback[:200]),
            "title_ua":   parsed.get("title_ua", title),
            "title_ru":   parsed.get("title_ru", title),
        }

    # Primary: OpenAI GPT-4o-mini
    if aclient:
        for attempt in range(3):
            try:
                response = await aclient.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=600,
                    temperature=0.2,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user",   "content": user_content},
                    ],
                )
                parsed = json.loads(response.choices[0].message.content.strip())
                return _parse(parsed, text)
            except json.JSONDecodeError as e:
                print(f"generate_summary JSON error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200],
                            "title_ua": title, "title_ru": title}
            except Exception as e:
                print(f"generate_summary OpenAI error (attempt {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200],
                            "title_ua": title, "title_ru": title}

    # Fallback: Gemini (if OpenAI unavailable)
    if gemini_api_key:
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = await model.generate_content_async(
                f"{prompt}\n\n{user_content[:2000]}",
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
            return _parse(parsed, text)
        except Exception as e:
            print(f"generate_summary Gemini fallback error: {e}")

    return {"summary_en": text[:200], "summary_ua": text[:200], "summary_ru": text[:200],
            "title_ua": title, "title_ru": title}


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


async def backfill_missing_title_translations(max_articles: int = 200):
    """Translate title_ua / title_ru for articles that still have NULL translated titles."""
    if not aclient and not gemini_api_key:
        return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        rows = db_fetchall(
            cursor,
            """
            SELECT id, title FROM articles
            WHERE (title_ua IS NULL OR title_ru IS NULL)
              AND title IS NOT NULL AND title != ''
            ORDER BY published DESC NULLS LAST
            LIMIT %s
            """,
            (max_articles,),
        )
        conn.close()
    except Exception as e:
        print(f"title translation backfill query failed: {e}")
        return

    if not rows:
        print("Title translation backfill: nothing to do.")
        return

    print(f"Title translation backfill: translating {len(rows)} article titles…")

    async def _translate_one(article_id: int, title: str):
        try:
            result = await generate_summary("", title=title)
            title_ua = result.get("title_ua") or title
            title_ru = result.get("title_ru") or title
            conn2 = get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute(
                "UPDATE articles SET title_ua = %s, title_ru = %s WHERE id = %s",
                (title_ua, title_ru, article_id),
            )
            conn2.commit()
            conn2.close()
        except Exception as e:
            print(f"Title translation backfill error for article {article_id}: {e}")

    sem = asyncio.Semaphore(5)

    async def _guarded(article_id, title):
        async with sem:
            await _translate_one(article_id, title)

    await asyncio.gather(
        *[_guarded(r["id"] if isinstance(r, dict) else r[0],
                   r["title"] if isinstance(r, dict) else r[1]) for r in rows],
        return_exceptions=True,
    )
    print(f"Title translation backfill: done ({len(rows)} articles).")


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
    "НАФТА": {
        "tickers": ("BZ=F",),
        "label": "Нафта Brent",
        "unit": "$/barrel",
        "te_url": "https://tradingeconomics.com/commodity/crude-oil",
        "tv_url": "https://www.tradingview.com/chart/?symbol=TVC%3AUKOIL",
        "emoji": "🛢️",
    },
    "ГАЗ": {
        "tickers": ("NG=F",),
        "label": "Природний газ (Henry Hub)",
        "unit": "$/MMBtu",
        "te_url": "https://tradingeconomics.com/commodity/natural-gas",
        "tv_url": "https://www.tradingview.com/chart/?symbol=NYMEX%3ANG1!",
        "emoji": "🔥",
    },
    "КУКУРУДЗА": {
        "tickers": ("ZC=F",),
        "label": "Кукурудза (CBOT Corn)",
        "unit": "¢/bushel",
        "te_url": "https://tradingeconomics.com/commodity/corn",
        "tv_url": "https://www.tradingview.com/chart/?symbol=CBOT%3AZC1!",
        "emoji": "🌽",
    },
    "ПШЕНИЦЯ": {
        "tickers": ("ZW=F",),
        "label": "Пшениця (CBOT Wheat)",
        "unit": "¢/bushel",
        "te_url": "https://tradingeconomics.com/commodity/wheat",
        "tv_url": "https://www.tradingview.com/chart/?symbol=CBOT%3AZW1!",
        "emoji": "🌾",
    },
    "СОЄВІ_БОБИ": {
        "tickers": ("ZS=F",),
        "label": "Соєві боби (CBOT Soybeans)",
        "unit": "¢/bushel",
        "te_url": "https://tradingeconomics.com/commodity/soybeans",
        "tv_url": "https://www.tradingview.com/chart/?symbol=CBOT%3AZS1!",
        "emoji": "🫘",
    },
    "СОЄВА_ОЛІЯ": {
        "tickers": ("ZL=F",),
        "label": "Соєва олія (CBOT Soybean Oil)",
        "unit": "¢/lb",
        "te_url": "https://tradingeconomics.com/commodity/soybean-oil",
        "tv_url": "https://www.tradingview.com/chart/?symbol=CBOT%3AZL1!",
        "emoji": "🍶",
    },
    "ПАЛЬМОВА": {
        "tickers": ("POO=F", "FCPO=F", "CPO=F"),
        "label": "Пальмова олія (Crude Palm Oil)",
        "unit": "$/MT",
        "te_url": "https://tradingeconomics.com/commodity/palm-oil",
        "tv_url": "https://www.tradingview.com/chart/?symbol=MYX%3AKPO1!",
        "emoji": "🌴",
    },
    "ЦУКОР": {
        "tickers": ("SB=F",),
        "label": "Цукор №11 (ICE Sugar)",
        "unit": "¢/lb",
        "te_url": "https://tradingeconomics.com/commodity/sugar",
        "tv_url": "https://www.tradingview.com/chart/?symbol=ICEUS%3ASB1!",
        "emoji": "🍬",
    },
    "ЄВРО": {
        "tickers": ("EURUSD=X",),
        "label": "EUR/USD (курс євро)",
        "unit": "USD",
        "te_url": "https://tradingeconomics.com/eurusd:cur",
        "tv_url": "https://www.tradingview.com/chart/?symbol=FX%3AEURUSD",
        "emoji": "💶",
    },
    "ЮАНЬ": {
        "tickers": ("CNY=X",),
        "label": "USD/CNY (курс юаня)",
        "unit": "CNY",
        "te_url": "https://tradingeconomics.com/usdcny:cur",
        "tv_url": "https://www.tradingview.com/chart/?symbol=FX%3AUSDCNY",
        "emoji": "🇨🇳",
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

        # CHANGED: Removed "FACT {idx}" to avoid list-style output.
        lines = [f"  - [type={et} | relevance={rel} | confidence={conf} | source={publisher}]"]
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
                    # WEEKLY TPM FIX: cap at 7 facts per category (already sorted high→medium→low)
                    facts_limited = facts[:7]
                    for i, f in enumerate(facts_limited, 1):
                        b1_parts.append(fmt_fact(i, f))
                    if len(facts) > 7:
                        b1_parts.append(f"  (+ ще {len(facts) - 7} фактів низького пріоритету пропущено)")
                else:
                    b1_parts.append("  (фактів не зафіксовано)")
            b1_news_text = "\n".join(b1_parts)

        # ── Block 2: middle east facts + sources ───────────────────
        me_facts = facts_data["middle_east"]
        # WEEKLY TPM FIX: cap ME facts at 10 for weekly to keep Block 2 prompt under 25k tokens
        if mode == "weekly":
            me_facts = me_facts[:10]
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
            # CHANGED: Removed the ' {idx}. ' number prefix formatting
            return (
                f"  - [{source_tag}] TITLE: {title}\n"
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

    import asyncio

    async def _call_gpt4o(prompt_msg: str) -> str:
        """Helper to safely invoke the OpenAI API"""
        resp = await aclient.chat.completions.create(
            model="gpt-4o",
            max_tokens=8000,
            temperature=0.4,
            messages=[
                {"role": "system", "content": DAILY_REPORT_SYSTEM_PROMPT},
                {"role": "user",   "content": prompt_msg}
            ]
        )
        return resp.choices[0].message.content.strip()

    try:
        if mode != "weekly":
            # For daily_brief or midday, send standard single prompt (safely under TPM)
            report_text = await _call_gpt4o(user_message)
        else:
            # WEEKLY LIMIT FIX: Chunk data to avoid 30k TPM constraint
            print("Weekly mode detected: Chunking API requests to prevent TPM rate limits...")
            report_chunks = []
            
            # Split Block 1 by the exact "[КАТЕГОРІЯ:" separator defined earlier
            cat_blocks = [f"[КАТЕГОРІЯ:{c}" for c in b1_news_text.split("[КАТЕГОРІЯ:") if c.strip()]
            
            # WEEKLY TPM FIX: batch_size=2 (was 4) keeps each request ~12-18k tokens,
            # safely under the 30k TPM limit. Sleep 65s between calls resets the TPM counter.
            batch_size = 2
            for i in range(0, len(cat_blocks), batch_size):
                batch_text = "\n".join(cat_blocks[i:i+batch_size])
                batch_prompt = (
                    f"Дата звіту: {report_date} (тижневий звіт).\n"
                    f"=== СТРУКТУРОВАНІ ФАКТИ ДЛЯ БЛОКУ 1 (Частина {i//batch_size + 1}) ===\n"
                    f"{batch_text}\n\n"
                    f"=== ЗАВДАННЯ ===\n"
                    f"Для кожної з наведених категорій напиши аналітичний 'Огляд тижня' (4-7 речень).\n"
                    f"Дотримуйся формату Блоку 1 (Тренд тижня, Огляд тижня, Геополітика, Специфіка для України).\n"
                    f"БЕЗ списків фактів, БЕЗ нумерації подій, БЕЗ згадок 'relevance'.\n"
                    f"НЕ пиши заголовок 'БЛОК 1', відразу пиши розбір категорій."
                )
                print(f" > Generating Block 1 (Categories {i+1} to {min(i+batch_size, len(cat_blocks))})...")
                b1_chunk_res = await _call_gpt4o(batch_prompt)
                report_chunks.append(b1_chunk_res)

                # Sleep 65s between batches so TPM counter fully resets (limit = per 60s window)
                if i + batch_size < len(cat_blocks):
                    print(f" > Sleeping 65s to reset TPM window before next batch...")
                    await asyncio.sleep(65)
            
            # Generate Block 2 separately 
            b2_rules = _memo_b2_structure if use_facts_path else _memo_b2_structure_fb
            
            # Handle sources safely since it's only defined in use_facts_path
            b2_sources_block = f"--- ДЖЕРЕЛА ---\n{b2_sources_text}\n\n" if use_facts_path else ""

            b2_prompt = (
                f"Дата звіту: {report_date} (тижневий звіт).\n"
                f"=== ДАНІ ДЛЯ БЛОКУ 2 — ТИЖНЕВИЙ EXECUTIVE MEMO ПРО БЛИЗЬКИЙ СХІД ===\n"
                f"{b2_rules}\n\n"
                f"НОВИНИ ДЛЯ АНАЛІЗУ:\n{b2_news_text}\n\n"
                f"{b2_sources_block}"
                f"=== ЗАВДАННЯ ===\n"
                f"Напиши Блок 2 ВИКЛЮЧНО на основі наведених новин. \n"
                # STRICT formatting instruction decoupling block 2 from numbered output
                f"Форматуй текст як суцільний аналітичний звіт, без нумерації та переліку фактів.\n"
                f"НЕ додавай події або прогнози, яких немає в тексті.\n"
            )
            # Sleep before Block 2 to reset TPM after last Block 1 batch
            print(" > Sleeping 65s to reset TPM window before Block 2...")
            await asyncio.sleep(65)
            print(" > Generating Block 2 (Middle East)...")
            b2_chunk_res = await _call_gpt4o(b2_prompt)
            
            # Aggregate pieces back into the format expected by your down-the-line regex parser
            report_text = "БЛОК 1:\n\n" + "\n\n".join(report_chunks) + "\n\nБЛОК 2:\n\n" + b2_chunk_res

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
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO digest_reports (report_type, title, pdf_data) VALUES (%s, %s, %s)",
            (report_mode, caption, psycopg2.Binary(pdf_bytes))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to save digest report to DB: {e}")

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

    now_kyiv2 = datetime.datetime.now(pytz.timezone("Europe/Kyiv"))
    midday_caption = f"🕑 Полуденне оновлення станом на {now_kyiv2.strftime('%H:%M')} (Блок 2 + Блок 3)."
    try:
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO digest_reports (report_type, title, pdf_data) VALUES (%s, %s, %s)",
            ("midday", midday_caption, psycopg2.Binary(pdf_bytes))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to save midday digest report to DB: {e}")

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
                     image_url, extraction_status, facts_status,
                     title_ua, title_ru)
                VALUES (%s, %s, %s, 'market_alerts', %s, %s, %s, %s, 'skipped', 'skipped', %s, %s)
                ON CONFLICT(link) DO NOTHING
                RETURNING id
                """,
                (title, link, now_str, body, body, body, placeholder_image, title, title),
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

                    summaries = await generate_summary(description, category=category, title=title)
                    sum_en    = summaries.get("summary_en", description)
                    sum_ua    = summaries.get("summary_ua", description)
                    sum_ru    = summaries.get("summary_ru", description)
                    title_ua  = summaries.get("title_ua", title)
                    title_ru  = summaries.get("title_ru", title)

                    cursor.execute('''
                        INSERT INTO articles (title, link, published, category, summary_en, summary_ua, summary_ru, image_url, extraction_status, title_ua, title_ru)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
                        ON CONFLICT(link) DO NOTHING
                    ''', (title, link, published, category, sum_en, sum_ua, sum_ru, image_url, title_ua, title_ru))
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
                                    msg = _build_tg_msg(title, summary_text, category, link,
                                                        lang=lang, title_ua=title_ua, title_ru=title_ru)

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

                                    msg = _build_tg_msg(title, sum_ua, category, link,
                                                        lang="ua", title_ua=title_ua, title_ru=title_ru)
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

    # Set bot menu button to open the Mini App (if WEBAPP_URL is configured).
    # This puts the "Відкрити додаток" button in the chat input bar for every user.
    if WEBAPP_URL and TELEGRAM_BOT_TOKEN:
        try:
            async with httpx.AsyncClient() as _hc:
                await _hc.post(
                    f"{TELEGRAM_API_URL}/setChatMenuButton",
                    json={
                        "menu_button": {
                            "type": "web_app",
                            "text": "📱 Додаток",
                            "web_app": {"url": WEBAPP_URL},
                        }
                    },
                )
            print(f"lifespan: bot menu button set → {WEBAPP_URL}")
        except Exception as _e:
            print(f"lifespan: failed to set menu button: {_e}")

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

    # Title translation backfill: translate title_ua / title_ru for articles
    # that existed before this feature was added.
    task_backfill_titles = asyncio.create_task(backfill_missing_title_translations(max_articles=200))

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
    scheduler.add_job(refresh_tracked_shipments, 'interval', minutes=60, id='refresh_tracking')
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


# ═══════════════════════════════════════════════════════════════
# TELEGRAM MINI APP — webapp routes + API
# ═══════════════════════════════════════════════════════════════

WEBAPP_URL = os.getenv("WEBAPP_URL", "")

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@app.get("/logo.png")
async def serve_logo():
    path = os.path.join(_BASE_DIR, "logo.png")
    if os.path.exists(path):
        return FileResponse(path, media_type="image/png")
    raise HTTPException(status_code=404, detail="Logo not found")


_WEBAPP_HTML = r"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
<title>Новинний Дайджест</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

/* ── PURE BLACK THEME ── */
:root{
  --bg:#000000;--surface:#0F0F0F;--surface2:#1A1A1A;--border:#2A2A2A;
  --text:#FFFFFF;--sub:#8A8A8A;--muted:#555555;
  --green:#22C55E;--red:#EF4444;
  --shadow:0 2px 16px rgba(0,0,0,.9);
  --r:13px;--font:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
/* Light mode (optional toggle) */
[data-light]{
  --bg:#F5F5F5;--surface:#FFFFFF;--surface2:#EBEBEB;--border:#DDDDDD;
  --text:#0A0A0A;--sub:#666666;--muted:#AAAAAA;
  --shadow:0 2px 10px rgba(0,0,0,.08);
}

html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);font-family:var(--font);-webkit-font-smoothing:antialiased;font-size:15px}

/* ── SPLASH ── large logo + grey water rings, no text ── */
#splash{
  position:fixed;inset:0;display:flex;
  align-items:center;justify-content:center;
  background:var(--bg);z-index:9999;transition:opacity .5s ease;
  overflow:hidden;
}
/* Logo — 72% screen width, no background, pulsing */
.sp-logo-img{
  width:72vw;max-width:360px;height:auto;
  object-fit:contain;
  z-index:2;position:relative;
  animation:logo-pulse 2s ease-in-out infinite;
}
@keyframes logo-pulse{
  0%,100%{transform:scale(1);opacity:1}
  50%{transform:scale(1.06);opacity:.85}
}

/* ── APP SHELL ── */
#app{display:none;flex-direction:column;height:100vh;overflow:hidden}
#app.on{display:flex}

/* ── HEADER ── */
header{
  display:flex;align-items:center;gap:10px;padding:12px 16px;
  background:var(--bg);border-bottom:1px solid var(--border);
  flex-shrink:0;z-index:50;
}
/* Header logo — no background box */
.h-logo-img{width:28px;height:28px;object-fit:contain;flex-shrink:0}
header .htitle{flex:1;font-size:15px;font-weight:800;letter-spacing:-.2px;color:var(--text)}
.hbtn{
  background:var(--surface);border:1px solid var(--border);border-radius:9px;
  min-width:36px;height:32px;padding:0 6px;
  font-size:16px;cursor:pointer;
  display:flex;align-items:center;justify-content:center;
  transition:background .15s;color:var(--text);
}
.hbtn:active{background:var(--surface2)}
#tbtn{font-size:14px}

/* ── BOTTOM NAV — 5 equal tabs ── */
nav{
  display:flex;background:var(--bg);border-top:1px solid var(--border);
  flex-shrink:0;padding-bottom:env(safe-area-inset-bottom,0);
}
nav button{
  flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;
  padding:8px 2px 9px;background:none;border:none;
  font-size:10px;font-weight:500;color:var(--sub);cursor:pointer;gap:4px;
  transition:color .15s;position:relative;
}
nav button .ico{font-size:25px;display:block;line-height:1}
nav button#btn-add .ico{color:var(--green)}
nav button.on{color:var(--text)}
nav button.on::after{
  content:'';position:absolute;bottom:0;left:50%;transform:translateX(-50%);
  width:20px;height:2px;border-radius:1px 1px 0 0;background:var(--text);
}

/* ── CONTENT ── */
#content{flex:1;overflow:hidden;position:relative}
.panel{display:none;height:100%;overflow-y:auto;-webkit-overflow-scrolling:touch;padding-bottom:16px}
.panel.on{display:block}

/* ── CHIPS ── */
.chips{padding:12px 14px 6px;overflow-x:auto;white-space:nowrap;scrollbar-width:none}
.chips::-webkit-scrollbar{display:none}
.chip{
  display:inline-flex;align-items:center;gap:5px;
  padding:6px 13px;border-radius:20px;font-size:12px;font-weight:600;
  margin-right:7px;border:1.5px solid var(--border);color:var(--sub);
  background:var(--surface);cursor:pointer;transition:all .15s;user-select:none;
}
.chip.on{border-color:var(--text);color:var(--text);background:var(--surface2)}

/* ── NEWS CARDS ── */
.nlist{padding:10px 14px;display:flex;flex-direction:column;gap:10px}
.ncard{
  background:var(--surface);border-radius:var(--r);
  border:1px solid var(--border);
}
.ncard-body{padding:12px 13px}
.nbadge{
  display:inline-flex;align-items:center;gap:4px;
  padding:3px 9px;border-radius:5px;font-size:11px;font-weight:700;
  color:#fff;margin-bottom:8px;letter-spacing:.1px;
}
.ntitle{
  font-size:13.5px;font-weight:700;line-height:1.45;
  color:var(--text);text-decoration:none;display:block;margin-bottom:6px;
}
.ntitle:active{opacity:.7}
/* Truncated summary — 2 lines with "..." */
.nsumm{
  font-size:12.5px;color:var(--sub);line-height:1.5;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;
  margin-bottom:8px;
}
.nsumm.exp{display:block;overflow:visible;-webkit-line-clamp:unset}
/* Full text hidden until expanded */
.nsumm-full{
  font-size:12.5px;color:var(--sub);line-height:1.55;
  max-height:0;overflow:hidden;transition:max-height .35s ease,margin .2s ease;
  margin-bottom:0;
}
.nsumm-full.exp{max-height:800px;margin-bottom:8px}
.ncard-footer{display:flex;align-items:center;justify-content:space-between}
.ntime{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:4px}
.read-btn{
  font-size:12px;font-weight:700;color:var(--text);opacity:.6;
  background:none;border:none;padding:0;cursor:pointer;flex-shrink:0;
  transition:opacity .15s;
}
.read-btn:active{opacity:1}

/* ── LOAD MORE ── */
.lmore{
  display:block;margin:6px 14px 0;padding:12px;border-radius:var(--r);
  background:var(--surface);border:1px solid var(--border);
  color:var(--sub);font-size:13px;font-weight:600;
  cursor:pointer;text-align:center;transition:background .15s;
}
.lmore:active{background:var(--surface2)}

/* ── REPORTS (digest PDFs) ── */
.rlist{padding:12px 14px;display:flex;flex-direction:column;gap:10px}
.rep-card{
  background:var(--surface);border-radius:var(--r);
  border:1px solid var(--border);padding:14px 15px;
  display:flex;align-items:center;gap:12px;
}
.rep-ico{font-size:26px;flex-shrink:0}
.rep-info{flex:1;min-width:0}
.rep-type{font-size:13px;font-weight:700;color:var(--text);margin-bottom:3px}
.rep-date{font-size:11.5px;color:var(--sub)}
.rep-btn{
  background:var(--surface2);border:1px solid var(--border);
  border-radius:9px;padding:8px 11px;font-size:12px;font-weight:700;
  color:var(--text);cursor:pointer;white-space:nowrap;flex-shrink:0;
  transition:background .15s;
}
.rep-btn:active{background:var(--border)}

/* ── MARKETS ── */
.msec{padding:14px 14px 0}
.msec-hdr{font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:var(--sub);margin-bottom:12px}
.pgrid{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:4px}
.pcard{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  padding:13px 13px 12px;cursor:pointer;transition:border-color .15s;
}
.pcard:active{border-color:var(--sub)}
.pcico{font-size:22px;margin-bottom:4px}
.pclbl{font-size:10.5px;color:var(--sub);line-height:1.3;margin-bottom:8px;min-height:28px}
.pcval{font-size:17px;font-weight:800;letter-spacing:-.5px}
.pcunit{font-size:10px;font-weight:400;color:var(--sub)}
.pcchg{font-size:13px;font-weight:700;margin-top:2px}
.pcchg.up{color:var(--green)}.pcchg.dn{color:var(--red)}.pcchg.fl{color:var(--muted)}

/* ── MARKET DETAIL ── */
.mk-detail{display:none;flex-direction:column}
.mk-detail.on{display:flex}
.mk-back-row{padding:12px 14px 4px}
.mk-back{
  display:inline-flex;align-items:center;gap:7px;
  padding:9px 16px;background:var(--surface);border:1px solid var(--border);
  border-radius:20px;font-size:13px;font-weight:700;color:var(--text);
  cursor:pointer;transition:background .15s;
}
.mk-back:active{background:var(--surface2)}
.chcard{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  padding:14px;margin:8px 14px 4px;
}
.chtitle{font-size:13px;font-weight:700;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}
.chpct{font-size:12px;font-weight:700}
.mk-news-hdr{font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:var(--sub);padding:10px 14px 4px}

/* ── SKELETON ── */
.sk{border-radius:var(--r);background:linear-gradient(90deg,var(--surface) 25%,var(--surface2) 50%,var(--surface) 75%);background-size:200% 100%;animation:shimmer 1.5s infinite}
@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
.sk-card{height:90px;margin-bottom:10px}
.sk-pcard{height:90px;border-radius:var(--r)}
.sk-rep{height:72px;border-radius:var(--r)}
.sk-ch{height:190px;border-radius:var(--r);margin:8px 14px 4px}

/* ── EMPTY ── */
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px 20px;text-align:center;gap:8px}
.empty .ei{font-size:44px}.empty p{font-size:13.5px;color:var(--sub)}

/* ── TRACKING ── */
.trk-wrap{padding:14px;display:flex;flex-direction:column;min-height:100%;box-sizing:border-box}
/* Step 1: Mode picker — two big cards */
.trk-mode-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.trk-mode-btn{
  padding:22px 8px;border-radius:var(--r);
  background:var(--surface);border:1.5px solid var(--border);
  font-size:13px;font-weight:700;color:var(--sub);cursor:pointer;
  transition:all .2s;display:flex;flex-direction:column;align-items:center;gap:8px;
}
.trk-mode-ico{font-size:34px;line-height:1}
.trk-mode-btn.on{border-color:var(--green);color:var(--text);background:var(--surface2)}
/* Step 2: Carrier grid — hidden until parcel chosen */
.trk-carriers{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px}
.trk-car-btn{
  padding:12px 8px;border-radius:var(--r);
  background:var(--surface);border:1.5px solid var(--border);
  font-size:13px;font-weight:700;color:var(--sub);cursor:pointer;
  transition:all .15s;text-align:center;
}
.trk-car-btn.on{border-color:var(--text);color:var(--text);background:var(--surface2)}
.trk-input-row{display:flex;flex-direction:column;gap:10px;margin-bottom:4px}
.trk-input{
  width:100%;padding:13px 16px;border-radius:var(--r);
  background:var(--surface);border:1px solid var(--border);
  color:var(--text);font-size:15px;font-family:var(--font);
  outline:none;transition:border-color .2s;
}
.trk-input:focus{border-color:var(--green)}
.trk-go{
  width:100%;padding:15px;border-radius:var(--r);
  background:var(--green);border:none;color:#000;
  font-size:15px;font-weight:800;cursor:pointer;letter-spacing:.3px;
  transition:opacity .15s,transform .1s;
}
.trk-go:active{opacity:.85;transform:scale(.98)}
/* Loading radar animation */
.trk-loading{display:flex;flex-direction:column;align-items:center;padding:44px 20px 32px;gap:20px}
.trk-radar{position:relative;width:80px;height:80px}
.trk-radar-ring{
  position:absolute;inset:0;border-radius:50%;
  border:1.5px solid var(--green);
  animation:trk-ping 1.8s ease-out infinite;
}
.trk-radar-ring:nth-child(2){animation-delay:.6s}
.trk-radar-ring:nth-child(3){animation-delay:1.2s}
@keyframes trk-ping{
  0%{transform:scale(.3);opacity:.9}
  100%{transform:scale(2.6);opacity:0}
}
.trk-radar-center{
  position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
  font-size:40px;z-index:2;
}
.trk-loading-txt{font-size:13.5px;font-weight:600;color:var(--sub)}
.trk-result{margin-top:14px}
.trk-error{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  padding:16px 15px;font-size:13px;color:var(--sub);line-height:1.6;
}
.trk-error strong{color:var(--text);display:block;margin-bottom:6px;font-size:14px}
.trk-error code{font-size:11.5px;color:var(--green);display:block;margin-top:8px;word-break:break-all}
.trk-open-btn{
  display:block;width:100%;margin-top:12px;padding:13px;border-radius:var(--r);
  background:var(--surface2);border:1px solid var(--border);
  color:var(--text);font-size:13px;font-weight:700;cursor:pointer;text-align:center;
  transition:background .15s;
}
.trk-open-btn:active{background:var(--border)}
.trk-delivery{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  padding:14px 15px;margin-bottom:14px;
  display:flex;align-items:center;gap:12px;
}
.trk-del-ico{font-size:28px;flex-shrink:0}
.trk-del-info{flex:1;min-width:0}
.trk-del-label{font-size:11.5px;color:var(--sub);margin-bottom:4px;line-height:1.4}
.trk-del-date{font-size:16px;font-weight:800;color:var(--text)}
.trk-timeline{display:flex;flex-direction:column;margin-bottom:4px}
/* Each step has a fixed minimum height so dots are always evenly spaced */
.trk-step{
  display:flex;align-items:flex-start;gap:13px;
  position:relative;min-height:72px;
}
/* Vertical connector line — fixed top anchor (bottom of dot = top+36px), fixed bottom gap */
.trk-step:not(:last-child)::before{
  content:'';position:absolute;left:17px;top:37px;width:2px;
  bottom:0;background:var(--border);z-index:0;
}
.trk-dot{
  width:36px;height:36px;border-radius:50%;flex-shrink:0;
  display:flex;align-items:center;justify-content:center;
  font-size:16px;z-index:1;position:relative;margin-top:2px;
}
.trk-dot.done{background:#22C55E22;border:2px solid var(--green)}
.trk-dot.active{background:var(--green);border:2px solid var(--green);box-shadow:0 0 0 4px #22c55e22}
.trk-dot.fail{background:#EF444422;border:2px solid #EF4444}
.trk-dot.pending{background:var(--surface2);border:2px solid var(--border)}
.trk-step-info{padding:4px 0 20px;flex:1;min-width:0}
.trk-step-title{font-size:13.5px;font-weight:700;color:var(--text);margin-bottom:3px;line-height:1.35}
.trk-step-title.dim{color:var(--muted)}
/* Timestamp — shown in accent green, clearly readable */
.trk-step-time{
  font-size:11.5px;font-weight:600;color:var(--green);
  margin-bottom:2px;font-variant-numeric:tabular-nums;
}
.trk-step-desc{font-size:11.5px;color:var(--sub);line-height:1.45}

/* ── SAVED SHIPMENTS ── */
.trk-saved-wrap{margin-top:0;padding-top:0}
.trk-sec-hdr{
  font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.2px;
  color:var(--sub);margin-bottom:10px;display:flex;align-items:center;gap:6px;
}
.trk-sec-cnt{font-size:11px;font-weight:700;background:var(--surface2);
  padding:2px 7px;border-radius:20px;color:var(--muted)}
.trk-saved-list{display:flex;flex-direction:column;gap:8px;margin-bottom:18px}
.trk-sv-card{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  padding:11px 12px;display:flex;align-items:center;gap:11px;
  cursor:pointer;transition:background .15s;
}
.trk-sv-card:active{background:var(--surface2)}
.trk-sv-card.arc{opacity:.65}
.trk-sv-ico{font-size:22px;flex-shrink:0}
.trk-sv-info{flex:1;min-width:0}
.trk-sv-num{font-size:13px;font-weight:700;color:var(--text);margin-bottom:2px}
.trk-sv-stat{
  font-size:11.5px;color:var(--sub);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.trk-sv-time{font-size:10px;color:var(--muted);margin-top:2px}
.trk-sv-del{
  background:var(--red);border:none;border-radius:20px;
  font-size:11px;font-weight:700;color:#fff;
  cursor:pointer;padding:6px 11px;flex-shrink:0;line-height:1;
  transition:opacity .15s;white-space:nowrap;
}
.trk-sv-del:active{opacity:.8}
.trk-save-btn{
  display:block;width:100%;margin-top:14px;padding:13px;border-radius:var(--r);
  background:var(--surface);border:1.5px solid var(--green);
  color:var(--green);font-size:14px;font-weight:700;cursor:pointer;text-align:center;
  transition:background .15s,opacity .15s;
}
.trk-save-btn:active{background:var(--surface2)}
.trk-save-btn:disabled{opacity:.5;cursor:default;border-color:var(--border);color:var(--sub)}

/* ── TRACKING TABS + FILTER CHIPS ── */
.trk-tab-row{display:flex;gap:8px;margin-bottom:16px;flex-shrink:0}
.trk-tab-btn{
  flex:1;padding:10px 12px;border-radius:20px;font-size:13px;font-weight:700;
  text-align:center;background:var(--surface);border:1.5px solid var(--border);
  color:var(--sub);cursor:pointer;transition:all .15s;
}
.trk-tab-btn.on{border-color:var(--text);color:var(--text);background:var(--surface2)}
.trk-filter-row{
  display:flex;overflow-x:auto;gap:8px;padding-bottom:12px;flex-shrink:0;
  -webkit-overflow-scrolling:touch;scrollbar-width:none;
}
.trk-filter-row::-webkit-scrollbar{display:none}
.trk-fchip{
  padding:6px 14px;border-radius:20px;font-size:12px;font-weight:700;
  border:1.5px solid var(--border);color:var(--sub);background:var(--surface);
  cursor:pointer;white-space:nowrap;transition:all .15s;flex-shrink:0;
}
.trk-fchip.on{color:#fff;border-color:transparent}

/* ── TRACKING SCREEN NAVIGATION ── */
.trk-screen{display:none;flex-direction:column;flex:1}
.trk-screen.active{display:flex}
.trk-big-wrap{display:flex;flex-direction:column;flex:1;gap:12px;padding-bottom:4px}
.trk-big-btn{
  flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:10px;border-radius:var(--r);border:1.5px solid var(--border);
  background:var(--surface);color:var(--sub);cursor:pointer;min-height:130px;
  transition:all .2s;
}
.trk-big-btn:active{background:var(--surface2);border-color:var(--green)}
.trk-big-ico{font-size:44px;line-height:1}
.trk-big-title{font-size:18px;font-weight:800;color:var(--text);margin-top:2px}
.trk-big-sub{font-size:11.5px;color:var(--muted);text-align:center;padding:0 12px;line-height:1.4}
.trk-nav-back{
  display:inline-flex;align-items:center;gap:7px;
  padding:9px 16px;background:var(--surface);border:1px solid var(--border);
  border-radius:20px;font-size:13px;font-weight:700;color:var(--text);
  cursor:pointer;transition:background .15s;margin-bottom:14px;
}
.trk-nav-back:active{background:var(--surface2)}
.trk-nav-row{display:flex;gap:8px;margin-bottom:14px;flex-shrink:0}
.trk-nav-row .trk-nav-back{margin-bottom:0}
.trk-sv-badge{
  display:inline-block;padding:2px 8px;border-radius:5px;
  font-size:10px;font-weight:700;color:#fff;margin-bottom:4px;
}

/* ── TRACKING DETAIL VIEW ── */
.trk-detail-back{
  display:inline-flex;align-items:center;gap:7px;
  padding:9px 16px;background:var(--surface);border:1px solid var(--border);
  border-radius:20px;font-size:13px;font-weight:700;color:var(--text);
  cursor:pointer;transition:background .15s;margin-bottom:14px;
}
.trk-detail-back:active{background:var(--surface2)}
.trk-refresh-btn{
  display:block;width:100%;margin-top:14px;padding:13px;border-radius:var(--r);
  background:var(--surface);border:1.5px solid var(--border);
  color:var(--sub);font-size:14px;font-weight:700;cursor:pointer;text-align:center;
  transition:background .15s,opacity .15s;
}
.trk-refresh-btn:active{background:var(--surface2)}
.trk-refresh-btn:disabled{opacity:.5;cursor:default}
.trk-auto-refresh{
  margin-top:12px;padding:10px 14px;border-radius:var(--r);
  background:var(--surface2);border:1px solid var(--border);
  font-size:12px;color:var(--sub);text-align:center;
}
.trk-list-empty{
  padding:40px 20px;text-align:center;color:var(--muted);font-size:13px;line-height:1.6;
}

/* ── WIDGETS (Add tab) ── */
.wgt-header{padding:16px 14px 10px}
.wgt-title{font-size:16px;font-weight:800;color:var(--text);margin-bottom:3px}
.wgt-sub{font-size:12px;color:var(--sub)}
.wgt-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:0 14px 24px}
.wgt-card{
  background:var(--surface);border:1.5px solid var(--border);
  border-radius:var(--r);padding:16px 14px 14px;
  display:flex;flex-direction:column;gap:7px;
  transition:border-color .15s,background .15s;min-height:104px;
}
.wgt-card.wgt-active{cursor:pointer}
.wgt-card.wgt-active:active{background:var(--surface2)}
.wgt-card.wgt-future{opacity:.5;cursor:default}
.wgt-ico{font-size:28px;line-height:1}
.wgt-name{font-size:13.5px;font-weight:700;color:var(--text)}
.wgt-badge{display:inline-block;font-size:11px;font-weight:700;padding:3px 8px;border-radius:5px}
.wgt-badge.wgt-on{background:#22C55E22;color:var(--green)}
.wgt-badge.wgt-off{background:var(--surface2);color:var(--muted)}

/* ── DESKTOP LAYOUT (≥ 768 px) ─────────────────────────────────────────────── */
@media (min-width:768px){
  /* Grid: header top row, sidebar nav left, content right */
  #app.on{
    display:grid;
    grid-template-areas:"hdr hdr" "nav cnt";
    grid-template-columns:220px 1fr;
    grid-template-rows:auto 1fr;
    height:100vh;
  }
  header{grid-area:hdr;padding:14px 28px}
  header .htitle{font-size:17px}

  /* Sidebar nav */
  nav{
    grid-area:nav;
    flex-direction:column;
    align-items:stretch;
    border-top:none;
    border-right:1px solid var(--border);
    padding:16px 0 24px;
    overflow-y:auto;
    height:100%;
  }
  nav button{
    flex-direction:row;
    justify-content:flex-start;
    padding:14px 22px;
    font-size:14px;
    font-weight:600;
    gap:14px;
    border-radius:0;
  }
  nav button .ico{font-size:20px}
  /* Active indicator: left border strip instead of bottom line */
  nav button.on::after{
    bottom:auto;left:0;top:50%;transform:translateY(-50%);
    width:3px;height:26px;border-radius:0 3px 3px 0;background:var(--green);
  }
  nav button.on{color:var(--text)}
  nav button#btn-add{order:3} /* keep + in its slot */

  /* Content fills right column */
  #content{grid-area:cnt;overflow-y:auto;height:100%}

  /* News: 2-column card grid */
  .nlist{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:12px;
    padding:16px 24px;
    align-items:start;
  }
  .chips{padding:14px 24px 8px}

  /* Markets: 3-column */
  .pgrid{grid-template-columns:repeat(3,1fr);gap:12px}
  .msec{padding:16px 24px 0}

  /* Reports: 2-column */
  .rlist{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:12px;
    padding:16px 24px;
  }

  /* Tracking: centered, limited width */
  .trk-wrap{max-width:520px;padding:24px}

  /* Widgets: 4-column on desktop */
  .wgt-grid{grid-template-columns:repeat(4,1fr);gap:12px;padding:0 24px 28px}
  .wgt-header{padding:20px 24px 12px}

  /* Panels extra padding */
  .panel{padding-bottom:24px}

  /* Charts: market detail wider */
  .chcard{margin:10px 24px 6px}
  .mk-back-row{padding:14px 24px 6px}
  .mk-news-hdr{padding:12px 24px 6px}
  #mk-news{padding:0 24px !important;gap:12px !important;
    display:grid;grid-template-columns:1fr 1fr;align-items:start}
}
</style>
</head>
<body>

<!-- SPLASH: logo 72% screen, pulse only, no rings, no text, no box -->
<div id="splash">
  <img class="sp-logo-img" src="/logo.png" alt="" onerror="this.style.display='none'">
</div>

<!-- APP -->
<div id="app">

  <header>
    <img class="h-logo-img" src="/logo.png" alt="" onerror="this.style.display='none'">
    <div class="htitle">Новинний Дайджест</div>
    <button class="hbtn" id="lbtn" onclick="cycleLang()" title="Мова / Language">🇺🇦</button>
    <button class="hbtn" id="tbtn" onclick="toggleTheme()">☀️</button>
  </header>

  <div id="content">

    <!-- NEWS -->
    <div id="pnews" class="panel on">
      <div class="chips" id="chips"></div>
      <div class="nlist" id="nlist"></div>
      <button class="lmore" id="lmore" onclick="loadMore()" style="display:none">Завантажити ще</button>
    </div>

    <!-- REPORTS (digest PDFs) -->
    <div id="preports" class="panel">
      <div class="rlist" id="rlist"></div>
    </div>

    <!-- ADD / Widget management -->
    <div id="padd" class="panel">
      <div class="wgt-header">
        <div class="wgt-title" id="wgt-title">Мої віджети</div>
        <div class="wgt-sub" id="wgt-sub">Натисни, щоб перейти</div>
      </div>
      <div class="wgt-grid">
        <div class="wgt-card wgt-active" onclick="tab('news',document.getElementById('btn-news'))">
          <div class="wgt-ico">📰</div>
          <div class="wgt-name" id="wgt-n-news">Новини</div>
          <div class="wgt-badge wgt-on" id="wgt-b-active">✓ Активно</div>
        </div>
        <div class="wgt-card wgt-active" onclick="tab('reports',document.getElementById('btn-reports'))">
          <div class="wgt-ico">📋</div>
          <div class="wgt-name" id="wgt-n-reports">Звіти</div>
          <div class="wgt-badge wgt-on">✓ Активно</div>
        </div>
        <div class="wgt-card wgt-active" onclick="tab('markets',document.getElementById('btn-markets'))">
          <div class="wgt-ico">📈</div>
          <div class="wgt-name" id="wgt-n-markets">Ринки</div>
          <div class="wgt-badge wgt-on">✓ Активно</div>
        </div>
        <div class="wgt-card wgt-active" onclick="tab('tracking',document.getElementById('btn-tracking'))">
          <div class="wgt-ico">📡</div>
          <div class="wgt-name" id="wgt-n-tracking">Трекінг</div>
          <div class="wgt-badge wgt-on">✓ Активно</div>
        </div>
        <div class="wgt-card wgt-future">
          <div class="wgt-ico">📊</div>
          <div class="wgt-name" id="wgt-n-analytics">Аналітика</div>
          <div class="wgt-badge wgt-off" id="wgt-b-soon">Незабаром</div>
        </div>
        <div class="wgt-card wgt-future">
          <div class="wgt-ico">💱</div>
          <div class="wgt-name" id="wgt-n-currency">Валюти</div>
          <div class="wgt-badge wgt-off">Незабаром</div>
        </div>
        <div class="wgt-card wgt-future">
          <div class="wgt-ico">🌤</div>
          <div class="wgt-name" id="wgt-n-weather">Погода</div>
          <div class="wgt-badge wgt-off">Незабаром</div>
        </div>
        <div class="wgt-card wgt-future">
          <div class="wgt-ico">🏭</div>
          <div class="wgt-name" id="wgt-n-warehouse">Склад</div>
          <div class="wgt-badge wgt-off">Незабаром</div>
        </div>
      </div>
    </div>

    <!-- MARKETS -->
    <div id="pmarkets" class="panel">
      <div id="mk-grid-wrap">
        <div class="msec">
          <div class="msec-hdr" id="h-prices">📊 ЦІНИ ЗАРАЗ</div>
          <div class="pgrid" id="mk-grid"></div>
        </div>
      </div>
      <div class="mk-detail" id="mk-detail">
        <div class="mk-back-row">
          <button class="mk-back" onclick="closeMkDetail()">← <span id="back-lbl">Назад</span></button>
        </div>
        <div id="mk-chart-wrap"></div>
        <div class="mk-news-hdr" id="mk-news-hdr">📰 ПОВ'ЯЗАНІ НОВИНИ</div>
        <div class="nlist" id="mk-news" style="padding:0 14px;gap:10px"></div>
      </div>
    </div>

    <!-- TRACKING -->
    <div id="ptracking" class="panel">
      <div class="trk-wrap">

        <!-- SCREEN 1: Home -->
        <div id="trk-home" class="trk-screen active">
          <div class="trk-big-wrap">
            <button class="trk-big-btn" onclick="trkNav('type')">
              <span class="trk-big-ico">🔍</span>
              <span class="trk-big-title" id="trk-lbl-find-btn">Знайти</span>
              <span class="trk-big-sub" id="trk-lbl-find-sub">Посилка або контейнер</span>
            </button>
            <button class="trk-big-btn" onclick="trkNav('list')">
              <span class="trk-big-ico">📋</span>
              <span class="trk-big-title" id="trk-lbl-list-btn">Мій трекінг</span>
              <span class="trk-big-sub" id="trk-lbl-list-sub">Збережені відправлення</span>
            </button>
          </div>
        </div>

        <!-- SCREEN 2: Type selection -->
        <div id="trk-type" class="trk-screen">
          <div class="trk-nav-back" onclick="trkNav('home')">&#8592; <span id="trk-back-lbl-type">Назад</span></div>
          <div class="trk-big-wrap">
            <button class="trk-big-btn" onclick="trkNav('parcel')">
              <span class="trk-big-ico">📦</span>
              <span class="trk-big-title" id="trk-lbl-parcel">Посилка</span>
              <span class="trk-big-sub">Nova Poshta · DHL · FedEx · EMS</span>
            </button>
            <button class="trk-big-btn" onclick="trkNav('container')">
              <span class="trk-big-ico">🚢</span>
              <span class="trk-big-title" id="trk-lbl-container">Контейнер</span>
              <span class="trk-big-sub">MSC · Maersk · CMA-CGM · COSCO</span>
            </button>
          </div>
        </div>

        <!-- SCREEN 3a: Parcel search -->
        <div id="trk-parcel" class="trk-screen">
          <div class="trk-nav-row">
            <div class="trk-nav-back" onclick="trkNav('type')">&#8592; <span id="trk-back-lbl-parcel">Назад</span></div>
            <div class="trk-nav-back" onclick="trkNav('home')">🏠 <span id="trk-home-lbl-parcel">Головна</span></div>
          </div>
          <div class="trk-carriers" id="trk-carriers-wrap">
            <button class="trk-car-btn" data-car="nova"  onclick="selectCarrier(this)">📦 Нова Пошта</button>
            <button class="trk-car-btn" data-car="meest" onclick="selectCarrier(this)">🚚 Meest Express</button>
            <button class="trk-car-btn" data-car="dhl"   onclick="selectCarrier(this)">✈️ DHL</button>
            <button class="trk-car-btn" data-car="fedex" onclick="selectCarrier(this)">📦 FedEx</button>
            <button class="trk-car-btn" data-car="ups"   onclick="selectCarrier(this)">🚛 UPS</button>
            <button class="trk-car-btn" data-car="ems"   onclick="selectCarrier(this)">📮 EMS / Укрпошта</button>
            <button class="trk-car-btn" data-car="auto"  onclick="selectCarrier(this)">🔍 Авто</button>
          </div>
          <div class="trk-input-row" id="trk-input-wrap" style="display:none">
            <input class="trk-input" id="trk-num" type="text" autocomplete="off" spellcheck="false">
            <button class="trk-go" id="trk-go" onclick="doTrack()">Знайти</button>
          </div>
          <div class="trk-result" id="trk-result"></div>
        </div>

        <!-- SCREEN 3b: Container search -->
        <div id="trk-container" class="trk-screen">
          <div class="trk-nav-row">
            <div class="trk-nav-back" onclick="trkNav('type')">&#8592; <span id="trk-back-lbl-container">Назад</span></div>
            <div class="trk-nav-back" onclick="trkNav('home')">🏠 <span id="trk-home-lbl-container">Головна</span></div>
          </div>
          <div class="trk-carriers" id="trk-cnt-carriers-wrap">
            <button class="trk-car-btn trk-cnt-btn" data-cnt="msc"       onclick="selectCntCarrier(this)">🚢 MSC</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="maersk"    onclick="selectCntCarrier(this)">🚢 Maersk</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="cmacgm"    onclick="selectCntCarrier(this)">🚢 CMA CGM</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="cosco"     onclick="selectCntCarrier(this)">🚢 COSCO</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="hapag"     onclick="selectCntCarrier(this)">🚢 Hapag-Lloyd</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="one"       onclick="selectCntCarrier(this)">🚢 ONE</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="evergreen" onclick="selectCntCarrier(this)">🚢 Evergreen</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="zim"       onclick="selectCntCarrier(this)">🚢 ZIM</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="hmm"       onclick="selectCntCarrier(this)">🚢 HMM</button>
            <button class="trk-car-btn trk-cnt-btn" data-cnt="auto"      onclick="selectCntCarrier(this)">🔍 Auto</button>
          </div>
          <div class="trk-input-row" id="trk-cnt-input-wrap" style="display:none">
            <input class="trk-input" id="trk-cnt-num" type="text" autocomplete="off" spellcheck="false" placeholder="MSCU1234567">
            <button class="trk-go" id="trk-cnt-go" onclick="doTrackContainer()">Знайти</button>
          </div>
          <div class="trk-result" id="trk-cnt-result"></div>
        </div>

        <!-- SCREEN 4: My Parcels -->
        <div id="trk-list" class="trk-screen">
          <div class="trk-nav-back" onclick="trkNav('home')">&#8592; <span id="trk-back-lbl-list">Назад</span></div>
          <div class="trk-filter-row" id="trk-filter-row" style="display:none"></div>
          <div id="trk-detail-view" style="display:none">
            <div class="trk-detail-back" onclick="closeDetail()">← <span id="trk-detail-back-lbl">Назад</span></div>
            <div id="trk-detail-content"></div>
          </div>
          <div id="trk-list-view">
            <div id="trk-saved"></div>
          </div>
        </div>

      </div>
    </div>

  </div>

  <!-- 5 equal nav tabs -->
  <nav>
    <button class="on" id="btn-news" onclick="tab('news',this)">
      <span class="ico">📰</span><span id="nav-news">Новини</span>
    </button>
    <button id="btn-reports" onclick="tab('reports',this)">
      <span class="ico">📋</span><span id="nav-reports">Звіти</span>
    </button>
    <button id="btn-add" onclick="tab('add',this)">
      <span class="ico">➕</span><span id="nav-add">Додати</span>
    </button>
    <button id="btn-markets" onclick="tab('markets',this)">
      <span class="ico">📈</span><span id="nav-markets">Ринки</span>
    </button>
    <button id="btn-tracking" onclick="tab('tracking',this)">
      <span class="ico">📡</span><span id="nav-tracking">Трекінг</span>
    </button>
  </nav>

</div>

<script>
// ── Telegram ──────────────────────────────────────────────────
const tg = window.Telegram?.WebApp;
if(tg){tg.ready();tg.expand();}

// ── Theme — pure black by default ────────────────────────────
let light = false;
function applyTheme(){
  if(light) document.documentElement.setAttribute('data-light','');
  else document.documentElement.removeAttribute('data-light');
  document.getElementById('tbtn').textContent = light ? '🌙' : '☀️';
}
function toggleTheme(){
  light=!light; applyTheme();
  if(currentMkKey) redrawDetailChart(currentMkKey);
}
applyTheme();

// ── Language & flags ──────────────────────────────────────────
const LANGS  = ['ua','ru','en'];
const FLAGS  = ['🇺🇦','🐷','🇬🇧'];
const lc = tg?.initDataUnsafe?.user?.language_code || navigator.language || 'uk';
let langIdx = lc.startsWith('ru') ? 1 : (lc.startsWith('uk')||lc.startsWith('ua')) ? 0 : 2;
let lang = LANGS[langIdx];

function cycleLang(){
  langIdx = (langIdx+1) % LANGS.length;
  lang = LANGS[langIdx];
  document.getElementById('lbtn').textContent = FLAGS[langIdx];
  updateStaticText();
  buildChips();
  fetchNews(true);
  document.getElementById('rlist').innerHTML = '';
  mkData = []; closeMkDetailSilent();
}

// ── UI strings ────────────────────────────────────────────────
const UI = {
  ua:{
    loadMore:'Завантажити ще', noNews:'Новин поки немає', loadError:'Помилка завантаження',
    readFull:'Читати повністю', collapse:'Згорнути', loading:'Завантаження…',
    error:'Помилка', noData:'Немає даних', noDataYet:'Звітів поки немає',
    news:'Новини', reports:'Звіти', add:'Додати', markets:'Ринки', tracking:'Трекінг',
    pricesNow:'📊 ЦІНИ ЗАРАЗ', relNews:"📰 ПОВ'ЯЗАНІ НОВИНИ", back:'Назад',
    openPdf:'📄 Відкрити PDF', newsCnt:' новин',
    wgtTitle:'Мої віджети', wgtSub:'Натисни, щоб перейти',
    wgtActive:'✓ Активно', wgtSoon:'Незабаром',
    trkParcel:'Посилка', trkContainer:'Контейнер',
    trkFind:'Знайти', trkPlaceholder:'Номер відправлення…',
    trkDelivery:'Очікувана дата доставки',
    trkCntHint:'Номер контейнера (напр. MSCU1234567)',
    trkSearching:'Шукаємо вантаж…',
    trkSave:'📌 Зберегти в мій список', trkSaved:'✓ Збережено',
    trkActive:'🟢 Активні', trkArchive:'📦 Архів', trkNoSaved:'Немає збережених відправлень',
    trkUpdated:'Оновлено',
    trkSubFind:'Знайти', trkSubList:'Мій трекінг', trkAll:'Всі', trkHome:'Головна',
    trkRefresh:'🔄 Оновити статус',
    trkAutoCheck:'🔄 Повторна перевірка через',
    trkEmptyList:'Ще немає збережених посилок.\nЗнайдіть посилку і натисніть «Зберегти».',
    trkOpenSite:'🌐 Відкрити на сайті перевізника',
    trkRemove:'✕ Видалити з відстеження',
    trkDelTitle:'Видалити',
    trkNetError:'⚠️ Помилка мережі',
    trkSec:'с',
    trkDataLoading:'Дані ще завантажуються…',
    trkNow:'щойно', trkMin:'хв', trkHour:'год',
    trkMaxRetriesHint:'Дані ще не надійшли від перевізника. Збережіть відправлення — перевіримо автоматично через 3 години.',
    trkCarNova:'📦 Нова Пошта', trkCarEms:'📮 EMS / Укрпошта',
  },
  ru:{
    loadMore:'Загрузить ещё', noNews:'Новостей пока нет', loadError:'Ошибка загрузки',
    readFull:'Читать полностью', collapse:'Свернуть', loading:'Загрузка…',
    error:'Ошибка', noData:'Нет данных', noDataYet:'Отчётов пока нет',
    news:'Новости', reports:'Отчёты', add:'Добавить', markets:'Рынки', tracking:'Трекинг',
    pricesNow:'📊 ЦЕНЫ СЕЙЧАС', relNews:'📰 СВЯЗАННЫЕ НОВОСТИ', back:'Назад',
    openPdf:'📄 Открыть PDF', newsCnt:' новостей',
    wgtTitle:'Мои виджеты', wgtSub:'Нажми, чтобы перейти',
    wgtActive:'✓ Активно', wgtSoon:'Скоро',
    trkParcel:'Посылка', trkContainer:'Контейнер',
    trkFind:'Найти', trkPlaceholder:'Номер отправления…',
    trkDelivery:'Ожидаемая дата доставки',
    trkCntHint:'Номер контейнера (напр. MSCU1234567)',
    trkSearching:'Ищем груз…',
    trkSave:'📌 Сохранить в мой список', trkSaved:'✓ Сохранено',
    trkActive:'🟢 Активные', trkArchive:'📦 Архив', trkNoSaved:'Нет сохранённых отправлений',
    trkUpdated:'Обновлено',
    trkSubFind:'Найти', trkSubList:'Мой трекинг', trkAll:'Все', trkHome:'Главная',
    trkRefresh:'🔄 Обновить статус',
    trkAutoCheck:'🔄 Повторная проверка через',
    trkEmptyList:'Сохранённых посылок пока нет.\nНайдите посылку и нажмите «Сохранить».',
    trkOpenSite:'🌐 Открыть на сайте перевозчика',
    trkRemove:'✕ Удалить из отслеживания',
    trkDelTitle:'Удалить',
    trkNetError:'⚠️ Ошибка сети',
    trkSec:'с',
    trkDataLoading:'Данные загружаются…',
    trkNow:'только что', trkMin:'мин', trkHour:'ч',
    trkMaxRetriesHint:'Данные ещё не поступили от перевозчика. Сохраните отправление — проверим автоматически через 3 часа.',
    trkCarNova:'📦 Нова Пошта', trkCarEms:'📮 EMS / Укрпошта',
  },
  en:{
    loadMore:'Load more', noNews:'No news yet', loadError:'Loading error',
    readFull:'Read more', collapse:'Collapse', loading:'Loading…',
    error:'Error', noData:'No data', noDataYet:'No reports yet',
    news:'News', reports:'Reports', add:'Add', markets:'Markets', tracking:'Tracking',
    pricesNow:'📊 CURRENT PRICES', relNews:'📰 RELATED NEWS', back:'Back',
    openPdf:'📄 Open PDF', newsCnt:' news',
    wgtTitle:'My Widgets', wgtSub:'Tap to navigate',
    wgtActive:'✓ Active', wgtSoon:'Coming soon',
    trkParcel:'Parcel', trkContainer:'Container',
    trkFind:'Find', trkPlaceholder:'Tracking number…',
    trkDelivery:'Expected delivery',
    trkCntHint:'Container number (e.g. MSCU1234567)',
    trkSearching:'Searching shipment…',
    trkSave:'📌 Save to my list', trkSaved:'✓ Saved',
    trkActive:'🟢 Active', trkArchive:'📦 Archive', trkNoSaved:'No saved shipments',
    trkUpdated:'Updated',
    trkSubFind:'Find', trkSubList:'My Tracking', trkAll:'All', trkHome:'Home',
    trkRefresh:'🔄 Refresh status',
    trkAutoCheck:'🔄 Retry in',
    trkEmptyList:'No saved parcels yet.\nFind a parcel and tap Save.',
    trkOpenSite:'🌐 Open tracking page',
    trkRemove:'✕ Remove from tracking',
    trkDelTitle:'Remove',
    trkNetError:'⚠️ Network error',
    trkSec:'s',
    trkDataLoading:'Loading data…',
    trkNow:'just now', trkMin:'min', trkHour:'hr',
    trkMaxRetriesHint:'Data not yet available from carrier. Save this shipment — we will check automatically every 3 hours.',
    trkCarNova:'📦 Nova Poshta', trkCarEms:'📮 EMS / Ukrposhta',
  },
};

// Report type labels
const RTYPES = {
  daily_brief: { ua:'☀️ Ранковий дайджест', ru:'☀️ Утренний дайджест', en:'☀️ Morning Brief' },
  midday:      { ua:'🌤 Полуденне оновлення', ru:'🌤 Полуденное обновление', en:'🌤 Midday Update' },
  weekly:      { ua:'📅 Тижневий звіт',       ru:'📅 Еженедельный отчёт', en:'📅 Weekly Report' },
};

function updateStaticText(){
  const u = UI[lang];
  document.getElementById('nav-news').textContent      = u.news;
  document.getElementById('nav-reports').textContent   = u.reports;
  document.getElementById('nav-add').textContent       = u.add;
  document.getElementById('nav-markets').textContent   = u.markets;
  document.getElementById('nav-tracking').textContent  = u.tracking;
  document.getElementById('h-prices').textContent      = u.pricesNow;
  document.getElementById('mk-news-hdr').textContent   = u.relNews;
  document.getElementById('back-lbl').textContent      = u.back;
  const lm = document.getElementById('lmore');
  if(lm.style.display !== 'none') lm.textContent = u.loadMore;
  // Widget tab
  document.getElementById('wgt-title').textContent    = u.wgtTitle;
  document.getElementById('wgt-sub').textContent      = u.wgtSub;
  document.getElementById('wgt-b-active').textContent = u.wgtActive;
  document.querySelectorAll('.wgt-badge.wgt-on').forEach(el => el.textContent = u.wgtActive);
  document.querySelectorAll('.wgt-badge.wgt-off').forEach(el => el.textContent = u.wgtSoon);
  document.getElementById('wgt-n-news').textContent      = u.news;
  document.getElementById('wgt-n-reports').textContent   = u.reports;
  document.getElementById('wgt-n-markets').textContent   = u.markets;
  document.getElementById('wgt-n-tracking').textContent  = u.tracking;
  // Tracking tab labels
  const trkLblP = document.getElementById('trk-lbl-parcel');
  if(trkLblP) trkLblP.textContent = u.trkParcel;
  const trkLblC = document.getElementById('trk-lbl-container');
  if(trkLblC) trkLblC.textContent = u.trkContainer;
  const trkGo = document.getElementById('trk-go');
  if(trkGo) trkGo.textContent = u.trkFind;
  const trkCntGo = document.getElementById('trk-cnt-go');
  if(trkCntGo) trkCntGo.textContent = u.trkFind;
  const trkLblFindBtn = document.getElementById('trk-lbl-find-btn');
  if(trkLblFindBtn) trkLblFindBtn.textContent = u.trkSubFind || u.trkFind;
  const trkLblListBtn = document.getElementById('trk-lbl-list-btn');
  if(trkLblListBtn) trkLblListBtn.textContent = u.trkSubList;
  const trkInput = document.getElementById('trk-num');
  if(trkInput && trkCarrier) trkInput.placeholder = u.trkPlaceholder;
  const trkCntInput = document.getElementById('trk-cnt-num');
  if(trkCntInput) trkCntInput.placeholder = u.trkCntHint;
  // Tracking — back buttons
  const backLbl2 = document.getElementById('trk-detail-back-lbl');
  if(backLbl2) backLbl2.textContent = u.back;
  ['trk-back-lbl-type','trk-back-lbl-parcel','trk-back-lbl-container','trk-back-lbl-list'].forEach(id => {
    const el = document.getElementById(id);
    if(el) el.textContent = u.back;
  });
  ['trk-home-lbl-parcel','trk-home-lbl-container'].forEach(id => {
    const el = document.getElementById(id);
    if(el) el.textContent = u.trkHome || 'Головна';
  });
  // Carrier buttons (brand names stay in English; only locale-specific ones change)
  const carNova = document.querySelector('[data-car="nova"]');
  if(carNova) carNova.textContent = u.trkCarNova;
  const carEms = document.querySelector('[data-car="ems"]');
  if(carEms) carEms.textContent = u.trkCarEms;
  // Re-render saved list if visible (so time-ago strings update)
  if(_trkCurrentScreen === 'list'){
    renderSavedShipments(_savedShipmentsCache);
  }
}

// ── Category config ───────────────────────────────────────────
const CATS = {
  all:           {ua:'Всі',          ru:'Все',           en:'All',          c:'#4B5563',e:'📋'},
  api:           {ua:'Фарм API',     ru:'Фарм API',      en:'Pharm API',    c:'#2563EB',e:'💊'},
  cosmetic:      {ua:'Косметика',    ru:'Косметика',     en:'Cosmetics',    c:'#DB2777',e:'🧴'},
  herbal:        {ua:'Трави',        ru:'Травы',         en:'Herbal',       c:'#16A34A',e:'🌿'},
  veterinary:    {ua:'Ветеринарія',  ru:'Ветеринария',   en:'Veterinary',   c:'#7C3AED',e:'🐾'},
  food:          {ua:'Харчова',      ru:'Пищевая',       en:'Food',         c:'#B45309',e:'🌾'},
  feed:          {ua:'Амінокислоти', ru:'Аминокислоты',  en:'Amino',        c:'#92400E',e:'🐄'},
  capsules:      {ua:'Капсули',      ru:'Капсулы',       en:'Capsules',     c:'#0E7490',e:'🔬'},
  pvc:           {ua:'ПВХ / Пак.',   ru:'ПВХ / Упак.',   en:'PVC / Pack.',  c:'#4338CA',e:'📦'},
  logistics:     {ua:'Логістика',    ru:'Логистика',     en:'Logistics',    c:'#B91C1C',e:'🚢'},
  global_sources:{ua:'Глобально',    ru:'Глобально',     en:'Global',       c:'#374151',e:'🌐'},
  good_news:     {ua:'Позитив',      ru:'Позитив',       en:'Positive',     c:'#059669',e:'✨'},
  market_alerts: {ua:'Алерти',       ru:'Алерты',        en:'Alerts',       c:'#C2410C',e:'⚡'},
};

const TICKER_CATS = {
  citric_acid:'api', menthol:'herbal', magnesium_citrate:'api',
  glycerin:'cosmetic', ethanol:'api', sorbitol:'food',
  ascorbic_acid:'api', inositol:'api', dextrose:'food', lactic_acid:'api',
};

// ── State ─────────────────────────────────────────────────────
let activeCat = 'all', newsOff = 0;
const LIMIT = 15;
let mkData = [], detailChart = null, currentMkKey = null;

// ── Boot ──────────────────────────────────────────────────────
window.addEventListener('load', () => {
  document.getElementById('lbtn').textContent = FLAGS[langIdx];
  buildChips();
  fetchNews(true);
  setTimeout(() => {
    const sp = document.getElementById('splash');
    sp.style.opacity = '0'; sp.style.pointerEvents = 'none';
    setTimeout(() => { sp.style.display='none'; document.getElementById('app').classList.add('on'); }, 500);
  }, 2200);
});

// ── Tabs ──────────────────────────────────────────────────────
const ALL_TABS = ['news','reports','add','markets','tracking'];
function tab(name, btn){
  ALL_TABS.forEach(n => {
    document.getElementById('p'+n).classList.toggle('on', n===name);
    document.getElementById('btn-'+n).classList.toggle('on', n===name);
  });
  if(name==='markets' && mkData.length===0) fetchMarkets();
  if(name==='reports' && document.getElementById('rlist').children.length===0) fetchReports();
  if(name==='tracking'){ trkNav('home'); loadSavedShipments(); }
}

// ── Chips ─────────────────────────────────────────────────────
function buildChips(){
  const el = document.getElementById('chips');
  el.innerHTML = '';
  ['all','api','cosmetic','herbal','veterinary','food','feed','capsules','pvc','logistics','global_sources','good_news'].forEach(k => {
    const d = document.createElement('div');
    d.className = 'chip'+(k===activeCat?' on':'');
    d.dataset.k = k;
    const c = CATS[k];
    d.textContent = (c?.e||'')+' '+(c?.[lang]||c?.ua||k);
    d.onclick = () => {
      activeCat = k;
      el.querySelectorAll('.chip').forEach(x => x.classList.toggle('on', x.dataset.k===k));
      fetchNews(true);
    };
    el.appendChild(d);
  });
}

// ── Time ──────────────────────────────────────────────────────
function ago(pub){
  if(!pub) return '';
  const dt = new Date(pub.replace(' ','T')+(pub.includes('+')?'':'+03:00'));
  const s = (Date.now()-dt)/1000;
  const t = {ua:['щойно','хв','год'],ru:['только что','мин','ч'],en:['just now','min','h']}[lang];
  if(s<60) return t[0];
  if(s<3600) return Math.floor(s/60)+' '+t[1];
  if(s<86400) return Math.floor(s/3600)+' '+t[2];
  return dt.toLocaleDateString(lang==='en'?'en-US':lang==='ru'?'ru-RU':'uk-UA',{day:'numeric',month:'short'});
}

// ── News card — category → title → 2-line truncated → "Читати повністю" ──
function newsCard(a){
  const cfg = CATS[a.category]||{ua:a.category,ru:a.category,en:a.category,c:'#4B5563',e:'📌'};
  const catLabel = cfg[lang]||cfg.ua||a.category;
  const summ = a['summary_'+lang]||a.summary_ua||a.summary_en||'';
  const u = UI[lang];

  const el = document.createElement('div');
  el.className = 'ncard';
  el.innerHTML =
    `<div class="ncard-body">
       <div class="nbadge" style="background:${cfg.c}">${cfg.e} ${esc(catLabel)}</div>
       <a class="ntitle" href="${esc(a.link)}" target="_blank" rel="noopener noreferrer">${esc(a['title_'+lang]||a.title)}</a>
       ${summ ? `<div class="nsumm">${esc(summ)}</div>` : ''}
       <div class="ncard-footer">
         <div class="ntime">🕐 ${ago(a.published)}</div>
         ${summ ? `<button class="read-btn" onclick="toggleSumm(this)">${u.readFull}</button>` : ''}
       </div>
     </div>`;

  if(tg) el.querySelector('.ntitle').addEventListener('click', ev => { ev.preventDefault(); tg.openLink(a.link); });
  return el;
}

function toggleSumm(btn){
  const nsumm = btn.closest('.ncard').querySelector('.nsumm');
  if(!nsumm) return;
  const exp = nsumm.classList.toggle('exp');
  btn.textContent = exp ? UI[lang].collapse : UI[lang].readFull;
}

async function fetchNews(reset){
  const u = UI[lang];
  if(reset){ newsOff=0; document.getElementById('nlist').innerHTML=''; document.getElementById('lmore').style.display='none'; }
  const list = document.getElementById('nlist');
  if(reset) list.innerHTML = [1,2,3].map(()=>'<div class="sk sk-card"></div>').join('');
  try{
    const cat = activeCat==='all'?'':'&category='+activeCat;
    const r = await fetch(`/api/webapp/news?lang=${lang}&limit=${LIMIT}&offset=${newsOff}${cat}`);
    const data = await r.json();
    if(reset) list.innerHTML = '';
    if(!data.length && reset){ list.innerHTML=`<div class="empty"><div class="ei">📭</div><p>${u.noNews}</p></div>`; return; }
    data.forEach(a => list.appendChild(newsCard(a)));
    newsOff += data.length;
    const lm = document.getElementById('lmore');
    lm.style.display = data.length>=LIMIT?'block':'none';
    if(data.length>=LIMIT) lm.textContent = u.loadMore;
  } catch {
    if(reset) list.innerHTML=`<div class="empty"><div class="ei">⚠️</div><p>${u.loadError}</p></div>`;
  }
}
function loadMore(){ fetchNews(false); }

// ── Reports — digest PDFs ─────────────────────────────────────
async function fetchReports(){
  const u = UI[lang];
  const el = document.getElementById('rlist');
  el.innerHTML = [1,2,3].map(()=>'<div class="sk sk-rep"></div>').join('');
  try{
    const r = await fetch('/api/webapp/digest_reports?limit=10');
    const reps = await r.json();
    el.innerHTML = '';
    if(!reps.length){ el.innerHTML=`<div class="empty"><div class="ei">📭</div><p>${u.noDataYet}</p></div>`; return; }
    reps.forEach(rep => el.appendChild(buildRepCard(rep)));
  } catch { el.innerHTML=`<div class="empty"><div class="ei">⚠️</div><p>${u.loadError}</p></div>`; }
}

function buildRepCard(rep){
  const u = UI[lang];
  const rt = RTYPES[rep.report_type] || { ua:'📄 Звіт', ru:'📄 Отчёт', en:'📄 Report' };
  const label = rt[lang]||rt.ua;
  const dt = rep.created_at ? new Date(rep.created_at) : null;
  const dateStr = dt
    ? dt.toLocaleDateString(lang==='en'?'en-US':lang==='ru'?'ru-RU':'uk-UA',{day:'numeric',month:'long'})
      +' · '+dt.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})
    : '';
  const pdfUrl = `/api/webapp/digest_reports/${rep.id}/pdf`;

  const card = document.createElement('div');
  card.className = 'rep-card';
  card.innerHTML =
    `<div class="rep-info">
       <div class="rep-type">${esc(label)}</div>
       ${dateStr ? `<div class="rep-date">📅 ${esc(dateStr)}</div>` : ''}
     </div>
     <button class="rep-btn" onclick="openPdf('${esc(pdfUrl)}')">${u.openPdf}</button>`;
  return card;
}

function openPdf(url){
  if(tg) tg.openLink(window.location.origin+url);
  else window.open(url,'_blank');
}

// ── Markets ───────────────────────────────────────────────────
async function fetchMarkets(){
  const u = UI[lang];
  const grid = document.getElementById('mk-grid');
  grid.innerHTML = Array(10).fill('<div class="sk sk-pcard"></div>').join('');
  try{
    const r = await fetch('/api/webapp/markets');
    mkData = await r.json();
    renderGrid(mkData);
  } catch {
    grid.innerHTML=`<div class="empty" style="grid-column:span 2"><div class="ei">⚠️</div><p>${u.loadError}</p></div>`;
  }
}

function renderGrid(data){
  const grid = document.getElementById('mk-grid'); grid.innerHTML='';
  data.forEach(m => {
    const pct = m.change_pct, sign = pct>=0?'+':'';
    const cls = Math.abs(pct)<0.05?'fl':pct>=0?'up':'dn';
    const card = document.createElement('div'); card.className='pcard';
    card.innerHTML=
      `<div class="pcico">${m.emoji}</div>
       <div class="pclbl">${esc(m.label)}</div>
       <div class="pcval">${m.current} <span class="pcunit">${m.unit}</span></div>
       <div class="pcchg ${cls}">${sign}${pct.toFixed(2)}%</div>`;
    card.onclick = () => openMkDetail(m);
    grid.appendChild(card);
  });
}

async function openMkDetail(m){
  currentMkKey = m.key;
  document.getElementById('mk-grid-wrap').style.display = 'none';
  document.getElementById('mk-detail').classList.add('on');

  const chartWrap = document.getElementById('mk-chart-wrap');
  const newsArea  = document.getElementById('mk-news');
  chartWrap.innerHTML = '<div class="sk sk-ch"></div>';
  newsArea.innerHTML  = '<div class="sk sk-card"></div><div class="sk sk-card"></div>';

  const pct = m.change_pct;
  const pctCol = pct>=0 ? '#22C55E' : '#EF4444';
  const pctStr = (pct>=0?'+':'')+pct.toFixed(2)+'%';

  // Chart
  try{
    const r = await fetch('/api/webapp/chart/'+m.key);
    const d = await r.json();
    chartWrap.innerHTML =
      `<div class="chcard">
         <div class="chtitle">
           <span>${esc((d.emoji||m.emoji||'')+' '+(d.label||m.label))}</span>
           <span class="chpct" style="color:${pctCol}">${pctStr}</span>
         </div>
         <canvas id="mkcv" height="150"></canvas>
       </div>`;
    if(detailChart){detailChart.destroy();detailChart=null;}
    const prices = d.prices||[];
    if(prices.length) _drawChart(prices, d.dates, d.unit||'');
  } catch {
    chartWrap.innerHTML=`<div class="empty"><div class="ei">⚠️</div><p>${UI[lang].error}</p></div>`;
  }

  // Related news (same card style as news tab)
  try{
    const cats = TICKER_CATS[m.key]||'global_sources';
    const r = await fetch(`/api/webapp/news?category=${cats}&lang=${lang}&limit=8`);
    const articles = await r.json();
    newsArea.innerHTML = '';
    if(!articles.length){ newsArea.innerHTML=`<div class="empty"><div class="ei">📭</div><p>${UI[lang].noNews}</p></div>`; return; }
    articles.forEach(a => newsArea.appendChild(newsCard(a)));
  } catch {
    newsArea.innerHTML=`<div class="empty"><div class="ei">⚠️</div><p>${UI[lang].loadError}</p></div>`;
  }
}

function _drawChart(prices, dates, unit){
  if(detailChart){detailChart.destroy();detailChart=null;}
  const lineCol = prices[prices.length-1]>=prices[0]?'#22C55E':'#EF4444';
  const tc = light?'#666666':'#8A8A8A';
  const gc = light?'#DDDDDD':'#2A2A2A';
  const bg = light?'#FFFFFF':'#0F0F0F';
  const ctx = document.getElementById('mkcv').getContext('2d');
  detailChart = new Chart(ctx,{
    type:'line',
    data:{labels:dates,datasets:[{data:prices,borderColor:lineCol,backgroundColor:lineCol+'22',borderWidth:2,fill:true,tension:.35,pointRadius:0,pointHitRadius:12}]},
    options:{
      responsive:true,maintainAspectRatio:true,
      plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,backgroundColor:bg,borderColor:gc,borderWidth:1,titleColor:tc,bodyColor:tc,callbacks:{label:c=>`${c.parsed.y.toFixed(2)} ${unit}`}}},
      scales:{x:{grid:{color:gc},ticks:{color:tc,maxTicksLimit:5,maxRotation:0}},y:{grid:{color:gc},ticks:{color:tc,maxTicksLimit:5,callback:v=>v>=1000?Math.round(v/100)/10+'k':v}}}
    }
  });
}

async function redrawDetailChart(key){
  if(!key) return;
  const m = mkData.find(x=>x.key===key); if(!m) return;
  try{
    const r = await fetch('/api/webapp/chart/'+key);
    const d = await r.json();
    if((d.prices||[]).length) _drawChart(d.prices, d.dates, d.unit||'');
  } catch {}
}

function closeMkDetail(){ closeMkDetailSilent(); }
function closeMkDetailSilent(){
  if(detailChart){detailChart.destroy();detailChart=null;}
  document.getElementById('mk-detail').classList.remove('on');
  document.getElementById('mk-grid-wrap').style.display = '';
  currentMkKey = null;
}

// ── Tracking ──────────────────────────────────────────────────
let trkCarrier = null;
let trkMode = null;
let _trkCntLine = null;
let _lastTrkData = null;
let _trkAutoRetryTimer = null;
let _savedShipmentsCache = {active:[], archive:[]};
let _trkCurrentScreen = 'home';
let _trkFilter = 'all';
const _CARRIER_COLORS = {
  'Nova Poshta':'#C8102E','DHL':'#D40511','FedEx':'#4D148C',
  'UPS':'#8B4513','EMS':'#003B7A','Meest':'#E65C00',
  'MSC':'#005798','Maersk':'#42B0D5','CMA CGM':'#0A3161',
  'COSCO':'#003087','Hapag-Lloyd':'#F09800','ONE':'#E4002B',
  'Evergreen':'#00A651','ZIM':'#005DAA','HMM':'#0050A0',
};
function trkSetFilter(carrier, btn){
  _trkFilter = carrier;
  document.querySelectorAll('#trk-filter-row .trk-fchip').forEach(el=>{
    el.classList.remove('on');
    el.style.background='';
    el.style.borderColor='';
  });
  btn.classList.add('on');
  const color = btn.dataset.color || '#4B5563';
  btn.style.background = color;
  btn.style.borderColor = color;
  renderSavedShipments(_savedShipmentsCache);
}

const _TRK_SCREENS = ['trk-home','trk-type','trk-parcel','trk-container','trk-list'];

// ── Screen navigation ─────────────────────────────────────────
function trkNav(screen){
  _TRK_SCREENS.forEach(id => {
    const el = document.getElementById(id);
    if(el) el.classList.remove('active');
  });

  if(screen !== 'list'){
    const dv = document.getElementById('trk-detail-view');
    if(dv) dv.style.display = 'none';
    const lv = document.getElementById('trk-list-view');
    if(lv) lv.style.display = '';
  }

  const idMap = {home:'trk-home',type:'trk-type',parcel:'trk-parcel',container:'trk-container',list:'trk-list'};
  const el = document.getElementById(idMap[screen] || 'trk-home');
  if(el) el.classList.add('active');
  _trkCurrentScreen = screen;

  if(screen === 'parcel'){
    trkMode = 'parcel';
    trkCarrier = null;
    document.querySelectorAll('.trk-car-btn').forEach(b => b.classList.remove('on'));
    document.getElementById('trk-input-wrap').style.display = 'none';
    document.getElementById('trk-result').innerHTML = '';
    if(_trkAutoRetryTimer){ clearTimeout(_trkAutoRetryTimer); _trkAutoRetryTimer = null; }
  } else if(screen === 'container'){
    trkMode = 'container';
    trkCarrier = 'auto';
    _trkCntLine = null;
    document.querySelectorAll('.trk-cnt-btn').forEach(b => b.classList.remove('on'));
    document.getElementById('trk-cnt-input-wrap').style.display = 'none';
    document.getElementById('trk-cnt-result').innerHTML = '';
    if(_trkAutoRetryTimer){ clearTimeout(_trkAutoRetryTimer); _trkAutoRetryTimer = null; }
  } else if(screen === 'list'){
    _trkFilter = 'all';
    loadSavedShipments();
  }
}

function _trkReset(){
  const inp = document.getElementById('trk-num');
  if(inp) inp.value = '';
  const res = document.getElementById('trk-result');
  if(res) res.innerHTML = '';
  if(_trkAutoRetryTimer){ clearTimeout(_trkAutoRetryTimer); _trkAutoRetryTimer = null; }
}

function selectCarrier(btn){
  document.querySelectorAll('.trk-car-btn').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  trkCarrier = btn.dataset.car;
  document.getElementById('trk-input-wrap').style.display = '';
  document.getElementById('trk-num').placeholder = UI[lang].trkPlaceholder;
  _trkReset();
  setTimeout(() => document.getElementById('trk-num').focus(), 80);
}

function selectCntCarrier(btn){
  document.querySelectorAll('.trk-cnt-btn').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  _trkCntLine = btn.textContent.trim();
  document.getElementById('trk-cnt-input-wrap').style.display = '';
  document.getElementById('trk-cnt-num').placeholder = UI[lang].trkCntHint;
  document.getElementById('trk-cnt-result').innerHTML = '';
  setTimeout(() => document.getElementById('trk-cnt-num').focus(), 80);
}

async function doTrackContainer(){
  const raw = document.getElementById('trk-cnt-num').value.trim();
  if(!raw){ document.getElementById('trk-cnt-num').focus(); return; }
  const num = raw.toUpperCase().replace(/[\s\-]/g,'');
  const res = document.getElementById('trk-cnt-result');
  res.innerHTML =
    `<div class="trk-loading">
       <div class="trk-radar">
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-center">🚢</div>
       </div>
       <div class="trk-loading-txt">${esc(UI[lang].trkSearching)}</div>
     </div>`;
  try{
    const r = await fetch(`/api/webapp/track?number=${encodeURIComponent(num)}&carrier=auto`);
    const data = await r.json();
    _lastTrkData = data.ok ? {...data, _cntLine: _trkCntLine} : null;
    res.innerHTML = renderTrackResult(data, num);
    if(data.ok && data.steps && data.steps.length===1 && data.steps[0].status==='pending'){
      _scheduleAutoRetry(num, 'auto', res, 1);
    }
  } catch(e){
    res.innerHTML = `<div class="trk-error"><strong>${esc(UI[lang].trkNetError)}</strong>${esc(String(e))}</div>`;
  }
}

function setTrkMode(mode){ /* legacy — handled by trkNav now */ }

async function doTrack(){
  const raw = document.getElementById('trk-num').value.trim();
  if(!raw){ document.getElementById('trk-num').focus(); return; }
  const num = raw.toUpperCase().replace(/[\s\-]/g,'');
  const res = document.getElementById('trk-result');
  const ico = trkMode==='container' ? '🚢' : '📦';
  res.innerHTML =
    `<div class="trk-loading">
       <div class="trk-radar">
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-ring"></div>
         <div class="trk-radar-center">${ico}</div>
       </div>
       <div class="trk-loading-txt">${esc(UI[lang].trkSearching)}</div>
     </div>`;

  const carrier = trkMode==='container' ? 'auto' : trkCarrier;
  try{
    const r = await fetch(`/api/webapp/track?number=${encodeURIComponent(num)}&carrier=${carrier}`);
    const data = await r.json();
    res.innerHTML = renderTrackResult(data, num);
    // Auto-retry if data is still pending (max 3 attempts)
    if(data.ok && data.steps && data.steps.length === 1 && data.steps[0].status === 'pending'){
      _scheduleAutoRetry(num, carrier, res, 1);
    }
  } catch(e) {
    res.innerHTML = `<div class="trk-error"><strong>${esc(UI[lang].trkNetError)}</strong>${esc(String(e))}</div>`;
  }
}

const TRK_MAX_RETRIES = 3;

function _scheduleAutoRetry(num, carrier, resEl, attempt){
  if(_trkAutoRetryTimer){ clearTimeout(_trkAutoRetryTimer); }

  // After max retries — show "saved, will check in background" and stop
  if(attempt > TRK_MAX_RETRIES){
    const hint = UI[lang].trkMaxRetriesHint;
    // Preserve current content and just add/update the auto-refresh note
    const existing = document.getElementById('trk-auto-countdown');
    if(existing){
      const note = existing.closest('.trk-auto-refresh');
      if(note) note.innerHTML = '⏸ ' + esc(hint);
    }
    return;
  }

  let secs = 45;
  const countEl = document.getElementById('trk-auto-countdown');
  if(countEl) countEl.textContent = secs + UI[lang].trkSec;
  const tick = setInterval(()=>{
    secs--;
    const el = document.getElementById('trk-auto-countdown');
    if(el) el.textContent = secs + UI[lang].trkSec;
    if(secs <= 0) clearInterval(tick);
  }, 1000);

  _trkAutoRetryTimer = setTimeout(async ()=>{
    clearInterval(tick);
    if(!resEl.isConnected) return;
    const ico = carrier==='auto' || trkMode==='container' ? '🚢' : '📦';
    resEl.innerHTML =
      `<div class="trk-loading">
         <div class="trk-radar">
           <div class="trk-radar-ring"></div>
           <div class="trk-radar-ring"></div>
           <div class="trk-radar-ring"></div>
           <div class="trk-radar-center">${ico}</div>
         </div>
         <div class="trk-loading-txt">${esc(UI[lang].trkSearching)}</div>
       </div>`;
    try{
      const r2 = await fetch(`/api/webapp/track?number=${encodeURIComponent(num)}&carrier=${carrier}`);
      const d2 = await r2.json();
      resEl.innerHTML = renderTrackResult(d2, num);
      if(d2.ok && d2.steps && d2.steps.length === 1 && d2.steps[0].status === 'pending'){
        _scheduleAutoRetry(num, carrier, resEl, attempt + 1);
      }
    } catch {}
  }, 45000);
}

function renderTrackResult(d, num){
  _lastTrkData = d.ok ? d : null;
  if(!d.ok){
    const hint = d.hint ? `<code>${esc(d.hint)}</code>` : '';
    return `<div class="trk-error"><strong>⚠️ ${esc(d.error||'Помилка')}</strong>${hint}</div>`;
  }

  let html = '';

  // ── Container ──────────────────────────────────────────────────────────────
  if(d.type === 'container'){
    const lineName = d.line || d.carrier || '';
    html += `<div class="trk-delivery">
      <div class="trk-del-ico">🚢</div>
      <div class="trk-del-info">
        <div class="trk-del-label">${esc(lineName)}</div>
        <div class="trk-del-date">${esc(d.number||num)}</div>
      </div>
    </div>`;
    if(d.tracking_url){
      html += `<button class="trk-open-btn" data-url="${esc(d.tracking_url)}" onclick="openTrkUrl(this.dataset.url)">${esc(UI[lang].trkOpenSite)}</button>`;
    }
  }

  // ── Parcel ─────────────────────────────────────────────────────────────────
  if(d.type === 'parcel'){
    const carrierLabel = d.carrier || '';
    const scheduled = d.scheduled_delivery || '';
    html += `<div class="trk-delivery">
      <div class="trk-del-ico">📦</div>
      <div class="trk-del-info">
        <div class="trk-del-label">${esc(carrierLabel)} · ${esc(d.number||num)}</div>
        <div class="trk-del-date">${esc(d.status||'')}</div>
        ${scheduled?`<div class="trk-del-label" style="margin-top:3px">${esc(UI[lang].trkDelivery)}: ${esc(scheduled)}</div>`:''}
      </div>
    </div>`;
  }

  // ── Steps timeline ─────────────────────────────────────────────────────────
  const steps = d.steps || [];
  const isPending = steps.length === 1 && steps[0].status === 'pending';
  if(steps.length){
    html += '<div class="trk-timeline">';
    steps.forEach(s => {
      const dim  = (s.status==='pending'||s.status==='fail') ? ' dim' : '';
      html += `<div class="trk-step">
        <div class="trk-dot ${s.status||'pending'}">${s.icon||'📍'}</div>
        <div class="trk-step-info">
          <div class="trk-step-title${dim}">${esc(s.title||'')}</div>
          ${s.time ? `<div class="trk-step-time">🕐 ${esc(s.time)}</div>` : ''}
          ${s.desc ? `<div class="trk-step-desc">${esc(s.desc)}</div>` : ''}
        </div>
      </div>`;
    });
    html += '</div>';
  }

  // Auto-retry countdown badge (shown when pending)
  if(isPending){
    html += `<div class="trk-auto-refresh"><strong id="trk-auto-countdown">45с</strong></div>`;
  }

  html += `<button class="trk-save-btn" onclick="saveTrkShipment()">${UI[lang].trkSave}</button>`;
  return html;
}

function openTrkUrl(url){
  if(tg) tg.openLink(url);
  else window.open(url, '_blank');
}

// ── User ID ───────────────────────────────────────────────────
function getTrkUserId(){
  const uid = tg?.initDataUnsafe?.user?.id;
  if(uid) return uid;
  let luid = localStorage.getItem('trk_uid');
  if(!luid){ luid = String(Date.now()); localStorage.setItem('trk_uid', luid); }
  return parseInt(luid);
}

// ── Save shipment ─────────────────────────────────────────────
async function saveTrkShipment(){
  if(!_lastTrkData?.ok) return;
  const d = _lastTrkData;
  const uid = getTrkUserId();
  const body = {
    user_id: uid,
    number: d.number || '',
    carrier: trkCarrier || 'auto',
    type: trkMode || 'parcel',
    carrier_name: d.carrier || d.line || _trkCntLine || '',
    status_text: d.status || '',
    tracking_url: d.tracking_url || '',
    steps: d.steps || [],
  };
  const btn = document.querySelector('.trk-save-btn');
  if(btn){ btn.disabled = true; btn.textContent = '…'; }
  try{
    const r = await fetch('/api/webapp/track/save',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    const resp = await r.json();
    if(resp.ok){
      if(btn){ btn.textContent = UI[lang].trkSaved; }
      trkNav('list');
    } else {
      if(btn){ btn.disabled=false; btn.textContent=UI[lang].trkSave; }
    }
  } catch {
    if(btn){ btn.disabled=false; btn.textContent=UI[lang].trkSave; }
  }
}

// ── Remove shipment ───────────────────────────────────────────
async function removeTrkShipment(number, ev){
  ev.stopPropagation();
  const uid = getTrkUserId();
  try{
    await fetch(`/api/webapp/track/remove?user_id=${uid}&number=${encodeURIComponent(number)}`,
      {method:'DELETE'});
    // Refresh list or go back if we're in detail view for this number
    if(document.getElementById('trk-detail-view').style.display !== 'none'){
      closeDetail();
    } else {
      loadSavedShipments();
    }
  } catch {}
}

// ── Load & render saved list ──────────────────────────────────
async function loadSavedShipments(){
  const uid = getTrkUserId();
  try{
    const r = await fetch(`/api/webapp/track/list?user_id=${uid}`);
    const data = await r.json();
    _savedShipmentsCache = data;
    renderSavedShipments(data);
  } catch { document.getElementById('trk-saved').innerHTML = ''; }
}

function renderSavedShipments(data){
  const el = document.getElementById('trk-saved');
  const active  = data.active  || [];
  const archive = data.archive || [];
  const u = UI[lang];

  // Render filter chips
  const filterRow = document.getElementById('trk-filter-row');
  if(filterRow){
    const carriers = [...new Set(active.map(s=>s.carrier_name).filter(Boolean))];
    if(carriers.length > 1){
      let fhtml = `<button class="trk-fchip${_trkFilter==='all'?' on':''}" style="${_trkFilter==='all'?'background:#4B5563;border-color:#4B5563':''}" data-color="#4B5563" onclick="trkSetFilter('all',this)">📋 ${u.trkAll||'Всі'}</button>`;
      carriers.forEach(c=>{
        const color = _CARRIER_COLORS[c] || '#4B5563';
        const isOn = _trkFilter === c;
        fhtml += `<button class="trk-fchip${isOn?' on':''}" style="${isOn?`background:${color};border-color:${color}`:''}" data-color="${esc(color)}" onclick="trkSetFilter(${JSON.stringify(c)},this)">${esc(c)}</button>`;
      });
      filterRow.innerHTML = fhtml;
      filterRow.style.display = '';
    } else {
      filterRow.style.display = 'none';
    }
  }

  // Apply carrier filter
  const activeWithIdx = active.map((s,i)=>({s,idx:i}));
  const filtered = _trkFilter === 'all' ? activeWithIdx : activeWithIdx.filter(({s})=>s.carrier_name===_trkFilter);

  if(!filtered.length && !archive.length){
    el.innerHTML = `<div class="trk-list-empty">${esc(u.trkEmptyList||u.trkNoSaved)}</div>`;
    return;
  }

  function fmtAgo(iso){
    if(!iso) return '';
    const s = (Date.now() - new Date(iso)) / 1000;
    if(s < 60)    return u.trkUpdated+': '+u.trkNow;
    if(s < 3600)  return u.trkUpdated+': '+Math.floor(s/60)+' '+u.trkMin;
    if(s < 86400) return u.trkUpdated+': '+Math.floor(s/3600)+' '+u.trkHour;
    return u.trkUpdated+': '+new Date(iso).toLocaleDateString(
      lang==='en'?'en-US':lang==='ru'?'ru-RU':'uk-UA',{day:'numeric',month:'short'});
  }

  function card(s, isArchive, idx){
    const ico = s.type==='container' ? '🚢' : '📦';
    const cls = isArchive ? ' arc' : '';
    const isPending = !s.steps || !s.steps.length ||
      (s.steps.length === 1 && s.steps[0].status === 'pending');
    const stat = isPending ? '—' : (s.status_text || '—');
    const time = isArchive
      ? (s.delivered_at ? '✅ '+new Date(s.delivered_at).toLocaleDateString(
          lang==='en'?'en-US':lang==='ru'?'ru-RU':'uk-UA',{day:'numeric',month:'short'}) : '')
      : fmtAgo(s.last_checked);
    const cname = s.carrier_name || '';
    const ccolor = cname ? (_CARRIER_COLORS[cname] || '#4B5563') : '';
    const badge = cname ? `<span class="trk-sv-badge" style="background:${ccolor}">${esc(cname)}</span>` : '';
    return `<div class="trk-sv-card${cls}" onclick="openDetailByIdx(${idx})">
      <div class="trk-sv-ico">${ico}</div>
      <div class="trk-sv-info">
        ${badge}
        <div class="trk-sv-num">${esc(s.number)}</div>
        <div class="trk-sv-stat">${esc(stat)}</div>
        ${time?`<div class="trk-sv-time">${esc(time)}</div>`:''}
      </div>
      <button class="trk-sv-del" onclick="removeTrkShipment('${esc(s.number)}',event)" title="${esc(u.trkDelTitle)}">✕</button>
    </div>`;
  }

  let html = '<div class="trk-saved-wrap">';
  if(_trkFilter === 'all'){
    // Group by carrier when showing all
    const groups = {};
    filtered.forEach(({s,idx}) => {
      const key = s.carrier_name || '—';
      if(!groups[key]) groups[key] = [];
      groups[key].push({s, idx});
    });
    for(const [cname, items] of Object.entries(groups)){
      const ico = items[0].s.type === 'container' ? '🚢' : '📦';
      html += `<div class="trk-sec-hdr">${ico} ${esc(cname)}<span class="trk-sec-cnt">${items.length}</span></div>`;
      html += '<div class="trk-saved-list">'+items.map(({s,idx})=>card(s,false,idx)).join('')+'</div>';
    }
  } else {
    // Filtered view — flat list, no group header
    html += '<div class="trk-saved-list">'+filtered.map(({s,idx})=>card(s,false,idx)).join('')+'</div>';
  }
  if(archive.length){
    html += `<div class="trk-sec-hdr">${u.trkArchive}<span class="trk-sec-cnt">${archive.length}</span></div>`;
    html += '<div class="trk-saved-list">'+archive.map((s,i)=>card(s,true,active.length+i)).join('')+'</div>';
  }
  html += '</div>';
  el.innerHTML = html;
}

// ── Detail view (cached, no API call) ────────────────────────
let _currentDetailNumber = null;

function openDetailByIdx(idx){
  const all = [...(_savedShipmentsCache.active||[]), ...(_savedShipmentsCache.archive||[])];
  const shipment = all[idx];
  if(shipment) openDetail(shipment);
}

function openDetail(shipment){
  _currentDetailNumber = shipment.number;
  document.getElementById('trk-list-view').style.display = 'none';
  document.getElementById('trk-detail-view').style.display = '';
  renderDetailContent(shipment);
}

function closeDetail(){
  _currentDetailNumber = null;
  document.getElementById('trk-detail-view').style.display = 'none';
  document.getElementById('trk-list-view').style.display = '';
  loadSavedShipments();
}

function renderDetailContent(s, refreshing){
  const u = UI[lang];
  const steps = s.steps || [];
  const ico = s.type === 'container' ? '🚢' : '📦';
  const cname = s.carrier_name || '';
  const numLabel = cname ? `${s.number} · ${cname}` : s.number;

  let html = '';
  // Header card
  html += `<div class="trk-delivery">
    <div class="trk-del-ico">${ico}</div>
    <div class="trk-del-info">
      <div class="trk-del-label">${esc(numLabel)}</div>
      <div class="trk-del-date">${esc(s.status_text||'—')}</div>
    </div>
  </div>`;

  // Steps timeline from cache (skip for pending/unregistered shipments)
  const hasMeaningfulSteps = steps.length > 0 &&
    !(steps.length === 1 && steps[0].status === 'pending');
  if(hasMeaningfulSteps){
    html += '<div class="trk-timeline">';
    steps.forEach(st => {
      const dim = (st.status==='pending'||st.status==='fail') ? ' dim' : '';
      html += `<div class="trk-step">
        <div class="trk-dot ${st.status||'pending'}">${st.icon||'📍'}</div>
        <div class="trk-step-info">
          <div class="trk-step-title${dim}">${esc(st.title||'')}</div>
          ${st.time ? `<div class="trk-step-time">🕐 ${esc(st.time)}</div>` : ''}
          ${st.desc ? `<div class="trk-step-desc">${esc(st.desc)}</div>` : ''}
        </div>
      </div>`;
    });
    html += '</div>';
  }

  // Tracking URL button for containers
  if(s.tracking_url){
    html += `<button class="trk-open-btn" data-url="${esc(s.tracking_url)}" onclick="openTrkUrl(this.dataset.url)">${esc(u.trkOpenSite)}</button>`;
  }

  // Refresh button
  html += `<button class="trk-refresh-btn" id="trk-refresh-btn" onclick="refreshDetail('${esc(s.number)}','${esc(s.carrier)}','${esc(s.type)}')" ${refreshing?'disabled':''}>
    ${refreshing ? '⏳ …' : esc(u.trkRefresh)}
  </button>`;

  // Remove button (red)
  html += `<button class="trk-open-btn" style="margin-top:8px;background:var(--red);border-color:var(--red);color:#fff"
    onclick="removeTrkShipment('${esc(s.number)}',event)">${esc(u.trkRemove)}</button>`;

  document.getElementById('trk-detail-content').innerHTML = html;
}

async function refreshDetail(number, carrier, type){
  const btn = document.getElementById('trk-refresh-btn');
  if(btn){ btn.disabled=true; btn.textContent='⏳ …'; }

  // Find the cached shipment to keep its data while loading
  const all = [...(_savedShipmentsCache.active||[]), ...(_savedShipmentsCache.archive||[])];
  const cached = all.find(s=>s.number===number) || {number, carrier, type, status_text:'', steps:[]};
  renderDetailContent(cached, true);

  try{
    const r = await fetch(`/api/webapp/track?number=${encodeURIComponent(number)}&carrier=${carrier}`);
    const data = await r.json();
    if(data.ok){
      // Update DB with fresh data
      const uid = getTrkUserId();
      await fetch('/api/webapp/track/save',{
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          user_id: uid,
          number: data.number || number,
          carrier: carrier,
          type: type,
          carrier_name: data.carrier || data.line || cached.carrier_name || '',
          status_text: data.status || '',
          tracking_url: data.tracking_url || cached.tracking_url || '',
          steps: data.steps || [],
        }),
      });
      // Re-fetch from DB and show updated
      const r2 = await fetch(`/api/webapp/track/list?user_id=${uid}`);
      const listData = await r2.json();
      _savedShipmentsCache = listData;
      const all2 = [...(listData.active||[]), ...(listData.archive||[])];
      const updated = all2.find(s=>s.number===number) || {number, carrier, type, status_text: data.status||'', steps: data.steps||[]};
      renderDetailContent(updated, false);
    } else {
      renderDetailContent(cached, false);
    }
  } catch {
    renderDetailContent(cached, false);
  }
}

// ── Escape ─────────────────────────────────────────────────────
function esc(s){ return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
</script>
</body>
</html>
"""


@app.get("/webapp")
async def serve_webapp():
    path = os.path.join(_BASE_DIR, "webapp.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return HTMLResponse(content=_WEBAPP_HTML, status_code=200)


@app.get("/api/webapp/news")
def api_news(category: str = "all", lang: str = "ua", limit: int = 15, offset: int = 0):
    limit = min(limit, 50)
    conn = get_db_connection()
    cursor = conn.cursor()
    excluded = list(INTERNAL_CATEGORIES)
    base_cols = "id, title, title_ua, title_ru, link, published, category, summary_en, summary_ua, summary_ru, image_url"
    if category == "all":
        if excluded:
            ph = ",".join(["%s"] * len(excluded))
            rows = db_fetchall(cursor,
                f"SELECT {base_cols} FROM articles "
                f"WHERE category NOT IN ({ph}) "
                f"ORDER BY published DESC LIMIT %s OFFSET %s",
                (*excluded, limit, offset),
            )
        else:
            rows = db_fetchall(cursor,
                f"SELECT {base_cols} FROM articles ORDER BY published DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
    else:
        # Support comma-separated categories, e.g. "food,feed,cosmetic"
        cats = [c.strip() for c in category.split(",") if c.strip()]
        if len(cats) == 1:
            rows = db_fetchall(cursor,
                f"SELECT {base_cols} FROM articles WHERE category = %s "
                f"ORDER BY published DESC LIMIT %s OFFSET %s",
                (cats[0], limit, offset),
            )
        else:
            ph = ",".join(["%s"] * len(cats))
            rows = db_fetchall(cursor,
                f"SELECT {base_cols} FROM articles WHERE category IN ({ph}) "
                f"ORDER BY published DESC LIMIT %s OFFSET %s",
                (*cats, limit, offset),
            )
    conn.close()
    return rows


@app.get("/api/webapp/report_days")
def api_report_days():
    """Return unique dates (last 30 days) with article counts, newest first."""
    conn = get_db_connection()
    cursor = conn.cursor()
    excluded = list(INTERNAL_CATEGORIES | NON_REPORT_CATEGORIES)
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    ph = ",".join(["%s"] * len(excluded)) if excluded else "'__none__'"
    params = (*excluded, cutoff) if excluded else (cutoff,)
    rows = db_fetchall(cursor,
        f"""
        SELECT
            LEFT(published, 10) AS date,
            COUNT(*) AS count
        FROM articles
        WHERE category NOT IN ({ph})
          AND published >= %s
        GROUP BY LEFT(published, 10)
        ORDER BY date DESC
        LIMIT 30
        """,
        params,
    )
    conn.close()
    return rows


@app.get("/api/webapp/report/{date}")
def api_report_date(date: str, lang: str = "ua"):
    """Return articles for a given date (YYYY-MM-DD) grouped by category."""
    # Validate date format to prevent injection
    import re as _re
    if not _re.match(r'^\d{4}-\d{2}-\d{2}$', date):
        raise HTTPException(status_code=400, detail="Invalid date format")
    conn = get_db_connection()
    cursor = conn.cursor()
    excluded = list(INTERNAL_CATEGORIES | NON_REPORT_CATEGORIES)
    ph = ",".join(["%s"] * len(excluded)) if excluded else "'__none__'"
    params = (*excluded, date, date)
    rows = db_fetchall(cursor,
        f"""
        SELECT title, title_ua, title_ru, link, published, category, summary_en, summary_ua, summary_ru
        FROM articles
        WHERE category NOT IN ({ph})
          AND published >= %s
          AND published < (%s::date + INTERVAL '1 day')::text
        ORDER BY category, published DESC
        """,
        params,
    )
    conn.close()

    report_cat_order = ["api","cosmetic","herbal","veterinary","food","feed",
                        "capsules","pvc","logistics","global_sources"]
    by_cat: dict[str, list] = {c: [] for c in report_cat_order}
    for r in rows:
        cat = r["category"]
        if cat in by_cat:
            by_cat[cat].append(r)
        else:
            by_cat.setdefault(cat, []).append(r)
    # Remove empty
    by_cat = {k: v for k, v in by_cat.items() if v}
    return {"date": date, "by_category": by_cat}


@app.get("/api/webapp/digest_reports")
def api_digest_reports(limit: int = 50):
    """List saved digest reports (morning/midday/weekly PDFs), newest first."""
    conn = get_db_connection()
    cursor = conn.cursor()
    rows = db_fetchall(cursor,
        "SELECT id, report_type, title, created_at FROM digest_reports ORDER BY created_at DESC LIMIT %s",
        (min(limit, 100),)
    )
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "report_type": r["report_type"],
            "title": r["title"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })
    return result


@app.get("/api/webapp/digest_reports/{report_id}/pdf")
def api_digest_report_pdf(report_id: int):
    """Serve a saved digest report PDF by ID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    rows = db_fetchall(cursor,
        "SELECT pdf_data, report_type FROM digest_reports WHERE id = %s",
        (report_id,)
    )
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="Report not found")
    r = rows[0]
    pdf_data = bytes(r["pdf_data"])
    fname = f"{r['report_type']}_{report_id}.pdf"
    return Response(
        content=pdf_data,
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=\"{fname}\""}
    )


# ── Markets price cache (15 min TTL) ──────────────────────────
_mk_cache: dict = {"data": None, "ts": 0.0}
_MK_TTL = 900


@app.get("/api/webapp/markets")
async def api_markets():
    """Return current intraday prices for all CHART_TICKERS."""
    import time as _time
    now = _time.time()
    if _mk_cache["data"] and (now - _mk_cache["ts"]) < _MK_TTL:
        return _mk_cache["data"]

    if not CHARTS_AVAILABLE:
        raise HTTPException(status_code=503, detail="yfinance not available")

    result = []
    for key, cfg in CHART_TICKERS.items():
        try:
            info = await asyncio.to_thread(_get_intraday_price_info, cfg["tickers"])
        except Exception:
            info = None
        result.append({
            "key":        key,
            "label":      cfg["label"],
            "emoji":      cfg["emoji"],
            "unit":       cfg["unit"],
            "current":    info["current"] if info else 0,
            "change_pct": info["change_pct"] if info else 0.0,
            "as_of":      info["as_of"] if info else "",
        })

    _mk_cache["data"] = result
    _mk_cache["ts"] = now
    return result


# ── Chart history cache (1 h TTL) ─────────────────────────────
_ch_cache: dict = {}
_CH_TTL = 3600


@app.get("/api/webapp/chart/{key}")
async def api_chart(key: str, days: int = 30):
    """Return daily close prices (last `days` days) for a CHART_TICKERS key."""
    import time as _time
    if key not in CHART_TICKERS:
        raise HTTPException(status_code=404, detail="Unknown commodity key")

    now = _time.time()
    cached = _ch_cache.get(key)
    if cached and (now - cached["ts"]) < _CH_TTL:
        return cached["data"]

    if not CHARTS_AVAILABLE:
        raise HTTPException(status_code=503, detail="yfinance not available")

    cfg = CHART_TICKERS[key]

    def _fetch():
        for sym in cfg["tickers"]:
            try:
                import yfinance as yf
                tk = yf.Ticker(sym)
                df = tk.history(period=f"{days+5}d", interval="1d")
                if df is not None and not df.empty:
                    df = df.tail(days)
                    dates  = [d.strftime("%d.%m") for d in df.index]
                    prices = [round(float(v), 2) for v in df["Close"].values]
                    return {"dates": dates, "prices": prices}
            except Exception:
                continue
        return None

    raw = await asyncio.to_thread(_fetch)
    if raw is None:
        raise HTTPException(status_code=503, detail="No data available")

    data = {
        "key":    key,
        "label":  cfg["label"],
        "emoji":  cfg["emoji"],
        "unit":   cfg["unit"],
        "dates":  raw["dates"],
        "prices": raw["prices"],
    }
    _ch_cache[key] = {"data": data, "ts": now}
    return data


# ─── PARCEL & CONTAINER TRACKING ─────────────────────────────────────────────
import re as _tre

NOVA_POSHTA_API_KEY   = os.getenv("NOVA_POSHTA_API_KEY", "")
SEVENTEEN_TRACK_KEY   = os.getenv("SEVENTEEN_TRACK_KEY", "")   # https://17track.net/en/apiDoc

# ISO 6346 prefix → shipping line name + tracking URL template
_CONTAINER_LINES: dict[str, tuple[str, str]] = {
    "MAEU": ("Maersk",        "https://www.maersk.com/tracking/{n}"),
    "MSKU": ("Maersk",        "https://www.maersk.com/tracking/{n}"),
    "MRKU": ("Maersk",        "https://www.maersk.com/tracking/{n}"),
    "MSCU": ("MSC",           "https://www.msc.com/track-a-shipment?trackingNumber={n}"),
    "MEDU": ("MSC",           "https://www.msc.com/track-a-shipment?trackingNumber={n}"),
    "MSDU": ("MSC",           "https://www.msc.com/track-a-shipment?trackingNumber={n}"),
    "CMAU": ("CMA CGM",       "https://www.cma-cgm.com/ebusiness/tracking/search?SearchBy=Container&Reference={n}"),
    "CGMU": ("CMA CGM",       "https://www.cma-cgm.com/ebusiness/tracking/search?SearchBy=Container&Reference={n}"),
    "APLU": ("APL / CMA CGM", "https://www.cma-cgm.com/ebusiness/tracking/search?SearchBy=Container&Reference={n}"),
    "HLCU": ("Hapag-Lloyd",   "https://www.hapag-lloyd.com/en/online-business/track/track-by-container-solution.html?container={n}"),
    "HLBU": ("Hapag-Lloyd",   "https://www.hapag-lloyd.com/en/online-business/track/track-by-container-solution.html?container={n}"),
    "OOLU": ("OOCL",          "https://www.oocl.com/eng/ourservices/eservices/cargotracking/Pages/cargotracking.aspx?ContainerNumber={n}"),
    "OCLU": ("OOCL",          "https://www.oocl.com/eng/ourservices/eservices/cargotracking/Pages/cargotracking.aspx?ContainerNumber={n}"),
    "EGLV": ("Evergreen",     "https://www.evergreen-line.com/static/jsp/tracking.jsp?cn={n}"),
    "EGHU": ("Evergreen",     "https://www.evergreen-line.com/static/jsp/tracking.jsp?cn={n}"),
    "COSU": ("COSCO",         "https://elines.coscoshipping.com/ebusiness/cargoTracking?trackingType=CONTAINER&number={n}"),
    "CBHU": ("COSCO",         "https://elines.coscoshipping.com/ebusiness/cargoTracking?trackingType=CONTAINER&number={n}"),
    "YMLU": ("Yang Ming",     "https://www.yangming.com/e-service/Track_Trace/track_trace_cargo_tracking.aspx?query_type=1&bl_no={n}"),
    "YMMU": ("Yang Ming",     "https://www.yangming.com/e-service/Track_Trace/track_trace_cargo_tracking.aspx?query_type=1&bl_no={n}"),
    "ONEY": ("ONE",           "https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking?trkQry={n}"),
    "ONEU": ("ONE",           "https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking?trkQry={n}"),
    "ZIMU": ("ZIM",           "https://www.zim.com/tools/track-a-shipment?num={n}"),
    "NYKU": ("NYK",           "https://www.nyk.com/english/container/tracking/?type=CN&num={n}"),
}
_FALLBACK_TRACK_URL = "https://www.track-trace.com/container#{n}"

def _is_container(num: str) -> bool:
    return bool(_tre.match(r'^[A-Z]{4}[0-9]{7}$', num))

def _is_nova_poshta(num: str) -> bool:
    return bool(_tre.match(r'^59\d{12}$', num) or _tre.match(r'^\d{14}$', num))

def _container_info(num: str) -> tuple[str, str]:
    prefix = num[:4]
    line, url_tpl = _CONTAINER_LINES.get(prefix, ("Unknown carrier", _FALLBACK_TRACK_URL))
    return line, url_tpl.format(n=num)


async def _track_nova_poshta(number: str) -> dict:
    if not NOVA_POSHTA_API_KEY:
        return {
            "ok": False,
            "error": "Nova Poshta API key not set",
            "hint": "Додайте NOVA_POSHTA_API_KEY у .env (безкоштовно: developers.novaposhta.ua)",
        }
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.post(
                "https://api.novaposhta.ua/v2.0/json/",
                json={
                    "apiKey": NOVA_POSHTA_API_KEY,
                    "modelName": "TrackingDocument",
                    "calledMethod": "getStatusDocuments",
                    "methodProperties": {"Documents": [{"DocumentNumber": number}]},
                },
            )
            data = r.json()
    except Exception as e:
        return {"ok": False, "error": f"Network error: {e}"}

    if not data.get("success") or not data.get("data"):
        errs = data.get("errors", [])
        return {"ok": False, "error": errs[0] if errs else "Not found"}

    doc = data["data"][0]
    status_code = str(doc.get("StatusCode", ""))
    status_desc = doc.get("StatusDescription", "") or doc.get("Status", "") or "—"

    # Service type → delivery method
    # WarehouseWarehouse = відділення→відділення
    # WarehouseAddress   = відділення→адреса (кур'єр до отримувача)
    # AddressWarehouse   = адреса→відділення
    # AddressAddress     = адреса→адреса (кур'єр від і до)
    service_type = doc.get("ServiceType", "")
    courier_to_recipient = service_type.endswith("Address")

    # Comprehensive status code map: (icon, label, is_success, is_fail)
    # StatusCode 14 = "Вручено отримувачу" (courier delivery), NOT "Відмова"
    _NP_STATUS: dict[str, tuple[str, str, bool, bool]] = {
        "1":   ("📦", "Замовлення прийнято",               False, False),
        "2":   ("🗑️", "Видалено",                          False, True),
        "3":   ("❓", "Не знайдено",                        False, True),
        "4":   ("🚚", "В дорозі",                           False, False),
        "5":   ("🏪", "Прибуло на відділення",              False, False),
        "6":   ("⏳", "На зберіганні",                      False, False),
        "7":   ("💳", "Очікує залучення коштів",            False, False),
        "8":   ("↩️", "Повернення",                         False, True),
        "9":   ("✅", "Вручено",                             True,  False),
        "10":  ("↪️", "Переадресовано",                     False, False),
        "11":  ("🔄", "Невдала спроба вручення",            False, False),
        "14":  ("✅", "Вручено отримувачу",                 True,  False),
        "41":  ("📝", "Попереднє замовлення",               False, False),
        "101": ("📦", "Замовлення прийнято",                False, False),
        "102": ("🏭", "Відправлено",                        False, False),
        "103": ("🚚", "В дорозі",                           False, False),
        "104": ("🏙️", "Прибуло в місто отримувача",         False, False),
        "105": ("🚫", "Відмова від отримання",              False, True),
        "106": ("↩️", "Повернення відправнику",             False, True),
        "107": ("🏠", "Вручено кур'єром",                  True,  False),
        "108": ("↪️", "Переадресовано",                    False, False),
        "110": ("🔄", "Невдала спроба вручення кур'єром",  False, False),
    }

    icon, mapped_label, is_success, is_fail = _NP_STATUS.get(
        status_code, ("📦", status_desc, False, False)
    )

    # For delivered-via-courier statuses — use more descriptive label
    if status_code in ("9", "14") and courier_to_recipient:
        icon, mapped_label = "🏠", "Вручено кур'єром"

    # Prefer API description when it's non-trivial
    display_label = status_desc if status_desc and status_desc != "—" else mapped_label

    def _fmt_np_date(raw: str) -> str:
        """Parse Nova Poshta date (DD.MM.YYYY HH:MM:SS or ISO) → readable string."""
        if not raw:
            return ""
        for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.datetime.strptime(raw.strip(), fmt)
                return dt.strftime("%d.%m.%Y %H:%M") if (
                    "H" in fmt or "T" in fmt
                ) else dt.strftime("%d.%m.%Y")
            except ValueError:
                continue
        return raw  # return as-is if parsing fails

    date_created  = _fmt_np_date(doc.get("DateCreated", ""))
    date_scan     = _fmt_np_date(doc.get("DateScan", ""))
    date_actual   = _fmt_np_date(doc.get("ActualDeliveryDate", ""))
    date_sched    = doc.get("ScheduledDeliveryDate", "")

    city_sender    = doc.get("CitySender", "") or doc.get("CitySenderDescription", "")
    city_recipient = doc.get("CityRecipient", "") or doc.get("CityRecipientDescription", "")
    branch_sender  = doc.get("WarehouseSender", "") or doc.get("WarehouseSenderDescription", "")
    branch_recip   = doc.get("WarehouseRecipient", "") or doc.get("WarehouseRecipientDescription", "")

    # ── Build timeline steps ──────────────────────────────────────────────────
    steps = []

    # Step: Created
    steps.append({
        "status": "done",
        "icon": "📦",
        "title": "Замовлення прийнято",
        "desc": city_sender + (f" · {branch_sender}" if branch_sender else ""),
        "time": date_created,
    })

    # Step: Sent (add only if we're past the "accepted" stage)
    _past_sent = {"4","5","6","7","9","10","11","14",
                  "103","104","105","106","107","108","110"}
    if status_code in _past_sent:
        steps.append({
            "status": "done",
            "icon": "🚛",
            "title": "Відправлено",
            "desc": city_sender,
            "time": "",
        })

    # Step: In transit (add if we're at branch-arrival or later)
    _past_transit = {"5","6","7","9","10","11","14",
                     "104","105","106","107","108","110"}
    if status_code in _past_transit and status_code not in ("5","104"):
        steps.append({
            "status": "done",
            "icon": "🚚",
            "title": "В дорозі",
            "desc": "",
            "time": "",
        })

    # Step: Current status (main event) — skip if it duplicates "created"
    if status_code not in ("1", "41", "101"):
        step_st = "fail" if is_fail else ("done" if is_success else "active")
        steps.append({
            "status": step_st,
            "icon": icon,
            "title": display_label,
            "desc": (city_recipient + (f" · {branch_recip}" if branch_recip else "")) or "",
            "time": date_actual or date_scan,
        })

    # Step: Pending delivery (only if not yet terminal)
    if not is_success and not is_fail:
        if courier_to_recipient:
            pending_label = "Вручення кур'єром"
        else:
            pending_label = "Готово до отримання у відділенні"
        steps.append({
            "status": "pending",
            "icon": "🏁",
            "title": pending_label,
            "desc": f"Очікується: {date_sched}" if date_sched else "Очікується",
            "time": "",
        })

    return {
        "ok": True,
        "type": "parcel",
        "carrier": "Нова Пошта",
        "number": number,
        "status": display_label,
        "city_sender": city_sender,
        "city_recipient": city_recipient,
        "scheduled_delivery": date_sched,
        "actual_delivery": date_actual,
        "steps": steps,
    }


def _parse_17track_v24(data: dict, number: str) -> dict | None:
    """
    Parse 17track API v2.4 response (gettrackinfo OR getrealtimetrackinfo).
    Returns None → caller should retry with carrier_code=0.
    Returns dict with ok=True/False.
    """
    if data.get("code") != 0:
        return {
            "ok": False,
            "error": data.get("message") or data.get("msg") or "API error",
        }

    payload  = data.get("data") or {}
    accepted = payload.get("accepted") or []
    rejected = payload.get("rejected") or []

    if not accepted:
        if rejected:
            err = rejected[0].get("error") or {}
            msg = err.get("message") or err.get("msg") or "Not found"
            if "invalid" in msg.lower() or "format" in msg.lower():
                return None  # wrong carrier code → signal retry
            return {"ok": False, "error": msg}
        return {"ok": False, "error": "Not found"}

    item       = accepted[0]
    track_info = item.get("track_info") or {}

    # ── Status (v2.4 string codes) ────────────────────────────────────────────
    _STATUS_MAP = {
        "NotFound":           ("📦", "Не знайдено",               "pending"),
        "InfoReceived":       ("📝", "Інформацію отримано",       "active"),
        "InTransit":          ("🚚", "В дорозі",                  "active"),
        "Expired":            ("⏰", "Термін зберігання минув",   "fail"),
        "AvailableForPickup": ("🏪", "Готово до отримання",       "active"),
        "OutForDelivery":     ("🚀", "Виїхав на доставку",        "active"),
        "DeliveryFailure":    ("⚠️", "Невдала спроба доставки",  "active"),
        "Delivered":          ("✅", "Доставлено",                "done"),
        "Exception":          ("⚠️", "Виняток",                  "fail"),
    }

    latest_status = track_info.get("latest_status") or {}
    status_str    = latest_status.get("status") or "NotFound"
    e_icon, e_label, e_state = _STATUS_MAP.get(status_str, ("📦", status_str, "active"))

    # ── Carrier name from providers ───────────────────────────────────────────
    tracking  = track_info.get("tracking") or {}
    providers = tracking.get("providers") or []
    events    = []
    carrier_name = ""
    if providers:
        p = providers[0]
        prov_info    = p.get("provider") or {}
        carrier_name = prov_info.get("name") or ""
        events       = p.get("events") or []
    if not carrier_name:
        carrier_name = str(item.get("carrier", ""))

    # ── Estimated delivery ────────────────────────────────────────────────────
    time_metrics = track_info.get("time_metrics") or {}
    edd          = time_metrics.get("estimated_delivery_date") or {}
    scheduled_delivery = edd.get("from") or ""
    if scheduled_delivery:
        try:
            scheduled_delivery = datetime.datetime.fromisoformat(
                scheduled_delivery.replace("Z", "+00:00")
            ).strftime("%d.%m.%Y")
        except Exception:
            pass

    # ── No events yet ─────────────────────────────────────────────────────────
    if not events:
        return {
            "ok": True, "type": "parcel",
            "carrier": carrier_name, "number": number,
            "status": "Трекінг зареєстровано.",
            "steps": [{
                "status": "pending", "icon": "🔄",
                "title": "Запит відправлено до перевізника",
                "desc": "",
                "time": "",
            }],
        }

    # ── Build timeline (events newest-first → reverse to chronological) ───────
    sliced = events[:15]
    total  = len(sliced)
    steps  = []
    for i, ev in enumerate(reversed(sliced)):
        is_last = (i == total - 1)
        st  = e_state if is_last else "done"
        ico = e_icon  if is_last else "📍"

        # Format ISO timestamp → readable
        time_str = ev.get("time_iso") or ev.get("time_utc") or ""
        if time_str:
            try:
                time_str = datetime.datetime.fromisoformat(
                    time_str.replace("Z", "+00:00")
                ).strftime("%d.%m.%Y %H:%M")
            except Exception:
                pass

        steps.append({
            "status": st,
            "icon":   ico,
            "title":  ev.get("description") or "",
            "desc":   ev.get("location") or "",
            "time":   time_str,
        })

    current_status = e_label if status_str not in ("NotFound",) else (steps[-1]["title"] if steps else "")

    result = {
        "ok":      True,
        "type":    "parcel",
        "carrier": carrier_name,
        "number":  number,
        "status":  current_status,
        "steps":   steps,
    }
    if scheduled_delivery:
        result["scheduled_delivery"] = scheduled_delivery
    return result


async def _track_17track(number: str, carrier_code: int = 0, realtime: bool = True) -> dict:
    """
    Universal tracking via 17TRACK API v2.4.

    Логика:
    1. Регистрируем номер в 17TRACK.
    2. Если пользователь запросил трекинг вручную — пробуем getRealTimeTrackInfo.
    3. Если real-time не дал события — пробуем gettrackinfo.
    4. Если данных еще нет — возвращаем pending, а не общую ошибку.

    realtime=True  → getRealTimeTrackInfo (forces carrier fetch, 1 credit, 3h cache)
    realtime=False → gettrackinfo only (uses 17track's own cache, background refresh)
    """

    if not SEVENTEEN_TRACK_KEY:
        return {
            "ok": False,
            "error": "17TRACK API key is not set",
            "hint": "Добавьте SEVENTEEN_TRACK_KEY в .env",
        }

    BASE = "https://api.17track.net/track/v2.4"

    headers = {
        "17token": SEVENTEEN_TRACK_KEY,
        "Content-Type": "application/json",
    }

    def _body(extra: dict | None = None, use_carrier: bool = True) -> list:
        item = {
            "number": number,
            "auto_detection": True,
        }

        # carrier передаем только если пользователь явно выбрал перевозчика
        if use_carrier and carrier_code:
            item["carrier"] = carrier_code

        if extra:
            item.update(extra)

        return [item]

    def _has_events(result: dict | None) -> bool:
        return bool(
            result
            and result.get("ok")
            and result.get("steps")
            and result["steps"][0].get("status") != "pending"
        )

    async def _post_17track(
        client: httpx.AsyncClient,
        endpoint: str,
        body: list,
    ) -> tuple[dict | None, dict | None]:
        """
        Возвращает:
        - data, None — если HTTP и JSON нормальные
        - data, error_dict — если есть ошибка API
        """

        url = f"{BASE}/{endpoint}"

        try:
            response = await client.post(
                url,
                json=body,
                headers=headers,
            )
        except httpx.RequestError as e:
            return None, {
                "ok": False,
                "error": f"17TRACK network error on {endpoint}: {str(e)}",
            }

        try:
            data = response.json()
        except Exception:
            return None, {
                "ok": False,
                "error": f"17TRACK returned non-JSON response on {endpoint}",
                "http_status": response.status_code,
                "raw_response": response.text[:500],
            }

        if response.status_code != 200:
            return data, {
                "ok": False,
                "error": f"17TRACK HTTP error on {endpoint}",
                "http_status": response.status_code,
                "api_response": data,
            }

        if data.get("code") != 0:
            return data, {
                "ok": False,
                "error": f"17TRACK API error on {endpoint}",
                "api_code": data.get("code"),
                "api_message": data.get("message") or data.get("msg") or "Unknown API error",
                "api_response": data,
            }

        return data, None

    def _register_rejected_error(data: dict | None) -> dict | None:
        """
        Проверяем, не отклонил ли 17TRACK регистрацию номера.
        Если номер уже зарегистрирован — это не считаем критической ошибкой.
        """

        if not data:
            return {
                "ok": False,
                "error": "Empty response from 17TRACK register",
            }

        payload = data.get("data") or {}
        accepted = payload.get("accepted") or []
        rejected = payload.get("rejected") or []

        if accepted:
            return None

        if rejected:
            err = rejected[0].get("error") or {}
            msg = err.get("message") or err.get("msg") or "Tracking number rejected"

            msg_lower = msg.lower()

            # Если номер уже был зарегистрирован раньше — это нормально
            if (
                "already" in msg_lower
                or "exist" in msg_lower
                or "registered" in msg_lower
            ):
                return None

            return {
                "ok": False,
                "error": f"17TRACK register rejected: {msg}",
                "api_response": data,
            }

        return None

    last_error: dict | None = None
    result: dict | None = None

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:

            # 1. Register tracking number
            register_data, register_error = await _post_17track(
                client,
                "register",
                _body(),
            )

            if register_error:
                return register_error

            rejected_error = _register_rejected_error(register_data)
            if rejected_error:
                return rejected_error

            # 2. Real-time request — только для ручного запроса пользователя
            if realtime:
                realtime_data, realtime_error = await _post_17track(
                    client,
                    "getRealTimeTrackInfo",
                    _body({"cacheLevel": 0}),
                )

                if realtime_error:
                    last_error = realtime_error
                else:
                    result = _parse_17track_v24(realtime_data, number)

                    # Если parser вернул None — вероятно, carrier code неправильный.
                    # Пробуем еще раз без carrier code, через auto-detection.
                    if result is None and carrier_code:
                        realtime_data_auto, realtime_error_auto = await _post_17track(
                            client,
                            "getRealTimeTrackInfo",
                            _body({"cacheLevel": 0}, use_carrier=False),
                        )

                        if realtime_error_auto:
                            last_error = realtime_error_auto
                        else:
                            result = _parse_17track_v24(realtime_data_auto, number)

                    if _has_events(result):
                        return result

            # 3. Cached lookup / fallback
            cached_data, cached_error = await _post_17track(
                client,
                "gettrackinfo",
                _body(),
            )

            if cached_error:
                last_error = cached_error
            else:
                result2 = _parse_17track_v24(cached_data, number)

                # Если carrier code был неправильный — пробуем auto-detection
                if result2 is None and carrier_code:
                    cached_data_auto, cached_error_auto = await _post_17track(
                        client,
                        "gettrackinfo",
                        _body(use_carrier=False),
                    )

                    if cached_error_auto:
                        last_error = cached_error_auto
                    else:
                        result2 = _parse_17track_v24(cached_data_auto, number)

                if _has_events(result2):
                    return result2

                if result2:
                    return result2

            # 4. Если 17TRACK принял номер, но событий еще нет
            if result:
                return result

            if last_error:
                return last_error

            return {
                "ok": True,
                "type": "parcel",
                "carrier": "",
                "number": number,
                "status": "Трекінг зареєстровано. Дані з'являться після оновлення 17TRACK.",
                "steps": [
                    {
                        "status": "pending",
                        "icon": "🔄",
                        "title": "Запит відправлено в 17TRACK",
                        "desc": "Інформація по посилці ще оновлюється",
                        "time": "",
                    }
                ],
            }

    except Exception as e:
        return {
            "ok": False,
            "error": f"Unexpected 17TRACK error: {str(e)}",
        }


@app.get("/api/webapp/track")
async def api_webapp_track(number: str, carrier: str = "auto", _bg: bool = False):
    """Unified parcel & sea-container tracking endpoint. _bg=True → background refresh (no realtime)."""
    n = number.strip().upper().replace(" ", "").replace("-", "")
    if not n:
        raise HTTPException(status_code=400, detail="number required")

    # ── Sea container (ISO 6346: 4 letters + 7 digits) ──────────────────────
    if _is_container(n):
        line, tracking_url = _container_info(n)
        base = {
            "ok": True,
            "type": "container",
            "number": n,
            "carrier": line,
            "line": line,
            "tracking_url": tracking_url,
        }
        if SEVENTEEN_TRACK_KEY:
            result = await _track_17track(n, 0, realtime=not _bg)
            # Always stamp container metadata regardless of 17track result
            result["type"] = "container"
            result["line"] = line
            result["tracking_url"] = tracking_url
            if not result.get("ok"):
                # 17track error → return container with link + pending step
                result["ok"] = True
                result.setdefault("status", "Трекінг зареєстровано.")
                result["steps"] = [{
                    "status": "pending", "icon": "🔄",
                    "title": "Запит відправлено до перевізника",
                    "desc": "",
                    "time": "",
                }]
                result.pop("error", None)
                result.pop("hint", None)
            return result
        # No 17track key — return link only
        base["status"] = "Відкрийте офіційний сайт перевізника"
        base["steps"] = []
        base["no_api"] = True
        return base

    # ── Parcel ───────────────────────────────────────────────────────────────
    if carrier == "nova" or (carrier == "auto" and _is_nova_poshta(n)):
        return await _track_nova_poshta(n)

    # DHL / FedEx / UPS / EMS / Meest → 17track
    # Carrier codes per 17track API: 0=auto, 2=DHL, 4=UPS, 100003=FedEx, 100162=Meest
    _CARRIER_CODES = {
        "dhl":   2,
        "ups":   4,
        "fedex": 100003,
        "ems":   3,
        "meest": 100162,
    }
    if SEVENTEEN_TRACK_KEY:
        code = _CARRIER_CODES.get(carrier, 0)
        return await _track_17track(n, code, realtime=not _bg)

    # No API keys at all
    return {
        "ok": False,
        "error": "Необхідний API ключ",
        "hint": (
            "Для Нової Пошти: NOVA_POSHTA_API_KEY (безкоштовно на developers.novaposhta.ua)\n"
            "Для DHL/FedEx/UPS/EMS/Meest та контейнерів: SEVENTEEN_TRACK_KEY (безкоштовно на 17track.net/en/apiDoc)"
        ),
    }


# ─── SAVED SHIPMENTS (per-user tracking list) ────────────────────────────────

@app.post("/api/webapp/track/save")
async def api_track_save(request: Request):
    """Save a tracking number to the user's personal list."""
    body = await request.json()
    user_id  = int(body.get("user_id", 0))
    number   = str(body.get("number", "")).upper().strip()[:60]
    carrier  = str(body.get("carrier", "auto"))[:50]
    type_    = str(body.get("type", "parcel"))[:20]
    cname    = str(body.get("carrier_name", ""))[:150]
    status   = str(body.get("status_text", ""))[:500]
    turl     = str(body.get("tracking_url", ""))[:500]
    steps_raw = body.get("steps", [])
    steps_json = json.dumps(steps_raw, ensure_ascii=False)[:8000]

    if not number or not user_id:
        raise HTTPException(status_code=400, detail="number and user_id required")

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tracked_shipments
                    (user_id, number, carrier, type, carrier_name, status_text, tracking_url, steps_json, last_checked)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (user_id, number) DO UPDATE SET
                    carrier      = EXCLUDED.carrier,
                    carrier_name = EXCLUDED.carrier_name,
                    status_text  = EXCLUDED.status_text,
                    tracking_url = EXCLUDED.tracking_url,
                    steps_json   = EXCLUDED.steps_json
                RETURNING id
                """,
                (user_id, number, carrier, type_, cname, status, turl, steps_json),
            )
            row = cur.fetchone()
            conn.commit()
        return {"ok": True, "id": row[0]}
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/api/webapp/track/list")
async def api_track_list(user_id: int):
    """Return active (≤15) and archived (≤15) shipments for a user."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, number, carrier, type, carrier_name, status_text,
                       tracking_url, steps_json, added_at, last_checked
                FROM tracked_shipments
                WHERE user_id=%s AND is_delivered=FALSE
                ORDER BY added_at DESC LIMIT 15
                """,
                (user_id,),
            )
            active = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT id, number, carrier, type, carrier_name, status_text,
                       tracking_url, steps_json, added_at, delivered_at, last_checked
                FROM tracked_shipments
                WHERE user_id=%s AND is_delivered=TRUE
                ORDER BY delivered_at DESC LIMIT 15
                """,
                (user_id,),
            )
            archive = [dict(r) for r in cur.fetchall()]

        def _fmt(d):
            return d.isoformat() if d else None

        for row in active + archive:
            for k in ("added_at", "last_checked", "delivered_at"):
                if k in row and row[k]:
                    row[k] = _fmt(row[k])
            # Deserialize steps_json → steps list
            raw_steps = row.pop("steps_json", "") or ""
            try:
                row["steps"] = json.loads(raw_steps) if raw_steps else []
            except Exception:
                row["steps"] = []

        return {"active": active, "archive": archive}
    finally:
        if conn:
            conn.close()


@app.delete("/api/webapp/track/remove")
async def api_track_remove(user_id: int, number: str):
    """Remove a shipment from the user's tracking list."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tracked_shipments WHERE user_id=%s AND number=%s",
                (user_id, number.upper()),
            )
            conn.commit()
        return {"ok": True}
    finally:
        if conn:
            conn.close()


async def refresh_tracked_shipments():
    """Background job: re-query every non-delivered shipment, mark delivered ones."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (number) id, number, carrier, type
                FROM tracked_shipments
                WHERE is_delivered = FALSE
                  AND (last_checked IS NULL
                       OR last_checked < NOW() - INTERVAL '1 hour')
                ORDER BY number, last_checked ASC NULLS FIRST
                LIMIT 40
                """
            )
            rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[refresh_tracking] DB read error: {e}")
        return
    finally:
        if conn:
            conn.close()

    _delivered_kw = {"отримано", "доставлено", "delivered", "получено", "вручено", "видано"}

    for row in rows:
        await asyncio.sleep(0.8)
        try:
            result = await api_webapp_track(row["number"], row["carrier"], _bg=True)
            if not result.get("ok"):
                continue

            status_text  = result.get("status", "")
            carrier_name = result.get("carrier", "") or result.get("line", "")
            tracking_url = result.get("tracking_url", "")
            steps_json   = json.dumps(result.get("steps", []), ensure_ascii=False)[:8000]

            is_delivered = any(kw in status_text.lower() for kw in _delivered_kw)
            if not is_delivered:
                for step in result.get("steps", []):
                    if any(kw in step.get("title", "").lower() for kw in _delivered_kw):
                        is_delivered = True
                        break

            conn2 = None
            try:
                conn2 = get_db_connection()
                with conn2.cursor() as cur2:
                    if is_delivered:
                        cur2.execute(
                            """
                            UPDATE tracked_shipments SET
                                status_text=%(s)s, carrier_name=%(c)s, tracking_url=%(u)s,
                                steps_json=%(j)s,
                                is_delivered=TRUE, delivered_at=NOW(), last_checked=NOW()
                            WHERE number=%(n)s AND is_delivered=FALSE
                            """,
                            {"s": status_text, "c": carrier_name, "u": tracking_url,
                             "j": steps_json, "n": row["number"]},
                        )
                    else:
                        cur2.execute(
                            """
                            UPDATE tracked_shipments SET
                                status_text=%(s)s, carrier_name=%(c)s,
                                steps_json=%(j)s, last_checked=NOW()
                            WHERE number=%(n)s AND is_delivered=FALSE
                            """,
                            {"s": status_text, "c": carrier_name,
                             "j": steps_json, "n": row["number"]},
                        )
                    conn2.commit()
            except Exception as e2:
                if conn2:
                    conn2.rollback()
                print(f"[refresh_tracking] update {row['number']}: {e2}")
            finally:
                if conn2:
                    conn2.close()
        except Exception as e:
            print(f"[refresh_tracking] check {row['number']}: {e}")