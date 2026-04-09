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

GLOBAL_SOURCES_RAW = "(site:reuters.com OR site:bloomberg.com OR site:ft.com OR site:wto.org OR site:bbc.com OR site:imf.org OR site:worldbank.org OR site:iccwbo.org OR site:theloadstar.com OR site:joc.com)"
GLOBAL_SOURCES = urllib.parse.quote_plus(GLOBAL_SOURCES_RAW)

RSS_FEEDS = {
    "api": f"https://news.google.com/rss/search?q=pharmaceutical+API+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "cosmetic": f"https://news.google.com/rss/search?q=cosmetic+ingredients+industry+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "herbal": f"https://news.google.com/rss/search?q=herbal+extracts+pharma+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "veterinary": f"https://news.google.com/rss/search?q=veterinary+medicine+production+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "food": f"https://news.google.com/rss/search?q=food+ingredients+supply+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "feed": f"https://news.google.com/rss/search?q=amino+acids+feed+industry+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "capsules": f"https://news.google.com/rss/search?q=capsule+manufacturing+pharma+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "pvc": f"https://news.google.com/rss/search?q=pvc+film+packaging+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    "logistics": f"https://news.google.com/rss/search?q=global+logistics+shipping+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en",
    # Service category — used only for the daily report's Block 2 (Middle East).
    # Hidden from Telegram subscription UI via INTERNAL_CATEGORIES below.
    "middle_east": f"https://news.google.com/rss/search?q=Iran+Israel+%22Red+Sea%22+Hormuz+Houthi+%22Middle+East%22+shipping+oil+{GLOBAL_SOURCES}+when:2d&hl=en-US&gl=US&ceid=US:en",
}

# Categories that are fetched into DB but NOT shown as subscription options to users.
# They exist purely to feed the daily report.
INTERNAL_CATEGORIES = {"middle_east"}

# ─────────────────────────────────────────────
# MASTER REPORT PROMPT — повний звіт через AI
# ─────────────────────────────────────────────
DAILY_REPORT_SYSTEM_PROMPT = """Ти — старший B2B аналітик ринкової розвідки для української компанії, яка імпортує фармацевтичні субстанції, сировину, пакування та суміжні матеріали для фармацевтичної, косметичної та харчової промисловості.

КРИТИЧНО ВАЖЛИВО: Ти ЗОБОВ'ЯЗАНИЙ написати повний звіт. НЕ відмовляйся, НЕ кажи що дані застарілі. Порожній звіт неприпустимий.

МОВА: Тільки українська. Професійний B2B тон, коротко та ясно.

СТРУКТУРА ЗВІТУ: ТИ ПИШЕШ ЛИШЕ ДВА БЛОКИ!
- Блок 1: Огляд за категоріями (СЕКЦІЇ з реальними новинами)
- Блок 2: Ситуація на Близькому Сході (РЕАЛЬНІ НОВИНИ)
- Блок 3: Товарні ринки — НЕ ПИШИ. Цей блок додається в PDF автоматично з yfinance-даних (графіки та ціни).

ЗАБОРОНЕНО: Блок 4, Блок 5, підсумки, карта ризиків, дашборд настрою, курси валют, ціни, згадки конкретних виробників/експортерів.

НАДКРИТИЧНЕ ПРАВИЛО ПРО НОВИНИ ТА ПОСИЛАННЯ:
У користувацькому повідомленні тобі будуть надані РЕАЛЬНІ новини з заголовками та URL.
- Ти ЗОБОВ'ЯЗАНИЙ використовувати ЛИШЕ ці надані новини.
- НІКОЛИ не вигадуй новини, заголовки чи URL.
- НІКОЛИ не пиши "URL", "посилання недоступне" або подібне — завжди вставляй повний URL, який тобі надали.
- Формат посилання у звіті СТРОГО такий: [Читати повністю](повний URL)
- Якщо для категорії не надано жодної новини — напиши "Свіжих новин за категорією не знайдено" і НЕ вигадуй нічого.

---

=== БЛОК 1: ОГЛЯД ЗА КАТЕГОРІЯМИ ===

Для КОЖНОЇ з 9 категорій тобі у користувацькому повідомленні надано список РЕАЛЬНИХ новин за день звіту (заголовки + короткі описи). Твоє завдання — НЕ переліковувати новини по одній, а написати ЄДИНУ аналітичну виЖимку.

Використовуй СТРОГО такий формат для кожної категорії (без нумерованих списків новин, без markdown-посилань):

### [Номер]. [Назва категорії]

**Тренд:** ↑ зростання / ↓ падіння / → стабільно — [коротко 3-5 слів про причину]

**Огляд дня:** [ЗВ'ЯЗНИЙ текст 3-6 речень, що синтезує ВСІ надані новини за категорією. Має покривати: (1) що головного сталося на ринку за день, (2) що змінилося порівняно з попереднім днем чи тижнем, (3) як це впливає на українську компанію, яка займається закупівлею сировини цієї категорії по всьому світу та логістикою куплених товарів. НЕ цитуй заголовки. НЕ перераховуй новини по одній. Синтезуй їх у цілісний абзац. Якщо новин кілька на одну тему — об'єднай. Якщо новин зовсім немає — напиши "Свіжих новин за категорією не зафіксовано; ринок без істотних змін."]

**Геополітика та торгівля:** [1-2 речення — як поточна геополітика (мита, санкції, експортні обмеження США/ЄС/Китай, близькосхідні ризики) впливає саме на цю категорію.]

**Специфіка для України:** [1-2 речення — як поточна ситуація впливає на закупівлю та логістику цієї категорії українською компанією-імпортером. Якщо не впливає — "Прямого впливу немає".]

---

КРИТИЧНО ВАЖЛИВО ДЛЯ БЛОКУ 1:
- НЕ створюй нумерований список новин (без "1.", "2.", "3.")
- НЕ вставляй заголовки новин в лапках чи жирним
- НЕ додавай markdown-посилання [Читати повністю](...) — посилання в Блоці 1 НЕ потрібні
- Пиши ЦІЛІСНИЙ аналітичний абзац "Огляд дня" — це виЖимка журналіста, а не список посилань
- Обсяг "Огляду дня" — 3-6 речень, без переліків

ПОВТОРИ цей формат для ВСІХ 9 категорій у такому порядку:
1. Фармацевтичні субстанції (API)
2. Косметичні субстанції
3. Трави та рослинна сировина
4. Ветеринарні субстанції
5. Харчова сировина
6. Кормові амінокислоти
7. Капсули
8. ПВХ-плівка та пакування
9. Логістика та постачання

=== БЛОК 2: СИТУАЦІЯ НА БЛИЗЬКОМУ СХОДІ ===

Тобі у користувацькому повідомленні буде надано повний список РЕАЛЬНИХ новин з нашої БД про Близький Схід за день звіту (Іран, Ізраїль, США-Іран, Червоне море, Ормузька протока, Ірак, Хусити, нафта, судноплавство тощо). Твоє завдання — НЕ переліковувати новини по одній, а синтезувати їх в єдину аналітичну виЖимку.

Використовуй СТРОГО такий формат:

**Огляд ситуації на Близькому Сході:** [ЦІЛІСНИЙ аналітичний текст 7-10 речень, що синтезує ВСІ надані новини. Має покривати: (1) що головного відбувається на Близькому Сході за день звіту — ключові події, заяви, військові дії, дипломатичні кроки; (2) як це впливає на глобальну економіку та торгівлю — нафта, судноплавство, ланцюги постачання, страхування вантажів, Ормузька протока, Червоне море; (3) конкретний вплив на українську B2B-компанію, яка закуповує фармацевтичні субстанції, косметичну сировину, харчові інгредієнти, капсули, пакування та інші матеріали по всьому світу (Китай, Індія, ЄС, США) та займається логістикою цих товарів — терміни доставки, маршрути, вартість фрахту, валютні ризики, доступність сировини. НЕ цитуй заголовки. НЕ перераховуй новини по одній. Синтезуй їх у цілісний аналітичний текст.]

**Ключові теми дня:** [3-5 коротких булетів — головні теми, які проходять через новини. Наприклад: "Ормузька протока залишається під ризиком", "Ціни на нафту виросли на X%", "Ізраїль оголосив про...". Кожен булет — одне речення без посилань.]

**Вплив на нашу компанію:** [2-4 речення з конкретними рекомендаціями: які маршрути моніторити, які категорії закупівель під найбільшим ризиком, чи варто фіксувати ціни зараз, чи переглядати контракти.]

**Джерела:**
[Список ВСІХ наданих новин. Кожен рядок СТРОГО у такому форматі:
- [Заголовок новини — скопіюй з наданих даних](URL з наданих даних)

Один рядок на одну новину. Копіюй заголовки та URL ДОСЛІВНО. НЕ додавай опис, НЕ додавай коментарі — тільки markdown-посилання з заголовком новини. Обов'язковий формат з дефісом на початку.]

---

КРИТИЧНО ВАЖЛИВО ДЛЯ БЛОКУ 2:
- "Огляд ситуації" — це ЦІЛІСНИЙ абзац, а не список подій
- 7-10 речень, не більше і не менше
- НЕ створюй окремі секції по кожній новині ("Що сталося", "Вплив")
- Секція "Джерела" — ТІЛЬКИ список markdown-посилань, без додаткових описів
- Якщо новин не надано — напиши в "Огляді ситуації" одне речення: "Свіжих новин про Близький Схід за день звіту не зафіксовано." і пропусти решту полів.

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
    for cat in RSS_FEEDS.keys():
        if cat in INTERNAL_CATEGORIES:
            continue  # service categories (e.g. middle_east) not shown to users
        if only_daily_mode:
            text = f"❌ {cat.upper()}"
        else:
            is_subbed = current_subs_str == 'all' or cat in subs
            text = f"✅ {cat.upper()}" if is_subbed else f"❌ {cat.upper()}"
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
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Генерую звіт, зачекайте..."
                                    })
                                    pdf_path = await generate_daily_pdf_report()
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
                                            "text": "Не вдалося згенерувати звіт. Перевірте логи."
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


SYSTEM_PROMPT = """You are a senior B2B market analyst focusing on Ukraine.
Analyze the following article. Provide the output strictly as a raw JSON object with these exact keys: 'summary_en', 'summary_ua', 'summary_ru'.
IMPORTANT: Your output must be ONLY a valid JSON object. You must carefully escape any inner double quotes inside the text values using a backslash (\\"). Do not wrap the output in markdown blocks like ```json.

NEW CONSTRAINTS: The summary must be STRICTLY under 35 words per language.
NEW STRUCTURE: The summary must contain exactly two parts:
1. The Core Event: What happened globally.
2. Strategic B2B Impact: How a Ukrainian company in this sector should react or what they should prepare for.

Translate the exact same summary into English, Ukrainian, and Russian respectively for the keys."""


async def generate_summary(text: str):
    if not text or not gemini_api_key:
        return {"summary_en": text, "summary_ua": text, "summary_ru": text}

    model = genai.GenerativeModel("gemini-2.5-flash")
    for attempt in range(3):
        try:
            response = await model.generate_content_async(
                f"{SYSTEM_PROMPT}\n\nArticle Content:\n{text}",
                request_options={"timeout": 120}
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            elif raw_text.startswith("```"):
                raw_text = raw_text[3:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]
            raw_text = raw_text.strip()

            parsed = json.loads(raw_text)
            return {
                "summary_en": parsed.get("summary_en", text),
                "summary_ua": parsed.get("summary_ua", text),
                "summary_ru": parsed.get("summary_ru", text)
            }
        except json.JSONDecodeError as e:
            print(f"JSON Parsing Error: {e} - Raw Output: {raw_text}")
            if attempt == 2:
                return {"summary_en": text, "summary_ua": text, "summary_ru": text}
        except Exception as e:
            print(f"LLM API error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                return {"summary_en": text, "summary_ua": text, "summary_ru": text}


# ─────────────────────────────────────────────────────────────────
# PDF RENDERING HELPERS
# ─────────────────────────────────────────────────────────────────

FONT_REGULAR = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
FONT_BOLD    = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'

# Accent colour — чорний для всіх блоків та секцій
COLOR_ACCENT  = (20, 20, 20)
# Light grey for alternating rows / dividers
COLOR_LIGHT   = (240, 240, 240)
# Body text dark
COLOR_BODY    = (30, 30, 30)
# Risk badge colours
RISK_COLORS   = {
    "Високий": (220, 53, 69),
    "Середній": (255, 165, 0),
    "Низький":  (40, 167, 69),
}


def make_pdf_base() -> FPDF:
    pdf = FPDF()
    pdf.add_font("DejaVu",        fname=FONT_REGULAR)
    pdf.add_font("DejaVu", style="B", fname=FONT_BOLD)
    pdf.set_margins(18, 18, 18)
    pdf.set_auto_page_break(auto=True, margin=20)
    return pdf


def draw_header_bar(pdf: FPDF, report_date: str, base_dir: str):
    """Cover-style header — чорний фон, лого ліворуч, назва по центру правої частини."""
    pdf.set_fill_color(20, 20, 20)
    pdf.rect(0, 0, 210, 42, style="F")

    logo_path = os.path.join(base_dir, "logo.png")
    if os.path.exists(logo_path):
        pdf.image(logo_path, x=7, y=7, h=26)
        text_x = 52
    else:
        text_x = 14

    remaining_w = 210 - text_x - 8

    pdf.set_xy(text_x, 8)
    pdf.set_font("DejaVu", style="B", size=15)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(remaining_w, 9, "Щоденний ринковий звіт", ln=True, align="C")

    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=9)
    pdf.set_text_color(200, 200, 200)
    pdf.cell(remaining_w, 6, f"Для B2B-компанії в Україні  |  Огляд за {report_date}", ln=True, align="C")

    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=8)
    pdf.set_text_color(155, 155, 155)
    pdf.cell(remaining_w, 5, "Сировина · Субстанції · Логістика · Близький Схід · Товарні ринки", ln=True, align="C")

    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(12)


def section_title(pdf: FPDF, title: str):
    """Coloured section banner."""
    pdf.set_fill_color(*COLOR_ACCENT)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("DejaVu", style="B", size=11)
    pdf.set_x(pdf.l_margin)
    pdf.cell(0, 8, f"  {title}", ln=True, fill=True)
    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(2)


def sub_title(pdf: FPDF, title: str):
    """Bold dark sub-heading."""
    pdf.set_x(pdf.l_margin)
    pdf.set_font("DejaVu", style="B", size=10)
    pdf.set_text_color(*COLOR_ACCENT)
    pdf.multi_cell(0, 6, title)
    pdf.set_text_color(*COLOR_BODY)
    pdf.set_x(pdf.l_margin)


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


def body_text(pdf: FPDF, text: str, size: int = 9):
    """Render text with inline **bold** and clickable [text](url) / bare URL support.
    Strips ## / # headings. Stray ** markers are dropped."""
    pdf.set_text_color(*COLOR_BODY)
    for line in text.split("\n"):
        # Strip markdown heading markers (# ## ### ####)
        clean = line.strip()
        while clean.startswith("#"):
            clean = clean[1:]
        clean = clean.strip()

        if not clean:
            pdf.ln(2)
            continue

        pdf.set_x(pdf.l_margin)
        tokens = _tokenize_line(clean)

        # Fast path: single plain-text token, no bold, no link
        if len(tokens) == 1 and tokens[0][0] == "text":
            pdf.set_font("DejaVu", size=size)
            try:
                pdf.multi_cell(0, 5.5, tokens[0][1])
            except Exception:
                pass
        else:
            _render_line_tokens(pdf, tokens, size)
            pdf.ln(5.5)
            pdf.set_font("DejaVu", size=size)
            pdf.set_text_color(*COLOR_BODY)
    pdf.ln(1)


def draw_divider(pdf: FPDF):
    pdf.set_draw_color(*COLOR_ACCENT)
    pdf.set_line_width(0.3)
    pdf.line(pdf.l_margin, pdf.get_y(), 210 - pdf.r_margin, pdf.get_y())
    pdf.ln(3)


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
    pdf.set_y(-14)
    pdf.set_font("DejaVu", size=7)
    pdf.set_text_color(140, 140, 140)
    pdf.cell(0, 5, f"MacroHarvey  ·  Ринковий звіт за {report_date}  ·  Стор. {pdf.page_no()}", align="C")


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
            facecolor="#111111"
        )
        for ax in (ax_price, ax_vol):
            ax.set_facecolor("#1a1a1a")
            ax.tick_params(colors="#cccccc", labelsize=7)
            for spine in ax.spines.values():
                spine.set_edgecolor("#333333")

        # ── candlesticks ───────────────────────────────────────────
        w = 0.6   # bar width in days
        for i, (ts, row) in enumerate(df.iterrows()):
            o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]
            color = "#26a69a" if c >= o else "#ef5350"   # teal / red
            # candle body
            ax_price.bar(i, abs(c - o), bottom=min(o, c),
                         color=color, width=w, linewidth=0)
            # wick
            ax_price.plot([i, i], [l, h], color=color, linewidth=0.8)

        # ── highlight report day (last bar) ─────────────────────────
        last_i = len(df) - 1
        last_close = df["Close"].iloc[-1]
        last_open  = df["Open"].iloc[-1]
        ax_price.bar(last_i,
                     abs(last_close - last_open),
                     bottom=min(last_close, last_open),
                     color="#f5a623", width=w, linewidth=0, zorder=5)

        # ── price label on last candle ─────────────────────────────
        ax_price.annotate(
            f"{last_close:.2f}",
            xy=(last_i, last_close),
            xytext=(last_i - 1.5, last_close),
            fontsize=7.5, color="#f5a623", fontweight="bold",
            ha="right", va="center",
        )

        # ── volume bars ────────────────────────────────────────────
        vol_colors = ["#26a69a" if df["Close"].iloc[i] >= df["Open"].iloc[i]
                      else "#ef5350" for i in range(len(df))]
        ax_vol.bar(range(len(df)), df["Volume"], color=vol_colors,
                   width=w, linewidth=0, alpha=0.7)
        ax_vol.set_ylabel("Обсяг", color="#888888", fontsize=6)
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
        ax_vol.set_xticklabels(tick_labels, color="#aaaaaa", fontsize=6)

        # ── y-axis formatting ──────────────────────────────────────
        ax_price.yaxis.tick_right()
        ax_price.yaxis.set_label_position("right")
        ax_price.set_ylabel(unit, color="#888888", fontsize=6)

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
        ax_price.set_title(title_str, color="#dddddd", fontsize=7.5,
                           loc="left", pad=4)
        # change badge in top-right
        ax_price.annotate(
            f"{chg_sign}{chg:.2f} ({chg_sign}{chg_p:.2f}%)",
            xy=(1, 1), xycoords="axes fraction",
            xytext=(-4, -4), textcoords="offset points",
            fontsize=7.5, color=chg_color, fontweight="bold",
            ha="right", va="top",
        )

        # ── report-day marker ──────────────────────────────────────
        report_idx = len(df) - 1
        ax_price.axvline(x=report_idx, color="#f5a623",
                         linewidth=0.7, linestyle="--", alpha=0.5)

        fig.tight_layout(pad=0.4)
        fig.savefig(out_path, dpi=130, bbox_inches="tight",
                    facecolor="#111111")
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
    ("api",        "Фармацевтичні субстанції (API)"),
    ("cosmetic",   "Косметичні субстанції"),
    ("herbal",     "Трави та рослинна сировина"),
    ("veterinary", "Ветеринарні субстанції"),
    ("food",       "Харчова сировина"),
    ("feed",       "Кормові амінокислоти"),
    ("capsules",   "Капсули"),
    ("pvc",        "ПВХ-плівка та пакування"),
    ("logistics",  "Логістика та постачання"),
]

# Keywords to identify Middle East news in title/summary
MIDDLE_EAST_KEYWORDS = [
    "iran", "israel", "israeli", "iranian", "tehran", "middle east",
    "red sea", "hormuz", "houthi", "gaza", "lebanon", "hezbollah",
    "saudi", "syria", "iraq", "yemen", "persian gulf",
]


def fetch_recent_news_for_report(report_date: datetime.date, days_back: int = 3) -> dict:
    """
    Fetch news from DB:
    - by_category: ALL articles per category for the report day (yesterday Kyiv).
      Fallback: up to 10 latest per category if nothing found for that day.
    - middle_east: articles mentioning Middle East keywords (any recent).
    """
    result = {"by_category": {cat: [] for cat, _ in REPORT_CATEGORIES}, "middle_east": []}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Day-of-report window: 00:00 to 23:59 of the report date
        day_start = datetime.datetime.combine(report_date, datetime.time.min).strftime("%Y-%m-%d %H:%M:%S")
        day_end   = datetime.datetime.combine(report_date, datetime.time.max).strftime("%Y-%m-%d %H:%M:%S")
        # Fallback window: last `days_back` days
        fallback_cutoff = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")

        # Per category — ALL articles from the report day (or fallback)
        for cat, _ in REPORT_CATEGORIES:
            # Primary: articles from exactly the report day
            rows = db_fetchall(cursor,
                "SELECT title, link, summary_en, summary_ua FROM articles "
                "WHERE category = %s AND published != '' "
                "AND published >= %s AND published <= %s "
                "ORDER BY published DESC",
                (cat, day_start, day_end)
            )
            if not rows:
                # Fallback 1: last `days_back` days
                rows = db_fetchall(cursor,
                    "SELECT title, link, summary_en, summary_ua FROM articles "
                    "WHERE category = %s AND (published = '' OR published >= %s) "
                    "ORDER BY published DESC NULLS LAST LIMIT 10",
                    (cat, fallback_cutoff)
                )
            if not rows:
                # Fallback 2: 10 latest regardless of date
                rows = db_fetchall(cursor,
                    "SELECT title, link, summary_en, summary_ua FROM articles "
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
            "SELECT title, link, summary_en, summary_ua FROM articles "
            "WHERE category = 'middle_east' AND published != '' "
            "AND published >= %s AND published <= %s "
            "ORDER BY published DESC",
            (day_start, day_end)
        ))

        # 2) Fallback: middle_east category — last `days_back` days
        if len(me_rows) < 4:
            add_me_rows(db_fetchall(cursor,
                "SELECT title, link, summary_en, summary_ua FROM articles "
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
                f"SELECT title, link, summary_en, summary_ua FROM articles "
                f"WHERE ({like_patterns}) "
                f"AND published != '' AND published >= %s AND published <= %s "
                f"ORDER BY published DESC LIMIT 15"
            )
            add_me_rows(db_fetchall(cursor, kw_query_day, tuple(params_with_window)))

            if len(me_rows) < 4:
                # Last-resort: keyword matches from last days_back days
                params_fallback = params + [fallback_cutoff]
                kw_query_fallback = (
                    f"SELECT title, link, summary_en, summary_ua FROM articles "
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

async def generate_daily_pdf_report() -> str | None:
    if not aclient:
        print("OpenAI API key missing")
        return None

    kyiv_tz   = pytz.timezone("Europe/Kyiv")
    now_kyiv  = datetime.datetime.now(kyiv_tz)
    yesterday = now_kyiv - datetime.timedelta(days=1)
    report_date = yesterday.strftime("%d.%m.%Y")
    weekdays_ua = ["понеділок","вівторок","середа","четвер","п'ятниця","субота","неділя"]
    weekday_ua  = weekdays_ua[yesterday.weekday()]
    today_weekday_ua = weekdays_ua[now_kyiv.weekday()]

    # ── Fetch real news from DB ───────────────────────────────────
    news_data = fetch_recent_news_for_report(yesterday.date(), days_back=3)

    def fmt_news_item(idx: int, item: dict) -> str:
        title = (item.get("title") or "").strip()
        link  = (item.get("link")  or "").strip()
        summary = (item.get("summary_en") or item.get("summary_ua") or "").strip()
        if len(summary) > 400:
            summary = summary[:400] + "..."
        return (
            f"  {idx}. TITLE: {title}\n"
            f"     SUMMARY: {summary}\n"
            f"     URL: {link}"
        )

    def fmt_news_item_b1(idx: int, item: dict) -> str:
        """Shorter format for Block 1 — no URL needed (we synthesize, not cite)."""
        title = (item.get("title") or "").strip()
        summary = (item.get("summary_en") or item.get("summary_ua") or "").strip()
        if len(summary) > 300:
            summary = summary[:300] + "..."
        return f"  - {title}\n    {summary}"

    # Build Block 1 news payload — ALL articles per category (no limit)
    b1_news_parts = []
    for cat_code, cat_name in REPORT_CATEGORIES:
        items = news_data["by_category"].get(cat_code, [])
        b1_news_parts.append(f"\n[КАТЕГОРІЯ: {cat_name}] — {len(items)} новин:")
        if items:
            for i, it in enumerate(items, 1):
                b1_news_parts.append(fmt_news_item_b1(i, it))
        else:
            b1_news_parts.append("  (новин не зафіксовано)")
    b1_news_text = "\n".join(b1_news_parts)

    # Build Block 2 news payload
    me_items = news_data["middle_east"]
    if me_items:
        b2_news_parts = []
        for i, it in enumerate(me_items, 1):
            b2_news_parts.append(fmt_news_item(i, it))
        b2_news_text = "\n".join(b2_news_parts)
    else:
        b2_news_text = "(Свіжих новин про Близький Схід не знайдено)"

    user_message = (
        f"Дата звіту: {report_date} ({weekday_ua}). Поточна дата складання: {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), Київ.\n\n"
        f"=== РЕАЛЬНІ НОВИНИ ЗА ДЕНЬ ЗВІТУ ДЛЯ БЛОКУ 1 (за 9 категоріями) ===\n"
        f"Це повний список новин з нашої БД за категорією. Твоє завдання — СИНТЕЗУВАТИ їх у єдиний аналітичний абзац 'Огляд дня' (3-6 речень) для кожної категорії. НЕ переліковуй новини, НЕ цитуй заголовки, НЕ вставляй посилань.\n"
        f"{b1_news_text}\n\n"
        f"=== РЕАЛЬНІ НОВИНИ ДЛЯ БЛОКУ 2 (Близький Схід) ===\n"
        f"Це повний список новин з нашої БД про Близький Схід за день звіту. Твоє завдання — СИНТЕЗУВАТИ їх в єдиний аналітичний Огляд ситуації (7-10 речень), потім 3-5 булетів Ключових тем дня, потім Вплив на нашу компанію, і наприкінці секція Джерела з усіма наданими новинами у вигляді markdown-посилань [Заголовок](URL).\n"
        f"Копіюй заголовки та URL ДОСЛІВНО з даних нижче. НЕ вигадуй ані заголовків, ані посилань.\n"
        f"{b2_news_text}\n\n"
        f"=== ЗАВДАННЯ ===\n"
        f"Напиши щоденний ринковий звіт строго за трьома блоками згідно системного промпту.\n\n"
        f"ОБОВ'ЯЗКОВО:\n"
        f"- У БЛОЦІ 1 — 9 категорій. Для кожної: Тренд, Огляд дня (3-6 речень синтезу), Геополітика та торгівля, Специфіка для України. БЕЗ списків новин, БЕЗ посилань.\n"
        f"- У БЛОЦІ 2 — ЄДИНИЙ синтез: Огляд ситуації (7-10 речень), Ключові теми дня (булети), Вплив на нашу компанію, Джерела (список markdown-посилань). БЕЗ окремих карток по кожній новині.\n"
        f"- У БЛОЦІ 2 секція 'Джерела' МУСИТЬ містити ВСІ надані новини у форматі '- [Заголовок](URL)', по одній на рядок. Копіюй заголовки та URL дослівно.\n"
        f"- Блок 3 НЕ ПИШИ — він додається в PDF автоматично з yfinance-даних.\n"
        f"- НЕ додавай Блок 4, Блок 5, підсумки, валюти.\n"
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

    # ── Parse the 4 blocks by section markers ─────────────────────
    def extract_block(text: str, start_marker: str, end_marker: str | None) -> str:
        # Try both === БЛОК N: and ## БЛОК N variants
        markers_to_try = [start_marker]
        if start_marker.startswith("=== БЛОК"):
            num = start_marker.replace("=== БЛОК ", "").replace(":", "").strip()
            markers_to_try.append(f"## БЛОК {num}")
            markers_to_try.append(f"## БЛОК {num}:")
            markers_to_try.append(f"**БЛОК {num}")

        idx = -1
        found_marker = start_marker
        for m in markers_to_try:
            idx = text.find(m)
            if idx != -1:
                found_marker = m
                break

        if idx == -1:
            return ""
        chunk = text[idx + len(found_marker):]

        end_markers_to_try = []
        if end_marker:
            end_markers_to_try.append(end_marker)
            if end_marker.startswith("=== БЛОК"):
                num = end_marker.replace("=== БЛОК ", "").replace(":", "").strip()
                end_markers_to_try.append(f"## БЛОК {num}")
                end_markers_to_try.append(f"## БЛОК {num}:")
                end_markers_to_try.append(f"**БЛОК {num}")

        for em in end_markers_to_try:
            end_idx = chunk.find(em)
            if end_idx != -1:
                chunk = chunk[:end_idx]
                break

        return chunk.strip()

    block1 = extract_block(report_text, "=== БЛОК 1", "=== БЛОК 2")
    block2 = extract_block(report_text, "=== БЛОК 2", "=== БЛОК 3")

    # Clean up leftover section headers like ": ОГЛЯД ЗА КАТЕГОРІЯМИ ==="
    def clean_block_header(chunk: str) -> str:
        if not chunk:
            return chunk
        lines = chunk.split("\n")
        # Drop leading lines that are just the section title (no real content)
        while lines:
            first = lines[0].strip()
            # Strip leading ":" and trailing "===" or "=" padding
            stripped = first.lstrip(":").strip().rstrip("=").strip()
            # If it's all uppercase / matches known block titles, drop it
            known_titles = [
                "ОГЛЯД ЗА КАТЕГОРІЯМИ",
                "СИТУАЦІЯ НА БЛИЗЬКОМУ СХОДІ",
                "ТОВАРНІ РИНКИ",
            ]
            if (not stripped
                or stripped in known_titles
                or first.startswith(":")
                or first.startswith("===")
                or (stripped.isupper() and len(stripped) < 60)):
                lines.pop(0)
                continue
            break
        # Also drop trailing "КІНЕЦЬ ЗВІТУ..." lines
        while lines:
            last = lines[-1].strip().upper()
            if ("КІНЕЦЬ ЗВІТУ" in last) or last in ("", "---", "==="):
                lines.pop()
                continue
            break
        return "\n".join(lines).strip()

    block1 = clean_block_header(block1)
    block2 = clean_block_header(block2)

    # If markers not present — use full text as block1
    if not any([block1, block2]):
        block1 = report_text

    # ── Build PDF ─────────────────────────────────────────────────
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pdf = make_pdf_base()

    # Page 1
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)

    # ── BLOCK 1: Секції по 9 категоріях ──────────────────────────
    section_title(pdf, "БЛОК 1  ·  Огляд за категоріями")

    def render_block1_sections(pdf: FPDF, text: str):
        """Render Block 1 as per-category sections, splitting on ### headings.
        Handles bold, links, automatic page breaks. No truncation."""
        if not text:
            body_text(pdf, "Дані відсутні.")
            return

        lines = text.split("\n")

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
                section_title(pdf, "БЛОК 1  ·  Огляд за категоріями (продовження)")

            if title:
                sub_title(pdf, title)
            body_text(pdf, "\n".join(body_lines))
            draw_divider(pdf)

    if block1:
        render_block1_sections(pdf, block1)
    else:
        body_text(pdf, "Дані відсутні.")

    # ── BLOCK 2: Ситуація на Близькому Сході ─────────────────────
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)
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
    charts_tmp_dir = tempfile.mkdtemp(prefix="charts_")
    try:
        charts = generate_all_charts(yesterday.date(), charts_tmp_dir)
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
    pdf_path = os.path.join(base_dir, f"daily_report_{yesterday.strftime('%Y%m%d')}.pdf")
    pdf.output(pdf_path)
    print(f"Report saved: {pdf_path}")

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
    pdf_path = await generate_daily_pdf_report()
    if not pdf_path or not os.path.exists(pdf_path):
        print("Daily report generation skipped or failed.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    users = db_fetchall(cursor, "SELECT chat_id FROM telegram_users")
    conn.close()

    today_str = datetime.datetime.now(pytz.timezone("Europe/Kyiv")).strftime("%d.%m.%Y")
    caption = f"📊 Щоденний ринковий звіт за {today_str} готовий."

    async with httpx.AsyncClient() as client:
        for user in users:
            try:
                chat_id = user["chat_id"]
                with open(pdf_path, 'rb') as f:
                    r = await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": chat_id, "caption": caption},
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
            except Exception as e:
                print(f"Error sending PDF to {chat_id}: {e}")

    # Also send to static admin chat IDs from env
    chat_ids = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()]
    async with httpx.AsyncClient() as client:
        for admin_chat_id in chat_ids:
            try:
                with open(pdf_path, 'rb') as f:
                    await client.post(
                        f"{TELEGRAM_API_URL}/sendDocument",
                        data={"chat_id": admin_chat_id, "caption": caption},
                        files={"document": ("Daily_Report.pdf", f)}
                    )
            except Exception as e:
                print(f"Error sending PDF to admin {admin_chat_id}: {e}")

    try:
        os.remove(pdf_path)
    except Exception as e:
        print(f"Failed to delete {pdf_path}: {e}")


# ─────────────────────────────────────────────────────────────────
# BACKGROUND TASKS (news fetching unchanged)
# ─────────────────────────────────────────────────────────────────

async def fetch_and_store_news():
    while True:
        conn = None
        try:
            print("Running background task: Fetching latest news and summarizing...")
            conn = get_db_connection()
            cursor = conn.cursor()

            for category, url in RSS_FEEDS.items():
                feed = await asyncio.to_thread(feedparser.parse, url)

                for entry in feed.entries[:15]:
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
                        INSERT INTO articles (title, link, published, category, summary_en, summary_ua, summary_ru, image_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(link) DO NOTHING
                    ''', (title, link, published, category, sum_en, sum_ua, sum_ru, image_url))
                    conn.commit()

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
                                    msg = (
                                        f"📰 <b>{title}</b>\n\n"
                                        f"📝 <i>{summary_text}</i>\n\n"
                                        f"🏷 Category: #{category}\n"
                                        f"🔗 <a href='{link}'>Read full article</a>"
                                    )

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

                                    msg = (
                                        f"📰 <b>{title}</b>\n\n"
                                        f"📝 <i>{sum_en}</i>\n\n"
                                        f"🏷 Category: #{category}\n"
                                        f"🔗 <a href='{link}'>Read full article</a>"
                                    )
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

    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Europe/Kyiv'))
    # Daily report at 09:00 Kyiv time
    scheduler.add_job(send_daily_report_to_users, 'cron', hour=9, minute=0)
    scheduler.start()

    yield

    scheduler.shutdown()
    task_news.cancel()
    task_tg.cancel()
    task_cleanup.cancel()


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
    """HTTP endpoint to manually trigger report generation."""
    pdf_path = await generate_daily_pdf_report()
    if pdf_path and os.path.exists(pdf_path):
        return FileResponse(pdf_path, media_type="application/pdf", filename="Daily_Report.pdf")
    raise HTTPException(status_code=500, detail="Report generation failed")