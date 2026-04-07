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
    "logistics": f"https://news.google.com/rss/search?q=global+logistics+shipping+{GLOBAL_SOURCES}+when:7d&hl=en-US&gl=US&ceid=US:en"
}

# ─────────────────────────────────────────────
# MASTER REPORT PROMPT — повний звіт через AI
# ─────────────────────────────────────────────
DAILY_REPORT_SYSTEM_PROMPT = """Ти — старший аналітик ринку сировини та субстанцій для B2B-компанії в Україні, яка імпортує фармацевтичні, косметичні, ветеринарні субстанції, трави, харчову сировину, кормові амінокислоти, капсули, ПВХ-плівку та відстежує логістику.

Твоє завдання: написати КОРОТКИЙ щоденний ринковий звіт за вчорашній день (Europe/Kyiv) у форматі, придатному для PDF.

МОВА: Тільки українська. Діловий стиль. Для B2B-аудиторії.

СТРУКТУРА ЗВІТУ:

=== БЛОК 1: ОГЛЯД ЗА КАТЕГОРІЯМИ ===

Для кожної з 9 категорій напиши СТИСЛИЙ розділ (не більше 6 рядків на категорію).

Формат кожної категорії:
[НАЗВА КАТЕГОРІЇ]
• Що сталося: [1–2 факти вчорашнього дня]
• Ризик/Можливість: [1 речення]
• Дія: [1 конкретна дія]
• Рівень: [Високий / Середній / Низький]

Якщо новин немає — написати: "Суттєвих подій не виявлено. Моніторинг: [watchpoint]"

Категорії в точному порядку:
1. Фармацевтичні субстанції (API)
2. Косметичні субстанції
3. Трави
4. Ветеринарні субстанції
5. Харчова сировина
6. Кормові амінокислоти
7. Капсули (тверді / м'які)
8. ПВХ-плівка
9. Логістика та постачання для імпорту сировини

=== БЛОК 2: БЛИЗЬКИЙ СХІД — НОВИНИ ДНЯ ===

Знайди і опиши 3–5 найважливіших новин вчорашнього дня пов'язаних з:
Іран, Ізраїль, Саудівська Аравія, ОАЕ, Катар, Ірак, Туреччина, Червоне море, Ормузька протока, атаки хуситів, санкції проти Ірану, регіональна нестабільність, нафтовий ринок Близького Сходу.

Фільтр: тільки новини що можуть впливати на імпорт сировини, ціни на нафту, логістику або глобальні ланцюги постачання для України.

Формат кожної новини:
→ [Заголовок] | [Джерело] | [Дата]
  [2–3 рядки: що сталося + вплив на наш імпорт]

=== БЛОК 3: ТОВАРНІ РИНКИ — ЦІНИ ТА РУХИ ===

Для кожного з 3 товарів надай:

🌽 КУКУРУДЗА (Corn — CBOT ZC1!)
Ціна вчора: [$/бушель]
Зміна: [+/- % від попереднього дня]
Аналіз: [2–3 рядки: погода, попит, експорт США, фундаментал, що вплинуло]
Джерело: TradingView / CBOT

🛢️ НАФТА (Crude Oil — WTI або Brent)
Ціна вчора: [$/барель]
Зміна: [+/- %]
Аналіз: [2–3 рядки: ОПЕК, геополітика, запаси EIA, попит]
Джерело: TradingView / EIA

🌴 ПАЛЬМОВА ОЛІЯ (Palm Oil — BMD FCPO)
Ціна вчора: [MYR/MT]
Зміна: [+/- %]
Аналіз: [2–3 рядки: врожай Малайзія/Індонезія, попит Китай/Індія, курс рінгіт]
Джерело: TradingView / BMD

=== БЛОК 4: ПІДСУМОК І ДІЇ ===

КЛЮЧОВІ ВИСНОВКИ (3–5 пунктів, по одному реченню):
• ...
• ...

ДІЇ СЬОГОДНІ:
• ...
• ...

ДІЇ НА ТИЖДЕНЬ:
• ...
• ...

КАРТА РИЗИКІВ (таблиця):
Категорія | Сигнал | Рівень ризику | Рекомендована дія
---------|--------|--------------|------------------
... | ... | ... | ...

ВАЖЛИВО:
- Використовуй тільки реальні дані за вчорашній день
- Не вигадуй ціни або факти яких не знаєш — пиши "дані уточнюються"
- Звіт має читатися за 5–7 хвилин
- Кожна категорія — максимум 6 рядків
- Загальний обсяг: компактний executive brief"""


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

# Accent colour (dark navy)
COLOR_ACCENT  = (26, 54, 93)
# Light grey for alternating rows / dividers
COLOR_LIGHT   = (240, 244, 248)
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
    """Cover-style header with logo + title."""
    pdf.set_fill_color(*COLOR_ACCENT)
    pdf.rect(0, 0, 210, 42, style="F")

    logo_path = os.path.join(base_dir, "logo.png")
    if os.path.exists(logo_path):
        pdf.image(logo_path, x=8, y=6, h=28)
        text_x = 42
    else:
        text_x = 12

    pdf.set_xy(text_x, 7)
    pdf.set_font("DejaVu", style="B", size=16)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 9, "Щоденний ринковий звіт", ln=True)

    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=10)
    pdf.set_text_color(180, 210, 255)
    pdf.cell(0, 6, f"Для B2B-компанії в Україні  |  Огляд за {report_date}", ln=True)

    pdf.set_x(text_x)
    pdf.set_font("DejaVu", size=9)
    pdf.set_text_color(140, 180, 230)
    pdf.cell(0, 5, "Сировина · Субстанції · Логістика · Близький Схід · Товарні ринки", ln=True)

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


def body_text(pdf: FPDF, text: str, size: int = 9):
    """Render plain text, stripping markdown artefacts."""
    pdf.set_font("DejaVu", size=size)
    pdf.set_text_color(*COLOR_BODY)
    for line in text.split("\n"):
        clean = line.replace("**", "").replace("##", "").replace("#", "").strip()
        if not clean:
            pdf.ln(2)
            continue
        pdf.set_x(pdf.l_margin)
        try:
            pdf.multi_cell(0, 5.5, clean)
        except Exception:
            pass
    pdf.ln(1)


def draw_divider(pdf: FPDF):
    pdf.set_draw_color(*COLOR_ACCENT)
    pdf.set_line_width(0.3)
    pdf.line(pdf.l_margin, pdf.get_y(), 210 - pdf.r_margin, pdf.get_y())
    pdf.ln(3)


def draw_risk_table(pdf: FPDF, lines: list[str]):
    """Renders a simple risk table from pipe-delimited lines."""
    headers = ["Категорія", "Сигнал", "Рівень", "Дія"]
    col_w   = [38, 55, 22, 65]

    pdf.set_font("DejaVu", style="B", size=8)
    pdf.set_fill_color(*COLOR_ACCENT)
    pdf.set_text_color(255, 255, 255)
    pdf.set_x(pdf.l_margin)
    for i, h in enumerate(headers):
        pdf.cell(col_w[i], 6, h, border=0, fill=True)
    pdf.ln()
    pdf.set_text_color(*COLOR_BODY)

    alternate = False
    for line in lines:
        line = line.strip()
        if not line or set(line.replace("|", "").replace("-", "").strip()) == set():
            continue
        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 4:
            continue
        # skip header repeat
        if cells[0].lower() in ("категорія", "category"):
            continue

        pdf.set_fill_color(*(COLOR_LIGHT if alternate else (255, 255, 255)))
        alternate = not alternate
        pdf.set_font("DejaVu", size=8)
        pdf.set_x(pdf.l_margin)

        risk_level = cells[2] if len(cells) > 2 else ""
        rc = RISK_COLORS.get(risk_level, COLOR_BODY)

        for i, cell_text in enumerate(cells[:4]):
            if i == 2:
                pdf.set_text_color(*rc)
            else:
                pdf.set_text_color(*COLOR_BODY)
            pdf.cell(col_w[i], 6, cell_text[:45], border=0, fill=True)
        pdf.ln()

    pdf.set_text_color(*COLOR_BODY)
    pdf.ln(3)


def draw_footer(pdf: FPDF, report_date: str):
    pdf.set_y(-14)
    pdf.set_font("DejaVu", size=7)
    pdf.set_text_color(140, 140, 140)
    pdf.cell(0, 5, f"MacroHarvey  ·  Ринковий звіт за {report_date}  ·  Стор. {pdf.page_no()}", align="C")


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

    user_message = (
        f"Сьогодні {now_kyiv.strftime('%d.%m.%Y')} ({today_weekday_ua}), "
        f"Київ (Europe/Kyiv). "
        f"Напиши повний щоденний ринковий звіт за вчора — {report_date} ({weekday_ua}). "
        f"Використай своє актуальне знання ринків, новин та цін. "
        f"Дотримуйся структури та формату зазначеного у системному промпті."
    )

    print(f"Generating prompt-based daily report for {report_date}...")

    try:
        response = await aclient.chat.completions.create(
            model="gpt-4o",
            max_tokens=3800,
            temperature=0.3,
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
        idx = text.find(start_marker)
        if idx == -1:
            return ""
        chunk = text[idx + len(start_marker):]
        if end_marker:
            end_idx = chunk.find(end_marker)
            if end_idx != -1:
                chunk = chunk[:end_idx]
        return chunk.strip()

    block1 = extract_block(report_text, "=== БЛОК 1:", "=== БЛОК 2:")
    block2 = extract_block(report_text, "=== БЛОК 2:", "=== БЛОК 3:")
    block3 = extract_block(report_text, "=== БЛОК 3:", "=== БЛОК 4:")
    block4 = extract_block(report_text, "=== БЛОК 4:", None)

    # If markers not present — use full text as block1
    if not any([block1, block2, block3, block4]):
        block1 = report_text

    # ── Build PDF ─────────────────────────────────────────────────
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pdf = make_pdf_base()

    # Page 1
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)

    # ── BLOCK 1: 9 categories ────────────────────────────────────
    section_title(pdf, "БЛОК 1  ·  Огляд за категоріями")

    CAT_LABELS = {
        "1.": "1. Фармацевтичні субстанції (API)",
        "2.": "2. Косметичні субстанції",
        "3.": "3. Трави",
        "4.": "4. Ветеринарні субстанції",
        "5.": "5. Харчова сировина",
        "6.": "6. Кормові амінокислоти",
        "7.": "7. Капсули",
        "8.": "8. ПВХ-плівка",
        "9.": "9. Логістика та постачання",
    }

    b1_lines = block1.split("\n") if block1 else report_text.split("\n")
    current_cat_lines: list[str] = []
    current_cat_title = ""

    def flush_category(pdf: FPDF, title: str, lines: list[str]):
        if not title and not lines:
            return
        if title:
            sub_title(pdf, title)
        body_text(pdf, "\n".join(lines))
        draw_divider(pdf)

    for raw_line in b1_lines:
        line = raw_line.strip()
        # Detect category heading (starts with digit dot)
        is_cat_heading = (
            len(line) > 3
            and line[0].isdigit()
            and line[1] == "."
            and not line.startswith("===")
        )
        if is_cat_heading:
            flush_category(pdf, current_cat_title, current_cat_lines)
            current_cat_title = line
            current_cat_lines = []
        else:
            current_cat_lines.append(line)
    flush_category(pdf, current_cat_title, current_cat_lines)

    # ── BLOCK 2: Middle East ──────────────────────────────────────
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)
    section_title(pdf, "БЛОК 2  ·  Близький Схід — Новини дня")
    if block2:
        body_text(pdf, block2)
    else:
        body_text(pdf, "Даних по Близькому Сходу за вчора не знайдено.")
    draw_divider(pdf)

    # ── BLOCK 3: Commodities ──────────────────────────────────────
    section_title(pdf, "БЛОК 3  ·  Товарні ринки")

    commodity_icons = {
        "КУКУРУДЗА":     "🌽 КУКУРУДЗА (Corn — CBOT ZC1!)",
        "НАФТА":         "🛢️ НАФТА (WTI / Brent)",
        "ПАЛЬМОВА":      "🌴 ПАЛЬМОВА ОЛІЯ (BMD FCPO)",
    }

    if block3:
        b3_lines = block3.split("\n")
        current_com_lines: list[str] = []
        current_com_title = ""

        def flush_commodity(pdf, title, lines):
            if not title and not lines:
                return
            if title:
                sub_title(pdf, title)
            body_text(pdf, "\n".join(lines))
            # TradingView reference
            tv_links = {
                "КУКУРУДЗА":  "https://www.tradingview.com/chart/?symbol=CBOT%3AZC1!",
                "НАФТА":      "https://www.tradingview.com/chart/?symbol=TVC%3AUSOIL",
                "ПАЛЬМОВА":   "https://www.tradingview.com/chart/?symbol=MYX%3AKPO1!",
            }
            for key, url in tv_links.items():
                if key in title.upper():
                    pdf.set_font("DejaVu", size=8)
                    pdf.set_text_color(26, 100, 200)
                    pdf.set_x(pdf.l_margin)
                    pdf.cell(0, 5, f"Графік TradingView: {url}", ln=True)
                    pdf.set_text_color(*COLOR_BODY)
                    break
            draw_divider(pdf)

        for raw_line in b3_lines:
            line = raw_line.strip()
            is_com = any(k in line.upper() for k in commodity_icons.keys()) and len(line) < 80
            if is_com:
                flush_commodity(pdf, current_com_title, current_com_lines)
                current_com_title = line
                current_com_lines = []
            else:
                current_com_lines.append(line)
        flush_commodity(pdf, current_com_title, current_com_lines)
    else:
        body_text(pdf, "Дані по товарних ринках будуть додані наступного дня.")

    # ── BLOCK 4: Summary + actions + risk map ─────────────────────
    pdf.add_page()
    draw_header_bar(pdf, report_date, base_dir)
    section_title(pdf, "БЛОК 4  ·  Підсумок і рекомендовані дії")

    if block4:
        b4_lines = block4.split("\n")
        risk_table_lines: list[str] = []
        in_risk_table = False
        pre_table_lines: list[str] = []

        for line in b4_lines:
            stripped = line.strip()
            # Detect risk table start
            if "КАРТА РИЗИКІВ" in stripped.upper() or (
                stripped.startswith("Категорія") and "|" in stripped
            ):
                in_risk_table = True
                body_text(pdf, "\n".join(pre_table_lines))
                pre_table_lines = []
                sub_title(pdf, "Карта ризиків")
                continue
            if in_risk_table:
                risk_table_lines.append(stripped)
            else:
                pre_table_lines.append(stripped)

        if pre_table_lines:
            body_text(pdf, "\n".join(pre_table_lines))
        if risk_table_lines:
            draw_risk_table(pdf, risk_table_lines)
    else:
        body_text(pdf, "Підсумок та карта ризиків недоступні.")

    # Footer on every page
    for page_num in range(1, pdf.page_no() + 1):
        pdf.page = page_num
        draw_footer(pdf, report_date)

    pdf_path = os.path.join(base_dir, f"daily_report_{yesterday.strftime('%Y%m%d')}.pdf")
    pdf.output(pdf_path)
    print(f"Report saved: {pdf_path}")
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

    today_str = datetime.datetime.now().strftime("%d.%m.%Y")
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
    rows   = db_fetchall(cursor,
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url "
        "FROM articles ORDER BY published DESC LIMIT 1000"
    )
    conn.close()
    return rows


@app.get("/alerts")
def get_latest_alerts():
    conn   = get_db_connection()
    cursor = conn.cursor()
    rows   = db_fetchall(cursor,
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