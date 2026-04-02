import asyncio
import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import feedparser
import google.generativeai as genai
from dotenv import load_dotenv
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

    # Таблица учёта отправленных новостей — предотвращает дубли при рестарте
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

gemini_api_key = os.getenv("GEMINI_API_KEY")
if gemini_api_key:
    genai.configure(api_key=gemini_api_key)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
aclient = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

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

def get_topics_keyboard(current_subs_str, only_daily_mode=False):
    subs = current_subs_str.split(',') if current_subs_str != 'all' else []
    keyboard = []

    all_text = "✅ All Topics" if current_subs_str == 'all' else "🔘 All Topics"
    keyboard.append([{"text": all_text, "callback_data": "topic_all"}])

    row = []
    for cat in RSS_FEEDS.keys():
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
                                        "text": "Generating report, please wait..."
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
                                            "text": "Failed to generate report. Check logs."
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


async def generate_daily_pdf_report():
    if not aclient:
        print("OpenAI API key missing")
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.datetime.now()
    since = today - datetime.timedelta(hours=24)

    date_str_start = since.strftime("%Y-%m-%d %H:%M:%S")
    date_str_end = today.strftime("%Y-%m-%d %H:%M:%S")

    rows = db_fetchall(cursor,
        "SELECT title, link, category, summary_en FROM articles WHERE published >= %s AND published <= %s",
        (date_str_start, date_str_end)
    )
    conn.close()

    if not rows:
        print("No articles from yesterday")
        return None

    categories = {}
    for r in rows:
        cat = r['category']
        if cat not in categories:
            categories[cat] = ""
        categories[cat] += f"- {r['title']}: {r['summary_en']}\n"

    category_summaries = {}
    for cat, content in categories.items():
        try:
            resp = await aclient.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Ти аналітик фармацевтичного ринку. Виділи лише ключові факти без зайвого. Відповідай українською мовою."},
                    {"role": "user", "content": f"Категорія: {cat}\nНовини:\n{content}"}
                ]
            )
            category_summaries[cat] = resp.choices[0].message.content
        except Exception as e:
            print(f"OpenAI MAP error for {cat}: {e}")
            category_summaries[cat] = "Не вдалося узагальнити."

    reduce_content = ""
    for cat, summary in category_summaries.items():
        reduce_content += f"--- Категорія: {cat} ---\n{summary}\n\n"

    prompt = "Ти B2B стратег. Створи Executive Summary українською мовою. Структура: 1. Головні події дня, 2. Розбивка по категоріях, 3. Практичні бізнес-інсайти для українських компаній."

    try:
        response = await aclient.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": reduce_content}
            ]
        )
        report_text = response.choices[0].message.content
    except Exception as e:
        print(f"OpenAI REDUCE error: {e}")
        return None

    base_dir = os.path.dirname(os.path.abspath(__file__))
    font_path = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
    font_bold_path = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'

    # add_font должен быть ДО add_page
    pdf = FPDF()
    pdf.set_margins(15, 15, 15)
    pdf.add_font("DejaVu", fname=font_path)
    pdf.add_font("DejaVu", style="B", fname=font_bold_path)
    pdf.add_page()

    logo_path = os.path.join(base_dir, 'logo.png')
    if os.path.exists(logo_path):
        pdf.image(logo_path, x=15, y=15, w=25)
        pdf.ln(22)

    pdf.set_font("DejaVu", style="B", size=14)
    pdf.multi_cell(0, 10, text="Premium Pharmaceutical Intelligence - Daily Report")
    pdf.set_font("DejaVu", size=10)
    pdf.multi_cell(0, 7, text=today.strftime("%Y-%m-%d"))
    pdf.ln(4)

    for line in report_text.split('\n'):
        clean = line.replace('**', '').replace('##', '').replace('#', '').strip()
        if not clean:
            pdf.ln(2)
            continue
        is_header = (
            clean[:3] in ['1. ', '2. ', '3. ', '4. ', '5. '] or
            (line.strip().startswith('**') and line.strip().endswith('**'))
        )
        if is_header:
            pdf.ln(2)
            pdf.set_font("DejaVu", style="B", size=11)
            pdf.multi_cell(0, 7, text=clean)
            pdf.set_font("DejaVu", size=10)
        else:
            pdf.multi_cell(0, 6, text=clean)

    pdf_path = os.path.join(base_dir, f'daily_report_{today.strftime("%Y%m%d")}.pdf')
    pdf.output(pdf_path)
    return pdf_path


async def send_daily_report_to_users():
    pdf_path = await generate_daily_pdf_report()
    if not pdf_path or not os.path.exists(pdf_path):
        print("Daily report generation skipped or failed.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    users = db_fetchall(cursor, "SELECT chat_id FROM telegram_users")
    conn.close()

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    caption = f"📊 Your Daily Executive Summary for {today_str} is ready."

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

    try:
        os.remove(pdf_path)
    except Exception as e:
        print(f"Failed to delete {pdf_path}: {e}")


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
                    title = getattr(entry, "title", "")
                    raw_link = getattr(entry, "link", "")
                    link = raw_link.split('?')[0] if raw_link else ""
                    link = link.strip()

                    if not link or not title:
                        continue

                    try:
                        # Проверка по link
                        cursor.execute("SELECT 1 FROM articles WHERE link = %s", (link,))
                        if cursor.fetchone() is not None:
                            print(f"Duplicate skipped: {link}")
                            continue
                        # Проверка по заголовку — защита от смены URL
                        cursor.execute("SELECT 1 FROM articles WHERE title = %s", (title,))
                        if cursor.fetchone() is not None:
                            print(f"Duplicate by title skipped: {title[:60]}")
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
                            # 1. Broadcast to database users
                            users = db_fetchall(cursor,
                                "SELECT chat_id, language, subscriptions, only_daily_mode FROM telegram_users"
                            )
                            for user in users:
                                try:
                                    if user["only_daily_mode"]:
                                        continue

                                    chat_id = user["chat_id"]
                                    lang = user["language"]
                                    subs = user["subscriptions"] if user["subscriptions"] else "all"

                                    if subs != "all":
                                        if category not in subs.split(","):
                                            continue

                                    # Проверяем, не отправляли ли уже эту новость этому пользователю
                                    cursor.execute(
                                        "SELECT 1 FROM telegram_sent WHERE chat_id = %s AND article_link = %s",
                                        (chat_id, link)
                                    )
                                    if cursor.fetchone() is not None:
                                        continue

                                    summary_text = summaries.get(f"summary_{lang}", sum_en)
                                    msg = f"📰 <b>{title}</b>\n\n📝 <i>{summary_text}</i>\n\n🏷 Category: #{category}\n🔗 <a href='{link}'>Read full article</a>"

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

                            # 2. Broadcast to static admin chat IDs from env
                            chat_ids = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()]
                            for admin_chat_id in chat_ids:
                                try:
                                    cursor.execute(
                                        "SELECT 1 FROM telegram_sent WHERE chat_id = %s AND article_link = %s",
                                        (int(admin_chat_id), link)
                                    )
                                    if cursor.fetchone() is not None:
                                        continue

                                    msg = f"📰 <b>{title}</b>\n\n📝 <i>{sum_en}</i>\n\n🏷 Category: #{category}\n🔗 <a href='{link}'>Read full article</a>"
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task_news = asyncio.create_task(fetch_and_store_news())
    task_tg = asyncio.create_task(poll_telegram_updates())
    task_cleanup = asyncio.create_task(cleanup_old_news())

    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Europe/Kyiv'))
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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, "index.html")
    return FileResponse(index_path)


@app.get("/news")
def get_all_news():
    conn = get_db_connection()
    cursor = conn.cursor()
    rows = db_fetchall(cursor,
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles ORDER BY published DESC LIMIT 1000"
    )
    conn.close()
    return rows


@app.get("/alerts")
def get_latest_alerts():
    conn = get_db_connection()
    cursor = conn.cursor()
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

    now = datetime.datetime.now(tz)
    results = []
    for r in rows:
        pub_str = r["published"]
        dt_obj = None
        if pub_str:
            try:
                dt_obj = datetime.datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S")
                dt_obj = dt_obj.replace(tzinfo=tz)
            except ValueError:
                try:
                    dt_email = email.utils.parsedate_to_datetime(pub_str)
                    dt_obj = dt_email.astimezone(tz)
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
        r["published"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        results.append(r)

    return results


@app.get("/news/{category}")
def get_category_news(category: str):
    if category not in RSS_FEEDS:
        raise HTTPException(status_code=404, detail="Category not found")

    conn = get_db_connection()
    cursor = conn.cursor()
    rows = db_fetchall(cursor,
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles WHERE category = %s ORDER BY published DESC LIMIT 15",
        (category,)
    )
    conn.close()
    return rows