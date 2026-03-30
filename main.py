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
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'articles.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            chat_id INTEGER PRIMARY KEY,
            language TEXT DEFAULT 'en',
            subscriptions TEXT DEFAULT 'all'
        )
    ''')
    conn.commit()
    conn.close()

load_dotenv()

gemini_api_key = os.getenv("GEMINI_API_KEY")
if gemini_api_key:
    genai.configure(api_key=gemini_api_key)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

RSS_FEEDS = {
    "api": "https://news.google.com/rss/search?q=pharmaceutical+API+when:7d&hl=en-US&gl=US&ceid=US:en",
    "cosmetic": "https://news.google.com/rss/search?q=cosmetic+ingredients+industry+when:7d&hl=en-US&gl=US&ceid=US:en",
    "herbal": "https://news.google.com/rss/search?q=herbal+extracts+pharma+when:7d&hl=en-US&gl=US&ceid=US:en",
    "veterinary": "https://news.google.com/rss/search?q=veterinary+medicine+production+when:7d&hl=en-US&gl=US&ceid=US:en",
    "food": "https://news.google.com/rss/search?q=food+ingredients+supply+when:7d&hl=en-US&gl=US&ceid=US:en",
    "feed": "https://news.google.com/rss/search?q=amino+acids+feed+industry+when:7d&hl=en-US&gl=US&ceid=US:en",
    "capsules": "https://news.google.com/rss/search?q=capsule+manufacturing+pharma+when:7d&hl=en-US&gl=US&ceid=US:en",
    "pvc": "https://news.google.com/rss/search?q=pvc+film+packaging+when:7d&hl=en-US&gl=US&ceid=US:en",
    "logistics": "https://news.google.com/rss/search?q=global+logistics+shipping+when:7d&hl=en-US&gl=US&ceid=US:en"
}

def get_topics_keyboard(current_subs_str):
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
        
    return {"inline_keyboard": keyboard}

async def poll_telegram_updates():
    offset = 0
    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(f"{TELEGRAM_API_URL}/getUpdates", params={"offset": offset, "timeout": 30}, timeout=40)
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
                                    cursor.execute("INSERT INTO telegram_users (chat_id, language) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET language=excluded.language", (chat_id, lang))
                                    conn.commit()
                                    cursor.execute("SELECT subscriptions FROM telegram_users WHERE chat_id = ?", (chat_id,))
                                    user_row = cursor.fetchone()
                                    conn.close()
                                    
                                    current_subs = user_row["subscriptions"] if user_row and user_row["subscriptions"] else "all"
                                    
                                    msg_map = {
                                        "ru": "Язык установлен на Русский!\\nПожалуйста, выберите интересующие вас темы:",
                                        "ua": "Мову встановлено на Українську!\\nБудь ласка, оберіть цікаві для вас теми:",
                                        "en": "Language set to English!\\nPlease select your preferred news topics:"
                                    }
                                    
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": msg_map[lang],
                                        "reply_markup": get_topics_keyboard(current_subs)
                                    })
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})
                                    
                                elif data_cb == "menu_lang":
                                    keyboard = {
                                        "inline_keyboard": [
                                            [
                                                {"text": "🇷🇺 RU", "callback_data": "lang_ru"},
                                                {"text": "🇺🇦 UA", "callback_data": "lang_ua"},
                                                {"text": "🇬🇧 EN", "callback_data": "lang_en"}
                                            ]
                                        ]
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
                                    cursor.execute("SELECT subscriptions FROM telegram_users WHERE chat_id = ?", (chat_id,))
                                    user_row = cursor.fetchone()
                                    conn.close()
                                    
                                    current_subs = user_row["subscriptions"] if user_row and user_row["subscriptions"] else "all"
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Please select your preferred topics:",
                                        "reply_markup": get_topics_keyboard(current_subs)
                                    })
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})
                                    
                                elif data_cb.startswith("topic_"):
                                    conn = get_db_connection()
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT subscriptions FROM telegram_users WHERE chat_id = ?", (chat_id,))
                                    user_row = cursor.fetchone()
                                    
                                    if user_row:
                                        current_subs = user_row["subscriptions"] if user_row["subscriptions"] else "all"
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
                                                
                                        cursor.execute("UPDATE telegram_users SET subscriptions = ? WHERE chat_id = ?", (new_subs, chat_id))
                                        conn.commit()
                                        
                                        await client.post(f"{TELEGRAM_API_URL}/editMessageReplyMarkup", json={
                                            "chat_id": chat_id,
                                            "message_id": cb["message"]["message_id"],
                                            "reply_markup": get_topics_keyboard(new_subs)
                                        })
                                    conn.close()
                                    await client.post(f"{TELEGRAM_API_URL}/answerCallbackQuery", json={"callback_query_id": cb["id"]})
                                    
                            elif "message" in update and "text" in update["message"]:
                                msg = update["message"]
                                chat_id = msg["chat"]["id"]
                                text = msg["text"]
                                
                                if text.startswith("/start"):
                                    keyboard = {
                                        "inline_keyboard": [
                                            [
                                                {"text": "🇷🇺 RU", "callback_data": "lang_ru"},
                                                {"text": "🇺🇦 UA", "callback_data": "lang_ua"},
                                                {"text": "🇬🇧 EN", "callback_data": "lang_en"}
                                            ]
                                        ]
                                    }
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": "Welcome to MacroHarvey! / Ласкаво просимо! / Добро пожаловать!\\nPlease select your language:",
                                        "reply_markup": keyboard
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
            except Exception as e:
                # Silently catch timeouts or bot API errors
                pass
            await asyncio.sleep(2)

SYSTEM_PROMPT = """You are a senior B2B market analyst focusing on Ukraine.
Analyze the following article. Provide the output strictly as a raw JSON object with these exact keys: 'summary_en', 'summary_ua', 'summary_ru'.
Do not include any other text, markdown formatting, or ```json blocks.

Each key must contain a concise 2-3 sentence summary:
1. Briefly state the core event.
2. Explain how this impacts Ukrainian B2B traders or supply chains.
3. Give an actionable recommendation.
Translate the exact same summary into English, Ukrainian, and Russian respectively for the keys."""

async def generate_summary(text: str):
    if not text or not gemini_api_key:
        return {"summary_en": text, "summary_ua": text, "summary_ru": text}
    
    model = genai.GenerativeModel("gemini-3.1-pro")
    for attempt in range(3):
        try:
            response = await model.generate_content_async(
                f"{SYSTEM_PROMPT}\n\nArticle Content:\n{text}",
                request_options={"timeout": 120}
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.startswith("```"):
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

async def fetch_and_store_news():
    while True:
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
                    
                    cursor.execute("SELECT 1 FROM articles WHERE link = ?", (link,))
                    if cursor.fetchone():
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
                        from datetime import datetime
                        try:
                            from zoneinfo import ZoneInfo
                            tz = ZoneInfo("Europe/Kyiv")
                        except ImportError:
                            from datetime import timezone, timedelta
                            tz = timezone(timedelta(hours=2))
                        published = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
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
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(link) DO NOTHING
                    ''', (title, link, published, category, sum_en, sum_ua, sum_ru, image_url))
                    conn.commit()
                    
                    try:
                        async with httpx.AsyncClient() as client:
                            # 1. Broadcast to database users
                            cursor.execute("SELECT chat_id, language, subscriptions FROM telegram_users")
                            users = cursor.fetchall()
                            if users:
                                for user in users:
                                    try:
                                        chat_id = user["chat_id"]
                                        lang = user["language"]
                                        subs = user["subscriptions"] if user["subscriptions"] else "all"
                                        
                                        if subs != "all":
                                            sub_list = subs.split(",")
                                            if category not in sub_list:
                                                continue
                                                
                                        summary_text = summaries.get(f"summary_{lang}", sum_en)
                                        msg = f"📰 <b>{title}</b>\n\n📝 <i>{summary_text}</i>\n\n🏷 Category: #{category}\n🔗 <a href='{link}'>Read full article</a>"
                                        
                                        await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                            "chat_id": chat_id,
                                            "text": msg,
                                            "parse_mode": "HTML"
                                        })
                                    except Exception as e:
                                        print(f"Error sending to DB user {user['chat_id']}: {e}")

                            # 2. Broadcast to specific chat IDs from environment variable
                            chat_ids = [id.strip() for id in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if id.strip()]
                            for admin_chat_id in chat_ids:
                                try:
                                    msg = f"📰 <b>{title}</b>\n\n📝 <i>{sum_en}</i>\n\n🏷 Category: #{category}\n🔗 <a href='{link}'>Read full article</a>"
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": admin_chat_id,
                                        "text": msg,
                                        "parse_mode": "HTML"
                                    })
                                except Exception as e:
                                    print(f"Error sending to static chat_id {admin_chat_id}: {e}")

                    except Exception as e:
                        print(f"Error broadcasting to Telegram: {e}")
            
            conn.close()
            print("Successfully updated news database.")
        except Exception as e:
            print(f"Error fetching news: {e}")
        
        await asyncio.sleep(900)

async def cleanup_old_news():
    from datetime import datetime, timedelta
    while True:
        try:
            print("Running cleanup_old_news: Deleting articles older than 30 days...")
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM articles WHERE published != '' AND published < ?", (cutoff_date,))
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            print(f"Cleanup finished. Deleted {deleted_count} old articles.")
        except Exception as e:
            print(f"Error during cleanup_old_news: {e}")
        
        await asyncio.sleep(86400)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task_news = asyncio.create_task(fetch_and_store_news())
    task_tg = asyncio.create_task(poll_telegram_updates())
    task_cleanup = asyncio.create_task(cleanup_old_news())
    yield
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
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, "index.html")
    return FileResponse(index_path)

@app.get("/news")
def get_all_news():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles ORDER BY published DESC LIMIT 1000"
    )
    rows = cursor.fetchall()
        
    conn.close()
    return [dict(row) for row in rows]

@app.get("/alerts")
def get_latest_alerts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT title, link, published FROM articles ORDER BY published DESC LIMIT 5"
    )
    rows = cursor.fetchall()
    conn.close()
    
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Kyiv")
    except ImportError:
        from datetime import timezone, timedelta
        tz = timezone(timedelta(hours=2))
        
    now = datetime.now(tz)
    
    results = []
    for row in rows:
        r = dict(row)
        pub_str = r["published"]
        dt_obj = None
        if pub_str:
            try:
                dt_obj = datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S")
                dt_obj = dt_obj.replace(tzinfo=tz)
            except ValueError:
                import email.utils
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
            if mins <= 1:
                display_time = "Just now"
            else:
                display_time = f"{mins} mins ago"
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
    cursor.execute(
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles WHERE category = ? ORDER BY published DESC LIMIT 15",
        (category,)
    )
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]
