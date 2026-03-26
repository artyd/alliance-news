import asyncio
import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import feedparser
from database import init_db, get_db_connection
import google.generativeai as genai
from dotenv import load_dotenv
import email.utils
import re
import httpx
import psycopg2.extras

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
                                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                    cursor.execute("INSERT INTO telegram_users (chat_id, language) VALUES (%s, %s) ON CONFLICT(chat_id) DO UPDATE SET language=%s", (chat_id, lang, lang))
                                    conn.commit()
                                    conn.close()
                                    
                                    msg_map = {
                                        "ru": "Язык установлен на Русский! Вы будете получать новости MacroHarvey.",
                                        "ua": "Мову встановлено на Українську! Ви отримуватимете новини MacroHarvey.",
                                        "en": "Language set to English! You will receive MacroHarvey news."
                                    }
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={"chat_id": chat_id, "text": msg_map[lang]})
                                    
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
            except Exception as e:
                # Silently catch timeouts or bot API errors
                pass
            await asyncio.sleep(2)

SYSTEM_PROMPT = "You are a senior B2B market analyst focusing on Ukraine. Analyze this global news article and write a concise, 2-3 sentence summary. Step 1: Briefly state the core event. Step 2: Explain how this specifically impacts Ukrainian B2B traders, manufacturers, importers, or supply chains. Step 3: Give a direct, actionable recommendation on how a business operating in Ukraine should react (e.g., 'diversify routes', 'expect price hikes'). Output ONLY a valid JSON object with three keys: 'en', 'ua', and 'ru', containing the exact same summary translated into English, Ukrainian, and Russian respectively."

async def generate_summary(text: str):
    if not text or not gemini_api_key:
        return {"en": text, "ua": text, "ru": text}
    
    model = genai.GenerativeModel("gemini-3.1-pro-preview")
    for attempt in range(3):
        try:
            response = await model.generate_content_async(
                f"{SYSTEM_PROMPT}\n\nArticle Content:\n{text}",
                generation_config={"response_mime_type": "application/json"}
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```"):
                raw_text = raw_text.replace("```json", "").replace("```", "").strip()
            return json.loads(raw_text)
        except Exception as e:
            print(f"LLM API error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                return {"en": text, "ua": text, "ru": text}

async def fetch_and_store_news():
    while True:
        try:
            print("Running background task: Fetching latest news and summarizing...")
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            for category, url in RSS_FEEDS.items():
                feed = await asyncio.to_thread(feedparser.parse, url)
                
                for entry in feed.entries[:15]:
                    title = getattr(entry, "title", "")
                    link = getattr(entry, "link", "")
                    published = getattr(entry, "published", "")
                    try:
                        if published:
                            dt = email.utils.parsedate_to_datetime(published)
                            published = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
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
                    
                    cursor.execute("SELECT id FROM articles WHERE link = %s", (link,))
                    if cursor.fetchone():
                        continue
                    
                    summaries = await generate_summary(description)
                    sum_en = summaries.get("en", description)
                    sum_ua = summaries.get("ua", description)
                    sum_ru = summaries.get("ru", description)
                    
                    cursor.execute('''
                        INSERT INTO articles (title, link, published, category, summary_en, summary_ua, summary_ru, image_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(link) DO NOTHING
                    ''', (title, link, published, category, sum_en, sum_ua, sum_ru, image_url))
                    conn.commit()
                    
                    try:
                        cursor.execute("SELECT chat_id, language FROM telegram_users")
                        users = cursor.fetchall()
                        if users:
                            async with httpx.AsyncClient() as client:
                                for user in users:
                                    chat_id = user["chat_id"]
                                    lang = user["language"]
                                    summary_text = summaries.get(lang, sum_en)
                                    msg = f"📰 <b>{title}</b>\n\n📝 <i>{summary_text}</i>\n\n🏷 Category: #{category}\n🔗 <a href='{link}'>Read full article</a>"
                                    
                                    await client.post(f"{TELEGRAM_API_URL}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": msg,
                                        "parse_mode": "HTML"
                                    })
                    except Exception as e:
                        print(f"Error broadcasting to Telegram: {e}")
            
            conn.close()
            print("Successfully updated news database.")
        except Exception as e:
            print(f"Error fetching news: {e}")
        
        await asyncio.sleep(900)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task_news = asyncio.create_task(fetch_and_store_news())
    task_tg = asyncio.create_task(poll_telegram_updates())
    yield
    task_news.cancel()
    task_tg.cancel()

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
    return FileResponse("index.html")

@app.get("/news")
def get_all_news():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    grouped_news = {}
    for category in RSS_FEEDS.keys():
        cursor.execute(
            "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles WHERE category = %s ORDER BY published DESC LIMIT 15", 
            (category,)
        )
        grouped_news[category] = [dict(row) for row in cursor.fetchall()]
        
    conn.close()
    return grouped_news

@app.get("/alerts")
def get_latest_alerts():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        "SELECT title, link, published FROM articles ORDER BY published DESC LIMIT 5"
    )
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/news/{category}")
def get_category_news(category: str):
    if category not in RSS_FEEDS:
        raise HTTPException(status_code=404, detail="Category not found")
        
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        "SELECT title, link, published, category, summary_en, summary_ua, summary_ru, image_url FROM articles WHERE category = %s ORDER BY published DESC LIMIT 15",
        (category,)
    )
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]
