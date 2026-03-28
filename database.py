import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")

def init_db():
    conn = psycopg2.connect(SQLALCHEMY_DATABASE_URL)
    conn.autocommit = True
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
            language TEXT DEFAULT 'en'
        )
    ''')
    cursor.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS subscriptions TEXT DEFAULT 'all'")
    
    conn.close()

def get_db_connection():
    conn = psycopg2.connect(SQLALCHEMY_DATABASE_URL)
    return conn
