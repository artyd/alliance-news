# MacroHarvey — Вступ і структура

## Що таке цей проект

**MacroHarvey** — B2B-платформа моніторингу ринкових новин для **української фармацевтичної/хімічної компанії-імпортера**. Платформа автоматично:

- збирає новини з Google News RSS по **13 тематичних категоріях** кожні 15 хвилин
- дедублікує статті за URL **та** заголовком до вставки
- перекладає та аналізує через **GPT-4o-mini** (EN / UA / RU) з fallback на Gemini
- витягує повний текст (Stage 1 — Trafilatura) + структуровані факти (Stage 2 — GPT-4o-mini)
- генерує **PDF-звіти** (09:00 daily/weekly, 14:00 midday) з аналітикою та графіками
- надсилає новини та звіти через **Telegram-бот** підписникам
- моніторить 10 товарних ринків кожні 30 хв (Telegram-алерти ±7%)
- відображає новини у **Telegram Mini App** (5 вкладок) та **index.html** (SPA)

## Структура файлів

```
news/
├── main.py              # Backend (FastAPI + всі задачі), ~7000 рядків
├── database.py          # Застарілий модуль (SQLite-легасі, не використовується)
├── index.html           # Фронтенд (SPA, Vanilla JS/HTML/CSS)
├── requirements.txt     # Python-залежності
├── .env                 # API ключі (не в git)
├── .gitignore
├── DejaVuSans.ttf       # Шрифт для PDF (кирилиця, regular)
├── DejaVuSans-Bold.ttf  # Жирний варіант шрифту
├── logo.png             # Лого для PDF + Mini App splash
└── docs/                # Ця папка — розбита документація
```

## Змінні оточення (.env)

| Змінна | Призначення |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (Supabase або інший PostgreSQL) |
| `GEMINI_API_KEY` | Google Gemini API — fallback для саммарі |
| `OPENAI_API_KEY` | OpenAI — основний (GPT-4o-mini саммарі, Stage 2, звіт) |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram-бота |
| `TELEGRAM_CHAT_ID` | Comma-separated список адмін chat_id (завжди отримують UA) |
| `WEBAPP_URL` | URL Mini App (напр. `https://178-104-96-245.sslip.io/webapp`) |
| `NOVA_POSHTA_API_KEY` | Нова Пошта API (трекінг посилок) |
| `SEVENTEEN_TRACK_KEY` | 17track API (DHL, EMS, Meest, контейнери) |

## Технологічний стек

- **Python 3.11+**, FastAPI, uvicorn (ASGI)
- **PostgreSQL** (Supabase / будь-який PostgreSQL-хостинг)
- **Hetzner** (Німеччина) — продакшн-сервер
- **Caddy** — reverse proxy + автоматичний Let's Encrypt SSL
- **sslip.io** — wildcard DNS для HTTPS без купівлі домену
