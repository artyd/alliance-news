# MacroHarvey — Bug Log

> Кожен запис: баг, причина, фікс, як уникнути в майбутньому.
> Свіжі записи — зверху.

---

## 2026-05-14 — Сесія 27 (JS Parse Error + Stale webapp.html)

### BUG-009 — Стала `webapp.html` (від 28 квітня) подавалась замість `_WEBAPP_HTML` з main.py
**Область:** Backend, /webapp route  
**Симптом:** Всі виправлення з попередніх сесій (tracking fix, boot fix, null-guards) не давали ефекту. Telegram Mini App відкривався але жодна кнопка, вкладка, трекінг, новини не працювали.  
**Причина:** Файл `webapp.html` існував на диску (дата: 28 квітня). Маршрут `/webapp` спочатку перевіряв `os.path.exists("webapp.html")` і якщо файл існував — повертав його, повністю ігноруючи весь актуальний код у `_WEBAPP_HTML` з main.py.  
**Фікс:** `webapp.html` перейменовано в `webapp.html.bak`. Маршрут `/webapp` оновлено — тепер завжди повертає `_WEBAPP_HTML` з main.py + логує джерело.  
**Як уникнути:** Ніколи не залишати `webapp.html` поруч з main.py якщо основна логіка в `_WEBAPP_HTML`. Або навпаки — завжди явно вказати який файл має пріоритет.

---

### BUG-008 — Literal U+2028/U+2029 в JS regex ламали парсинг всього скрипту
**Область:** JS, tracking input normalization  
**Симптом:** App відкривався візуально (HTML + CSS рендерились), але весь JavaScript не виконувався. Жодна кнопка, вкладка, функція не реагували на натискання.  
**Причина:** В `doTrack()` і `doTrackContainer()` regex для нормалізації трекінг-номерів містив **літеральні** символи U+2028 (Line Separator) і U+2029 (Paragraph Separator) всередині `/[...]/` regex literal. Згідно зі специфікацією JS (до ES2019), ці символи завершують рядковий та регулярний літерал — весь `<script>` блок ставав синтаксично невалідним. Браузер мовчки ігнорував весь JS.  
**Причина появи:** Символи були скопійовані з Python backend коду де нормалізація використовує `unicodedata` — вони невидимі в більшості редакторів і не виявляються без спеціального hex-аналізу.  
**Фікс:**  
1. Додана функція `normalizeTrackingNumber(raw)` з regex виключно з `\uXXXX` escape-послідовностями (U+200B, U+200C, U+200D, U+200E, U+200F, U+202F, U+205F, U+3000, U+FEFF, U+2010-U+2015 тощо).  
2. `doTrack()` і `doTrackContainer()` тепер використовують `normalizeTrackingNumber(raw)`.  
3. Перевірено: `node --check` на вилученому JS — EXIT:0.  
4. Перевірено: сканування `_WEBAPP_HTML` — U+2028 і U+2029 відсутні.  
**Як уникнути:** Правило: **ніколи не вставляти невидимі Unicode символи (U+2000–U+202F, U+2028, U+2029, U+FEFF тощо) в JS regex або рядкові літерали**. Завжди використовуй `\uXXXX`. Після кожної сесії де JS змінювався — запускати `node --check extracted_webapp.js`.

---

## 2026-05-14 — Сесія 26 (Bug Audit Pass)

### BUG-007 — `tab()` викликав неіснуючі функції `fetchCurrencies` / `fetchWarehouse`
**Область:** JS, tab switching  
**Симптом:** При перемиканні на таби 'currency'/'warehouse' (якщо їх додати до ALL_TABS) — нічого не відбувається, без помилки (захист `typeof` спрацьовував).  
**Причина:** Реальні функції називаються `loadCurrencies()` і `loadWarehouse()` (задані в addNav-секції), але в `tab()` були хардкоджені старі назви.  
**Фікс:** Замінено `fetchCurrencies` → `loadCurrencies`, `fetchWarehouse` → `loadWarehouse` в тілі `tab()`.  
**Як уникнути:** При додаванні нової функції — одразу перевіряти всі місця де вона викликається по імені.

---

### BUG-006 — Unsafe `getElementById` без null-check у критичних функціях
**Область:** JS, DOM  
**Симптом:** Якщо будь-який DOM-елемент відсутній (наприклад, при рендері сторінки з помилкою або зміні структури HTML) — весь JS крашився з `Cannot read property of null`.  
**Причина:** `buildChips()`, `renderGrid()`, `fetchMarkets()`, `fetchNews()`, `fetchReports()`, `applyTheme()`, `cycleLang()`, `updateStaticText()` — всі звертались до результату `getElementById()` без перевірки на `null`.  
**Фікс:** Додані null-guards (`if(!el) return;`, `if(el) el.textContent = ...`) у всіх перерахованих функціях.  
**Як уникнути:** Правило: завжди `const el = getElementById(...); if(!el) return;` перед будь-яким зверненням до властивостей елементу. Ніколи не chain-ити одразу `.innerHTML`, `.textContent`, `.style` без null-check.

---

### BUG-005 — Splash застрявав назавжди при JS-помилці
**Область:** JS, boot  
**Симптом:** При будь-якій JS-помилці під час завантаження — splash залишався назавжди. App не з'являвся.  
**Причина:** Старий boot-код: `setTimeout(() => hideSplash(), 2200)` планувався ПІСЛЯ синхронних викликів `buildChips()` + `fetchNews()`. Якщо будь-який з них кидав виключення — setTimeout ніколи не реєструвався.  
**Фікси (шари захисту):**
1. CSS animation: `#app { animation: forceShowApp .1s linear 3s forwards }` — спрацьовує без JS.
2. Inline `<script>` після `<body>` — мінімальний скрипт, `setTimeout(3000)`, запускається до основного бандлу.
3. `setTimeout(hideSplash, 2500)` — перше що робиться в `load` event, до будь-яких try/catch.
4. `window.onerror` + `unhandledrejection` — глобальні хендлери, викликають `hideSplash()` при будь-якій помилці.
5. `body.boot-fallback` CSS class — встановлюється `hideSplash()`, override через `!important`.  
**Як уникнути:** Правило: splash-hide НІКОЛИ не повинен залежати від успіху бізнес-логіки. Завжди плануй `setTimeout(hideSplash, N)` першим у `load`, до будь-яких ініціалізацій.

---

### BUG-004 — Telegram WebView кешував стару HTML
**Область:** Backend, Telegram  
**Симптом:** Після деплою нової версії Telegram відкривав стару HTML з кешу.  
**Причина:** Telegram webview кешує Mini App HTML по URL. Якщо URL не змінювався — старий HTML завантажувався.  
**Фікс:**
1. `webapp_url_with_version()` — Python-функція, генерує `WEBAPP_URL?v=<timestamp>` при кожному виклику.
2. `/webapp` endpoint тепер повертає `Cache-Control: no-store, no-cache, must-revalidate, max-age=0`.
3. Всі три місця де Telegram отримує URL (`/start`, `/app`, `/menu`) тепер використовують `webapp_url_with_version()`.  
**Як уникнути:** Завжди додавай `?v=` або `Cache-Control: no-store` для Mini App ендпоінтів. При деплої — також можна надіслати `/start` заново щоб отримати свіжу кнопку.

---

## 2026-05-14 — Сесія 26 (Tracking + Feature Expansion)

### BUG-003 — SQL `last_checked = NULL` при INSERT відстежуваних посилок
**Область:** Backend, SQL  
**Симптом:** `last_checked` завжди NULL в БД, сортування/фільтрація по даті не працювала.  
**Причина:** INSERT у `tracked_shipments` не передавав `NOW()` для `last_checked`. ON CONFLICT UPDATE також не оновлював `last_checked` і `type`.  
**Фікс:** INSERT: `last_checked = NOW()`. ON CONFLICT SET: `type = EXCLUDED.type, last_checked = NOW()`.  
**Як уникнути:** При додаванні нових колонок в INSERT — завжди перевіряти чи вони включені в ON CONFLICT UPDATE.

---

### BUG-002 — Nova Poshta повертала `ok=False` замість pending, блокуючи збереження
**Область:** Backend, Tracking  
**Симптом:** Посилки Nova Poshta не зберігались. Навіть при валідному трекінг-номері — кнопка "Зберегти" не реагувала.  
**Причина (backend):** `_track_nova_poshta()` повертала `{ok: False}` при будь-якій помилці API.  
**Причина (frontend):** `doTrack()` не встановлював `_lastTrkData`, `renderTrackResult()` скидав його в `null` при `ok=false`.  
**Фікс:** Backend: всі гілки помилок повертають `{ok: True, is_pending: True, ...}`. Frontend: `doTrack()` завжди встановлює `_lastTrkData` — або з даних API, або з fallback pending-об'єкту.  
**Як уникнути:** Контракт: `api_webapp_track` ЗАВЖДИ повертає `ok=True + can_save=True + tracking_url + carrier_name`. Ніколи не додавати `return {"ok": False}` без safety-wrap.

---

### BUG-001 — 17TRACK `register` помилка блокувала cached fallback
**Область:** Backend, Tracking  
**Симптом:** При rate-limit або тимчасовій помилці `register` ендпоінту 17TRACK — функція одразу повертала помилку, не намагаючись отримати кешовані дані через `gettrackinfo`.  
**Причина:** `if register_error: return register_error` — негайний `return` замість fallback.  
**Фікс:** При `register_error` — зберігаємо в `last_error`, продовжуємо до `gettrackinfo`. Realtime запит тільки якщо `not last_error`.  
**Як уникнути:** API-функції трекінгу повинні мати багаторівневий fallback: realtime → cached → pending. Ніколи не `return` одразу при помилці першого рівня.

---

## Правила для майбутніх сесій

1. **Splash safety:** `setTimeout(hideSplash, N)` — завжди перший рядок у `load` event.
2. **Null-safe DOM:** `const el = getElementById(...); if(!el) return;` — обов'язково перед `.innerHTML`, `.textContent`, `.style`.
3. **Tracking contract:** `ok=True + can_save=True + tracking_url + carrier_name` — завжди для посилок.
4. **Tab functions:** Функції у `tab()` мають відповідати реальним іменам (не alias'ам).
5. **Cache busting:** `webapp_url_with_version()` для всіх Telegram кнопок. `/webapp` endpoint: `Cache-Control: no-store`.
6. **SQL upsert:** ON CONFLICT UPDATE завжди включає всі колонки що можуть змінитись, включаючи timestamp.
7. **API responses:** Всі endpoints повертають JSON. `try/except` обов'язковий. `{"ok": false, "error": "..."}` при помилці.
