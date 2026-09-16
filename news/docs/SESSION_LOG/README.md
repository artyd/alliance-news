# MacroHarvey — Журнал сесій

> Кожна сесія — короткий запис: що зроблено, як, які проблеми вирішені.
> Свіжі сесії — зверху.

---

## Сесія 26 | 2026-05-14

**Що зроблено:** Повний фікс системи трекінгу — Nova Poshta + 17TRACK + save flow.

### Кореневі причини багів

**Баг 1 — Посилки не зберігались (Nova Poshta і всі інші):**
- `doTrack()` (JS) ніколи не встановлював `_lastTrkData` для посилок (тільки `doTrackContainer` це робив).
- `renderTrackResult()` скидав `_lastTrkData = null` при `ok=false` → кнопка "Зберегти" завжди повертала без дії.

**Баг 2 — Nova Poshta повертала помилку замість pending:**
- `_track_nova_poshta()` повертала `{ok: False}` при будь-якій помилці API (мережа, `success=false`, немає даних).
- Це блокувало збереження посилки.

**Баг 3 — 17TRACK відразу повертав помилку при невдалій реєстрації:**
- Якщо `register` ендпоінт падав (rate limit, тимчасова помилка), функція повертала `ok=False` одразу, не намагаючись `gettrackinfo` (кешовані дані).

**Баг 4 — SQL ON CONFLICT не оновлював `last_checked` і `type`:**
- INSERT ставив `last_checked = NULL` замість `NOW()`.
- ON CONFLICT UPDATE пропускав `type` і `last_checked`.

### Виправлення

**`_track_nova_poshta()`:**
- Немає API ключа / мережева помилка / `success=false` → тепер `ok=True` + pending статус замість `ok=False`.
- Додані детальні `logger.info/warning/error` логи.

**`api_webapp_track()`:**
- Покращена нормалізація номера: unicode пробіли, нерозривні дефіси, zero-width символи.
- Safety wrap для Nova Poshta і 17TRACK: якщо повертають `ok=False` → конвертується у `ok=True` + pending.
- Немає жодного API ключа → тепер `ok=True` + pending (замість `ok=False`).
- Доданий `_PENDING_STEP` константа.
- Логи по кожному кроку.

**`_track_17track()`:**
- При помилці реєстрації (`register_error`) → не повертає одразу, зберігає до `last_error`, намагається `gettrackinfo` як fallback.
- При відкиданні реєстрації (`rejected_error`) → теж fallback замість негайного повернення.
- Realtime запит — тільки якщо реєстрація пройшла (`if realtime and not last_error`).
- `last_error` в кінці → friendly pending замість `ok=False`.
- Детальні логи на кожному кроці.

**`/api/webapp/track/save` SQL:**
- INSERT: `last_checked = NOW()` (замість `NULL`).
- ON CONFLICT UPDATE: додано `type = EXCLUDED.type` і `last_checked = NOW()`.

**`doTrack()` (JS):**
- Завжди встановлює `_lastTrkData` (як `doTrackContainer` вже робив): якщо `data.ok` → spread data; інакше → pending fallback.
- Покращена нормалізація числа: regex для unicode пробілів, zero-width символів, різних дефісів.

**`renderTrackResult()` (JS):**
- Прибрано `_lastTrkData = null` при `ok=false` — `doTrack()` вже встановив fallback.

**`doTrackContainer()` (JS):**
- Така ж покращена нормалізація числа.

### Баги виправлено
- **Nova Poshta посилки не зберігались** → `doTrack` не встановлював `_lastTrkData` + `_track_nova_poshta` повертала `ok=False` → подвійний фікс
- **17TRACK помилка при реєстрації блокувала збереження** → fallback до cached `gettrackinfo` + safety wrap у `api_webapp_track`
- **`last_checked = NULL` в БД** → `NOW()` в INSERT та ON CONFLICT
- **`type` губився при ON CONFLICT UPDATE** → додано `type = EXCLUDED.type`

### Доповнення (та ж сесія)

**Вимога:** навіть якщо live tracking недоступний, посилку ОБОВ'ЯЗКОВО можна додати до списку — достатньо номера + перевізника + URL.

**Нові константи та функції:**
- `_CARRIER_INFO` — dict `{carrier_code: (carrier_name, url_template)}` для 6 перевізників + fallback 17track URL
- `_FALLBACK_PARCEL_URL` — `https://www.17track.net/en/track#nums={n}`
- `_stamp_tracking_meta(result, n, carrier_code)` — мутує dict result: додає `tracking_url`, `carrier_name`, `can_save=True`, `is_pending=True` (якщо немає реальних подій)

**`api_webapp_track` Python:**
- Після кожного результату (NP і 17TRACK) викликається `_stamp_tracking_meta(result, n, carrier)`
- Відповідь завжди включає `tracking_url`, `carrier_name`, `can_save: true`, `is_pending: true` (при pending)
- Немає API ключів → `ok=True` + `_stamp_tracking_meta` (was ok=False)

**`renderTrackResult` JS:**
- `carrierLabel = d.carrier_name || d.carrier` — використовує явне ім'я
- Для посилок: якщо є `d.tracking_url` → показує кнопку "Відкрити офіційний сайт" (раніше тільки для контейнерів)
- `isPending = d.is_pending || ...` — враховує backend-флаг
- Auto-retry countdown тільки якщо `rawSteps.length > 0` (не для URL-only посилок)
- Save кнопка: `if(d.can_save !== false)` замість безумовного

**`saveTrkShipment` JS:**
- Guard: `if(!_lastTrkData?.can_save && !_lastTrkData?.ok) return;`
- `carrier_name: d.carrier_name || d.carrier || ...` — правильна пріоритизація
- `type: d.type || trkMode || 'parcel'` — тип береться з відповіді

**`doTrack()` JS fallback:**
- Включає `can_save: true, is_pending: true, tracking_url: '', carrier_name: ''`

### Файли змінено
`main.py` (~7209–7271, 7690–7713, 7835–7980, 6393, 6430–6624, 6636–6657)

---

## Сесія 25 | 2026-05-03

**Що зроблено:** Filter chips трекінгу — повний рефактор + фікс збереження контейнерів + брендові кольори.

### 1. Нові глобальні константи (JS)

**`_CARRIER_COLORS`** — оновлені брендові кольори + нові псевдоніми:
- `Nova Poshta / Nova Post` → `#DA291C`
- `DHL` → `#FFCC00` · `FedEx` → `#4D148C` · `UPS` → `#351C15`
- `Meest / Meest Express` → `#0057B8`
- `EMS / EMS Ukraine / Укрпошта` → `#FF6600`
- `MSC` → `#0097A7` (бірюзовий для контейнерів)

**`_CARRIER_CODE_MAP`** (новий) — `nova→'Nova Poshta'`, `dhl→'DHL'`, `fedex→'FedEx'`, `ups→'UPS'`, `ems→'EMS'`, `meest→'Meest'`

### 2. Filter chips — повний рефактор (`renderSavedShipments`)

**Проблема 1 — HTML escaping bug (чіпи не натискались):**
- `JSON.stringify("Meest")` = `"Meest"` → в HTML: `onclick="trkSetFilter("Meest",this)"` — браузер обривав атрибут на першій `"` → onclick не спрацьовував
- Фікс: `data-carrier="meest"` на кожній кнопці + `onclick="trkSetFilter(this.dataset.carrier,this)"`

**Проблема 2 — чіпи лише для наявних перевізників:**
- Стало: `CARRIER_CHIPS` — статичний масив 8 чіпів (завжди всі, незалежно від того що є в списку):
  `all` / `nova` / `meest` / `dhl` / `fedex` / `ups` / `ems` / `container`
- Кольори chip збігаються з `_CARRIER_COLORS`

**Нова логіка фільтрації:**
- `_trkFilter` = код перевізника (`'nova'`, `'dhl'`, `'container'`, `'all'`)
- Локальний `CARRIER_NAMES`: `nova→['Nova Poshta','Nova Post']`, `meest→['Meest','Meest Express']`, `ems→['EMS','EMS Ukraine','Укрпошта']`
- Порівняння: `CARRIER_NAMES[_trkFilter].includes(s.carrier_name) || s.carrier === _trkFilter`
- Підтримує і старі посилки (лише `s.carrier` заповнено) і нові (є `s.carrier_name` від 17track)

### 3. Фікс збереження контейнерів

**JS — `doTrackContainer`:**
- Було: `_lastTrkData = data.ok ? {...data} : null` → якщо API fail → `null` → кнопка "Зберегти" не реагувала
- Стало: завжди будується `_lastTrkData` з `{ok:true, type:'container', number, carrier, line, tracking_url, steps}`; якщо `data.ok`, поля берутся з відповіді API, інакше — з `_trkCntLine`
- `renderTrackResult` викликається з `_lastTrkData` (не з сирим `data`)

**Python — `api_webapp_track` (контейнерний блок):**
- `_container_info(n)` обгорнуто в `try/except` → при помилці `line='', tracking_url=''`
- Весь `_track_17track` блок обгорнуто в `try/except` → при виключенні повертається `base` (ok:True, tracking_url, no_api:True) замість HTTP 500

**`renderTrackResult` — countdown лише для посилок:**
- Було: countdown "45с" показувався для ВСІХ pending (включаючи контейнери)
- Стало: `if(isPending && d.type !== 'container')` — контейнери не показують countdown

### Баги виправлено
- **Filter chips не натискались** → HTML escaping bug → фікс: `data-carrier` атрибут
- **Filter chips лише для наявних перевізників** → статичний список 8 чіпів
- **Контейнер не зберігався** → `_lastTrkData=null` при API помилці → фікс: JS завжди будує ok:true об'єкт + Python try/except
- **Countdown для контейнерів** → `d.type !== 'container'` guard

### Файли змінено
`main.py` (~6297–6714, 6393–6426, 6522–6573, 7753–7792) — JS globals, renderSavedShipments, doTrackContainer, renderTrackResult, api_webapp_track (Python)

---

## Сесія 22 | 2026-05-02

**Що зроблено:** Реорганізація документації.

- `PROJECT_OVERVIEW.md` (~1500 рядків) розбито на 11 тематичних MD-файлів у папці `docs/`
- Створено `INDEX.md` — навігатор з описом кожного файлу та швидким пошуком
- Створено `SESSION_LOG.md` — цей файл для запису майбутніх сесій

**Структура `docs/`:**
- `01_intro.md` — що таке проект, файли, env vars
- `02_database.md` — всі таблиці БД, індекси
- `03_categories_pipeline.md` — категорії, Stage 1+2, paywall, семафори
- `04_scheduler.md` — APScheduler cron-задачі
- `05_pdf_reports.md` — PDF режими, блоки, рендеринг, facts-first
- `06_market_telegram.md` — market alerts, Telegram-бот
- `07_api_frontend_deps.md` — REST API, index.html, залежності
- `08_startup_architecture.md` — lifespan, HTTPS, архітектурні рішення
- `09_changelog_bugs.md` — хронологія сесій 1-21, всі відомі баги
- `10_mini_app_features.md` — Mini App функціонал (сесії 20-21)
- `11_design_freeze.md` — CSS-еталон Mini App (незмінний)

**Мета:** Скорочення кількості токенів при роботі з проектом. Замість читання всього коду — читати потрібний MD-файл.

---

## Сесія 23 | 2026-05-02

**Що зроблено:** Редизайн вкладки Трекінг + виправлення 17track auto-detect.

- Кнопка «Назад» у трекінгу → стиль як у Ринків (inline-flex, border-radius:20px, `var(--surface)` фон, `var(--text)` колір)
- Кнопка «Видалити з відстеження» → червона (`var(--red)` фон, білий текст) у детальному вигляді та у списку (✕ pill)
- Замінено Screen 1 (великі кнопки) на постійний tab row у верхній частині: 🔍 Знайти / 📋 Мій трекінг
- «Мій трекінг» — додано filter chips по перевізниках (фірмові кольори: DHL=#D40511, FedEx=#4D148C, Nova Poshta=#C8102E тощо)
- При фільтрі по перевізнику — flat список без групових заголовків; при «Всі» — групування як раніше
- Pending/незареєстровані посилки: прибрано loading emoji/текст з детального вигляду; у картці показує «—»
- Додано кнопку «🔍 Авто» до вибору перевізника → `carrier_code=0` + `auto_detection=True` (для Mids Express та інших)
- Нові UI рядки: `trkAll` (Всі/Все/All), `trkSubList` → «Мій трекінг»

**Баги виправлено:**
- Bug: Mids Express та інші перевізники не трекались → причина: немає кнопки «Авто» → виправлено: додана кнопка Авто яка посилає `carrier_code=0` → 17track auto-detects

**Файли змінено:** main.py (CSS, HTML, JS), docs/SESSION_LOG.md

## Сесія 24 | 2026-05-02

**Що зроблено:** Доопрацювання UI трекінгу — детальний вигляд + filter chips.

- Кнопка «🔍 Авто» у виборі перевізника → розтягнута на всю ширину (`grid-column:1/-1`)
- Групові заголовки (`trk-sec-hdr`) видалено з активного списку — залишено тільки кольорові badge на картках
- Screen 4: `trk-nav-back` + `trk-filter-row` переміщено всередину `trk-list-view` → автоматично ховаються при відкритті деталі
- Детальний вигляд: замість однієї кнопки «Назад» — два-кнопковий `trk-nav-row` (← Назад + 🏠 Головна)
- `trk-delivery` card у деталях замінено на `trk-carrier-banner`: великий кольоровий напис перевізника, номер, статус
- CSS додано: `.trk-carrier-banner`, `.trk-carrier-bname`, `.trk-carrier-bnum`, `.trk-carrier-bstat`
- Filter chips: тепер завжди видимі (не тільки коли >1 перевізник), статичний порядок (Nova Poshta → Meest → DHL → FedEx → UPS → EMS), окремий chip «🚢 Контейнери» для контейнерів
- Фільтр «container» → `s.type==='container'` замість `carrier_name`
- `setLang`: додано оновлення `trk-detail-home-lbl`

**Файли змінено:** main.py (CSS, HTML, JS), docs/SESSION_LOG.md

<!-- Нові сесії додавати ВИЩЕ цього рядка -->

---

## Ідеї для розвитку (backlog)

- Push-нотифікації при виході нових звітів (не тільки spike alerts)
- Пошук по новинах у Mini App
- Фільтр новин по даті в Mini App
- Збереження улюблених статей (bookmark)
- Налаштування підписки прямо в Mini App (зараз тільки в боті)
- Порівняльний графік двох інструментів
- Автоматичний аналіз кореляції: ціна → новини (яка новина рухала ціну)
- Weekly email digest (доповнення до Telegram)
- Multitenancy — кілька компаній з різними категоріями

## Шаблон запису сесії

```
## Сесія XX | YYYY-MM-DD

**Що зроблено:** (1 рядок)

- Деталь 1
- Деталь 2

**Баги виправлено:**
- Bug: [симптом] → [причина] → [виправлення]

**Файли змінено:** main.py:XXXX, ...
```
