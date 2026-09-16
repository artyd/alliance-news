# MacroHarvey — Розклад планувальника (APScheduler)

> Всі cron-задачі для звітів обмежені `day_of_week='mon-fri'` (щоб уникнути вихідних).
> Timezone: **Europe/Kyiv**.

## Таблиця cron-задач

| Час (Kyiv) | Дні | Задача |
|---|---|---|
| Кожні 15 хв | Що дня | `fetch_and_store_news()` — новини з RSS |
| Кожні 2 год | Що дня | `backfill_missing_full_text()` — Stage 1 backfill (150 статей) |
| Кожні 2 год 30 хв | Що дня | `backfill_missing_facts()` — Stage 2 backfill (100 статей) |
| **08:30** | Пн–Пт | `backfill_missing_full_text()` — перед ранковим звітом |
| **08:45** | Пн–Пт | `backfill_missing_facts()` — 15 хв до звіту |
| **09:00** | **Пн–Пт** | `send_daily_report_to_users()` — Пн–Чт: `daily_brief`; **Пт: `weekly`** |
| **13:30** | Пн–Пт | `backfill_missing_full_text()` — перед полуденним звітом |
| **13:45** | Пн–Пт | `backfill_missing_facts()` — 15 хв до звіту |
| **14:00** | **Пн–Пт** | `send_midday_report_to_users()` — полуденний PDF (Block 2 + Block 3) |
| Кожних 30 хв (08:00–23:00) | Що дня | `monitor_market_alerts()` — моніторинг цін ±7% |
| Щодня | Що дня | `cleanup_old_news()` — видалення статей і `telegram_sent` старших 30 днів |

## Логіка вибору режиму у `send_daily_report_to_users()`

Функція викликається щодня о 09:00 — режим обирається **автоматично** за днем тижня:

```python
is_friday = now_kyiv.weekday() == 4   # 0=Пн … 4=Пт
report_mode = "weekly" if is_friday else "daily_brief"
```

Один scheduler rule = два режими. П'ятниця → повний тижневий звіт (Block 1+2+3, 7-денне вікно).

## Важливі примітки

- `day_of_week='mon-fri'` **обов'язковий** для всіх звітних задач. Без нього APScheduler запускається щодня. (Bug виправлено у Сесії 17)
- Backfill запускається **двічі** перед кожним звітом (08:30/08:45 та 13:30/13:45), щоб Stage 1 і Stage 2 встигли наповнити дані перед генерацією.
- `monitor_market_alerts()` — окремий asyncio loop, незалежний від fetch-циклу.
