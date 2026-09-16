# Widget Manager — Bottom Navigation Update

## What Was Changed
- The "Add" tab was redesigned from a placeholder into a full widget manager.
- Users can choose which widgets appear in the bottom navigation bar.
- Up to 4 widgets can be active at the same time (fixed Add button always remains).
- Widget preferences are saved per-user to PostgreSQL and restored on next open.

## Why It Was Needed
- The original bottom nav had a fixed set of tabs with no customization.
- Users needed a way to show only the tabs they actually use.
- The app was growing in features and needed a scalable navigation system.

## How It Works Now
1. On app load, `loadUserWidgetPrefs()` fetches the user's saved widget list from `/api/webapp/widget-prefs`.
2. `renderBottomNav()` builds the bottom nav dynamically based on `_widgetPrefs`.
3. Opening the Add tab shows `renderWidgetManager()` — a grid of all 6 available widgets.
4. Tapping a widget card toggles it active/inactive (max 4 active enforced client-side).
5. A "Save" button calls `saveUserWidgetPrefs()` which POSTs to `/api/webapp/widget-prefs`.
6. The nav re-renders immediately after saving.

## Key Files / Functions / Endpoints
- **File:** `main.py` (all backend + frontend in one file)
- **Backend:**
  - Table: `user_widget_prefs` (user_id TEXT, prefs JSONB)
  - `GET /api/webapp/widget-prefs` — returns saved widget list for user
  - `POST /api/webapp/widget-prefs` — saves widget list for user
- **Frontend JS:**
  - `WIDGETS` constant — 6 widget definitions (key, icon, label in ua/ru/en)
  - `loadUserWidgetPrefs()` — fetches saved prefs or returns defaults
  - `saveUserWidgetPrefs()` — POSTs current selection
  - `renderBottomNav()` — builds `<nav id="bottom-nav">` from `_widgetPrefs`
  - `renderWidgetManager()` — builds the Add tab widget grid
  - `_widgetPrefs` — in-memory array of currently active widget keys
- **Default widgets:** `['news', 'reports', 'markets', 'tracking']`
- **All available widgets:** `news`, `reports`, `currencies`, `markets`, `tracking`, `weather`

## Known Limitations / Future Improvements
- Widget ordering within the nav is fixed to the order saved; drag-to-reorder is not yet implemented.
- The Add button is always fixed at position 3 (center); this cannot be changed.
- Widget preferences are stored by Telegram user ID; anonymous users get the defaults.
- Future: allow drag-and-drop reordering of active widgets in the bottom nav.
