# Currencies Tab — UI Improvement

## What Was Changed
- The Currencies tab was redesigned to match the visual style of the improved Markets tab.
- Two view modes added: **compact** (rate cards) and **chart** (30-day rate graph).
- Users can add, remove, and reorder currencies.
- Currency selection is persisted per-user in PostgreSQL.
- The add-currency modal save button was fixed to stay visible above the bottom navigation.
- Modal z-index raised from 200 to 400 so it renders above the bottom nav.

## Why It Was Needed
- The original Currencies tab had a basic list with no customization.
- Users wanted to track only the currencies relevant to their business.
- The modal save button was hidden behind the bottom navigation bar on mobile.
- There was no chart view for exchange rate history.

## How It Works Now
1. On Currencies tab open, `loadCurrencies()` fetches the user's saved currency list and rates.
2. `renderCurrencies()` renders either compact cards or chart views depending on mode.
3. The view toggle button (💱) switches between compact and chart mode.
4. The "Edit currencies" button opens the currency edit modal.
5. In the modal, users check/uncheck currencies to add or remove them.
6. Saving POSTs the new currency list to the backend.
7. The compact view shows the current rate and 24h change for each currency.
8. The chart view shows a 30-day rate history using Chart.js.

## Key Files / Functions / Endpoints
- **File:** `main.py`
- **Backend:**
  - Table: `user_currency_prefs` (user_id TEXT, prefs JSONB)
  - `GET /api/webapp/currency-prefs` — returns saved currency list for user
  - `POST /api/webapp/currency-prefs` — saves currency list
  - `GET /api/webapp/currency-rates` — returns current rates for all tracked currencies
  - `GET /api/webapp/currency-chart/{code}` — returns 30-day rate history
- **Frontend JS:**
  - `loadCurrencies()` — fetches prefs and rates
  - `renderCurrencies()` — renders compact or chart view
  - `toggleCurrencyView()` — switches between compact and chart
  - `openCurrEditModal()` / `saveCurrModal()` — edit modal logic
- **CSS fixes:**
  - `.mk-modal-ov { z-index: 400 }` — modal above bottom nav
  - `.mk-modal-footer { padding-bottom: max(16px, calc(env(safe-area-inset-bottom) + 12px)) }` — safe area padding

## Known Limitations / Future Improvements
- Exchange rates are fetched from a cached external API; updates may lag by up to 15 minutes.
- The chart view shows one currency at a time; multi-currency comparison is not yet supported.
- Future: custom base currency selection, rate alerts/notifications.
