# Markets Tab — UI Improvement

## What Was Changed
- The Markets tab received a visual and functional overhaul.
- Instrument cards have improved layout and styling.
- Chart detail view shows a 30-day Chart.js price graph.
- Users can edit (add/remove) which charts are shown and reorder them.
- Chart order is persisted per-user in PostgreSQL.
- Related news is displayed inside the chart detail view.

## Why It Was Needed
- The original Markets tab was a basic read-only grid.
- Users needed to customize which instruments they track.
- Price cards had inconsistent sizing and lacked visual hierarchy.
- The chart order reset on every page load.

## How It Works Now
1. On Markets tab open, `fetchMarkets()` loads all instrument price data.
2. `renderMarkets()` builds cards using the user's saved chart order.
3. The "Edit order" button enters edit mode — cards become draggable.
4. Drag-and-drop reordering uses pointer events and data attributes.
5. The "Add chart" button opens `.mk-modal-ov` — a bottom sheet with all available instruments.
6. Toggling an instrument in the modal adds/removes it from the grid.
7. Saving the modal POSTs the new order/selection to the backend.
8. Clicking a card enters detail view: full chart + related news.

## Key Files / Functions / Endpoints
- **File:** `main.py`
- **Backend:**
  - Table: `user_market_prefs` (user_id TEXT, prefs JSONB)
  - `GET /api/webapp/market-prefs` — returns saved chart list and order
  - `POST /api/webapp/market-prefs` — saves chart list and order
  - `GET /api/webapp/market-data/{code}` — returns 30-day OHLC data for a symbol
- **Frontend JS:**
  - `mkData` — in-memory array of market instrument data
  - `fetchMarkets()` — loads price data for all instruments
  - `renderMarkets()` — renders the instrument grid
  - `openMkDetail(code)` — enters chart detail view
  - `mkDragStart/mkDragOver/mkDrop` — drag-and-drop handlers
  - `openMkAddModal()` / `saveMkModal()` — add-chart modal logic
- **CSS classes:** `.pgrid`, `.pcard`, `.mk-modal-ov`, `.mk-modal-footer`

## Known Limitations / Future Improvements
- Drag-and-drop on touch devices (mobile) uses pointer events but may behave differently across browsers.
- Real-time price updates are not implemented; data refreshes on tab open.
- Future: live price tickers, more instruments (metals, crypto), custom date ranges for charts.
