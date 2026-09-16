# Weather Widget v1 — New Feature

## What Was Changed
- A new **Weather** widget was added to the app as a fully selectable tab.
- A 3D interactive globe (Globe.GL + WebGL) displays city markers worldwide.
- Users can search for any city and get current weather + 5-day forecast.
- Ukrainian city markers are shown in amber (#FBBF24); world capitals in green (#22C55E).
- A local index of 43 Ukrainian cities and 49 world capitals enables offline search fallback.
- Russian city queries show a "pig overlay" Easter egg instead of weather results.
- Weather data comes from the Open-Meteo API (no API key required).
- The globe card is displayed as a large square using `aspect-ratio: 1/1`.

## Why It Was Needed
- Users requested weather for logistics and market planning (Ukraine, EU, Asia).
- The app needed a visually engaging tab to complement the data-heavy markets/currencies tabs.
- A 3D globe provides geographic context for where partners and shipments are located.

## How It Works Now

### Globe
1. When the Weather tab opens, `ensureWeatherLoaded()` is called inside a `requestAnimationFrame`.
2. `loadGlobeGLScript()` loads Globe.GL from jsdelivr CDN (with unpkg as fallback).
3. After the script loads, `initWeatherGlobe()` initializes the 3D globe with `WEATHER_CITIES` markers.
4. Clicking a marker calls `selectWeatherLocation()` which fetches weather for that city.
5. `resizeWeatherGlobe()` measures the card's actual width and sets the globe dimensions explicitly.

### City Search
1. User types a city name in the search input and presses Enter or the 🔎 button.
2. `searchWeatherCity()` first checks `isRussianCityQuery()` — Russian cities show the pig overlay.
3. Non-blocked queries call `GET /api/webapp/weather/search?q=...` on the backend.
4. The backend checks `_RU_BLOCKED_QUERIES`, then searches `_UA_CITY_INDEX` + `_WORLD_CAPITALS_NO_RU` locally.
5. If local results are insufficient, Open-Meteo Geocoding API is queried (always with `language=en`).
6. Results are merged and returned; Russian results are filtered out.

### Weather Card
1. `selectWeatherLocation()` calls `GET /api/webapp/weather/current?lat=...&lon=...`.
2. The backend fetches from Open-Meteo Forecast API (no key needed).
3. WMO weather codes are mapped to emoji + text by `_wmo_weather()`.
4. `renderWeatherCard()` displays temperature, feels-like, humidity, wind, clouds, pressure, precipitation.
5. `renderWeatherForecast()` shows a 5-day forecast row with daily high/low and conditions.

### Russian City Easter Egg
- Queries matching Russian city names return `{ blocked: true }` from the backend.
- Client-side `isRussianCityQuery()` also catches them before the API call.
- A full-screen pig overlay appears with a typewriter animation: "хрю-хрю 🐷 хрю-хрю".

## Key Files / Functions / Endpoints
- **File:** `main.py`
- **Backend:**
  - `_UA_CITY_INDEX` — 43 Ukrainian cities with alternate name spellings
  - `_WORLD_CAPITALS_NO_RU` — 49 world capitals (Russia excluded)
  - `_RU_BLOCKED_QUERIES` — set of ~40 Russian city name variants
  - `_normalize_city_q()`, `_is_ru_blocked()`, `_local_city_search()`, `_filter_ru_results()`, `_merge_city_results()` — search helpers
  - `GET /api/webapp/weather/search` — city geocoding with local fallback + Russian filter
  - `GET /api/webapp/weather/current` — current conditions + 5-day forecast from Open-Meteo
  - `_wmo_weather(code)` — maps WMO codes 0–99 to (text, emoji)
- **Frontend JS:**
  - `WEATHER_CITIES` — ~60 city markers for the globe (26 Ukrainian, 34 world)
  - `loadGlobeGLScript()` — dual-CDN promise-based Globe.GL loader
  - `ensureWeatherLoaded()` — triggers script load + init on Weather tab open
  - `initWeatherGlobe()` — sets up Globe.GL instance with markers and click handlers
  - `resizeWeatherGlobe()` — measures card width, sets explicit height, updates Globe dimensions
  - `searchWeatherCity()` — handles search input with Russian city check
  - `selectWeatherLocation()` — fetches and renders weather for a location
  - `renderWeatherCard()` / `renderWeatherForecast()` — UI rendering functions
  - `showPigOverlay()` / `closePigOverlay()` / `startPigTypingAnimation()` — Easter egg
- **CSS:** `.weather-globe-card` (aspect-ratio:1/1, min-height:280px), `#weather-globe` (position:absolute;inset:0)
- **Caches:** `_weather_search_cache` (30 min TTL), `_weather_current_cache` (10 min TTL)

## Known Limitations / Future Improvements
- Globe.GL requires WebGL — devices without GPU support see a text fallback.
- Open-Meteo has a fair-use rate limit; heavy traffic could cause timeouts.
- The globe texture images are loaded from unpkg.com CDN — offline use is not supported.
- Weather Widget v2 ideas: hourly forecast, UV index, air quality, animated weather layers on the globe, precipitation radar overlay.
