# Desktop / Fullscreen / Responsive Layout Update

## What Was Changed
- A fullscreen toggle button (`⛶`, id=`fsbtn`) was added to the header.
- CSS `body.fullscreen-layout` fallback class added for environments where native fullscreen is restricted.
- `.hbtn.active` CSS added so the fullscreen button shows an active state.
- Weather tab displays in a two-column layout at screen widths ≥ 980px (globe left, weather card right).
- Panel max-width set to 1400px at ≥ 1280px screen width to prevent content from stretching too wide.
- The 3D globe initialization bug was fixed: the globe was invisible due to zero height at init time.
- Globe resizing now sets `card.style.height` explicitly — no longer relying on `aspect-ratio` alone.
- `ResizeObserver` added to watch the globe card and trigger resize on container size changes.
- Window resize handler is now debounced (120ms) and calls `handleAppViewportResize()`.
- `orientationchange` event added.
- Telegram `viewportChanged` event added.
- `loadGlobeGLScript()` now has a dual-CDN fallback (jsdelivr → unpkg) with a Promise-based loader.
- Fullscreen toggle calls `tg.expand()` first, then attempts `requestFullscreen()`, then falls back to the CSS class.

## Why It Was Needed
- The 3D globe was not rendering because `container.clientHeight` was 0 at initialization time.
- The globe card used `aspect-ratio:1/1` for height, but this is not computed synchronously — Globe.GL saw height=0 and created an invisible canvas.
- The fullscreen button existed in HTML but had no CSS for its active state and didn't resize the globe after toggling.
- Wide Telegram Desktop/Web windows caused Weather content to look cramped or misaligned.
- Window resize events fired too frequently, causing performance issues.

## How It Works Now

### Globe Fix
1. `#weather-globe` now uses `position:absolute;inset:0` — fills the card regardless of height computation timing.
2. `.weather-globe-card` has `min-height:280px` as a fallback guarantee.
3. `initWeatherGlobe()` measures `card.getBoundingClientRect().width` and sets `card.style.height = size + 'px'` **before** creating the Globe instance.
4. `resizeWeatherGlobe()` always sets `card.style.height` explicitly so the square is maintained after every resize.
5. Two resize calls are scheduled after init: 80ms and 350ms.

### Globe Script Loading
1. `loadGlobeGLScript()` returns a single shared Promise.
2. First tries `https://cdn.jsdelivr.net/npm/globe.gl/dist/globe.gl.min.js`.
3. On error, tries `https://unpkg.com/globe.gl/dist/globe.gl.min.js`.
4. If both fail, `weatherGlobeLoadFailed = true` and the fallback message is shown.
5. City search and weather card continue to work even when the globe is unavailable.

### Tab Switch Timing
1. `tab('weather')` now wraps `ensureWeatherLoaded()` in `requestAnimationFrame`.
2. This ensures the panel is fully visible and layout is computed before Globe.GL measures dimensions.

### Fullscreen
1. Button click calls `toggleDesktopFullscreen()`.
2. Calls `tg.expand()` first.
3. Tries native `requestFullscreen()` on `#app` (with webkit/ms vendor prefixes).
4. On success: adds `body.fullscreen-layout` class.
5. On failure: toggles `body.fullscreen-layout` as CSS fallback.
6. After toggle: calls `updateFullscreenButtonState()` and schedules two resize calls (120ms, 450ms).
7. `document.fullscreenchange` event also triggers resize.

### Two-Column Weather Layout (≥ 980px)
- `.weather-main` becomes a CSS grid: `minmax(0,640px) minmax(280px,1fr)`.
- Left column: globe card.
- Right column: weather card + forecast (`.weather-side`).
- On mobile: single column flex layout (default).

### Resize Handling
- `window.resize` → debounced 120ms → `handleAppViewportResize()`.
- `window.orientationchange` → 250ms delay → `handleAppViewportResize()`.
- `document.fullscreenchange` → 120ms + 450ms → `handleAppViewportResize()`.
- `tg.onEvent('viewportChanged')` → 120ms → `handleAppViewportResize()`.
- `ResizeObserver` on `.weather-globe-card` → `resizeWeatherGlobe()` on any size change.

## Key Files / Functions / Endpoints
- **File:** `main.py`
- **CSS:**
  - `#weather-globe { position:absolute; inset:0 }` — globe fill fix
  - `.weather-globe-card { min-height:280px }` — height fallback
  - `.weather-main / .weather-side` — two-column wrapper
  - `@media(min-width:980px)` — two-column weather layout
  - `@media(min-width:1280px) { .panel { max-width:1400px } }` — panel width cap
  - `body.fullscreen-layout #app { width:100vw; max-width:none }` — fullscreen CSS
  - `.hbtn.active { border-color:var(--green); color:var(--green) }` — active button state
- **HTML:** `<button class="hbtn" id="fsbtn" onclick="toggleDesktopFullscreen()">⛶</button>` in header
- **JS functions:**
  - `loadGlobeGLScript()` — dual-CDN Globe.GL loader with Promise
  - `initWeatherGlobe()` — fixed: measures card rect, sets height before Globe init
  - `resizeWeatherGlobe()` — fixed: measures card rect, sets explicit height
  - `handleAppViewportResize()` — central resize dispatcher
  - `observeWeatherGlobeResize()` — ResizeObserver setup
  - `toggleDesktopFullscreen()` — fixed: tg.expand first, resize after
  - `updateFullscreenButtonState()` — updates button title and active class

## Known Limitations / Telegram Desktop Notes
- True OS-level fullscreen inside Telegram Desktop's WebView may be blocked depending on platform/version.
- The implementation is best-effort: `tg.expand()` + native `requestFullscreen()` + CSS `fullscreen-layout` fallback.
- Telegram Web (browser) typically allows native fullscreen without issues.
- Globe.GL textures (earth images) are loaded from unpkg.com CDN — no offline support.
- Future: Weather Widget v2 could use a more lightweight globe renderer for lower-end devices.
