"""Mini App UI test (Playwright): every screen/state × 3 phone sizes × 2 themes.

Fails on: elements sticking out of the screen or their card, clipped text,
page-level horizontal scroll, JavaScript errors. Runs in CI before every
deploy; locally it is skipped unless UI_TESTS=1 (needs Postgres in
DATABASE_URL and `playwright install chromium`).
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

import pytest

if os.getenv("UI_TESTS") != "1":
    pytest.skip("UI tests run in CI (set UI_TESTS=1 to run locally)", allow_module_level=True)

from playwright.sync_api import sync_playwright  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 8791
BASE = f"http://127.0.0.1:{PORT}"
FX = json.load(open(os.path.join(HERE, "fixtures.json"), encoding="utf-8"))
CHECK_JS = open(os.path.join(HERE, "check_layout.js"), encoding="utf-8").read()
VIEWPORTS = [(360, 740), (390, 844), (430, 932)]
STATES = [
    ("today", ""), ("feed", ""), ("strikes", ""), ("markets", ""), ("currencies", ""),
    ("reports", ""), ("tracking", ""), ("weather", ""), ("warehouse", ""), ("me", ""),
    ("strikes", "openStrike(Object.keys(STRIKES)[0])"),
    ("today", "openNews(Object.keys(NEWS)[0])"),
    ("today", "openAI()"),
    ("today", "openSearch();byId('gq').value='фарм';renderSearch('фарм')"),
    ("markets", "openMkDetail(mkData[0])"),
    ("tracking", "trkPane('mine');trkNav('parcel');selectCarrier(document.querySelector('[data-car=dhl]'))"),
    ("tracking", "setTimeout(()=>openCorp(CORP[1].key),400)"),
    ("tracking", "corpFilter='done';renderCorp()"),
    ("weather", "wxSearch()"),
    ("currencies", "openCurrencyModal()"),
    ("feed", "feedDept='procurement';renderFeedChips();fetchFeed(true)"),
]


@pytest.fixture(scope="module")
def server():
    proc = subprocess.Popen([sys.executable, os.path.join(HERE, "app_server.py"), str(PORT)])
    for _ in range(60):
        try:
            urllib.request.urlopen(BASE + "/webapp", timeout=2)
            break
        except Exception:
            time.sleep(1)
    else:
        proc.kill()
        pytest.fail("UI test server did not start")
    yield
    proc.terminate()


def _route_external(route):
    """Weather comes from Open-Meteo in the browser — serve the fixture so the
    test is deterministic and offline-safe."""
    url = route.request.url
    body = FX["weather_ports"] if "latitude=31.2304" in url else FX["weather_city"]
    route.fulfill(status=200, content_type="application/json", body=json.dumps(body))


def _set_theme(theme):
    req = urllib.request.Request(BASE + "/api/webapp/user/app-prefs", data=json.dumps({"user_id": 777, "theme": theme}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req)


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_layout(server, theme):
    _set_theme(theme)
    problems = []
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=os.getenv("PLAYWRIGHT_CHROMIUM_CHANNEL") or None)
        for vw, vh in VIEWPORTS:
            ctx = browser.new_context(viewport={"width": vw, "height": vh}, device_scale_factor=2, is_mobile=True, has_touch=True)
            ctx.add_init_script("localStorage.setItem('trk_uid','777')")
            ctx.route("**/api.open-meteo.com/**", _route_external)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            for n, (view, run) in enumerate(STATES):
                page.goto(f"{BASE}/webapp?n={n}#{view}")
                page.wait_for_function("document.getElementById('app').classList.contains('on')", timeout=20000)
                page.wait_for_timeout(1500)
                if run:
                    page.evaluate(f"() => {{ {run} }}")
                    page.wait_for_timeout(1200)
                for issue in page.evaluate(CHECK_JS):
                    problems.append(f"{theme} {vw}px {view} {run[:30]}: {issue}")
            problems += [f"{theme} {vw}px JS error: {e}" for e in errors]
            ctx.close()
        browser.close()
    assert not problems, "\n".join(problems[:40])
