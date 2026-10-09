"""Run the real FastAPI app for UI tests: no lifespan (no Telegram polling, no
news loops, no pushes), a database seeded from tests/ui/fixtures.json, and
relaxed Telegram auth so a plain browser can call the API.

    DATABASE_URL=postgresql://... python tests/ui/app_server.py [port]
"""
import contextlib
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
os.environ.setdefault("TG_AUTH_REQUIRED", "0")
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ.setdefault("OPENAI_API_KEY", "")
os.chdir(ROOT)
sys.path.insert(0, ROOT)

import main  # noqa: E402
import uvicorn  # noqa: E402

FX = json.load(open(os.path.join(HERE, "fixtures.json"), encoding="utf-8"))
TEST_UID = 777


def seed():
    main.init_db()
    conn = main.get_db_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE articles, strike_events, strike_items, digest_reports, corp_shipments, "
                "telegram_users, digest_issues RESTART IDENTITY CASCADE")
    for a in FX["news"]:
        cur.execute("INSERT INTO articles (id, title, title_ua, link, published, category, summary_en, summary_ua) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (a["id"], a["title"], a.get("title_ua"), a["link"], a["published"], a["category"],
                     a.get("summary_en"), a.get("summary_ua")))
    for e in FX["strikes"]:
        cur.execute("INSERT INTO strike_events (id, sent_at, attack_date, company, object_type, city, region, "
                    "is_pharma, watchlist, headline, summary, card_json, update_count) "
                    "VALUES (%s, NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (e["id"], e.get("attack_date"), e.get("company"), e.get("object_type"), e.get("city"),
                     e.get("region"), e.get("is_pharma"), e.get("watchlist"), e.get("headline"), e.get("summary"),
                     json.dumps(e.get("card") or {}, ensure_ascii=False), e.get("update_count") or 0))
        for s in (e.get("sources") or [])[:3]:
            cur.execute("INSERT INTO strike_items (link, source, title, status, event_id, published) "
                        "VALUES (%s,%s,%s,'matched',%s,NOW()) ON CONFLICT DO NOTHING",
                        (s["link"], s["source"], s.get("title"), e["id"]))
    for r in FX["digest_reports"]:
        cur.execute("INSERT INTO digest_reports (id, report_type, title, created_at) VALUES (%s,%s,%s,%s)",
                    (r["id"], r["report_type"], r["title"], r["created_at"]))
    items = [{"dept": "procurement", "name": "Закупівля", "title": "Тестовий дайджест закупівлі", "teaser": "Опис", "url": "https://telegra.ph/"}]
    cur.execute("INSERT INTO digest_issues (mode, date_str, items_json) VALUES ('daily_brief', '01.01.2099', %s)",
                (json.dumps(items, ensure_ascii=False),))
    for it in FX["corp"]:
        cur.execute("INSERT INTO corp_shipments (key, data_json, number, in_sheet) VALUES (%s,%s,%s,TRUE)",
                    (it["key"], json.dumps(it, ensure_ascii=False), it["number"] or None))
    cur.execute("INSERT INTO telegram_users (chat_id, language, subscriptions) VALUES (%s, 'ua', 'all')", (TEST_UID,))
    conn.commit()
    conn.close()
    main._mk_cache.update(data=FX["markets"], ts=9e18)
    main._curr_cache.update(data=FX["currencies"], ts=9e18)


@contextlib.asynccontextmanager
async def _no_lifespan(app):
    yield


if __name__ == "__main__":
    seed()
    main.app.router.lifespan_context = _no_lifespan
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8791
    print(f"UI test server on http://127.0.0.1:{port}/webapp", flush=True)
    uvicorn.run(main.app, host="127.0.0.1", port=port, log_level="warning")
