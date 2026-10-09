from app.assistant_sql import check_sql


def test_allows_plain_selects_and_ctes():
    assert check_sql("SELECT title FROM articles WHERE category = 'api' ORDER BY published DESC LIMIT 5") is None
    assert check_sql("select count(*) from strike_events e join strike_items i on i.event_id = e.id") is None
    assert check_sql("WITH s AS (SELECT * FROM corp_shipments WHERE in_sheet) SELECT count(*) FROM s;") is None


def test_rejects_writes_and_tricks():
    assert check_sql("DELETE FROM articles") is not None
    assert check_sql("SELECT 1; DROP TABLE articles") is not None
    assert check_sql("UPDATE articles SET title='x'") is not None
    assert check_sql("SELECT * FROM articles -- comment") is not None
    assert check_sql("SELECT pg_sleep(10)") is not None
    assert check_sql("SELECT * INTO x FROM articles") is not None


def test_rejects_personal_and_system_tables():
    for t in ("telegram_users", "user_notes", "user_favorites", "corp_follows", "tracked_shipments",
              "user_app_prefs", "pg_catalog.pg_user", "information_schema.tables"):
        assert check_sql(f"SELECT * FROM {t}") is not None, t
    assert check_sql("SELECT * FROM articles a JOIN user_notes n ON true") is not None
