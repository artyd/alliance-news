-- MacroHarvey — canonical database schema (reference).
--
-- The application self-migrates on startup via init_db() in main.py using
-- idempotent CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS statements,
-- so this file is NOT executed automatically. It is the single readable source
-- of truth for the current schema and can bootstrap a fresh database manually:
--     psql "$DATABASE_URL" -f migrations/schema.sql
--
-- If schema changes ever get complex (renames, data backfills, destructive
-- changes), migrate to Alembic — see migrations/README.md.

CREATE TABLE IF NOT EXISTS articles (
    id                      SERIAL PRIMARY KEY,
    title                   TEXT NOT NULL,
    link                    TEXT UNIQUE NOT NULL,
    published               TEXT,
    category                TEXT NOT NULL,
    image_url               TEXT,
    summary_en              TEXT,
    summary_ua              TEXT,
    summary_ru              TEXT,
    full_text               TEXT,
    extraction_status       TEXT DEFAULT 'pending',   -- pending|ok|failed|paywalled|skipped
    extraction_attempted_at TIMESTAMP,
    final_url               TEXT,
    title_ua                TEXT,
    title_ru                TEXT,
    facts_status            TEXT DEFAULT 'pending',    -- pending|ok|failed|skipped
    facts_attempted_at      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_articles_extraction_status  ON articles(extraction_status);
CREATE INDEX IF NOT EXISTS idx_articles_category_published ON articles(category, published DESC);
CREATE INDEX IF NOT EXISTS idx_articles_facts_status       ON articles(facts_status);
CREATE INDEX IF NOT EXISTS idx_articles_title              ON articles(title);

CREATE TABLE IF NOT EXISTS telegram_users (
    chat_id         BIGINT PRIMARY KEY,
    language        TEXT DEFAULT 'en',
    subscriptions   TEXT DEFAULT 'all',
    only_daily_mode BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS telegram_sent (
    id           SERIAL PRIMARY KEY,
    chat_id      BIGINT NOT NULL,
    article_link TEXT NOT NULL,
    sent_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(chat_id, article_link)
);
CREATE INDEX IF NOT EXISTS idx_sent_link ON telegram_sent(article_link);

CREATE TABLE IF NOT EXISTS article_facts (
    id                  SERIAL PRIMARY KEY,
    article_id          INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    event_type          TEXT,
    what_happened       TEXT NOT NULL,
    who                 TEXT,
    where_loc           TEXT,
    magnitude           TEXT,
    affected_sectors    TEXT,
    supply_chain_impact TEXT,
    ukraine_relevance   TEXT,
    confidence          TEXT,
    source_url          TEXT,
    source_publisher    TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_facts_article_id ON article_facts(article_id);
CREATE INDEX IF NOT EXISTS idx_facts_sectors    ON article_facts(affected_sectors);
CREATE INDEX IF NOT EXISTS idx_facts_created     ON article_facts(created_at DESC);

CREATE TABLE IF NOT EXISTS digest_reports (
    id          SERIAL PRIMARY KEY,
    report_type TEXT NOT NULL,
    title       TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    pdf_data    BYTEA
);
CREATE INDEX IF NOT EXISTS idx_digest_reports_created ON digest_reports(created_at DESC);

CREATE TABLE IF NOT EXISTS tracked_shipments (
    id           SERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    number       VARCHAR(60) NOT NULL,
    carrier      VARCHAR(50) NOT NULL DEFAULT 'auto',
    type         VARCHAR(20) NOT NULL DEFAULT 'parcel',
    carrier_name VARCHAR(150) DEFAULT '',
    status_text  TEXT DEFAULT '',
    tracking_url TEXT DEFAULT '',
    steps_json   TEXT DEFAULT '',
    is_delivered BOOLEAN DEFAULT FALSE,
    added_at     TIMESTAMPTZ DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    last_checked TIMESTAMPTZ,
    UNIQUE(user_id, number)
);
CREATE INDEX IF NOT EXISTS idx_tsv_user ON tracked_shipments(user_id, is_delivered);

CREATE TABLE IF NOT EXISTS user_market_prefs (
    user_id    BIGINT PRIMARY KEY,
    keys_csv   TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_currency_prefs (
    user_id    BIGINT PRIMARY KEY,
    codes_csv  TEXT NOT NULL DEFAULT '',
    view_mode  TEXT NOT NULL DEFAULT 'compact',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_widget_prefs (
    user_id    BIGINT PRIMARY KEY,
    keys_csv   TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
