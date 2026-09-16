# Migrations

The schema currently self-applies on startup via `init_db()` in `main.py`
(idempotent `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`).
`schema.sql` here is the human-readable canonical reference and a manual
bootstrap for a fresh database:

```bash
psql "$DATABASE_URL" -f migrations/schema.sql
```

## When to move to Alembic

The current approach is fine for additive changes (new tables/columns). Adopt
Alembic once you need any of:

- column renames or type changes,
- data backfills tied to a schema change,
- dropping columns/tables safely,
- versioned, reversible history across environments.

Path:
```bash
pip install alembic
alembic init migrations/alembic
# set sqlalchemy.url from DATABASE_URL, then autogenerate against the live schema
alembic revision --autogenerate -m "baseline"
```
Then stop calling the DDL in `init_db()` and let Alembic own the schema.
