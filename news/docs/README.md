# MacroHarvey — Documentation Index

> Main navigation for all project documentation.
> Each topic is a folder. See its README.md for the overview.

## Topics

| Folder | Description |
|--------|-------------|
| [INDEX](INDEX/) | Master navigation index |
| [01_intro](01_intro/) | Project introduction and overview |
| [02_database](02_database/) | Database schema and migrations |
| [03_categories_pipeline](03_categories_pipeline/) | News categories and processing pipeline |
| [04_scheduler](04_scheduler/) | APScheduler jobs and timing |
| [05_pdf_reports](05_pdf_reports/) | PDF report generation |
| [06_market_telegram](06_market_telegram/) | Market data and Telegram delivery |
| [07_api_frontend_deps](07_api_frontend_deps/) | API routes and frontend dependencies |
| [08_startup_architecture](08_startup_architecture/) | App startup and architecture overview |
| [09_changelog_bugs](09_changelog_bugs/) | Full changelog and known bugs history |
| [10_mini_app_features](10_mini_app_features/) | Telegram Mini App features |
| [11_design_freeze](11_design_freeze/) | Design freeze rules — CSS reference |
| [SESSION_LOG](SESSION_LOG/) | Per-session work log |
| [BUG_LOG](BUG_LOG/) | Bug registry with root causes and fixes |

## Subfolder conventions

Inside every topic folder:

| Subfolder | Purpose |
|-----------|---------|
| `sessions/` | Per-session work notes (session-XXX.md) |
| `bugs/` | Bug reports (bug-XXX-short-title.md) |
| `changes/` | Feature/change records (change-XXX-short-title.md) |
| `notes/` | General notes and context (note-XXX-short-title.md) |

## Rules for future updates

- Fix a bug → add `bugs/bug-XXX.md` in the relevant topic folder.
- Add a feature → add `changes/change-XXX.md`.
- Session work log → add `sessions/session-XXX.md` to SESSION_LOG/.
- Update `README.md` in a topic folder only if the main overview changed.
- Update this top-level `docs/README.md` only when a new topic folder is created.
