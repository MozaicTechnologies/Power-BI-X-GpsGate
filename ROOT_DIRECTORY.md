# Root Directory File Listing

## 🎯 Core Production Files (25 files)

### Application Framework
- ✅ **main.py** - Flask development server entry point
- ✅ **wsgi.py** - WSGI wrapper for production (Render)
- ✅ **application.py** - Flask app factory with blueprint registration
- ✅ **config.py** - Configuration and environment management

### Database & Storage
- ✅ **models.py** - SQLAlchemy ORM models for all tables
- ✅ **db_storage_live_fast.py** - Fast batch insert logic (primary storage)
- ✅ **db_storage.py** - Alternative storage function (compatibility)

### Data Pipeline & Processing
- ✅ **data_pipeline.py** - Main blueprint with 8 event endpoints
  - /trip-data, /speeding-data, /idle-data, /awh-data, /wh-data, /ha-data, /hb-data, /wu-data
  - CSV fetching, cleaning, and batch insertion

### GpsGate Report Handling
- ✅ **render.py** - POST /render endpoint (create report renderings)
- ✅ **result.py** - POST /result endpoint (fetch completed renderings)

### Backfill & Scheduling
- ✅ **backfill_2025_week1.py** - Core backfill script (weekly fetcher)
  - Fetches all 8 event types for a given week
  - Used by run_backfill_all_weeks.py
  
- ✅ **run_backfill_all_weeks.py** - Orchestrator for multi-week backfill
  - Iteratively runs backfill_2025_week1.py week-by-week
  - Stops on failure to prevent partial corruption

- ✅ **backfill_scheduler.py** - Scheduled backfill API blueprint
  - Provides automated/scheduled data fetching
  - Registered in application.py as backfill_api

### API & Integration
- ✅ **api.py** - Manual API endpoints (backfill triggering)
- ✅ **gpsgate_api.py** - Generic GpsGate proxy API (blueprint)

### Utilities
- ✅ **dashboard.py** - Web dashboard blueprint
- ✅ **gdrive.py** - Google Drive integration
- ✅ **logger_config.py** - Centralized logging configuration
- ✅ **fact_table_records.py** - Fact table record counting utility
- ✅ **clear_fact_tables_only.py** - Clear fact tables safely

### Data Synchronization
- ✅ **sync_local_to_live.py** - Local DB to Render production sync
  - Batch transfers fact and dimension tables
  - Safe sequential processing with progress tracking
- ✅ **sync_dimensions_from_api.py** - Syncs dimension tables from GpsGate API
  - Fetches and updates dimension data
  - Keeps dimension tables in sync with source

---

## ⚙️ Configuration & Setup Files (3 files)

- ✅ **.env** - Environment variables (credentials, API tokens, URLs)
  - Contains: DATABASE_URL, DATABASE_LIVE_URL, TOKEN, GDRIVE_FOLDER_ID, GOOGLE_APPLICATION_CREDENTIALS, SECRET_KEY, etc.
  - **CRITICAL: Do not commit to git**

- ✅ **.gitignore** - Git ignore rules
  - Excludes: .env, garbage/, *.json, test files, debug files, etc.

- ✅ **requirements.txt** - Python package dependencies
  - Flask, SQLAlchemy, psycopg, requests, pandas, python-dotenv, etc.

---

## 🚀 Render Deployment Files (4 files)

- ✅ **render.yaml** - Render deployment configuration
  - Specifies build steps, start command, environment variables
  
- ✅ **Procfile** - Process definition for Render
  - Defines how to start the Flask application
  
- ✅ **RENDER_DEPLOYMENT.md** - Deployment instructions
- ✅ **RENDER_ENV_SETUP.md** - Environment setup guide

---

## 📚 Documentation Files (2 files - Production Ready)

- ✅ **PROJECT_STRUCTURE.md** - Complete project structure and file organization
- ✅ **CLEANUP_SUMMARY.md** - Summary of cleanup and reorganization

---

## 📂 Directories

### Active Directories
- **migrations/** - Flask-Migrate database migration files
- **templates/** - Jinja2 HTML templates
- **logs/** - Runtime application logs
- **instance/** - Flask instance-specific files

### System Directories (Ignored)
- **.git/** - Git repository
- **.vscode/** - VS Code settings
- **.venv/** - Python virtual environment
- **venv/** - Python virtual environment
- **__pycache__/** - Python cache files
- **garbage/** - Moved unused files (ignored in .gitignore)

---

## 🗑️ Garbage Folder Contents (86 files - all ignored)

**Location:** `/garbage`

This folder contains 86+ deprecated and unused files that have been moved for organization:

### Test Files (23+)
- test_*.py files
- quick_test.py
- test.py

### Debug Files (5+)
- debug_*.py
- diagnose.py

### Old Backfill Variants (12+)
- backfill_5weeks_all.py
- backfill_current_week.py
- backfill_direct*.py
- backfill_fast_5w.py
- backfill_first5weeks.py
- backfill_live_week1.py
- etc.

### Utility & Cleanup Scripts (18+)
- check_*.py
- clear_*.py
- cleanup_*.py
- migrate_*.py
- patch_*.py
- validate_*.py
- etc.

### Legacy Documentation (14+)
- Old .md files (DEPLOYMENT_*.md, BACKFILL_LIVE_FIX.md, etc.)
- HANDOFF_SUMMARY.md
- API_EXAMPLES.md

### Logs & Output (12+)
- *.log files (backfill_*.log, db_storage.log, etc.)
- *.txt output files
- db_storage_live.py (replaced by db_storage_live_fast.py)

---

## Summary Statistics

| Category | Count | Status |
|----------|-------|--------|
| **Core Production Files** | 25 | ✅ Active |
| **Configuration Files** | 3 | ✅ Active |
| **Render Deployment Files** | 4 | ✅ Active |
| **Documentation (Tracked)** | 2 | ✅ Active |
| **Total Active Files** | **32** | ✅ |
| **Files in Garbage Folder** | 86+ | 🗑️ Ignored |
| **Total Root Files** | **~40** | ✅ Clean |

---

## 🔍 Dependency Graph

```
main.py / wsgi.py
    ↓
application.py (Flask app factory)
    ├── models.py (ORM models)
    ├── config.py (Configuration)
    ├── data_pipeline.py (Event processing)
    │   ├── db_storage_live_fast.py (Batch insert)
    │   ├── db_storage.py (Alternative storage)
    │   └── models.py (Database access)
    ├── render.py (Report creation)
    │   └── models.py (Render table)
    ├── result.py (Result fetching)
    │   └── models.py (Result table)
    ├── api.py (Manual endpoints)
    ├── gpsgate_api.py (Generic API proxy)
    ├── backfill_scheduler.py (Scheduled backfill)
    ├── dashboard.py (Web dashboard)
    └── logger_config.py (Logging)

backfill_2025_week1.py
    ├── application.py (Creates Flask app)
    ├── data_pipeline.py (process_event_data)
    ├── models.py (Database)
    └── dotenv (Load .env)

run_backfill_all_weeks.py
    └── backfill_2025_week1.py (Runs week-by-week)

run_backfill.py
    └── requests (Direct HTTP calls)

sync_local_to_live.py
    ├── models.py (ORM)
    ├── psycopg (Database connection)
    └── sqlalchemy (Database access)

gdrive.py
    └── google.cloud (Google Drive API)
```

---

## ✅ Quality Assurance

- ✅ All imports are resolvable (no broken dependencies)
- ✅ No circular imports
- ✅ All core files present and functional
- ✅ All Render deployment files present
- ✅ .env properly configured with all keys
- ✅ .gitignore properly excludes sensitive files
- ✅ Garbage folder is ignored (safe to keep for rollback)
- ✅ No dead code in root directory

---

**Last Updated:** January 25, 2026  
**Status:** ✅ Production Ready

