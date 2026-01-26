# Power BI X GpsGate - Project Structure

## Overview
This project is a Flask-based data pipeline that fetches fleet tracking data from GpsGate API, processes it, and syncs to PostgreSQL databases.

---

## 🔑 CORE APPLICATION FILES (Production)

### Main Application
- **main.py** - Flask development server entry point
- **wsgi.py** - WSGI production application wrapper (for Render deployment)
- **application.py** - Flask app factory and blueprint registration
- **config.py** - Configuration settings (database URLs, environment variables)

### Database & Models
- **models.py** - SQLAlchemy ORM models (Render, Result, ConsolidatedRequest, Fact Tables)
- **db_storage_live_fast.py** - Fast batch insert for fact tables (10-100x faster than row-by-row)

### Data Pipeline
- **data_pipeline.py** - Main blueprint for /trip-data, /speeding-data, /idle-data, /awh-data, /wh-data, /ha-data, /hb-data, /wu-data endpoints
  - Handles GpsGate API fetching, CSV processing, and database storage
  - Implements CSV cleaning with multi-encoding support
  - Batch inserting to optimize database performance

### GpsGate Report Rendering
- **render.py** - `/render` endpoint to create report renderings in GpsGate
  - Handles parameter merging for different report types
  - Implements idempotency via database caching
  
- **result.py** - `/result` endpoint to fetch completed renderings
  - Polls GpsGate for render status
  - Downloads CSV results and stores in Result cache

### Backfill & Scheduling
- **backfill_2025_week1.py** - Core backfill script for weekly data fetching
  - Processes all 8 event types (Trip, Speeding, Idle, AWH, WH, HA, HB, WU)
  - Uses the Flask endpoints for batch processing
  - Accepts --week_start and --week_end arguments
  
- **run_backfill_all_weeks.py** - Orchestrator to run backfill_2025_week1.py week-by-week
  - Prevents partial data corruption by stopping on failure
  - Iterates from START_DATE to TODAY
  
- **backfill_scheduler.py** - Scheduled backfill API blueprint
  - Provides scheduled/automated data fetching endpoints
  - Registered in application.py as backfill_api

### Utilities
- **api.py** - Manual API endpoints for triggering backfill and data fetch
- **gpsgate_api.py** - Generic GpsGate proxy API (blueprint)
- **dashboard.py** - Web dashboard blueprint (if enabled)
- **gdrive.py** - Google Drive integration for uploading results
- **logger_config.py** - Centralized logging configuration
- **fact_table_records.py** - Utility to count records in fact tables
- **clear_fact_tables_only.py** - Safe utility to clear fact tables
- **db_storage.py** - Alternative storage function (slower, kept for compatibility)

### Data Synchronization
- **sync_local_to_live.py** - Syncs local database to Render production database
  - Batch transfers all fact and dimension tables
  - Safe sequential processing with progress tracking
- **sync_dimensions_from_api.py** - Syncs dimension tables from GpsGate API
  - Fetches and updates dimension data (drivers, vehicles, tags, reports, events)
  - Keeps dimension tables in sync with GpsGate source

---

## 📋 CONFIGURATION FILES

- **.env** - Environment variables (database URLs, API tokens, credentials)
  - **DO NOT COMMIT**: Contains sensitive credentials
  - Already in .gitignore
  
- **requirements.txt** - Python dependencies
- **.gitignore** - Git ignore rules
  - Excludes: .env, test files, debug files, garbage folder, .md files (except RENDER_*.md)

---

## 📦 DEPLOYMENT FILES (Keep for Render)

- **render.yaml** - Render deployment configuration
- **Procfile** - Process file for Render (defines how to start the app)
- **RENDER_DEPLOYMENT.md** - Render deployment instructions
- **RENDER_ENV_SETUP.md** - Render environment setup guide

---

## 🗑️ GARBAGE FOLDER

**Location:** `/garbage`

Contains 80+ unused/deprecated files:
- Old backfill variants (backfill_*.py, backfill_*.log)
- Test files (test_*.py)
- Debug scripts (debug_*.py, diagnose.py)
- Utility scripts (check_*.py, clear_*.py, cleanup_*.py, migrate_*.py, etc.)
- Legacy documentation (*.md files except RENDER_*.md)

**These files are ignored in .gitignore**

---

## 🔄 DATA FLOW

```
GpsGate API
    ↓
(Flask Routes in data_pipeline.py)
    ↓
/trip-data, /speeding-data, /idle-data, /awh-data, /wh-data, /ha-data, /hb-data, /wu-data
    ↓
(process_event_data function)
    ↓
CSV Download & Clean (clean_csv_data)
    ↓
db_storage_live_fast.py (Batch Insert to PostgreSQL)
    ↓
FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU (Tables)
```

---

## 🏃 Running the Application

### Development Mode
```bash
python main.py
```
Runs on `http://127.0.0.1:5000`

### Backfill Data (One Week)
```bash
python backfill_2025_week1.py --week_start 2025-01-01 --week_end 2025-01-08
```

### Backfill Multiple Weeks
```bash
python run_backfill_all_weeks.py
```

### Sync Local to Render (Live Database)
```bash
python sync_local_to_live.py
```

---

## 🔐 Environment Variables (.env)

### Database Configuration
- `DATABASE_URL` - Local PostgreSQL connection (development)
- `DATABASE_LIVE_URL` - Render PostgreSQL connection (production)

### GpsGate API
- `TOKEN` - GpsGate API authentication token

### Google Drive
- `GDRIVE_FOLDER_ID` - Folder ID for uploading CSV results
- `GOOGLE_APPLICATION_CREDENTIALS` - Service account JSON file path

### Flask
- `FLASK_ENV` - Environment (development/production)
- `SECRET_KEY` - Flask secret key for sessions
- `BACKEND_HOST` - Backend service URL (default: http://127.0.0.1:5000)

### Logging
- `LOG_LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR)
- `DEBUG` - Debug mode flag (false in production)

---

## 📊 Database Tables

### Fact Tables (Events)
- `fact_trip` - Trip events
- `fact_speeding` - Speeding events
- `fact_idle` - Idle events
- `fact_awh` - AWH (Automated Work Hours) events
- `fact_wh` - WH (Work Hours) events
- `fact_ha` - HA events
- `fact_hb` - HB events
- `fact_wu` - WU events

### Cache Tables
- `render` - Stores created report rendering IDs (idempotency)
- `result` - Stores downloaded CSV results and GDrive links
- `consolidated_requests` - Caches cleaned CSV data

### Dimension Tables
- `dim_drivers` - Driver information
- `dim_tags` - Vehicle tags/groups
- `dim_vehicles` - Vehicle information
- `dim_reports` - Report templates
- `dim_event_rules` - Event rules
- `dim_vehicle_custom_fields` - Vehicle custom field definitions

---

## ✅ Recent Changes (Cleanup)

1. ✅ Moved 80+ unused files to `/garbage` folder
2. ✅ Updated `.gitignore` to exclude:
   - garbage/ folder
   - All .md files except RENDER_DEPLOYMENT.md and RENDER_ENV_SETUP.md
   - *.json (credentials)
   - Test and debug files
3. ✅ Reorganized `.env` with clear sections and all necessary keys
4. ✅ Kept all Render deployment files (render.yaml, Procfile, RENDER_*.md)

---

## 📝 Notes

- **DO NOT commit .env** - It contains sensitive credentials
- **Garbage folder is ignored** - Safe to keep for reference/rollback
- **All imports are from core files** - No dependencies on files in garbage/
- **Production:** Uses Render's PostgreSQL (DATABASE_LIVE_URL)
- **Development:** Uses local PostgreSQL (DATABASE_URL)

---

**Last Updated:** January 25, 2026
