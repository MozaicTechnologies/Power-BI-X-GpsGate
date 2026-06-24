# Power-BI-X-GpsGate Backend Documentation

## 1. Project Overview

This backend is a Flask-based service designed to collect, process, store, and expose GpsGate data for Power BI reporting.
The system uses PostgreSQL as the main database, Redis as the Celery broker/result backend, Celery for background jobs, and Nginx/Certbot for production web access and SSL handling.

The main purpose of the backend is:

* Store GpsGate application configuration.
* Fetch and process GpsGate reports/events.
* Save normalized data into fact tables.
* Run daily and weekly background sync jobs.
* Provide dashboard, API, result, render, pipeline, and backfill endpoints.
* Support multi-application / multi-customer GpsGate data separation.

---

## 2. High-Level Architecture

The project is organized like this:

```text
backend/
│
├── app/
│   ├── models/
│   ├── routes/
│   ├── services/
│   ├── static/
│   ├── tasks/
│   ├── templates/
│   ├── utils/
│   ├── __init__.py
│   ├── celery_app.py
│   └── config.py
│
├── nginx/
├── output/
├── scripts/
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── celery_worker.py
├── docker-compose.yml
├── entrypoint.sh
├── init-letsencrypt.sh
├── main.py
├── requirements.txt
└── wsgi.py
```

---

## 3. Root-Level Files

### `Dockerfile`

Builds the Python backend Docker image.

Main responsibilities:

* Uses `python:3.11-slim`.
* Sets `/app` as the working directory.
* Installs system dependencies like `gcc` and `libpq-dev`.
* Installs Python dependencies from `requirements.txt`.
* Copies project files into the image.
* Makes `entrypoint.sh` executable.
* Starts the app using Gunicorn.

Default command:

```bash
gunicorn wsgi:app --bind 0.0.0.0:5000 --workers 2 --timeout 120
```

---

### `docker-compose.yml`

Defines the full development/production stack.

Main services:

* `redis`: message broker and Celery result backend.
* `db`: PostgreSQL database.
* `nginx`: reverse proxy for HTTP/HTTPS.
* `certbot`: automatic SSL certificate renewal.
* `web`: Flask/Gunicorn web application.
* `worker`: Celery worker for background jobs.
* `beat`: Celery Beat scheduler for periodic tasks.
* `flower`: Celery monitoring dashboard.

Important volumes:

* `redis_data`
* `pg_data`
* `beat_data`

Important note: database credentials should not be hardcoded in this file. They should be moved to `.env`.

---

### `.env.example`

Example environment configuration file.

Expected purpose:

* Shows which environment variables are required.
* Usually includes variables such as database URL, Redis URL, GpsGate token, base URL, secret key, and Flower credentials.

---

### `.dockerignore`

Controls which files should not be copied into the Docker image.

Typical purpose:

* Exclude unnecessary local files.
* Reduce Docker image size.
* Prevent copying secrets, cache files, virtual environments, and logs.

---

### `.gitignore`

Controls which files Git should ignore.

Typical purpose:

* Ignore `.env`.
* Ignore Python cache files.
* Ignore local database files.
* Ignore logs and temporary output files.

---

### `requirements.txt`

Python dependency list.

Expected dependencies include Flask, SQLAlchemy, Celery, Redis, PostgreSQL driver, Flask-Migrate, Flask-Login, and other libraries used by the backend.

---

### `entrypoint.sh`

Container startup script.

Expected responsibilities:

* Run initialization commands before starting the web server.
* Possibly wait for database availability.
* Run migrations or setup commands.
* Then execute the final container command.

---

### `init-letsencrypt.sh`

SSL initialization helper.

Expected responsibilities:

* Prepare Certbot folders.
* Request initial Let’s Encrypt certificate.
* Configure Nginx/Certbot certificate files.

---

### `main.py`

Local application entry file.

Expected purpose:

* Creates or imports the Flask application.
* Runs the app directly in development mode.

Usually used for local testing, not production.

---

### `wsgi.py`

Production WSGI entry point.

Purpose:

* Exposes the Flask app object for Gunicorn.
* Gunicorn uses this file with `wsgi:app`.

---

### `celery_worker.py`

Celery entry point.

Purpose:

* Loads the Flask application.
* Imports Celery tasks.
* Exposes Celery app for the worker, beat, and Flower commands.

Used by:

```bash
celery -A celery_worker worker
celery -A celery_worker beat
celery -A celery_worker flower
```

---

## 4. `app/` Package

### `app/__init__.py`

Main Flask application factory.

Main responsibilities:

* Loads environment variables.
* Creates Flask app.
* Loads configuration from `Config`.
* Initializes SQLAlchemy.
* Initializes Flask-Login.
* Initializes rate limiter.
* Configures Celery with Flask context.
* Enables Flask-Migrate.
* Registers all route blueprints.

Registered blueprints:

* `auth_bp`
* `render_bp`
* `result_bp`
* `backfill_api`
* `api_bp`
* `dashboard_bp`
* `pipeline_bp`

This file is the central place where the whole backend application is assembled.

---

### `app/config.py`

Central configuration file.

Main responsibilities:

* Reads environment variables.
* Configures Flask secret key.
* Configures database connection.
* Normalizes PostgreSQL connection URLs.
* Sets SQLAlchemy pool options.
* Configures Celery broker/result backend.
* Defines Celery Beat scheduled jobs.
* Reads GpsGate token and base URL.

Important scheduled jobs:

* `daily-sync`: runs daily at 02:00.
* `weekly-backfill`: runs weekly on Monday at 03:00.

Important config values:

* `DATABASE_URL`
* `CELERY_BROKER_URL`
* `CELERY_RESULT_BACKEND`
* `TOKEN`
* `BASE_URL`

---

### `app/celery_app.py`

Celery configuration module.

Main responsibilities:

* Creates the Celery instance.
* Binds Celery to the Flask app context.
* Makes Celery tasks run inside Flask application context.
* Loads Celery configuration from Flask config.

This is important because background tasks often need database access and Flask config values.

---

## 5. `app/models/`

### `app/models/__init__.py`

Package initializer for models.

Expected purpose:

* Exposes `db` and model classes from `models.py`.
* Allows imports like:

```python
from app.models import db
```

---

### `app/models/models.py`

Database schema definition file.

This is one of the most important files in the backend. It defines SQLAlchemy models and database tables.

Main model groups:

---

#### `GpsGateApplication`

Table:

```text
gpsgate_application
```

Purpose:

Stores each GpsGate application/customer configuration.

Important fields:

* `application_id`
* `token`
* `tag_name`
* `trip_report_name`
* `event_report_name`
* Event rule names
* Tag/report/event IDs
* `created_at`

This model is the central configuration table for multi-application support.

---

#### `Render`

Table:

```text
render
```

Purpose:

Stores report render requests from GpsGate.

Important fields:

* `app_id`
* `gpsgate_application_id`
* `period_start`
* `period_end`
* `tag_id`
* `event_id`
* `report_id`
* `render_id`
* `created_at`

This table tracks render jobs and links them to a specific GpsGate application.

---

#### `Result`

Table:

```text
result
```

Purpose:

Stores generated report result metadata.

Important fields:

* `app_id`
* `gpsgate_application_id`
* `report_id`
* `render_id`
* `filepath`
* `gdrive_file_id`
* `gdrive_link`
* `uploaded_at`
* `created_at`

This table tracks completed reports and their storage location.

---

#### Fact Tables

These tables store normalized report/event data for Power BI.

Main fact tables:

* `FactTrip`
* `FactSpeeding`
* `FactIdle`
* `FactAWH`
* `FactWH`
* `FactHA`
* `FactHB`
* `FactWU`
* Other event-related fact models depending on the rest of the file.

Purpose:

* Store trip data.
* Store speeding events.
* Store idle events.
* Store working hour / after working hour events.
* Store harsh acceleration/braking/cornering-like driver behavior events.
* Prevent duplicates using unique constraints.
* Separate data per GpsGate application using `gpsgate_application_id`.

Most fact tables include:

* `app_id`
* `gpsgate_application_id`
* `tag_id`
* `event_date`
* `start_time`
* `vehicle`
* `driver`
* `location`
* `address`
* `duration`
* `duration_s`
* `is_duplicate`
* `created_at`

---

## 6. `app/routes/`

This folder contains Flask route/blueprint files. These files handle HTTP requests and connect the frontend/API layer to services and database logic.

### `app/routes/__init__.py`

Package initializer for route modules.

Expected purpose:

* Makes the `routes` directory a Python package.
* May be empty.

---

### `app/routes/auth.py`

Authentication routes.

Expected responsibilities:

* Login page handling.
* Logout handling.
* User session management.
* Flask-Login configuration.
* Rate limiting setup.

This file protects dashboard/admin pages from unauthorized access.

---

### `app/routes/api.py`

General API endpoints.

Expected responsibilities:

* Expose backend data through JSON APIs.
* Provide endpoints for dashboard or frontend actions.
* Trigger sync or configuration operations.
* Return GpsGate application data, status, or processed results.

This is likely the main API interface for internal frontend/backend communication.

---

### `app/routes/backfill.py`

Backfill API routes.

Expected responsibilities:

* Trigger historical data backfill.
* Start backfill Celery tasks.
* Check backfill progress/status.
* Allow manual reprocessing of older GpsGate data.

This route is useful when historical data is missing or when a new customer/application is added.

---

### `app/routes/dashboard.py`

Dashboard page routes.

Expected responsibilities:

* Render the main dashboard page.
* Provide dashboard data to templates.
* Show overview/status of sync jobs, applications, reports, and backfills.

---

### `app/routes/pipeline.py`

Pipeline routes.

Expected responsibilities:

* Trigger or manage the data pipeline.
* Connect render/result/backfill/sync actions into a workflow.
* Provide endpoints for pipeline status.

This file likely represents the operational control layer of the ETL pipeline.

---

### `app/routes/render.py`

Render routes.

Expected responsibilities:

* Create GpsGate report render requests.
* Store render metadata in the `Render` table.
* Start or track report generation.

---

### `app/routes/result.py`

Result routes.

Expected responsibilities:

* Fetch report results.
* Store or return processed output.
* Link report results to render IDs.
* Possibly handle file links or Google Drive result links.

---

## 7. `app/services/`

This folder contains business logic. Routes should stay thin, while services do the real processing.

### `app/services/__init__.py`

Package initializer for services.

Expected purpose:

* Makes services importable as a package.

---

### `app/services/customer_config.py`

GpsGate application/customer configuration logic.

Expected responsibilities:

* Read customer/application config.
* Create or update GpsGate application records.
* Resolve token, tag, report, and event rule IDs.
* Help support multi-application setup.

Even though the model is now called `GpsGateApplication`, this file name suggests it originally handled `CustomerConfig` logic.

---

### `app/services/db_storage.py`

Database storage service.

Expected responsibilities:

* Insert parsed GpsGate data into database tables.
* Convert API/report rows into SQLAlchemy model objects.
* Handle duplicate detection.
* Store trip, speeding, idle, and event records.

This file is part of the ETL layer.

---

### `app/services/db_storage_live_fast.py`

Optimized live database storage service.

Expected responsibilities:

* Faster insert/update logic for live or frequent data.
* Possibly uses bulk insert/upsert patterns.
* Optimized for performance compared to `db_storage.py`.

Use this for high-volume or live sync paths.

---

### `app/services/event_processor.py`

Event processing service.

Expected responsibilities:

* Parse raw GpsGate event data.
* Normalize event fields.
* Map event types to the correct fact tables.
* Extract vehicle, driver, time, location, duration, severity, and event metadata.

This is likely the main transformation layer for event reports.

---

### `app/services/sync_dimensions.py`

Dimension sync service.

Expected responsibilities:

* Sync dimension tables from GpsGate.
* Keep lookup/reference data updated.
* Sync vehicles, drivers, tags, applications, or other dimension-like data.

This supports clean Power BI models by separating dimensions from fact tables.

---

## 8. `app/tasks/`

This folder contains Celery background jobs.

### `app/tasks/__init__.py`

Task package initializer.

Expected purpose:

* Imports/registers Celery tasks.
* Ensures Celery worker can discover task functions.

---

### Other task files

Based on the project configuration, the backend has scheduled tasks named:

* `tasks.daily_sync`
* `tasks.weekly_backfill`

Their purpose:

* `daily_sync`: fetch and process recent GpsGate data every day.
* `weekly_backfill`: reprocess or fill missing historical data weekly.

---

## 9. `app/templates/`

This folder contains HTML pages rendered by Flask.

### `login.html`

Login page.

Purpose:

* Shows the admin/user login form.
* Sends credentials to authentication routes.

---

### `dashboard.html`

Main dashboard page.

Purpose:

* Shows backend status, sync status, application data, reports, or controls.
* Used by `dashboard.py`.

---

### `backfill_dashboard.html`

Backfill control page.

Purpose:

* Allows user/admin to run historical backfill.
* Shows backfill status or progress.

---

## 10. `app/static/`

Static frontend assets.

### `app/static/css/`

CSS files.

Purpose:

* Styling for login page, dashboard page, and backfill dashboard.

---

### `app/static/js/`

JavaScript files.

Purpose:

* Frontend dashboard behavior.
* API calls to backend routes.
* Backfill/pipeline status polling.
* Button actions and dynamic page updates.

---

## 11. `app/utils/`

Utility/helper code.

### `app/utils/__init__.py`

Package initializer.

---

### `app/utils/logger.py`

Logging helper.

Expected responsibilities:

* Configure application logging.
* Provide reusable logger setup.
* Standardize log format for web, worker, and background jobs.

---

## 12. `nginx/`

Nginx configuration folder.

Expected contents:

* `nginx.conf`

Purpose:

* Reverse proxy traffic to the Flask/Gunicorn app.
* Serve HTTP/HTTPS.
* Handle Certbot challenge path.
* Forward requests to the `web` container.
* Support production domain deployment.

---

## 13. `scripts/`

Utility and maintenance scripts.

### `scripts/backfill_2025_week1.py`

Manual backfill script.

Purpose:

* Run a specific historical backfill for the first week of 2025.
* Useful for one-time recovery or data repair.

---

### `scripts/generate_admin_hash.py`

Admin password hash generator.

Purpose:

* Generate hashed password for admin login.
* Helps avoid storing plain text passwords.

---

### `scripts/migrate_dimension_tables_for_multi_app.py`

Migration helper for multi-application support.

Purpose:

* Update dimension tables to support multiple GpsGate applications.
* Likely adds `gpsgate_application_id` or similar foreign keys.
* Helps migrate older single-application data model to a multi-application model.

---

## 14. Data Flow

Typical data flow:

```text
GpsGate API / Reports
        ↓
Route or Celery Task
        ↓
Service Layer
        ↓
Event Processor / Storage Service
        ↓
PostgreSQL Fact + Dimension Tables
        ↓
Power BI
```

For scheduled jobs:

```text
Celery Beat
    ↓
Celery Worker
    ↓
Daily Sync / Weekly Backfill Task
    ↓
GpsGate API
    ↓
Database Storage
    ↓
Power BI Dataset
```

---

## 15. Main Technologies

* Python 3.11
* Flask
* Flask-SQLAlchemy
* Flask-Migrate
* Flask-Login
* Celery
* Redis
* PostgreSQL
* Gunicorn
* Nginx
* Certbot
* Docker Compose
* GpsGate API
* Power BI

---

## 16. Important Notes for Future Development

### Security

Move sensitive values out of `docker-compose.yml` and into `.env`.

Especially:

* Database password
* Database URL
* GpsGate token
* Flask secret key
* Flower username/password

### Database

The project already supports `gpsgate_application_id`, which is good for multi-customer or multi-application separation.

When adding new tables, always include:

* `gpsgate_application_id`
* `app_id`
* Proper unique constraints
* Indexes on date, vehicle, and application fields

### Celery

Keep long-running sync/backfill logic inside Celery tasks, not Flask routes.

Routes should only:

* Validate request
* Trigger task
* Return task ID/status

### Power BI

Fact tables should stay clean and stable because Power BI depends on their schema.

Avoid frequent column renaming unless Power BI model is updated too.

### Logging

All sync/backfill processes should write clear logs:

* Application ID
* Date range
* Report type
* Number of records inserted
* Number of duplicates skipped
* Errors from GpsGate API

---

## 17. Suggested Documentation Improvement

Add a `README.md` inside `backend/` with:

* How to run locally
* Required `.env` variables
* Docker Compose commands
* Migration commands
* Celery worker/beat commands
* How to add a new GpsGate application
* How to run daily sync manually
* How to run backfill manually
* Power BI table explanation

Suggested command section:

```bash
docker compose up -d --build
docker compose logs -f web
docker compose logs -f worker
docker compose logs -f beat
docker compose exec web flask db upgrade
```

---

## 18. Summary

This backend is an ETL and API system between GpsGate and Power BI.

It has four main responsibilities:

1. Manage GpsGate application/customer configuration.
2. Fetch and process GpsGate report/event data.
3. Store clean fact/dimension data in PostgreSQL.
4. Expose dashboard/API tools and scheduled jobs for monitoring and automation.

The most important files are:

* `app/__init__.py`: creates and wires the Flask app.
* `app/config.py`: controls environment, database, Redis, Celery, and GpsGate settings.
* `app/models/models.py`: defines all database tables.
* `app/routes/*.py`: exposes web/API endpoints.
* `app/services/*.py`: contains business logic and data processing.
* `app/tasks/*.py`: contains Celery background jobs.
* `docker-compose.yml`: runs the full stack.
* `Dockerfile`: builds the backend image.
