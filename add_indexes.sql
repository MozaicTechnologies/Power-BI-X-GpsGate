-- Performance indexes — run once against the existing database
-- Safe to run multiple times (uses IF NOT EXISTS)

-- 1. job_log.started_at — queried + sorted every 5 seconds by the dashboard
CREATE INDEX IF NOT EXISTS ix_job_log_started_at
    ON job_log (started_at DESC);

-- 2. render lookup — 6-column filter used by the pipeline on every sync
CREATE INDEX IF NOT EXISTS ix_render_lookup
    ON render (gpsgate_application_id, period_start, period_end, tag_id, report_id, event_id);
