# Render Deployment Guide

## Current Status
- Development branch: **Ready for deployment**
- Latest commit: `a137cb8` - Cleanup and consolidation complete
- Database: PostgreSQL on Render.com (Singapore region)
- Status: All 8 endpoints working at 96%+ success rate

## Deployment Steps

### 1. **Merge Development to Main**
```bash
git checkout main
git pull origin main
git merge development --no-ff -m "release: deploy backfill pipeline v1.0"
git push origin main
```

### 2. **Deploy to Render**

Render will automatically deploy when you push to main. If automatic deployment is not configured:

#### Option A: Deploy via Render Dashboard
1. Go to [Render Dashboard](https://dashboard.render.com)
2. Select your service
3. Click "Manual Deploy" → "Deploy latest commit"
4. Wait for build to complete (2-5 minutes)

#### Option B: Connect GitHub (Auto-Deploy)
1. In Render Dashboard, go to your service settings
2. Connect GitHub repository
3. Set branch to `main`
4. Enable "Auto-deploy" on push
5. All future pushes to `main` will deploy automatically

### 3. **Verify Deployment**

Once deployed, verify health:
```bash
# Test the API endpoint
curl https://your-render-service-url/health

# Expected response:
# {"status": "ok", "database": "connected"}
```

## Database Configuration

### Environment Variables (Set in Render)
```
DATABASE_URL=postgresql+psycopg://[user]:[password]@[host]:5432/[db]
DATABASE_LIVE_URL=postgresql+psycopg://[user]:[password]@[host]:5432/[db]
FLASK_ENV=production
```

### Current Live Database
- **Host**: dpg-d39587nfte5s73ci7as0-a.singapore-postgres.render.com
- **Database**: fleetdb
- **Region**: Singapore (optimal for Oman)
- **Tables**: 8 fact tables + Render + Result

## Production Features

### Backfill Pipeline
- **Complete data import**: All 8 event types (Trip, Speeding, Idle, AWH, WH, HA, HB, WU)
- **Multi-level deduplication**: CSV-level + Database-level
- **Error handling**: Per-row failure tracking with IntegrityError handling
- **Row accounting**: Complete visibility into data flow at each stage

### Data Quality Metrics (Latest Run)
```
Total Rows Fetched:        100,575
- Internal CSV Dupes:         -182
= After Dedup:             100,393
- DB-Level Duplicates:        -15
- Failed Inserts:          -3,765
= TOTAL INSERTED:           96,613
Success Rate: 96.1%
```

### Endpoints Available
- `GET /health` - Health check
- `POST /trip-data` - Fetch & store Trip events
- `POST /speeding-data` - Fetch & store Speeding events
- `POST /idle-data` - Fetch & store Idle events
- `POST /awh-data` - Fetch & store AWH events
- `POST /wh-data` - Fetch & store WH events
- `POST /ha-data` - Fetch & store HA events
- `POST /hb-data` - Fetch & store HB events
- `POST /wu-data` - Fetch & store WU events

## Automation Setup (Optional)

### Weekly Backfill via Cron
For automatic weekly data pulls, use Render's cron job feature:

1. In Render Dashboard, go to "Background Workers"
2. Create new background worker with:
   ```
   Command: python fetch_one_week.py trip 0
   Schedule: 0 0 * * 0  (Weekly Sunday midnight UTC)
   ```

### Or Use External Scheduler
```bash
# Example: Call backfill endpoint every Sunday
curl -X POST https://your-render-service-url/backfill \
  -H "Content-Type: application/json" \
  -d '{
    "app_id": "6",
    "token": "YOUR_GPSGATE_TOKEN",
    "base_url": "https://omantracking2.com"
  }'
```

## Troubleshooting

### Database Connection Issues
```bash
# Check live database connectivity
python check_live_schema.py

# Verify environment variables
echo $DATABASE_LIVE_URL
```

### Data Not Inserting
1. Check database schema: `python db_summary.py`
2. Review logs: `db_storage.log`
3. Verify API credentials in environment

### Performance Issues
- Trip events: ~5 min per week
- Other events: ~2-3 min per week
- Total for all 8 endpoints: ~30-40 min per week

## Files Included

### Core Application
- `application.py` - Flask app factory
- `main.py` - Entry point with routes
- `models.py` - Database schema (8 fact tables)
- `wsgi.py` - Production WSGI entry (Render uses this)

### Data Pipeline
- `data_pipeline.py` - Main data processing engine
- `trip_data_pipeline.py` - Trip-specific endpoint
- `db_storage.py` - Database insertion with dedup logic
- `db_storage_live.py` - Live DB schema compatibility

### Utilities
- `clear_fact_tables.py` - Clear all data (dev only)
- `clear_and_backfill.py` - Clean slate backfill
- `migrate_db.py` - Schema migrations
- `migrate_live_to_local.py` - Sync between servers
- `add_is_duplicate_column.py` - Add duplicate tracking

### Backfill Scripts (Local Development)
- `backfill_direct_python.py` - Week 1 only
- `backfill_live_week1.py` - Use live DB
- `run_clean_backfill.ps1` - PowerShell automation
- `fetch_one_week.py` - Fetch specific week/endpoint

## Security Checklist

✅ **Before Production**
- [ ] All secrets in environment variables (not in code)
- [ ] DATABASE_URL and DATABASE_LIVE_URL set in Render
- [ ] GpsGate API token in environment
- [ ] HTTPS enabled (Render default)
- [ ] No test files in production build
- [ ] .gitignore verified for sensitive files
- [ ] Error logging configured

## Rollback Procedure

If issues occur:
```bash
# Revert to previous stable version
git revert a137cb8  # Revert commit
git push origin main

# Or switch to previous commit
git reset --hard 164e71b  # Previous working version
git push -f origin main   # Force push to revert
```

Then re-deploy in Render.

## Next Steps

1. Verify `development` branch is ready in GitHub
2. Merge `development` → `main`
3. Deploy on Render (automatic or manual)
4. Monitor logs: `Logs` tab in Render Dashboard
5. Run backfill on demand or set up automation

---

**Deployment Date**: January 13, 2026
**Version**: 1.0-production
**Status**: Ready for deployment
