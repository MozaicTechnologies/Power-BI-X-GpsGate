# Render Deployment - Environment Variables Setup

## ⚠️ Required Before Deployment

Your Render deployment failed because **DATABASE_URL environment variable** wasn't set.

---

## Steps to Fix (Do This Now)

### 1. Go to Render Dashboard
https://dashboard.render.com

### 2. Select Your Service
- Click on "Power-BI-X-GpsGate"
- Go to "Environment" tab

### 3. Add Environment Variables

| Variable Name | Value | Required |
|---------------|-------|----------|
| `DATABASE_URL` | Your PostgreSQL connection URL | ✅ YES |
| `SECRET_KEY` | Random string (or leave default) | ❌ No |
| `GDRIVE_FOLDER_ID` | Your Google Drive folder ID | ⚠️ For backfill |
| `GOOGLE_APPLICATION_CREDENTIALS` | Google auth JSON (or path) | ⚠️ For backfill |
| `GPSGATE_API_KEY` | Your GpsGate API token | ⚠️ For backfill |

---

## What to Set

### DATABASE_URL (CRITICAL - Required)

**Format for PostgreSQL:**
```
postgresql+psycopg://username:password@host:port/database?sslmode=require
```

**Example:**
```
postgresql+psycopg://user:password@db.example.com:5432/my_database?sslmode=require
```

**Where to get this:**
- If using Render PostgreSQL: Dashboard → Databases → Copy connection string
- If using external database: Get from your database provider
- ⚠️ Must include `?sslmode=require` at the end

### SECRET_KEY (Optional)
- Can be any random string
- Or leave blank to use default
- Only used for Flask sessions

### GDRIVE_FOLDER_ID (Optional - For Backfill)
- Your Google Drive folder ID
- Format: Long alphanumeric string
- Found in Google Drive folder URL

### GOOGLE_APPLICATION_CREDENTIALS (Optional - For Backfill)
- Path to Google auth JSON file
- Or raw JSON content
- Only needed if using Google Drive

### GPSGATE_API_KEY (Optional - For Backfill)
- Your GpsGate API authentication token
- Only needed for live data fetching

---

## Quick Setup (Minimum Required)

1. **Get your DATABASE_URL** from your database provider
2. **Go to Render dashboard** → Select service → Environment
3. **Add one variable:**
   - Key: `DATABASE_URL`
   - Value: Your PostgreSQL connection string (with `?sslmode=require`)
4. **Click "Save"** and Render will auto-redeploy

---

## After Setting Variables

1. Render will automatically redeploy
2. Check "Logs" tab to verify app starts
3. Look for: `Running on http://127.0.0.1:5000` 
4. If error, review logs and update variables

---

## Testing After Deployment

Once redeployed with DATABASE_URL set:

```bash
# Test health endpoint
curl https://your-render-url.onrender.com/api/health

# Should return:
{
  "status": "healthy",
  "timestamp": "2025-01-13T...",
  "active_backfill_operations": 0
}
```

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `No module named 'psycopg'` | PostgreSQL driver missing | ✅ Fixed in requirements.txt |
| `DATABASE_URL is not set` | Missing env variable | Add DATABASE_URL to Environment |
| `Connection refused` | Database URL wrong | Verify connection string format |
| `SSL certificate error` | Missing ?sslmode=require | Add `?sslmode=require` to URL |

---

## Files Just Created

✅ **Procfile** - Tells Render how to start the app (`gunicorn wsgi:app`)  
✅ **render.yaml** - Explicit Render configuration  
✅ **config.py updated** - Better error handling for missing DATABASE_URL  

**New Commit:** `1f427d9`

---

## Next Steps

1. Set DATABASE_URL in Render environment variables
2. Wait for auto-redeploy (2-5 minutes)
3. Check "Logs" for startup messages
4. Test with `/api/health` endpoint
5. Start backfill when ready

---

**Status:** Code is ready. Just waiting for your DATABASE_URL to be set in Render.
