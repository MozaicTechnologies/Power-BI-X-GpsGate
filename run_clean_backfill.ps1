# Clean backfill script - Delete all data then insert fresh (single command)

Write-Host "========================================" -ForegroundColor Green
Write-Host "CLEAN BACKFILL - Delete & Re-insert Fresh Data" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

Write-Host "`n[STEP 1/2] Clearing all fact tables..." -ForegroundColor Cyan
venv\Scripts\python.exe clear_and_backfill.py

Write-Host "`n[STEP 2/2] Running backfill with UPSERT logic..." -ForegroundColor Cyan
venv\Scripts\python.exe backfill_direct_python.py

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "BACKFILL COMPLETE - Fresh data inserted" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
