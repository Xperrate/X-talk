@echo off
chcp 65001 >nul
setlocal
set found=
for /f %%p in ('powershell -NoProfile -Command "(Get-NetTCPConnection -LocalPort 8321 -State Listen -ErrorAction SilentlyContinue).OwningProcess"') do (
    set found=1
    taskkill /F /PID %%p >nul
)
if defined found (
    echo [OK] group chat service stopped ^(port 8321 freed^).
) else (
    echo [INFO] not running: nothing is listening on port 8321.
)
pause
