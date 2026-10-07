@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

REM ---- already running? just open the page again ----
call :checkport && (
    start "" http://127.0.0.1:8321
    echo [OK] service is already running, opened browser directly.
    timeout /t 2 >nul
    exit /b 0
)

set "PY=python"
if exist ".venv\Scripts\python.exe" set "PY=.venv\Scripts\python.exe"

if "%PY%"=="python" (
    where python >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] Python not found in PATH. Install from https://python.org first ^(check "Add to PATH"^).
        pause
        exit /b 1
    )
)

if not exist "config.json" (
    if exist "config.example.json" (
        copy /Y "config.example.json" "config.json" >nul
        echo [INFO] created config.json from config.example.json.
    ) else (
        echo [ERROR] config.json not found. Run setup.bat or create config.json manually.
        pause
        exit /b 1
    )
)

REM ---- install dependencies on first run if missing ----
"%PY%" -c "import fastapi, uvicorn, httpx" >nul 2>nul
if errorlevel 1 (
    echo [INFO] installing dependencies ^(first time^)...
    "%PY%" -m pip install -r requirements.txt
    if errorlevel 1 goto :err_deps
)

REM ---- default to local-only mode ----
set "HOST=127.0.0.1"
set "PORT=8321"

REM ---- start the service in its own log window ----
start "X-Talk Service" cmd /k "%PY%" server.py

set /a n=0
:waitloop
call :checkport && goto :openbrowser
set /a n+=1
if %n% geq 20 (
    echo [ERROR] service did not come up within timeout, check the log window for the reason.
    pause
    exit /b 1
)
timeout /t 1 >nul
goto waitloop

:openbrowser
start "" http://127.0.0.1:8321
echo [OK] service started at http://127.0.0.1:8321 ^(log window stays open; close it to stop^).
timeout /t 3 >nul
exit /b 0

:err_deps
echo [ERROR] dependency install failed. Run manually in this folder: pip install -r requirements.txt
pause
exit /b 1

REM returns success if something is LISTENING on port 8321
:checkport
netstat -ano | findstr ":8321" | findstr "LISTENING" >nul 2>nul
exit /b %errorlevel%
