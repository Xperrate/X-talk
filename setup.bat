@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python not found in PATH. Install Python 3.10+ from https://python.org and check "Add to PATH".
    pause
    exit /b 1
)

if not exist ".venv" (
    echo [INFO] creating local virtual environment...
    python -m venv .venv
    if errorlevel 1 goto :err
)

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" (
    echo [ERROR] virtual environment Python was not found: %PY%
    pause
    exit /b 1
)

echo [INFO] installing dependencies...
"%PY%" -m pip install --upgrade pip
if errorlevel 1 goto :err
"%PY%" -m pip install -r requirements.txt
if errorlevel 1 goto :err

if not exist "config.json" (
    if exist "config.example.json" (
        copy /Y "config.example.json" "config.json" >nul
        echo [OK] created config.json from config.example.json.
    ) else (
        echo [WARN] config.example.json not found. Create config.json manually.
    )
) else (
    echo [OK] config.json already exists.
)

echo.
echo [OK] Local setup complete.
echo Next steps:
echo   1. Edit config.json and set model/base_url.
echo   2. Start your local OpenAI-compatible model endpoint.
echo   3. Double-click start.bat.
pause
exit /b 0

:err
echo [ERROR] setup failed. Check the messages above.
pause
exit /b 1
