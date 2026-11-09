@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ============================================================================
rem  DQ Accelerator - SINGLE ENTRY POINT (Windows)
rem  Checks required software, sets up environment, starts backend + frontend.
rem  Run this file for first-time setup AND daily use.
rem ============================================================================

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "VENV_PY=%ROOT%\.venv\Scripts\python.exe"
set "LOGDIR=%ROOT%\logs"
set "MISSING=0"

title DQ Accelerator - Startup
cls
echo ============================================================
echo   DQ ACCELERATOR - Startup Check
echo   %ROOT%
echo ============================================================
echo.

rem ---------------------------------------------------------------- 1. PYTHON
echo [1/6] Checking Python 3.12+ ...
set "PYOK="
for /f "tokens=2 delims= " %%v in ('python --version 2^>nul') do set "PYVER=%%v"
if not defined PYVER (
    echo      [MISSING] Python not found
    echo      Install: https://www.python.org/downloads/release/python-3129/
    set "MISSING=1"
) else (
    for /f "tokens=1,2 delims=." %%a in ("%PYVER%") do (
        if "%%a"=="3" if %%b GEQ 12 set "PYOK=1"
    )
    if defined PYOK (echo      [OK] Python %PYVER%) else (
        echo      [WARN] Python %PYVER% - need 3.12+. Install: https://www.python.org/downloads/release/python-3129/
        set "MISSING=1"
    )
)

rem ---------------------------------------------------------------- 2. JAVA
echo [2/6] Checking Java JDK 21+ ...
set "JAVAOK="
for /f "tokens=3" %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do set "JVER=%%~v"
if not defined JVER (
    echo      [MISSING] Java not found
    echo      Install JDK 21: https://learn.microsoft.com/en-us/java/openjdk/download
    set "MISSING=1"
) else (
    set "JVER=%JVER:"=%"
    for /f "tokens=1 delims=." %%a in ("%JVER%") do if %%a GEQ 21 set "JAVAOK=1"
    if defined JAVAOK (echo      [OK] Java %JVER%) else (
        echo      [WARN] Java %JVER% - need 21+. Install: https://learn.microsoft.com/en-us/java/openjdk/download
        set "MISSING=1"
    )
)

rem ---------------------------------------------------------------- 3. NODE
echo [3/6] Checking Node.js 18+ ...
set "NODEOK="
for /f "tokens=1 delims=v" %%v in ('node --version 2^>nul') do set "NVER=%%v"
if not defined NVER (
    echo      [MISSING] Node.js not found
    echo      Install LTS: https://nodejs.org/en/download
    set "MISSING=1"
) else (
    for /f "tokens=1 delims=v." %%a in ("%NVER%") do set "NMAJ=%%a"
    set "NMAJ=%NMAJ:v=%"
    if %NMAJ% GEQ 18 (echo      [OK] Node.js v%NVER%) else (
        echo      [WARN] Node.js v%NVER% - need 18+. Install: https://nodejs.org/en/download
        set "MISSING=1"
    )
)

if "%MISSING%"=="1" (
    echo.
    echo ============================================================
    echo   Missing required software - install the items above,
    echo   then re-run DQ_start.bat
    echo   Full guide: docs\SETUP.md
    echo ============================================================
    pause
    exit /b 1
)

rem ------------------------------------------------------------- 4. PYTHON VENV
echo.
echo [4/6] Python virtual environment ...
if not exist "%VENV_PY%" (
    echo      Creating .venv ...
    python -m venv "%ROOT%\.venv"
    if errorlevel 1 ( echo      [ERROR] venv creation failed & pause & exit /b 1 )
)
"%VENV_PY%" -m pip install --upgrade pip -q

rem Install deps (fast - only installs what's missing/changed)
echo      Installing requirements (this takes a few minutes first time)...
mkdir "%ROOT%\.tmp\pip-cache" 2>nul
set "PIP_CACHE_DIR=%ROOT%\.tmp\pip-cache"
"%VENV_PY%" -m pip install -r "%ROOT%\requirements.txt" -q
if errorlevel 1 (
    echo      [ERROR] pip install failed - check logs, then retry
    pause & exit /b 1
)
echo      [OK] Python deps installed

rem ------------------------------------------------------------- 5. FRONTEND
echo [5/6] Frontend dependencies + build ...
if not exist "%ROOT%\ui\frontend\node_modules" (
    echo      npm install (first time - a few minutes)...
    pushd "%ROOT%\ui\frontend"
    call npm install --silent
    if errorlevel 1 ( echo      [ERROR] npm install failed & popd & pause & exit /b 1 )
    popd
)
if not exist "%ROOT%\ui\frontend\dist\index.html" (
    echo      Building production bundle...
    pushd "%ROOT%\ui\frontend"
    call npm run build --silent
    if errorlevel 1 ( echo      [ERROR] npm build failed & popd & pause & exit /b 1 )
    popd
)
echo      [OK] Frontend ready

rem ------------------------------------------------------------- 6. LAUNCH
echo [6/6] Starting services ...
mkdir "%LOGDIR%\backend" 2>nul & mkdir "%LOGDIR%\frontend" 2>nul
mkdir "%LOGDIR%\engine" 2>nul & mkdir "%LOGDIR%\jvm" 2>nul & mkdir "%LOGDIR%\diagnostics" 2>nul
set "DQ_LOG_DIR=%LOGDIR%"

rem Backend window
start "DQ-Backend" powershell -NoExit -Command ^
  "$env:DQ_LOG_DIR='%LOGDIR%'; & '%VENV_PY%' -m uvicorn main:app --app-dir '%ROOT%\ui\backend' --host 127.0.0.1 --port 8000 2>&1 | Tee-Object -Append '%LOGDIR%\backend\backend_console.log'"

timeout /t 4 >nul

rem Frontend dev window (hot reload; alternatively backend can serve dist/ - see SETUP.md)
start "DQ-Frontend" powershell -NoExit -Command "Set-Location '%ROOT%\ui\frontend'; npm run dev 2>&1 | Tee-Object -Append '%LOGDIR%\frontend\frontend_console.log'"

timeout /t 4 >nul
start "" http://localhost:5173

echo.
echo ============================================================
echo   RUNNING
echo   Backend API : http://127.0.0.1:8000  (docs: /docs)
echo   Frontend UI : http://localhost:5173
echo   Logs        : %LOGDIR%\
echo   To enable real LLM: set DATABRICKS_HOST/TOKEN/DQX_MODEL_ENDPOINT
echo   then restart this launcher.
echo ============================================================
echo.
echo Close the "DQ-Backend" and "DQ-Frontend" windows to stop.
pause
