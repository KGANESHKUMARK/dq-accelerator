@echo off
REM ============================================================================
REM DQ Accelerator - New Machine Setup Script
REM ============================================================================
REM This script sets up the complete environment on a fresh Windows machine.
REM Run from the project root directory.
REM ============================================================================

echo.
echo ═══════════════════════════════════════════════════════════════════════════
echo   DQ ACCELERATOR - NEW MACHINE SETUP
echo ═══════════════════════════════════════════════════════════════════════════
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python 3.12 not found. Please install Python 3.12 from python.org
    pause
    exit /b 1
)
echo [OK] Python found

REM Check Java (JDK 21)
java -version >nul 2>&1
if errorlevel 1 (
    echo [WARN] Java JDK 21 not found. Spark requires JDK 21.
    echo        Download from: https://www.oracle.com/java/technologies/downloads/
    echo        Continuing anyway...
) else (
    echo [OK] Java found
)

REM Create virtual environment
if not exist .venv (
    echo.
    echo [1/5] Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
) else (
    echo [OK] Virtual environment already exists
)

REM Activate venv
call .\.venv\Scripts\activate.bat

REM Upgrade pip
echo.
echo [2/5] Upgrading pip...
python -m pip install --upgrade pip -q

REM Install requirements
echo.
echo [3/5] Installing Python dependencies...
echo       This may take 5-10 minutes on first run...
pip install -r requirements.txt -q
if errorlevel 1 (
    echo [ERROR] Failed to install requirements
    pause
    exit /b 1
)

REM Install DQX with LLM extras
echo.
echo [4/5] Installing DQX LLM extensions...
pip install "databricks-labs-dqx[llm]" -q
if errorlevel 1 (
    echo [WARN] DQX LLM install failed - will use MockAI fallback
) else (
    echo [OK] DQX LLM installed
)

REM Install frontend dependencies
echo.
echo [5/5] Setting up frontend...
cd ui\frontend
if not exist node_modules (
    echo       Installing Node.js dependencies (npm install)...
    npm install --silent
    if errorlevel 1 (
        echo [WARN] Frontend dependencies failed - UI will not work
        cd ..\..
    ) else (
        echo [OK] Frontend dependencies installed
        cd ..\..
    )
) else (
    echo [OK] Frontend dependencies already installed
    cd ..\..
)

echo.
echo ═══════════════════════════════════════════════════════════════════════════
echo   SETUP COMPLETE
echo ═══════════════════════════════════════════════════════════════════════════
echo.
echo Next steps:
echo   1. Set environment variables (if using DQX):
echo        set DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
echo        set DATABRICKS_TOKEN=your-pat-token
echo        set DQX_MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct
echo.
echo   2. Start the application:
echo        start.bat          (interactive menu)
echo        start_ui.bat       (backend + frontend)
echo.
echo   3. Open browser:
echo        Backend:  http://127.0.0.1:8000
echo        Frontend: http://localhost:5173
echo.
pause
