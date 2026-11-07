@echo off
setlocal
rem DQ Accelerator UI launcher - backend (FastAPI :8000) + frontend (Vite :5173)
rem Console output is mirrored into logs\backend\ and logs\frontend\
set "ROOT=%~dp0"
set "LOGDIR=%ROOT%logs"
mkdir "%LOGDIR%\backend" 2>nul
mkdir "%LOGDIR%\frontend" 2>nul
mkdir "%LOGDIR%\engine" 2>nul
mkdir "%LOGDIR%\jvm" 2>nul
mkdir "%LOGDIR%\diagnostics" 2>nul
set "DQ_LOG_DIR=%LOGDIR%"
start "DQ-API" powershell -NoExit -Command ". '%ROOT%scripts\env.ps1'; & '%ROOT%.venv\Scripts\python.exe' -m uvicorn main:app --app-dir '%ROOT%ui\backend' --host 127.0.0.1 --port 8000 2>&1 | Tee-Object -Append '%LOGDIR%\backend\backend_console.log'"
timeout /t 3 >nul
start "DQ-UI" powershell -NoExit -Command "Set-Location '%ROOT%ui\frontend'; npm run dev 2>&1 | Tee-Object -Append '%LOGDIR%\frontend\frontend_console.log'"
timeout /t 4 >nul
start "" http://localhost:5173
echo Backend: http://127.0.0.1:8000  Frontend: http://localhost:5173
echo Logs:    %LOGDIR%\
