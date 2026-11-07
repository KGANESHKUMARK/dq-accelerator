@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ============================================================================
rem  DQ Accelerator - one-click launcher (Windows)
rem  Sets up the project-local environment (everything stays on D:) and shows
rem  an interactive menu: demo, tests, dqctl commands.
rem ============================================================================

set "PROJECT_ROOT=%~dp0"
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"
set "SCRATCH=%PROJECT_ROOT%\.tmp"
set "LOGDIR=%PROJECT_ROOT%\logs"
set "VENV_PY=%PROJECT_ROOT%\.venv\Scripts\python.exe"

rem --- environment: keep ALL temp/cache/scratch inside the project (D:) -------
if not exist "%SCRATCH%\temp" mkdir "%SCRATCH%\temp"
if not exist "%SCRATCH%\pip-cache" mkdir "%SCRATCH%\pip-cache"
if not exist "%LOGDIR%\engine" mkdir "%LOGDIR%\engine"
if not exist "%LOGDIR%\jvm" mkdir "%LOGDIR%\jvm"
if not exist "%LOGDIR%\backend" mkdir "%LOGDIR%\backend"
if not exist "%LOGDIR%\frontend" mkdir "%LOGDIR%\frontend"
if not exist "%LOGDIR%\diagnostics" mkdir "%LOGDIR%\diagnostics"
set "TEMP=%SCRATCH%\temp"
set "TMP=%SCRATCH%\temp"
set "PIP_CACHE_DIR=%SCRATCH%\pip-cache"
set "DQ_SCRATCH_DIR=%SCRATCH%"
set "DQ_LOG_DIR=%LOGDIR%"
set "PYTHONUTF8=1"
set "PYTHONDONTWRITEBYTECODE="
set "PYSPARK_PYTHON=%VENV_PY%"
set "PYSPARK_DRIVER_PYTHON=%VENV_PY%"

rem --- virtualenv must exist ---------------------------------------------------
if not exist "%VENV_PY%" (
    echo [setup] virtualenv not found - running scripts\setup.ps1 ...
    powershell -NoProfile -ExecutionPolicy Bypass -File "%PROJECT_ROOT%\scripts\setup.ps1"
    if errorlevel 1 ( echo [ERROR] setup failed. & pause & exit /b 1 )
)

:menu
cls
echo ============================================================
echo   DQ ACCELERATOR - Data Quality Engine POC
echo   %PROJECT_ROOT%
echo ============================================================
echo.
echo   [1] Run standalone Spark demo   (examples\local - writes data\ + reports\)
echo   [2] Run FULL test suite         (~9 min, 130 tests on local Spark)
echo   [3] Run UNIT tests only         (~1 sec, no Spark)
echo   [4] dqctl menu                  (profile / run / ruleset / pipeline)
echo   [5] Regenerate sample data      (scripts\generate_sample_data.py)
echo   [6] Runtime self-check          (scripts\check_runtime.py)
echo   [7] Open docs folder
echo   [8] Launch Web UI           (React :5173 + FastAPI :8000)
echo   [0] Exit
echo.
set /p "CHOICE=Select option: "

if "%CHOICE:~0,1%"=="1" goto demo
if "%CHOICE:~0,1%"=="2" goto fulltests
if "%CHOICE:~0,1%"=="3" goto unittest
if "%CHOICE:~0,1%"=="4" goto dqctlmenu
if "%CHOICE:~0,1%"=="5" goto gendata
if "%CHOICE:~0,1%"=="6" goto runtimecheck
if "%CHOICE:~0,1%"=="7" goto docs
if "%CHOICE:~0,1%"=="8" goto ui
if "%CHOICE:~0,1%"=="0" goto :eof

echo.
echo Invalid choice.
pause
goto menu

rem ==========================================================================
:demo
echo.
echo === Running standalone Spark demo (local[2], REMEDIATE mode) ===
echo.
"%VENV_PY%" "%PROJECT_ROOT%\examples\local\standalone_spark_example.py"
echo.
echo === Demo finished. Outputs in data\ and reports\ ===
pause
goto menu

rem ==========================================================================
:fulltests
echo.
echo === Running FULL test suite (~9 min - starts local Spark) ===
echo.
"%VENV_PY%" -m pytest "%PROJECT_ROOT%\packages\dq-core\tests" -v
echo.
pause
goto menu

rem ==========================================================================
:unittest
echo.
echo === Running unit tests (no Spark) ===
echo.
"%VENV_PY%" -m pytest "%PROJECT_ROOT%\packages\dq-core\tests\unit" -v
echo.
pause
goto menu

rem ==========================================================================
:dqctlmenu
:dlmenu
cls
echo ============================================================
echo   dqctl - DQ Accelerator CLI
echo ============================================================
echo.
echo   [p] profile a file        dqctl profile sample_data\bronze\customers.csv
echo   [r] run DQ on a file      dqctl run ... --mode OBSERVE_ONLY^|ENFORCE^|REMEDIATE
echo   [s] ruleset commands      publish / list / diff / activate / rollback / verify
echo   [l] pipeline commands     onboard / list / test / activate / pause / offboard
echo   [b] back to main menu
set /p "DC=Select: "

if /i "%DC:~0,1%"=="p" goto dprofile
if /i "%DC:~0,1%"=="r" goto drun
if /i "%DC:~0,1%"=="s" goto druleset
if /i "%DC:~0,1%"=="l" goto dpipeline
if /i "%DC:~0,1%"=="b" goto menu
goto dlmenu

:dprofile
echo.
set /p "SRC=Source CSV/Parquet path [sample_data\bronze\customers.csv]: "
if "%SRC%"=="" set "SRC=sample_data\bronze\customers.csv"
set /p "DS=Dataset name [customers]: "
if "%DS%"=="" set "DS=customers"
"%VENV_PY%" -m dq_core.cli.main profile "%PROJECT_ROOT%\%SRC%" --dataset %DS% --out "%PROJECT_ROOT%\.tmp\cli_profile.json"
pause
goto dlmenu

drun
echo.
set /p "SRC=Source CSV/Parquet path [sample_data\bronze\customers.csv]: "
if "%SRC%"=="" set "SRC=sample_data\bronze\customers.csv"
set /p "RS=RuleSet YAML [config\rulesets\customers_v1.yaml]: "
if "%RS%"=="" set "RS=config\rulesets\customers_v1.yaml"
set /p "MODE=Mode OBSERVE_ONLY / ENFORCE / REMEDIATE [OBSERVE_ONLY]: "
if "%MODE%"=="" set "MODE=OBSERVE_ONLY"
set /p "DS=Dataset name [customers]: "
if "%DS%"=="" set "DS=customers"
"%VENV_PY%" -m dq_core.cli.main run "%PROJECT_ROOT%\%SRC%" --ruleset "%PROJECT_ROOT%\%RS%" --policy "%PROJECT_ROOT%\config\policies\default.yaml" --mode %MODE% --dataset %DS% --report "%PROJECT_ROOT%\reports\cli_run.json"
pause
goto dlmenu

druleset
echo.
echo   [1] list            [2] publish v1.0     [3] publish v1.1
echo   [4] diff 1.0 1.1    [5] activate 1.1     [6] rollback to 1.0
set /p "RC=Choice: "
if "%RC:~0,1%"=="1" "%VENV_PY%" -m dq_core.cli.main ruleset list
if "%RC:~0,1%"=="2" "%VENV_PY%" -m dq_core.cli.main ruleset publish "%PROJECT_ROOT%\config\rulesets\customers_v1.yaml" --activate --actor startbat
if "%RC:~0,1%"=="3" "%VENV_PY%" -m dq_core.cli.main ruleset publish "%PROJECT_ROOT%\config\rulesets\customers_v1_1.yaml" --no-activate --actor startbat
if "%RC:~0,1%"=="4" "%VENV_PY%" -m dq_core.cli.main ruleset diff customers_silver 1.0 1.1
if "%RC:~0,1%"=="5" "%VENV_PY%" -m dq_core.cli.main ruleset activate customers_silver 1.1 --actor startbat
if "%RC:~0,1%"=="6" "%VENV_PY%" -m dq_core.cli.main ruleset rollback customers_silver 1.0 --actor startbat
pause
goto dlmenu

dpipeline
echo.
echo   [1] list   [2] onboard customer   [3] test   [4] activate
echo   [5] pause  [6] offboard
set /p "PC=Choice: "
if "%PC:~0,1%"=="1" "%VENV_PY%" -m dq_core.cli.main pipeline list
if "%PC:~0,1%"=="2" "%VENV_PY%" -m dq_core.cli.main pipeline onboard --manifest "%PROJECT_ROOT%\config\pipelines\customer_pipeline.yaml" --actor startbat
if "%PC:~0,1%"=="3" "%VENV_PY%" -m dq_core.cli.main pipeline test DATABRICKS_CUSTOMER_PIPELINE --actor startbat
if "%PC:~0,1%"=="4" "%VENV_PY%" -m dq_core.cli.main pipeline activate DATABRICKS_CUSTOMER_PIPELINE --actor startbat
if "%PC:~0,1%"=="5" "%VENV_PY%" -m dq_core.cli.main pipeline pause DATABRICKS_CUSTOMER_PIPELINE --reason "paused via start.bat" --actor startbat
if "%PC:~0,1%"=="6" "%VENV_PY%" -m dq_core.cli.main pipeline offboard DATABRICKS_CUSTOMER_PIPELINE --reason "offboarded via start.bat" --actor startbat
pause
goto dlmenu

rem ==========================================================================
:gendata
echo.
"%VENV_PY%" "%PROJECT_ROOT%\scripts\generate_sample_data.py"
pause
goto menu

:runtimecheck
echo.
"%VENV_PY%" "%PROJECT_ROOT%\scripts\check_runtime.py"
pause
goto menu

:docs
explorer "%PROJECT_ROOT%\docs"
goto menu

:ui
echo.
echo === Launching Web UI (React + FastAPI) ===
call "%PROJECT_ROOT%\start_ui.bat"
goto menu
