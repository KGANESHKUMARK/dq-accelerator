# Dot-source this file:  . .\scripts\env.ps1
# Keeps every temporary / cache / scratch file inside the project folder (no C: drive usage).
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Scratch = Join-Path $ProjectRoot ".tmp"
$LogDir = Join-Path $ProjectRoot "logs"
foreach ($d in @("temp", "pip-cache")) { New-Item -ItemType Directory -Force -Path (Join-Path $Scratch $d) | Out-Null }
foreach ($d in @("engine", "jvm", "backend", "frontend", "diagnostics")) { New-Item -ItemType Directory -Force -Path (Join-Path $LogDir $d) | Out-Null }

$env:TEMP = Join-Path $Scratch "temp"
$env:TMP = $env:TEMP
$env:PIP_CACHE_DIR = Join-Path $Scratch "pip-cache"
$env:DQ_SCRATCH_DIR = $Scratch
$env:DQ_LOG_DIR = $LogDir
$env:PYTHONDONTWRITEBYTECODE = $null
$env:PYTHONUTF8 = "1"
$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $VenvPython
$env:PYSPARK_DRIVER_PYTHON = $VenvPython
Set-Location $ProjectRoot
