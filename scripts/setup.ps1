# One-time local setup (Windows PowerShell). Everything is installed inside the project folder:
#   .venv\        Python virtual environment (Python 3.12)
#   .tmp\         pip cache, TEMP/TMP, Spark scratch, compiled Windows Hadoop shim
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
param([string]$Python = "")
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "env.ps1")

if (-not (Test-Path $VenvPython)) {
    if (-not $Python) {
        $Python = (& py -3.12 -c "import sys; print(sys.executable)") 2>$null
        if (-not $Python) { throw "Python 3.12 not found. Pass -Python <path to python3.12.exe>" }
    }
    Write-Host "Creating virtual environment with $Python"
    & $Python -m venv (Join-Path $ProjectRoot ".venv")
}

Write-Host "Installing pinned dependencies into $VenvPython"
& $VenvPython -m pip install --disable-pip-version-check -r (Join-Path $ProjectRoot "requirements.txt")
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }

Write-Host "Verifying runtime (starts a local Spark session, builds the Windows Hadoop shim if needed)"
& $VenvPython (Join-Path $ProjectRoot "scripts\check_runtime.py")
if ($LASTEXITCODE -ne 0) { throw "runtime check failed" }
Write-Host "Setup complete."
