# DQ Accelerator — Setup Guide (Windows)

Single entry point: **`DQ_start.bat`** — checks software, installs deps, starts backend + frontend.

## Prerequisites (install once)

| Software | Version | Download |
|---|---|---|
| Python | 3.12+ | https://www.python.org/downloads/ (check "Add to PATH") |
| Java JDK | 21+ | https://learn.microsoft.com/en-us/java/openjdk/download |
| Node.js | 18+ LTS | https://nodejs.org/en/download |

Verify: `python --version`, `java -version`, `node --version` — or just run `DQ_start.bat`; it checks and shows the links.

## Daily use

```
DQ_start.bat
```

What it does: verifies Python/Java/Node → creates `.venv` → `pip install -r requirements.txt` → `npm install` + `npm run build` → opens **DQ-Backend** (:8000) and **DQ-Frontend** (:5173) windows + browser.

Stop: close the two service windows.

## Real LLM (optional — Databricks Model Serving)

Set env vars **before** running `DQ_start.bat` (System properties → Environment Variables, or a `.env`/powershell profile):

```
DATABRICKS_HOST   = https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN  = <personal-access-token>
DQX_MODEL_ENDPOINT= databricks-meta-llama-3-3-70b-instruct
```

`config/engine.yaml` already has `ai.provider: dqx` — without credentials it auto-falls back to MockAI. Status visible in UI → **Settings → AI remediation provider** + **System status** (shows which software/env vars are detected).

## Repo layout

```
packages/dq-core/   engine (pip package, editable)
config/             engine.yaml, rulesets, pipelines, policies, approvals, ai
ui/backend/         FastAPI (jobs.py, main.py)  → http://127.0.0.1:8000
ui/frontend/        React+Vite                  → http://localhost:5173
sample_data/        synthetic CSVs (CDR 1GB regenerable via scripts/)
data/  registry/  reports/  logs/  .tmp/       runtime artefacts (gitignored)
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `pip` fails on pyspark | delete `.venv`, re-run `DQ_start.bat` |
| Spark OOM on big file | raise `runtime.driver_memory` in `config/engine.yaml` |
| Settings shows MockAI | env vars not set → set them, restart launcher |
| Port busy | another backend running → close stray `python`/`uvicorn` processes |
