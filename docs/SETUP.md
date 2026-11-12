# DQ Accelerator — Setup Guide (Windows)

Single entry point: **`DQ_start.bat`** — checks software, installs deps, starts backend + frontend.

## Prerequisites (install once)

| Software | Version | Download |
|---|---|---|
| Python | 3.12+ | https://www.python.org/downloads/ (check "Add to PATH") |
| Java JDK | 21+ | https://learn.microsoft.com/en-us/java/openjdk/download |
| Node.js | 18+ LTS | https://nodejs.org/en/download |

Verify: `python --version`, `java -version`, `node --version` — or just run `DQ_start.bat`; it checks and shows the links.

## Fresh machine — first run

1. Install the 3 prerequisites above (one-time, ~15 min).
2. `git clone <repo-url>` (or copy the folder) to e.g. `D:\DQ-Accelerator`. Everything installs **inside the project folder** — no system-wide changes.
3. Double-click **`DQ_start.bat`**. First run takes ~10 min (venv + pip + npm); later runs ~30 s.
4. The launcher prints `[1/6]`–`[6/6]` progress; `[MISSING]` = install that item and re-run. If the window closes instantly, the bat file may be corrupted — re-copy it (it must have **CRLF** line endings and no BOM).
5. **Seed the metadata store** (one-time — `data/` and `registry/` are gitignored, so a fresh clone starts empty):
   ```
   .venv\Scripts\python.exe scripts\migrate_registry_to_nosql.py --from-config
   ```
   This publishes all `config/rulesets/*.yaml` and onboards all `config/pipelines/*.yaml` into the NoSQL store. Safe to re-run.
6. Browser auto-opens `http://localhost:5173` (or open it manually).

### Verify it worked

- UI loads → **Dashboard** shows KPIs (not a blank/error page)
- UI → **Pipelines** lists the seeded pipelines (GOAT_HERD, FABRIC_ORDERS, BANK_TRANSACTIONS, …)
- UI → **Rulesets** lists published rulesets with versions
- `curl http://127.0.0.1:8000/api/health` → `{"status":"ok"}`
- API docs: `http://127.0.0.1:8000/docs`
- UI → **Settings → System status**: Spark engine OK; AI provider shows `mock` (or `dqx` if `.env` set)
- UI → **Settings → Metadata store**: backend `nosql`, engine `tinydb`, collections populated

## Daily use

```
DQ_start.bat
```

What it does: verifies Python/Java/Node → creates `.venv` → `pip install -r requirements.txt` → installs `requirements-llm.txt` **only if `DATABRICKS_TOKEN` is set** → `npm install` + `npm run build` → opens **DQ-Backend** (:8000) and **DQ-Frontend** (:5173) windows + browser.

Legacy launchers (`start.bat`, `start_ui.bat`, `setup_new_machine.bat`) are in `archive/` — not needed.

Stop: close the two service windows.

## Real LLM (optional — Databricks Model Serving)

Core deps are in `requirements.txt` — LLM deps are in **`requirements-llm.txt`** and are installed **automatically by `DQ_start.bat` only when `DATABRICKS_TOKEN` is set**. Without them the app runs with MockAI (no external LLM).

Create a `.env` file in the project root (gitignored — never commit it):

```
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<personal-access-token>
DQX_MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct
```

`.env` is loaded automatically by `DQ_start.bat` **and** by the backend on startup — so the token survives restarts and works however the app is launched. Real env vars still take precedence if both are set.

Or set env vars manually **before** running `DQ_start.bat` (same three variables as above).

Or install LLM deps manually: `pip install -r requirements-llm.txt`

`config/engine.yaml` has `ai.provider: dqx` — without credentials it auto-falls back to MockAI. Status visible in UI → **Settings → AI remediation provider** + **System status**.

## DQ Assistant (chatbot)

Floating `✦ DQ Assistant` button bottom-right on every page. Answers questions about onboarded pipelines, rulesets, jobs, anomalies, reports, setup steps, and roadmap — using the same Databricks LLM when credentials exist, built-in answers otherwise.

- Toggle on/off: **Settings → DQ Assistant** (writes `engine.yaml` → `assistant.enabled`)
- Chat history: `logs/assistant/chat_log.jsonl` (JSONL, one entry per turn)
- Endpoints: `POST /api/chat`, `GET/PUT /api/assistant/enabled`, `GET /api/chat/log`

## Hot config reload

After editing `config/engine.yaml` (provider, thresholds, runtime), use **Settings → Apply config changes** — no backend restart needed. For `runtime.*` changes (memory/partitions) use **Reload + restart Spark** (~30 s).

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
| Browser doesn't open | go to `http://localhost:5173` manually |
| `[MISSING]` on first run | install the printed software, re-run `DQ_start.bat` — it resumes where it left off |
