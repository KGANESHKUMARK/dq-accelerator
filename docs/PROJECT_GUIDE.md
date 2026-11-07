# DQ Accelerator — Complete Project Guide

> One document covering: **what this is**, **setup**, **full project structure**,
> and **how to run everything**. For deep-dives see the linked docs in §8.

---

## 1. What is this project?

**DQ Accelerator** is a working Proof of Concept (POC) for an *Enterprise Data
Quality Accelerator* — a reusable **Apache Spark (PySpark) engine** that plugs
into existing Databricks-style data pipelines and adds data-quality capabilities
with minimal code changes.

In plain terms: you have a pipeline that reads raw data, transforms it, and
writes it out. This engine sits **on top of your existing DataFrames** and:

1. **Profiles** the data (nulls, duplicates, types, patterns, anomalies).
2. **Generates candidate quality rules** from that evidence (a human approves them).
3. **Validates** data against versioned, immutable rule sets.
4. **Fixes** problems — deterministic corrections (trim, case, lookup, dates)
   plus *simulated* AI proposals gated by a **90 % confidence threshold**.
5. **Quarantines** anything uncertain/unresolved and **re-validates** every fix.
6. Runs the whole thing through **Bronze → Silver → Gold** layers with
   quality gates, metrics and JSON audit reports.
7. Lets you **onboard / pause / offboard** pipelines via a lightweight registry.

### The three integration modes

| Mode | What it does |
|---|---|
| `OBSERVE_ONLY` | Measure & report only — pipeline output is **not** changed. All rows pass; failures are flagged. |
| `ENFORCE` | Clean rows continue; failing rows go to **quarantine**. No corrections. |
| `REMEDIATE` | Approved fixes applied → **re-validated** → clean rows accepted; the rest quarantined. |

### What it is NOT (honest scope)

- ✅ **Verified on local Spark 4.1.3** (Windows 11, Java 21, Python 3.12).
- ⚠️ Databricks integration = runnable notebook-style script + contract tests —
  **no real Databricks workspace was used**.
- ❌ No UI, no REST API, no database, no real LLM, no production
  Airflow/ADF/dbt/Flink connectors (extension contracts only).

---

## 2. Prerequisites

| Requirement | Version used | Notes |
|---|---|---|
| OS | Windows 11 | Scripts are PowerShell; Linux/macOS need equivalent env vars |
| Python | **3.12** | via `py -3.12` launcher |
| Java/JDK | **21** | `JAVA_HOME=C:\Program Files\Java\jdk-21` |
| Disk | ~4 GB free on **D:** | Everything installs inside the project — **C: is never used** |
| PowerShell | 5.1+ | all scripts are `.ps1` |

**Important project rule:** the virtualenv, pip cache, TEMP/TMP and Spark
scratch all live under `.venv\` and `.tmp\` **inside this folder on D:**.
`scripts\env.ps1` sets this up automatically — do not bypass it.

---

## 3. Setup (one time, ~10–15 min)

Open **PowerShell** in the project root
(`D:\Siddharitha\SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED\Projects\Linkedin\DQ-Accelerator`):

```powershell
# 1. Full setup: create .venv, install pinned deps, verify Spark runtime
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1

# 2. Load environment for this session (TEMP/TMP/pip-cache -> .tmp on D:)
. .\scripts\env.ps1
```

`setup.ps1` does three things:
- creates `.venv\` with Python 3.12 (if missing),
- `pip install -r requirements.txt` (pyspark 4.1.3, py4j, PyYAML 6.0.2, pytest, `-e ./packages/dq-core`),
- runs `scripts\check_runtime.py` — starts a local Spark session, prints
  versions, and compiles the Windows Hadoop filesystem shim needed for
  Parquet writes on Windows (no `winutils.exe` required).

Expected tail of setup:

```
Spark 4.1.3 | Java 21.0.1 | Python 3.12.0 | master=local[2] | ANSI=ON | parquet roundtrip OK
Setup complete.
```

### Regenerate sample data (only if needed)

```powershell
& $VenvPython scripts\generate_sample_data.py
```

Writes `sample_data\bronze\customers.csv`, `transactions.csv` and
`sample_data\expected_results.json` (the oracle used by tests). Deterministic —
same output every run.

### Verify runtime any time

```powershell
& $VenvPython scripts\check_runtime.py
```

---

## 4. Full project structure

```
DQ-Accelerator/
│
├── README.md                     ← short overview + quick start
├── requirements.txt              ← pinned deps (pyspark==4.1.3, pytest==8.4.2, …)
├── pyproject.toml                ← pytest config (markers: spark/integration/e2e)
│
├── .venv\                         ← project virtualenv (Python 3.12)  [D: only]
├── .tmp\                          ← pip cache, TEMP/TMP, Spark scratch,
│   │                                compiled Windows Hadoop shim jar
├── .devin\                        ← tool config
│
├── packages/
│   └── dq-core/                  ← THE ENGINE (installable Python package)
│       ├── pyproject.toml        ← package metadata; `dqctl` CLI entry point
│       ├── src/dq_core/          ← all engine source code (see §5)
│       └── tests/                ← 130 automated tests
│           ├── conftest.py       ← shared Spark session + fixtures
│           ├── unit/             ← no Spark needed (metadata, policies, candidates…)
│           ├── integration/      ← real Spark runs (profiling, validation, modes…)
│           └── e2e/              ← reserved for end-to-end scenarios
│
├── config/                       ← ALL engine configuration (YAML)
│   ├── engine.yaml               ← local engine config (paths, thresholds, modes)
│   ├── engine.databricks.yaml    ← same but dbfs:/Volume paths for Databricks
│   ├── policies/
│   │   └── default.yaml          ← BANKING_DEFAULT policy: 90% threshold,
│   │                                sensitive columns, quality gate, approvals
│   ├── approvals/
│   │   └── approved_corrections.yaml   ← pre-approved corrections
│   ├── ai/
│   │   └── mock_ai_responses.yaml      ← SIMULATED AI proposals (no real LLM)
│   ├── rules/
│   │   └── lookups/country_codes.yaml  ← approved lookup table (country→ISO)
│   ├── rulesets/                 ← versioned rule sets (the "what to check")
│   │   ├── customers_v1.yaml         ← customers SILVER rules v1.0
│   │   ├── customers_v1_1.yaml       ← v1.1 (stricter age, +SUSPENDED status…)
│   │   ├── transactions_v1.yaml      ← transactions SILVER rules
│   │   ├── transactions_gold_v1.yaml ← transactions GOLD (ref-integrity, biz rules)
│   │   └── customer_summary_gold_v1.yaml
│   └── pipelines/                ← pipeline manifests for the registry
│       ├── customer_pipeline.yaml    ← DATABRICKS_CUSTOMER_PIPELINE
│       └── transaction_pipeline.yaml
│
├── integrations/
│   ├── databricks/              ← Databricks notebook-style examples
│   │   ├── customer_pipeline_original.py        ← "before" (no DQ)
│   │   ├── customer_pipeline_dq_integrated.py   ← "after" (~6 added lines)
│   │   └── README.md
│   └── contracts/
│       └── adapter_contract.py  ← PlatformAdapter ABC + Dataproc/Fabric/Airflow/ADF
│
├── examples/
│   ├── README.md
│   └── local/
│       └── standalone_spark_example.py  ← runnable local end-to-end demo
│
├── sample_data/                 ← SYNTHETIC banking data (deterministic)
│   ├── bronze/
│   │   ├── customers.csv        ← 38 rows, intentional defects
│   │   └── transactions.csv     ← 40 rows, intentional defects
│   └── expected_results.json    ← per-record expected outcomes (test oracle)
│
├── scripts/
│   ├── setup.ps1                ← one-time install + runtime check
│   ├── env.ps1                  ← dot-source: redirects TEMP/TMP/cache to .tmp
│   ├── check_runtime.py         ← Spark/Java/Python/parquet self-check
│   └── generate_sample_data.py  ← regenerates CSVs + expected_results.json
│
├── docs/                        ← 13 deep-dive documents (see §8)
├── data/                        ← runtime outputs (bronze/silver/gold/quarantine/audit)
├── reports/                     ← JSON execution reports written by runs
└── registry/                    ← runtime registries (rulesets + pipelines state)
```

---

## 5. Inside the engine — `packages/dq-core/src/dq_core/`

| Module | What it does |
|---|---|
| `engine.py` | **`DataQualityEngine`** — the public facade: `profile()`, `generate_candidate_rules()`, `validate()`, `run()` |
| `context/` | `PipelineContext`, `IntegrationMode`, `Stage`, `ExecutionContext` — run identity & mode contracts |
| `runtime/` | config loading, errors, logging, schema utils, Spark inspection, local-session factory, Windows Hadoop shim |
| `metadata/` | `RuleDefinition` / `RuleSet` models + YAML/JSON serialization + checksum |
| `rules/` | `RuleSetRegistry` (publish/activate/rollback/diff/verify — immutable versions) |
| `profiling/` | column stats, semantic type inference, patterns, anomalies → `ProfileReport` |
| `generation/` | `CandidateRuleGenerator` + `approve_candidates` (CANDIDATE → APPROVED) |
| `validation/` | native PySpark rule evaluation → per-rule + per-record results |
| `remediation/` | deterministic fixes + AI-proposal application |
| `ai/` | `AIRemediationProvider` interface + `MockAIProvider` (simulated) |
| `policies/` | confidence threshold, sensitive categories, approvals, quality gate |
| `quarantine/` | quarantine DataFrame builder (`dq_*` audit columns, status taxonomy) |
| `revalidation/` | re-runs rules on corrected records |
| `orchestration/` | `DQRunner` (mode dispatch), `MedallionRunner` (B/S/G), `DQResult`, quality gate |
| `pipelines/` | manifest model + lifecycle registry (DRAFT→…→OFFBOARDED) |
| `reporting/` | `ExecutionMetrics` |
| `io/` | `CsvSource`/`ParquetSource`, `StorageSink`/`StoragePaths` |
| `cli/main.py` | **`dqctl`** command-line tool |

Public import surface:

```python
from dq_core import DataQualityEngine, PipelineContext
```

---

## 6. How to run — every entry point

> Always start with `. .\scripts\env.ps1` in the same PowerShell session.
> `$VenvPython` = `.venv\Scripts\python.exe`.

### 0. One-click launcher (easiest)

Double-click **`start.bat`** in the project root — it sets up the D:-only
environment and shows an interactive menu: standalone demo, full/unit test
suites, a `dqctl` submenu (profile / run / ruleset / pipeline / bootstrap), regenerate
sample data, runtime check, and open docs. If `.venv` is missing it runs
`scripts\setup.ps1` automatically first.

### 6.1 Automated tests (the main verification)

```powershell
# Full suite — ~9 min (starts one shared local Spark session)
& $VenvPython -m pytest packages\dq-core\tests\ -v

# Fast subsets
& $VenvPython -m pytest packages\dq-core\tests\unit\ -v            # no Spark, seconds
& $VenvPython -m pytest packages\dq-core\tests\integration\ -v     # Spark tests only
```

**Latest verified result: `130 passed in 531s`.** See `docs\testing_report.md`.

### 6.2 Standalone runnable example

```powershell
& $VenvPython examples\local\standalone_spark_example.py
```

Creates its own SparkSession, loads `customers.csv`, runs REMEDIATE, writes
accepted/quarantine Parquet + a JSON report. Expected tail:

```
accepted=24  quarantined=13
deterministic_fixes=11 ai_fixes=4   quality gate: PASSED
quarantine by status: {PENDING_HUMAN_APPROVAL:2, BELOW_CONFIDENCE_THRESHOLD:3,
                       UNRESOLVED_NO_SAFE_REMEDIATION:7, CORRECTION_FAILED_REVALIDATION:1}
```

### 6.3 CLI — `dqctl` (five command groups)

```powershell
# --- bootstrap: source file -> approved rule set + onboarding manifest ---
& $VenvPython -m dq_core.cli.main bootstrap sample_data\bronze\patients.csv `
    --dataset patients --pipeline-id HEALTHCARE_PATIENT_PIPELINE `
    --policy ../policies/healthcare.yaml --copy --publish --onboard

# --- pipeline lifecycle ---
& $VenvPython -m dq_core.cli.main pipeline onboard  --manifest config\pipelines\customer_pipeline.yaml
& $VenvPython -m dq_core.cli.main pipeline list
& $VenvPython -m dq_core.cli.main pipeline test     DATABRICKS_CUSTOMER_PIPELINE
& $VenvPython -m dq_core.cli.main pipeline activate DATABRICKS_CUSTOMER_PIPELINE
& $VenvPython -m dq_core.cli.main pipeline pause    DATABRICKS_CUSTOMER_PIPELINE --reason "maintenance"
& $VenvPython -m dq_core.cli.main pipeline offboard DATABRICKS_CUSTOMER_PIPELINE --reason "done"

# --- rule set versioning ---
& $VenvPython -m dq_core.cli.main ruleset publish config\rulesets\customers_v1.yaml --activate
& $VenvPython -m dq_core.cli.main ruleset publish config\rulesets\customers_v1_1.yaml --no-activate
& $VenvPython -m dq_core.cli.main ruleset list
& $VenvPython -m dq_core.cli.main ruleset diff customers_silver 1.0 1.1
& $VenvPython -m dq_core.cli.main ruleset activate customers_silver 1.1
& $VenvPython -m dq_core.cli.main ruleset rollback customers_silver 1.0
& $VenvPython -m dq_core.cli.main ruleset verify  config\rulesets\customers_v1.yaml

# --- profiling ---
& $VenvPython -m dq_core.cli.main profile sample_data\bronze\customers.csv --dataset customers --out .tmp\profile.json

# --- run DQ on a file ---
& $VenvPython -m dq_core.cli.main run sample_data\bronze\customers.csv `
    --ruleset config\rulesets\customers_v1.yaml `
    --policy  config\policies\default.yaml `
    --mode REMEDIATE --dataset customers `
    --report reports\customers_run.json --out data\silver\customers
```

`--mode` accepts `OBSERVE_ONLY` (default) / `ENFORCE` / `REMEDIATE`.
`run` exits `0` when the quality gate passes, `1` otherwise.

#### 6.3.1 `bootstrap` — the fast path for new pipelines

`dqctl bootstrap` turns any CSV/Parquet source file into a complete onboarding
package in one command, so a new pipeline never starts from a hand-written YAML:

1. **Profiles** the source with the Spark profiler (FULL mode).
2. **Generates candidate rules** from the profile evidence
   (NOT_NULL, UNIQUE, REGEX, DATE_FORMAT, RANGE, ALLOWED_VALUES,
   STRING_LENGTH, DUPLICATE, SCHEMA_VALIDATION — referential-integrity
   rules are added when related profiles are supplied via the engine API).
3. **Auto-approves** all candidates into a DRAFT rule set — each rule keeps its
   `generation.confidence` + `evidence` so a steward can still audit what was
   generated (printed per-rule in the command output).
4. **Writes** `config/rulesets/<dataset>_v1.yaml` and
   `config/pipelines/<PIPELINE_ID>.yaml` with correct relative paths, then
   **validates** the manifest via `PipelineManifest.load`.
5. **Prints** the manifest ready to paste into **UI → Pipelines → Onboard**.

```powershell
dqctl bootstrap <source.csv|source.parquet> [options]
```

| Option | Default | Purpose |
|---|---|---|
| `--dataset` | source file stem | Dataset id used in ruleset/manifest |
| `--pipeline-id` | `<DATASET>_PIPELINE` | Pipeline id (validated against the manifest schema) |
| `--pipeline-name` | title-cased id | Display name |
| `--platform` | `DATABRICKS` | `DATABRICKS`, `STANDALONE_SPARK`, `DATAPROC`, `FABRIC`, `AIRFLOW`, `ADF`, `PYTHON`, `OTHER` |
| `--runtime` | `PYSPARK` | `PYSPARK`, `SQL`, `PYTHON`, `STREAMING` |
| `--stage` | `SILVER` | Stage the generated rule set applies to |
| `--dq-mode` | `OBSERVE_ONLY` | Initial enforcement mode — promote to `ENFORCE`/`REMEDIATE` after review |
| `--policy` | `../policies/default.yaml` | Policy reference written into the manifest (e.g. `../policies/healthcare.yaml` for PHI gating) |
| `--owner` / `--description` | empty / auto | Manifest metadata |
| `--approved-by` | `dqctl-bootstrap` | Steward identity recorded on the generated rules |
| `--copy` | off | Copy the source into `sample_data/bronze/` and reference it there (portable manifest) |
| `--out-dir` | `config` | Config root that receives `rulesets/` + `pipelines/` |
| `--profile-out` | off | Also write the JSON profile to a file |
| `--publish` | off | Publish + activate the generated rule set into the registry |
| `--onboard` | off | Register the pipeline in the registry (no UI paste needed) |
| `--force` | off | Overwrite previously generated files |

Generated rule sets are **DRAFT** — they must be published before
`ENFORCE`/`REMEDIATE` (`--publish` does this in the same run, or
`dqctl ruleset publish <yaml> --activate` later). Review the printed
confidence column: rules generated from weak evidence land at 0.6 and deserve a
human eye before production use.

Full example — generate, publish, onboard, then run:

```powershell
& $VenvPython -m dq_core.cli.main bootstrap data\orders.csv --dataset orders `
    --pipeline-id ORDERS_PIPELINE --owner "orders-team@example.com" `
    --copy --publish --onboard
& $VenvPython -m dq_core.cli.main pipeline test     ORDERS_PIPELINE
& $VenvPython -m dq_core.cli.main pipeline activate ORDERS_PIPELINE
& $VenvPython -m dq_core.cli.main run sample_data\bronze\orders.csv `
    --ruleset config\rulesets\orders_v1.yaml --mode OBSERVE_ONLY --dataset orders
```

### 6.4 Engine API from Python (the real integration pattern)

```python
from dq_core import DataQualityEngine, PipelineContext

engine  = DataQualityEngine.from_config("config/engine.yaml")
context = PipelineContext(
    pipeline_id="DATABRICKS_CUSTOMER_PIPELINE",
    pipeline_run_id="RUN_001",
    platform="DATABRICKS", runtime="PYSPARK",
    dataset="customers", stage="SILVER",
    integration_mode="REMEDIATE",
)
result = engine.run(
    dataframe=customers_df,                       # YOUR existing DataFrame
    ruleset="config/rulesets/customers_v1.yaml",  # or "customers_silver@active"
    policy="config/policies/default.yaml",
    context=context,
)
accepted_df   = result.accepted_dataframe        # continue pipeline with this
quarantine_df = result.quarantine_dataframe      # write to quarantine
result.write_report("reports/run.json")
print(result.summary_text())
```

The engine **never creates or stops** a SparkSession — it uses
`dataframe.sparkSession`, so the same call works on Databricks, Dataproc,
Fabric, or local Spark.

### 6.5 Databricks integration (pattern — needs a real workspace to execute)

`integrations\databricks\customer_pipeline_dq_integrated.py` — copy into a
notebook/job, attach the `dq-core` wheel, point `DQ_CONFIG_PATH` at
`config\engine.databricks.yaml` on a Volume/`dbfs:` path. ~6 added lines marked
`«DQ INTEGRATION»`. See `docs\databricks_onboarding.md`.

### 6.5b Web UI (React + FastAPI)

```powershell
.\start_ui.bat   # starts FastAPI :8000 + Vite :5173, opens browser
```

Or manually: `& $VenvPython -m uvicorn main:app --app-dir ui\backend --port 8000` then
`cd ui\frontend && npm run dev`. Pages: Dashboard, Pipelines (onboard + lifecycle),
Run DQ (submit + live job progress), Rule Sets (publish/diff/activate), Reports.
API contract: `ui\backend\main.py` — also callable by orchestrators. Jobs persist to
`data\jobs.db` (SQLite); real-time progress via `GET /api/jobs/{id}/stream` (SSE).

**Registry backend**: `engine.yaml registry.backend` — `file` (default, JSON under
`registry\`) or `sqlite` (SQL store at `registry.db_path`; `SqlRuleSetRegistry` /
`SqlPipelineRegistry` — same interface, production-shaped for multi-user).

### 6.6 Regenerate data / check runtime

```powershell
& $VenvPython scripts\generate_sample_data.py   # rebuild sample_data\
& $VenvPython scripts\check_runtime.py          # Spark/Java/parquet self-check
```

---

## 7. Where outputs land

| Path | Contents |
|---|---|
| `data\bronze\` | raw input preserved verbatim |
| `data\silver\` | accepted (clean + remediated) records |
| `data\gold\` | Gold-stage accepted records |
| `data\quarantine\silver|gold\` | quarantined rows with `dq_*` audit columns |
| `data\audit\` | remediation before→after log |
| `reports\` | `DQ_EXECUTION_REPORT` JSON per run + profile JSON |
| `registry\rulesets\`, `registry\pipelines\` | published rule versions, pipeline state |

---

## 8. Documentation map (`docs\`)

| File | Read it for |
|---|---|
| `architecture.md` | HLD, component map, Mermaid diagrams |
| `spark_core_engine.md` | LLD — every module explained |
| `databricks_pipeline_assessment.md` | what was supplied vs. synthetic, integration points |
| `databricks_onboarding.md` | step-by-step real-workspace onboarding |
| `profiling.md` | profiling modes & metrics |
| `rule_management.md` | RuleSet versioning, immutability, diff/rollback |
| `remediation.md` | deterministic + mock-AI remediation flows |
| `confidence_policy.md` | 90 % threshold, sensitive columns, approvals |
| `quarantine.md` | quarantine schema & status taxonomy |
| `medallion_data_flow.md` | Bronze/Silver/Gold execution |
| `pipeline_adapters.md` | portability matrix (Dataproc/Fabric/Airflow/ADF/…) |
| `testing_report.md` | executed evidence — 130 tests, CLI outputs, defect log |
| `poc_demo_script.md` | 21-step narrated demo with expected output |

---

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| `Spark home not found` / `HADOOP_HOME unset` | run `. .\scripts\env.ps1` first; it sets `PYSPARK_PYTHON`/`DQ_SCRATCH_DIR` |
| Parquet write fails on Windows | `check_runtime.py` rebuilds the shim jar under `.tmp\hadoop-shim\` |
| `No space left on device` | you're on C: — always dot-source `env.ps1` so TEMP/cache stay on D: |
| `… is not published` in ENFORCE/REMEDIATE | `dqctl ruleset publish <yaml> --activate` first |
| `Invalid ruleset_id` | ids must match `[a-z][a-z0-9_]*` (min length) — don't use `"t"` |
| Tests ERROR `PATH_NOT_FOUND …sample_data` | `conftest.py` root is fixed; re-pull / don't move the repo folder |
| PowerShell shows `NativeCommandError` on stderr | harmless — Spark writes warnings to stderr; check exit code/output |

---

## 10. Verified status snapshot

- **Tests:** `130 passed` (unit + Spark integration) — `docs\testing_report.md`
- **CLI:** pipeline lifecycle, ruleset publish/diff/activate/rollback/verify,
  `run`, `profile` — all executed with real output
- **Standalone example:** executed — 24 accepted / 13 quarantined / gate PASSED
- **Expected-results conformance:** every per-record outcome in
  `sample_data\expected_results.json` reproduced by the engine
- **Not executed:** real Databricks workspace, real LLM, production
  orchestrators — documented as contracts/future work
