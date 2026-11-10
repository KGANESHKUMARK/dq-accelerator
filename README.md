# DQ Accelerator — Databricks-first, portable Apache Spark POC

A working Proof of Concept for an **Enterprise Data Quality Accelerator**: a reusable
Apache Spark (PySpark) core engine that plugs into existing Databricks-style pipelines
with a handful of lines, profiles data, generates candidate rules, executes
metadata-driven validation, remediates deterministically and via **real LLM calls
(Databricks Model Serving)** or a mock AI fallback, quarantines uncertain records,
revalidates corrections, and supports a **Bronze → Silver → Gold** medallion flow with
quality gates, metrics and audit reports.

> **Status: working local POC with real Databricks LLM integration.** The engine has
> been verified on a local Apache Spark 4.1.3 / Java 21 / Python 3.12 harness on
> Windows 11, including a **1 GB / 6.3M-row CDR dataset** processed end-to-end.
> **Real Databricks Model Serving LLM calls are working** — DQXProvider uses LiteLLM
> to call `databricks-meta-llama-3-3-70b-instruct` for value correction with confidence
> scoring. MockAI remains as automatic fallback when credentials are absent.
> See `docs/testing_report.md` for exactly what was verified and how.
>
> **Current development boundary:** `docs/POC_CONTEXT.md`. Remaining POC work
> is P0–P4 in `ROADMAP.md`; production orchestration remains deferred.

## What is in the box

| Component | Location | Status |
|---|---|---|
| Reusable `dq-core` engine (`DataQualityEngine`) | `packages/dq-core/src/dq_core/` | Implemented & tested |
| Profiling (full / sample / approximate) | `dq_core.profiling` | Implemented & tested |
| Candidate rule generation | `dq_core.generation` | Implemented & tested |
| RuleSet models, versioning, registry, diff, rollback | `dq_core.metadata`, `dq_core.rules` | Implemented & tested |
| Native PySpark validation engine | `dq_core.validation` | Implemented & tested |
| Deterministic remediation | `dq_core.remediation.deterministic` | Implemented & tested |
| **Real LLM remediation (Databricks Model Serving)** | `dq_core.ai.dqx_provider` | **Implemented & verified** |
| Mock AI fallback + confidence policy (90 % default) | `dq_core.ai`, `dq_core.policies` | Implemented & tested |
| Quarantine + post-correction revalidation | `dq_core.quarantine`, `dq_core.revalidation` | Implemented & tested |
| Bronze/Silver/Gold medallion runner | `dq_core.orchestration.medallion` | Implemented & tested |
| Pipeline lifecycle registry (onboard→offboard) | `dq_core.pipelines` | Implemented & tested |
| `dqctl` CLI | `dq_core.cli.main` | Implemented & verified |
| **React + FastAPI UI** (dashboard, anomalies, approvals, trends) | `ui/` | Implemented & verified |
| Databricks PySpark integration example | `integrations/databricks/` | Runnable script, **not executed on Databricks** |
| Portability adapter contracts | `integrations/contracts/` | Contract-tested |
| Synthetic sample data (banking, healthcare, telecom, retail, finance) | `sample_data/` | Deterministic, includes 1GB CDR |
| Automated test suite | `packages/dq-core/tests/` | 130 tests (65 unit + 65 integration) |

## Quick start (Windows / PowerShell)

All artefacts stay on the **D: drive inside this project** — virtualenv, pip cache,
Spark scratch and temp files. Nothing is written to C:.

### One file: `DQ_start.bat`

Double-click **`DQ_start.bat`** — it checks Python 3.12 / JDK 21 / Node 18+
(prints download links if missing), creates `.venv`, installs
`requirements.txt` (+ `requirements-llm.txt` when `DATABRICKS_TOKEN` is set),
runs `npm install` + build, then launches backend (`:8000`) + frontend (`:5173`)
in separate windows and opens the browser.

```powershell
.\DQ_start.bat          # daily use + first-time setup
```

### Manual steps (advanced)

```powershell
.\scripts\setup.ps1     # creates .venv, redirects TEMP/TMP/pip cache to .tmp on D:
. .\scripts\env.ps1      # activate env for the session
& $VenvPython scripts\check_runtime.py    # verify Spark runtime
& $VenvPython scripts\generate_sample_data.py   # regenerate synthetic data
.\archive\start_ui.bat                    # legacy launcher (old; use DQ_start.bat)
& $VenvPython -m pytest packages\dq-core\tests\ -v   # test suite (~8 min)
```

**Legacy launchers** live in `archive/` — `DQ_start.bat` is the only entry point.

**Optional: Enable real Databricks LLM remediation**

Set env vars **before** running `DQ_start.bat` (System Properties → Environment
Variables, or `$env:` in PowerShell). When `DATABRICKS_TOKEN` is present the
launcher auto-installs `requirements-llm.txt`:

```
DATABRICKS_HOST   = https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN  = <personal-access-token>
DQX_MODEL_ENDPOINT= databricks-meta-llama-3-3-70b-instruct
```

Without credentials the app runs MockAI (deterministic YAML lookup, no external
calls). `config/engine.yaml` → `ai.provider: dqx` is the default; DQXProvider
auto-falls back to MockAI on failure.

## UI features

- **Dashboard** — real report-derived metrics, per-pipeline trend cards
  (clickable → opens pipeline detail), run timeline, acceptance rates
- **Pipelines** — registry + lifecycle (test → activate → pause → offboard),
  per-pipeline trend sparkline, executions, lifecycle history
- **Onboard** — 5-step wizard: upload → profile → review rules → publish →
  pipeline register (no YAML hand-writing)
- **Run DQ** — live progress panel with phase chips (Queued → Spark →
  Validating → AI remediation → Revalidating → Report)
- **Anomalies / Approvals** — lifecycle inbox + quarantine review
- **Settings** — system status (Python/Java/Node/packages/env vars),
  **Apply config** hot-reload buttons (engine.yaml changes without restart),
  AI provider status, **DQ Assistant** toggle + chat log
- **DQ Assistant** — Genie-style chatbot (floating panel) that answers about
  onboarded pipelines, rulesets, jobs, anomalies, reports, setup, and roadmap;
  uses real Databricks LLM when credentials are set, built-in answers otherwise

## Minimal integration example

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
    dataframe=customers_df,                    # your EXISTING DataFrame
    ruleset="config/rulesets/customers_v1.yaml",
    policy="config/policies/default.yaml",
    context=context,
)
accepted_df   = result.accepted_dataframe      # continue your pipeline with this
quarantine_df = result.quarantine_dataframe    # uncertain / unresolved records
print(result.summary_text())
```

The engine never creates or stops a `SparkSession` — it always uses the session
attached to the input DataFrame, so it works unchanged on Databricks, Dataproc,
Microsoft Fabric Spark, or a standalone local Spark app.

## CLI (`dqctl`)

```powershell
# one-command onboarding: profile a source file -> approved ruleset + manifest
dqctl bootstrap data/new_source.csv --dataset orders --pipeline-id ORDERS_PIPELINE \
    --copy --publish --onboard          # see docs/PROJECT_GUIDE.md §6.3.1 for all flags

dqctl pipeline onboard --manifest config/pipelines/customer_pipeline.yaml
dqctl pipeline list
dqctl pipeline test|activate|pause|offboard <PIPELINE_ID>

dqctl ruleset publish config/rulesets/customers_v1.yaml
dqctl ruleset list
dqctl ruleset diff customers_silver 1.0 1.1
dqctl ruleset activate|rollback|verify ...

dqctl profile sample_data/bronze/customers.csv --dataset customers
dqctl run sample_data/bronze/customers.csv --ruleset config/rulesets/customers_v1.yaml `
       --policy config/policies/default.yaml --mode REMEDIATE `
       --report reports/run.json --out data/silver/customers
```

## Documentation

**Which doc for what** — pick by your role:

### Start here

| Document | Read it if you… |
|---|---|
| [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md) | Want the master guide: what it is, setup, full structure, how to run everything |
| [`docs/SETUP.md`](docs/SETUP.md) | Are setting up on a fresh machine (`DQ_start.bat`, prerequisites, `.env`) |
| [`docs/end_to_end_demo.md`](docs/end_to_end_demo.md) | Are about to present — fresh machine → live demo runbook in order |
| [`docs/ui_demo_guide.md`](docs/ui_demo_guide.md) | Are demoing the UI to a client — tab-by-tab walkthrough |

### Developers

| Document | Read it if you… |
|---|---|
| [`docs/DEVELOPER_GUIDE.md`](docs/DEVELOPER_GUIDE.md) | Are joining the project — build phases, verification, release process, extension points |
| [`docs/architecture.md`](docs/architecture.md) | Need the architecture: HLD/LLD diagrams, component map, status, failure model |
| [`docs/spark_core_engine.md`](docs/spark_core_engine.md) | Are hacking on `dq-core` internals — module-by-module LLD |
| [`docs/testing_report.md`](docs/testing_report.md) | Want the executed-test evidence and defect log |
| [`docs/POC.md`](docs/POC.md) | Need the POC use case + P0–P4 verification status |
| [`docs/POC_CONTEXT.md`](docs/POC_CONTEXT.md) | Need authoritative POC guardrails and priorities |
| [`ROADMAP.md`](ROADMAP.md) | Want the forward plan (post-POC phases) |

### Feature deep-dives (engine internals)

| Document | Covers |
|---|---|
| [`docs/profiling.md`](docs/profiling.md) | Profiling modes (FULL/SAMPLE/APPROXIMATE), metrics, anomalies |
| [`docs/rule_management.md`](docs/rule_management.md) | RuleSet versioning, registry, publish immutability, diff/rollback |
| [`docs/remediation.md`](docs/remediation.md) | Deterministic + AI remediation flows, outcome taxonomy |
| [`docs/confidence_policy.md`](docs/confidence_policy.md) | 90% threshold, sensitive columns, approvals/rejections memory |
| [`docs/quarantine.md`](docs/quarantine.md) | Quarantine schema, statuses, revalidation |
| [`docs/medallion_data_flow.md`](docs/medallion_data_flow.md) | Bronze/Silver/Gold stage execution + physical persistence |
| [`docs/pipeline_adapters.md`](docs/pipeline_adapters.md) | Portability matrix + per-platform adapter contracts |

### Demo & assessment assets

| Document | Covers |
|---|---|
| [`docs/poc_demo_script.md`](docs/poc_demo_script.md) | 21-step CLI demo script with expected output |
| [`docs/presentation_deck.md`](docs/presentation_deck.md) | Slide-ready narrative for stakeholder presentations |
| [`docs/databricks_pipeline_assessment.md`](docs/databricks_pipeline_assessment.md) | Assessment of the supplied/synthetic pipeline assets |
| [`docs/databricks_onboarding.md`](docs/databricks_onboarding.md) | Step-by-step Databricks workspace onboarding pattern |

## Honest scope statement

- **Executed locally:** profiling, rule generation, validation, all three modes,
  deterministic + real-LLM remediation, quarantine/revalidation, medallion flow,
  metrics/reports, pipeline lifecycle, CLI — all on local Spark 4.1.3.
- **Real Databricks LLM:** DQXProvider uses LiteLLM to call `databricks-meta-llama-3-3-70b-instruct`
  on Databricks Model Serving for value correction. MockAI auto-fallback when credentials absent.
- **Large file support:** 1 GB / 6.3M-row CDR file processed end-to-end (auto-sampling,
  memory tuning via `config/engine.yaml` runtime section).
- **Full UI:** React 18 + FastAPI dashboard with real data — health scores, pipeline
  trends, anomaly lifecycle, approval inbox, governed Run DQ, onboarding wizard.
- **Not executed on Databricks cluster:** the engine runs on local Spark; the
  Databricks integration is a notebook-style script plus contract-tested adapter.
- **Not built (by design):** PostgreSQL governance backend, authentication/SSO,
  multi-tenancy, production Airflow/ADF/dbt/Flink connectors.
