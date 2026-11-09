# DQ Accelerator — Databricks-first, portable Apache Spark POC

A working Proof of Concept for an **Enterprise Data Quality Accelerator**: a reusable
Apache Spark (PySpark) core engine that plugs into existing Databricks-style pipelines
with a handful of lines, profiles data, generates candidate rules, executes
metadata-driven validation, remediates deterministically and via **real LLM calls
(Databricks Model Serving)** or a mock AI fallback, quarantines uncertain records,
revalidates corrections, and drives a **Bronze → Silver → Gold** medallion flow with
quality gates, metrics and audit reports.

> **Status: working local POC with real Databricks LLM integration.** The engine has
> been verified on a local Apache Spark 4.1.3 / Java 21 / Python 3.12 harness on
> Windows 11, including a **1 GB / 6.3M-row CDR dataset** processed end-to-end.
> **Real Databricks Model Serving LLM calls are working** — DQXProvider uses LiteLLM
> to call `databricks-meta-llama-3-3-70b-instruct` for value correction with confidence
> scoring. MockAI remains as automatic fallback when credentials are absent.
> See `docs/testing_report.md` for exactly what was verified and how.

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

```powershell
# 1. New machine: run the automated setup (Python 3.12 + JDK 21 required)
.\setup_new_machine.bat

# 2. Or manual setup (if you prefer):
.\scripts\setup.ps1        # creates .venv, redirects TEMP/TMP/pip cache to .tmp on D:
. .\scripts\env.ps1         # activate env for the session

# 3. Verify the Spark runtime (Python / Spark / Java / Parquet round-trip)
& $VenvPython scripts\check_runtime.py

# 4. Regenerate the deterministic synthetic data if needed
& $VenvPython scripts\generate_sample_data.py

# 5. Start the full application (backend + frontend)
.\start_ui.bat

# 6. Run the test suite (~8 min; starts a local Spark session)
& $VenvPython -m pytest packages\dq-core\tests\ -v
```

**Optional: Enable real Databricks LLM remediation**

```powershell
# Set your Databricks credentials (get PAT from workspace → User Settings → Developer)
set DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
set DATABRICKS_TOKEN=your-pat-token
set DQX_MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct

# Edit config/engine.yaml → ai.provider: dqx (already done by default)
# Restart backend → Settings page shows green "Connected to Databricks Model Serving"
```

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

**Start here: [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md)** — one document with
setup, the full annotated project structure, what the project is, and how to run
everything (tests, CLI, examples, API).

| Document | Contents |
|---|---|
| `docs/PROJECT_GUIDE.md` | **Master guide: setup + structure + how to run** |
| `docs/architecture.md` | HLD, component map, Mermaid diagrams |
| `docs/databricks_pipeline_assessment.md` | Assessment of the supplied/synthetic pipeline |
| `docs/databricks_onboarding.md` | Step-by-step Databricks onboarding |
| `docs/spark_core_engine.md` | LLD: module-by-module engine internals |
| `docs/profiling.md` | Profiling modes, metrics, anomalies |
| `docs/rule_management.md` | RuleSet versioning, registry, immutability |
| `docs/remediation.md` | Deterministic + mock-AI remediation flows |
| `docs/confidence_policy.md` | 90 % threshold, sensitive columns, approvals |
| `docs/quarantine.md` | Quarantine schema and revalidation |
| `docs/medallion_data_flow.md` | Bronze/Silver/Gold execution |
| `docs/pipeline_adapters.md` | Portability matrix and adapter contracts |
| `docs/testing_report.md` | Executed tests and evidence |
| `docs/poc_demo_script.md` | 21-step repeatable demo with expected output |

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
