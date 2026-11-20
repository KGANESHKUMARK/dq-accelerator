# Pipeline Onboarding — Integrating DQ Accelerator into Existing Cluster Pipelines

> **For teams with existing Databricks / EMR / Fabric / Dataproc pipelines that are
> already running in production.** This guide shows how to add DQ Accelerator
> without replacing, removing, or rewriting your existing Spark code.

---

## 1. The Core Idea: Insert, Don't Replace

The DQ Accelerator engine accepts your **existing Spark DataFrame** and returns
an **accepted DataFrame** (plus quarantine). Your pipeline continues exactly as
before — you just swap in the DQ-validated frame where you used to write the
raw/transformed frame.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  YOUR EXISTING PIPELINE (unchanged)                                     │
│  read → transform → write                                               │
│                        │                                                │
│                        ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  DQ ACCELERATOR INSERTION POINT (3 lines)                       │   │
│  │  engine.run(existing_df, ruleset, policy, context) → DQResult   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                        │                                                │
│    accepted_df ──────┴──────────► your existing downstream code         │
│    quarantine_df ────────────────► write to quarantine path (new)       │
│                        │                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  reports, metrics, quality gate (auto-generated)                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

**What stays the same:** Your read, your transformations, your write path, your
downstream consumers, your cluster, your SparkSession.

**What changes:** You add one `engine.run()` call between transform and write,
and you write the DQ-accepted frame instead of the raw transformed frame.

---

## 2. Before / After — Databricks Notebook

### Before (your existing pipeline, unchanged)

```python
# customer_pipeline_original.py
SOURCE_PATH  = "dbfs:/mnt/raw/customers/"
BRONZE_PATH  = "dbfs:/mnt/bronze/customers/"
SILVER_PATH  = "dbfs:/mnt/silver/customers/"

customers_raw = spark.read.parquet(SOURCE_PATH)
customers_raw.write.mode("overwrite").parquet(BRONZE_PATH)

from pyspark.sql import functions as F

customers_silver = (
    customers_raw
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email",         F.trim(F.lower(F.col("email"))))
    .withColumn("country",       F.trim(F.upper(F.col("country"))))
    .filter(F.col("customer_id").isNotNull())
)

customers_silver.write.mode("overwrite").parquet(SILVER_PATH)

# downstream
display(customers_silver)
```

### After (DQ Accelerator integrated — 3 lines added)

```python
# customer_pipeline_dq_integrated.py
SOURCE_PATH     = "dbfs:/mnt/raw/customers/"
BRONZE_PATH     = "dbfs:/mnt/bronze/customers/"
SILVER_PATH     = "dbfs:/mnt/silver/customers/"
QUARANTINE_PATH = "dbfs:/mnt/quarantine/silver/customers/"
DQ_CONFIG_PATH  = "/Volumes/main/dq_accelerator/config/engine.databricks.yaml"
RULESET_REF     = "customers_silver@active"
POLICY_PATH     = "/Volumes/main/dq_accelerator/config/policies/default.yaml"
PIPELINE_ID     = "DATABRICKS_CUSTOMER_PIPELINE"
PIPELINE_RUN_ID = dbutils.notebook.entry_point.getCurrentBindings().get("run_id", "MANUAL_001")

# «DQ INTEGRATION» — import (1 line)
from dq_core import DataQualityEngine, PipelineContext

# ----- BRONZE: read raw data (unchanged) -----------------------------------
customers_raw = spark.read.parquet(SOURCE_PATH)
customers_raw.write.mode("overwrite").parquet(BRONZE_PATH)

# ----- SILVER: existing transformations (unchanged) ------------------------
from pyspark.sql import functions as F

customers_transformed = (
    customers_raw
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email",         F.trim(F.lower(F.col("email"))))
    .withColumn("country",       F.trim(F.upper(F.col("country"))))
    .filter(F.col("customer_id").isNotNull())
)

# «DQ INTEGRATION» — init engine + context + run (3 lines)
engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)
context = PipelineContext(
    pipeline_id=PIPELINE_ID,
    pipeline_run_id=PIPELINE_RUN_ID,
    platform="DATABRICKS",
    runtime="PYSPARK",
    dataset="customers",
    stage="SILVER",
    ruleset_version="1.0",
    integration_mode="REMEDIATE",
)
dq_result = engine.run(
    dataframe=customers_transformed,   # ← your EXISTING DataFrame, unchanged
    ruleset=RULESET_REF,
    policy=POLICY_PATH,
    context=context,
)

# «DQ INTEGRATION» — use DQ-accepted frame (1 line change)
customers_silver = dq_result.accepted_dataframe

# «DQ INTEGRATION» — write quarantine (new, alongside existing)
dq_result.quarantine_dataframe.write.mode("overwrite").parquet(QUARANTINE_PATH)

# Write Silver (original path, now DQ-validated)
customers_silver.write.mode("overwrite").parquet(SILVER_PATH)

# Optional: fail the job if quality gate is breached
# dq_result.raise_if_gate_failed()

# downstream (unchanged)
display(customers_silver)
print(dq_result.summary_text())
```

### What changed vs. what didn't

| Aspect | Before | After |
|--------|--------|-------|
| Read source | `spark.read.parquet(...)` | **unchanged** |
| Transformations | trim/lower/upper/filter | **unchanged** |
| Write path | `write.mode("overwrite").parquet(SILVER_PATH)` | **unchanged** (but now writes DQ-accepted frame) |
| Downstream code | `display(customers_silver)` | **unchanged** |
| New import | — | `from dq_core import DataQualityEngine, PipelineContext` |
| New call | — | `engine.run(dataframe=..., ruleset=..., policy=..., context=...)` |
| New output | — | `dq_result.quarantine_dataframe` + `reports/*.json` |
| Cluster / SparkSession | your existing cluster | **unchanged** (engine uses `df.sparkSession`) |

---

## 3. Onboarding Steps (Practical)

### Step 1: Install dq-core on your cluster

**Databricks:**
```bash
# Install the wheel as a cluster library or via init script
%pip install /Volumes/main/dq_accelerator/libs/dq_core-0.1.0-py3-none-any.whl
```

**EMR / Dataproc / Fabric:**
```bash
# Install on cluster nodes or via bootstrap action
pip install /path/to/dq_core-0.1.0-py3-none-any.whl
```

The wheel has minimal dependencies (PyYAML, click) and **zero Databricks imports** — it runs on any PySpark 3.x/4.x cluster.

### Step 2: Prepare config and ruleset

Copy the config template and adapt paths for your platform:

```yaml
# config/engine.databricks.yaml (example)
registry:
  backend: file
  rulesets_path: /Volumes/main/dq_accelerator/registry/rulesets
  pipelines_path: /Volumes/main/dq_accelerator/registry/pipelines

profiling:
  mode: APPROXIMATE
  sample_fraction: 0.30
  limit_rows: 10000

validation:
  default_mode: REMEDIATE
  confidence_threshold: 0.90

ai:
  provider: dqx          # or "mock" for testing
  dqx_model_endpoint: databricks/your-model-serving-endpoint

logging:
  level: INFO
  format: json
```

Create or publish a ruleset for your dataset:

```bash
# Via CLI (run on your cluster or locally with the wheel)
dqctl bootstrap /path/to/sample_data.csv \
    --publish \
    --onboard \
    --pipeline-id CUSTOMER_PIPELINE \
    --platform DATABRICKS
```

This generates:
- `config/rulesets/customers_silver_v1.yaml` — candidate rules from profiling
- `config/pipelines/CUSTOMER_PIPELINE.yaml` — pipeline manifest

Then a data steward reviews/approves rules in the UI (or via CLI `dqctl ruleset activate customers_silver@1.0`).

### Step 3: Add the 3-line integration to your notebook/job

Insert `engine.run()` between your transform and write (see Section 2).

### Step 4: Verify and promote

- Run the pipeline on a sample/subset first
- Check `reports/*.json` for quality gate pass/fail
- Review quarantine rows in the UI or via `GET /api/quarantine/pending`
- Promote the ruleset from `DRAFT` → `REGISTERED` → `TESTED` → `ACTIVE`
- Once stable, enable `dq_result.raise_if_gate_failed()` to block bad data

---

## 4. Platform-Specific Notes

| Platform | What changes | What stays the same |
|----------|-------------|---------------------|
| **Databricks** | Config paths use `dbfs:/` or `/Volumes/...`; `spark` and `display` are built-ins | Engine code identical; `df.sparkSession` works |
| **AWS EMR** | Config paths use `s3a://...`; cluster has Spark pre-installed | Engine code identical |
| **Microsoft Fabric** | Config paths use Lakehouse/OneLake paths | Engine code identical |
| **Google Dataproc** | Config paths use `gs://...`; cluster has Spark pre-installed | Engine code identical |
| **Standalone Spark** | Config paths use local/`file://` paths | Engine code identical |

The `PlatformAdapter` contract (in `integrations/contracts/adapter_contract.py`) ensures the same `DQResult` is returned regardless of platform:

```python
class PlatformAdapter(ABC):
    platform_name: str
    def execute(self, context, ruleset, policy, mode, **kwargs) -> DQResult: ...
```

If you want a thin adapter wrapper for your orchestration layer (Airflow, ADF, etc.), subclass `PlatformAdapter` and delegate to `engine.run()`.

---

## 5. Modes — Choose Based on Your Risk Tolerance

| Mode | When to use | Behavior |
|------|-------------|----------|
| `OBSERVE_ONLY` | First run / cautious onboarding | All rows pass through; violations flagged in report only; no data modified |
| `ENFORCE` | You want hard blocking | Failing rows → quarantine; clean rows → accepted; no remediation attempted |
| `REMEDIATE` | You want automatic fixes | Deterministic fixes first → AI proposals → confidence gate → revalidate → quarantine unresolved |

**Recommended onboarding path:**
1. Week 1: `OBSERVE_ONLY` — see what would fail without blocking anything
2. Week 2: `ENFORCE` — quarantine failures, review in UI
3. Week 3: `REMEDIATE` — auto-fix what's safe, quarantine the rest

---

## 6. What the Existing Team Needs to Know

| Question | Answer |
|----------|--------|
| Do we rewrite our pipeline? | **No.** 3-line insertion only. |
| Do we change our cluster? | **No.** Same Spark cluster, same session. |
| Do we change downstream consumers? | **No.** They receive the same DataFrame type (DQ-accepted). |
| Do we lose data on failures? | **No.** Failed rows go to quarantine with full audit columns, not dropped. |
| Can we roll back? | **Yes.** Rulesets are versioned and immutable; remove the 3 lines and you're back to original. |
| What if DQ engine fails? | Wrap `engine.run()` in try/except and fall back to writing the original transformed frame (your choice). |
| Do we need Databricks credentials? | Only if using the real LLM (DQX provider). Otherwise MockAI works for testing. |

---

## 7. Minimal Production-Ready Pattern

```python
from dq_core import DataQualityEngine, PipelineContext

# 1. Init once (outside the hot path if possible)
engine = DataQualityEngine.from_config(DQ_CONFIG_PATH)

# 2. Your existing transform
df_transformed = your_existing_transform(df_raw)

# 3. DQ validation
context = PipelineContext(
    pipeline_id="YOUR_PIPELINE",
    pipeline_run_id="RUN_123",
    platform="DATABRICKS",  # or EMR / FABRIC / DATAPROC / LOCAL
    runtime="PYSPARK",
    dataset="your_dataset",
    stage="SILVER",
    integration_mode="REMEDIATE",
)
dq_result = engine.run(
    dataframe=df_transformed,
    ruleset="your_ruleset@active",
    policy="/path/to/policy.yaml",
    context=context,
)

# 4. Use accepted, log quarantine
df_transformed = dq_result.accepted_dataframe
dq_result.quarantine_dataframe.write.mode("append").parquet(QUARANTINE_PATH)

# 5. Optional gate check
if not dq_result.quality_gate.gate_passed:
    raise QualityGateFailedError(dq_result.quality_gate.reason)
```

---

## 8. Files in This Repo (Reference)

| File | Purpose |
|------|---------|
| `integrations/databricks/customer_pipeline_original.py` | Before state (no DQ) |
| `integrations/databricks/customer_pipeline_dq_integrated.py` | After state (DQ integrated) |
| `integrations/emr/loans_pipeline_original.py` | Before state (EMR) |
| `integrations/emr/loans_pipeline_dq_integrated.py` | After state (EMR) |
| `integrations/fabric/orders_pipeline_original.py` | Before state (Fabric) |
| `integrations/fabric/orders_pipeline_dq_integrated.py` | After state (Fabric) |
| `integrations/contracts/adapter_contract.py` | PlatformAdapter ABC |
| `packages/dq-core/src/dq_core/engine.py` | `DataQualityEngine.run()` — the entry point |
| `packages/dq-core/src/dq_core/context/__init__.py` | `PipelineContext`, `IntegrationMode` |
| `packages/dq-core/src/dq_core/orchestration/result.py` | `DQResult` — the return contract |

---

## 9. Honest Boundary

The engine and adapters are **contract-proven for Databricks / EMR / Fabric / Dataproc** — the same `engine.run()` call works on all. A **real cluster execution** on your specific Databricks/EMR/Fabric workspace is the one open item: it needs your workspace credentials and cluster access to execute. The integration pattern (3-line insertion) is identical; only config paths differ.

---

**Branch:** tracked via git. The presenting developer should use their own repo state.