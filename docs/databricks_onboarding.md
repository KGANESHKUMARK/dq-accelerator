# Databricks Onboarding Guide

How to onboard an **existing** Databricks PySpark pipeline into the DQ Accelerator
with minimal changes. This guide reflects what was built and locally verified; the
final "run inside a real workspace" step still requires Databricks access.

## 1. Prerequisites

- A Databricks workspace with a cluster/runtime that supports PySpark ≥3.5 (the POC
  was built against Spark 4.1.3 semantics; DBR 14+ / DBR 18 LTS are compatible).
- `dq-core` packaged as a wheel and attached to the cluster
  (Compute → Libraries → Install → upload wheel, or an init script /
  `%pip install /Volumes/.../dq_core-0.1.0-py3-none-any.whl`).
- A YAML config bundle (`engine.databricks.yaml` + `rulesets/` + `policies/` +
  `approvals/` + `ai/`) on a path the **driver** can read
  (Unity Catalog Volume, `dbfs:/` mount, or workspace files).
- Python ≥3.10 on the cluster (DBR 14+ ships 3.11/3.12).

## 2. Deployment layout on Databricks

```
/Volumes/<catalog>/<schema>/dq_accelerator/
├── config/
│   ├── engine.databricks.yaml      # paths point at dbfs:/Volumes, not local disk
│   ├── policies/default.yaml
│   ├── approvals/approved_corrections.yaml
│   ├── ai/mock_ai_responses.yaml
│   └── rulesets/                   # customers_v1.yaml, customers_v1_1.yaml, …
└── registry/                       # created on first publish (rulesets/, pipelines/)
```

`config/engine.databricks.yaml` differs from the local `engine.yaml` only in
`outputs.base_path` / `reports_path` (point them at `dbfs:/` or a Volume mount).
Everything else — profiling, validation, remediation, policies — is identical.

## 3. Onboard the pipeline (manifest)

### 3.0 Fast path — `dqctl bootstrap`

If you have a representative Bronze file (CSV/Parquet extract of the table),
one command produces the manifest **and** a draft ruleset:

```bash
dqctl bootstrap /dbfs/bronze/orders.csv --dataset orders \
    --pipeline-id ORDERS_PIPELINE --policy ../policies/default.yaml \
    --copy --publish --onboard
```

It profiles the file, generates candidate rules with confidence/evidence,
writes `config/rulesets/orders_v1.yaml` + `config/pipelines/ORDERS_PIPELINE.yaml`,
validates the manifest, and (with `--publish --onboard`) registers both assets.
Review the generated rules before promoting past `OBSERVE_ONLY`. Full flag
reference: `docs/PROJECT_GUIDE.md` §6.3.1.

For a UC/Hive table (no file), profile the DataFrame in a notebook via
`engine.profile(df, "orders")` + `CandidateRuleGenerator`, or export a Bronze
sample to CSV first and bootstrap from that.

### 3.1 Manual manifest

Create `config/pipelines/<name>.yaml`:

```yaml
pipeline:
  pipeline_id: DATABRICKS_CUSTOMER_PIPELINE
  pipeline_name: Customer Processing Pipeline
  platform: DATABRICKS
  runtime: PYSPARK
  integration_mode: INLINE
  dq_mode: OBSERVE_ONLY          # start observe-only, then promote
  owner: data-engineering@example.com
  datasets:
    - dataset_id: customers
      format: PARQUET
      stages: [BRONZE, SILVER, GOLD]
      rulesets:
        SILVER: ../rulesets/customers_v1.yaml
        GOLD:   ../rulesets/customer_summary_gold_v1.yaml
      test_data: ../../sample_data/bronze/customers.csv
```

Register it (from a machine with the repo, or via `dqctl` on the driver):

```bash
dqctl pipeline onboard --manifest config/pipelines/customer_pipeline.yaml
dqctl pipeline test     DATABRICKS_CUSTOMER_PIPELINE
dqctl pipeline activate DATABRICKS_CUSTOMER_PIPELINE
```

Lifecycle: `DRAFT → REGISTERED → TESTED → ACTIVE → PAUSED → OFFBOARDED`.
A `PAUSED`/`OFFBOARDED` pipeline passes data through untouched — offboarding never
deletes source data or execution history.

## 4. Integrate into the notebook (≈6 added lines)

See `integrations/databricks/customer_pipeline_dq_integrated.py`. Pattern:

```python
from dq_core import DataQualityEngine, PipelineContext      # 1

engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)     # 2
context = PipelineContext(                                  # 3
    pipeline_id=PIPELINE_ID, pipeline_run_id=RUN_ID,
    platform="DATABRICKS", runtime="PYSPARK",
    dataset="customers", stage="SILVER",
    integration_mode="OBSERVE_ONLY",                        # start here
)
dq_result = engine.run(                                     # 4
    dataframe=customers_silver,                             # existing DF
    ruleset="customers_silver@active",                      # registry ref
    policy=POLICY_PATH,
    context=context,
)
customers_silver = dq_result.accepted_dataframe            # 5 (only changed line)
dq_result.quarantine_dataframe.write.mode("overwrite").parquet(QUARANTINE_PATH)  # 6
```

Rollout order recommended by the POC:
1. `OBSERVE_ONLY` — metrics/reports only; pipeline output unchanged.
2. `ENFORCE` — clean rows continue, failing rows quarantined.
3. `REMEDIATE` — approved deterministic + eligible AI corrections, revalidated.

## 5. Publish rule sets before enforcing

`ENFORCE`/`REMEDIATE` require a **published, checksum-verified** rule set:

```bash
dqctl ruleset publish config/rulesets/customers_v1.yaml --activate
dqctl ruleset list
dqctl ruleset diff customers_silver 1.0 1.1      # review before activating 1.1
dqctl ruleset activate customers_silver 1.1
dqctl ruleset rollback customers_silver 1.0     # instant rollback = re-activate old version
```

Published versions are immutable: editing a published YAML and re-publishing the
same version fails the checksum check.

## 6. What to verify inside the workspace

- [ ] `dq-core` wheel installs on the cluster (`import dq_core; dq_core.__version__`).
- [ ] `engine.run(...)` returns `DQResult` with non-empty `accepted_dataframe`.
- [ ] `quarantine_dataframe` schema contains the `dq_*` audit columns.
- [ ] `reports/*.json` written to the configured reports path.
- [ ] Bronze path byte-identical before/after (engine never mutates source).
- [ ] `dqctl pipeline list` shows execution history recorded per run.
- [ ] Quality-gate failure behaviour (`raise_if_gate_failed`) as designed.

## 7. Known limitations on Databricks

- The local Windows Hadoop filesystem shim is **not needed** on Databricks/Linux —
  it is compiled only for the local harness.
- `MockAIProvider` reads `config/ai/mock_ai_responses.yaml`; swap `ai.provider`
  for a real provider implementation of `AIRemediationProvider` when ready —
  the engine contract is provider-agnostic.
- Pipeline registry is file-based (POC); a governance DB is a later phase.
