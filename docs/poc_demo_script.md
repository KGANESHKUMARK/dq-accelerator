# POC Demo Script — 21 Steps

Repeatable demonstration of the DQ Accelerator. Run from the repo root in
PowerShell after `. .\scripts\env.ps1`. Every step lists the command and the
**expected** output (all outputs below were actually observed during
verification — see `docs/testing_report.md`).

> Narration tip: steps 1–9 prove "understand the data", steps 10–16 prove
> "control the data", steps 17–21 prove "govern the pipeline".

---

## STEP 1 — Show the existing pipeline

```powershell
Get-Content integrations\databricks\customer_pipeline_original.py
```

**Narration:** "This is the customer's existing Databricks-style pipeline —
read Parquet → light Silver transforms → write. No quality logic. We're not
rewriting it; we insert the engine at one point." **Expected:** Bronze read,
three `withColumn` transforms, `filter(customer_id.isNotNull())`, Silver write.
*(Synthetic demo pipeline — created for this POC.)*

## STEP 2 — Register the pipeline manifest

```powershell
& $VenvPython -m dq_core.cli.main pipeline onboard --manifest config\pipelines\customer_pipeline.yaml --actor demo
```

**Expected:**
```
OK  Onboarded DATABRICKS_CUSTOMER_PIPELINE  state=REGISTERED
```
Manifest declares platform=DATABRICKS, runtime=PYSPARK, datasets=customers
(BRONZE/SILVER/GOLD), rulesets per stage.

## STEP 3 — The few lines needed to integrate

```powershell
Get-Content integrations\databricks\customer_pipeline_dq_integrated.py
```

**Narration:** "Six added lines marked «DQ INTEGRATION»: one import, engine
init, a PipelineContext, `engine.run(existing_df, …)`, then the pipeline
continues with `accepted_dataframe`. Everything else is the original code."
**Expected:** original transforms untouched; `dq_result.accepted_dataframe`
replaces the write source.

## STEP 4 — Execute profiling on the existing DataFrame

```powershell
& $VenvPython -m dq_core.cli.main profile sample_data\bronze\customers.csv --dataset customers --out .tmp\demo_profile.json
```

**Expected:** `Profile: customers rows=38 columns=8 mode=FULL` + per-column
table + anomalies list (whitespace, age outlier 150, mixed date formats,
rare categories, string-typed numerics/dates). Spark startup ≈15 s, profile ≈25 s.

## STEP 5 — Inspect schema, nulls, duplicates, distributions, anomalies

Open `.tmp/demo_profile.json` (or read the console output from STEP 4).

**Narration:** "Profiler found: 1 duplicate row, 1% nulls in id/email/city,
age stored as string with a 150-year-old outlier, `created_date` mixing
`yyyy-MM-dd` and `dd/MM/yyyy`, country containing 'Atlantis'/'Bharat'/'India'."
**Expected:** `duplicate_rows=1`, `primary_key_candidates=[customer_id]`-ish,
anomaly types listed in `docs/profiling.md` §3.

## STEP 6 — Generate candidate rules

```powershell
& $VenvPython -c "from dq_core.engine import DataQualityEngine; from dq_core.runtime.config import EngineConfig; from dq_core.io.sources import CsvSource; e=DataQualityEngine(EngineConfig.from_yaml('config/engine.yaml')); s=e._spark if False else None" 2>$null; echo "(see test: test_candidate_rule_generation)"
```

Simpler — candidate generation is exercised by `test_profiling.py::test_candidate_rule_generation`
and unit tests; for a live look:

```powershell
& $VenvPython -m pytest packages\dq-core\tests\integration\test_profiling.py::test_candidate_rule_generation -v
```

**Expected:** `PASSED` — generates NOT_NULL/UNIQUE/REGEX/RANGE/ALLOWED_VALUES/
DATE_FORMAT/DATA_TYPE/STRING_LENGTH/DUPLICATE/SCHEMA_VALIDATION candidates,
all `status=CANDIDATE` with confidence + evidence.

## STEP 7 — Review & approve RuleSet V1.0

```powershell
& $VenvPython -m dq_core.cli.main ruleset publish config\rulesets\customers_v1.yaml --activate --actor demo
```

**Narration:** "V1.0 was steward-reviewed: 17 rules covering id/email/name/
age/city/country/status/date, duplicates, schema. Published = immutable +
checksum `51caa7545a08`. Candidates never auto-activate."
**Expected:** `OK customers_silver@1.0 checksum=51caa7545a08 activate=True`

## STEP 8 — Execute validation

```powershell
& $VenvPython -m dq_core.cli.main run sample_data\bronze\customers.csv --ruleset config\rulesets\customers_v1.yaml --policy config\policies\default.yaml --mode ENFORCE --dataset customers
```

**Expected:** `input=38 accepted=16 quarantined=22` (ENFORCE: no fixes, failing
rows split out). Duration ≈13 s.

## STEP 9 — Passed/failed counts and reasons

Same command output + `.tmp` report, or inspect quarantine frame columns:
`dq_failed_rule_ids`, `dq_failure_reasons`.

**Narration:** "22 records fail ≥1 rule — e.g. C0012 conflicting duplicate id,
C0023 missing email, C0028 bad date. Each row lists ALL its violations, not
just the first."

## STEP 10 — OBSERVE_ONLY mode

```powershell
& $VenvPython -m dq_core.cli.main run sample_data\bronze\customers.csv --ruleset config\rulesets\customers_v1.yaml --mode OBSERVE_ONLY --dataset customers --report .tmp\demo_observe.json
```

**Expected:** `accepted=38 quarantined=22` — **all 38 rows pass through**;
22 flagged `OBSERVED_ONLY`. Pipeline output unchanged. Gate fails on
`max_quarantine_rate` (22/38 > 40 %) — by design, it *reports* rather than blocks.

## STEP 11 — Deterministic remediation

Covered by `test_remediation.py` (5 tests) — or run REMEDIATE and inspect the
audit log:

```powershell
& $VenvPython -m dq_core.cli.main run sample_data\bronze\transactions.csv --ruleset config\rulesets\transactions_v1.yaml --policy config\policies\default.yaml --mode REMEDIATE --dataset transactions --report .tmp\demo_tx_remediate.json
```

**Expected:** `input=40 accepted=30 accepted_remediated=4 quarantined=9
removed_duplicates=1`, `deterministic_fixes` > 0. Audit rows show
before→after (trim, lookup `'Bharat'→'IN'`, date `15/01/2024→2024-01-15`).

## STEP 12 — MockAIProvider proposals

`config/ai/mock_ai_responses.yaml` + `test_remediation.py::test_mock_ai_produces_proposals`.

**Narration:** "Simulated AI proposals — record, rule, column, original →
proposed value, confidence, reason, evidence, provider, model version. Batched
by distinct value, never per-row, never executed code."
**Expected:** proposals for country/status/email cases incl. high (0.95+),
low (0.34/0.62/0.70) and invalid (`'IND'`) confidence fixtures.

## STEP 13 — Set threshold to 90 %

`config/policies/default.yaml` → `confidence.threshold: 0.90`.
Unit proof: `test_policies.py::TestConfidenceBoundaries` (5 boundary cases).

## STEP 14 — Eligible high-confidence correction

From the customers REMEDIATE run: C0021 (AI status @0.97 + date
standardisation) and C0022 (AI country @0.95) → `ACCEPTED_REMEDIATED`;
C0034 @**0.90 exactly** → applied (inclusive boundary).

## STEP 15 — Low-confidence → quarantine

C0029 @0.34, C0030 @0.62, C0035 @0.8999 → `QUARANTINED:BELOW_CONFIDENCE_THRESHOLD`.
Expected-results rows 30/31/36 confirm.

## STEP 16 — Post-remediation validation

`test_remediation.py::test_invalid_ai_correction_fails_revalidation` +
`DQResult.final_validation_results`.

**Narration:** "C0028: AI proposes 'IND' @0.91 — above threshold — but the
approved lookup wants ISO-2. Revalidation catches it →
`CORRECTION_FAILED_REVALIDATION`. High confidence ≠ correctness."

## STEP 17 — Silver write + Gold checks

Medallion tests: `test_medallion.py` (4 tests). Live equivalent — the
transaction pipeline manifest wires `transactions_gold_v1.yaml` at GOLD with
`reference_data={"customers": accepted_customers}`.

**Expected Gold results:** `accepted=26 quarantined=4 warnings=1
summary_customers=18 summary_total_amount=2519147.89`. Orphans →
`REFERENTIAL_INTEGRITY` quarantine; negative PURCHASE → `BUSINESS_RULE`
quarantine; negative REFUND → accepted (configured business rule, not blanket
rejection).

## STEP 18 — RuleSet V1.1 vs V1.0

```powershell
& $VenvPython -m dq_core.cli.main ruleset publish config\rulesets\customers_v1_1.yaml --no-activate --actor demo
& $VenvPython -m dq_core.cli.main ruleset diff customers_silver 1.0 1.1
& $VenvPython -m dq_core.cli.main ruleset activate customers_silver 1.1 --actor demo
& $VenvPython -m dq_core.cli.main ruleset rollback customers_silver 1.0 --reason "v1.1 review" --actor demo
```

**Expected diff:** `added=1 (+CUST_017) modified=3 (CUST_007 max 120→110,
CUST_010 +SUSPENDED, CUST_013 severity→ERROR) unchanged=13`.
**Effect:** C0026 (age 115) quarantined under v1.1, accepted under v1.0 —
`test_engine_modes.py::test_v1_1_comparison_with_v1_0` proves
`quarantine_v1.1 > quarantine_v1.0`.

## STEP 19 — Execution metrics & JSON report

```powershell
Get-Content .tmp\demo_observe.json | Select-Object -First 40
```

**Expected:** `report_type=DQ_EXECUTION_REPORT` with `execution_context`
(pipeline, ruleset id+version+checksum, policy, threshold 0.9, engine/runtime
versions), `validation_results` per-rule pass/fail, `execution_metrics`
(input/accepted/quarantined/duplicates/warnings/durations/phases),
`quality_gate` result+failed checks, `remediation_results` (deterministic/AI
counts, blocked-by-policy), profile summary.

## STEP 20 — Same engine API on a standalone local DataFrame

```powershell
& $VenvPython -m pytest packages\dq-core\tests\integration\test_portability_contracts.py -v
```

**Narration:** "Identical `engine.run(df,…)` call — the DataFrame's own
SparkSession is used; nothing Databricks-specific is imported. The whole suite
(130 tests) runs on this local Spark session."
**Expected:** `9 passed` incl. `DataprocAdapter`/`FabricSparkAdapter` contract
execution on real local DataFrames.

## STEP 21 — Pause & offboard without deleting data

```powershell
& $VenvPython -m dq_core.cli.main pipeline list
& $VenvPython -m dq_core.cli.main pipeline pause DATABRICKS_CUSTOMER_PIPELINE --reason demo --actor demo
& $VenvPython -m dq_core.cli.main pipeline offboard DATABRICKS_CUSTOMER_PIPELINE --reason demo --actor demo
& $VenvPython -m dq_core.cli.main pipeline list
```

**Expected:** states `ACTIVE→PAUSED→OFFBOARDED`; `sample_data/` and
`registry/` untouched; `test_pipeline_lifecycle_integration.py` proves a
PAUSED pipeline's `engine.run` returns `skipped=True` with data passed through.

---

## Quick full-demo alternative (one command per stage)

```powershell
# Medallion B/S/G in one shot (used by test_medallion.py):
& $VenvPython -m pytest packages\dq-core\tests\integration\test_medallion.py -v
```

## What this demo does NOT show

Real Databricks execution (no workspace available), real LLM calls (mock
provider, labelled simulated), UI, or production orchestrator integrations —
see `docs/pipeline_adapters.md` for the contract surface.
