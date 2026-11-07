# Spark Core Engine — Low Level Design

Module-by-module internals of `dq-core`. Package root:
`packages/dq-core/src/dq_core/`.

## 1. Package map

```
dq_core/
├── engine.py               # DataQualityEngine facade (public API)
├── version.py              # package version
├── context/__init__.py     # IntegrationMode, Stage, PipelineContext, ExecutionContext
├── runtime/                # config, errors, logging, schema, inspection, compat, local_spark
│   └── resources/          # WindowsLocalFileSystem.java + compiled jar, log4j2 props
├── metadata/               # models.py (RuleDefinition/RuleSet), serialization.py (YAML/JSON)
├── rules/                  # registry.py (versioned store), diff.py (version comparison)
├── profiling/              # profiler.py, models.py, patterns.py
├── generation/             # generator.py (CandidateRuleGenerator, approve_candidates)
├── validation/             # engine.py, expressions.py, columns.py, models.py
├── remediation/            # deterministic.py, ai_remediation.py
├── ai/                     # base.py (AIRemediationProvider), mock.py (MockAIProvider)
├── policies/               # models.py, confidence.py, approvals.py
├── quarantine/             # builder.py (dq_* columns, status taxonomy)
├── revalidation/           # revalidator.py (post-correction validation)
├── orchestration/          # runner.py (DQRunner), medallion.py, result.py, gate.py
├── pipelines/              # manifest.py, registry.py (lifecycle states)
├── reporting/              # metrics.py (ExecutionMetrics)
├── io/                     # sources.py (CSV/Parquet), sinks.py (StorageSink/Paths)
└── cli/main.py             # dqctl
```

## 2. Public API surface

```python
from dq_core import DataQualityEngine, PipelineContext

engine = DataQualityEngine.from_config("config/engine.yaml")   # or DataQualityEngine(cfg)

engine.profile(df, "customers", mode="FULL"|"SAMPLE"|"APPROXIMATE") -> ProfileReport
engine.generate_candidate_rules(profile, related_profiles=..., stage=...) -> RuleSet (CANDIDATE)
engine.validate(df, ruleset, reference_data=...) -> ValidationOutcome
engine.run(df, ruleset, policy, context, mode=..., reference_data=..., profile=bool,
           confidence_threshold=...) -> DQResult
```

`ruleset`/`policy` accept: model object · YAML/JSON path · registry ref
`"customers_silver@active"` / `"customers_silver@1.1"`.

## 3. DataQualityEngine responsibilities (`engine.py`)

1. Load/normalize ruleset + policy.
2. Resolve integration mode (arg → context → policy → config default).
3. Verify ruleset immutability against the registry when configured
   (`enforce_published_immutability`); require published rulesets for
   ENFORCE/REMEDIATE (`require_published_for_enforcement`).
4. Pipeline lifecycle check: `PAUSED`/`OFFBOARDED` → return a **skipped**
   `DQResult` (data passes through; `skip_reason` recorded).
5. Delegate to `DQRunner`, then record execution history in the pipeline registry.

The engine **never creates or stops** a SparkSession — `get_spark(df)` returns
`df.sparkSession` (`runtime/compat.py`).

## 4. ValidationEngine (`validation/engine.py` + `expressions.py` + `columns.py`)

- Executes each `RuleDefinition` as **native PySpark Column expressions** —
  declarative rule types only; no `eval()`/generated code.
- Rule types: `NOT_NULL, UNIQUE, DUPLICATE, REGEX, RANGE, ALLOWED_VALUES,
  DATA_TYPE, DATE_FORMAT, STRING_LENGTH, REFERENTIAL_INTEGRITY,
  SCHEMA_VALIDATION, ROW_COUNT` (+ condition support via `RuleCondition`).
- Scopes: `ROW_LEVEL` (per-record pass/fail mask), `DATASET_LEVEL`
  (aggregates/row-count/schema), `CROSS_DATASET` (referential integrity with
  `reference_data` frames).
- Output per rule: total/passed/failed records, pass/fail %, failure reasons,
  duration. Per-record violations are accumulated as a **list** (multiple
  violations preserved), tagged ERROR/WARNING/INFO.
- Returns `accepted`/`failed` masks; `ValidationOutcome` carries the report +
  per-rule detail used downstream by remediation/quarantine.

## 5. Remediation (`remediation/`)

**Deterministic** (`deterministic.py`) — rule-declared, policy-permitted actions:
`TRIM, UPPERCASE, LOWERCASE, LOOKUP_MAPPING (approved lookup tables),
DATE_STANDARDIZATION, NULL_REPLACEMENT (only when approved), DEDUPLICATE`
(explicit business keys + survivor policy). Every fix records original value,
action, rule ID → audit DataFrame.

**AI** (`ai_remediation.py` + `ai/mock.py`) — `MockAIProvider` returns scripted,
clearly-labelled simulated proposals (record ID, rule, column, original →
proposed value, confidence, reason, evidence, provider, model version). Proposals
are **batched by distinct (rule,column,value)** — never one LLM-style call per
row. Includes high-confidence (≥0.90), low-confidence (<0.90) and
invalid-correction fixtures in `config/ai/mock_ai_responses.yaml`.

## 6. Confidence & sensitive-data policy (`policies/`)

- `confidence.py`: threshold as `Decimal` in [0,1]; default **0.90**.
  Boundary semantics: `confidence >= threshold` → eligible; `<` → quarantine
  (`BELOW_CONFIDENCE_THRESHOLD`). Tested at 0.89 / 0.8999 / 0.90 / 0.95 / 1.00.
- `models.py`: `RemediationPolicyConfig` — AI enable/auto-apply caps,
  `sensitive_categories` (customer_identity, account_identifiers,
  transaction_amounts, regulatory), `permitted_deterministic_actions`,
  `QualityGatePolicy`, approvals file path.
- Sensitive columns: high-confidence AI alone is **not** sufficient — corrections
  there require human approval (`PENDING_HUMAN_APPROVAL`) unless a permitted
  deterministic reference-based action applies.
- `approvals.py`: loads `config/approvals/approved_corrections.yaml` — pre-approved
  corrections that bypass the threshold.

## 7. Quarantine & revalidation (`quarantine/`, `revalidation/`)

Quarantine DataFrame columns: original record + `dq_run_id`, `dq_pipeline_id`,
`dq_dataset_id`, `dq_stage`, `dq_ruleset_ref`, failed rule IDs, failure reasons,
proposed correction JSON, confidence, `dq_remediation_status`, timestamps.

Status taxonomy observed in runs:
`OBSERVED_ONLY, UNRESOLVED_NO_SAFE_REMEDIATION, BELOW_CONFIDENCE_THRESHOLD,
PENDING_HUMAN_APPROVAL, CORRECTION_FAILED_REVALIDATION, DEDUPLICATE_REMOVED`.

`revalidator.py` re-runs the failed rules against corrected rows; failures go back
to quarantine — a bad AI guess can never leak into Silver.

## 8. Orchestration (`orchestration/`)

- `runner.py` (`DQRunner`): mode dispatch — OBSERVE_ONLY (validate+report only),
  ENFORCE (split), REMEDIATE (deterministic → AI → policy → revalidate → split).
  Produces `DQResult`.
- `gate.py`: quality-gate evaluation (`max_quarantine_rate`, dataset-level
  failures, not-evaluated rules, min accepted).
- `medallion.py` (`MedallionRunner`): BRONZE (preserve + profile) → SILVER
  (validate/remediate) → GOLD (ENFORCE business rules on accepted Silver) with
  `StoragePaths`/`StorageSink` writes and JSON reports.
- `result.py` (`DQResult`): the single contract returned to callers —
  `accepted_dataframe`, `quarantine_dataframe`, `validation_results`,
  `remediation_results`, `execution_metrics`, `quality_gate`,
  `final_validation_results`, `profile`, `to_dict()/write_report()/summary_text()`.

## 9. Registries (`rules/registry.py`, `pipelines/registry.py`)

- `RuleSetRegistry`: publish (immutable, checksum), activate, rollback, diff,
  verify, list versions. Stored as YAML + `active.json` pointer under
  `registry/rulesets/`.
- `PipelineRegistry`: manifest → state machine
  `DRAFT→REGISTERED→TESTED→ACTIVE→PAUSED→(ACTIVE|OFFBOARDED)`; records execution
  history; offboard keeps manifest + history, disables enforcement.
- Both are lightweight filesystem stores (POC) — swappable for a governance DB.

## 10. Runtime (`runtime/`)

- `config.py`: `EngineConfig.from_yaml` — engine defaults, registry paths,
  profiling/validation/AI/output/logging sections. Relative paths resolve against
  the config file.
- `local_spark.py`: `create_local_spark_session(LocalSparkSettings)` — builds a
  local session for tests/CLI; sets ANSI on, registers the Windows FS shim,
  redirects scratch to `.tmp` on D:.
- `resources/WindowsLocalFileSystem.java` (+ compiled `.jar`): minimal Hadoop
  `LocalFileSystem` shim fixing `NativeIO$Windows.access0` on Windows so Parquet
  writes work without `winutils.exe`/Hadoop binaries. Compiled at setup time;
  auditable source, no third-party binaries.
- `inspection.py`: `inspect_runtime(spark)` → versions, master, ANSI flag,
  timezone — embedded in every execution report.
- `errors.py`: typed errors (`ConfigurationError`, `RuleDefinitionError`,
  `ImmutableVersionError`, `PolicyError`, `QualityGateFailed`, …).

## 11. I/O (`io/`)

- `sources.py`: `CsvSource`, `ParquetSource`, `read_source(SourceSpec)` —
  infers format, reads via the **caller's** SparkSession.
- `sinks.py`: `StorageSink` + `StoragePaths` — `bronze/silver/gold/quarantine/
  audit` layout under configurable base path; format parquet default.

## 12. CLI (`cli/main.py` → `dqctl`)

Groups: `pipeline` (onboard/list/test/activate/pause/offboard),
`ruleset` (list/publish/activate/rollback/diff/verify), `bootstrap`, `run`,
`profile`.

`bootstrap <source>` is the new-pipeline fast path: it profiles a CSV/Parquet
file, runs `CandidateRuleGenerator` + `approve_candidates`, writes a DRAFT
ruleset (`config/rulesets/<dataset>_v1.yaml`) and a validated manifest
(`config/pipelines/<ID>.yaml`), and can `--publish` the ruleset and `--onboard`
the pipeline in the same run. Full flag reference: `docs/PROJECT_GUIDE.md`
§6.3.1.

Verified commands and outputs are in `docs/poc_demo_script.md` and
`docs/testing_report.md`.
