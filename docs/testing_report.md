# Testing Report — Executed Evidence

All results below are from **actual executions** on this machine. Nothing is
simulated except the explicitly-labelled `MockAIProvider` proposals.

## 1. Environment under test

| Item | Value |
|---|---|
| OS | Windows 11 AMD64 |
| Python | 3.12.0 (project `.venv` on D:) |
| Apache Spark / PySpark | 4.1.3 |
| Java | OpenJDK 21.0.1 (`JAVA_HOME=C:\Program Files\Java\jdk-21`) |
| Spark master | `local[2]` (tests) / `local[4]` (CLI default) |
| ANSI SQL | `spark.sql.ansi.enabled=true` (matches Databricks behaviour) |
| Session timezone | UTC |
| Scratch/temp/pip cache | `.tmp/` inside the project on D: (C: untouched) |
| Windows Parquet fix | `WindowsLocalFileSystem` Java shim compiled to `.tmp/hadoop-shim/dq-windows-localfs.jar` |

Runtime self-check (`scripts/check_runtime.py`) reported: Spark 4.1.3, Java 21,
ANSI on, UTC, and a successful local Parquet write/read round-trip (2 rows).

## 2. Formal test suite — final result

```
& $VenvPython -m pytest packages\dq-core\tests\ -q --tb=short
........................................................................ [ 55%]
..........................................................               [100%]
130 passed in 531.05s (0:08:51)
```

**130 tests, 0 failures, 0 errors** — run on 2026-11-06 against the final code.

### Coverage map (mandatory scenarios → tests)

| Required scenario | Test(s) | Result |
|---|---|---|
| Spark session starts | `test_spark_session.py::test_spark_session_starts`, `test_spark_ansi_enabled` | ✅ |
| Parquet round-trip | `test_local_parquet_roundtrip`, `test_dataframe_creation_and_count` | ✅ |
| Profiling works | `test_profiling.py` (10 tests: completes, anomalies, duplicates, email/date inference, mixed formats, candidates, evidence, sample, approximate) | ✅ |
| Candidate rules generated | `test_profiling.py::test_candidate_rule_generation`, `test_all_candidate_rules_have_evidence`, `test_candidate_generation.py` (19 unit tests) | ✅ |
| RuleSet v1.0 loads | `test_validation.py::test_ruleset_v1_loads_correctly` | ✅ |
| RuleSet v1.1 created | `test_validation.py::test_ruleset_v1_1_loads_correctly`, `test_v1_1_stricter_age_rule` | ✅ |
| Published versions unchanged | `test_engine_modes.py::test_published_versions_are_immutable`, `test_metadata.py::test_immutable_version_error` | ✅ |
| Validation finds failed records | `test_validation.py::test_validation_identifies_failed_records` | ✅ |
| Multiple violations per record | `test_validation.py::test_multiple_violations_per_record` | ✅ |
| Deterministic remediation | `test_remediation.py` (trim, lookup, date standardisation, approved null-replace, dedupe) | ✅ |
| Mock AI proposals | `test_remediation.py::test_mock_ai_produces_proposals` | ✅ |
| 90 % threshold | `test_policies.py::TestConfidenceBoundaries` (0.89/0.8999/0.90/0.95/1.00) + `test_ai_exactly_90_percent_applied`, `test_ai_low_confidence_below_threshold` | ✅ |
| Low-confidence → quarantine | `test_remediation.py::test_ai_low_confidence_below_threshold` | ✅ |
| Sensitive fields need approval | `test_policies.py::test_sensitive_column_requires_approval_even_at_100`, `test_transaction_amount_sensitive` | ✅ |
| Post-validation works | `test_remediation.py::test_invalid_ai_correction_fails_revalidation`, `test_engine_modes.py::test_post_validation_works` | ✅ |
| Accepted → Silver | `test_engine_modes.py::test_accepted_written_to_silver` | ✅ |
| Gold business checks | `test_medallion.py::test_gold_level_checks_referential_integrity`, `test_medallion_bronze_silver_gold` | ✅ |
| Bronze unchanged | `test_engine_modes.py::test_bronze_remains_unchanged` | ✅ |
| Execution metrics | `test_medallion.py::test_execution_metrics_generated`, `test_json_report_serialises` | ✅ |
| Standalone Spark example | whole suite runs on local Spark; `examples/` + CLI `run`/`profile` verified | ✅ |
| Generic PySpark API on existing DF | every engine test passes an existing DataFrame; `test_portability_contracts.py` | ✅ |
| Pipeline lifecycle | `test_pipeline_lifecycle.py` (unit) + `test_pipeline_lifecycle_integration.py` (pause/offboard/history) + CLI verified | ✅ |
| Medallion B/S/G | `test_medallion.py` (4 tests) | ✅ |
| Portability contracts | `test_portability_contracts.py` (9 tests) | ✅ |

## 3. End-to-end runs executed via CLI (real output)

### 3.1 OBSERVE_ONLY — customers

```
input=38 accepted=38 quarantined=22 duplicates_removed=0 warnings=1
quality gate: FAILED ['max_quarantine_rate']   (expected: 22/38 flagged > 40 % gate)
duration=13449 ms   ruleset=customers_silver@1.0 published=True
```
Report: `.tmp/cli_observe_report.json`

### 3.2 Profile — customers (excerpt)

```
Profile: customers  rows=38  columns=8  mode=FULL
  duplicate rows=1 groups=1
  Anomalies: LEADING_TRAILING_WHITESPACE (id/name/email),
             OUTLIERS age (max=150), TYPE_INCONSISTENCY age 2.63 %,
             RARE_CATEGORY_VALUES country/status,
             INCONSISTENT_FORMATTING status {ACTIVE, active},
             MIXED_DATE_FORMATS created_date {yyyy-MM-dd 94.7 %, dd/MM/yyyy 2.6 %}
  Schema anomalies: TYPED_DATA_STORED_AS_STRING age, created_date
```
JSON: `.tmp/cli_profile.json`

### 3.3 Pipeline lifecycle (dqctl)

```
onboard  → state=REGISTERED
test     → state=TESTED
activate → state=ACTIVE
pause    → state=PAUSED  ("data will pass through until reactivated")
offboard → state=OFFBOARDED ("customer data and history retained")
list     → OFFBOARDED  DATABRICKS  REMEDIATE
```

### 3.4 Ruleset versioning (dqctl)

```
publish customers_v1.yaml    → customers_silver@1.0  checksum=51caa7545a08 ACTIVE
publish customers_v1_1.yaml  → customers_silver@1.1  checksum=3443228fac0a
diff 1.0→1.1                 → added=1 modified=3 unchanged=13 (+CUST_017,
                               CUST_007 max 120→110, CUST_010 +SUSPENDED,
                               CUST_013 severity WARNING→ERROR)
activate 1.1 → rollback 1.0  → verify → "published and unchanged"
```

### 3.5 Expected-results conformance

`sample_data/expected_results.json` (generated by `scripts/generate_sample_data.py`)
was used as the oracle; medallion/engine tests assert the per-record outcomes:

- customers REMEDIATE v1.0: `accepted=24, accepted_remediated=8, quarantined=13,
  removed_duplicates=1` — matches.
- customers ENFORCE v1.0: `accepted=16, quarantined=22` — matches.
- customers OBSERVE_ONLY: `accepted=38, flagged=22` — matches CLI run above.
- customers REMEDIATE v1.1: `quarantined=14` (C0026 age 115 now fails) — matches.
- transactions REMEDIATE: `accepted=30, accepted_remediated=4, quarantined=9,
  removed_duplicates=1`; Gold `accepted=26, quarantined=4, warnings=1,
  summary_customers=18, summary_total_amount=2519147.89` — matches.

## 4. Defects found & fixed during verification

| Defect | Symptom | Fix |
|---|---|---|
| `conftest.py` path off-by-one | all Spark tests ERROR `PATH_NOT_FOUND …/Linkedin/sample_data` | `parents[4]`→`parents[3]` |
| `cli/main.py` same off-by-one | ROOT resolved to `Linkedin\` | `parents[6]`→`parents[5]` |
| Spark 4 `createDataFrame` | `StructType can not accept object 0` | pass row tuples, not scalars |
| Circular import | `cannot import name 'IntegrationMode'` | lightweight `runtime/__init__.py` |
| ANSI-mode lambda bug | `CAST_INVALID_INPUT` on severity filter | closure factory instead of default-arg lambda |
| `derive()` same-version | `RuleDefinitionError` in immutability test | construct modified RuleSet directly (with `lookups`) |
| Candidate ruleset id | `Invalid ruleset_id 't'` | use valid ids in tests |
| Windows Parquet | `HADOOP_HOME unset`, `NativeIO$Windows.access0` | auditable `WindowsLocalFileSystem` shim jar |

## 5. What was NOT executed (honest list)

- ❌ No Databricks workspace/cluster — Databricks path is a notebook-style
  script + contract-tested adapter only.
- ❌ No real LLM — `MockAIProvider` outputs are scripted and labelled simulated.
- ❌ No production Airflow/ADF/dbt/Flink/Snowflake connectors — contracts only.
- ❌ No UI / REST service / auth / governance DB — out of POC scope.
