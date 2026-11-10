# DQ Accelerator POC — Clear Use Case

## 1. Business use case

A data engineering team already has PySpark pipelines. They do not want those
pipelines rewritten. They want a reusable Data Quality Accelerator that can be
inserted at any stage, understand the data, recommend quality rules, apply safe
corrections, and preserve every uncertain record for human review.

The POC uses a healthcare dataset as the clearest demonstration:

1. Raw patient records arrive from a source file.
2. The framework lands those records unchanged in Bronze.
3. It profiles the data and proposes quality rules.
4. A steward reviews and publishes a versioned Silver ruleset.
5. Silver validation repairs safe issues and quarantines uncertain rows.
6. Gold validation checks publication-ready constraints.
7. The UI shows what happened, why, and where every output was written.

The same flow applies to banking, pharmacy, and telecom datasets. Healthcare is
only the first minimal proof.

## 2. Actors

| Actor | Responsibility |
|---|---|
| Data engineer | Integrates the engine into an existing Spark pipeline |
| Data steward | Reviews candidate rules, thresholds, and quarantined records |
| Pipeline owner | Onboards, tests, activates, pauses, or offboards a pipeline |
| Reviewer | Approves or rejects sensitive or low-confidence corrections |
| Platform engineer | Runs the same engine locally now and on cloud Spark later |

## 3. Minimal end-to-end flow

```text
source file
    -> Bronze landing (raw, unchanged)
    -> profile + candidate rules
    -> steward-approved Silver ruleset
    -> Silver DQ run
         -> accepted Silver data
         -> Silver quarantine
         -> remediation audit
    -> Gold DQ run on Silver accepted data
         -> accepted Gold data
         -> Gold quarantine
    -> JSON report + UI summary
```

## 4. Functional requirements in scope

- Plug into an existing Spark DataFrame without owning its SparkSession.
- Support CSV and Parquet inputs for the POC.
- Profile schema, nulls, duplicates, ranges, patterns, and anomalies.
- Generate, review, customize, version, publish, activate, and roll back rules.
- Run `OBSERVE_ONLY`, `ENFORCE`, and `REMEDIATE`.
- Apply deterministic fixes before AI proposals.
- Use 90% as the default confidence threshold.
- Require approval for sensitive columns.
- Revalidate every applied correction.
- Quarantine unresolved or unsafe records with original values and reasons.
- Persist Bronze, Silver, Gold, quarantine, and audit outputs.
- Show results and written paths in CLI, JSON reports, and UI.
- Keep the engine portable across Spark platforms.

## 5. Phase completion criteria

### P0 — Physical medallion persistence

A minimal run writes:

- `data/bronze/<dataset>/raw`
- `data/silver/<dataset>/accepted`
- `data/gold/<dataset>/accepted`
- `data/quarantine/silver/<dataset>`
- `data/quarantine/gold/<dataset>`
- `data/audit/silver/<dataset>`
- stage JSON reports

Verified by reading the written data back with Spark and checking row counts.

### P1 — Managed Bronze ingestion

A source file is landed into Bronze with an ingestion manifest containing:

- source path
- checksum
- schema
- row count
- ingestion timestamp

The medallion run can use this managed Bronze landing as its input.

### P2 — Terminal remediation outcome

Every eligible violation reaches exactly one terminal outcome:

- deterministic fix
- AI proposal applied
- below 90% confidence
- approval required
- no safe proposal
- correction failed revalidation

The outcome counts are visible in the result and report.

### P3 — Seven quality pillars

The report and UI show scores for:

- completeness
- uniqueness
- validity
- consistency
- accuracy
- timeliness
- integrity

The result also contains a domain recommendation. Healthcare is the first
domain template.

### P4 — Cross-platform evidence

The same minimal dataset and engine contract run through the Spark adapter
under the platform contexts used by the POC. This phase must not claim a real
Databricks, Fabric, EMR, or Dataproc cluster execution unless that execution
actually happens. Current evidence is local Spark plus the shared Spark adapter
contract.

## 5a. Verification status (minimal dataset: `sample_data/minimal/patients_minimal.csv`)

| Phase | Status | Evidence |
|---|---|---|
| P0 | ✅ Verified | API run on `patients_minimal` + `goat_herd_animals`: Bronze raw persisted, Silver/Gold accepted + quarantine + audit log, all paths returned in `written_paths` |
| P1 | ✅ Verified | `_ingest.json` per dataset: source checksum, schema, row count, timestamp (patients `0a819f28…`, goat `dbdb7a3a…`) |
| P2 | ✅ Verified | `outcome_audit` in every report; goat v1.1 run showed `DETERMINISTIC_FIX`, `BLOCKED_SENSITIVE_DATA`, `BELOW_CONFIDENCE_THRESHOLD` (real LLM @0.80), `NO_SAFE_PROPOSAL`; 5 unit tests |
| P3 | ✅ Verified | `quality_pillars` in reports + pipeline-run API/UI: goat overall 99.59, agriculture recommendation; domain detection for healthcare/agriculture/pharmacy/telecom/banking |
| P4 | ◐ Partial | Identical outcomes under DATABRICKS/FABRIC/STANDALONE_SPARK/LOCAL on local Spark; `engine.databricks.yaml` loads; evidence in `reports/p4_portability_evidence.json`. **Real cloud Spark run still open — not claimed.** |

### Second verified domain — goat herd (agriculture)

`GOAT_HERD_PIPELINE` was onboarded live through the wizard API
(`sample_data/bronze/goat_herd_animals.csv`, 63 rows): 30 candidates → 28
approved → `goat_herd_animals_silver@1.0` published → v1.1/v1.2 added steward
remediation policies (deterministic + AI-assisted) → Gold ruleset added.
Medallion REMEDIATE run on v1.2: Silver 54 acc/6 quar (gate PASS, 99.66),
Gold 53 acc/1 quar (gate PASS, 99.69). Outcome spectrum with real DQX LLM:
`AI_APPLIED` (0.95 conf), `HUMAN_APPROVED_AI` (steward-approved 0.80),
`BLOCKED_SENSITIVE_DATA`, `DETERMINISTIC_FIX`, `NO_SAFE_PROPOSAL`.
See `docs/ui_demo_guide.md` for the client-facing walkthrough.

## 6. Explicitly deferred

Authentication, RBAC, scheduling, Airflow, ADF, streaming, data warehouses,
alert delivery, PostgreSQL, and Kubernetes remain out of scope until P0–P4 are
complete and separately approved.

## 7. Safety rule

No existing working flow may be broken. New persistence and scoring behavior is
additive. Existing single-stage Run DQ remains available while the medallion
flow gains physical outputs.
