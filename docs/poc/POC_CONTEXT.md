# POC Context and Guardrails

This is the authoritative context for ongoing development. If a proposed task
does not advance one of the priorities below, it is out of scope until the POC
is complete.

## Objective

Build a reusable, platform-independent Data Quality Accelerator on open-source
Apache Spark/PySpark. The framework must plug into an existing pipeline with a
small integration change, not replace that pipeline.

The primary local runtime is Apache Spark. Databricks is the first enterprise
target, while the same engine contract must remain usable from Fabric,
Dataproc, EMR, and standalone Spark.

## What the POC must prove

1. A user can onboard a dataset and pipeline through the UI or CLI.
2. The framework lands raw data in Bronze and profiles it.
3. It generates candidate rules and lets a user review, customize, version,
   publish, activate, and roll them back.
4. It applies the selected ruleset at Bronze, Silver, or Gold.
5. It fixes eligible issues deterministically, then uses AI where configured.
6. Corrections at or above 90% confidence can be applied; sensitive corrections
   require human approval.
7. Corrections below 90%, unsafe proposals, and failed revalidations remain in
   quarantine for review.
8. Accepted data and quarantine are physically persisted through the medallion
   stages, with reports, metrics, and audit history.
9. The same Spark engine contract can be embedded in another PySpark pipeline
   without rewriting the host pipeline.

## Verified baseline

- Local Spark engine, profiling, validation, remediation, quarantine, reports,
  registries, CLI, and UI are implemented.
- Rule versioning, lifecycle governance, and confidence policy are implemented.
- Real Databricks Model Serving calls have been verified.
- **P0 done:** `MedallionRunner` + `POST /api/pipelines/{id}/run` physically
  persist Bronze, Silver, Gold, quarantine, audit, and report outputs.
- **P1 done:** managed Bronze ingestion writes a lineage manifest (source
  checksum, schema, row count, timestamp).
- **P2 done:** `RemediationResults.outcome_audit` assigns a terminal outcome to
  every failed rule example; counts ship in every JSON report.
- **P3 done:** `quality_pillars` adds seven-pillar scores and a domain
  recommendation to every `DQResult` report.
- **P4 partial:** identical engine outcomes verified on local Spark under
  DATABRICKS / FABRIC / STANDALONE_SPARK / LOCAL labels, and
  `engine.databricks.yaml` loads via `${DQ_ROOT}`. **No real cloud Spark
  cluster has been executed** — that remains the only open P4 item.
- **Second domain verified:** `GOAT_HERD_PIPELINE` (agriculture, 63 rows)
  onboarded via wizard API → silver v1.1 with mixed deterministic/AI
  remediation → gold ruleset → medallion REMEDIATE run with outcome spectrum
  (deterministic fix, blocked-sensitive, below-threshold real-LLM proposals,
  no-safe-proposal) and pillar scores in UI.
- **UI surfaces P0–P3:** pipeline-run view shows per-stage quality scores,
  remediation outcome badges, domain recommendations, and written output
  paths.
- Cross-platform support is **code-portable and contract-tested**, not executed
  on real Fabric, EMR, Dataproc, or Databricks Spark compute.

## Ordered priorities

| Priority | Work | Done when |
|---|---|---|
| P0 | Physical Bronze/Silver/Gold persistence | Each stage writes data, quarantine, audit, and report paths |
| P1 | Managed Bronze ingestion | Upload/source landing includes schema, checksum, count, and timestamp |
| P2 | Complete remediation outcomes | Every eligible violation has an auditable terminal outcome |
| P3 | Seven-pillar score and domain recommendations | Scores and recommendations appear in reports and UI |
| P4 | Real cross-platform evidence | Contract + config + identical-results evidence done; a real Databricks Spark run remains the only open item and must not be claimed until executed |
| P5 | Production platform | Auth, scheduling, orchestration adapters, alerting channels, external registry, Kubernetes |

No P5 work starts while P0–P4 remains open.

## Architecture decisions that must not drift

- The engine accepts an existing Spark DataFrame and never owns the caller's
  SparkSession.
- Rules remain metadata-driven YAML, immutable once published, and selected by
  version or active pointer.
- AI output is a proposal, never an unconditional mutation.
- Revalidation is mandatory after every applied correction.
- Quarantine retains the original record and full decision provenance.
- Platform-specific code stays in adapters or examples; `dq-core` remains free
  of Databricks, Fabric, EMR, and Dataproc imports.
- Local POC storage uses project-local paths. Cloud paths are configuration,
  not a separate engine implementation.

## Non-goals until P5

- Authentication, RBAC, tenants, and production user management.
- Airflow, ADF, dbt, Kafka, Flink, Snowflake, BigQuery, or Redshift execution.
- Email, webhook, or chat alert delivery.
- Kubernetes, high availability, and multi-process job distribution.
- A replacement for Databricks Workflows, Fabric pipelines, or EMR
  orchestration. This framework integrates into those pipelines.

## Acceptance narrative

The demo should show an existing pipeline, add the small DQ integration, land
raw data in Bronze, profile and approve rules, run Silver and Gold, persist
accepted/quarantine outputs, explain AI and 90% decisions, and display the
quality result in the UI. Claims of cloud execution must match actual evidence.
