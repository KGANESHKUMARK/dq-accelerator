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
- Cross-platform support is currently **code-portable and contract-tested**,
  not executed on real Fabric, EMR, Dataproc, or Databricks Spark compute.
- Bronze/Silver/Gold stage execution exists, but physical stage persistence is
  not yet complete.

## Ordered priorities

| Priority | Work | Done when |
|---|---|---|
| P0 | Physical Bronze/Silver/Gold persistence | Each stage writes data, quarantine, audit, and report paths |
| P1 | Managed Bronze ingestion | Upload/source landing includes schema, checksum, count, and timestamp |
| P2 | Complete remediation outcomes | Every eligible violation has an auditable terminal outcome |
| P3 | Seven-pillar score and domain recommendations | Scores and recommendations appear in reports and UI |
| P4 | Real cross-platform evidence | At minimum one real Databricks Spark run; other platforms clearly evidenced or marked unverified |
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
