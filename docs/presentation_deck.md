# Presentation Deck Points

Use these points in order. Do not move a roadmap item into the “verified”
slides.

## 1. Problem

- Data pipelines fail from nulls, duplicates, invalid formats, inconsistent
  categories, and late discovery of bad records.
- Quality logic is often hardcoded in each Databricks, Fabric, or Spark
  pipeline.
- Manual cleanup does not scale and has no audit trail.

## 2. POC objective

- Build one reusable Apache Spark data-quality framework.
- Insert it into an existing pipeline with a small change.
- Keep the host pipeline and its Spark session intact.
- Operate onboarding, rules, execution, and results through UI and CLI.

## 3. Architecture

- UI and CLI submit metadata-driven runs.
- `dq-core` profiles, validates, remediates, revalidates, and quarantines.
- Rules and policies are versioned YAML, not code.
- The same engine contract targets Databricks, Fabric, EMR, Dataproc, and
  standalone Spark.
- Diagrams: `docs/architecture.md`.

## 4. Verified capabilities

- Dataset profiling and candidate-rule generation.
- Human review, customization, publishing, activation, and rollback.
- Deterministic repairs plus real Databricks Model Serving proposals.
- 90% confidence threshold, sensitive-data approval, and quarantine.
- Reports, metrics, anomaly inbox, pipeline lifecycle, and live progress.
- Local Spark validation, including the large CDR benchmark.

## 5. Demo flow

1. Show the original pipeline.
2. Show the few lines added to call the engine.
3. Upload or select data and land it in Bronze.
4. Profile, review, and publish a versioned ruleset.
5. Run Silver and Gold.
6. Show accepted outputs, quarantine, AI decisions, and audit report.
7. Show the same result in the UI.

## 6. Honest boundaries

- Cloud portability is implemented and contract-tested, but real cloud Spark
  execution is not yet verified.
- Physical Bronze/Silver/Gold persistence is the next development priority.
- Seven-pillar scoring and domain recommendations are planned, not complete.
- Authentication, scheduling, external orchestration, and alert delivery are
  intentionally deferred.

## 7. Delivery sequence

| Order | Message |
|---|---|
| P0 | Persist raw, accepted, quarantine, and audit data per stage |
| P1 | Make Bronze a managed landing, not just an uploaded file |
| P2 | Account for every remediation outcome |
| P3 | Add formal quality-pillar scores and domain recommendations |
| P4 | Prove the same code on real Databricks Spark |
| P5 | Add production security, scheduling, and operations |

## 8. Close

“The POC proves the reusable Spark quality engine and user workflow. The next
sprint completes physical medallion persistence and ingestion so the demo
shows data actually moving from Bronze to Gold with a full audit trail.”
