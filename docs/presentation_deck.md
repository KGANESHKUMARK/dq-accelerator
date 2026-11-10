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
- 90% confidence threshold, sensitive-data approval gates, and quarantine.
- **Physical medallion persistence** — Bronze/Silver/Gold, quarantine, audit
  logs, and reports written to configurable storage with lineage manifests.
- **Terminal outcome audit** — every failed value classified
  (deterministic / AI-applied / below-threshold / blocked-sensitive /
  no-safe-proposal).
- **Seven-pillar quality scores** + domain recommendations in every report
  and the pipeline-run UI.
- Reports, metrics, anomaly inbox, pipeline lifecycle, and live progress.
- Local Spark validation, including the large CDR benchmark.
- Two verified domains end-to-end: healthcare patients and goat-herd
  agriculture (onboarded live through the UI wizard API).

## 5. Demo flow (UI-first — see `docs/ui_demo_guide.md`)

1. Onboard wizard: upload CSV → profile → review candidate rules → publish +
   register → test → activate.
2. Run DQ → **Run all stages** → watch live phase progress.
3. Show the stage table with per-stage quality scores.
4. Show remediation outcome badges — deterministic fixes, blocked sensitive
   data, below-90% AI proposals withheld.
5. Expand **Written outputs** — physical Bronze/Silver/Gold/quarantine/audit
   paths on disk.
6. Open the JSON report — pillars, outcome audit, AI decisions (`simulated`
   flag proves real vs mock LLM).

## 6. Honest boundaries

- Cloud portability is implemented and contract-tested; identical engine
  outcomes verified under DATABRICKS/FABRIC/STANDALONE_SPARK/LOCAL labels on
  local Spark — **a real cloud Spark cluster run is still open (P4)**.
- Databricks Model Serving LLM calls are real (`simulated=false` in reports);
  one fallback-to-mock decision is flagged honestly in the audit.
- Authentication, scheduling, external orchestration, and alert delivery are
  intentionally deferred (P5).

## 7. Delivery sequence

| Order | Message | Status |
|---|---|---|
| P0 | Persist raw, accepted, quarantine, and audit data per stage | ✅ Done |
| P1 | Make Bronze a managed landing, not just an uploaded file | ✅ Done |
| P2 | Account for every remediation outcome | ✅ Done |
| P3 | Add formal quality-pillar scores and domain recommendations | ✅ Done |
| P4 | Prove the same code on real Databricks Spark | ◐ Contract evidence done; cluster run open |
| P5 | Add production security, scheduling, and operations | Deferred |

## 8. Close

“The POC proves the reusable Spark quality engine end-to-end: onboard a dataset
through the UI, land raw data in Bronze with lineage, approve versioned rules,
run Silver and Gold with physical outputs, and watch every AI decision —
applied, withheld, or blocked — land in quarantine with a full audit trail.”
