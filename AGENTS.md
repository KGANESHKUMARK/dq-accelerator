# Session Memory — DQ Accelerator POC

Last updated: 2026-07-14

## Project

- Root: `D:\Siddharitha\SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED\Projects\Linkedin\DQ-Accelerator`
- Goal: reusable Apache Spark data-quality framework for existing PySpark pipelines.
- Do not confuse this repository with BankOps AI Command Center.

## Current truth

- Local Spark engine, UI, CLI, onboarding, rule versioning, quarantine, reports,
  and lifecycle are working.
- Real Databricks Model Serving remediation was verified earlier.
- Credentials belong in gitignored `.env`; never commit them.
- **P0–P3 complete and verified** on `sample_data/minimal/patients_minimal.csv`:
  physical medallion persistence (`MedallionRunner` + `/api/pipelines/{id}/run`),
  managed Bronze ingest manifest, `outcome_audit` terminal outcomes, and
  `quality_pillars` seven-pillar scores.
- **P4 partial:** identical local-Spark outcomes under all platform labels +
  `engine.databricks.yaml` config load (`reports/p4_portability_evidence.json`).
  No real cloud Spark cluster has been executed — do not claim it.
- Remaining honest gaps: real Databricks/Fabric/EMR/Dataproc cluster run,
  pharmacy/telecom recommendation templates (UI surfacing of outcomes and
  pillar scores is now done).
- Second verified domain: `GOAT_HERD_PIPELINE` + `goat_herd_animals` dataset.
  Silver ruleset now at **v1.2** (published+active): breed/gender AI-assisted
  auto_apply → real LLM `AI_APPLIED` @0.95; steward approvals in
  `config/approvals/approved_corrections.yaml` → `HUMAN_APPROVED_AI` @0.80;
  DOB blocked-sensitive; dedupe deterministic; unfixables quarantined.
  Gold ruleset + breed lookup `config/rules/lookups/goat_breeds.yaml`.
  Best run: Silver 54 acc/6 quar (99.66), Gold 53/54 (99.69), gate PASS.
- Client-facing demo/onboarding walkthrough: `docs/ui_demo_guide.md`.
- Fresh-machine → live demo runbook (prereqs, assets, ordered demo acts,
  verified numbers, honest boundaries): `docs/end_to_end_demo.md`.
- Approvals inbox fixed: reject persists to `config/approvals/rejected_corrections.yaml`,
  pending inbox filters actioned rows, `GET /api/quarantine/decisions` audit trail,
  Reports viewer shows quality_pillars + outcome_audit sections.
- Engine rejection memory: `RejectionStore`/`Rejection` in `policies/approvals.py`,
  `Decision.REJECTED_BY_HUMAN` in confidence.py (checked after approval, before
  threshold), `policy.rejections_path` auto-derives `rejected_corrections.yaml`
  beside approvals file (or `approvals.rejections_file` in policy YAML), outcome
  `REJECTED_BY_HUMAN` in outcomes.py. 4 new unit tests; 74 unit + 65 integration green.
- Hotel 1M-row demo asset: `sample_data/bronze/hotel_reservations.parquet`
  (1,000,000 rows, 42.8 MB, seeded defects); gold ruleset
  `config/rulesets/hotel_reservations_gold_v1.yaml` published as
  `hotel_reservations_gold@1.0`. Silver ruleset + pipeline manifest are
  created by the user through the UI onboarding wizard.

## Development guardrails

- Preserve all existing working behavior.
- Implement and verify P0, then P1, then P2, then P3, then P4.
- Use a minimal dataset for each phase before proceeding.
- Do not commit or push until the user reviews and explicitly approves.
- Do not begin auth, scheduling, orchestration adapters, alerting, or
  Kubernetes while P0–P4 is open.
- Authoritative documents:
  - `docs/POC.md`
  - `docs/POC_CONTEXT.md`
  - `ROADMAP.md`
  - `docs/architecture.md`
  - `docs/DEVELOPER_GUIDE.md` (build phases + release process)

## Active implementation plan

1. ~~Persist raw, accepted, quarantine, and audit outputs for each medallion stage.~~ Done (P0).
2. ~~Add managed Bronze ingestion metadata.~~ Done (P1).
3. ~~Add a terminal outcome for every eligible violation.~~ Done (P2).
4. ~~Add seven-pillar scores and domain recommendations.~~ Done (P3).
5. P4 remainder: real Databricks Spark cluster run (needs workspace job submission;
   LLM endpoint token alone does not provide Spark compute). Optional stretch —
   only claim if actually executed.
6. Polish: surface `outcome_audit` counts and `quality_pillars` in the UI report
   view; add pharmacy/telecom recommendation templates.
5. Verify the same minimal contract through the portable Spark adapter without
   fabricating cloud execution evidence.

## feature/nosql-metadata branch (in progress, local commits only)

- **NoSQL metadata store** (`dq_core/metadata/store.py`): MetadataStore abstraction,
  TinyDB embedded default + MongoDB + DynamoDB (boto3) backends; collections for
  ruleset_index/versions, pipelines, manifests, policies, approvals, rejections,
  profiles, candidates, jobs, metrics. `registry.backend: nosql` + `registry.metadata.*`.
- Doc-backed registries (`registries/doc_store.py`), migration script
  `scripts/migrate_registry_to_nosql.py` (ran: 12 rulesets/18 versions/10 pipelines
  → `data/metadata.nosql.json`). API: `/api/metadata/status|collection/{n}|switch`.
  CLI: `dqctl metadata status|switch`. Settings UI card with collection browser.
- **DQX-adoption Tier1+2**: `alerts:` config (webhook + fail_pipeline via
  `alerts/webhook.py`); metrics recorder → `metrics` collection/JSONL;
  ODCS import (`contracts/odcs.py` + `dqctl ruleset import-odcs`);
  LLM rule gen from descriptions (`generation/llm_generator.py` + `dqctl ruleset
  generate`, LiteLLM path + heuristic fallback); 4 new dataset rule types
  (NULL_RATE, DISTINCT_RATIO, FRESHNESS, COLUMN_AGGREGATE — verified under Spark);
  robust MAD outliers (`robust_outlier_count`, IQR/1.349) in profiler.
- 80 unit tests green. Backend live on nosql/tinydb.
