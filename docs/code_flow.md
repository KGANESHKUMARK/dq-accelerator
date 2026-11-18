# DQ Accelerator — Code Flow

End-to-end map of how a request travels: **UI → FastAPI → background job →
dq-core engine → Spark → parquet/Delta outputs + NoSQL metadata**.

---

## 0. Big picture

```
Browser (React SPA :5173)
   │  fetch() wrappers            ui/frontend/src/api.ts
   ▼
FastAPI backend (:8000)          ui/backend/main.py  (~1900 lines, all endpoints)
   │  J.submit(kind, fn)          ui/backend/jobs.py  (thread pool + job store + Spark singleton)
   ▼
dq-core engine                  packages/dq-core/src/dq_core/
   │  engine.run(df, manifest)    engine.py
   │  MedallionRunner             orchestration/medallion.py
   ▼
Apache Spark (local[N] default) runtime/local_spark.py
   │
   ├─► data/{bronze,silver,gold}/<dataset>/     parquet dirs (part-files + _SUCCESS)
   ├─► data/quarantine/<stage>/<dataset>/
   ├─► reports/*.json                            execution reports
   └─► data/metadata.nosql.json                  NoSQL collections (TinyDB default)
```

---

## 1. Startup flow

```
DQ_start.bat
 ├─ [1/6] python --version      → require 3.12–3.14 (3.15+ rejected: no wheels)
 ├─ [2/6] java -version         → require 17+
 ├─ [3/6] node --version        → require 18+
 ├─ [4/6] .venv + pip install -r requirements.txt
 │         └─ if DATABRICKS_TOKEN set → also requirements-llm.txt (failure → MockAI warn)
 ├─ [5/6] ui/frontend npm install + npm run build
 └─ [6/6] start two windows:
     ├─ uvicorn main:app --app-dir ui/backend :8000   → logs/backend/
     └─ npm run dev :5173                             → logs/frontend/
```

Backend boot (`main.py`): FastAPI app → routers → `J` job manager →
`J.get_spark()` lazily creates the Spark session **on first use**
(`runtime/local_spark.py` — `local[N]`, driver memory from
`config/engine.yaml`, Windows parquet shim, Delta auto-config when
`DQ_OUTPUT_FORMAT=delta`).

---

## 2. Onboarding flow (the wizard)

```
UI: Onboard → Step1 source → Step2 options → Step3 profile → Step4 rules → Step5 finalize
 │
 ├─ POST /api/onboard/upload          main.py:onboard_upload
 │     saves file → data/uploads/<name>  (any size)
 │
 ├─ POST /api/onboard/preview         main.py:onboard_preview
 │     J.submit("bootstrap_preview", _do_bootstrap_preview)
 │       ├─ _spark_engine()           jobs.py:get_spark() + engine from engine.yaml
 │       ├─ _read_any_source()        NEW: AUTO|CSV|PARQUET|JSON|DELTA|TABLE
 │       │     files → repo-root guard; TABLE→spark.read.table,
 │       │     DELTA→spark.read.format("delta"), JSON→json reader
 │       ├─ _profile_mode_for()       >50MB file → SAMPLE else FULL
 │       ├─ engine.profile(df, ds)    profiling/profiler.py
 │       │     column stats (null%, distinct, percentiles, IQR outliers,
 │       │     robust z-score, top values, date formats, whitespace…)
 │       │     _anomalies()           ALL_NULL/HIGH_NULL_RATE/DOMINANT_VALUE/
 │       │                            OUTLIERS/ROBUST_OUTLIERS/TYPE_INCONSISTENCY…
 │       │     + pii/detector.py      PII_SUSPECTED (name+value signals)
 │       │     + profiling/drift.py   DRIFT_* vs last profiles-collection doc
 │       ├─ CandidateRuleGenerator    generation/generator.py
 │       │     profile → CANDIDATE RuleDefinitions (confidence+evidence)
 │       └─ returns {rows, cols, mode, anomalies, pk_candidates, candidates[]}
 │     UI polls GET /api/jobs/{id} → renders success box + anomaly cards
 │
 └─ POST /api/onboard/finalize       main.py:onboard_finalize
       ├─ load preview job result → approved_rule_ids → RuleSet (status PUBLISHED)
       ├─ write ruleset YAML → config/rulesets/<id>_v1.yaml
       ├─ write manifest YAML    → config/pipelines/<PIPELINE>.yaml
       ├─ registry.publish()     → checksum + version entry
       ├─ NoSQL inserts: candidates + profiles collections
       └─ optional onboard → lifecycle state ACTIVE
```

CLI equivalent: `dqctl bootstrap <source>` → same generator path.
Contract path: `dqctl ruleset import-odcs contract.yaml` →
`contracts/odcs.py` → same CANDIDATE ruleset.
LLM path: `generation/llm_generator.py` — business description →
LiteLLM/Databricks or keyword heuristics → same CANDIDATE contract.

---

## 3. Run DQ flow (execution)

```
UI: Pipelines → Run DQ (stage + mode pickers)
 │
 ├─ POST /api/pipelines/{id}/run     main.py → J.submit("dq_run", ...)
 │
 └─ _do_run()  (jobs.py worker thread)
     ├─ manifest_from_store()        metadata/resolver.py — NoSQL first,
     │                               YAML fallback (config/pipelines/*.yaml)
     ├─ ruleset load                 registries/ — versioned, checksummed,
     │                               immutable; rollback = pick older version
     ├─ _read_source()               io/sources.py — manifest source
     ├─ engine.run(df, manifest)     engine.py → MedallionRunner
     │   │
     │   ├─ BRONZE  write raw df → data/bronze/<ds>/   (parquet|delta)
     │   │            + ingestion manifest/lineage doc
     │   ├─ SILVER  validate rules → per-rule pass/fail
     │   │            ├─ deterministic remediation   remediation/
     │   │            ├─ AI proposals (needs ≥90% conf) ai/dqx_provider.py
     │   │            │     LiteLLM → Databricks endpoint | MockAI fallback
     │   │            ├─ approval memory hit?        policies/approvals.py
     │   │            │     approved → HUMAN_APPROVED_AI auto-apply
     │   │            │     rejected → REJECTED_BY_HUMAN (never resurfaces)
     │   │            ├─ below threshold/unfixable → quarantine/
     │   │            ├─ revalidation pass           revalidation/
     │   │            └─ write data/silver/<ds>/ + data/quarantine/silver/<ds>/
     │   ├─ GOLD    stricter ruleset → same loop → data/gold/<ds>/
     │   │            gate fail → QualityGateFailedError (alerts.fail_pipeline)
     │   └─ report  reports/<run>.json — rule results, outcome_audit,
     │                quality_pillars (7 scores), execution_metrics
     │
     ├─ metrics/recorder.py → metrics collection (score trend source)
     ├─ alerts/webhook.py   → gate_fail/quarantine_threshold events
     │     format: generic | slack | teams  (alerts.webhook_format)
     └─ job result → UI progress phases + final summary
```

UI polls `GET /api/jobs/{id}` (or `/stream` SSE) → progress text like
`phase=validate | validating all rules`.

---

## 4. Read paths (what the UI reads back)

| UI | Endpoint | Reads |
|---|---|---|
| Dashboard KPIs/journey | `/api/summary`, `/api/journey` | reports/*.json + jobs.db |
| Reports list/detail | `/api/reports`, `/api/reports/{name}` | reports/*.json |
| **Stage data preview** | `/api/data/available`, `/api/data/preview` | `spark.read.parquet(data/{stage}/{ds})` limit 50 |
| Quarantine inbox | `/api/quarantine/pending`, `/decisions` | last 10 REMEDIATE reports' `ai_decisions` |
| Approve/Reject | `/api/quarantine/approve|reject` | `config/approvals/*.yaml` + NoSQL `approvals`/`rejections` |
| Metrics trend | `/api/metrics/recent` | `metrics` collection |
| Anomalies | `/api/anomalies` | `data/anomalies.db` |
| Rule Sets | `/api/rulesets*` | `config/rulesets/*.yaml` + `rulesets` collection |
| Settings store | `/api/metadata/status|collection|switch` | `metadata/store.py` backends |

---

## 5. Module map (dq-core)

| Package | Responsibility |
|---|---|
| `engine.py` | `DQEngine.run()` entry — wires everything |
| `runtime/` | `config.py` (engine.yaml→dataclasses), `local_spark.py` (session), `jobs`, `utils` |
| `io/` | `sources.py` (CSV/PARQUET/JSON/DELTA/TABLE readers), `sinks.py` (parquet/delta writers, StoragePaths) |
| `orchestration/` | `medallion.py` stage runner, `result.py` |
| `profiling/` | `profiler.py` stats+anomalies, `patterns.py`, `models.py`, **`drift.py`** run-over-run drift |
| `pii/` | **`detector.py`** — name+value PII entity detection → sensitive categories |
| `generation/` | `generator.py` profile→rules, `llm_generator.py` description→rules |
| `contracts/` | `odcs.py` ODCS contract→ruleset |
| `rules/` `validation/` | 18 rule types → Spark expressions, per-rule results |
| `remediation/` | deterministic fixes (trim/case/dedupe/lookup…) |
| `revalidation/` | post-fix re-check pass |
| `ai/` | `base.py` provider iface, `mock.py`, `dqx_provider.py` (LiteLLM→Databricks) |
| `policies/` | `confidence.py` 90% gate + decision memory, `models.py` thresholds/sensitive cols, `approvals.py` stores |
| `quarantine/` | quarantine record routing |
| `metadata/` | `store.py` (TinyDB/Mongo/DynamoDB/SQLite), `resolver.py` store-first loaders, `models.py` |
| `registries/` | ruleset versioning, checksums, publish/rollback |
| `metrics/` | `recorder.py` → metrics collection |
| `alerts/` | `webhook.py` gate events → generic/slack/teams payloads |
| `reporting/` | report JSON writer (rule results, pillars, outcomes, metrics) |
| `cli/` | `dqctl` — bootstrap, run, ruleset import-odcs, seed |

---

## 6. Key flows to narrate in a demo

- **"No hand-written rules"**: profile → `CandidateRuleGenerator` → CANDIDATE
  rules w/ confidence+evidence → steward approve → PUBLISHED ruleset@1.0
- **"AI only above 90%"**: `policies/confidence.py` — deterministic first,
  LLM proposals carry confidence, below threshold → quarantine,
  approvals/rejections remembered forever
- **"Governance everywhere"**: `metadata/store.py` — same collection API on
  TinyDB (laptop) / MongoDB / DynamoDB (AWS) — swap = config only
- **"Portable Spark"**: `runtime/local_spark.py` local session ↔ same
  `engine.run` on Databricks/EMR/Dataproc; `io/sources.py` abstracts
  file/table/delta; `outputs.format: delta` + catalog naming ready
- **"Audit everything"**: `outcome_audit` per record, `quality_pillars`
  7 scores, metrics collection → dashboard trends, reports/*.json
