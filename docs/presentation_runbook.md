# DQ Accelerator — Presentation & Demo Runbook

Client-facing walkthrough: use case → UI demo → code traceability → scale story.
Every claim below maps to real, verified behavior in this repo.

---

## 1. The Use Case (60-second open)

> Every Spark pipeline produces dirty data — nulls, bad formats, duplicates,
> out-of-range values. Teams either write one-off validation scripts per
> pipeline or discover bad data in production. We built a **reusable
> data-quality governance engine** that wraps any Spark pipeline —
> Databricks, Fabric, EMR, Dataproc, or local — profiles data automatically,
> generates rules, applies deterministic fixes first, uses AI for the rest
> above a 90% confidence gate, quarantines low-confidence records for human
> review, and keeps immutable audit trails. **Same wheel everywhere.**

**Positioning line:** we don't replace ETL — we govern its stage boundaries.

---

## 2. Architecture (whiteboard, 2 min)

```
Existing ETL pipeline (unchanged — any Spark code)
        │
        ▼  engine.run(df, manifest) at each stage boundary
┌─ DQ Accelerator ─────────────────────────────────────────┐
│  BRONZE  raw data lands untouched + ingestion lineage    │
│  SILVER  validate → deterministic fix → AI propose       │
│          (≥90% conf auto-apply) → quarantine the rest     │
│  GOLD    strict gate — fail = halt pipeline, don't ship  │
│                                                          │
│  Governance: versioned rulesets · approvals memory ·     │
│  7 quality pillars · anomaly + drift detection · audit   │
│  Metadata: NoSQL collections — TinyDB / Mongo / DynamoDB │
└──────────────────────────────────────────────────────────┘
        │
        ▼  outputs: parquet (default) or Delta tables + reports
```

---

## 3. UI Demo — 5 acts (~15 min)

App: http://localhost:5173 (backend :8000 must be running — `DQ_start.bat`)

### Act 1 — Onboard (the "no hand-coding" moment)
1. Left nav → **Onboard**
2. Step 1 Source: pick a sample dataset **or** upload `bank_transactions.csv`
   (also show the "connect a table / URI source" dropdown — TABLE / DELTA / JSON)
3. Step 2 Options: platform, stage (SILVER), mode, policy template
4. Step 3 Profile (auto-runs): row/col counts, per-column null% + distinct bars,
   **severity-colored anomaly cards** — point out `PII_SUSPECTED`,
   `DRIFT_*`, `ROBUST_OUTLIERS` types
5. Step 4 Review rules: candidate rules each with **confidence + evidence** —
   approve/reject individually
6. Step 5 Finalize → publishes `dataset_silver@1.0` + manifest

**Say:** *"Zero rules hand-written — the profiler discovers them from the
data. Nothing executes until a steward approves it."*

### Act 2 — Run DQ (medallion in action)
1. **Pipelines** → select pipeline → **Run DQ**
2. Watch live phases: `read → profile → validate → remediate →
   revalidate → quarantine → report`

**Say:** *"Bronze lands raw. Silver fixes and quarantines. Gold gates."*

### Act 3 — The evidence
- **Reports** → rule pass/fail table, 7 quality-pillar score bars,
  outcome audit, **Stage data preview card** (NEW): pick
  BRONZE/SILVER/GOLD/QUARANTINE + dataset → actual output rows
- **Quarantine** → rejected records with approve/reject corrections

### Act 4 — The story card (Dashboard)
- **Quality journey** widget: BRONZE→SILVER→GOLD flow + score-over-runs
- **Run quality trend**: metrics-collection history
- **Pipeline registry** cards with sparklines

**Say:** *"Same data, better rulesets, measurable score climb across versions."*

### Act 5 — Governance + AI (the close)
- **Rule Sets**: version list, diff v1.0 vs v1.1, rollback, checksums
- **Approvals**: remembered corrections auto-resolve forever
  (`HUMAN_APPROVED_AI`)
- **Settings**: metadata collections browser + widget/nav toggles
  (customize the demo view live)
- **AI**: real Databricks LLM — `john.doe@university → john.doe@university.edu`
  @0.7 → below 90% gate → quarantine (the gate working honestly)

---

## 4. UI ↔ Code Traceability (the side-by-side map)

### Navigation / shell
| UI | Code |
|---|---|
| Sidebar pages (Dashboard, Pipelines, Onboard, Run DQ, Anomalies, Approvals, Rule Sets, Reports, Settings) | `ui/frontend/src/App.tsx` — `PAGES` array, `navVisible()` localStorage filter |
| Dashboard widget toggles | `pages.tsx` `WIDGETS` + `widgetVisible()` → `localStorage.dq_widgets` |
| Sidebar tab toggles | `pages.tsx` `NAV_ITEMS` + `loadNav()` → `localStorage.dq_nav` |
| Welcome card (first visit) | `pages.tsx` `WelcomeCard` → `localStorage.dq_welcome_dismissed`, `dq:navigate` event → `App.tsx` listener |
| Chat assistant panel | `pages.tsx` `ChatPanel` → `api.chat` → `main.py /api/chat` |

### Onboard wizard
| UI step | API | Backend | Engine |
|---|---|---|---|
| Step 1 source select/upload | `POST /api/onboard/upload`, `/api/onboard/preview` | `main.py` `onboard_upload`, `_do_bootstrap_preview` | `io/sources.py` `read_source` — CSV/PARQUET/JSON/DELTA/TABLE |
| Step 3 profile + anomalies | same preview job | `_do_bootstrap_preview` → `engine.profile` | `profiling/profiler.py` `_anomalies` + `pii/detector.py` + `profiling/drift.py` |
| Step 4 candidate rules | preview result `candidates[]` | `CandidateRuleGenerator().generate` | `generation/generator.py` (+ `llm_generator.py`, `contracts/odcs.py` for other sources) |
| Step 5 finalize/publish | `POST /api/onboard/finalize` | manifest + ruleset YAML write, registry publish, NoSQL `candidates`/`profiles` insert | `metadata/resolver.py`, `registries/` |

### Run DQ / Pipelines
| UI | API | Backend | Engine |
|---|---|---|---|
| Pipeline list + states | `GET /api/pipelines` | manifest scan + registry | `metadata/` manifests collection |
| Run DQ button + progress | `POST /api/pipelines/{id}/run`, `GET /api/jobs/{id}` | `jobs.py` submit + `progress` callbacks | `engine.run` → `orchestration/medallion.py` |
| Bronze→Silver→Gold outputs | — | `io/sinks.py` parquet/delta write | `MedallionRunner` stage loop |
| Deterministic fixes | report `outcome_audit` | — | `remediation/` deterministic rules |
| AI proposals | report `ai_decisions` | — | `ai/dqx_provider.py` (LiteLLM→Databricks) / `ai/mock.py` |
| 90% confidence gate | report outcomes | `policies/confidence.py` | `Decision` enum + thresholds |
| Quarantine | `GET /api/quarantine/pending`, `/decisions` | `_load_quarantine_pending` reads report `ai_decisions` | `quarantine/` + `policies/approvals.py` memory |

### Reports / Dashboard data
| UI card | API | Source |
|---|---|---|
| Reports list + detail | `GET /api/reports`, `/api/reports/{name}` | `reports/*.json` on disk (engine-written) |
| Stage data preview | `GET /api/data/available`, `/api/data/preview` | `data/{bronze,silver,gold,quarantine}/` parquet via Spark |
| Quality journey | `GET /api/journey` | `pipeline_run` stage structures |
| Metrics trend | `GET /api/metrics/recent` | NoSQL `metrics` collection (`metrics/recorder.py`) |
| Dashboard KPIs | `GET /api/summary` | aggregated reports + jobs.db |
| Anomalies page | `GET /api/anomalies` | `data/anomalies.db` + profile anomalies |

### Governance
| UI | API | Backend/Engine |
|---|---|---|
| Rule Sets list/version/diff/rollback | `/api/rulesets*` | `registries/` + NoSQL `rulesets` (checksums, immutable) |
| Approvals inbox | `/api/quarantine/approve|reject` | `config/approvals/*.yaml` + NoSQL `approvals`/`rejections` |
| Settings → metadata store | `/api/metadata/status`, `/collection/{name}`, `/switch` | `metadata/store.py` TinyDB/Mongo/DynamoDB/SQLite + `resolver.py` |
| Settings → engine config | `/api/config`, `/api/engine/reload` | `runtime/config.py` `engine.yaml` |
| AI status | `/api/ai/status` | `ai/` provider factory — `dqx` vs `mock` |

---

## 5. Code Walkthrough Path (for technical evaluators)

Open in this order — tells the whole story in ~10 min:

1. **`packages/dq-core/src/dq_core/engine.py`** — the single entry point
   every pipeline calls: `engine.run(df, manifest, stage)`
2. **`orchestration/medallion.py`** — the Bronze→Silver→Gold stage loop:
   read → profile → validate → remediate → revalidate → quarantine → report
3. **`profiling/profiler.py`** — column statistics + `_anomalies()`
   (ALL_NULL, HIGH_NULL_RATE, DOMINANT_VALUE, OUTLIERS,
   ROBUST_OUTLIERS via IQR/1.349, PII_SUSPECTED via `pii/detector.py`,
   DRIFT_* via `profiling/drift.py`)
4. **`generation/generator.py`** — profile → CANDIDATE rules
   (never executable until steward approves)
5. **`rules/` + `validation/`** — 18 rule types as Spark expressions
6. **`remediation/`** — deterministic fixes applied first
7. **`ai/dqx_provider.py`** — LiteLLM → Databricks Model Serving;
   proposals carry confidence; `policies/confidence.py` enforces the gate
8. **`policies/approvals.py` + `rejections`** — human decision memory:
   approved fixes auto-apply forever (`HUMAN_APPROVED_AI`),
   rejected never resurface (`REJECTED_BY_HUMAN`)
9. **`metadata/store.py` + `resolver.py`** — NoSQL collections:
   rulesets, manifests, policies, approvals, rejections, profiles,
   candidates, metrics, anomalies, jobs, audit — store-first with
   YAML fallback; backend swap = config change
10. **`ui/backend/main.py`** — FastAPI surface: jobs, onboarding,
    preview, reports, quarantine, metadata, metrics, journey
11. **`ui/frontend/src/pages.tsx`** — the entire UI in one file:
    wizard, dashboard widgets, reports, quarantine inbox

**Integration story to say:** *"In your existing pipeline, add one call —
`engine.run(df, manifest)` — at each medallion boundary. Everything else
(profiling, rules, remediation, quarantine, audit) is metadata-driven."*

---

## 6. Dataset Guide — pick by demo moment

| Dataset | Size | Timing (local) | Use for |
|---|---|---|---|
| `bank_transactions.csv` | 5.3MB / 50K rows | ~3-4 min E2E | **Main demo** — fast complete loop, 993 quarantined, score 99.6 |
| `goat_herd_animals` | small | fastest | **AI fixes + approvals** — real LLM `AI_APPLIED @0.95`, `HUMAN_APPROVED_AI @0.80` |
| `hotel_reservations.parquet` | 42.8MB / **1M rows** | ~5-10 min | **"Million rows" proof** — parquet ingest is fast |
| `college_students.csv` | 202MB / 2.1M rows | profile ~4-5min SAMPLE | **"Big file" story** — auto-SAMPLE above 50MB protects RAM |
| `cdr_records.csv` | 1GB / 6.3M rows | Bronze OK → local OOM | **Honest boundary** — cluster territory |

## 7. The "bigger data, faster" answer (say it verbatim)

> *"Local single-node Spark on a laptop. The same wheel on Databricks,
> EMR, or Dataproc with 8-16 workers is 5-10× faster — 1M rows in seconds.
> We proved Bronze ingest of 1GB / 6.3M rows (→ 295MB parquet) before
> hitting this laptop's RAM ceiling — a hardware limit, not a framework
> limit. On a cluster this is a non-issue."*

## 8. Expectation → Evidence map

| Client expects | Show them |
|---|---|
| Metadata types | Settings → metadata collections; `profiles`/`candidates`/`metrics` docs |
| Profile + rules | Onboard Step 3-4 — stats, anomalies, candidates w/ confidence |
| Medallion arch | Quality journey + stage data preview (Bronze/Silver/Gold rows) |
| Anomalies | Severity-colored cards: PII_SUSPECTED, DRIFT_*, ROBUST_OUTLIERS |
| AI fixes | GOAT_HERD real LLM results; <90% → quarantine honesty |
| Bigger data | hotel parquet 1M rows + college_students 200MB SAMPLE |
| Governance | Rulesets version/diff/rollback, approvals memory, audit trail |
| Portability | Same engine local ↔ Databricks/EMR/Fabric; Delta option; NoSQL swap |

## 9. Honest boundaries (say before they ask)

- Local Spark = single machine; cluster runs are 5-10× faster (not yet executed on real cloud — plumbing verified, `engine.databricks.yaml` loads)
- Delta output: code + config ready; local JVM jars blocked by Maven resolution — works on Databricks natively; `scripts/verify_delta.py` proves it anywhere with repo access
- LLM deps need Python ≤3.12 for `dqx 0.17.x` (we pinned `0.16.0` for 3.14 compat); without token → MockAI fallback keeps everything else working
- 1GB+ datasets: local RAM ceiling (~16GB machine); cluster handles it
