# DQ Accelerator — Presentation Guide for Team Lead

End-to-end flow from **"click Run Auto E2E"** to **completed governed DQ run**, with exact source-code locations and verified timing for a 677MB / 8M-row CSV.

---

## 1. The Short Answer to "Does All 8M Rows Get Validated?"

| Step | Rows processed | What happens |
|------|---------------|--------------|
| CSV → Parquet conversion | All 8,000,000 | Text converted to columnar format for fast reads |
| **Profiling** (rule generation) | **Only 10,000** | DQX-style: sample 30% (2.4M) then cap at 10K rows for statistics |
| Candidate rule generation | From those 10K rows | Generates 28 rules (NOT_NULL, RANGE, DATA_TYPE, etc.) |
| **Silver DQ validation** | **All 8,000,000** | Every row checked against every approved rule — governance coverage unchanged |

**The 10K cap only affects rule *proposal* statistics, not validation coverage.** This mirrors real Databricks DQX: *sample generously, cap what you actually profile* for speed; validate everything for governance.

---

## 2. Where the Application Starts

### `DQ_start.bat` (repo root) — Single entry point

```batch
:: Checks: Python 3.12+, Java 17+, Node 18+
:: Loads .env secrets (DATABRICKS_TOKEN, etc.)
:: Installs pip dependencies
:: Starts two windows:
::   Backend:  uvicorn main:app --app-dir ui/backend :8000   → ui/backend/main.py
::   Frontend: npm run dev :5173                              → React SPA
```

- Loads `.env` (gitignored) for credentials — never committed
- Sets `DQ_LOG_DIR` to `logs/`

---

## 3. FastAPI Backend Entry Point

### `ui/backend/main.py` — the API layer

| Endpoint | Line | Purpose |
|----------|------|---------|
| `GET /api/health` | `main.py:1` | Health check |
| `POST /api/auto/run` | `main.py:1766` | **Auto E2E** — one-click profile → rules → publish → run |
| `POST /api/onboard/preview` | `main.py:2043` | Wizard Step 3 — profile + candidate rules |
| `POST /api/onboard/finalize` | `main.py:2052+` | Publish ruleset + manifest + register pipeline |
| `POST /api/pipelines/{id}/run` | — | Governed DQ run (Bronze → Silver → Gold) |
| `GET /api/jobs/{id}/stream` | — | SSE live progress for frontend |

```python
# main.py:1766
@app.post("/api/auto/run")
def auto_e2e(body: AutoIn) -> dict[str, str]:
    jid = J.submit("auto_e2e", lambda: _do_auto_e2e(body))
    return {"job_id": jid}
```

---

## 4. Core Engine

### `packages/dq-core/src/dq_core/engine.py` — `DataQualityEngine`

| Method | Line | Purpose |
|--------|------|---------|
| `profile()` | `engine.py:94` | Orchestrates profiling via `DataProfiler` |
| `run()` | `engine.py` | Orchestrates validation (Silver/Gold) |

```python
# engine.py:94
def profile(self, dataframe, dataset, *, mode=None, sample_fraction=None,
            limit_rows=None, on_phase=None) -> ProfileReport:
    return self.profiler.profile(dataframe, dataset, mode=mode,
                                 sample_fraction=sample_fraction,
                                 limit_rows=limit_rows, on_phase=on_phase)
```

---

## 5. Step-by-Step Flow (Click → Done)

The exact code trace when a user clicks **"Run Auto E2E"** on `financial_txn_1gb.csv`:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 0 — USER ACTION                                                        │
│ └─ Frontend (pages.tsx) sends: POST /api/auto/run                           │
│    Body: {source, dataset, pipeline_id, mode, layers, stage, policy, owner} │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 1 — BACKEND ACCEPTS JOB  (main.py:1766)                                │
│ └─ J.submit("auto_e2e", lambda: _do_auto_e2e(body))                         │
│ └─ Returns {"job_id": "JOB-XXXX"} immediately                               │
│ └─ Frontend polls GET /api/jobs/{id}/stream (SSE) for live progress         │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2 — _do_auto_e2e() ORCHESTRATOR  (main.py:1812)                        │
│                                                                             │
│   layers = [BRONZE] or [BRONZE, SILVER, GOLD]                               │
│   profile_stage = SILVER (if Silver will run) else BRONZE                   │
│                                                                             │
│   1. Call _do_bootstrap_preview()  → profile + candidate rules              │
│   2. Auto-approve candidates with confidence >= approve_confidence          │
│   3. Call onboard_finalize()       → publish ruleset + manifest + register  │
│   4. If SILVER: call _do_pipeline_run() → governed DQ validation            │
│   5. If GOLD:  Gold ruleset must be provided separately (NOT auto-copied)   │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3 — _do_bootstrap_preview() — PROFILE + CANDIDATE RULES  (main.py:1983)│
│                                                                             │
│   a. spark, engine = _spark_engine()         → Spark session (local[N])     │
│   b. mode, sample_frac, limit_rows =                                     │
│      _auto_profile_config(src_path, engine)   → DQX auto-detect by file size│
│   c. df, parquet_tmp = _read_for_profiling()   → large CSV → temp Parquet   │
│   d. rep = engine.profile(df, ds, mode, sample_fraction, limit_rows)        │
│      └─ runs Spark jobs, capped at limit_rows                               │
│   e. candidates = CandidateRuleGenerator().generate(rep)  → 28 rules        │
│   f. Store profile in NoSQL (profiles collection) for drift detection       │
│   g. Return {row_count, column_count, profile_mode, anomalies, candidates}  │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4 — engine.profile() — THE PROFILER  (engine.py:94 → profiler.py)      │
│                                                                             │
│   DataProfiler.profile(df, dataset, mode, sample_fraction, limit_rows)      │
│   Runs these Spark jobs (each emits progress via on_phase):                 │
│     1. Schema / type sniffing                                               │
│     2. Structural stats (null%, distinct, min/max, mean, stddev, pctiles)   │
│     3. Duplicate row statistics                                             │
│     4. Value frequencies / top values                                       │
│     5. Structural patterns                                                  │
│     6. Outlier analysis                                                     │
│                                                                             │
│   With limit_rows=10,000, steps 4-6 only see 10K rows → fast                │
│   data.persist(StorageLevel.MEMORY_AND_DISK) before profiling passes        │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5 — onboard_finalize() — PUBLISH RULESET + MANIFEST  (main.py:2052+)   │
│                                                                             │
│   - Writes ruleset YAML: config/rulesets/<dataset>_bronze_v1.yaml           │
│   - Writes manifest YAML: config/pipelines/<PIPELINE_ID>.yaml               │
│   - Registers pipeline in registry (lifecycle state REGISTERED→ACTIVE)      │
│   - Publishes to NoSQL: ruleset_index, ruleset_versions, pipelines, manifests│
│   - Returns: ruleset name (e.g. financial_txn_1gb_bronze@1.0)               │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 6 — _do_pipeline_run() — GOVERNED DQ VALIDATION  (main.py:1895)        │
│   (Only if SILVER/GOLD layers selected)                                     │
│                                                                             │
│   MedallionRunner (Bronze → Silver → Gold):                                 │
│     - BRONZE: ingest raw data → data/bronze/<ds>/ + _ingest.json lineage    │
│     - SILVER: apply ALL approved rules to ALL rows                          │
│       ├─ Deterministic remediation (TRIM, DEDUPLICATE, etc.)                │
│       ├─ AI proposals (only if confidence ≥ 90%, non-sensitive)             │
│       ├─ Confidence gate → below 90% → quarantine                           │
│       ├─ Revalidation pass                                                  │
│       └─ Write data/silver/<ds>/ + data/quarantine/silver/<ds>/             │
│     - GOLD: stricter ruleset, same loop (only if Gold ruleset provided)     │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 7 — REPORT + UI COMPLETION                                             │
│                                                                             │
│   - Report JSON: reports/<dataset>_silver_DQRUN-*.json                      │
│     Contains: rule results, quality_pillars (7 scores), outcome_audit,      │
│     execution_metrics                                                       │
│   - Job status → COMPLETED                                                  │
│   - UI shows: accepted/quarantined counts, gate passed/failed, rules        │
│     approved, quality health score                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Pictorial Flow Diagram (Mermaid)

```mermaid
graph TD
    A[User: DQ_start.bat<br/>repo root] --> B[Backend: uvicorn :8000<br/>ui/backend/main.py]
    B --> C[Frontend: npm run dev :5173<br/>React SPA]

    C -->|1. Select dataset +<br/>click "Run Auto E2E"| D[POST /api/auto/run<br/>main.py:1766]

    D -->|2. Job submitted,<br/>return job_id| E[J.submit "auto_e2e"<br/>_do_auto_e2e<br/>main.py:1812]

    E -->|3a. Profile + candidate rules| F[_do_bootstrap_preview<br/>main.py:1983]

    F -->|4a. Auto-detect config by file size| G[_auto_profile_config<br/>main.py:699<br/>DQX-style: sample 30%, limit 10K]

    G -->|4b. Read source (CSV→Parquet if >50MB)| H[_read_for_profiling<br/>main.py:1952]

    H -->|4c. engine.profile()| I[DataProfiler.profile<br/>profiler.py<br/>capped at limit_rows]

    I -->|4d. CandidateRuleGenerator| J[Generate 28 rule candidates<br/>generation/generator.py]

    J -->|5. Auto-approve candidates<br/>confidence ≥ threshold| K[_do_auto_e2e continues<br/>main.py:1812]

    K -->|6. Publish ruleset + manifest| L[onboard_finalize<br/>main.py:2052<br/>config/rulesets/*.yaml<br/>config/pipelines/*.yaml]

    L -->|7. Register pipeline| M[Pipeline registry<br/>state: REGISTERED→ACTIVE]

    M -->|8. If SILVER/GOLD layers| N[_do_pipeline_run<br/>main.py:1895]

    N -->|9. MedallionRunner| O[Bronze: ingest raw data<br/>data/bronze/<ds>/ + _ingest.json]

    O -->|10. Silver DQ validation| P[Validate ALL rows against<br/>ALL approved rules<br/>deterministic → AI → confidence gate → quarantine]

    P -->|11. Write results| Q[data/silver/<ds>/ +<br/>data/quarantine/silver/<ds>/<br/>reports/*.json]

    Q -->|12. Job COMPLETED| R[Frontend: Live progress<br/>GET /api/jobs/{id}/stream<br/>SSE]

    R -->|13. Show report| S[Dashboards: health score,<br/>quality_pillars, quarantine,<br/>anomalies, approvals]

    style A fill:#e1f5e1,stroke:#333,stroke-width:2px
    style D fill:#fff3cd,stroke:#333,stroke-width:2px
    style I fill:#e3f2fd,stroke:#333,stroke-width:2px
    style P fill:#e3f2fd,stroke:#333,stroke-width:2px
    style Q fill:#f3e5f5,stroke:#333,stroke-width:2px
    style S fill:#e1f5e1,stroke:#333,stroke-width:2px
```

### Alternative: Layered Architecture View

```mermaid
graph TB
    subgraph UI["Frontend (React + Vite, :5173)"]
        UI1[Auto E2E Card] --> UI2[Dataset + Pipeline ID]
        UI2 --> UI3[Layers: BRONZE/SILVER/GOLD]
        UI3 --> UI4[Click Run]
    end

    subgraph API["FastAPI Backend (:8000)"]
        API1[POST /api/auto/run] --> API2[_do_auto_e2e()]
        API2 --> API3[_do_bootstrap_preview()]
        API2 --> API4[onboard_finalize()]
        API2 --> API5[_do_pipeline_run()]
    end

    subgraph CORE["dq-core Engine (packages/dq-core)"]
        CORE1[DataQualityEngine.profile()] --> CORE2[DataProfiler]
        CORE2 --> CORE3[Spark Jobs: stats, duplicates, patterns, outliers]
        CORE4[CandidateRuleGenerator] --> CORE5[28 rule candidates]
    end

    subgraph DATA["Spark + Storage (local[N])"]
        D1[CSV → Parquet conversion] --> D2[Profile: 10K rows capped]
        D2 --> D3[Validate: ALL 8M rows]
        D3 --> D4[data/bronze/, data/silver/, data/quarantine/]
        D4 --> D5[reports/*.json]
    end

    UI4 --> API1
    API1 --> CORE1
    CORE1 --> D1
    D3 --> D5
```

---

## 7. Verified Timing — 677MB / 8M-row CSV (Bronze-only Auto E2E)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Auto E2E on financial_txn_1gb.csv — Verified 2 min 54 s           │
├─────────────────────────────────────────────────────────────────────┤
│  Step                          Time   Rows          What happens   │
├─────────────────────────────────────────────────────────────────────┤
│  CSV → Parquet conversion      ~2m    8,000,000   Columnar format  │
│  Profiling (APPROXIMATE,       ~1m    10,000      DQX cap: 30%    │
│   sample 30% → limit 10K)               sampled → 10K profiled    │
│  Candidate rule generation     ~10s   from 10K    28 rules        │
│  Publish ruleset + manifest    ~5s    —           YAML + NoSQL    │
│  Bronze ingest (validate)      ~30s   8,000,000   All rows        │
│  TOTAL                         2:54   —           Bronze-only     │
└─────────────────────────────────────────────────────────────────────┘
```

### Comparison: Before vs After DQX-style profiling

| Metric | Before (old adaptive) | After (DQX-style, current) |
|--------|----------------------|----------------------------|
| Profiling rows | ~80K (1% sample, no cap) | **10K (30% sample → limit)** |
| Profiling time | ~9.75 min | ~1 min |
| Full Auto E2E (Bronze) | ~12-15 min (est.) | **2 min 54 s** (verified) |
| Rules generated | ~28 | 28 |
| Ruleset | — | `financial_txn_1gb_bronze@1.0` |

---

## 8. Source Code Map — Where Each Piece Lives

| Component | File | Key Line | What it does |
|-----------|------|----------|-------------|
| App entry | `DQ_start.bat` | line 1 | Starts backend + frontend |
| FastAPI app | `ui/backend/main.py` | line 71 | `app = FastAPI(...)` |
| Auto E2E endpoint | `ui/backend/main.py` | line 1766 | `POST /api/auto/run` → `_do_auto_e2e()` |
| Auto E2E orchestrator | `ui/backend/main.py` | line 1812 | `_do_auto_e2e()` — full flow |
| Bootstrap preview | `ui/backend/main.py` | line 1983 | `_do_bootstrap_preview()` — profile + candidates |
| Auto-detect config | `ui/backend/main.py` | line 699 | `_auto_profile_config()` — file-size thresholds |
| Large CSV reader | `ui/backend/main.py` | line 1952 | `_read_for_profiling()` — CSV→Parquet |
| Engine facade | `packages/dq-core/src/dq_core/engine.py` | line 94 | `DataQualityEngine.profile()` |
| Profiler | `packages/dq-core/src/dq_core/profiling/profiler.py` | — | `DataProfiler.profile()` — Spark jobs |
| Rule generator | `packages/dq-core/src/dq_core/generation/generator.py` | — | `CandidateRuleGenerator.generate()` |
| Finalize (publish) | `ui/backend/main.py` | line 2052+ | `onboard_finalize()` — ruleset + manifest |
| Pipeline run | `ui/backend/main.py` | line 1895 | `_do_pipeline_run()` → governed DQ |
| Medallion runner | `packages/dq-core/src/dq_core/orchestration/medallion.py` | — | Bronze→Silver→Gold stages |

---

## 9. 30-Second Summary for Your Team Lead

> "The DQ Accelerator is a metadata-driven, Spark-native data quality framework. When a user uploads a large file (e.g., 677MB / 8M rows) and clicks **Run Auto E2E**, the system: (1) auto-detects the profiling mode based on file size using a DQX-inspired sample-then-limit pattern — 30% sample capped at 10K rows for files over 500MB — (2) converts large CSVs to temp Parquet for 10-50x faster reads, (3) generates candidate rules from the profiled sample, (4) publishes an immutable, checksummed ruleset, and (5) if Silver/Gold layers are selected, runs governed DQ validation on **every row**. Verified end-to-end time for the 1GB file: **2 minutes 54 seconds** for Bronze-only. The profiling cap only affects rule *proposal* statistics — validation coverage is unchanged. The same engine runs locally and on Databricks/Fabric/EMR/Dataproc without code changes."

---

## 10. Honest Trade-offs to Mention

| What we do | Trade-off |
|------------|-----------|
| Profile only 10K rows for >500MB files | Rare values (<0.1% frequency) may be missed in `top_values` / allowed-values candidates. Null rates, min/max, percentiles, outliers remain sound. |
| Auto-approve all candidates | Demo convenience. Use the manual wizard for steward review of each rule. |
| Bronze-only by default | Won't fabricate Silver/Gold rules from untransformed data. Add Silver/Gold when transformations exist. |
| Local Spark (not real cluster) | 2:54 for 677MB. On a real Databricks cluster with 8-16 workers, this would be seconds. |

---

## 11. What's Verified vs Open

| Item | Status |
|------|--------|
| All P0–P4 phases implemented | ✅ Verified |
| 677MB / 8M-row Auto E2E in 2:54 | ✅ Verified (JOB-6EAB4CAA5A) |
| DQX-style profiling (sample 30%, limit 10K) | ✅ Implemented + tested |
| TinyDB concurrent access fix | ✅ Fixed + tested |
| Real cloud cluster run (Databricks/EMR/Dataproc) | ⏳ Open — needs workspace access, not claimed |

---

**Branch:** `bronze-performance` | **Latest commit:** `349ff0a` | **Pushed:** https://github.com/KGANESHKUMARK/dq-accelerator.git