# Architecture — HLD, LLD, and Current POC Status

> **Current state:** local Spark POC verified end-to-end — P0–P3 complete
> (physical medallion persistence, managed Bronze ingestion, outcome audit,
> seven quality pillars). P4 partial: identical outcomes under all platform
> labels + Spark 3.5↔4.x matrix; a real cloud Spark run remains open.
>
> **Scope authority:** `docs/poc/POC_CONTEXT.md`. Do not extend into production
> orchestration while P0–P4 in `ROADMAP.md` are open.

## 0. Architecture views

### 0.1 HLD — layered architecture

```mermaid
flowchart TB
    USER["Data steward / pipeline owner"]

    subgraph L1["① UI LAYER"]
        UI["React UI — 10 pages + assistant"]
    end
    style L1 fill:#e3f2fd,stroke:#1565c0

    subgraph L2["② API LAYER — FastAPI"]
        API["REST endpoints + SSE progress"]
        WRK["Job worker — auto_e2e · dq_run · pipeline_run · profile"]
    end
    style L2 fill:#e8f5e9,stroke:#2e7d32

    subgraph L3["③ ENGINE LAYER — dq-core · portable PySpark"]
        ENG["DataQualityEngine<br/>Profiler · RuleGen · Validation<br/>Remediation · MedallionRunner · Registries"]
    end
    style L3 fill:#fff3e0,stroke:#e65100

    subgraph L4["④ CONFIG & PERSISTENCE"]
        CFG["config/*.yaml<br/>rulesets · policies · approvals · manifests"]
        META[("metadata store<br/>metadata.nosql.json · jobs.db · anomalies.db")]
        OUT[("data/ + reports/<br/>uploads → bronze·silver·gold<br/>quarantine · audit · reports")]
    end
    style L4 fill:#f3e5f5,stroke:#6a1b9a

    subgraph EXT["EXTERNAL SYSTEMS"]
        LLM["Databricks Model Serving / MockAI"]
        CLD["Host pipelines — Databricks · EMR · Fabric · Dataproc"]
    end
    style EXT fill:#eceff1,stroke:#546e7a,stroke-dasharray:5 5

    USER --> UI
    UI -->|"REST + SSE"| API
    API --> WRK
    WRK -->|"DataFrame · ruleset · policy · mode"| ENG
    ENG -->|"rulesets · policies · lifecycle"| CFG
    ENG -->|"profiles · pipelines · metrics"| META
    ENG -->|"persist medallion · quarantine · audit · reports"| OUT
    WRK -->|"job state · progress"| META
    ENG -.->|"values"| LLM
    LLM -.->|"proposals · corrections"| ENG
    CLD -.->|"engine.run(df, ruleset, policy)"| ENG
```

All nodes above are implemented and verified on local Spark — including
physical Bronze/Silver/Gold persistence (P0), managed Bronze ingest manifests
(P1), terminal outcome audit (P2), seven quality pillars (P3), and
cross-Spark-version portability evidence (P4).

### 0.1a Two onboarding paths — dataset vs pipeline

Dataset onboarding *discovers* the contract from data; pipeline onboarding
*governs* an already-running pipeline that references published rulesets.
They share the same manifest format and lifecycle.

```mermaid
flowchart TB
    subgraph DS["Dataset onboarding — discover & publish"]
        direction TB
        U["Source file<br/>upload or sample_data"] --> P["Profile<br/>size-tiered sampling"]
        P --> C["Candidate rules<br/>confidence + evidence"]
        C --> RV{"Steward review<br/>approve / reject"}
        RV -->|"auto-approve (Auto E2E)"| PV["Publish ruleset<br/>immutable + checksum"]
        RV -->|"manual approve"| PV
        PV --> MF["Manifest + pipeline<br/>REGISTERED"]
    end

    subgraph PO["Pipeline onboarding — govern existing pipeline"]
        direction TB
        MY["Owner-supplied manifest YAML<br/>platform · runtime · datasets<br/>rulesets.SILVER/GOLD · policy"]
        MY --> OB["POST /api/pipelines/onboard<br/>(no profiling required)"]
        OB --> LC["Lifecycle<br/>REGISTERED→TESTED→ACTIVE⇄PAUSED"]
        LC --> RN["POST /run → MedallionRunner<br/>engine.run() contract on DataFrame"]
    end

    MF -. "produces the manifest format" .-> MY
```

Key distinction: `rulesets.BRONZE` in a manifest is a **contract artifact**
(published by dataset onboarding); `rulesets.SILVER`/`GOLD` are the
**enforcement stages** — `MedallionRunner` always persists Bronze raw, then
requires a SILVER ruleset for first validation. A BRONZE-only Auto E2E run
generates the contract but performs **no row-level validation**.

### 0.1b Auto E2E — single-job orchestration

```mermaid
sequenceDiagram
    participant UI as Onboard UI
    participant API as FastAPI /api/auto/run
    participant W as jobs.py worker
    participant E as dq-core
    participant M as metadata store

    UI->>API: POST /api/auto/run {source, layers[], mode}
    API->>W: submit auto_e2e job (SSE progress)
    W->>E: profile(source) — size-tiered sampling
    E-->>W: profile report + anomalies
    W->>E: generate candidate rules
    W->>E: auto-approve candidates
    W->>M: publish ruleset version(s)
    W->>M: write manifest + register pipeline
    alt SILVER or GOLD selected
        W->>E: MedallionRunner.run — BRONZE→SILVER→GOLD
        E-->>W: accepted / quarantine / gate + written_paths
    else BRONZE only
        Note over W: contract generation only — no row-level validation
    end
    W-->>UI: job result → dashboard metrics
```

### 0.1c Profiling & validation sampling policy

Profiling is discovery (bounded sample); governed enforcement validates all
rows. Ad-hoc `dq_run` samples large files; `pipeline_run` never samples.

| Trigger | Mode | Sample | Row cap |
|---|---|---|---|
| Profile < 50 MB | FULL | — | — |
| Profile 50–100 MB | SAMPLE | 5 % | 100 K |
| Profile 100–500 MB | SAMPLE | 10 % | 50 K |
| Profile > 500 MB | APPROXIMATE | 30 % | 10 K |
| `dq_run` file > 50 MB | — | ~1 %, seed 42 | ~100 K target |
| `pipeline_run` governed | — | none — all rows | — |

Thresholds: `engine.yaml → validation.large_file_threshold_mb` (50),
`large_file_sample_rows` (100 000); profile tiers in
`ui/backend/main.py::_auto_profile_config`.

### 0.2 HLD — deployment boundary

```mermaid
flowchart TB
    subgraph LOCAL["Verified now"]
        LS["Local Spark 4.1.3"]
        APP["UI + FastAPI + dq-core"]
        LLM["Remote Databricks Model Serving"]
    end
    subgraph TARGET["Same engine contract, not yet cloud-executed"]
        DBX["Databricks Spark"]
        FAB["Fabric Spark"]
        EMR["Amazon EMR"]
        DPC["Google Dataproc"]
    end
    APP --> LS
    APP -. verified LLM call .-> LLM
    APP -. portable code only .-> DBX & FAB & EMR & DPC
```

### 0.3 LLD — one stage execution

```mermaid
sequenceDiagram
    participant Host as Host pipeline / API
    participant Engine as DataQualityEngine
    participant Rules as RuleSet registry
    participant Spark as Caller SparkSession
    participant Policy as Confidence policy
    participant Store as Stage sink

    Host->>Engine: run(existing DataFrame, ruleset, policy, stage)
    Engine->>Rules: resolve version and checksum
    Engine->>Spark: validate with native expressions
    Engine->>Spark: apply deterministic fixes
    Engine->>Policy: evaluate AI proposals and approvals
    Policy-->>Engine: apply, approval, below threshold, or no proposal
    Engine->>Spark: revalidate corrected rows
    Engine->>Store: write accepted, quarantine, audit, report
    Engine-->>Host: accepted DataFrame + DQResult
```

The sink call is implemented (`StorageSink` + `StoragePaths`): every stage
physically persists accepted/quarantine/audit outputs plus JSON reports.

### 0.4 Persisted layout (as implemented)

```text
data/
  uploads/                            user-uploaded sources (kept by factory reset)
  bronze/<dataset>/                  raw landing (parquet) + _ingest.json manifest
  silver/<dataset>/                  accepted Silver output
  gold/<dataset>/                    accepted Gold output
  quarantine/<stage>/<dataset>/      quarantined rows + dq_* provenance
  quarantine/<stage>/<dataset>_removed_duplicates/   dedupe audit rows
  audit/<stage>/<dataset>_remediation_log/           per-record fix/proposal log
  metadata.nosql.json                document store — pipelines, rulesets,
                                     manifests, profiles, candidates, jobs
                                     (tinydb embedded | mongodb | dynamodb)
  jobs.db                            SQLite job records + progress history
  anomalies.db                       anomaly lifecycle (OPEN/ACK/RESOLVED)
reports/
  <dataset>_<stage>_<run>.json   execution report (metrics, pillars, outcomes)
config/
  rulesets/*.yaml                 published immutable ruleset versions (checksum-sealed)
  pipelines/*.yaml                pipeline manifests (per dataset/pipeline)
  policies/*.yaml                 remediation policies (default, healthcare, …)
  approvals/*.yaml                steward approvals + rejections (memory)
  ai/, rules/lookups/             mock AI responses, lookup tables
registry/                         file-backend fallback only (backend=file)
```

### 0.5 Pipeline lifecycle state machine

Enforced by `PipelineRegistry.TRANSITIONS`; append-only history with actor +
reason on every transition. `PAUSED`, `DEACTIVATING`, `OFFBOARDED` are
pass-through states — data flows untouched.

```mermaid
stateDiagram-v2
    [*] --> DRAFT
    DRAFT --> REGISTERED: onboard (manifest validated)
    REGISTERED --> TESTED: test run
    REGISTERED --> DEACTIVATING: offboard
    TESTED --> TESTED: re-test
    TESTED --> ACTIVE: activate
    TESTED --> DEACTIVATING: offboard
    ACTIVE --> PAUSED: pause
    ACTIVE --> DEACTIVATING: offboard
    PAUSED --> ACTIVE: resume
    PAUSED --> TESTED: re-test
    PAUSED --> DEACTIVATING: offboard
    DEACTIVATING --> OFFBOARDED: finalize (data + history kept)
    OFFBOARDED --> DRAFT: re-onboard
```

## 1. Design goals

1. **Drop-in for existing PySpark pipelines** — the engine accepts an existing
   `DataFrame`, uses its own `SparkSession`, and returns DataFrames the pipeline
   continues with. No new Spark application, no session lifecycle ownership.
2. **Portable core** — `dq-core` has zero Databricks imports; the same wheel runs on
   Databricks, Dataproc, Fabric Spark, and standalone Spark.
3. **Metadata-driven, reviewable rules** — rules are versioned, checksummed,
   immutable once published, and every execution records the exact rule set version.
4. **Safe remediation** — deterministic fixes first, AI proposals second, confidence
   threshold (default 90 %), sensitive-column approval gates, mandatory
   post-correction revalidation, quarantine for everything uncertain.
5. **Observable** — every run emits metrics, a JSON audit report and per-record
   quarantine rows with full provenance.

## 2. High-level component map

```mermaid
flowchart LR
    subgraph Host["Host pipeline (Databricks notebook / job / standalone Spark)"]
        DF[("Existing Spark DataFrame")]
    end

    subgraph UI["Web console (local POC)"]
        FE["React SPA<br/>Dashboard · Pipelines · Onboard · Run DQ<br/>Anomalies · Approvals · Rule Sets<br/>Reports · Settings · Assistant"]
        API["FastAPI<br/>jobs + SSE progress · lifecycle<br/>approvals/rejections · reports"]
        FE --> API
    end

    subgraph DQ["dq-core engine"]
        ENG["DataQualityEngine<br/>(facade)"]
        PROF["Profiler<br/>full / sample / approx"]
        GEN["CandidateRuleGenerator"]
        VAL["ValidationEngine<br/>native PySpark exprs"]
        REM["Remediation<br/>deterministic + AI"]
        POL["Confidence &<br/>sensitive-data policy"]
        APRV["ApprovalStore +<br/>RejectionStore"]
        QAR["Quarantine +<br/>Revalidator"]
        REG["RuleSet &<br/>Pipeline registries"]
        MET["Metrics +<br/>JSON reports + pillars"]
    end

    subgraph Store["Config & state"]
        RS["config/rulesets/*.yaml"]
        POLY["config/policies/*.yaml"]
        META["metadata.nosql.json<br/>tinydb | mongodb | dynamodb<br/>pipelines · rulesets · manifests · profiles"]
        JOBS["data/jobs.db · anomalies.db"]
        AI["config/ai/mock_ai_responses.yaml"]
        APR["config/approvals/*.yaml"]
    end

    subgraph Out["Outputs"]
        ACC[("accepted DataFrame")]
        QUAR[("quarantine DataFrame")]
        REP["reports/*.json"]
        MED["physical Bronze/Silver/Gold/Quarantine/Audit sinks"]
    end

    DF --> ENG
    API --> ENG
    ENG --> PROF --> GEN
    ENG --> VAL --> REM --> POL --> APRV --> QAR
    ENG --> REG
    ENG --> MET
    RS & POLY & AI & APR --> ENG
    META <--> REG
    API --> JOBS
    ENG --> ACC & QUAR & REP
    ENG --> MED
```

## 3. Runtime layering

```mermaid
flowchart TB
    subgraph L1["Integration layer (thin)"]
        CLI["dqctl CLI"]
        DBX["Databricks notebook adapter"]
        ADP["PlatformAdapter contracts<br/>(Dataproc / Fabric / Airflow / ADF)"]
    end
    subgraph L2["Orchestration"]
        RUN["DQRunner – mode dispatch,<br/>quarantine build, gate eval"]
        MED["MedallionRunner – B/S/G flow"]
        RES["DQResult / DQResult contracts"]
    end
    subgraph L3["Engine core"]
        ENG["DataQualityEngine facade"]
        VAL["ValidationEngine"]
        REM["Deterministic + AI remediation"]
        REV["Revalidator"]
    end
    subgraph L4["Metadata & policy"]
        MOD["RuleDefinition / RuleSet models"]
        REG["RuleSetRegistry (immutable versions)"]
        PRE["PipelineRegistry (lifecycle)"]
        POL["RemediationPolicyConfig / approvals"]
    end
    subgraph L5["Runtime"]
        CFG["EngineConfig (YAML)"]
        SES["Spark session handling (never owned)"]
        SCH["Schema utilities"]
        LOG["Structured logging"]
        ERR["Typed errors"]
    end
    subgraph L6["Spark"]
        SP["PySpark DataFrame API only"]
    end

    L1 --> L2 --> L3 --> L4 --> L5 --> L6
```

## 4. End-to-end data flow (what actually executes)

```mermaid
flowchart TD
    A["Raw DataFrame<br/>(existing pipeline output)"] --> B{"Mode?"}

    B -->|OBSERVE_ONLY| V1["Validate + profile"]
    V1 --> O1["accepted = ALL rows<br/>failures flagged in report/quarantine"]

    B -->|ENFORCE| V2["Validate"]
    V2 --> O2["clean rows → accepted<br/>failed rows → quarantine"]

    B -->|REMEDIATE| V3["Validate"]
    V3 --> D["Deterministic fixes<br/>(approved actions only)"]
    D --> R1{"revalidate"}
    R1 -->|still failing| AI["AI proposals<br/>(batched, deduped)"]
    R1 -->|clean| ACC2["accepted"]
    AI --> P{"human decision?<br/>then confidence ≥ 0.90?<br/>not sensitive / approved?"}
    P -->|steward rejected| QR["quarantine:<br/>REJECTED_BY_HUMAN (permanent)"]
    P -->|approved by human| AP["apply correction<br/>→ HUMAN_APPROVED_AI"]
    P -->|yes, auto-apply| AP2["apply correction<br/>→ AI_APPLIED"]
    P -->|no| Q1["quarantine:<br/>BELOW_THRESHOLD / SENSITIVE / NO_PROPOSAL"]
    AP & AP2 --> R2{"revalidate"}
    R2 -->|clean| ACC2
    R2 -->|fails| Q2["quarantine:<br/>CORRECTION_FAILED_REVALIDATION"]

    O2 --> G["Quality gate<br/>max_quarantine_rate etc."]
    ACC2 --> G
    Q1 & Q2 & QR --> QUAR[("quarantine DataFrame<br/>dq_* audit columns")]
    G --> REP["DQResult → JSON report + metrics"]
```

## 5. Execution modes

| Mode | Validation | Deterministic fixes | AI proposals | Output effect |
|---|---|---|---|---|
| `OBSERVE_ONLY` | yes | no | collected only | `accepted` = all input rows; failures flagged in quarantine/report; pipeline output unchanged |
| `ENFORCE` | yes | no | no | failing rows → quarantine; clean rows → accepted |
| `REMEDIATE` | yes → revalidate | yes (approved only) | yes (policy-gated) | corrected rows revalidated → accepted; unresolved → quarantine |

Mode resolution order: explicit `mode=` argument → `context.integration_mode` →
`policy.default_integration_mode` → `engine.yaml default_mode` (OBSERVE_ONLY).

## 6. Key data contracts

- **`PipelineContext`** — `pipeline_id`, `pipeline_run_id`, `dataset`, `stage`
  (BRONZE/SILVER/GOLD), `platform`, `runtime`, optional `ruleset_version`,
  `integration_mode`. Every run is traceable to a pipeline run.
- **`DQResult`** — `accepted_dataframe`, `quarantine_dataframe`,
  `validation_results`, `remediation_results`, `execution_metrics`,
  `quality_gate`, optional `final_validation_results`, `profile`, `to_dict()` /
  `write_report()` JSON audit output.
- **`RuleSet`** — immutable once published; `checksum()` over canonical content;
  `derive()` creates a *new* version; registry enforces "no content change under an
  existing version".
- **Quarantine row** — original record + `dq_*` columns: run/pipeline/dataset IDs,
  failed rule IDs, failure reasons, proposed correction, confidence, remediation
  status, timestamps.

## 7. Development status — where we are

### 7.1 Checkpoint tracker (all 12 complete)

| CP | Scope | Status |
|---|---|---|
| 1 | Workspace/env inspection, pinned deps, synthetic pipeline + data | ✅ Done |
| 2 | `dq-core` package skeleton + runtime modules | ✅ Done |
| 3 | Profiling (full/sample/approximate) | ✅ Done + tested |
| 4 | Candidate rule generation + RuleSet models + registry | ✅ Done + tested |
| 5 | Validation engine (native PySpark, multi-violation) | ✅ Done + tested |
| 6 | Deterministic remediation | ✅ Done + tested |
| 7 | MockAIProvider + confidence policy (90 %) | ✅ Done + tested |
| 8 | Quarantine + revalidation + all 3 modes | ✅ Done + tested |
| 9 | Medallion B/S/G + metrics + reports + runner | ✅ Done + tested |
| 10 | Databricks adapter + pipeline registry + `dqctl` + manifests | ✅ Done + CLI-verified |
| 11 | Portability contracts + candidate-rule tests | ✅ Done + contract-tested |
| 12 | Test suite + demo + 15 docs + testing report | ✅ Done (130 tests green) |

### 7.2 Current truth and remaining POC work

| Area | State | Boundary |
|---|---|---|
| Local Spark engine and tests | ✅ Verified | 139 tests (74 unit + 65 integration); Spark 3.5.9↔4.1.3 matrix identical |
| UI, CLI, onboarding, lifecycle | ✅ Verified | 10 pages + assistant; Auto E2E + manual wizard + manifest onboarding; factory reset (`/api/admin/reset`, typed `RESET`) |
| Auto E2E single-job flow | ✅ Verified | profile → candidates → auto-approve → publish → manifest → governed run; BRONZE-only = contract generation (no row validation) |
| NoSQL metadata store | ✅ Verified | tinydb embedded default; mongodb/dynamodb backends; atomic writes; doc-backed registries |
| Large-file sampling policy | ✅ Verified | profile tiers (§0.1c); `dq_run` samples >50 MB → ~100K rows; governed `pipeline_run` validates all rows |
| Real LLM remediation | ✅ Verified | Databricks Model Serving (`simulated=false` per decision); MockAI fallback |
| Medallion chaining + physical persistence | ✅ Verified (P0) | Bronze/Silver/Gold/Quarantine/Audit all written per run |
| Managed Bronze ingestion | ✅ Verified (P1) | `_ingest.json`: checksum, schema, row count per dataset |
| Terminal outcome audit | ✅ Verified (P2) | All 7 outcomes incl. `REJECTED_BY_HUMAN` (rejection memory) |
| Seven-pillar scoring | ✅ Verified (P3) | Pillars + domain recommendations in reports and UI |
| Cross-platform evidence | ◐ Partial (P4) | Platform labels + Spark-version matrix proven; **real cloud Spark run still open — not claimed** |
| Auth, scheduling, adapters, alert delivery | ⏸ Deferred P5 | Do not begin before P4 closes |

### 7.3 Component maturity map

```mermaid
flowchart LR
    subgraph DONE["VERIFIED — P0 to P4"]
        A1["Local Spark engine"]
        A2["UI and CLI onboarding"]
        A3["Rule versioning"]
        A4["Deterministic and real LLM remediation"]
        A5["Quarantine policy + rejection memory"]
        A6["Stage chaining, physical persistence, pillars"]
        A7["Spark 3.5/4.x portability matrix"]
        A8["Auto E2E + NoSQL metadata store"]
    end
    subgraph OPEN["REMAINING — P4 tail"]
        D1["Real cloud Spark cluster run"]
    end
    subgraph LATER["DEFERRED — P5"]
        E1["Auth and multi-tenancy"]
        E2["Scheduling and orchestration adapters"]
        E3["Alert delivery"]
        E4["Production registry and Kubernetes"]
    end
    DONE --> OPEN --> LATER
```

| Area | State | Evidence |
|---|---|---|
| Profiling (FULL/SAMPLE/APPROXIMATE) | ✅ Implemented + 10 integration tests | anomalies, PK candidates, mixed formats detected on real data |
| Candidate rule generation | ✅ Implemented + 19 unit + 2 integration tests | all rule types emit CANDIDATE rules w/ confidence+evidence |
| RuleSet models/versioning/registry | ✅ Immutable publish, checksum verify, diff, rollback | `ImmutableVersionError`, diff 1.0→1.1 verified via CLI |
| Validation engine | ✅ 12 rule types, row+dataset scope, multi-violation | failed-record identification tests pass |
| Deterministic remediation | ✅ 7 actions, audit log, policy-gated | trim/lookup/date/null/dedupe tests pass |
| Mock AI + confidence policy | ✅ 90 % default, boundary-tested 0.89/0.8999/0.90/0.95/1.00 | sensitive columns need approval even @1.00 |
| Quarantine + revalidation | ✅ status taxonomy, `dq_*` columns, post-check | invalid `'IND'` correction caught |
| 3 integration modes | ✅ all tested | OBSERVE 38/38, ENFORCE 16/22, REMEDIATE 24+8/13 |
| Medallion B/S/G | ✅ Bronze preserved, Silver DQ, Gold ref-integrity+biz rules | transactions Gold: 26 acc/4 quar/18 summaries/2,519,147.89 |
| Metrics + reports | ✅ `DQResult.to_dict/write_report`, phase timings | JSON reports verified |
| Pipeline lifecycle | ✅ 7-state machine, offboard keeps data+history | CLI + integration tests + UI actions |
| `dqctl` CLI | ✅ pipeline/ruleset/run/profile commands | all exercised with real output |
| Local Spark harness | ✅ Spark 4.1.3 + Java 21 + Windows Parquet shim | `check_runtime.py` + test suite |
| Physical medallion persistence (P0) | ✅ Bronze/Silver/Gold/Quarantine/Audit written per run | `written_paths` in API + `data/` tree |
| Managed Bronze ingestion (P1) | ✅ `_ingest.json` checksum/schema/rows per dataset | file inspection per dataset |
| Outcome audit (P2) | ✅ 7 terminal outcomes incl. REJECTED_BY_HUMAN | `outcome_audit` in reports; goat full spectrum w/ real LLM |
| Quality pillars (P3) | ✅ 7 pillars + domain recommendation | report JSON + UI pillar cards |
| Approval + rejection memory | ✅ Approvals → HUMAN_APPROVED_AI; rejections → REJECTED_BY_HUMAN permanently | `config/approvals/*.yaml` + `RejectionStore` + UI decision history |
| Portability (P4) | 🔶 Strong local evidence | platform labels + Spark 3.5.9↔4.1.3 matrix, ANSI on/off — `reports/p4_*_evidence.json` |
| Databricks execution | ⚠️ Pattern + LLM verified — **no workspace Spark run** | Model Serving calls real (`simulated=false`); cluster run open |
| Non-Spark targets (SQL/Flink/etc.) | 📐 Extension contracts only | `docs/concepts/pipeline_adapters.md` |

### 7.3 Verified evidence snapshot

- **Tests:** `139 passed` — 74 unit (metadata, policies, schema, lifecycle,
  candidate-generation, remediation outcomes) + 65 integration (Spark session,
  profiling, validation, remediation, modes, medallion, pipeline lifecycle,
  portability).
- **P4 Spark matrix:** byte-identical metrics on the same wheel across
  Spark 3.5.9 ↔ 4.1.3, ANSI enabled/disabled, shuffle partitions 8 ↔ 32 —
  fresh venv containing only the wheel + pyspark
  (`reports/p4_spark_matrix_evidence.json`).
- **AI spectrum (goat, real LLM):** `AI_APPLIED` @0.95, `HUMAN_APPROVED_AI`
  @0.80, `BLOCKED_SENSITIVE_DATA`, `DETERMINISTIC_FIX`, `NO_SAFE_PROPOSAL` —
  every decision `simulated=false`.
- **Expected-results conformance:** every per-record outcome in
  `sample_data/expected_results.json` reproduced (customers 38 rows,
  transactions 40 rows, Gold aggregates exact).
- **CLI runs:** OBSERVE_ONLY report, profile JSON, ruleset publish/diff/
  activate/rollback/verify, pipeline onboard→offboard — all real output.
- **UI audit:** all 10 pages endpoint-verified; mutating actions persist +
  refresh (Approvals rejection memory fixed and engine-wired).
- **Defects found & fixed during verification:** logged in
  `docs/reference/testing_report.md` §4 plus approval-inbox no-op reject,
  undeclared `click` dependency, CSV `inferSchema` mismatch.

### 7.4 What remains (honest list)

- Real managed-Spark cluster run (Databricks/Fabric/EMR) — engine contract is
  proven portable; execution needs platform credentials (deployment, not engine).
- SQL/streaming adapters, governance DB — later phases by design.
- **PostgreSQL backend** for the registry — the active backend is the NoSQL
  document store (`registry.backend: nosql`, tinydb embedded default;
  mongodb/dynamodb supported). The `MetadataStore` seam means PostgreSQL
  drops in as another backend — no caller changes.
- **Job queue for scale-out** — `ui/backend/jobs.py` uses threads + a SQLite job
  store (correct for the in-process SparkSession). Production scale-out = one
  job-runner per cluster behind Celery/RQ/K8s; SSE progress already wired
  (`GET /api/jobs/{id}/stream`).
- **DQX parity gaps** — verified against `databrickslabs/dqx` source (clone in
  `.tmp/dqx-ref`); register G1–G11 in `ROADMAP.md`: stratified sampling,
  check-type breadth, scheduled runs, incremental validation, streaming,
  DLT expectations, PII detection, ML anomaly scoring, MCP surface, live
  cluster proof, comparison doc. Items we already exceed DQX on (rule
  versioning, lifecycle, medallion orchestration, AI remediation, approvals)
  are intentionally not rebuilt.

**New (verified) since §7.3:**
- `validation/engine.py` — REFERENTIAL_INTEGRITY honours `config.join_strategy`
  (`auto` default / `broadcast` / `merge` hint) for large dimension tables.
- `dq_core/registries/` — `factory.py` (backend switch) + `sql_store.py`
  (`SqlRuleSetRegistry`, `SqlPipelineRegistry` overriding only storage seams —
  all immutability/checksum/lifecycle guarantees reused unchanged). Verified:
  publish/verify/diff/immutability + full pipeline lifecycle on SQLite.
- `ui/` — React 18 + FastAPI console: Dashboard, Pipelines (onboard + lifecycle),
  Run DQ (async jobs, SSE real-time progress), Rule Sets, Reports. Jobs persist
  to `data/jobs.db` across restarts.

## 8. Failure & safety model

- Rules are declarative metadata; the validation engine builds **native PySpark
  Column expressions** — no `eval()`, no generated Python/SQL execution.
- Each rule is evaluated independently; a record may accumulate **multiple**
  violations (stored as a list, never collapsed).
- Dataset-level rules (ROW_COUNT, SCHEMA_VALIDATION, REFERENTIAL_INTEGRITY,
  DUPLICATE) are distinguished from row-level rules in results.
- Any correction is revalidated; a correction that still fails → quarantine with
  status `CORRECTION_FAILED_REVALIDATION`.
- Quality gate (`max_quarantine_rate`, `fail_on_dataset_level_errors`,
  `fail_on_not_evaluated_rules`, `min_accepted_records`) decides pass/fail and is
  recorded in metrics + report.

## 9. What is deliberately NOT in this POC

Authentication, multi-tenancy, production Airflow/ADF/dbt/Flink/Snowflake
connectors, Kubernetes deployment, alerting integrations, and real managed-cloud
Spark execution (contract-proven only). The React UI and FastAPI console ARE in
scope as the local POC surface. These are extension points documented
in `docs/concepts/pipeline_adapters.md`.
