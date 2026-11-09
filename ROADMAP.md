# DQ Accelerator — Roadmap & Progress

**Project**: Databricks-first, portable Apache Spark Data Quality Accelerator  
**Repo**: https://github.com/KGANESHKUMARK/dq-accelerator  
**Stack**: Python 3.12 · PySpark 4.1.3 · FastAPI · React 18 · Vite · SQLite · YAML governance

---

## What is this?

A reusable, platform-independent Data Quality Framework that drops into any existing
PySpark pipeline with minimal changes. The same engine runs on Databricks, AWS EMR,
Microsoft Fabric, Google Dataproc, or a local Spark session. Data quality rules,
policies, and pipeline lifecycle are all governed as versioned metadata (YAML + registry),
not hardcoded logic.

Core user journey:
```
Connect source → Profile data → Auto-generate rules → Steward review →
Publish ruleset → Onboard pipeline → Run DQ (OBSERVE → ENFORCE → REMEDIATE) →
Review anomalies → Fix / quarantine → Audit trail
```

---

## ✅ DONE — What is fully implemented and verified

### CP1 · Project foundation
- [x] Python 3.12 + PySpark 4.1.3 + JDK 21 environment pinned to D: drive
- [x] `pyproject.toml` + `requirements.txt` dependency management
- [x] Windows local Spark harness (`runtime/local_spark.py`) — custom FS shim, no winutils
- [x] All scratch / temp / JVM crash dumps / heap dumps stay under `.tmp` and `logs/` on D:
- [x] `start.bat` interactive menu launcher + `start_ui.bat` for backend + frontend
- [x] `scripts/env.ps1` — exports `DQ_LOG_DIR`, `TEMP`, `TMP`, `PIP_CACHE_DIR` all on D:
- [x] Structured rotating JSON logs: `logs/engine/`, `logs/jvm/`, `logs/backend/`, `logs/frontend/`, `logs/diagnostics/`

### CP2 · Synthetic pipeline assets
- [x] Banking: `customers.csv` (1M rows), `transactions.csv` — clearly labeled SYNTHETIC
- [x] Healthcare: `patients.csv` (44 rows, 12 cols, full PHI defect spectrum)
- [x] Retail/Fabric: `orders.csv` (30 rows, 12 cols, mixed formats + currency defects)
- [x] Finance/EMR: `loans.csv` (31 rows, 12 cols, credit score + email defects)
- [x] Employees: `employees.csv` (5 rows — wizard test dataset)
- [x] **Telecom CDR**: `cdr_records.csv` — **100 MB, 630,797 rows, 20 cols**, ~2.5% seeded defects
  - Generator: `scripts/generate_telecom_data.py` (CSV gitignored — above GitHub 100 MB limit)
- [x] `sample_data/expected_results.json` for reproducible demo assertions

### CP3 · Core engine — `packages/dq-core` (62 Python modules)

#### Profiling (`profiling/`)
- [x] FULL / SAMPLE / APPROXIMATE modes
- [x] Column stats: null%, distinct/unique counts, mean/stddev, min/max, percentiles, outliers
- [x] Anomaly detection: MIXED_DATE_FORMATS, TYPE_INCONSISTENCY, INCONSISTENT_FORMATTING,
  RARE_CATEGORY_VALUES, LEADING_TRAILING_WHITESPACE, TYPED_DATA_STORED_AS_STRING
- [x] Schema anomaly detection, PK / near-key candidates
- [x] Pattern detection (structural regex inference)
- [x] Auto-SAMPLE for files >50 MB (backend protection against OOM)

#### Rule generation (`generation/`)
- [x] `CandidateRuleGenerator` — 9 rule types with confidence + evidence metadata:
  NOT_NULL, UNIQUE, REGEX, DATE_FORMAT, DATA_TYPE, RANGE, ALLOWED_VALUES,
  STRING_LENGTH, DUPLICATE, SCHEMA_VALIDATION, REFERENTIAL_INTEGRITY
- [x] `approve_candidates` — steward review gate; unapproved rules never execute
- [x] `dqctl bootstrap` — source file → profile → ruleset YAML + manifest in one command

#### Metadata / Rulesets (`metadata/`, `rules/`)
- [x] Versioned immutable rulesets: SHA-256 checksum, publish / activate / rollback / diff
- [x] `RuleSet`, `RuleDefinition`, `RemediationStep` dataclasses
- [x] File registry + SQLite registry (factory pattern — swap without engine change)
- [x] Tamper detection enforced on ENFORCE/REMEDIATE modes

#### Validation (`validation/`)
- [x] Pure PySpark DataFrame expressions — zero row-by-row Python in hot path
- [x] Single aggregation pass for all row-level rules
- [x] Rule types: NOT_NULL, UNIQUE, REGEX, RANGE, DATE_FORMAT, ALLOWED_VALUES,
  STRING_LENGTH, DATA_TYPE, DUPLICATE, SCHEMA_VALIDATION, ROW_COUNT,
  REFERENTIAL_INTEGRITY (auto/broadcast/merge join strategy), RECONCILIATION
- [x] Per-rule: total / passed / failed / pass_pct / failure_reasons / examples

#### Remediation (`remediation/`)
- [x] Deterministic: TRIM, UPPERCASE, LOWERCASE, LOOKUP_MAPPING, DATE_STANDARDIZATION,
  NULL_REPLACEMENT, DEDUPLICATE — all composed in a single DataFrame projection
- [x] Mock AI provider (`ai/mock.py`): config-driven YAML lookup + similarity fallback
  - Correctly labeled as mock/simulated — NOT a real LLM
  - Similarity fallback capped at 0.85 so it never auto-applies (requires human review)
- [x] Confidence threshold engine (default 90%):
  - AUTO_APPLY: ≥ threshold, non-sensitive column
  - BELOW_CONFIDENCE_THRESHOLD: < threshold
  - PENDING_HUMAN_APPROVAL: sensitive column regardless of confidence

#### Quarantine + Revalidation (`quarantine/`, `revalidation/`)
- [x] OBSERVE_ONLY: all rows accepted, violations flagged (never modifies pipeline output)
- [x] ENFORCE: failed rows → quarantine with ENFORCE_NO_REMEDIATION_ATTEMPTED
- [x] REMEDIATE: deterministic → intermediate validate → AI → confidence gate → revalidate
- [x] Quarantine statuses: UNRESOLVED_NO_SAFE_REMEDIATION, PENDING_HUMAN_APPROVAL,
  BELOW_CONFIDENCE_THRESHOLD, ENFORCE_NO_REMEDIATION_ATTEMPTED
- [x] Quality gate: quarantine rate threshold, gate_passed flag on every run

#### Orchestration (`orchestration/`)
- [x] `DQRunner` — mode-aware execution with per-phase timing
- [x] `MedallionRunner` — Bronze → Silver → Gold multi-stage pipeline
- [x] `DQResult` — accepted_dataframe, quarantine_dataframe, validation_results,
  remediation_results, execution_metrics, quality_gate, profile_report
- [x] Pass-through for PAUSED / OFFBOARDED pipelines (data returned untouched)

#### Pipeline lifecycle (`pipelines/`)
- [x] State machine: DRAFT → REGISTERED → TESTED → ACTIVE ⇄ PAUSED → DEACTIVATING → OFFBOARDED → DRAFT
- [x] Append-only audit history with actor + reason on every transition
- [x] `record_execution` — per-run summary persisted to registry (last 50 kept)
- [x] PipelineManifest: `format: TABLE/DELTA/PARQUET/CSV`, multi-dataset, multi-stage

#### Policies (`policies/`)
- [x] `RemediationPolicyConfig` — confidence threshold, sensitive column categories
- [x] `ConfidencePolicyEngine` — AUTO_APPLY / BELOW_THRESHOLD / PENDING_HUMAN_APPROVAL
- [x] `ApprovalStore` — `config/approvals/approved_corrections.yaml`
- [x] Banking default policy + Healthcare PHI policy (patient_identity, clinical_identifiers)

#### Reporting + Audit (`reporting/`)
- [x] Per-run JSON report: `reports/<dataset>_<run_id>.json`
  - All phase durations, per-rule results, quarantine breakdown, gate decision
- [x] `ExecutionMetrics` — 30+ fields including remediation stats, AI decisions breakdown
- [x] Structured engine events in `logs/engine/dq_core.log` (rotating, JSON format)

#### Runtime (`runtime/`)
- [x] `local_spark.py` — Windows harness, JVM `-XX:ErrorFile` + heap dump to `logs/jvm/`
- [x] `logging.py` — `configure_logging`, `DQ_LOG_DIR` env, rotating file handler
- [x] `compat.py` — Spark version abstraction (3.x / 4.x safe expressions)
- [x] `config.py` — `EngineConfig` from YAML
- [x] `inspection.py` — runtime detection (Databricks / EMR / local)

#### CLI (`cli/`)
- [x] `dqctl pipeline onboard|list|test|activate|pause|offboard`
- [x] `dqctl ruleset publish|list|activate|rollback|diff|verify`
- [x] `dqctl bootstrap <source> [--profile-mode SAMPLE] [--driver-memory 2g] [--publish] [--onboard]`
- [x] `dqctl run <source> --ruleset --policy --mode`
- [x] `dqctl profile <source> --mode`

### CP4 · Tests — 130 passing
- [x] 65 unit tests (no Spark): metadata, pipeline lifecycle, policies, candidate generation, schema utils
- [x] 65 integration tests (Spark local[2]): engine modes, medallion, profiling, remediation,
  validation, portability contracts, pipeline lifecycle integration, Spark session
- [x] Last verified clean run: `130 passed in 537.19s`

### CP5 · Configuration assets (`config/`)
- [x] Rulesets: `customers_v1`, `customers_v1_1`, `customer_summary_gold_v1`,
  `transactions_v1`, `transactions_gold_v1`, `patients_v1`,
  `orders_v1`, `loans_v1`, `employees_v1`, `cdr_records_v1`
- [x] Policies: `default.yaml` (banking), `healthcare.yaml` (PHI)
- [x] Pipelines: 7 manifests (customers, transactions, healthcare, Fabric orders, EMR loans,
  employees UI demo, telecom CDR)
- [x] Mock AI responses: `config/ai/mock_ai_responses.yaml`
- [x] Approved corrections: `config/approvals/approved_corrections.yaml`
- [x] Engine settings: `engine.yaml` (local), `engine.databricks.yaml`

### CP6 · Integration samples (`integrations/`)
- [x] Databricks: `customer_pipeline_original.py` + `customer_pipeline_dq_integrated.py`
- [x] Microsoft Fabric: `orders_pipeline_original.py` + `orders_pipeline_dq_integrated.py`
- [x] AWS EMR: `loans_pipeline_original.py` + `loans_pipeline_dq_integrated.py`
- [x] Contracts: `adapter_contract.py` — interface all adapters must implement

### CP7 · FastAPI backend (`ui/backend/`)
- [x] 23 REST endpoints: health, pipelines CRUD + lifecycle, rulesets publish/activate/rollback/diff,
  datasets, DQ run, profile, jobs (SQLite-persisted), SSE stream, reports, onboard wizard
- [x] `jobs.py`: SQLite job store (`data/jobs.db`), SSE event queue, `progress_cb`
- [x] `GET /api/jobs/{id}/stream` — SSE: QUEUED → RUNNING → progress events → COMPLETED/FAILED
- [x] `POST /api/onboard/upload` — multipart file upload → `data/uploads/`
- [x] `POST /api/onboard/preview` — profile + candidate generation as a job (SSE)
- [x] `POST /api/onboard/finalize` — approve candidates → save ruleset + manifest → optional publish/onboard
- [x] Spark singleton (`local[8]`/2g) shared across all jobs

### CP8 · React 18 frontend (`ui/frontend/`)
- [x] 6-tab navigation: Dashboard, Pipelines, Onboard, Run DQ, Rule Sets, Reports
- [x] **Pipelines**: manifest onboard, lifecycle stepper (real state machine), FlowGuide
- [x] **Onboard wizard**: 5 steps — Source → Options → Profile → Review rules → Finalize
  - File upload or sample_data selection
  - Live SSE progress with spinner
  - Rule review checklist with colour-coded confidence
- [x] **Run DQ**: governed mode (pipeline dropdown → manifest-bound dataset/ruleset/policy/source)
  + ad-hoc mode; SSE progress; past result re-view from history dropdown
- [x] **Rule Sets**: publish, activate, rollback, diff viewer
- [x] **Reports**: list + JSON viewer
- [x] **Charts**: anomaly cards, schema anomaly cards, PK candidates, per-column stat cards
  with null-rate bars, rule pass-rate bars, quarantine breakdown, quality gate badge
- [x] Past job re-view: Dashboard "view" button + Run DQ history dropdown — SQLite-backed
- [x] Frontend build clean: `172 KB` JS bundle

### CP9 · Verified end-to-end runs
- [x] Healthcare REMEDIATE: 44 in → 30 accepted → 13 quarantined (PHI gate proven)
- [x] Customers REMEDIATE: 1.3M rows / 98.5 MB in 56.5s (`local[8]`/2g)
- [x] Wizard: employees.csv upload → 15 candidates → 10 approved → published → REGISTERED → ENFORCE run
- [x] Governed Run DQ: manifest binding verified (source/ruleset/policy locked to manifest)

### CP10 · Documentation
- [x] `README.md` — setup, CLI reference, quick start
- [x] `docs/architecture.md` — full system architecture + development status
- [x] `docs/PROJECT_GUIDE.md` — setup, structure, CLI §6.3 (bootstrap §6.3.1, wizard §6.3.2)
- [x] `docs/spark_core_engine.md`, `remediation.md`, `quarantine.md`, `profiling.md`,
  `rule_management.md`, `pipeline_adapters.md`, `databricks_onboarding.md`,
  `testing_report.md`, `poc_demo_script.md`, `confidence_policy.md`, `medallion_data_flow.md`

---

## ⚠️ KNOWN GAPS — Honest limitations

| Gap | Detail |
|---|---|
| **DQX uses LiteLLM, not DQX rule detection** | We use LiteLLM directly for value correction (more appropriate than DQX's `detect_business_rules_with_llm` which generates rules, not corrections). DQX package is installed but we bypass its rule-gen API. |
| **No real Databricks run** | Engine + adapter are contract-ready; needs workspace URL + token |
| **No anomaly lifecycle** | Anomalies are fire-and-forget — no open/acknowledged/resolved workflow |
| **No cross-run trends** | Each run is isolated; no sparkline history across runs |
| **No human approval UI** | `PENDING_HUMAN_APPROVAL` rows have no browser-based resolution path |
| **No rule editor in UI** | Rules are YAML only — wizard generates them but no inline editing |
| **No connection model** | Wizard works on local files only; Databricks tables need a live session + credentials |
| **UI not client-attractive** | Flat tabs, no health score, no anomaly inbox, raw JSON in Reports |
| **E2E tests empty** | `tests/e2e/` exists but has zero tests |
| **No multi-tenancy / auth** | Single-process POC; no login, no RBAC |
| **Healthcare Gold ruleset** | Bronze→Silver done; Gold never added (customers has Gold) |

---

## ✅ DONE — Phase A: UI redesign (client-attractive)

### A1 · Layout — left sidebar + header breadcrumb ✅
- [x] Persistent left sidebar: OVERVIEW / GOVERN / OBSERVE / CONFIGURE / SYSTEM sections
- [x] Health dot + API status in sidebar footer
- [x] Approval pending count badge on Anomalies nav item
- [x] Top-bar breadcrumb + runtime info chip

### A2 · Dashboard — data health command centre ✅
- [x] Overall health score (accepted ÷ total across last N runs) — live from job history
- [x] Pipeline status grid with per-pipeline pass-rate sparklines
- [x] KPI tiles: active pipelines, open anomalies, completed jobs
- [x] Recent jobs table with inline view button (re-renders charts from SQLite)

### A3 · Reports — summary cards ✅
- [x] Per-report summary: health %, input/accepted/quarantined stat tiles, quality gate badge
- [x] Top failing rules with pass-rate bars
- [x] Quarantine breakdown by status
- [x] Raw JSON collapsed to `<details>` (accessible but not the default view)

### A4 · Pages rebuilt with new design system ✅
- [x] Pipelines: status grid, lifecycle stepper, history table, executions table, trend sparkline
- [x] Onboard wizard: 5-step with new design language, colour-coded confidence checklist
- [x] Run DQ: governed/ad-hoc mode with clearer layout, all 9 rulesets in ad-hoc list
- [x] Rule Sets: grouped by ID, diff summary cards with added/removed/modified counts
- [x] Settings: AI provider live status + DQX setup guide + confidence policy reference

## ✅ DONE — Phase B: Missing workflows

### B1 · Anomaly lifecycle (SQLite) ✅
- [x] `data/anomalies.db` — anomalies table with status, owner, note, timestamps
- [x] `GET /api/anomalies` + `PATCH /api/anomalies/{id}` — status/owner/note update
- [x] `GET /api/anomalies/stats` — open/acknowledged/resolved counts for sidebar badge
- [x] Auto-ingestion from every completed dq_run (failed rules → anomalies) + profile jobs
- [x] Anomalies page: open/acknowledged/resolved tabs, acknowledge form with owner+note
- [x] **E2E verified**: 27 anomalies from orders ENFORCE + healthcare REMEDIATE runs
  - `NOT_NULL/customer_id`, `RANGE/unit_price`, `ALLOWED_VALUES/currency`, etc.

### B2 · Cross-run trend history ✅
- [x] `GET /api/pipelines/{id}/trend` — last 30 job summaries with pass_rate, accepted, gate
- [x] Sparkline component on Pipeline detail card + Dashboard pipeline grid
- [x] `GET /api/health/score` — overall health % from all recent runs
- [x] **E2E verified**: HEALTHCARE_PATIENT_PIPELINE_V2 → 15 trend points, health=74.7%

### B3 · Human approval inbox ✅
- [x] `GET /api/quarantine/pending` — reads ai_decisions from report JSON,
  filters `REQUIRES_APPROVAL` (= PENDING_HUMAN_APPROVAL), returns per-row detail
- [x] `POST /api/quarantine/approve` — writes to `config/approvals/approved_corrections.yaml`
- [x] `POST /api/quarantine/reject`
- [x] Approvals page: table with original/proposed values, confidence, approve/reject buttons
- [x] Sidebar nav badge shows pending count (polls every 15s)
- [x] **E2E verified**: 4 pending rows from healthcare runs (J0hn Smith/patient_name 0.98,
  r.khan@hospital-mail/contact_email 0.93); approve writes YAML correctly

## ✅ DONE — Phase C: DQX provider integration

### C1 · DQXProvider — real LLM working ✅
- [x] `packages/dq-core/src/dq_core/ai/dqx_provider.py`
  - Implements `AIRemediationProvider` interface — engine uses it identically to MockAI
  - Uses **LiteLLM directly** for value correction (not DQX's `detect_business_rules_with_llm`
    which generates rules, not corrections)
  - Calls `databricks/{endpoint}` on Databricks Model Serving via LiteLLM
  - Batches requests by (column, rule_type) — not one LLM call per row
  - Auto-fallback to MockAI when: credentials absent, endpoint unreachable, call fails
  - `describe()` reports: provider, model_version, dqx_available, credentials_set, simulated
- [x] `create_provider('dqx', ...)` in `ai/__init__.py`
- [x] `AISettings.dqx_model_endpoint` + `dqx_fallback_on_error` in `runtime/config.py`
- [x] `config/engine.yaml` — `provider: dqx` with `dqx_model_endpoint` set
- [x] `GET /api/ai/status` backend endpoint + Settings page live provider status
- [x] **Real LLM verified**: `actve` → `active` (conf=0.95), `  John  ` → `John` (conf=0.95),
  `hihg` → `high`, `medim` → `medium` — all real Databricks Model Serving responses
- [x] **E2E verified**: Settings page shows `provider: DQXProvider, simulated: false`

### C1 · DQX activation status — WORKING ✅
- [x] `pip install 'databricks-labs-dqx[llm]'` — installed (v0.17.2)
- [x] `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DQX_MODEL_ENDPOINT` — env vars configured
- [x] `config/engine.yaml` → `ai.provider: dqx` + `dqx_model_endpoint` set
- [x] Real LLM verified — `databricks-meta-llama-3-3-70b-instruct` returning corrections
- [x] Backend restart — Settings page shows green with `DQXProvider, simulated: false`
- [x] **Note**: `databricks-connect` must NOT be installed (breaks local PySpark 4.x)

## 🔨 DOING NOW — Phase A: UI redesign (client-attractive)

### A1 · Layout redesign — left sidebar + header breadcrumb
- [ ] Replace flat 6-tab top nav with persistent left sidebar
- [ ] Sections: Connections, Pipelines, Onboard, Run DQ, Anomalies, Rule Sets, Reports, Settings
- [ ] Header: breadcrumb path + pipeline context chip + API health dot
- [ ] Responsive collapse to icon-only sidebar on small screens
- [ ] Consistent card/panel design system (shadows, spacing, typography)

### A2 · Dashboard — data health command centre
- [ ] Overall health score 0–100 (passed records ÷ total across last 7 days)
- [ ] Pipeline status grid: each pipeline as a card with state badge + last run result
- [ ] Recent anomalies feed: last 10 anomalies with type, column, dataset, severity, timestamp
- [ ] Trend sparklines: null rate + pass rate across last N runs per pipeline
- [ ] Quick-action buttons per pipeline: Run DQ, Profile, View Anomalies

### A3 · Reports — summary cards replacing raw JSON
- [ ] Per-report summary card: health score badge, input/accepted/quarantined counts, gate result
- [ ] Top 5 failing rules with pass-rate bars
- [ ] Anomaly count with breakdown by type
- [ ] Raw JSON expandable section (keep for engineers, collapse by default)
- [ ] Download report as JSON / CSV button

### A4 · Rules page — inline editing
- [ ] Rule type-specific form fields (severity dropdown, description text, enable/disable toggle)
- [ ] Edit and re-publish as a new version (diff shown before confirm)
- [ ] Add a new rule to an existing ruleset from the UI

---

## 🔨 TODO NEXT — Phase B: Missing workflows

### B1 · Anomaly lifecycle management
- [ ] SQLite table: `anomalies` (id, run_id, pipeline_id, dataset, column, type, detail,
  severity, status, owner, resolution_note, created_at, updated_at)
- [ ] Backend: `GET /api/anomalies`, `PATCH /api/anomalies/{id}` (status/owner/note)
- [ ] UI: Anomalies page — inbox view (open/acknowledged/resolved tabs)
- [ ] Anomaly detail: affected records count, rule context, resolution form
- [ ] Status transitions: OPEN → ACKNOWLEDGED → RESOLVED (with undo)

### B2 · Cross-run trend history
- [ ] SQLite table: `run_history` (pipeline_id, run_id, dataset, ran_at, pass_rate,
  null_rate_avg, accepted, quarantined, gate_passed)
- [ ] Backend: `GET /api/pipelines/{id}/history/trend`
- [ ] UI: Pipeline detail → Trend tab with sparkline / line chart across last 30 runs
- [ ] Dashboard health score computed from run_history
- [ ] Anomaly count trend over time

### B3 · Human approval inbox
- [ ] Backend: `GET /api/quarantine?pipeline_id=&status=PENDING_HUMAN_APPROVAL`
  reads quarantine output from last REMEDIATE run
- [ ] `POST /api/quarantine/{record_id}/approve` → writes to `approved_corrections.yaml`
- [ ] `POST /api/quarantine/{record_id}/reject`  → marks permanently quarantined
- [ ] UI: Approvals page — table of PENDING rows with original value, proposed value,
  confidence, column, rule — Approve / Reject buttons
- [ ] Re-run trigger after bulk approval

---

## 🔨 TODO LATER — Phase C: DQX integration

### C1 · DQX provider swap
- [ ] `pip install 'databricks-labs-dqx[llm]'` added to requirements
- [ ] `ai/dqx_provider.py` implementing `AIRemediationProvider` interface:
  - `DQLLMEngine.detect_business_rules_with_llm()` for rule generation
  - Map DQX check results back to our `RemediationDecision` model
- [ ] `DQXAnomalyDetector` wrapping DQX Isolation Forest + SHAP
- [ ] `engine.yaml` switch: `ai.provider: dqx` vs `ai.provider: mock`
- [ ] Requires: Databricks workspace, Model Serving endpoint, `DATABRICKS_HOST` + `DATABRICKS_TOKEN`

### C2 · DQX 80+ check types bridge
- [ ] Map our 10 rule types to DQX equivalents where DQX is richer
  (PII detection, geo validation, aggregate checks, data contracts)
- [ ] Keep our portable Spark core for non-Databricks runtimes (EMR, Fabric)

### C3 · Connection model
- [ ] `connections/` module: Databricks Unity Catalog, S3/Glue, ADLS/Fabric, JDBC
- [ ] `POST /api/connections/test` — validate credentials without storing raw secrets
- [ ] Onboard wizard Step 1 becomes: pick connection type → browse tables/paths
- [ ] Requires: secrets management (Databricks Secrets, AWS Secrets Manager, or Azure Key Vault)

---

## 🔨 TODO FUTURE — Phase D and beyond

| Item | Notes |
|---|---|
| Healthcare Gold ruleset | Complete Bronze→Silver→Gold for patients pipeline |
| E2E test suite | Real job submission → result assertion in `tests/e2e/` |
| Historical trend charts | Requires Phase B2 run_history table |
| Auth / multi-tenancy | JWT auth, per-user pipeline visibility, RBAC |
| Airflow operator | `DQAcceleratorOperator` calling `dqctl run` as a task |
| dbt integration | dbt test macro wrapping our engine |
| Data contracts (ODCS) | Schema + quality rules from ODCS contract YAML |
| Kubernetes deployment | Helm chart for the backend + Vite static serve |
| Performance: 1 GB+ | Requires a real cluster; local-mode ceiling is ~100 MB |

---

## Performance benchmarks (local machine — Windows 11, 20 cores, ~16 GB RAM)

| Dataset | Rows | Size | Config | Total time |
|---|---|---|---|---|
| Customers | 1.3M | 98.5 MB | `local[8]`/2g | 56.5s |
| Patients REMEDIATE | 44 | <1 MB | `local[8]`/2g | 34s |
| CDR profile (SAMPLE) | 62,903 | 100 MB | `local[4]`/2g | ~60s |

> Local mode: ~1.7 MB/s effective throughput. Same code on a 10-node Databricks cluster
> scales linearly. 100 MB local ≈ 1 TB on a real cluster.

---

## Git history summary

| Commit | What |
|---|---|
| `75ba682` | Project scaffolding, packaging, .gitignore |
| `b9c5bd2` | dq-core engine: 59 modules, 130 tests |
| `9e0b67a` | YAML config: rulesets, policies, manifests, mock AI |
| `561bf75` | Synthetic datasets, examples, Databricks integration |
| `ad75a1e` | Windows launchers, env bootstrap |
| `bf666d6` | React + FastAPI UI: SSE, charts, lifecycle stepper |
| `93372de` | Docs: architecture, runbooks, testing report |
| `8c922e1` | dqctl bootstrap command |
| `8fe8c81` | docs: bootstrap documentation |
| `0c33ce9` | UI Onboard wizard: upload, preview, rule review, finalize |
| `9b4b64b` | Fabric + EMR sample pipelines, orders/loans datasets |
| `73880cf` | Governed Run DQ: manifest-bound selection |
| `5775cea` | fix: OOM on large-file profiling — tunable bootstrap + auto-SAMPLE |
| `227ef82` | Telecom CDR 100MB dataset + pipeline |
| current | ROADMAP.md + Phase A UI redesign |
