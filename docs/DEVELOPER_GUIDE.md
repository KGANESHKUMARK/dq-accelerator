# DQ Accelerator — Developer Guide

> For engineers joining or maintaining the project: **how it was built phase by
> phase**, **what each phase delivered and how it was verified**, and **how to
> build & release**. For setup/run instructions see `SETUP.md` and
> `PROJECT_GUIDE.md`; for POC scope see `POC.md`.

---

## 1. Development phases — the build sequence

The project grew in four waves. Each wave has a theme, deliverables, and
verification evidence.

### Wave 1 — Core engine (`dq-core` package)

| Sub-phase | Delivered | Key files |
|---|---|---|
| Spark adapter | DataFrame-only Spark layer — no RDD/SparkContext/JVM/UDF; ANSI-safe casts; Spark Connect compatible | `runtime/compat.py`, `runtime/schema.py` |
| Profiling | `DataProfiler` — 5-job wide aggregation: nulls, distinct, percentiles, stddev outliers, top values, structural patterns, date formats, PK/near-key candidates, anomalies; FULL/SAMPLE/APPROXIMATE modes | `profiling/profiler.py`, `profiling/models.py` |
| Rule engine | ~15 rule types (NOT_NULL, UNIQUE, RANGE, REGEX, ALLOWED_VALUES, SCHEMA_VALIDATION, REFERENTIAL_INTEGRITY, DUPLICATE, DATE_FORMAT, STRING_LENGTH, DATA_TYPE…) + `CandidateRuleGenerator` (CANDIDATE status, confidence + evidence per rule) | `validation/`, `generation/generator.py`, `metadata/` |
| Modes | `OBSERVE_ONLY` / `ENFORCE` / `REMEDIATE` in `DQRunner` | `orchestration/runner.py` |
| Remediation | Deterministic fixes + dedupe → intermediate re-validation → AI proposals → `ConfidencePolicyEngine` (90% threshold, approvals, sensitive-data block) → final revalidation → accepted/quarantine | `remediation/`, `policies/`, `revalidation/` |
| AI providers | `AIRemediationProvider` interface; `MockAIProvider` (default); `DQXProvider` (real Databricks Model Serving via LiteLLM, lazy import + auto-fallback) | `ai/` |
| Governance | Immutable versioned rulesets (SHA-256 checksums), pipeline lifecycle state machine, registries (file/SQLite backends) | `rules/registry.py`, `pipelines/registry.py`, `registries/` |
| Medallion | `MedallionRunner` — BRONZE persist + ingest manifest → SILVER remediate → GOLD enforce | `orchestration/medallion.py`, `io/` |

### Wave 2 — Web UI + backend (FastAPI + React)

| Sub-phase | Delivered | Key files |
|---|---|---|
| Backend | FastAPI app: pipelines CRUD/lifecycle/run, rulesets publish/activate/rollback/diff, jobs with live progress streaming, anomalies (SQLite), approvals/quarantine, reports, settings, assistant | `ui/backend/main.py` |
| Frontend | React+Vite SPA: Dashboard, Pipelines, Onboard wizard, Run DQ, Anomalies, Approvals, Rule Sets, Reports, Settings, Assistant chat | `ui/frontend/src/pages.tsx`, `api.ts` |
| Jobs | Async job runner with phase progress (`spark → read → validate → remediation → revalidation → quarantine → report`) | `ui/backend/` job manager |

### Wave 3 — Real LLM + demo assets

| Sub-phase | Delivered |
|---|---|
| DQX provider | Real Databricks Model Serving calls (`databricks-labs-dqx[llm]` + LiteLLM); `simulated=false` flag per decision; `requirements-llm.txt` optional deps |
| Sample datasets | patients, goat herd, hotel reservations (1M rows), telecom CDR (630K), orders, bank transactions (50K), healthcare minimal, SW employees |
| Demo pipelines | Healthcare, goat (agriculture), orders (Fabric), hotel, telecom, EMR loans, SW employees |

### Wave 4 — POC hardening (P0–P4 + post-audit fixes)

See §2 for the phase table. Post-audit additions: approval inbox fix
(rejections persist + decision history), `RejectionStore` +
`REJECTED_BY_HUMAN` engine memory, Reports viewer pillars/outcomes sections,
P4 Spark matrix evidence, `click` dependency fix.

---

## 2. POC phases — deliverables & verification

| Phase | Scope | Delivered | Verified by |
|---|---|---|---|
| **P0** | Physical medallion persistence | Bronze/Silver/Gold/Quarantine/Audit paths physically written per run; `written_paths` in API + UI | API + UI runs on patients & goat; `data/` tree inspected |
| **P1** | Managed Bronze ingestion | `_ingest.json` per dataset: source path, SHA-256 checksum, schema, row count, timestamp | File inspection per dataset |
| **P2** | Terminal remediation outcomes | Every proposal/decision gets a terminal outcome: `AI_APPLIED`, `HUMAN_APPROVED_AI`, `DETERMINISTIC_FIX`, `BELOW_CONFIDENCE_THRESHOLD`, `BLOCKED_SENSITIVE_DATA`, `NO_SAFE_PROPOSAL`, `REJECTED_BY_HUMAN` | `outcome_audit` in reports; 5+ unit tests; goat run showed all types with real LLM |
| **P3** | Seven quality pillars | completeness, uniqueness, validity, consistency, accuracy, timeliness, integrity + overall score + domain recommendation (healthcare/agriculture/pharmacy/telecom/banking/hospitality/generic) | Pillars in report JSON + UI cards; goat 99.59 agriculture, orders 99.69 generic |
| **P4** | Cross-platform evidence | Same engine contract under DATABRICKS/FABRIC/STANDALONE_SPARK/LOCAL labels; **strengthened**: byte-identical metrics on same wheel across Spark 3.5.9↔4.1.3, ANSI on/off, partitions 8↔32, fresh-venv install (wheel+pyspark only); zero `import databricks` | `reports/p4_portability_evidence.json`, `reports/p4_spark_matrix_evidence.json` |

**P4 honest boundary**: real managed-cluster execution (Databricks/Fabric/EMR)
is deployment work — the wheel is proven portable; cluster runs need platform
credentials and are NOT claimed.

---

## 3. Test & verification commands

```powershell
# Unit tests (no Spark, seconds)
.\.venv\Scripts\python.exe -m pytest packages\dq-core\tests\unit -v

# Integration tests (shared local Spark session, ~minutes)
.\.venv\Scripts\python.exe -m pytest packages\dq-core\tests\integration -v

# Full suite
.\.venv\Scripts\python.exe -m pytest packages\dq-core\tests -v

# Frontend build check
cd ui\frontend; npm run build

# Backend syntax check
.\.venv\Scripts\python.exe -m py_compile ui\backend\main.py
```

Current baseline: **74 unit + 65 integration tests green**.

---

## 4. Release process

### Versioning

- Engine version: `packages/dq-core/src/dq_core/version.py` → `__version__`
- Package version: `packages/dq-core/pyproject.toml` → `version`
- Frontend version: `ui/frontend/package.json` → `version`
- Keep all three in sync for a release.

### Build artifacts

```powershell
# 1. Engine wheel (the deployable artifact for any Spark platform)
.\.venv\Scripts\python.exe -m pip wheel packages\dq-core -w dist --no-deps
# → dist\dq_core-<version>-py3-none-any.whl  (~116 KB, deps: PyYAML + click)

# 2. Frontend production build
cd ui\frontend; npm run build   # → ui\frontend\dist\

# 3. Backend — no build step; runs via uvicorn
.\.venv\Scripts\python.exe -m uvicorn ui.backend.main:app --host 127.0.0.1 --port 8000
```

### Release checklist

1. `git status` clean except intended changes; review `git diff`
2. Full test suite green (unit + integration)
3. `npm run build` clean; backend compiles
4. Bump versions (3 files above) if releasing
5. Rebuild wheel → smoke-test install in fresh venv:
   `pip install dist\dq_core-*.whl pyspark` then `from dq_core.engine import DataQualityEngine`
6. Update `docs/POC.md` verification table + `AGENTS.md` memory if scope changed
7. Commit with conventional message (`feat:`/`fix:`/`docs:`/`chore:`) — **only on user approval**
8. Tag if cutting a formal release: `git tag v0.1.0`

### Deployment note (per platform)

The wheel is the only artifact a client platform needs:
- **Databricks**: cluster library `%pip install /Volumes/.../dq_core-*.whl` or job `libraries: [{whl: ...}]`
- **EMR/Dataproc**: `--py-files` or bootstrap install
- **Fabric**: notebook `%pip install` or lakehouse library
- Integration = ~6 lines: `engine.run(df, ruleset, policy, ctx, mode=...)` → use `result.accepted_dataframe`

---

## 5. Architecture map (where things live)

```
DQ-Accelerator/
├── packages/dq-core/          ← THE ENGINE (pure PySpark, zero cloud deps)
│   ├── src/dq_core/
│   │   ├── engine.py          ← DataQualityEngine public facade
│   │   ├── orchestration/     ← runner (modes), medallion, gate, result
│   │   ├── validation/        ← rule evaluation engine + expressions
│   │   ├── profiling/         ← DataProfiler + models
│   │   ├── generation/        ← CandidateRuleGenerator
│   │   ├── remediation/       ← deterministic + AI remediator + outcomes
│   │   ├── policies/          ← confidence engine, approvals, rejections
│   │   ├── ai/                ← provider interface, mock, dqx (lazy)
│   │   ├── pipelines/         ← manifest loader, lifecycle registry
│   │   ├── rules/             ← ruleset registry (versioned/immutable)
│   │   ├── registries/        ← file/sqlite backends + factory
│   │   ├── io/                ← sources/sinks (scheme-agnostic paths)
│   │   ├── reporting/         ← metrics, pillars, reports
│   │   ├── runtime/           ← config, compat, logging, local_spark
│   │   └── cli/               ← dqctl commands
│   └── tests/{unit,integration,e2e}/
├── ui/backend/main.py         ← FastAPI (jobs, lifecycle, approvals, reports)
├── ui/frontend/src/           ← React SPA (pages.tsx, api.ts)
├── config/
│   ├── pipelines/*.yaml       ← pipeline manifests (the onboarding contract)
│   ├── rulesets/*.yaml        ← versioned rule sets
│   ├── policies/*.yaml        ← remediation policies (threshold, gate, approvals)
│   ├── approvals/             ← approved_corrections.yaml + rejected_corrections.yaml
│   ├── ai/                    ← mock_ai_responses.yaml
│   └── engine*.yaml           ← engine config (paths, profiling, AI provider)
├── sample_data/bronze/        ← demo datasets (CSV/parquet)
├── registry/                  ← published rulesets + pipeline states (runtime)
├── data/                      ← medallion outputs (runtime, gitignored)
├── reports/                   ← DQ report JSONs + P4 evidence (gitignored mostly)
├── dist/                      ← built wheels
└── docs/                      ← this guide + POC.md + runbooks
```

## 6. Extension points (for future devs)

| To add… | Touch |
|---|---|
| New rule type | `metadata/models.py` (RuleType) + `validation/expressions.py` + generator mapping |
| New AI provider | implement `ai/base.py` interface + register in `ai/__init__.py` factory |
| New platform adapter | manifest `platform` enum + adapter contract doc (`docs/pipeline_adapters.md`); engine itself needs nothing |
| New domain template | `reporting/pillars.py` domain keywords + recommendation text |
| New registry backend | `registries/` — same interface as file/sqlite |
| UI page | `ui/frontend/src/pages.tsx` + route in `App` + API client in `api.ts` + endpoint in `main.py` |
