# End-to-End Demo Runbook — Fresh Machine → Working DQ Accelerator

**Audience:** the presenter. This document takes a machine with *nothing
installed* to a live, client-facing demo of the DQ Accelerator — in order,
with every file you need, every click you make, and what to say.

Everything below was verified against the real app. Where something is
synthetic or not-yet-real, it is labelled **HONEST BOUNDARY** — never improvise
past those labels in front of a client.

---

## 0. What this application IS (30-second version)

A **metadata-driven Data Quality framework on Apache Spark** that plugs into an
existing PySpark pipeline (Databricks, Fabric, EMR, Dataproc, or standalone)
with ~6 lines of code. It does not replace the client's pipeline — it governs
the data *inside* it:

```
client's raw data → BRONZE (persisted raw + lineage)
                 → SILVER (validated, deterministic + AI fixes, rest quarantined)
                 → GOLD   (cross-dataset integrity, summary rules)
```

Core promises the demo proves:

| Promise | How the demo shows it |
|---|---|
| One-click onboarding → full medallion | Onboard → **Auto E2E** card (profile→rules→publish→B→S→G in one job) |
| Profile + auto-generate rules, no code | Auto E2E or wizard → "Profile & generate rules" |
| Live step-by-step progress | JobStatus phase chips + progress line + bottom **Live activity** feed |
| Rules are versioned, immutable, steward-approved | Rule Sets page → **Rules (N)** panel + diff/activate/rollback |
| Deterministic fixes first, AI second | Outcome badges: `DETERMINISTIC_FIX`, `AI_APPLIED` |
| AI proposes, policy decides (90% threshold) | `BELOW_CONFIDENCE_THRESHOLD` quarantined |
| Sensitive data never leaves for external AI | `BLOCKED_SENSITIVE_DATA` outcome |
| Below-threshold → quarantine → human approves | Approvals page → `HUMAN_APPROVED_AI` |
| Physical medallion outputs + audit | Stage preview BRONZE/SILVER/GOLD/QUARANTINE rows |
| Seven-pillar quality score + domain advice | Report JSON `quality_pillars` |
| Portable Spark core | Same engine under all platform labels |

---

## 1. Fresh-machine prerequisites (install ONCE)

| Software | Version | Check | Download |
|---|---|---|---|
| Python | 3.12+ | `python --version` | python.org (tick "Add to PATH") |
| Java JDK | 21+ | `java -version` | Microsoft OpenJDK |
| Node.js | 18+ LTS | `node --version` | nodejs.org |
| Git | any | `git --version` | git-scm.com |

Copy the whole repo folder to the new machine (or `git clone`). No database,
no Docker, no cloud account is required for the local demo.

**Optional but recommended for the AI story:** a `.env` file in the repo root
(gitignored — never commit):

```
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<personal-access-token>
DQX_MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct
```

Without `.env` the app still runs fully — AI falls back to MockAI and every
simulated decision is labelled `simulated=true` in reports.

---

## 2. First boot — `DQ_start.bat`

Double-click **`DQ_start.bat`** in the repo root. It performs, in order:

1. Checks Python ≥3.12, Java ≥21, Node ≥18 — prints `[OK]`/`[MISSING]` per item
2. Creates `.venv`, installs `requirements.txt` (first run: several minutes)
3. If `DATABRICKS_TOKEN` is set, also installs `requirements-llm.txt`
4. `npm install` + `npm run build` for the frontend (first run: minutes)
5. Launches two windows and opens the browser:

| Service | URL | Purpose |
|---|---|---|
| **DQ-Backend** | http://127.0.0.1:8000 | FastAPI — all engine work, job queue |
| **DQ-Frontend** | http://localhost:5173 | React UI — what the client sees |
| API docs | http://127.0.0.1:8000/docs | Swagger UI (engineer audience) |

**Verify before demoing:**

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/health      # {"status":"ok",...}
Invoke-RestMethod http://127.0.0.1:8000/api/ai/status    # provider + credentials_set
```

- `credentials_set: true` → real Databricks LLM available
- `credentials_set: false` → MockAI mode (still demoable, say "simulated")

Stop everything: close the `DQ-Backend` and `DQ-Frontend` windows.

> **Memory config is already handled.** Spark memory/partitions live in
> `config/engine.yaml` (`runtime.*`) and can be changed live via
> **Settings → Apply config changes** or **Reload + restart Spark** — no code
> edit, no restart of the app needed for threshold/provider changes.

---

## 3. Asset inventory — what must exist in the repo

### 3.1 Sample datasets (`sample_data/` + `data/uploads/`)

**Auto-generated demo datasets (new — all verified end-to-end):**

| File | Rows | Domain | Auto-E2E result |
|---|---|---|---|
| `demo_insurance_claims.csv` | 20,000 | Insurance | 18 rules → 19,220 acc / 780 quar — **primary demo** |
| `demo_hospital_encounters.csv` | 50,000 | Healthcare | Full B→S→G — 48,405 acc / 1,595 quar |
| `demo_bank_loans.csv` | 60,000 | Banking | 33 rules — fast medium demo |
| `demo_airline_flights.csv` | 80,000 | Aviation | Bad statuses, negative delays, $1M fares |
| `demo_retail_inventory.csv` | 120,000 | Retail | Negative stock, bad categories |
| `demo_ecommerce_orders.csv` | 300,000 | E-commerce | 19 rules → 290,490 acc / 9,510 quar |
| `demo_telco_events.csv` / `.parquet` | **1,000,000** | Telecom | 22 rules → 974,714 acc / 25,286 quar — scale story |
| `demo_mongo_claims.json` | 20,000 | NoSQL documents | MongoDB-style docs (`_id`, nested objects) via JSON source |

**Classic datasets (`sample_data/bronze/` + `sample_data/minimal/`):**

| File | Rows | Domain | Best used for |
|---|---|---|---|
| `goat_herd_animals.csv` | 63 | Agriculture/livestock | **AI-outcome hero** — real LLM decisions, approvals |
| `hotel_reservations.parquet` | **1,000,000** | Hospitality | Large-file demo, auto-sampling path |
| `patients.csv` | ~38 | Healthcare | PHI/sensitive-column story |
| `sample_data/minimal/patients_minimal.csv` | 3 | Healthcare | Phase-verification fixture |
| `transactions.csv` | 40 | Finance | Gold referential-integrity demo |
| `customers.csv` | 38 | Retail | Ruleset versioning demo (v1.0 vs v1.1) |
| `employees.csv`, `sw_employees.csv`, `loans.csv`, `orders.csv`, `cdr_records.csv` | varies | HR/Finance/Retail/Telecom | Extra onboard demos; `cdr_records.csv` regenerates to ~1 GB via `scripts/` |

All datasets contain **seeded, realistic defects** (nulls, duplicates, bad
formats, case variants, outliers) — that is intentional; clean data demos
nothing. Generators: `scripts/gen_demo_datasets.py`, `scripts/gen_demo_datasets2.py`, `scripts/gen_hospital.py`.

### 3.2 YAML configuration (`config/`)

| Path | Role |
|---|---|
| `config/engine.yaml` | Engine runtime: Spark memory/partitions, AI provider, thresholds, sampling limit |
| `config/policies/default.yaml` | Quality gate (max quarantine rate), confidence threshold **0.90**, sensitive-column policy |
| `config/policies/healthcare.yaml` | Stricter PHI policy for the patients pipeline |
| `config/rulesets/*.yaml` | Versioned, immutable rule definitions per dataset+stage |
| `config/rules/lookups/*.yaml` | Approved value mappings (e.g. `boer→Boer`, country→ISO) |
| `config/pipelines/*.yaml` | Pipeline manifests — the "registration cards" |
| `config/approvals/approved_corrections.yaml` | Human-approved AI corrections store |
| `config/ai/mock_ai_responses.yaml` | Simulated AI fixtures (fallback only) |

### 3.3 Pre-registered pipelines (registry state on a fresh clone)

| Pipeline | Platform label | State | Dataset |
|---|---|---|---|
| `HEALTHCARE_PATIENT_PIPELINE_V2` | DATABRICKS | **ACTIVE** | patients |
| `SW_EMPLOYEES_PIPELINE` | DATABRICKS | **ACTIVE** | sw_employees |
| `TELECOM_CDR_1GB_PIPELINE` | DATABRICKS | **ACTIVE** | cdr_records (1 GB) |
| `GOAT_HERD_PIPELINE` | DATABRICKS | REGISTERED | goat_herd_animals |
| `DATABRICKS_TRANSACTION_PIPELINE` | DATABRICKS | REGISTERED | transactions |
| `HEALTHCARE_PATIENT_PIPELINE` | DATABRICKS | REGISTERED | patients |
| `EMPLOYEES_UI_DEMO` | STANDALONE_SPARK | REGISTERED | employees |
| `DATABRICKS_CUSTOMER_PIPELINE` | DATABRICKS | OFFBOARDED | customers |

> Registry/ruleset state lives in `registry/` (JSON) and `config/rulesets/`
> versions. If you demo on the *same* machine used for development, states may
> differ — check **Pipelines** page first.

---

## 4. Demo flow — in the order to present it

### Act 1 — Orientation (2 min): Dashboard + Pipelines

1. **Dashboard** — point at: active pipelines, jobs run, quality trend,
   quarantine backlog, AI provider status tile.
2. **Pipelines** — table of all registered pipelines with lifecycle state
   (`REGISTERED → TESTED → ACTIVE → PAUSED → OFFBOARDED`).
   **Say:** "Every client pipeline is governed by a manifest — platform label,
   datasets, per-stage rulesets, DQ mode. This is onboarding, not a rewrite."

### Act 2 — One-click onboarding (2–5 min): Auto E2E — THE hero moment

**Onboard** page → top card **Auto E2E — one click: profile → rules → publish → run**.

1. **Source file** dropdown → pick `demo_insurance_claims` (or **Browse / upload…**
   for any CSV/Parquet/JSON file — auto-fills pipeline ID from filename)
2. **Preview data** → first 20 rows table — *"raw input, defects visible"*
3. Pipeline ID auto-fills → Mode `REMEDIATE` → **▶ Run auto E2E**

Watch the live narrative — JobStatus chips + progress line + **bottom Live
activity feed** (step history, updates every 3s):

```
phase=profile   | profiled 20,000 rows · 14 anomalies · 18 candidate rules · 18 auto-approved
phase=validate  | ruleset demo_insurance_claims_silver@1.0 + manifest written · pipeline registered · gold ruleset ...
phase=read      | [BRONZE] persisting raw bronze layer + ingest manifest
phase=validate  | [SILVER] loading silver ruleset + profiling
phase=validate  | [SILVER] validating all rules against the data
phase=remediate | [SILVER] applying deterministic fixes
phase=ai        | [SILVER] AI corrections via LLM provider
phase=revalidate| [SILVER] re-validating corrected rows
phase=quarantine| [SILVER] building quarantine + quality gate
phase=report    | [SILVER] writing silver accepted + quarantine outputs
phase=validate  | [GOLD] loading gold ruleset (strict re-check) → validating → writing
phase=report    | writing run reports
```

*Say:* "One click onboards ANY file — the engine profiles it, generates rules
with confidence scores, publishes them immutably, generates the stricter Gold
ruleset, and runs the full Bronze→Silver→Gold medallion. Every step is live."*

On completion the result card shows accepted/quarantined per stage + two
buttons: **View auto-created profile →** (Profiles tab) and
**View ruleset + versions →** (Rule Sets tab).

> **Idempotent:** re-running the same dataset purges prior artifacts first —
> safe to demo repeatedly.

### Act 2b — Manual wizard (optional, 5 min): step-by-step governance

Same Onboard page, below the auto card (can be hidden via **Settings →
Onboard page sections → Manual run pipeline**). Use `goat_herd_animals.csv`
for the AI-outcome story or any client CSV/Parquet.

| # | Click | What happens | Say |
|---|---|---|---|
| 1 | Choose file → Upload | File lands in `data/uploads/` untouched | "Bronze is sacred — raw input is never mutated." |
| 2 | Dataset name `hotel_reservations`, stage **SILVER** → **Profile & generate rules** | Live phases: profiling → candidate rules (1–3 min on 1M rows) | "Profiler finds nulls, dupes, formats, ranges, anomalies — zero code." |
| 3 | Review candidates | ~35–45 rules with confidence + evidence | "Steward approves/rejects. Nothing auto-publishes. Reject STRING_LENGTH noise." |
| 4 | **Write ruleset + manifest + register** | Creates `hotel_reservations_v1.yaml` + pipeline manifest; publishes ruleset | "Immutable versioned YAML + registration in one click." |
| 5 | Pipelines → **test** → **activate** | State → TESTED → ACTIVE | "Governed lifecycle — test before activate." |

### Act 3 — The money shot (5–10 min): Run DQ

**Run DQ** → select the pipeline → **▶▶ Run all stages** → mode `REMEDIATE`.

Watch the phase chips with elapsed timer:
`spark → read → validate → AI remediation → revalidating → quarantine →
writing report → done`.

Result card shows:

- **Stage table** — BRONZE `PERSISTED` rows → SILVER/GOLD `COMPLETED` with
  input/accepted/quarantined/**score**/gate/time
- **Avg quality score** tile
- **Remediation outcomes** badges — the trust story
- **Recommendation** — domain-aware line (`hospitality:` / `agriculture:` …)
- **Written outputs** (expand) — every physical path under `data/`

### Act 4 — Inspect the evidence (5 min)

| Page | Show | Say |
|---|---|---|
| **Reports** | Open latest stage report JSON | "Full audit: per-rule results, `quality_pillars`, `outcome_audit`, AI decisions with `simulated` flag." |
| **Reports → AI activity** | Per-record AI fixes card | "Every AI fix auditable — original→proposed, confidence, LIVE/SIM badge." |
| **Reports → Stage data preview** | BRONZE/SILVER/GOLD/QUARANTINE rows | "Real parquet rows per layer — quarantine shows the actual rejected data." |
| **Profiles** | Auto-created profile per dataset | "The engine profiled this automatically — nulls, outliers, PII suspects, drift." |
| **Rule Sets → Rules (N)** | Click Rules on any ruleset card | "Here are the actual generated rules — type, column, confidence, evidence." |
| **Rule Sets** | Published versions, checksums, diff, activate/rollback | "Rules are data — versioned, checksummed, diffable, rollback-able." |
| **Anomalies** | Detected anomalies per dataset | "Statistical + format anomalies feed rule candidates." |
| **Approvals** | Pending/approved corrections | "Below-90% AI proposals wait here. Approve → next run applies as `HUMAN_APPROVED_AI`." |
| **Pipelines** | Lifecycle states + pause/activate/offboard | "Pipelines are governed assets — pause enforcement without code changes." |
| **Settings** | AI provider, thresholds, Spark config, **Sidebar tabs** + **Onboard page sections** toggles | "Hot-reload config; show/hide nav tabs and auto-vs-manual run for the audience." |

### Act 5 — (Optional, engineer audience) CLI path

```powershell
.\.venv\Scripts\python.exe -m dq_core.cli.main pipeline list
.\.venv\Scripts\python.exe -m dq_core.cli.main ruleset diff <id> 1.0 1.1
```

Full 21-step CLI demo script: `docs/poc_demo_script.md`.

---

## 5. Verified numbers to quote (do not improvise)

### Auto-generated demo datasets — all verified via Auto E2E

| Dataset | Rows | Rules | Accepted | Quarantined | Notes |
|---|---|---|---|---|---|
| `demo_insurance_claims` | 20,000 | 18 | 19,220 | 780 (~3.9%) | **Primary demo** — nulls, dup IDs, bad dates, $1M outliers |
| `demo_hospital_encounters` | 50,000 | ~22 | 48,405 | 1,595 (~3.2%) | Full B→S→G verified |
| `demo_ecommerce_orders` | 300,000 | 19 | 290,490 | 9,510 (~3.2%) | Bad emails, mixed dates, negatives |
| `demo_telco_events` | **1,000,000** | 22 | 974,714 | 25,286 (~2.5%) | SAMPLE profiling auto-engaged; `.parquet` = 32 MB vs 74 MB CSV |
| `demo_bank_loans` | 60,000 | 33 | — | — | Dup loan IDs, bad dates, negative income |

All gates PASS (quarantine rates far under the 40% policy threshold).

### Goat herd — hero dataset, ruleset `goat_herd_animals_silver@1.2`, REMEDIATE

```
BRONZE  63 rows persisted (raw + _ingest.json lineage)
SILVER  63 → 54 accepted / 6 quarantined   gate PASS   score 99.66
GOLD    54 → 53 accepted / 1 quarantined   gate PASS   score 99.69
```

Outcome audit — **all AI decisions `simulated=false` (real Databricks LLM)**:

| Outcome | Evidence |
|---|---|
| `AI_APPLIED` ×2 | `breed 'boer'`→`Boer`, `gender 'female'`→`FEMALE` @ **0.95 ≥ 90%** |
| `HUMAN_APPROVED_AI` ×2 | `health 'OK'`→`HEALTHY`, `vaccine 'done'`→`VACCINATED` @ 0.80, steward-approved |
| `BLOCKED_SENSITIVE_DATA` ×2 | `date_of_birth` values — never sent to external LLM |
| `DETERMINISTIC_FIX` | 3 duplicate rows removed |
| `NO_SAFE_PROPOSAL` ×6 | missing tags/dates, weight outlier → quarantined |

### Minimal healthcare fixture (3 rows) — P0–P4 verification

```
Silver: 3 in → 2 accepted / 1 quarantined     Gold: 2 in → 0 accepted / 2 quarantined
P3 score: overall 99.33 · validity 96.67 · healthcare recommendation
Ingest manifest: source SHA-256 0a819f28… + schema + row count + timestamp
```

### Portability evidence

`reports/p4_portability_evidence.json` — identical outcomes under
`DATABRICKS`, `FABRIC`, `STANDALONE_SPARK`, `LOCAL` on local Spark.

---

## 6. What the app is capable of (feature map)

| Area | Capability | Where |
|---|---|---|
| **Onboarding** | Upload CSV/Parquet → auto-profile → candidate rules → steward review → ruleset+manifest+register in one flow | Onboard |
| **Profiling** | Schema, nulls, duplicates, distinct counts, formats, ranges, categories, anomalies; FULL vs SAMPLE mode for large files | engine |
| **Rule engine** | NOT_NULL, UNIQUE, REGEX, RANGE, ALLOWED_VALUES, DATE_FORMAT, DATA_TYPE, STRING_LENGTH, DUPLICATE, REFERENTIAL_INTEGRITY, BUSINESS_RULE, SCHEMA_VALIDATION | rulesets |
| **Remediation** | Deterministic fixes (TRIM, DEDUPLICATE, lookup…) → AI proposals → confidence gate → revalidate → quarantine | REMEDIATE mode |
| **AI governance** | DQXProvider→Databricks Model Serving (Llama-3.3) or MockAI; per-decision `provider/model_version/simulated`; `max_requests_per_run` bound | engine.yaml + .env |
| **Confidence** | Default threshold 0.90, configurable per policy; boundary-tested | policies |
| **Sensitive data** | Column policy blocks external AI calls before they leave | policies |
| **Human-in-loop** | Approval store; approved corrections → `HUMAN_APPROVED_AI` next run | Approvals |
| **Medallion** | Physical BRONZE/SILVER/GOLD + per-stage quarantine + audit log + reports under `data/`/`reports/` | Run all stages |
| **Lineage** | `_ingest.json`: source path, SHA-256, schema, row count, timestamp | data/bronze/*/ |
| **Scoring** | Seven pillars + overall score + domain recommendation per stage | report JSON |
| **Versioning** | Immutable published rulesets, checksum, diff, activate, rollback | Rule Sets / CLI |
| **Lifecycle** | REGISTERED→TESTED→ACTIVE→PAUSED→OFFBOARDED; offboard preserves data | Pipelines |
| **Modes** | OBSERVE_ONLY (flag, pass through) / ENFORCE (split failures) / REMEDIATE (fix+quarantine) | manifest `dq_mode` |
| **UI** | Live job progress w/ phases+elapsed, duplicate-submit guard, anomaly inbox, quarantine review, reports browser, DQ Assistant chat | frontend |
| **CLI** | `dq_core.cli.main`: profile/run/pipeline/ruleset commands for engineers | terminal |
| **Portability** | Same `engine.run(df,…)` contract on Databricks/Fabric/EMR/Dataproc/standalone | adapters |

---

## 7. HONEST BOUNDARIES — say these if asked

| Claim | Truth |
|---|---|
| "Runs on real Databricks Spark?" | Engine + real Databricks **LLM** verified earlier. A run *on a Databricks cluster* is the one open P4 item — contract-tested locally, not yet executed on cloud Spark. |
| "Real AI?" | Yes — `DQXProvider` calls Databricks Model Serving when `.env` creds + `requirements-llm.txt` are installed; decisions show `simulated=false`. Without them → MockAI, labelled `simulated=true` with SIM badge in UI. |
| "Why so slow locally?" | Local single-node Spark: 20K rows ≈ 1–2 min, 1M rows ≈ 4–5 min (JVM + CSV parse + per-rule Spark jobs). Same engine on a Databricks cluster + Photon runs in seconds — speed is a runtime property, not a code property. Parquet sources are ~5–10× faster than CSV reads. |
| "Does Auto E2E skip human review?" | Yes — by design for the accelerator demo (auto-approves all candidates, `approve_confidence=0`). The manual wizard is the governed path where stewards review/approve/reject each rule. Both flows coexist; Settings can show/hide either. |
| "Do auto re-runs keep version history?" | Auto-E2E purges prior same-pipeline artifacts so reruns are idempotent — version history accumulates through the manual wizard/publish path instead. |
| "NoSQL as a data source?" | `demo_mongo_claims.json` proves document-shaped data (MongoDB-style `_id` + nested objects) flows through the same pipeline via the JSON source. Native MongoDB ingestion via spark-mongo connector is a roadmap item (needs cluster JVM deps). |
| "Pipelines are real?" | Manifests labelled `platform: DATABRICKS/FABRIC/...` are **synthetic stand-ins** for client pipelines (no client assets were supplied). Integration pattern is real and tested. |
| "1M rows = auto-sampling?" | Telco 1M profiling auto-engages SAMPLE mode (check report `mode=SAMPLE`); validation still covers all rows. Hotel parquet may run FULL under the 50 MB threshold. |
| "Production-ready?" | POC scope: no auth/RBAC, scheduling, multi-tenancy, or external alerting — those are P5+ roadmap. Formal schema-contract enforcement (expected-schema YAML → hard drift reject) is a noted gap; ODCS import + profile-driven type rules cover most of it. |

---

## 8. Pre-demo checklist

- [ ] `DQ_start.bat` ran clean; both service windows open
- [ ] `Invoke-RestMethod :8000/api/health` → ok; `/api/ai/status` → check `credentials_set`
- [ ] Browser at `localhost:5173`; hard-refresh once (Ctrl+F5)
- [ ] No job RUNNING (double-submit guard returns 409 by design — wait or pick another pipeline)
- [ ] Pick demo path: **goat** (fast, full AI spectrum) or **hotel 1M** (scale)
- [ ] `reports/` has prior runs if showing trend charts
- [ ] `.env` present if demoing real LLM; otherwise say "MockAI, labelled simulated"
- [ ] If hotel: expect 1–3 min profiling on 1M rows — that's a feature, narrate it

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| Port 8000 busy | Close stray `python`/`uvicorn` windows; re-run launcher |
| Settings shows MockAI | `.env` missing/invalid → set vars, restart launcher |
| Spark OOM on 1M rows | Settings → raise `runtime.driver_memory` → Reload + restart Spark |
| 409 on Run | A job is already running — wait for it or check Run DQ page |
| Quarantine gate fails | Expected on dirty data — OBSERVE_ONLY still reports; explain gate semantics |
| Lost state on fresh clone | Registry/rulesets are files — they travel with the repo |
