# Demo Walkthrough — UI click → code path

Demo-scoped nav: **Dashboard · Onboard · Run DQ · Anomalies · Approvals · Reports · Settings**

Say-this lines are in *italics*. File refs are repo-relative.

---

## ACT 1 — Dashboard (30 sec)

**Click:** Dashboard tab.

*"This is the control tower — pipeline count, job health, pending approvals badge."*

| UI element | Code path |
|---|---|
| Pipeline cards / counts | `api.pipelines()` → `GET /api/pipelines` → `ui/backend/main.py` `_pipelines()` → `PipelineRegistry` (NoSQL `pipelines` collection, YAML fallback) |
| Pending approvals badge | `api.quarantinePending()` → `GET /api/quarantine/pending` → reads last REMEDIATE reports' `remediation_results` |
| Health dot | `api.health()` → `GET /api/health` → engine + Spark session status |

---

## ACT 2 — Onboard → Auto E2E (the hero moment, ~2 min)

**Click:** Onboard tab → top card **"⚡ Auto E2E"** → pick `demo_insurance_claims` → **▶ Run auto E2E**.

*"One click does the whole governed lifecycle — watch the live phases."*

### What happens live (JobStatus card)

| Live phase you see | Code path |
|---|---|
| `phase=profile \| profiling demo_insurance_claims` | `api.autoRun()` → `POST /api/auto/run` → `J.submit("auto_e2e")` → `_do_auto_e2e()` → `_do_bootstrap_preview()` → `engine.profile()` → `profiling/profiler.py` (column stats, anomalies, IQR outliers) |
| PII suspects appear | `profiling/profiler.py` `_anomalies()` → `pii/detector.py` heuristics → `PII_SUSPECTED` anomalies |
| Drift vs last baseline | `_do_bootstrap_preview` → `profiles` collection lookup → `profiling/drift.py` `detect_drift()` |
| `phase=profile \| generating candidate rules` | `generation/generator.py` `CandidateRuleGenerator.generate()` → NOT_NULL/UNIQUE/RANGE/DATA_TYPE/ALLOWED_VALUES candidates w/ confidence |
| Profile persisted | `_do_bootstrap_preview` → `store.collection("profiles").insert_one()` → Profiles tab viewer |
| `phase=validate \| N rules approved — writing ruleset + manifest` | `_purge_pipeline_artifacts()` (idempotent reruns) → `onboard_finalize()` → `rules/registry.py` publish (immutable checksum) + `pipelines/manifest.py` manifest YAML + NoSQL `manifests`/`pipelines` |
| `phase=read \| landing raw data…medallion stages` | `_do_pipeline_run()` → `orchestration/medallion.py` `MedallionRunner.run()` → `raw_df.persist(MEMORY_AND_DISK)` → BRONZE write + ingest manifest → `engine.run()` silver |
| `phase=validate / remediate / revalidate` | `engine.run()` → `validation/engine.py` + `validation/expressions.py` (per-rule Spark checks) → `remediation/deterministic.py` + `ai_remediation.py` → `policies/confidence.py` 90% gate → quarantine |
| `✓ COMPLETED` | `reports/<dataset>_silver_DQRUN-*.json` written → `execution_metrics` + `quality_pillars` + `outcome_audit` |

*Key lines to show if asked:* `main.py` `_do_auto_e2e` (~line 1715), `medallion.py` `run()` (~line 92), `expressions.py` `build_row_check` (~line 96).

---

## ACT 3 — Reports (the proof, ~1 min)

**Click:** Reports tab → click the newest report → scroll → **AI activity** card → **Stage data preview**.

| UI element | Code path |
|---|---|
| Report list | `api.reports()` → `GET /api/reports` → `reports/*.json` sorted by mtime |
| Report detail (scores, pillars, audit) | `api.report(name)` → full JSON: `execution_metrics`, `quality_pillars` (7 pillars), `outcome_audit.items`, `validation_results` |
| **AI activity card** | `api.aiActivity()` → `GET /api/ai/activity` → scans reports' `remediation_results.{ai_decisions, outcome_audit.items, summary.ai_provider}` → table: column, value→proposal, confidence, outcome, LIVE/SIM badge |
| Stage preview rows | `api.dataPreview(ds, stage)` → `GET /api/data/preview` → `spark.read.parquet(data/<stage>/<dataset>)` → actual Bronze/Silver/Gold/Quarantine rows |

*Say:* "Every AI fix is auditable — original value, proposed value, confidence, and whether it was auto-applied or sent to human approval. The SIM/LIVE badge tells you if it was a real LLM call or the deterministic fallback."

---

## ACT 4 — Approvals (human-in-loop, ~30 sec)

**Click:** Approvals tab → pending quarantine records → Approve/Reject → **Remembered corrections** card.

| UI element | Code path |
|---|---|
| Pending inbox | `api.quarantinePending()` → last REMEDIATE reports' low-confidence/`NO_SAFE_PROPOSAL` records |
| Approve / Reject | `api.approveCorrection()` / `api.rejectCorrection()` → `config/approvals/approved_corrections.yaml` + `rejected_corrections.yaml` + NoSQL `approvals`/`rejections` |
| Remembered corrections | `api.quarantineDecisions()` → deduped `dataset+column+value→value` map — engine auto-applies `HUMAN_APPROVED_AI` on future runs |

*Say:* "AI proposes, steward decides once, engine remembers forever — that's the governance loop."

---

## ACT 5 — Anomalies (what profiling caught, ~30 sec)

**Click:** Anomalies tab.

| UI element | Code path |
|---|---|
| Anomaly list | stored per-run anomalies → `PII_SUSPECTED`, `HIGH_NULL_RATE`, `DOMINANT_VALUE`, outliers, `DRIFT` |
| Source | profile reports + `profiles` NoSQL collection + report `anomalies` |

---

## ACT 6 — Run DQ (optional deep-dive, ~1 min)

**Click:** Run DQ → governed pipeline (manifest-locked) OR ad-hoc (File/Table/Delta/JSON selector + auto-suggest ruleset/policy/pipeline-id).

| UI element | Code path |
|---|---|
| Governed run | `api.pipelineRun(pid)` → `POST /api/pipelines/{pid}/run` → `_do_pipeline_run()` → manifest `test_data`/`test_data_format` → `_read_any_source()` (CSV/Parquet/JSON/TABLE/DELTA) → `MedallionRunner` |
| Ad-hoc run | `api.run()` → `POST /api/dq/run` → `_do_run()` → `engine.load_ruleset()` + `_read_any_source()` → `engine.run()` |
| Source preview button | `api.sourcePreview()` → `GET /api/data/source-preview` → first 20 rows, no full scan |
| Live phases + cancel + stall warning | `usePoll(jobId)` → `GET /api/jobs/{id}` + `/stream` SSE → `jobs.py` `_prog()` checkpoints → `POST /api/jobs/{id}/cancel` → `JobCancelled` at next phase |
| Failed job details | red error box → `jobs.error` = full Python traceback |

---

## The 60-second architecture line (whiteboard)

```
UI (React) → FastAPI → jobs.py (worker threads, phase progress, cancel)
→ DataQualityEngine → profiler / validator / remediation / confidence gate
→ MedallionRunner → Spark DataFrame → Bronze/Silver/Gold/Quarantine parquet
→ reports/*.json → UI Reports/AI-activity/Approvals
```

**Metadata everywhere:** rulesets+manifests in `config/` YAML + NoSQL `data/metadata.nosql.json` (TinyDB; MongoDB/DynamoDB pluggable). Profiles, candidates, approvals, jobs, metrics — all in collections.

## If they ask "where's the AI?"

`config/engine.yaml` → `ai.provider: dqx` → `ai/dqx_provider.py` → Databricks Model Serving (`databricks-meta-llama-3-3-70b-instruct`). Falls back to `MockAI` when deps/token missing — the SIM badge shows which ran. `requirements-llm.txt` enables live calls.

## Honest boundaries (say before they ask)

- Local single-node Spark — 1M rows ≈ minutes; Databricks cluster + Photon = seconds. Speed is runtime, not code.
- DELTA needs Databricks/EMR jars; TABLE needs registered catalog tables (temp views work locally).
- P4 cloud execution not done — platform labels verified locally only.
