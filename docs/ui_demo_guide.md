# UI Demo & Onboarding Guide — DQ Accelerator

Client-facing walkthrough. Everything below was executed against the running
app (`DQ_start.bat` → backend :8000, frontend :5173). Screenshots should show
the **Run DQ** page and the **Onboard** wizard.

---

## Part A — Onboard a user dataset (5 steps, ~3 minutes)

The wizard mirrors what a client's data engineer will do. Use
`sample_data/bronze/goat_herd_animals.csv` for the live demo, or any client
CSV/Parquet.

| Step | UI action | What to say |
|---|---|---|
| 1. Upload | Onboard → choose file → **Upload** | "Drop the raw file. We store it untouched — Bronze is never mutated." |
| 2. Preview | Pick dataset name, stage **SILVER** → **Profile & generate rules** | Watch live progress: profiling → candidate rules. "The profiler finds nulls, duplicates, formats, ranges, anomalies — no code written." |
| 3. Review rules | Uncheck noise (STRING_LENGTH, risky DATA_TYPE), keep real checks | "Every rule shows confidence + evidence. The steward decides — nothing auto-publishes." |
| 4. Finalize | **Write ruleset + manifest + register** (publish + onboard ON) | "Immutable versioned YAML ruleset + pipeline manifest, published to the registry." |
| 5. Lifecycle | Pipelines → **test** → **activate** | "Governed lifecycle: test before activate, pause/offboard anytime." |

Result: pipeline shows `REGISTERED → TESTED → ACTIVE` with its ruleset
`<dataset>_silver@1.0`.

> Tip: reject `STRING_LENGTH` and unverified `DATA_TYPE` candidates — learned
> min/max length and inferred types produce false positives on real data.

## Part B — The demo run (the money shot)

Run DQ → select the pipeline → **▶▶ Run all stages** → mode `REMEDIATE`.

Watch the phase chips: `spark → read → validate → AI remediation →
revalidating → quarantine → writing report → done`, with elapsed timer.

### What the result card now shows (new in this build)

- **Stage table**: BRONZE `PERSISTED` rows → SILVER/GOLD `COMPLETED` with
  input/accepted/quarantined/gate/**Score**/time.
- **Avg quality score** tile (e.g. 99.6).
- **Remediation outcomes** badges — the trust story:
  `DETERMINISTIC_FIX`, `BLOCKED_SENSITIVE_DATA`,
  `BELOW_CONFIDENCE_THRESHOLD`, `NO_SAFE_PROPOSAL`.
- **Recommendation** line (domain-aware: agriculture/healthcare/…).
- **Written outputs** (expandable): every physical path —
  `data/bronze|silver|gold`, `data/quarantine/*`, `data/audit/*`, reports.

### Verified goat-herd numbers (say these — ruleset v1.2)

```
BRONZE  63 rows persisted (raw, unchanged + _ingest.json lineage)
SILVER  63 → 54 accepted / 6 quarantined   gate PASS   score 99.66
GOLD    54 → 53 accepted / 1 quarantined   gate PASS   score 99.69
```

Outcome evidence — **every path exercised with the real Databricks LLM**
(`simulated=false` on all AI decisions):

- `AI_APPLIED` ×2 — `breed 'boer'`→`Boer` and `gender 'female'`→`FEMALE`,
  LLM confidence **0.95 ≥ 90%** → auto-applied, then revalidated.
- `HUMAN_APPROVED_AI` ×2 — `health 'OK'`→`HEALTHY` and
  `vaccine 'done'`→`VACCINATED` at 0.80 (below threshold) → steward-approved
  via `approved_corrections.yaml` → applied on next run.
- `BLOCKED_SENSITIVE_DATA` ×2 — `date_of_birth` values never sent to the
  external LLM.
- `DETERMINISTIC_FIX` — 3 duplicate rows removed with survivor audit.
- `NO_SAFE_PROPOSAL` ×6 — missing tags/dates, weight outlier → quarantined
  for human review.

## Part C — Talking points (30-second versions)

1. **"We don't rewrite your pipeline."** Engine plugs into an existing
   Spark DataFrame; host keeps its SparkSession. 6 lines of integration.
2. **"Bronze is sacred."** Raw data lands unchanged with checksum + schema +
   timestamp lineage (`_ingest.json`).
3. **"Rules are data, not code."** YAML rulesets are versioned, immutable once
   published, steward-approved — diff/rollback built in.
4. **"AI proposes, policy decides."** Real Databricks LLM suggests fixes;
   ≥90% confidence + non-sensitive + auto_apply → applied; otherwise
   quarantined. Every decision is audited per record.
5. **"Nothing silently passes."** Below-threshold, blocked-sensitive, and
   unfixable records all land in quarantine with reasons — human approves,
   never the model alone.
6. **"Portable Spark core."** Same engine contract runs on Databricks,
   Fabric, EMR, Dataproc, standalone — verified identical results under all
   platform labels on local Spark.

## Part D — Pre-demo checklist

- [ ] `DQ_start.bat` launched; backend `:8000/api/health` = ok
- [ ] `.env` present (Databricks LLM creds) — `GET /api/ai/status` →
      `credentials_set: true`
- [ ] `GOAT_HERD_PIPELINE` state = ACTIVE (or re-run onboarding live)
- [ ] No job currently RUNNING (double-submit guard returns 409 by design)
- [ ] Browser on `http://localhost:5173`, Run DQ page open
- [ ] Reports dir has prior runs for trend charts

## Part E — If asked harder questions

| Question | Answer |
|---|---|
| "Did you run this on real Databricks Spark?" | Honest: engine + LLM endpoint verified; cloud Spark cluster run is the one open P4 item — contract-tested, not yet executed. |
| "What about 1 GB files?" | Local 1 GB CDR benchmark exists (~60s profile); memory/partitions configurable via `engine.yaml` + env vars. |
| "Can we approve quarantined rows?" | Yes — Quarantine page approve/reject feeds the approvals store; approved corrections apply as `HUMAN_APPROVED_AI` next run. |
| "Other platforms?" | Fabric/EMR/Dataproc manifests + adapter contracts ship in repo; same `dq-core` wheel drops into any PySpark job. |
