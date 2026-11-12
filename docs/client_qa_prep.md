# Client Q&A Prep — DQ Accelerator

> Every question from the project review transcript (and likely follow-ups),
> mapped to what we **actually built** with evidence pointers. Rule: never claim
> what isn't verified — each answer cites the artifact.

---

## 1. The reviewer's exact questions → our answers

### "Is there still a dependency on Hadoop libraries for Spark?"
**Answer:** No. The engine uses **DataFrame + SQL-function APIs only** — no RDDs,
no SparkContext, no JVM access, no Python UDFs (`runtime/compat.py` docstring).
Hadoop appears only as a *path scheme* (`s3://`, `dbfs://`, `abfs://`) inside
Spark's own filesystem layer — that's Spark, not a Hadoop dependency. Minimum
Spark 3.5; verified identical results on Spark 3.5.9 ↔ 4.1.3
(`reports/p4_spark_matrix_evidence.json`). Wheel contains **zero** cloud/Hadoop
imports — 74 files audited.

### "What is there in the metadata? You used CSV as metadata — not efficient!"
**Answer:** Metadata is **not CSV and not flat files anymore**. Governance
metadata lives in a **NoSQL document store** ("meta tables"/collections):
`ruleset_index`, `ruleset_versions`, `pipelines`, `manifests`, `policies`,
`approvals`, `rejections`, `profiles`, `candidates`, `jobs` —
`dq_core/metadata/store.py`. Backends: embedded TinyDB (default, zero-install),
MongoDB, **DynamoDB** (boto3, DynamoDB Local supported) — switch via
`registry.backend: nosql` + `metadata.engine`. YAML remains the *authoring/
review/export* format — versioned, diffable, git-friendly — but runtime state
is in the document DB. Live: `/api/metadata/status` shows collection counts.

### "What is the concept of registry? Where is data saved?"
**Answer:** Two registries with hard guarantees:
- **RuleSetRegistry** — immutable published versions, SHA-256 checksum verified
  on every read, tamper → `IntegrityError`, modify-after-publish →
  `ImmutableVersionError`, append-only history, activate/rollback.
- **PipelineRegistry** — 7-state lifecycle `DRAFT→REGISTERED→TESTED→ACTIVE⇄
  PAUSED→OFFBOARDED`; PAUSED/OFFBOARDED pipelines pass data through untouched;
  every execution recorded for metrics/trends.
- Storage is pluggable: file / SQLite / NoSQL doc store — same interface.

### "Show me bronze/silver/gold — what's the logic in each?"
**Answer:** `MedallionRunner` (`orchestration/medallion.py`):
- **BRONZE** — raw landing persisted untouched + `_ingest.json` lineage
  (source checksum, schema, row count) + profiling. Raw is *intentionally*
  unvalidated — that's the medallion contract.
- **SILVER** — full REMEDIATE: validate → deterministic fixes + dedupe →
  intermediate re-validation → AI proposals → confidence policy (90% gate,
  approvals, sensitive-block, rejections) → final revalidation → accepted /
  quarantine / removed-duplicates + audit log.
- **GOLD** — ENFORCE-only publication rules on Silver-accepted rows +
  quality gate → `data/gold/<dataset>`.
- Every stage writes physical outputs + JSON report (`data/`, `reports/`).

### "Did you implement DQ for the bronze layer?"
**Answer:** Bronze = persistence + profiling + lineage (by design — raw layer
is never corrected). DQ *rules* execute at Silver; Gold re-checks publication
constraints. Profiling runs at Bronze ingest so rule candidates are evidence-
based. This is the standard medallion pattern — honest framing.

### "How did you pass the file and generate rules? Show the recommendation component."
**Answer:** `CsvSource` reads with `inferSchema=false` — Bronze keeps raw text,
typing happens *after* validation (deliberate: don't trust inferred types on
dirty data). `DataProfiler` runs 5 Spark jobs capturing: null counts/%,
distinct counts, min/max, percentiles, stddev outliers, top values, structural
patterns, date-format matches, inferred types, PK/near-key candidates,
duplicates, anomalies. `CandidateRuleGenerator` maps evidence → rules with
**per-rule confidence + evidence + timestamp**, status CANDIDATE (never
executable until steward approves).

### "What sort of rules? Walk me through. Are we covering the 5-6 quality pillars?"
**Answer — this is our strongest card:**
- **~15 rule types**: NOT_NULL, UNIQUE, RANGE, REGEX, ALLOWED_VALUES,
  SCHEMA_VALIDATION, REFERENTIAL_INTEGRITY, DUPLICATE, DATE_FORMAT,
  STRING_LENGTH, DATA_TYPE, ROW_COUNT + dataset/row scopes.
- **All 7 quality pillars implemented** — completeness, uniqueness, validity,
  consistency, accuracy, timeliness, integrity — scored per run with
  MEASURED/NOT_MEASURED status, overall score, and **domain-specific
  recommendations** (healthcare/agriculture/pharmacy/telecom/banking/
  hospitality/generic). The reviewer said the other POC "partially covers"
  pillars — ours has all 7 verified in reports + UI pillar cards.
  Evidence: goat run 99.66, orders 99.69, reports/*.json `quality_pillars`.

### "Where are rules saved? Show rule catalog / rule 001."
**Answer:** Rules are **YAML documents** (not CSV): `config/rulesets/*.yaml` —
each rule has `rule_id`, `name`, `rule_type`, `column`, `severity`, `config`,
`remediation` block, `status` (CANDIDATE→APPROVED→PUBLISHED), `generation`
evidence. Published versions are immutable + checksummed in the registry.
Versioning demo: `orders_silver` 1.0→1.3 with diff/rollback in UI.

### "Did you look at DQX? Why didn't you use it?"
**Answer — we DID, and we use it:**
- `ai.provider: dqx` → `databricks-labs-dqx[llm]` via LiteLLM → Databricks
  Model Serving (`databricks-meta-llama-3-3-70b-instruct`). Real calls verified
  (`simulated=false` per decision; auto-fallback to MockAI without creds).
- **Why not DQX's validation engine**: it's Databricks-coupled (Unity Catalog,
  Delta, Lakeview). Ours is portable pure-PySpark — same wheel on EMR/Fabric/
  Dataproc/standalone. We took DQX's LLM capability, kept the portable core.
- Honest gap acknowledged: DQX has 80+ checks, streaming, ODCS contracts,
  Isolation-Forest anomaly detection, Lakeview dashboards, MCP server — ours
  has ~15 core rule types, batch only, statistical anomalies. Position: ours
  is the *portable governance/remediation layer*; DQX could be an adapter
  check-source on Databricks (documented extension point).

### "CSV for a 1M-record file — show bronze treatment."
**Answer:** Format-agnostic — engine sees DataFrames, not files. CSV is just
one `SourceSpec` format (PARQUET/CSV/DELTA/JSON/TABLE all supported).
Hotel demo: 1M-row parquet (42.8MB) onboarded through UI wizard end-to-end.
Large-file auto-sampling (>50MB → SAMPLE profile) built in.

---

## 2. Questions they WILL ask next (prepare these)

| Question | Our answer |
|---|---|
| "Concurrent updates to rules?" | Document-store collections (Mongo/Dynamo) are multi-writer; immutability enforced by checksum not locks; version = new doc, never overwrite |
| "Rule versioning + auditability?" | Immutable published versions, append-only history, `record_execution` per run, decision history endpoint, `_ingest.json` lineage |
| "Scale — 1M/1GB rows?" | Verified: hotel 1M rows, telecom CDR 630K/100MB, auto-sampling for >50MB; Spark does the work, engine adds ~6 DataFrame passes |
| "Sensitive data / PII?" | Sensitive columns never egress to external LLM (`BLOCKED_SENSITIVE_DATA` outcome); approval gates; policy YAML per domain |
| "What if AI proposes wrong fix?" | 90% confidence gate + mandatory revalidation — a "fix" that still fails → quarantine `CORRECTION_FAILED_REVALIDATION`; steward approve/reject persisted (`approved_corrections.yaml`/`rejected_corrections.yaml` → NoSQL collections) |
| "Rejected proposal comes back next run?" | **No** — `RejectionStore` → `REJECTED_BY_HUMAN` terminal outcome, permanent suppression |
| "How is this different from Great Expectations / dbt tests / DQX?" | Portable Spark-core (not Databricks-locked like DQX, not YAML-sprawl like GX), remediation+approval loop, immutable versioning, medallion persistence, self-contained UI — DQX adapter is a documented extension |
| "Where does compute run?" | Anywhere Spark 3.5+ runs — verified local Spark 3.5.9/4.1.3 identical; cloud run is deployment (wheel + 6-line hook), contract-proven |
| "Streaming?" | Not in POC scope — batch DataFrames only (honest; DQX covers streaming on Databricks) |
| "Security/ auth?" | Deferred P5 — local POC has none; document as roadmap |
| "Can we see it fail safely?" | OBSERVE_ONLY mode = zero data change; ENFORCE quarantines without fixing; gate fails pipeline on thresholds |

## 3. The one-line pitch (memorize)

> "Portable Spark data-quality engine: profiles data → generates candidate
> rules → steward approves → immutable versioned rulesets in a NoSQL metadata
> store → validate/fix/quarantine with a 90% AI-confidence gate and full audit —
> same wheel on Databricks, Fabric, EMR, or standalone Spark."

## 4. If they push on "primitive vs DQX"

Don't defend breadth — reposition: *"DQX is a Databricks-native checker.
Ours is the governance + remediation + portability layer that works on ANY
Spark — and on Databricks we can run DQX checks through an adapter. The demo
you saw (approvals, rejection memory, medallion persistence, 7 pillars,
Spark-version matrix) is the part DQX leaves to you."*
