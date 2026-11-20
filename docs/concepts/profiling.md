# Profiling

Automatic data profiling over existing Spark DataFrames. Implemented in
`dq_core/profiling/` (`profiler.py`, `models.py`, `patterns.py`).

## 1. Entry points

```python
report = engine.profile(df, "customers", mode="FULL")            # default
report = engine.profile(df, "customers", mode="SAMPLE", sample_fraction=0.10)
report = engine.profile(df, "customers", mode="APPROXIMATE")     # approx_count_distinct etc.
report = engine.profile(df, "customers", mode="APPROXIMATE",
                        sample_fraction=0.30, limit_rows=10_000)  # DQX-style cap
```

| Mode | Behaviour |
|---|---|
| `FULL` | Exact counts/aggregations over the whole DataFrame |
| `SAMPLE` | Deterministic sample (`sample_fraction`, seeded) then full profiling on the sample; `sampled=True`, `sample_fraction` recorded |
| `APPROXIMATE` | Uses `approx_count_distinct` / approx aggregates for speed on large data; `approximate=True` flagged in the report |

`limit_rows` (optional) applies `.limit(n)` **after** sampling and before any
profiling pass — the DQX pattern: *sample generously, cap what you actually
profile*. It composes with every mode (`FULL` + `limit_rows` also works).

All modes are **distributed** — stats are computed with Spark aggregations; the
driver only collects per-column summary rows, never the dataset.

## 1a. Auto-detect strategy (DQX-style, used by the UI/backend)

The backend never asks the user to pick a profiling mode. `_auto_profile_config()`
in `ui/backend/main.py` chooses `(mode, sample_fraction, limit_rows)` from the
**source file size**, mirroring `databrickslabs/dqx` (which samples 30% then
limits to 1,000 records — we scale the cap to our file sizes):

| Source size | Mode | Sample | Row limit | Rows actually profiled |
|---|---|---|---|---|
| < 50 MB | `FULL` | — | — | all rows |
| 50–100 MB | `SAMPLE` | 5% | 100,000 | ≤ 100K |
| 100–500 MB | `SAMPLE` | 10% | 50,000 | ≤ 50K |
| > 500 MB | `APPROXIMATE` | 30% | 10,000 | ≤ 10K |

Thresholds are constants inside `_auto_profile_config()` — tune them there for a
POC; promoting them to `engine.yaml` is a straightforward follow-up.

The same config feeds **all three profiling entry points**: the standalone
`/api/profile` job, the onboarding bootstrap preview (wizard Step 3), and Auto
E2E (which calls the preview path) — so every route gets identical behaviour.

### Large-file read path

For CSV sources above the threshold, `_read_for_profiling()` first converts the
file to a **temporary Parquet** directory (`.tmp/profile_<ds>_<ts>.parquet`),
profiles the Parquet, then deletes it in a `finally` block. Columnar reads are
~10–50× faster than re-parsing CSV text in every Spark job, and Parquet footer
metadata lets `row_count` skip a full `count()` scan in sampled modes.

### Verified numbers (local Spark, this machine)

| File | Size | Rows | Auto config | Profiling rows | Auto E2E (BRONZE) |
|---|---|---|---|---|---|
| `contacts_500.parquet` | <1 MB | 500 | FULL | 500 | ~30 s |
| `financial_txn_1gb.csv` | 677.7 MB | 8,000,000 | APPROXIMATE 30% → 10K | **10,000** | **2 min 54 s** |

Before the row limit, the same 677 MB file took ~9.75 min to profile (~80K
sampled rows, 1% fraction, no cap). The cap — not the sample fraction — is what
makes large-file profiling fast.

### Honest trade-offs

- Values rarer than ~0.1% of rows may be absent from the profiled sample, so
  `top_values` / `candidate_allowed_values` can miss them and approximate
  distinct counts are less exact. Null rates, min/max, means, percentiles,
  outlier counts and pattern detection remain statistically sound.
- **Profiling only feeds rule generation.** Silver/Gold validation still scans
  and checks **every row** — governance coverage is unchanged; only the
  statistics-gathering step is bounded (exactly like DQX).

## 2. What is profiled (per column)

`ColumnProfile` fields: `data_type` (Spark type), `inferred_type` (semantic guess:
text / integer / decimal / email / date / categorical / identifier / empty),
`non_null_count`, `null_count`, `null_percentage`, `distinct_count`
(+`distinct_is_approximate`), `unique_count`, `duplicate_value_count`,
`min`/`max`, `mean`, `stddev`, `percentiles` (p25/p50/p75/p95),
`negative_count`, `outlier_count` (>3σ), `min_length`/`max_length`/`avg_length`,
`blank_count`, `whitespace_count`, `numeric_castable_percentage`,
`integer_castable_percentage`, `email_match_percentage`,
`uppercase_percentage`, `date_format_matches` + `dominant_date_format` +
`date_min`/`date_max`, `top_values`, `top_patterns`,
`candidate_allowed_values`.

Dataset level: `row_count`, `column_count`, `schema`, `duplicate_rows`,
`duplicate_groups`, `primary_key_candidates`, `near_key_candidates`,
`anomalies[]`, `schema_anomalies[]`, `spark_jobs` count, `duration_ms`,
`profiled_at`, `mode`, `sampled`, `approximate`.

## 3. Semantic inference & patterns (`patterns.py`)

- **Patterns:** values are generalized to structural masks (e.g. `Aaaaa.aaaa@a.aa`
  → email-like, `9999-99-99` → date-like, `A9999` → id-like). `pattern_to_regex`
  converts a dominant pattern into a candidate REGEX rule.
- **Inferred types:** email regex hit-rate, date-format match rates, numeric
  castability, categorical cardinality → drive candidate rule confidence.
- **Anomaly types observed in the POC run:**
  `LEADING_TRAILING_WHITESPACE`, `OUTLIERS`, `TYPE_INCONSISTENCY`,
  `RARE_CATEGORY_VALUES`, `INCONSISTENT_FORMATTING`, `MIXED_DATE_FORMATS`,
  schema anomalies `TYPED_DATA_STORED_AS_STRING`.

## 4. Output

`ProfileReport` → `to_dict()` / `to_json()` / `write_json(path)` /
`summary_text()`. Example real output (customers, 38 rows) is reproduced in
`docs/presentation/poc_demo_script.md` STEP 5 — it detected the whitespace defect, the 150-year
outlier, mixed `yyyy-MM-dd`/`dd/MM/yyyy` dates, rare country/status values and
string-typed numerics/dates.

## 5. Design constraints honoured

- No `collect()` of raw rows; only bounded per-column aggregates.
- Deterministic sampling (seeded) so SAMPLE/APPROXIMATE runs are reproducible.
- Profile feeds `CandidateRuleGenerator` — profiling is the evidence layer for
  rule proposals; it never activates rules by itself.
