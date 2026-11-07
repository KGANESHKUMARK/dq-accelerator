# Profiling

Automatic data profiling over existing Spark DataFrames. Implemented in
`dq_core/profiling/` (`profiler.py`, `models.py`, `patterns.py`).

## 1. Entry points

```python
report = engine.profile(df, "customers", mode="FULL")            # default
report = engine.profile(df, "customers", mode="SAMPLE", sample_fraction=0.10)
report = engine.profile(df, "customers", mode="APPROXIMATE")     # approx_count_distinct etc.
```

| Mode | Behaviour |
|---|---|
| `FULL` | Exact counts/aggregations over the whole DataFrame |
| `SAMPLE` | Deterministic sample (`sample_fraction`, seeded) then full profiling on the sample; `sampled=True`, `sample_fraction` recorded |
| `APPROXIMATE` | Uses `approx_count_distinct` / approx aggregates for speed on large data; `approximate=True` flagged in the report |

All modes are **distributed** — stats are computed with Spark aggregations; the
driver only collects per-column summary rows, never the dataset.

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
`docs/poc_demo_script.md` STEP 5 — it detected the whitespace defect, the 150-year
outlier, mixed `yyyy-MM-dd`/`dd/MM/yyyy` dates, rare country/status values and
string-typed numerics/dates.

## 5. Design constraints honoured

- No `collect()` of raw rows; only bounded per-column aggregates.
- Deterministic sampling (seeded) so SAMPLE/APPROXIMATE runs are reproducible.
- Profile feeds `CandidateRuleGenerator` — profiling is the evidence layer for
  rule proposals; it never activates rules by itself.
