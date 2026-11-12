"""PySpark data profiler.

Only aggregates and bounded top-N results are collected to the driver; the dataset itself is
never collected. A profile normally costs 4-5 Spark jobs regardless of the number of columns:

1. one wide aggregation (counts, distinct, min/max, numeric + string statistics, castability,
   date-format and e-mail match rates) for every column;
2. full-row duplicate statistics;
3. value frequencies (top values, candidate allowed values, unique counts) - skipped for
   high-cardinality columns in APPROXIMATE mode;
4. structural patterns of string columns (top N per column);
5. outlier counts (|z| > k) for numeric / numeric-like columns.
"""

from __future__ import annotations

from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dq_core.profiling.models import ColumnProfile, ProfileReport
from dq_core.profiling.patterns import CANDIDATE_DATE_FORMATS, EMAIL_REGEX, SPECIAL_NAME_RE, structural_pattern
from dq_core.runtime.compat import col, safe_cast, safe_to_date, safe_to_timestamp
from dq_core.runtime.config import ProfilingSettings
from dq_core.runtime.logging import get_logger, log_event
from dq_core.runtime.schema import schema_to_list, user_columns
from dq_core.runtime.utils import Stopwatch, utc_now_iso

_logger = get_logger("profiling")
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double")


def _is_numeric(dtype: str) -> bool:
    return dtype in NUMERIC_TYPES or dtype.startswith("decimal")


def _pct(part: float | int | None, whole: int) -> float:
    return round(100.0 * float(part or 0) / whole, 2) if whole else 0.0


def _num(value: Any) -> float | None:
    return None if value is None else round(float(value), 6)


class DataProfiler:
    def __init__(self, settings: ProfilingSettings | None = None) -> None:
        self.settings = settings or ProfilingSettings()

    def profile(self, df: DataFrame, dataset: str, *, mode: str | None = None,
                sample_fraction: float | None = None) -> ProfileReport:
        s = self.settings
        mode = (mode or s.mode).upper()
        fraction = sample_fraction or s.sample_fraction
        started = utc_now_iso()
        jobs = 0
        with Stopwatch() as sw:
            source_rows: int | None = None
            data = df.select(*[col(c) for c in user_columns(df.columns)])
            if mode == "SAMPLE":
                source_rows = data.count()
                jobs += 1
                data = data.sample(withReplacement=False, fraction=fraction, seed=s.sample_seed)
            approximate = mode == "APPROXIMATE"
            data = data.persist(StorageLevel.MEMORY_AND_DISK)
            columns = data.columns
            dtypes = dict(data.dtypes)
            strings = [c for c in columns if dtypes[c] == "string"]

            # ---------------------------------------------------------------- job 1: wide aggregation
            aggs = [F.count(F.lit(1)).alias("__rows")]
            for i, c in enumerate(columns):
                x, t = col(c), dtypes[c]
                aggs += [F.count(x).alias(f"nn{i}"),
                         (F.approx_count_distinct(x, 0.01) if approximate else F.countDistinct(x)).alias(f"dc{i}")]
                if t == "string" or _is_numeric(t) or t in ("date", "timestamp", "timestamp_ntz"):
                    aggs += [F.min(x).cast("string").alias(f"mn{i}"), F.max(x).cast("string").alias(f"mx{i}")]
                numeric = safe_cast(c, "double") if t == "string" else (x.cast("double") if _is_numeric(t) else None)
                if numeric is not None:
                    aggs += [F.count(numeric).alias(f"nc{i}"), F.avg(numeric).alias(f"avg{i}"),
                             F.stddev_samp(numeric).alias(f"sd{i}"), F.min(numeric).alias(f"nmn{i}"),
                             F.max(numeric).alias(f"nmx{i}"),
                             F.percentile_approx(numeric, [0.25, 0.5, 0.75], 1000).alias(f"pc{i}"),
                             F.sum((numeric < 0).cast("int")).alias(f"neg{i}")]
                if t == "string":
                    length = F.length(x)
                    aggs += [F.min(length).alias(f"lmn{i}"), F.max(length).alias(f"lmx{i}"), F.avg(length).alias(f"lav{i}"),
                             F.sum((F.trim(x) == "").cast("int")).alias(f"bl{i}"),
                             F.sum((x != F.trim(x)).cast("int")).alias(f"ws{i}"),
                             F.count(safe_cast(c, "bigint")).alias(f"ic{i}"),
                             F.sum(x.rlike(EMAIL_REGEX).cast("int")).alias(f"em{i}"),
                             F.sum(((x == F.upper(x)) & x.rlike("[A-Za-z]")).cast("int")).alias(f"up{i}")]
                    for j, fmt in enumerate(CANDIDATE_DATE_FORMATS):
                        aggs.append(F.count(safe_to_timestamp(x, fmt)).alias(f"df{i}_{j}"))
                    parsed = safe_to_date(x, CANDIDATE_DATE_FORMATS[0])
                    aggs += [F.min(parsed).cast("string").alias(f"dmn{i}"), F.max(parsed).cast("string").alias(f"dmx{i}")]
            stats = data.agg(*aggs).collect()[0].asDict()
            jobs += 1
            rows = int(stats["__rows"])

            # ---------------------------------------------------------------- job 2: duplicates
            dup = (data.groupBy(*[col(c) for c in columns]).count().where(F.col("count") > 1)
                   .agg(F.count(F.lit(1)).alias("groups"), F.sum("count").alias("rows")).collect()[0])
            jobs += 1
            duplicate_groups = int(dup["groups"] or 0)
            duplicate_rows = int(dup["rows"] or 0) - duplicate_groups  # rows beyond the first occurrence

            # ---------------------------------------------------------------- job 3: value frequencies
            distinct_counts = {c: int(stats[f"dc{i}"] or 0) for i, c in enumerate(columns)}
            freq_cols = [c for c in columns if not approximate or distinct_counts[c] <= s.max_allowed_values]
            top_values: dict[str, list[dict[str, Any]]] = {c: [] for c in columns}
            unique_counts: dict[str, int | None] = {c: None for c in columns}
            if freq_cols and rows:
                pairs = F.explode(F.array(*[F.struct(F.lit(c).alias("column"), col(c).cast("string").alias("value"))
                                            for c in freq_cols]))
                vc = (data.select(pairs.alias("p")).select("p.column", "p.value").where(F.col("value").isNotNull())
                      .groupBy("column", "value").count().persist(StorageLevel.MEMORY_AND_DISK))
                for r in vc.groupBy("column").agg(F.sum((F.col("count") == 1).cast("int")).alias("u")).collect():
                    unique_counts[r["column"]] = int(r["u"] or 0)
                ranked = vc.withColumn("rk", F.row_number().over(
                    Window.partitionBy("column").orderBy(F.col("count").desc(), F.col("value"))))
                limit = max(s.top_values, s.max_allowed_values)
                for r in ranked.where(F.col("rk") <= limit).orderBy("column", "rk").collect():
                    top_values[r["column"]].append({"value": r["value"], "count": int(r["count"]),
                                                    "percentage": _pct(r["count"], rows)})
                vc.unpersist()
                jobs += 2

            # ---------------------------------------------------------------- job 4: patterns
            top_patterns: dict[str, list[dict[str, Any]]] = {c: [] for c in columns}
            if strings and rows:
                pairs = F.explode(F.array(*[F.struct(F.lit(c).alias("column"), structural_pattern(col(c)).alias("pattern"))
                                            for c in strings]))
                pc = (data.select(pairs.alias("p")).select("p.column", "p.pattern").where(F.col("pattern").isNotNull())
                      .groupBy("column", "pattern").count()
                      .withColumn("rk", F.row_number().over(Window.partitionBy("column").orderBy(F.col("count").desc(), F.col("pattern"))))
                      .where(F.col("rk") <= s.top_patterns).orderBy("column", "rk"))
                for r in pc.collect():
                    top_patterns[r["column"]].append({"pattern": r["pattern"], "count": int(r["count"]),
                                                      "percentage": _pct(r["count"], rows)})
                jobs += 1

            # ---------------------------------------------------------------- build column profiles
            profiles: dict[str, ColumnProfile] = {}
            for i, c in enumerate(columns):
                profiles[c] = self._column_profile(c, i, dtypes[c], stats, rows, approximate, distinct_counts[c],
                                                   unique_counts[c], top_values[c], top_patterns[c])

            # ---------------------------------------------------------------- job 5: outliers
            outlier_targets = {c: p for c, p in profiles.items() if p.mean is not None and p.stddev not in (None, 0.0)
                               and (_is_numeric(p.data_type) or (p.numeric_castable_percentage or 0) >= 100 * s.semantic_match_rate)}
            if outlier_targets and rows:
                exprs = []
                robust_targets: dict[str, tuple[float, float]] = {}
                for c, p in outlier_targets.items():
                    numeric = safe_cast(c, "double") if p.data_type == "string" else col(c).cast("double")
                    exprs.append(F.sum((F.abs(numeric - F.lit(p.mean)) > F.lit(s.outlier_stddevs * p.stddev)).cast("int")).alias(c))
                    pcts = p.percentiles or {}
                    p25, p50, p75 = pcts.get("p25"), pcts.get("p50"), pcts.get("p75")
                    if None not in (p25, p50, p75):
                        iqr = float(p75) - float(p25)
                        if iqr > 0:
                            robust_sigma = iqr / 1.349
                            robust_targets[c] = (float(p50), 3.5 * robust_sigma)
                            exprs.append(F.sum((F.abs(numeric - F.lit(float(p50))) > F.lit(3.5 * robust_sigma)).cast("int")).alias(f"{c}__robust"))
                out = data.agg(*exprs).collect()[0].asDict()
                for c in outlier_targets:
                    profiles[c].outlier_count = int(out[c] or 0)
                    if c in robust_targets:
                        profiles[c].robust_outlier_count = int(out.get(f"{c}__robust") or 0)
                jobs += 1
            data.unpersist()

            anomalies, schema_anomalies = self._anomalies(profiles, rows, columns)
            pk = [c for c, p in profiles.items() if rows and p.null_count == 0 and p.distinct_count == rows and not approximate]
            near = [{"column": c, "unique_ratio": round(p.distinct_count / p.non_null_count, 4), "null_count": p.null_count,
                     "duplicate_value_count": p.duplicate_value_count}
                    for c, p in profiles.items()
                    if c not in pk and p.non_null_count and p.distinct_count / p.non_null_count >= 0.9
                    and p.inferred_type not in ("date (stored as string)",) and p.distinct_count > s.max_allowed_values]

        report = ProfileReport(
            dataset=dataset, profiled_at=started, duration_ms=sw.elapsed_ms, mode=mode, sampled=mode == "SAMPLE",
            sample_fraction=fraction if mode == "SAMPLE" else None, approximate=approximate, row_count=rows,
            source_row_count=source_rows if mode == "SAMPLE" else rows, column_count=len(columns),
            schema=schema_to_list(df.select(*[col(c) for c in columns]).schema), columns=profiles,
            duplicate_rows=duplicate_rows, duplicate_groups=duplicate_groups, primary_key_candidates=pk,
            near_key_candidates=near, anomalies=anomalies, schema_anomalies=schema_anomalies, spark_jobs=jobs,
            notes=(["percentiles are approximate (percentile_approx)"]
                   + (["distinct counts are approximate (HyperLogLog++, rsd=0.01)"] if approximate else [])
                   + ([f"statistics computed on a {fraction:.0%} sample (seed {s.sample_seed})"] if mode == "SAMPLE" else [])))
        log_event(_logger, "profile_completed", dataset=dataset, rows=rows, columns=len(columns), mode=mode,
                  spark_jobs=jobs, duration_ms=report.duration_ms)
        return report

    # ------------------------------------------------------------------ helpers
    def _column_profile(self, c: str, i: int, dtype: str, st: dict[str, Any], rows: int, approximate: bool,
                        distinct: int, unique: int | None, top: list[dict[str, Any]],
                        patterns: list[dict[str, Any]]) -> ColumnProfile:
        s = self.settings
        non_null = int(st[f"nn{i}"] or 0)
        profile = ColumnProfile(
            name=c, data_type=dtype, inferred_type=dtype, non_null_count=non_null, null_count=rows - non_null,
            null_percentage=_pct(rows - non_null, rows), distinct_count=distinct, distinct_is_approximate=approximate,
            unique_count=unique, duplicate_value_count=max(non_null - distinct, 0),
            min=st.get(f"mn{i}"), max=st.get(f"mx{i}"), top_values=top[: s.top_values], top_patterns=patterns)
        numeric_count = int(st.get(f"nc{i}") or 0)
        if dtype == "string":
            profile.numeric_castable_percentage = _pct(numeric_count, non_null)
        numeric_like = _is_numeric(dtype) or (dtype == "string" and non_null > 0
                                              and (profile.numeric_castable_percentage or 0) >= 100 * s.semantic_match_rate)
        if f"avg{i}" in st and numeric_like and numeric_count:
            profile.mean, profile.stddev = _num(st.get(f"avg{i}")), _num(st.get(f"sd{i}"))
            pcs = st.get(f"pc{i}") or []
            profile.percentiles = {k: _num(v) for k, v in zip(("p25", "p50", "p75"), pcs) if v is not None}
            profile.negative_count = int(st.get(f"neg{i}") or 0)
            profile.min, profile.max = _num(st.get(f"nmn{i}")), _num(st.get(f"nmx{i}"))
        if dtype == "string":
            profile.min_length, profile.max_length = st.get(f"lmn{i}"), st.get(f"lmx{i}")
            profile.avg_length = _num(st.get(f"lav{i}"))
            profile.blank_count, profile.whitespace_count = int(st.get(f"bl{i}") or 0), int(st.get(f"ws{i}") or 0)
            profile.integer_castable_percentage = _pct(st.get(f"ic{i}"), non_null)
            profile.email_match_percentage = _pct(st.get(f"em{i}"), non_null)
            profile.uppercase_percentage = _pct(st.get(f"up{i}"), non_null)
            profile.date_format_matches = {fmt: _pct(st.get(f"df{i}_{j}"), non_null)
                                           for j, fmt in enumerate(CANDIDATE_DATE_FORMATS) if st.get(f"df{i}_{j}")}
            if profile.date_format_matches:
                profile.dominant_date_format = max(profile.date_format_matches, key=profile.date_format_matches.get)
            profile.date_min, profile.date_max = st.get(f"dmn{i}"), st.get(f"dmx{i}")
            threshold = 100 * s.semantic_match_rate
            if non_null == 0:
                profile.inferred_type = "empty"
            elif profile.email_match_percentage >= threshold:
                profile.inferred_type = "email"
            elif profile.dominant_date_format and profile.date_format_matches[profile.dominant_date_format] >= threshold:
                profile.inferred_type = "date (stored as string)"
            elif profile.integer_castable_percentage >= threshold:
                profile.inferred_type = "integer (stored as string)"
            elif (profile.numeric_castable_percentage or 0) >= threshold:
                profile.inferred_type = "decimal (stored as string)"
            elif distinct <= s.max_allowed_values and rows >= 2 * distinct:
                profile.inferred_type = "categorical"
            else:
                profile.inferred_type = "text"
        elif dtype in ("date", "timestamp", "timestamp_ntz"):
            profile.date_min, profile.date_max = profile.min, profile.max
        if 0 < distinct <= s.max_allowed_values and rows >= 2 * distinct and profile.inferred_type in ("categorical", dtype):
            profile.candidate_allowed_values = [t["value"] for t in top if t["value"] is not None]
        return profile

    def _anomalies(self, profiles: dict[str, ColumnProfile], rows: int,
                   columns: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        s = self.settings
        anomalies: list[dict[str, Any]] = []

        def add(column: str, kind: str, detail: str, severity: str = "WARNING") -> None:
            anomalies.append({"column": column, "type": kind, "severity": severity, "detail": detail})

        for c, p in profiles.items():
            if rows and p.null_count == rows:
                add(c, "ALL_NULL", "column contains only NULL values", "ERROR")
            elif p.null_percentage >= 100 * s.high_null_rate:
                add(c, "HIGH_NULL_RATE", f"{p.null_percentage}% NULL values")
            if p.top_values and p.distinct_count > 1 and p.top_values[0]["count"] >= s.dominant_value_rate * max(p.non_null_count, 1):
                add(c, "DOMINANT_VALUE", f"value '{p.top_values[0]['value']}' represents {p.top_values[0]['percentage']}% of rows")
            if p.outlier_count:
                add(c, "OUTLIERS", f"{p.outlier_count} value(s) beyond {s.outlier_stddevs} standard deviations "
                                   f"(mean={p.mean}, stddev={p.stddev}, min={p.min}, max={p.max})")
            if p.robust_outlier_count:
                add(c, "ROBUST_OUTLIERS", f"{p.robust_outlier_count} value(s) beyond 3.5 robust sigma "
                                          "(median±IQR/1.349 — resistant to masking by extreme values)")
            if p.negative_count and p.non_null_count and p.negative_count <= 0.1 * p.non_null_count:
                add(c, "RARE_NEGATIVE_VALUES", f"{p.negative_count} negative value(s) in a mostly positive column - "
                                               "review with the business (may be valid, e.g. refunds)", "INFO")
            if p.whitespace_count:
                add(c, "LEADING_TRAILING_WHITESPACE", f"{p.whitespace_count} value(s) with leading/trailing whitespace")
            if p.data_type == "string" and p.inferred_type.endswith("(stored as string)"):
                castable = (p.date_format_matches.get(p.dominant_date_format or "", 0) if p.inferred_type.startswith("date")
                            else p.numeric_castable_percentage or 0)
                if castable < 100:
                    add(c, "TYPE_INCONSISTENCY", f"{p.inferred_type}: {round(100 - castable, 2)}% of non-null values do not "
                                                 "conform to the inferred type")
            if len(p.date_format_matches) > 1:
                formats = {k: v for k, v in p.date_format_matches.items() if v > 0}
                add(c, "MIXED_DATE_FORMATS", f"multiple date formats detected: {formats}")
            if p.inferred_type == "categorical" and p.top_values:
                normalised: dict[str, set[str]] = {}
                for t in p.top_values:
                    normalised.setdefault(str(t["value"]).strip().upper(), set()).add(str(t["value"]))
                variants = {k: sorted(v) for k, v in normalised.items() if len(v) > 1}
                if variants:
                    add(c, "INCONSISTENT_FORMATTING", f"values differing only by case/whitespace: {variants}")
                counts = [t["count"] for t in p.top_values]
                rare = [t["value"] for t in p.top_values if t["count"] == 1]
                if rare and max(counts) >= 3:
                    add(c, "RARE_CATEGORY_VALUES", f"rare values (possible typos): {rare}")

        schema_anomalies: list[dict[str, Any]] = []
        seen: dict[str, str] = {}
        for c in columns:
            if SPECIAL_NAME_RE.search(c):
                schema_anomalies.append({"column": c, "type": "NON_STANDARD_COLUMN_NAME", "detail": "contains spaces or special characters"})
            if c.lower() in seen:
                schema_anomalies.append({"column": c, "type": "DUPLICATE_COLUMN_NAME", "detail": f"case-insensitive clash with {seen[c.lower()]}"})
            seen[c.lower()] = c
            p = profiles[c]
            if p.data_type == "string" and p.inferred_type.endswith("(stored as string)"):
                schema_anomalies.append({"column": c, "type": "TYPED_DATA_STORED_AS_STRING",
                                         "detail": f"{p.inferred_type}; consider a typed column after validation"})
        return anomalies, schema_anomalies
