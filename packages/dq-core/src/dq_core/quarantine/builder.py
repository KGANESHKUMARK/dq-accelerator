"""Quarantine DataFrame construction (one standard schema for every dataset).

Original values are kept as JSON (``original_record``) so quarantine tables from different
datasets share a schema and can be written to a single quarantine location.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dq_core.context import ExecutionContext, IntegrationMode
from dq_core.runtime.compat import col, get_spark
from dq_core.runtime.schema import user_columns
from dq_core.validation import columns as C

STATUS_OBSERVED = "OBSERVED_ONLY"
STATUS_ENFORCED = "ENFORCE_NO_REMEDIATION_ATTEMPTED"
STATUS_CORRECTION_FAILED = "CORRECTION_FAILED_REVALIDATION"
STATUS_PENDING_APPROVAL = "PENDING_HUMAN_APPROVAL"
STATUS_BELOW_THRESHOLD = "BELOW_CONFIDENCE_THRESHOLD"
STATUS_UNRESOLVED = "UNRESOLVED_NO_SAFE_REMEDIATION"

QUARANTINE_SCHEMA = T.StructType([
    T.StructField("quarantine_id", T.StringType()),
    T.StructField("record_ref", T.StringType()),
    T.StructField("record_key", T.StringType()),
    T.StructField("pipeline_id", T.StringType()),
    T.StructField("pipeline_run_id", T.StringType()),
    T.StructField("dq_run_id", T.StringType()),
    T.StructField("platform", T.StringType()),
    T.StructField("dataset_id", T.StringType()),
    T.StructField("stage", T.StringType()),
    T.StructField("ruleset_id", T.StringType()),
    T.StructField("ruleset_version", T.StringType()),
    T.StructField("integration_mode", T.StringType()),
    T.StructField("failed_rule_ids", T.ArrayType(T.StringType())),
    T.StructField("failure_reasons", T.ArrayType(T.StringType())),
    T.StructField("violations", C.VIOLATIONS_TYPE),
    T.StructField("original_record", T.StringType()),
    T.StructField("current_record", T.StringType()),
    T.StructField("proposed_corrections", C.PROPOSALS_TYPE),
    T.StructField("applied_corrections", C.FIXES_TYPE),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("remediation_status", T.StringType()),
    T.StructField("quarantined_at", T.StringType()),
])


def _is_error(v: Column) -> Column:
    return v["severity"] == F.lit("ERROR")


def remediation_status(mode: IntegrationMode) -> Column:
    if mode == IntegrationMode.OBSERVE_ONLY:
        return F.lit(STATUS_OBSERVED)
    if mode == IntegrationMode.ENFORCE:
        return F.lit(STATUS_ENFORCED)
    failed_cols = F.transform(F.filter(F.col(C.VIOLATIONS), _is_error), lambda v: v["column"])
    fixes = F.coalesce(F.col(C.FIXES), C.empty_array(C.FIXES_TYPE))
    proposals = F.coalesce(F.col(C.AI_PROPOSALS), C.empty_array(C.PROPOSALS_TYPE))
    # Precedence: an applied AI / human-approved correction that still fails wins; then pending
    # approval; then low confidence; then a deterministic correction that did not resolve the failure.
    ai_correction_failed = F.exists(fixes, lambda f: (f["source"] != F.lit("DETERMINISTIC")) & F.array_contains(failed_cols, f["column"]))
    pending = F.exists(proposals, lambda p: (p["decision"] == F.lit("REQUIRES_APPROVAL")) & F.array_contains(failed_cols, p["column"]))
    below = F.exists(proposals, lambda p: (p["decision"] == F.lit("BELOW_THRESHOLD")) & F.array_contains(failed_cols, p["column"]))
    deterministic_failed = F.exists(fixes, lambda f: (f["source"] == F.lit("DETERMINISTIC")) & F.array_contains(failed_cols, f["column"]))
    return (F.when(ai_correction_failed, F.lit(STATUS_CORRECTION_FAILED))
             .when(pending, F.lit(STATUS_PENDING_APPROVAL))
             .when(below, F.lit(STATUS_BELOW_THRESHOLD))
             .when(deterministic_failed, F.lit(STATUS_CORRECTION_FAILED))
             .otherwise(F.lit(STATUS_UNRESOLVED)))


def build_quarantine(failed: DataFrame, ctx: ExecutionContext, mode: IntegrationMode) -> DataFrame:
    df = C.ensure_remediation_columns(failed)
    errors = F.filter(F.col(C.VIOLATIONS), _is_error)
    failed_cols = F.transform(errors, lambda v: v["column"])
    relevant = F.filter(F.col(C.AI_PROPOSALS), lambda p: F.array_contains(failed_cols, p["column"]))
    users = user_columns(df.columns)
    pipeline = ctx.pipeline
    out = df.select(
        F.concat_ws(":", F.lit(ctx.run_id), F.col(C.RECORD_ID)).alias("quarantine_id"),
        F.col(C.RECORD_ID).alias("record_ref"),
        F.col(C.RECORD_KEY).alias("record_key"),
        F.lit(pipeline.pipeline_id).alias("pipeline_id"),
        F.lit(pipeline.pipeline_run_id).alias("pipeline_run_id"),
        F.lit(ctx.run_id).alias("dq_run_id"),
        F.lit(pipeline.platform).alias("platform"),
        F.lit(pipeline.dataset).alias("dataset_id"),
        F.lit(pipeline.stage.value).alias("stage"),
        F.lit(ctx.ruleset_id).alias("ruleset_id"),
        F.lit(ctx.ruleset_version).alias("ruleset_version"),
        F.lit(mode.value).alias("integration_mode"),
        F.transform(errors, lambda v: v["rule_id"]).alias("failed_rule_ids"),
        F.transform(errors, lambda v: F.concat(v["rule_id"], F.lit(": "), v["message"], F.lit(" (value="),
                                               F.coalesce(v["value"], F.lit("NULL")), F.lit(")"))).alias("failure_reasons"),
        F.col(C.VIOLATIONS).alias("violations"),
        F.col(C.ORIGINAL).alias("original_record"),
        F.to_json(F.struct(*[col(c) for c in users]), {"ignoreNullFields": "false"}).alias("current_record"),
        F.col(C.AI_PROPOSALS).alias("proposed_corrections"),
        F.col(C.FIXES).alias("applied_corrections"),
        F.array_max(F.transform(relevant, lambda p: p["confidence"])).alias("confidence"),
        remediation_status(mode).alias("remediation_status"),
        F.lit(ctx.started_at).alias("quarantined_at"),
    )
    return out.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in QUARANTINE_SCHEMA.fields])


def empty_quarantine(spark: Any) -> DataFrame:
    return spark.createDataFrame([], QUARANTINE_SCHEMA)


def empty_quarantine_like(df: DataFrame) -> DataFrame:
    return empty_quarantine(get_spark(df))
