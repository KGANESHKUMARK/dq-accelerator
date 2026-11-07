"""Names and types of the technical ``_dq_*`` columns the engine adds while processing."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from dq_core.runtime.schema import quote_identifier, user_columns

RECORD_ID = "_dq_record_id"
RECORD_KEY = "_dq_record_key"
ORIGINAL = "_dq_original"
VIOLATIONS = "_dq_violations"
ERROR_COUNT = "_dq_error_count"
WARNING_COUNT = "_dq_warning_count"
INFO_COUNT = "_dq_info_count"
STATUS = "_dq_status"
FIXES = "_dq_fixes"
AI_PROPOSALS = "_dq_ai_proposals"

ANNOTATION_COLUMNS = (VIOLATIONS, ERROR_COUNT, WARNING_COUNT, INFO_COUNT, STATUS)

VIOLATION_TYPE = T.StructType([
    T.StructField("rule_id", T.StringType()),
    T.StructField("rule_name", T.StringType()),
    T.StructField("rule_type", T.StringType()),
    T.StructField("column", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("message", T.StringType()),
    T.StructField("value", T.StringType()),
])
VIOLATIONS_TYPE = T.ArrayType(VIOLATION_TYPE)

FIX_TYPE = T.StructType([
    T.StructField("rule_id", T.StringType()),
    T.StructField("column", T.StringType()),
    T.StructField("source", T.StringType()),          # DETERMINISTIC | AI | HUMAN_APPROVED_AI
    T.StructField("action", T.StringType()),
    T.StructField("original_value", T.StringType()),
    T.StructField("new_value", T.StringType()),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("provider", T.StringType()),
    T.StructField("model_version", T.StringType()),
    T.StructField("decision", T.StringType()),
    T.StructField("reason", T.StringType()),
    T.StructField("simulated", T.BooleanType()),
])
FIXES_TYPE = T.ArrayType(FIX_TYPE)

PROPOSAL_TYPE = T.StructType([
    T.StructField("rule_order", T.IntegerType()),
    T.StructField("rule_id", T.StringType()),
    T.StructField("column", T.StringType()),
    T.StructField("original_value", T.StringType()),
    T.StructField("proposed_value", T.StringType()),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("decision", T.StringType()),
    T.StructField("decision_reason", T.StringType()),
    T.StructField("reason", T.StringType()),
    T.StructField("evidence", T.StringType()),
    T.StructField("provider", T.StringType()),
    T.StructField("model_version", T.StringType()),
    T.StructField("simulated", T.BooleanType()),
])
PROPOSALS_TYPE = T.ArrayType(PROPOSAL_TYPE)


def empty_array(array_type: T.ArrayType) -> Column:
    return F.array().cast(array_type)


def add_record_columns(df: DataFrame, record_key: tuple[str, ...] | list[str] = ()) -> DataFrame:
    """Add deterministic record identity columns if they are not present yet.

    * ``_dq_original``  - JSON of the original user columns (nulls preserved)
    * ``_dq_record_id`` - first 16 hex chars of SHA-256(original JSON) + '-' + occurrence number
      (identical duplicate rows get distinct, deterministic ids)
    * ``_dq_record_key`` - business key value(s) for human-readable traceability
    """
    if RECORD_ID in df.columns:
        return df
    cols = user_columns(df.columns)
    original = F.to_json(F.struct(*[F.col(quote_identifier(c)) for c in cols]), {"ignoreNullFields": "false"})
    out = df.withColumn(ORIGINAL, original)
    digest = F.substring(F.sha2(F.col(ORIGINAL), 256), 1, 16)
    out = out.withColumn("_dq_digest", digest)
    occurrence = F.row_number().over(Window.partitionBy("_dq_digest").orderBy("_dq_digest"))
    out = out.withColumn(RECORD_ID, F.concat_ws("-", F.col("_dq_digest"), occurrence.cast("string"))).drop("_dq_digest")
    keys = [k for k in record_key if k in cols]
    if keys:
        key_expr = F.concat_ws("|", *[F.coalesce(F.col(quote_identifier(k)).cast("string"), F.lit("<null>")) for k in keys])
    else:
        key_expr = F.lit(None).cast("string")
    return out.withColumn(RECORD_KEY, key_expr)


def strip_annotations(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in ANNOTATION_COLUMNS if c in df.columns])


def ensure_remediation_columns(df: DataFrame) -> DataFrame:
    if FIXES not in df.columns:
        df = df.withColumn(FIXES, empty_array(FIXES_TYPE))
    if AI_PROPOSALS not in df.columns:
        df = df.withColumn(AI_PROPOSALS, empty_array(PROPOSALS_TYPE))
    return df
