"""Spark API compatibility helpers.

Every expression used by the engine is ANSI-safe (Spark 4.x enables ``spark.sql.ansi.enabled``
by default, where an invalid ``CAST`` raises instead of returning NULL) and uses only DataFrame
APIs that also work over Spark Connect (Databricks shared/serverless compute): no RDDs, no
SparkContext, no JVM access, no Python UDFs.

Minimum supported Spark version: 3.5 (Databricks Runtime 14.3 LTS and later).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dq_core.runtime.schema import quote_identifier, validate_cast_type


def get_spark(df: DataFrame) -> Any:
    """Return the SparkSession that owns ``df`` (never creates a new session)."""
    session = getattr(df, "sparkSession", None)
    if session is None:  # pragma: no cover - PySpark < 3.3
        session = df.sql_ctx.sparkSession
    return session


def is_spark_connect(df: DataFrame) -> bool:
    return type(df).__module__.startswith("pyspark.sql.connect")


def spark_version_tuple(spark: Any) -> tuple[int, int]:
    major, minor, *_ = (spark.version.split(".") + ["0", "0"])
    return int(major), int("".join(ch for ch in minor if ch.isdigit()) or 0)


def safe_cast(column: str, type_name: str) -> Column:
    """``try_cast`` of a named column (NULL instead of an error for invalid values)."""
    type_sql = validate_cast_type(type_name)
    return F.expr(f"try_cast({quote_identifier(column)} AS {type_sql})")


def safe_to_timestamp(column: Column, fmt: str) -> Column:
    """Parse ``column`` with a Spark datetime pattern; NULL when it does not parse."""
    return F.try_to_timestamp(column, F.lit(fmt))


def safe_to_date(column: Column, fmt: str) -> Column:
    return F.to_date(safe_to_timestamp(column, fmt))


def first_or_null(array_column: Column) -> Column:
    """First element of an array, NULL for empty arrays (no ANSI out-of-bounds error)."""
    return F.when(F.size(array_column) > 0, array_column.getItem(0))


def compact(array_column: Column) -> Column:
    """Remove NULL elements from an array (portable alternative to array_compact)."""
    return F.filter(array_column, lambda item: item.isNotNull())


def as_string(column: str) -> Column:
    return F.col(quote_identifier(column)).cast("string")


def col(column: str) -> Column:
    return F.col(quote_identifier(column))
