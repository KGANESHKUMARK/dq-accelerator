"""Integration test: Spark session starts, parquet round-trip works."""

import pytest

pytestmark = pytest.mark.spark


def test_spark_session_starts(spark):
    assert spark is not None
    assert spark.version.startswith("4.")


def test_spark_ansi_enabled(spark):
    """Databricks Runtime 18 LTS enables ANSI by default; local harness must match."""
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


def test_local_parquet_roundtrip(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")
    out = tmp_path / "parquet_test"
    df.write.mode("overwrite").parquet(str(out))
    read_back = spark.read.parquet(str(out))
    assert read_back.count() == 2


def test_dataframe_creation_and_count(spark):
    df = spark.createDataFrame([(i,) for i in range(100)], "id int")
    assert df.count() == 100
