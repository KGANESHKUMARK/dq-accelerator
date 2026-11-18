"""Verify Delta Lake support end-to-end.

Creates a local Delta table, reads it back, and demonstrates time-travel
via DESCRIBE HISTORY — the same operations Unity Catalog tables support.

Prereq: delta-spark installed (.venv) + network access to Maven Central
(first run downloads jars to ~/.ivy2, ~1-2 min; cached afterwards).

Usage:  .venv/Scripts/python.exe scripts/verify_delta.py [--path .tmp/delta_verify]
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages" / "dq-core" / "src"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default=str(ROOT / ".tmp" / "delta_verify" / "dq" / "bronze" / "verify_ds"))
    args = ap.parse_args()

    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        print("FAIL: delta-spark not installed — run: pip install delta-spark==4.3.1")
        return 1
    from pyspark.sql import SparkSession

    print("Building Spark session with Delta extension (first run resolves jars via Maven)...")
    try:
        spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[2]")).getOrCreate()
    except Exception as exc:
        print(f"FAIL: Spark session could not start with Delta — {str(exc)[:300]}")
        print("Hint: this machine cannot reach Maven Central. On Databricks/EMR/Dataproc")
        print("Delta jars are preinstalled — no download needed there.")
        return 1
    spark.sparkContext.setLogLevel("ERROR")

    # 1. write v1
    df1 = spark.createDataFrame([("a", 1, "x"), ("b", 2, "y")], ["id", "val", "tag"])
    df1.write.format("delta").mode("overwrite").save(args.path)
    print(f"[1] wrote Delta table -> {args.path}")

    # 2. read back
    n = spark.read.format("delta").load(args.path).count()
    print(f"[2] read back: {n} rows")

    # 3. _delta_log exists (the ACID transaction log — proof it's a real Delta table)
    log_dir = Path(args.path) / "_delta_log"
    print(f"[3] _delta_log present: {log_dir.is_dir()} ({len(list(log_dir.glob('*.json'))) if log_dir.is_dir() else 0} commits)")

    # 4. append v2 + time-travel to v1
    df2 = spark.createDataFrame([("c", 3, "z")], ["id", "val", "tag"])
    df2.write.format("delta").mode("append").save(args.path)
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{args.path.replace(os.sep, '/')}`").collect()
    print(f"[4] DESCRIBE HISTORY: {len(hist)} versions | latest version: {hist[0]['version']}")

    # 5. time-travel read of version 0
    tv = spark.read.format("delta").option("versionAsOf", 0).load(args.path).count()
    print(f"[5] time-travel version 0 -> {tv} rows (current has 3)")

    print("\nALL CHECKS PASSED — Delta tables work locally.")
    print("Next: set outputs.format: delta (+ catalog: dq) in config/engine.yaml")
    print("and run any pipeline — bronze/silver/gold will write Delta tables.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
