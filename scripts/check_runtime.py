"""Verify the local runtime: Python, Java, Spark session start, ANSI mode, local Parquet write/read.

Usage:  .venv\\Scripts\\python.exe scripts\\check_runtime.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DQ_SCRATCH_DIR", str(ROOT / ".tmp"))

from dq_core.runtime.inspection import inspect_runtime  # noqa: E402
from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session  # noqa: E402


def main() -> int:
    spark = create_local_spark_session(LocalSparkSettings(app_name="dq-runtime-check", master="local[2]"))
    info = inspect_runtime(spark).to_dict()
    out_dir = ROOT / ".tmp" / "runtime_check" / "parquet"
    shutil.rmtree(out_dir.parent, ignore_errors=True)
    df = spark.createDataFrame([(1, "alpha"), (2, "beta")], "id int, label string")
    df.write.mode("overwrite").parquet(str(out_dir))
    read_back = spark.read.parquet(str(out_dir)).count()
    info["local_parquet_roundtrip_rows"] = read_back
    print(json.dumps(info, indent=2))
    spark.stop()
    return 0 if read_back == 2 else 1


if __name__ == "__main__":
    sys.exit(main())
