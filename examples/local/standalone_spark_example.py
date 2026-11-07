"""Standalone local Spark example – same engine API as the Databricks integration.

Creates and manages its OWN SparkSession (this is the standalone case; the
Databricks notebook instead uses the session Databricks provides).

Run from the repo root:
    . .\\scripts\\env.ps1
    & $VenvPython examples\\local\\standalone_spark_example.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dq_core import DataQualityEngine, PipelineContext                      # noqa: E402
from dq_core.context import IntegrationMode                               # noqa: E402
from dq_core.io.sources import CsvSource                                   # noqa: E402
from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session  # noqa: E402


def main() -> int:
    spark = create_local_spark_session(LocalSparkSettings(
        app_name="dq-standalone-example", master="local[2]", scratch_dir=str(ROOT / ".tmp")))
    try:
        # 1. existing "pipeline" DataFrame – here a CSV stand-in for a Bronze table
        df = CsvSource(str(ROOT / "sample_data" / "bronze" / "customers.csv")).read(spark)
        print(f"input rows: {df.count()}")

        # 2. engine on the EXISTING session/DataFrame
        engine = DataQualityEngine.from_config(ROOT / "config" / "engine.yaml")
        ctx = PipelineContext(
            pipeline_id="STANDALONE_CUSTOMER_PIPELINE",
            pipeline_run_id="LOCAL_DEMO_001",
            platform="LOCAL", runtime="PYSPARK",
            dataset="customers", stage="SILVER",
            integration_mode=IntegrationMode.REMEDIATE,
        )
        result = engine.run(
            dataframe=df,
            ruleset=ROOT / "config" / "rulesets" / "customers_v1.yaml",
            policy=ROOT / "config" / "policies" / "default.yaml",
            context=ctx,
        )

        # 3. continue the "pipeline" with the accepted frame; keep quarantine aside
        accepted = result.accepted_dataframe
        quarantine = result.quarantine_dataframe
        print(f"accepted={accepted.count()}  quarantined={quarantine.count()}")
        print(result.summary_text())

        # 4. persist locally (parquet) for inspection
        out = ROOT / "data" / "silver" / "customers_example"
        accepted.write.mode("overwrite").parquet(str(out))
        qout = ROOT / "data" / "quarantine" / "silver" / "customers_example"
        quarantine.write.mode("overwrite").parquet(str(qout))
        report = ROOT / "reports" / "standalone_example_report.json"
        result.write_report(report)
        print(f"\naccepted -> {out}\nquarantine -> {qout}\nreport    -> {report}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
