"""SYNTHETIC DEMO - AWS EMR Loans pipeline with DQ Accelerator integrated.

Same minimal-diff pattern: spark-submit job keeps its own session, transforms
are untouched, engine added inline (marked «DQ INTEGRATION») before the silver
write. On EMR, ship the wheel with --py-files or bake it into the AMI/venv;
config lives on s3a:// (or a bootstrap action copies it to local disk).
"""

# ============================================================================
#  SYNTHETIC DEMO - no real EMR cluster / S3 bucket used.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_RAW          = "s3a://lending-lake/raw/loans/"
S3_BRONZE       = "s3a://lending-lake/bronze/loans/"
S3_SILVER       = "s3a://lending-lake/silver/loans/"
S3_QUARANTINE   = "s3a://lending-lake/quarantine/silver/loans/"
DQ_CONFIG_PATH  = "s3a://lending-lake/dq_accelerator/config/engine.yaml"
RULESET_REF     = "loans_silver@active"
POLICY_PATH     = "s3a://lending-lake/dq_accelerator/config/policies/default.yaml"
PIPELINE_ID     = "EMR_LOANS_PIPELINE"

# «DQ INTEGRATION» — import engine (1 line) ─────────────────────────────────
from dq_core import DataQualityEngine, PipelineContext


def main() -> None:
    spark = (
        SparkSession.builder.appName("loans-pipeline-dq")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    try:
        # ----- BRONZE: unchanged --------------------------------------------------
        loans_raw = spark.read.parquet(S3_RAW)
        loans_raw.write.mode("overwrite").parquet(S3_BRONZE)

        # ----- SILVER transforms: unchanged ----------------------------------------
        loans_transformed = (
            loans_raw
            .withColumn("product", F.upper(F.trim(F.col("product"))))
            .withColumn("status",  F.upper(F.trim(F.col("status"))))
            .withColumn("credit_score", F.col("credit_score").cast("int"))
            .filter(F.col("loan_id").isNotNull())
        )

        # «DQ INTEGRATION» — run the engine on the existing DataFrame (3 + n lines) ──
        engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)
        context = PipelineContext(
            pipeline_id=PIPELINE_ID, pipeline_run_id="EMR_STEP_001",
            platform="STANDALONE_SPARK", runtime="PYSPARK",   # EMR = standalone Spark
            dataset="loans", stage="SILVER", ruleset_version="1.0",
            integration_mode="REMEDIATE",
        )
        dq_result = engine.run(dataframe=loans_transformed, ruleset=RULESET_REF,
                               policy=POLICY_PATH, context=context)

        # «DQ INTEGRATION» — accepted rows continue; quarantined rows aside ─────────
        loans_silver = dq_result.accepted_dataframe
        dq_result.quarantine_dataframe.write.mode("overwrite").parquet(S3_QUARANTINE)

        loans_silver.write.mode("overwrite").parquet(S3_SILVER)
        # dq_result.raise_if_gate_failed()   # optional: fail the EMR step

        print(dq_result.summary_text())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
