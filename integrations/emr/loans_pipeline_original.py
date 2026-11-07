# ============================================================================
#  SYNTHETIC DEMO – AWS EMR Loans Pipeline (BEFORE)
#  Marked SYNTHETIC: no real EMR cluster / S3 bucket used.
#
#  EMR runs plain spark-submit PySpark jobs — no notebook `spark` is injected,
#  so the script owns its session. Data lives on s3a:// (parquet) with schema
#  in the Glue metastore.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_RAW       = "s3a://lending-lake/raw/loans/"
S3_BRONZE    = "s3a://lending-lake/bronze/loans/"
S3_SILVER    = "s3a://lending-lake/silver/loans/"


def main() -> None:
    spark = (
        SparkSession.builder.appName("loans-pipeline")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    try:
        # ----- BRONZE ------------------------------------------------------
        loans_raw = spark.read.parquet(S3_RAW)
        loans_raw.write.mode("overwrite").parquet(S3_BRONZE)

        # ----- SILVER: basic transformations --------------------------------
        loans_silver = (
            loans_raw
            .withColumn("product", F.upper(F.trim(F.col("product"))))
            .withColumn("status",  F.upper(F.trim(F.col("status"))))
            .withColumn("credit_score", F.col("credit_score").cast("int"))
            .filter(F.col("loan_id").isNotNull())
        )
        loans_silver.write.mode("overwrite").parquet(S3_SILVER)

        # ----- Downstream: Glue catalog register, risk marts, etc. ----------
        loans_silver.show(20)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
