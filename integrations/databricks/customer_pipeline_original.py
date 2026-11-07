# ============================================================================
#  SYNTHETIC DEMO – Original Databricks Customer Pipeline
#  Marked SYNTHETIC: no real customers, no real Databricks workspace.
#
#  This is the BEFORE state: a typical Databricks notebook that reads a
#  Parquet source, does Bronze → Silver transformations, and writes output.
#  It has NO data quality logic.
# ============================================================================

# In a real Databricks notebook these would be widgets / environment variables.
SOURCE_PATH  = "dbfs:/mnt/raw/customers/"
BRONZE_PATH  = "dbfs:/mnt/bronze/customers/"
SILVER_PATH  = "dbfs:/mnt/silver/customers/"

# ----- BRONZE: read raw data ------------------------------------------------
customers_raw = spark.read.parquet(SOURCE_PATH)
customers_raw.write.mode("overwrite").parquet(BRONZE_PATH)

# ----- SILVER: basic transformations ----------------------------------------
from pyspark.sql import functions as F

customers_silver = (
    customers_raw
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email",         F.trim(F.lower(F.col("email"))))
    .withColumn("country",       F.trim(F.upper(F.col("country"))))
    .filter(F.col("customer_id").isNotNull())
)
customers_silver.write.mode("overwrite").parquet(SILVER_PATH)

# ----- Downstream processing ------------------------------------------------
# aggregations, ML feature engineering, analytics exports, etc.
display(customers_silver)
