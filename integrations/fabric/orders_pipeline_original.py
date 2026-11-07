# ============================================================================
#  SYNTHETIC DEMO – Microsoft Fabric Orders Lakehouse Pipeline (BEFORE)
#  Marked SYNTHETIC: no real Fabric workspace / OneLake used.
#
#  Fabric notebooks get an attached `spark` session and tables live in the
#  Lakehouse (Delta). Typical shape: bronze_ -> silver_ -> gold_ tables.
# ============================================================================

# In Fabric these resolve to OneLake/ABFS paths or Lakehouse table names.
BRONZE_TABLE = "bronze_orders"
SILVER_TABLE = "silver_orders"
RAW_PATH     = "Files/raw/orders/"

# ----- BRONZE: land raw files into a Delta table ----------------------------
orders_raw = spark.read.format("csv").option("header", "true").load(RAW_PATH)   # noqa: F821
orders_raw.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

# ----- SILVER: basic transformations ----------------------------------------
from pyspark.sql import functions as F

orders_silver = (
    spark.table(BRONZE_TABLE)                                                  # noqa: F821
    .withColumn("currency",  F.upper(F.trim(F.col("currency"))))
    .withColumn("status",    F.upper(F.trim(F.col("status"))))
    .withColumn("qty",       F.col("qty").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .filter(F.col("order_id").isNotNull())
)
orders_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

# ----- Downstream: semantic model refresh, Power BI, etc. --------------------
display(orders_silver)                                                        # noqa: F821
