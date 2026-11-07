"""SYNTHETIC DEMO - Microsoft Fabric Orders pipeline with DQ Accelerator integrated.

Same minimal-diff pattern as the Databricks sample: original transformations are
untouched; the DQ engine is added inline (marked «DQ INTEGRATION») between the
transform and the Delta write. Works in a Fabric notebook because the engine
only needs the DataFrame's SparkSession.

Requires: dq-core wheel in the Fabric environment
(Environment -> Public libraries -> Upload wheel).
"""

# ============================================================================
#  SYNTHETIC DEMO - no real Fabric workspace / credentials used.
# ============================================================================

BRONZE_TABLE    = "bronze_orders"
SILVER_TABLE    = "silver_orders"
QUARANTINE_TBL  = "quarantine_silver_orders"
RAW_PATH        = "Files/raw/orders/"
DQ_CONFIG_PATH  = "Files/dq_accelerator/config/engine.yaml"     # Lakehouse Files
RULESET_REF     = "orders_silver@active"
POLICY_PATH     = "Files/dq_accelerator/config/policies/default.yaml"
PIPELINE_ID     = "FABRIC_ORDERS_PIPELINE"

# «DQ INTEGRATION» — import engine (1 line) ─────────────────────────────────
from dq_core import DataQualityEngine, PipelineContext
from pyspark.sql import functions as F

# ----- BRONZE: unchanged ────────────────────────────────────────────────────
orders_raw = spark.read.format("csv").option("header", "true").load(RAW_PATH)   # noqa: F821
orders_raw.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

# ----- SILVER transforms: unchanged ─────────────────────────────────────────
orders_transformed = (
    spark.table(BRONZE_TABLE)                                                  # noqa: F821
    .withColumn("currency",  F.upper(F.trim(F.col("currency"))))
    .withColumn("status",    F.upper(F.trim(F.col("status"))))
    .withColumn("qty",       F.col("qty").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .filter(F.col("order_id").isNotNull())
)

# «DQ INTEGRATION» — run the engine on the existing DataFrame (3 + n lines) ──
engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)
context = PipelineContext(
    pipeline_id=PIPELINE_ID, pipeline_run_id="FABRIC_RUN_001",
    platform="FABRIC", runtime="PYSPARK",
    dataset="orders", stage="SILVER", ruleset_version="1.0",
    integration_mode="REMEDIATE",
)
dq_result = engine.run(dataframe=orders_transformed, ruleset=RULESET_REF,
                       policy=POLICY_PATH, context=context)

# «DQ INTEGRATION» — accepted rows continue to Silver; quarantined rows aside ─
orders_silver = dq_result.accepted_dataframe
dq_result.quarantine_dataframe.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TBL)

orders_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
# dq_result.raise_if_gate_failed()   # optional: fail the Fabric pipeline

print(dq_result.summary_text())
