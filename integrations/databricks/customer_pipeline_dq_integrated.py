"""SYNTHETIC DEMO – Databricks Customer Pipeline with DQ Accelerator integrated.

This is the AFTER state: the original pipeline is preserved exactly; the DQ engine is
added in the fewest possible lines (marked with «DQ INTEGRATION»).  No existing
transformation is changed or removed.

Requires: dq-core installed in the cluster (or via init script / library):
  %pip install /Volumes/main/dq_accelerator/libs/dq_core-0.1.0-py3-none-any.whl
"""

# ============================================================================
#  SYNTHETIC DEMO - no real Databricks workspace / credentials used.
#  Local execution is handled by scripts/run_medallion_demo.py using
#  the local Spark harness; this file shows the Databricks notebook pattern.
# ============================================================================

# In a real notebook these would come from widgets, job parameters or
# environment variables exposed by the Databricks Secrets API.
SOURCE_PATH     = "dbfs:/mnt/raw/customers/"
BRONZE_PATH     = "dbfs:/mnt/bronze/customers/"
SILVER_PATH     = "dbfs:/mnt/silver/customers/"
QUARANTINE_PATH = "dbfs:/mnt/quarantine/silver/customers/"
DQ_CONFIG_PATH  = "/Volumes/main/dq_accelerator/config/engine.databricks.yaml"
RULESET_REF     = "customers_silver@active"      # resolved by the registry
POLICY_PATH     = "/Volumes/main/dq_accelerator/config/policies/default.yaml"
PIPELINE_ID     = "DATABRICKS_CUSTOMER_PIPELINE"
PIPELINE_RUN_ID = dbutils.notebook.entry_point.getCurrentBindings().get("run_id", "MANUAL_001")  # noqa: F821

# «DQ INTEGRATION» — import engine (1 line) ─────────────────────────────────
from dq_core import DataQualityEngine, PipelineContext

# ----- BRONZE: read raw data  (unchanged from original) ─────────────────────
customers_raw = spark.read.parquet(SOURCE_PATH)   # noqa: F821 – spark is provided by Databricks
customers_raw.write.mode("overwrite").parquet(BRONZE_PATH)

# ----- SILVER: existing transformations (unchanged from original) ─────────────
from pyspark.sql import functions as F

customers_transformed = (
    customers_raw
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email",         F.trim(F.lower(F.col("email"))))
    .withColumn("country",       F.trim(F.upper(F.col("country"))))
    .filter(F.col("customer_id").isNotNull())
)

# «DQ INTEGRATION» — load engine and run  (3 lines) ──────────────────────────
engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)
context = PipelineContext(
    pipeline_id=PIPELINE_ID,
    pipeline_run_id=PIPELINE_RUN_ID,
    platform="DATABRICKS",
    runtime="PYSPARK",
    dataset="customers",
    stage="SILVER",
    ruleset_version="1.0",
    integration_mode="REMEDIATE",
)
dq_result = engine.run(
    dataframe=customers_transformed,   # ← the EXISTING DataFrame, unchanged
    ruleset=RULESET_REF,
    policy=POLICY_PATH,
    context=context,
)

# «DQ INTEGRATION» — replace original output with the DQ-accepted frame ──────
customers_silver = dq_result.accepted_dataframe   # 1 line change

# «DQ INTEGRATION» — write quarantine (new, alongside existing writes) ─────
dq_result.quarantine_dataframe.write.mode("overwrite").parquet(QUARANTINE_PATH)

# ----- Write Silver (original path, now DQ-validated) ──────────────────────
customers_silver.write.mode("overwrite").parquet(SILVER_PATH)

# «DQ INTEGRATION» — optional: raise if quality gate fails ───────────────────
# dq_result.raise_if_gate_failed()

# ----- Downstream processing (unchanged from original) ──────────────────────
# aggregations, ML feature engineering, analytics exports, etc.
display(customers_silver)                          # noqa: F821 – display is a Databricks built-in

print(dq_result.summary_text())
