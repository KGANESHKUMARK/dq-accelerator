# dq-core

Portable Apache Spark Data Quality Core Engine used by the DQ Accelerator POC.

```python
from dq_core import DataQualityEngine
from dq_core.context import PipelineContext

engine = DataQualityEngine.from_config("config/engine.yaml")
result = engine.run(
    dataframe=customers_df,                       # any existing PySpark DataFrame
    ruleset="config/rulesets/customers_v1.yaml",
    policy="config/policies/default.yaml",
    context=PipelineContext(pipeline_id="P1", pipeline_run_id="R1", dataset="customers"),
)
accepted_df = result.accepted_dataframe
quarantine_df = result.quarantine_dataframe
```

* Uses the SparkSession of the input DataFrame - never creates or stops sessions inline.
* Standard PySpark DataFrame APIs only (no RDD / JVM access / Python UDFs), ANSI-safe expressions.
* Runtime dependency: PyYAML. PySpark is provided by the platform (`pip install dq-core[spark]` locally).

See the repository `docs/` folder for architecture, rule management, remediation and policies.
