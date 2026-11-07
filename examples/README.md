# Examples

Runnable demonstrations of the DQ Accelerator engine API.

| Folder | What it shows |
|---|---|
| `local/standalone_spark_example.py` | Standalone Spark app that creates its own `SparkSession`, loads CSV via `CsvSource`, runs `engine.run(..., REMEDIATE)`, writes accepted/quarantine parquet + JSON report. **Verified locally.** |
| `../integrations/databricks/` | Databricks notebook-style scripts: `customer_pipeline_original.py` (before) and `customer_pipeline_dq_integrated.py` (after, ~6 added lines). **Not executed on a real workspace.** |

Run the local example:

```powershell
. .\scripts\env.ps1
& $VenvPython examples\local\standalone_spark_example.py
```
