# Databricks Integration

## What was changed in the existing pipeline

| File | Status |
|------|--------|
| `customer_pipeline_original.py` | **Unchanged** – the original pipeline as received |
| `customer_pipeline_dq_integrated.py` | **Integration** – 5 added lines, 1 changed line |

### Lines added / changed
```python
from dq_core import DataQualityEngine, PipelineContext  # +1 import

engine  = DataQualityEngine.from_config(DQ_CONFIG_PATH)  # +1 engine init
context = PipelineContext(...)                            # +3 context (run_id, platform, etc.)
dq_result = engine.run(dataframe=customers_transformed, ...)  # +1 engine call

customers_silver = dq_result.accepted_dataframe  # changed: was customers_transformed
dq_result.quarantine_dataframe.write...          # +1 quarantine write (new – not a replacement)
```

The original `customers_raw`, `customers_transformed`, and all downstream reads remain untouched.

## Running locally

```powershell
. .\scripts\env.ps1
python scripts\run_medallion_demo.py
```

## Databricks cluster requirements

| Component | Minimum | Notes |
|-----------|---------|-------|
| Databricks Runtime | 14.3 LTS | Spark 3.5.0, Python 3.11+ |
| Preferred | 18 LTS | Spark 4.1.0, Python 3.12 (matches local dev env) |
| dq-core wheel | 0.1.0 | Install via init script or `%pip install` |
| Unity Catalog | Optional | Needed for Volume-based config paths |

## Integration modes

| Mode | Behaviour |
|------|-----------|
| `OBSERVE_ONLY` | Profile + validate; pipeline output unchanged |
| `ENFORCE` | Split accepted / quarantined; no corrections |
| `REMEDIATE` | Deterministic + AI corrections; revalidation; quarantine |
