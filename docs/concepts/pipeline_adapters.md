# Pipeline Adapters & Portability

Design goal: one portable Apache Spark core (`dq-core`) usable from Databricks
today and from other Spark runtimes / orchestrators / non-Spark systems tomorrow —
without rewriting the engine.

## 1. Integration contract

`integrations/contracts/adapter_contract.py` defines `PlatformAdapter` (ABC):

```python
class PlatformAdapter(ABC):
    platform_name: str
    def execute(self, context: PipelineContext, ruleset: RuleSet,
                policy: RemediationPolicyConfig, mode: IntegrationMode,
                **kwargs) -> DQResult: ...
```

Every adapter returns the **same `DQResult`** contract, so downstream consumers
(accepted/quarantine frames, reports, metrics) are platform-independent.

## 2. Support matrix — actual vs. designed

| Target | Kind | Status in POC | How it integrates |
|---|---|---|---|
| **Standalone Apache Spark** | Spark runtime | ✅ **Fully implemented + tested** | `create_local_spark_session` + `engine.run(df,…)` |
| **Generic PySpark DataFrame** | Spark runtime | ✅ **Fully implemented + tested** | engine uses `df.sparkSession`; any compatible DF works |
| **Databricks inline PySpark** | Spark runtime | ✅ **Implemented; contract-tested; not run on a workspace** | notebook/job calls `engine.run(existing_df,…)` — see `integrations/databricks/` |
| **CSV / Parquet sources** | I/O | ✅ Implemented | `dq_core.io.sources` |
| **Local storage sink** | I/O | ✅ Implemented | `dq_core.io.sinks` (parquet/json/csv) |
| **Google Dataproc** | Spark runtime | 🔶 Contract-tested (`DataprocAdapter`) | same PySpark API; needs cluster config + paths |
| **Microsoft Fabric Spark** | Spark runtime | 🔶 Contract-tested (`FabricSparkAdapter`) | same PySpark API; Lakehouse paths instead of dbfs |
| **Airflow** | Orchestrator (NOT a Spark runtime) | 📐 Contract only (`AirflowCliAdapter` → `NotImplementedError`) | `BashOperator`/`KubernetesPodOperator` runs `dqctl run …` on a Spark-capable worker/image; report via XCom |
| **Azure Data Factory** | Orchestrator | 📐 Contract only (`AdfActivityAdapter`) | `WebActivity`/Databricks-job activity invokes the engine or `dqctl` |
| **Fabric Data Factory / Dagster / Prefect** | Orchestrator | 📐 Same CLI/REST pattern as ADF | call `dqctl` or a thin job wrapper |
| **REST / CLI** | Interface | ✅ `dqctl` CLI implemented | any orchestrator can shell out |
| **dbt / SQL warehouses (Snowflake, BigQuery, Redshift)** | Non-Spark | 📐 Extension point (`SqlAdapter` TODO) | needs a SQL-execution adapter translating rules → SQL; engine core unchanged |
| **Kafka / Flink** | Streaming | 📐 Extension point (`StreamingAdapter` TODO) | Flink needs DataStream API + its own runtime integration — PySpark DataFrame API does not apply |
| **Informatica / Talend** | ETL suites | 📐 Via file/CLI contract | read/write via supported formats + `dqctl` |

Legend: ✅ implemented & tested · 🔶 contract-tested · 📐 designed contract only.

## 3. Adapter implementations in repo

- `SparkDataFrameAdapter` — wraps `DataQualityEngine.run()`; requires a
  `dataframe=` kwarg; used directly for standalone/Databricks and subclassed by
  `DataprocAdapter` (`platform_name="DATAPROC"`) and `FabricSparkAdapter`
  (`platform_name="FABRIC"`).
- `AirflowCliAdapter`, `AdfActivityAdapter` — concrete placeholders that raise
  `NotImplementedError` with pointers to this doc; they exist so the contract
  surface is stable while real integrations are built.

Contract tests: `packages/dq-core/tests/integration/test_portability_contracts.py`
(9 tests) verify the ABC, delegation, required-DataFrame error, Dataproc/Fabric
names + OBSERVE_ONLY execution, `describe()`, and the orchestrator placeholders.

## 4. What portability buys (and what it doesn't)

**Buys:** rule sets, policies, profiling, validation semantics, remediation
rules, quarantine schema, reports and the `DQResult` contract are identical on
every Spark runtime — onboard once, run anywhere PySpark runs.

**Doesn't:** orchestration systems (Airflow/ADF) never execute PySpark
themselves — they *invoke* a Spark job/CLI. SQL warehouses and Flink need
separate execution adapters (rule→SQL translation, DataStream API). Those are
designed extension points, deliberately not faked in this POC.

## 5. Adding a new adapter (guide)

1. Subclass `PlatformAdapter`; implement `platform_name` + `execute()`.
2. For any PySpark runtime: delegate to `SparkDataFrameAdapter`/`engine.run` —
   only platform context (paths, session config) differs.
3. For orchestrators: implement a thin caller that shells `dqctl run` (or your
   REST wrapper) and parses the JSON report into `DQResult`-equivalent metadata.
4. Contract-test: platform name, delegation, error paths — mirror
   `test_portability_contracts.py`.
