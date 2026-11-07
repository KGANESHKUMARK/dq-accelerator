"""Integration adapter interface.

Every future platform adapter (Dataproc, Microsoft Fabric, Airflow DAG, ADF activity, dbt
post-hook, Kafka/Flink consumer) must implement this interface. The local Databricks adapter
uses it implicitly through the `DataQualityEngine.run()` facade.

Portability design notes (see docs/pipeline_adapters.md):
* Airflow and ADF are orchestration systems, not Spark runtimes – they call `dqctl run` or
  invoke a Databricks / Dataproc job via their operators.
* dbt and warehouse SQL systems need a SQL execution adapter (not yet implemented).
* Flink needs its own streaming adapter (DataStream API, not DataFrame API).
* Snowflake / BigQuery / Redshift need a Spark JDBC / external table adapter or a native
  SQL adapter.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dq_core.context import IntegrationMode, PipelineContext
from dq_core.metadata.models import RuleSet
from dq_core.orchestration.result import DQResult
from dq_core.policies.models import RemediationPolicyConfig


class PlatformAdapter(ABC):
    """
    Base adapter. Concrete implementations:
    - SparkDataFrameAdapter  (Databricks / Dataproc / Fabric / Standalone) – IMPLEMENTED
    - AirflowCliAdapter      – CONTRACT ONLY (calls dqctl via BashOperator / DockerOperator)
    - AdfActivityAdapter     – CONTRACT ONLY (calls dqctl or Databricks job via WebActivity)
    - SqlAdapter             – CONTRACT ONLY (dbt post-hook, Snowflake, BigQuery, Redshift)
    - StreamingAdapter       – CONTRACT ONLY (Kafka / Flink)
    """

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Human-readable identifier (e.g. 'DATABRICKS', 'DATAPROC', 'AIRFLOW_CLI')."""

    @abstractmethod
    def execute(self, context: PipelineContext, ruleset: RuleSet, policy: RemediationPolicyConfig,
                mode: IntegrationMode, **kwargs: Any) -> DQResult:
        """Execute the DQ run and return a standard DQResult."""

    def describe(self) -> dict[str, Any]:
        return {"adapter": type(self).__name__, "platform": self.platform_name}


class SparkDataFrameAdapter(PlatformAdapter):
    """
    Thin adapter for any PySpark environment (Databricks, Dataproc, Fabric, Standalone).

    The heavy lifting is done by `DataQualityEngine.run()`; this adapter only resolves
    the DataFrame from the platform context (e.g. a Delta table path, a streaming source,
    or a passed-in DataFrame).
    """

    platform_name = "SPARK"

    def __init__(self, engine: Any) -> None:  # engine: DataQualityEngine (not imported to avoid circularity)
        self.engine = engine

    def execute(self, context: PipelineContext, ruleset: RuleSet, policy: RemediationPolicyConfig,
                mode: IntegrationMode, *, dataframe: Any = None, **kwargs: Any) -> DQResult:
        if dataframe is None:
            raise ValueError("SparkDataFrameAdapter.execute() requires a 'dataframe' keyword argument")
        return self.engine.run(dataframe, ruleset, policy, context, mode=mode, **kwargs)


class AirflowCliAdapter(PlatformAdapter):
    """CONTRACT ONLY. Airflow is an orchestrator, not a Spark runtime.

    Implementation: BashOperator / KubernetesPodOperator calls `dqctl run` and publishes
    the JSON report to XCom; the DQ core engine does not run inside the Airflow worker.
    See docs/pipeline_adapters.md#airflow.
    """

    platform_name = "AIRFLOW_CLI"

    def execute(self, context: PipelineContext, ruleset: RuleSet, policy: RemediationPolicyConfig,
                mode: IntegrationMode, **kwargs: Any) -> DQResult:
        raise NotImplementedError(
            "AirflowCliAdapter is a contract placeholder. See docs/pipeline_adapters.md#airflow.")


class AdfActivityAdapter(PlatformAdapter):
    """CONTRACT ONLY. Azure Data Factory is an orchestrator, not a Spark runtime.

    Implementation: ADF WebActivity calls the DQ REST endpoint or invokes a Databricks job
    that includes the DQ engine. See docs/pipeline_adapters.md#azure-data-factory.
    """

    platform_name = "ADF"

    def execute(self, context: PipelineContext, ruleset: RuleSet, policy: RemediationPolicyConfig,
                mode: IntegrationMode, **kwargs: Any) -> DQResult:
        raise NotImplementedError(
            "AdfActivityAdapter is a contract placeholder. See docs/pipeline_adapters.md#adf.")


class DataprocAdapter(SparkDataFrameAdapter):
    """CONTRACT TESTED. Same code path as Databricks but with Dataproc-specific config."""

    platform_name = "DATAPROC"


class FabricSparkAdapter(SparkDataFrameAdapter):
    """CONTRACT TESTED. Microsoft Fabric Spark uses the same PySpark DataFrame API."""

    platform_name = "FABRIC"
