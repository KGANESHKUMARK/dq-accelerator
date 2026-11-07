"""Contract tests for portability adapters (integrations/contracts/adapter_contract.py).

These tests verify the adapter *contract* only – they do not claim Dataproc/Fabric/Airflow/ADF
executions. SparkDataFrameAdapter is exercised against a real local Spark DataFrame; the
orchestration-only adapters must raise NotImplementedError by design.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# integrations/contracts is outside the installed package – add it to sys.path for the test.
_CONTRACTS = Path(__file__).resolve().parents[4] / "integrations" / "contracts"
sys.path.insert(0, str(_CONTRACTS))

from adapter_contract import (  # noqa: E402
    AdfActivityAdapter,
    AirflowCliAdapter,
    DataprocAdapter,
    FabricSparkAdapter,
    PlatformAdapter,
    SparkDataFrameAdapter,
)

from dq_core.context import IntegrationMode, PipelineContext  # noqa: E402
from dq_core.metadata.models import RuleType  # noqa: E402

pytestmark = pytest.mark.spark


def _tiny_ruleset():
    from dq_core.metadata.models import RuleDefinition, RuleSet, RuleSetStatus

    rule = RuleDefinition(rule_id="T001", name="id not null", rule_type=RuleType.NOT_NULL,
                          dataset="t", column="id", stage="SILVER")
    return RuleSet(ruleset_id="t_silver", dataset="t", version="1.0", rules=(rule,),
                   stage="SILVER", status=RuleSetStatus.PUBLISHED)


@pytest.fixture(scope="module")
def published_tiny(engine):
    rs = _tiny_ruleset()
    engine.ruleset_registry.publish(rs, "contract_test", activate=True)
    return rs


def _ctx():
    return PipelineContext(pipeline_id="CONTRACT_TEST", pipeline_run_id="R1",
                           dataset="t", stage="SILVER", platform="LOCAL")


def _policy():
    from dq_core.policies.models import RemediationPolicyConfig
    return RemediationPolicyConfig()


class TestAdapterContract:
    def test_platform_adapter_is_abstract(self):
        with pytest.raises(TypeError):
            PlatformAdapter()  # type: ignore[abstract]

    def test_spark_adapter_delegates_to_engine(self, spark, engine, published_tiny):
        df = spark.createDataFrame([(1,), (2,), (None,)], "id int")
        adapter = SparkDataFrameAdapter(engine)
        result = adapter.execute(_ctx(), published_tiny, _policy(),
                                 IntegrationMode.ENFORCE, dataframe=df)
        assert result.accepted_dataframe.count() == 2
        assert result.quarantine_dataframe.count() == 1

    def test_spark_adapter_requires_dataframe(self, engine):
        adapter = SparkDataFrameAdapter(engine)
        with pytest.raises(ValueError, match="dataframe"):
            adapter.execute(_ctx(), _tiny_ruleset(), _policy(), IntegrationMode.ENFORCE)

    def test_dataproc_adapter_is_spark_contract(self, spark, engine):
        df = spark.createDataFrame([(1,), (2,)], "id int")
        adapter = DataprocAdapter(engine)
        assert adapter.platform_name == "DATAPROC"
        result = adapter.execute(_ctx(), _tiny_ruleset(), _policy(),
                                 IntegrationMode.OBSERVE_ONLY, dataframe=df)
        assert result.execution_metrics.input_records == 2

    def test_fabric_adapter_is_spark_contract(self, spark, engine):
        df = spark.createDataFrame([(1,), (2,)], "id int")
        adapter = FabricSparkAdapter(engine)
        assert adapter.platform_name == "FABRIC"
        result = adapter.execute(_ctx(), _tiny_ruleset(), _policy(),
                                 IntegrationMode.OBSERVE_ONLY, dataframe=df)
        assert result.execution_metrics.input_records == 2

    def test_describe_returns_metadata(self, engine):
        info = SparkDataFrameAdapter(engine).describe()
        assert info["platform"] == "SPARK"
        assert "SparkDataFrameAdapter" in info["adapter"]


class TestOrchestrationOnlyAdapters:
    """Airflow/ADF are orchestrators, not Spark runtimes – contract placeholders only."""

    def test_airflow_adapter_raises_not_implemented(self):
        adapter = AirflowCliAdapter()
        with pytest.raises(NotImplementedError, match="contract placeholder"):
            adapter.execute(_ctx(), _tiny_ruleset(), _policy(), IntegrationMode.ENFORCE)

    def test_adf_adapter_raises_not_implemented(self):
        adapter = AdfActivityAdapter()
        with pytest.raises(NotImplementedError, match="contract placeholder"):
            adapter.execute(_ctx(), _tiny_ruleset(), _policy(), IntegrationMode.ENFORCE)

    def test_airflow_platform_name(self):
        assert AirflowCliAdapter.platform_name == "AIRFLOW_CLI"
        assert AdfActivityAdapter.platform_name == "ADF"
