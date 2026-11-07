"""Integration tests: Bronze → Silver → Gold medallion pipeline."""

import pytest

pytestmark = pytest.mark.spark

ROOT = "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator"


def _engine():
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(f"{ROOT}/config/engine.yaml")
    cfg.registry.rulesets_path = None
    cfg.registry.pipelines_path = None
    cfg.registry.require_published_for_enforcement = False
    cfg.registry.enforce_pipeline_lifecycle = False
    return DataQualityEngine(cfg)


class TestMedallionFlow:
    def test_medallion_bronze_silver_gold(self, spark, customers_df, transactions_df,
                                          customers_v1, transactions_v1, transactions_gold_v1,
                                          default_policy, tmp_path):
        from dq_core.context import IntegrationMode
        from dq_core.io.sinks import StoragePaths, StorageSink
        from dq_core.orchestration.medallion import MedallionRunner

        engine = _engine()
        paths = StoragePaths(str(tmp_path / "data"))
        runner = MedallionRunner(engine, paths)
        result = runner.run(
            customers_df, "customers",
            pipeline_id="TEST_MED", pipeline_run_id="R001",
            silver_ruleset=customers_v1, policy=default_policy,
            silver_mode=IntegrationMode.REMEDIATE, profile=False,
            write_bronze=True, write_silver=True, write_gold=False,
            write_quarantine=True, write_reports=False)
        assert result.bronze_rows == 38
        assert result.silver_result.accepted_dataframe.count() >= 20
        assert result.silver_result.quarantine_dataframe.count() >= 1
        assert "bronze" in result.written_paths
        assert "silver_accepted" in result.written_paths

    def test_gold_level_checks_referential_integrity(self, spark, customers_df, transactions_df,
                                                      customers_v1, transactions_v1,
                                                      transactions_gold_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode
        from dq_core.engine import DataQualityEngine
        from dq_core.validation.engine import ValidationEngine

        engine = _engine()
        # First run Silver on customers to get the accepted set
        from dq_core.context import PipelineContext
        ctx = PipelineContext(pipeline_id="TEST", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        silver_cust = engine.run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.REMEDIATE)

        # Now run Gold on transactions with the Silver-accepted customers as reference
        txn_ctx = PipelineContext(pipeline_id="TEST", pipeline_run_id="R", dataset="transactions", platform="LOCAL")
        gold_result = engine.run(transactions_df, transactions_gold_v1, default_policy, txn_ctx,
                                 mode=IntegrationMode.ENFORCE,
                                 reference_data={"customers": silver_cust.accepted_dataframe})
        # TXG_001 (referential integrity) should flag transactions referencing unknown / quarantined customers
        ri_result = gold_result.validation_results.result("TXG_001")
        assert ri_result.failed_records >= 1

    def test_execution_metrics_generated(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode, PipelineContext

        ctx = PipelineContext(pipeline_id="METRICS_TEST", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        result = _engine().run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.REMEDIATE)
        m = result.execution_metrics
        assert m.engine_version
        assert m.spark_version
        assert m.started_at
        assert m.finished_at
        assert m.duration_ms > 0
        assert m.input_records == 38

    def test_json_report_serialises(self, customers_df, customers_v1, default_policy, tmp_path):
        import json
        from dq_core.context import IntegrationMode, PipelineContext

        ctx = PipelineContext(pipeline_id="JSON_TEST", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        result = _engine().run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.ENFORCE)
        report_path = tmp_path / "report.json"
        result.write_report(report_path)
        data = json.loads(report_path.read_text())
        assert data["report_type"] == "DQ_EXECUTION_REPORT"
        assert "execution_metrics" in data
        assert "validation_results" in data
