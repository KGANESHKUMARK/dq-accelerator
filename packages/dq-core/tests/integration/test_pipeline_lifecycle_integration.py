"""Integration tests: pipeline lifecycle + engine lifecycle enforcement."""

import pytest

pytestmark = pytest.mark.spark

ROOT = "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator"


def _engine(tmp_path):
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(f"{ROOT}/config/engine.yaml")
    cfg.registry.rulesets_path = str(tmp_path / "rulesets")
    cfg.registry.pipelines_path = str(tmp_path / "pipelines")
    cfg.registry.require_published_for_enforcement = True
    cfg.registry.enforce_pipeline_lifecycle = True
    return DataQualityEngine(cfg)


def _register_pipeline(engine, pipeline_id="LTEST_PIPE"):
    from dq_core.pipelines.manifest import PipelineManifest

    mf = PipelineManifest.from_dict({
        "pipeline_id": pipeline_id, "pipeline_name": "Lifecycle Test", "platform": "DATABRICKS",
        "runtime": "PYSPARK", "integration_mode": "INLINE", "dq_mode": "OBSERVE_ONLY",
        "datasets": [{"dataset_id": "customers", "format": "PARQUET", "stages": ["SILVER"]}]})
    engine.pipeline_registry.onboard(mf, "test")
    engine.pipeline_registry.record_test(pipeline_id, {}, passed=True)
    engine.pipeline_registry.activate(pipeline_id)


class TestPipelineLifecycleIntegration:
    def test_paused_pipeline_passes_data_through(self, customers_df, customers_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode, PipelineContext

        engine = _engine(tmp_path / "lc1")
        engine.ruleset_registry.publish(customers_v1, "test", activate=True)
        _register_pipeline(engine, "PAUSE_PIPE")
        engine.pipeline_registry.pause("PAUSE_PIPE")

        ctx = PipelineContext(pipeline_id="PAUSE_PIPE", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        result = engine.run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.REMEDIATE)
        assert result.skipped is True
        assert result.accepted_dataframe.count() == 38  # unchanged – data passes through

    def test_active_pipeline_runs_dq(self, customers_df, customers_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode, PipelineContext

        engine = _engine(tmp_path / "lc2")
        engine.ruleset_registry.publish(customers_v1, "test", activate=True)
        _register_pipeline(engine, "ACTIVE_PIPE")

        ctx = PipelineContext(pipeline_id="ACTIVE_PIPE", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        result = engine.run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.ENFORCE)
        assert result.skipped is False
        assert result.accepted_dataframe.count() < 38

    def test_offboarding_disables_integration(self, customers_df, customers_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode, PipelineContext

        engine = _engine(tmp_path / "lc3")
        engine.ruleset_registry.publish(customers_v1, "test", activate=True)
        _register_pipeline(engine, "OFFBOARD_PIPE")
        engine.pipeline_registry.offboard("OFFBOARD_PIPE")

        ctx = PipelineContext(pipeline_id="OFFBOARD_PIPE", pipeline_run_id="R", dataset="customers", platform="LOCAL")
        result = engine.run(customers_df, customers_v1, default_policy, ctx, mode=IntegrationMode.REMEDIATE)
        assert result.skipped is True

    def test_offboard_does_not_delete_history(self, customers_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode, PipelineContext

        engine = _engine(tmp_path / "lc4")
        engine.ruleset_registry.publish(customers_v1, "test", activate=True)
        _register_pipeline(engine, "HIST_PIPE")
        engine.pipeline_registry.offboard("HIST_PIPE")
        entry = engine.pipeline_registry.get("HIST_PIPE")
        assert entry["offboarding"]["data_deleted"] is False
        assert entry["offboarding"]["history_retained"] is True
        assert len(entry["history"]) >= 5
