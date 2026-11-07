"""Integration tests: all three engine modes with the banking customer dataset."""

import pytest

pytestmark = pytest.mark.spark

ROOT = "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator"


def _engine(tmp_path=None):
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(f"{ROOT}/config/engine.yaml")
    if tmp_path:
        cfg.registry.rulesets_path = str(tmp_path / "rulesets")
        cfg.registry.pipelines_path = str(tmp_path / "pipelines")
    else:
        cfg.registry.rulesets_path = None
        cfg.registry.pipelines_path = None
        cfg.registry.require_published_for_enforcement = False
        cfg.registry.enforce_pipeline_lifecycle = False
    return DataQualityEngine(cfg)


def _ctx(mode=None):
    from dq_core.context import PipelineContext
    return PipelineContext(pipeline_id="TEST_P", pipeline_run_id="R001",
                           dataset="customers", platform="LOCAL",
                           integration_mode=mode)


class TestObserveOnly:
    def test_observe_only_does_not_modify_output(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        engine = _engine()
        result = engine.run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.OBSERVE_ONLY)
        # In OBSERVE_ONLY the original df is returned as-is
        assert result.accepted_dataframe.count() == 38
        assert result.quarantine_dataframe.count() == 22    # all failures still quarantined for visibility
        assert result.execution_metrics.deterministic_corrections == 0
        assert result.execution_metrics.ai_corrections_applied == 0
        assert result.execution_metrics.integration_mode == "OBSERVE_ONLY"

    def test_observe_only_produces_validation_report(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.OBSERVE_ONLY)
        assert result.validation_results is not None
        assert result.validation_results.failed_records > 0


class TestEnforce:
    def test_enforce_splits_accepted_and_quarantine(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.ENFORCE)
        assert result.accepted_dataframe.count() == 16
        assert result.quarantine_dataframe.count() == 22
        assert result.execution_metrics.deterministic_corrections == 0

    def test_enforce_quarantine_status(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode
        from dq_core.quarantine import STATUS_ENFORCED

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.ENFORCE)
        statuses = {r["remediation_status"] for r in result.quarantine_dataframe.select("remediation_status").collect()}
        assert STATUS_ENFORCED in statuses


class TestRemediate:
    def test_remediate_produces_more_accepted_than_enforce(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        engine = _engine()
        enforce = engine.run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.ENFORCE)
        remediate = engine.run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert remediate.accepted_dataframe.count() > enforce.accepted_dataframe.count()

    def test_bronze_remains_unchanged(self, customers_df, customers_v1, default_policy):
        """Bronze DataFrame (input) must have same count before and after REMEDIATE."""
        from dq_core.context import IntegrationMode

        count_before = customers_df.count()
        _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert customers_df.count() == count_before

    def test_remediate_deterministic_corrections_applied(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert result.execution_metrics.deterministic_corrections >= 10

    def test_remediate_ai_corrections_applied(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert result.execution_metrics.ai_corrections_applied >= 3

    def test_remediate_produces_duplicate_removed_count(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert result.execution_metrics.removed_duplicates == 1

    def test_quality_gate_passes_after_remediation(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert result.quality_gate.passed

    def test_quarantine_status_breakdown(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        breakdown = result.execution_metrics.quarantine_status_breakdown
        assert "PENDING_HUMAN_APPROVAL" in breakdown       # C0031 (email), C0032 (name)
        assert "BELOW_CONFIDENCE_THRESHOLD" in breakdown   # C0030 (0.62), C0035 (0.8999)
        assert "UNRESOLVED_NO_SAFE_REMEDIATION" in breakdown

    def test_post_validation_works(self, customers_df, customers_v1, default_policy):
        from dq_core.context import IntegrationMode

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        assert result.final_validation_results is not None
        # Accepted records must pass final validation
        assert result.final_validation_results.passed_records == result.accepted_dataframe.count()

    def test_accepted_written_to_silver(self, spark, customers_df, customers_v1, default_policy, tmp_path):
        from dq_core.context import IntegrationMode
        from dq_core.io.sinks import StoragePaths, StorageSink

        result = _engine().run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        sink = StorageSink()
        silver_path = str(tmp_path / "silver" / "customers")
        sink.write(result.accepted_dataframe, silver_path)
        read_back = spark.read.parquet(silver_path)
        assert read_back.count() == result.accepted_dataframe.count()

    def test_v1_1_comparison_with_v1_0(self, customers_df, customers_v1, customers_v1_1, default_policy):
        """v1.1 should quarantine more records (tighter age max)."""
        from dq_core.context import IntegrationMode

        engine = _engine()
        r10 = engine.run(customers_df, customers_v1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        r11 = engine.run(customers_df, customers_v1_1, default_policy, _ctx(), mode=IntegrationMode.REMEDIATE)
        # C0026 (115 years old) passes v1.0 but fails v1.1
        assert r11.quarantine_dataframe.count() > r10.quarantine_dataframe.count()


class TestVersionManagement:
    def test_published_versions_are_immutable(self, tmp_path, customers_v1):
        from dq_core.metadata.models import RuleSet
        from dq_core.rules.registry import RuleSetRegistry
        from dq_core.runtime.errors import ImmutableVersionError

        reg = RuleSetRegistry(tmp_path / "reg")
        reg.publish(customers_v1, "user1")
        # Attempt to publish different content under the same version
        modified = RuleSet(ruleset_id=customers_v1.ruleset_id, dataset=customers_v1.dataset,
                           version="1.0", rules=customers_v1.rules[1:], parent_version=None,
                           stage=customers_v1.stage, status=customers_v1.status,
                           lookups=customers_v1.lookups)
        assert modified.checksum() != customers_v1.checksum()
        with pytest.raises(ImmutableVersionError):
            reg.publish(modified, "user2")

    def test_ruleset_diff_shows_changes(self, customers_v1, customers_v1_1):
        from dq_core.rules.diff import diff_rulesets

        diff = diff_rulesets(customers_v1, customers_v1_1)
        assert diff.has_changes
        assert diff.from_version == "1.0"
        assert diff.to_version == "1.1"
        # CUST_017 is new in v1.1
        assert "CUST_017" in diff.added
        # CUST_010, CUST_007, CUST_013 are modified
        modified_ids = {c.rule_id for c in diff.modified}
        assert "CUST_010" in modified_ids
        assert "CUST_007" in modified_ids

    def test_rollback_through_registry(self, tmp_path, customers_v1, customers_v1_1):
        from dq_core.rules.registry import RuleSetRegistry

        reg = RuleSetRegistry(tmp_path / "reg")
        reg.publish(customers_v1, "u", activate=True)
        reg.publish(customers_v1_1, "u", activate=True)
        assert reg.active_version("customers_silver") == "1.1"
        reg.rollback("customers_silver", "1.0", reason="test rollback")
        assert reg.active_version("customers_silver") == "1.0"
