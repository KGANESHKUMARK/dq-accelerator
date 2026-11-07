"""Integration tests: profiling and candidate rule generation."""

import pytest

pytestmark = pytest.mark.spark


def test_profile_completes(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler
    from dq_core.runtime.config import ProfilingSettings

    report = DataProfiler(ProfilingSettings(mode="FULL")).profile(customers_df, "customers")
    assert report.row_count == 38
    assert report.column_count == 8
    assert report.columns["customer_id"] is not None


def test_profile_detects_anomalies(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    types = {a["type"] for a in report.anomalies}
    assert "LEADING_TRAILING_WHITESPACE" in types
    assert "OUTLIERS" in types
    assert "INCONSISTENT_FORMATTING" in types


def test_profile_detects_duplicate_rows(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    assert report.duplicate_rows >= 1   # C0005 is duplicated in the dataset


def test_profile_infers_email_column(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    assert report.columns["email"].inferred_type == "email"


def test_profile_infers_date_column(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    assert "date" in report.columns["created_date"].inferred_type.lower()


def test_profile_detects_mixed_date_formats(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    assert len(report.columns["created_date"].date_format_matches) > 1


def test_candidate_rule_generation(spark, customers_df):
    from dq_core.generation.generator import CandidateRuleGenerator
    from dq_core.metadata.models import RuleStatus, RuleType
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    candidates = CandidateRuleGenerator().generate(report)
    types = {r.rule_type for r in candidates.rules}
    statuses = {r.status for r in candidates.rules}
    assert RuleType.NOT_NULL in types
    assert RuleType.SCHEMA_VALIDATION in types
    assert RuleType.DUPLICATE in types
    assert statuses == {RuleStatus.CANDIDATE}  # all must be CANDIDATE


def test_all_candidate_rules_have_evidence(spark, customers_df):
    from dq_core.generation.generator import CandidateRuleGenerator
    from dq_core.profiling.profiler import DataProfiler

    report = DataProfiler().profile(customers_df, "customers")
    candidates = CandidateRuleGenerator().generate(report)
    for rule in candidates.rules:
        assert rule.generation is not None
        assert "confidence" in rule.generation
        assert "evidence" in rule.generation


def test_approximate_profiling(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler
    from dq_core.runtime.config import ProfilingSettings

    report = DataProfiler(ProfilingSettings(mode="APPROXIMATE")).profile(customers_df, "customers")
    assert report.approximate is True
    assert report.row_count > 0


def test_sample_profiling(spark, customers_df):
    from dq_core.profiling.profiler import DataProfiler
    from dq_core.runtime.config import ProfilingSettings

    report = DataProfiler(ProfilingSettings(mode="SAMPLE", sample_fraction=0.5)).profile(customers_df, "customers")
    assert report.sampled is True
    assert report.source_row_count == 38
