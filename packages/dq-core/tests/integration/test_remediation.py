"""Integration tests: deterministic remediation + AI + confidence policy."""

import pytest

pytestmark = pytest.mark.spark


def test_deterministic_trim_works(spark, customers_v1, default_policy):
    from dq_core.remediation.deterministic import DeterministicRemediator
    from dq_core.validation import columns as C
    from dq_core.validation.engine import ValidationEngine

    df = spark.createDataFrame(
        [(" C0019", "Chloe Dubois", "chloe@x.com", "37", "London", "GB", "ACTIVE", "2024-01-01")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    annotated = ValidationEngine().validate(df, customers_v1).annotated
    rem = DeterministicRemediator(default_policy)
    out = rem.apply(annotated, customers_v1)
    result = out.dataframe.select("customer_id").collect()[0][0]
    assert result == "C0019"


def test_deterministic_lookup_mapping(spark, customers_v1, default_policy):
    from dq_core.remediation.deterministic import DeterministicRemediator
    from dq_core.validation.engine import ValidationEngine

    df = spark.createDataFrame(
        [("C0020", "Dan K", "dan@x.com", "46", "Seattle", "usa", "ACTIVE", "2024-01-01")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    annotated = ValidationEngine().validate(df, customers_v1).annotated
    out = DeterministicRemediator(default_policy).apply(annotated, customers_v1)
    country = out.dataframe.select("country").collect()[0][0]
    assert country == "US"


def test_deterministic_date_standardization(spark, customers_v1, default_policy):
    from dq_core.remediation.deterministic import DeterministicRemediator
    from dq_core.validation.engine import ValidationEngine

    df = spark.createDataFrame(
        [("C0021", "Kavya Menon", "k@x.com", "28", "Bengaluru", "IN", "ACTIVE", "15/01/2024")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    annotated = ValidationEngine().validate(df, customers_v1).annotated
    out = DeterministicRemediator(default_policy).apply(annotated, customers_v1)
    date_val = out.dataframe.select("created_date").collect()[0][0]
    assert date_val == "2024-01-15"


def test_null_replacement_approved(spark, customers_v1, default_policy):
    from dq_core.remediation.deterministic import DeterministicRemediator
    from dq_core.validation.engine import ValidationEngine

    df = spark.createDataFrame(
        [("C0027", "Ayesha Khan", "a@x.com", "29", None, "IN", "ACTIVE", "2024-01-01")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    annotated = ValidationEngine().validate(df, customers_v1).annotated
    out = DeterministicRemediator(default_policy).apply(annotated, customers_v1)
    city = out.dataframe.select("city").collect()[0][0]
    assert city == "UNKNOWN"


def test_deduplication_removes_duplicate(spark, customers_v1, default_policy):
    from dq_core.remediation.deterministic import DeterministicRemediator
    from dq_core.validation.engine import ValidationEngine

    df = spark.createDataFrame(
        [("C0005", "Wei Chen", "w@x.com", "38", "Singapore", "SG", "ACTIVE", "2023-04-02"),
         ("C0005", "Wei Chen", "w@x.com", "38", "Singapore", "SG", "ACTIVE", "2023-04-02")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    annotated = ValidationEngine().validate(df, customers_v1).annotated
    out = DeterministicRemediator(default_policy).apply(annotated, customers_v1)
    assert out.dataframe.count() == 1
    assert out.removed is not None
    assert out.removed.count() == 1


def test_mock_ai_produces_proposals(customers_v1, default_policy):
    from dq_core.ai.mock import MockAIProvider
    from dq_core.ai.base import ProposalRequest

    provider = MockAIProvider.from_file(
        "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator/config/ai/mock_ai_responses.yaml")
    requests = [ProposalRequest(request_id="R-001", record_id="rec-1", record_key="C0021",
                                rule_id="CUST_010", rule_type="ALLOWED_VALUES", column="customer_status",
                                original_value="ACTVE", failure_reason="not in allowed values",
                                dataset="customers")]
    proposals = provider.propose(requests)
    assert len(proposals) == 1
    assert proposals[0].proposed_value == "ACTIVE"
    assert proposals[0].confidence >= 0.90
    assert proposals[0].simulated is True


def test_ai_low_confidence_below_threshold(customers_v1, default_policy):
    from dq_core.ai.mock import MockAIProvider
    from dq_core.ai.base import ProposalRequest
    from dq_core.metadata.models import RuleDefinition, RuleType
    from dq_core.policies.confidence import ConfidencePolicyEngine, Decision

    provider = MockAIProvider.from_file(
        "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator/config/ai/mock_ai_responses.yaml")
    requests = [ProposalRequest(request_id="R-002", record_id="rec-2", record_key="C0030",
                                rule_id="CUST_010", rule_type="ALLOWED_VALUES", column="customer_status",
                                original_value="ACT", failure_reason="not in allowed values",
                                dataset="customers")]
    proposals = provider.propose(requests)
    assert proposals[0].confidence == 0.62

    rule = RuleDefinition.from_dict({"rule_id": "CUST_010", "name": "n",
                                     "rule_type": "ALLOWED_VALUES", "dataset": "customers",
                                     "column": "customer_status", "config": {"values": ["ACTIVE"]},
                                     "remediation": {"ai_assisted": {"enabled": True, "auto_apply": True}}})
    engine = ConfidencePolicyEngine(default_policy)
    decision = engine.evaluate(rule=rule, column="customer_status", original_value="ACT",
                               proposed_value="ACTIVE", confidence=0.62, dataset="customers")
    assert decision.decision == Decision.BELOW_THRESHOLD


def test_ai_exactly_90_percent_applied(customers_v1, default_policy):
    from dq_core.ai.mock import MockAIProvider
    from dq_core.ai.base import ProposalRequest
    from dq_core.metadata.models import RuleDefinition
    from dq_core.policies.confidence import ConfidencePolicyEngine, Decision

    provider = MockAIProvider.from_file(
        "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator/config/ai/mock_ai_responses.yaml")
    requests = [ProposalRequest(request_id="R-003", record_id="rec-3", record_key="C0034",
                                rule_id="CUST_010", rule_type="ALLOWED_VALUES", column="customer_status",
                                original_value="CLSD", failure_reason="not in allowed values",
                                dataset="customers")]
    proposals = provider.propose(requests)
    assert proposals[0].confidence == 0.90

    rule = RuleDefinition.from_dict({"rule_id": "CUST_010", "name": "n",
                                     "rule_type": "ALLOWED_VALUES", "dataset": "customers",
                                     "column": "customer_status", "config": {"values": ["ACTIVE", "CLOSED"]},
                                     "remediation": {"ai_assisted": {"enabled": True, "auto_apply": True}}})
    decision = ConfidencePolicyEngine(default_policy).evaluate(
        rule=rule, column="customer_status", original_value="CLSD",
        proposed_value="CLOSED", confidence=0.90, dataset="customers")
    assert decision.decision == Decision.AUTO_APPLY


def test_invalid_ai_correction_fails_revalidation(spark, customers_v1, default_policy):
    """C0028: AI proposes 'IND' (invalid ISO code) → applied → revalidation fails → quarantined."""
    from dq_core.context import IntegrationMode, PipelineContext
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    root = "D:/Siddharitha/SIDDHARITHA TECHNOLOGIES PRIVATE LIMITED/Projects/Linkedin/DQ-Accelerator"
    cfg = EngineConfig.from_yaml(f"{root}/config/engine.yaml")
    # bypass registry for this isolated test
    cfg.registry.rulesets_path = None
    cfg.registry.pipelines_path = None
    cfg.registry.require_published_for_enforcement = False
    cfg.registry.enforce_pipeline_lifecycle = False
    engine = DataQualityEngine(cfg)

    df = spark.createDataFrame(
        [("C0028", "Mohammed Rafi", "m@x.com", "63", "Chennai", "Bharat", "ACTIVE", "2024-04-01")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    ctx = PipelineContext(pipeline_id="TEST", pipeline_run_id="R1", dataset="customers", platform="LOCAL")
    result = engine.run(df, customers_v1, default_policy, ctx, mode=IntegrationMode.REMEDIATE)
    # The AI applies 'IND' but it fails revalidation (not in allowed values); quarantine expected
    assert result.quarantine_dataframe.count() >= 1
    statuses = {r["remediation_status"] for r in result.quarantine_dataframe.select("remediation_status").collect()}
    assert "CORRECTION_FAILED_REVALIDATION" in statuses
