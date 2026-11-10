"""Unit tests for the confidence policy engine boundary conditions."""

from __future__ import annotations

from decimal import Decimal

import pytest

from dq_core.metadata.models import RuleDefinition, RuleType
from dq_core.policies.confidence import ConfidencePolicyEngine, Decision
from dq_core.policies.models import RemediationPolicyConfig, parse_threshold


# helper rule with AI auto_apply enabled
def _rule(**overrides) -> RuleDefinition:
    base = dict(rule_id="R1", name="n", rule_type=RuleType.ALLOWED_VALUES, dataset="ds",
                column="status", config={"values": ["ACTIVE"]})
    base.update(overrides)
    # inject ai_assisted via remediation dict
    if "auto_apply" in overrides:
        from dq_core.metadata.models import AIRemediationSettings, RemediationPolicy
        base["remediation"] = RemediationPolicy(
            ai_assisted=AIRemediationSettings(enabled=True, auto_apply=overrides.pop("auto_apply")))
    return RuleDefinition.from_dict(base)


def _engine(threshold="0.90", **policy_kwargs) -> ConfidencePolicyEngine:
    policy = RemediationPolicyConfig(confidence_threshold=parse_threshold(threshold), **policy_kwargs)
    return ConfidencePolicyEngine(policy)


class TestConfidenceBoundaries:
    """Tests for every required boundary value from Section 12."""

    @pytest.mark.parametrize("confidence,expected", [
        ("0.89",    Decision.BELOW_THRESHOLD),
        ("0.8999",  Decision.BELOW_THRESHOLD),
        ("0.90",    Decision.AUTO_APPLY),          # exactly at threshold → AUTO_APPLY
        ("0.95",    Decision.AUTO_APPLY),
        ("1.00",    Decision.AUTO_APPLY),
    ])
    def test_threshold_boundaries(self, confidence, expected):
        rule = _rule()
        from dq_core.metadata.models import AIRemediationSettings, RemediationPolicy
        rule2 = RuleDefinition.from_dict({**rule.to_dict(), "remediation": {"ai_assisted": {"enabled": True, "auto_apply": True}}})
        engine = _engine("0.90")
        d = engine.evaluate(rule=rule2, column="status", original_value="ACTVE",
                             proposed_value="ACTIVE", confidence=confidence, dataset="ds")
        assert d.decision == expected, f"confidence={confidence}: got {d.decision}, expected {expected}"

    def test_no_proposal_returns_no_proposal(self):
        rule = _rule()
        d = _engine().evaluate(rule=rule, column="c", original_value="X",
                               proposed_value=None, confidence="0.95", dataset="ds")
        assert d.decision == Decision.NO_PROPOSAL

    def test_no_change_rejected(self):
        rule = _rule()
        d = _engine().evaluate(rule=rule, column="c", original_value="ACTIVE",
                               proposed_value="ACTIVE", confidence="1.00", dataset="ds")
        assert d.decision == Decision.REJECTED_NO_CHANGE

    def test_invalid_confidence_rejected(self):
        rule = _rule()
        d = _engine().evaluate(rule=rule, column="c", original_value="X",
                               proposed_value="Y", confidence="banana", dataset="ds")
        assert d.decision == Decision.REJECTED_INVALID_CONFIDENCE

    def test_sensitive_column_requires_approval_even_at_100(self):
        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "NOT_NULL",
                                         "dataset": "ds", "column": "customer_id"})
        d = _engine("0.90").evaluate(rule=rule, column="customer_id", original_value="X",
                                     proposed_value="Y", confidence="1.00", dataset="ds")
        assert d.decision == Decision.REQUIRES_APPROVAL
        assert d.sensitive_category == "customer_identity"

    def test_transaction_amount_sensitive(self):
        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "DATA_TYPE",
                                         "dataset": "ds", "column": "transaction_amount",
                                         "config": {"data_type": "decimal(18,2)"}})
        d = _engine("0.90").evaluate(rule=rule, column="transaction_amount", original_value="1O50.00",
                                     proposed_value="1050.00", confidence="0.97", dataset="ds")
        assert d.decision == Decision.REQUIRES_APPROVAL
        assert d.sensitive_category == "transaction_amounts"

    def test_human_approval_overrides_all(self):
        from dq_core.policies.approvals import Approval, ApprovalStore

        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "NOT_NULL",
                                         "dataset": "ds", "column": "customer_id"})
        store = ApprovalStore([Approval(dataset="ds", column="customer_id",
                                        original_value="BAD_ID", approved_value="C0001",
                                        approved_by="steward@bank.example")])
        engine = ConfidencePolicyEngine(RemediationPolicyConfig(), store)
        d = engine.evaluate(rule=rule, column="customer_id", original_value="BAD_ID",
                            proposed_value="C0001", confidence="0.30", dataset="ds")
        assert d.decision == Decision.APPROVED_BY_HUMAN
        assert d.approved_by == "steward@bank.example"


class TestParseThreshold:
    def test_valid(self):
        assert parse_threshold("0.90") == Decimal("0.90")
        assert parse_threshold(0.9) == Decimal("0.9")
        assert parse_threshold(1) == Decimal("1")

    def test_invalid_string(self):
        from dq_core.runtime.errors import PolicyError
        with pytest.raises(PolicyError):
            parse_threshold("abc")

    def test_out_of_range(self):
        from dq_core.runtime.errors import PolicyError
        with pytest.raises(PolicyError):
            parse_threshold(1.5)


class TestRejectionStore:
    def test_rejection_suppresses_matching_proposal(self):
        from dq_core.policies.approvals import Rejection, RejectionStore

        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "NOT_NULL",
                                         "dataset": "ds", "column": "status"})
        store = RejectionStore([Rejection(dataset="ds", column="status",
                                          original_value="OK", proposed_value="HEALTHY",
                                          rejected_by="steward@x", reason="wrong semantics")])
        engine = ConfidencePolicyEngine(RemediationPolicyConfig(), rejections=store)
        d = engine.evaluate(rule=rule, column="status", original_value="OK",
                            proposed_value="HEALTHY", confidence="0.99", dataset="ds")
        assert d.decision == Decision.REJECTED_BY_HUMAN
        assert "steward@x" in d.reason

    def test_rejection_does_not_block_different_proposal(self):
        from dq_core.policies.approvals import Rejection, RejectionStore
        from dq_core.metadata.models import AIRemediationSettings, RemediationPolicy

        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "ALLOWED_VALUES",
                                         "dataset": "ds", "column": "status",
                                         "config": {"values": ["ACTIVE"]},
                                         "remediation": {"ai_assisted": {"enabled": True, "auto_apply": True}}})
        store = RejectionStore([Rejection(dataset="ds", column="status",
                                          original_value="OK", proposed_value="HEALTHY")])
        engine = ConfidencePolicyEngine(RemediationPolicyConfig(), rejections=store)
        d = engine.evaluate(rule=rule, column="status", original_value="ACTVE",
                            proposed_value="ACTIVE", confidence="0.97", dataset="ds")
        assert d.decision == Decision.AUTO_APPLY

    def test_approval_wins_over_rejection(self):
        from dq_core.policies.approvals import Approval, ApprovalStore, Rejection, RejectionStore

        rule = RuleDefinition.from_dict({"rule_id": "R1", "name": "n", "rule_type": "NOT_NULL",
                                         "dataset": "ds", "column": "status"})
        approvals = ApprovalStore([Approval(dataset="ds", column="status", original_value="OK",
                                            approved_value="HEALTHY", approved_by="boss@x")])
        rejections = RejectionStore([Rejection(dataset="ds", column="status", original_value="OK",
                                               proposed_value="HEALTHY", rejected_by="steward@x")])
        engine = ConfidencePolicyEngine(RemediationPolicyConfig(), approvals, rejections)
        d = engine.evaluate(rule=rule, column="status", original_value="OK",
                            proposed_value="HEALTHY", confidence="0.50", dataset="ds")
        assert d.decision == Decision.APPROVED_BY_HUMAN

    def test_rejection_store_load_from_yaml(self, tmp_path):
        import yaml
        from dq_core.policies.approvals import RejectionStore

        f = tmp_path / "rejected_corrections.yaml"
        f.write_text(yaml.safe_dump({"rejections": [
            {"dataset": "ds", "column": "c", "original_value": "a",
             "proposed_value": "b", "rejected_by": "s", "rejected_at": "t", "reason": "r"}]}))
        store = RejectionStore.load(f)
        assert len(store) == 1
        assert store.find("ds", "c", "a", "b") is not None
        assert store.find("ds", "c", "a", "other") is None
