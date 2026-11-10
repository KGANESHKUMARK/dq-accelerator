"""Unit tests for terminal remediation outcome accounting (P2)."""

from __future__ import annotations

from types import SimpleNamespace

from dq_core.policies.confidence import Decision
from dq_core.remediation.outcomes import remediation_outcomes


def _result(rule_id: str, column: str, failed: int, examples: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(rule_id=rule_id, column=column, failed_records=failed,
                           examples=examples or [])


def test_deterministic_rule_gets_single_terminal_outcome():
    report = SimpleNamespace(results=[_result("R1", "email", 3)])
    out = remediation_outcomes(report, {"R1"}, [], [])
    assert out["items"] == [{"rule_id": "R1", "column": "email", "outcome": "DETERMINISTIC_FIX"}]
    assert out["counts"] == {"DETERMINISTIC_FIX": 1}


def test_ai_decision_maps_to_terminal_outcome():
    report = SimpleNamespace(results=[_result("R2", "phone", 2, ["abc", "12x"])])
    decisions = [
        {"rule_id": "R2", "original_value": "abc", "decision": Decision.AUTO_APPLY.value},
        {"rule_id": "R2", "original_value": "12x", "decision": Decision.BELOW_THRESHOLD.value},
    ]
    out = remediation_outcomes(report, set(), decisions, [])
    by_value = {i["value"]: i["outcome"] for i in out["items"]}
    assert by_value == {"abc": "AI_APPLIED", "12x": "BELOW_CONFIDENCE_THRESHOLD"}


def test_blocked_sensitive_egress_is_terminal():
    report = SimpleNamespace(results=[_result("R3", "ssn", 1, ["123-45-6789"])])
    blocked = [{"rule_id": "R3", "value": "123-45-6789",
                "decision": Decision.BLOCKED_SENSITIVE_DATA_EGRESS.value}]
    out = remediation_outcomes(report, set(), [], blocked)
    assert out["items"][0]["outcome"] == "BLOCKED_SENSITIVE_DATA"


def test_unmatched_failure_is_no_safe_proposal_not_crash():
    report = SimpleNamespace(results=[_result("R4", "name", 5, ["  ", None])])
    out = remediation_outcomes(report, set(), [], [])
    assert all(i["outcome"] == "NO_SAFE_PROPOSAL" for i in out["items"])
    assert len(out["items"]) == 2


def test_passing_rules_produce_no_outcomes():
    report = SimpleNamespace(results=[_result("R5", "id", 0, [])])
    out = remediation_outcomes(report, set(), [], [])
    assert out["items"] == []
    assert out["all_terminal"] is True
