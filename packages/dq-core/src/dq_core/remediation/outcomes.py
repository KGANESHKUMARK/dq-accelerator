"""Terminal remediation outcome audit.

This module does not decide whether a correction is safe. It accounts for every
failed rule after the existing deterministic and AI policy decisions.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from dq_core.policies.confidence import Decision


_TERMINAL = {
    Decision.AUTO_APPLY.value: "AI_APPLIED",
    Decision.APPROVED_BY_HUMAN.value: "HUMAN_APPROVED_AI",
    Decision.BELOW_THRESHOLD.value: "BELOW_CONFIDENCE_THRESHOLD",
    Decision.REQUIRES_APPROVAL.value: "PENDING_HUMAN_APPROVAL",
    Decision.NO_PROPOSAL.value: "NO_SAFE_PROPOSAL",
    Decision.REJECTED_NO_CHANGE.value: "NO_SAFE_PROPOSAL",
    Decision.REJECTED_INVALID_CONFIDENCE.value: "NO_SAFE_PROPOSAL",
    Decision.REJECTED_BY_HUMAN.value: "REJECTED_BY_HUMAN",
    Decision.BLOCKED_SENSITIVE_DATA_EGRESS.value: "BLOCKED_SENSITIVE_DATA",
    Decision.NOT_EVALUATED_LIMIT.value: "NOT_EVALUATED_LIMIT",
}


def remediation_outcomes(validation_report, deterministic_rules: set[str], ai_decisions: list[dict[str, Any]],
                         blocked: list[dict[str, Any]]) -> dict[str, Any]:
    """Return one terminal outcome per failed rule example."""
    outcomes: list[dict[str, Any]] = []
    decisions = {(d.get("rule_id"), str(d.get("original_value"))): d for d in ai_decisions}
    blocked_keys = {(d.get("rule_id"), str(d.get("value"))): d for d in blocked}
    for result in getattr(validation_report, "results", []):
        if result.failed_records <= 0:
            continue
        if result.rule_id in deterministic_rules:
            outcomes.append({"rule_id": result.rule_id, "column": result.column, "outcome": "DETERMINISTIC_FIX"})
            continue
        examples = result.examples or [None]
        for example in examples:
            key = (result.rule_id, str(example))
            decision = decisions.get(key) or blocked_keys.get(key)
            raw = decision.get("decision") if decision else None
            outcomes.append({
                "rule_id": result.rule_id, "column": result.column, "value": example,
                "outcome": _TERMINAL.get(raw, "NO_SAFE_PROPOSAL"),
            })
    return {"items": outcomes, "counts": dict(Counter(item["outcome"] for item in outcomes)),
            "all_terminal": all(item["outcome"] for item in outcomes)}
