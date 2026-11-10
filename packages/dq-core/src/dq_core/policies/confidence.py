"""Confidence policy engine: decides what happens to each AI correction proposal.

Decision order (first match wins):

1. no proposal / unchanged value / invalid confidence      -> NO_PROPOSAL / REJECTED_*
2. matching human approval on file                         -> APPROVED_BY_HUMAN (applied)
3. matching human rejection on file                        -> REJECTED_BY_HUMAN (withheld)
4. confidence  < threshold                                 -> BELOW_THRESHOLD (quarantine)
5. sensitive column (identity, account, amount, regulatory) -> REQUIRES_APPROVAL (quarantine)
6. rule policy does not allow auto-apply / AI auto-apply off -> REQUIRES_APPROVAL (quarantine)
7. otherwise                                               -> AUTO_APPLY (then revalidated)

Comparisons use ``Decimal`` so 0.8999 < 0.90 <= 0.90 exactly (no binary float surprises).
A high confidence alone is never treated as proof of correctness: every applied correction is
revalidated and sensitive columns always need a human.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from dq_core.metadata.models import RuleDefinition
from dq_core.policies.approvals import ApprovalStore, RejectionStore
from dq_core.policies.models import RemediationPolicyConfig


class Decision(str, Enum):
    AUTO_APPLY = "AUTO_APPLY"
    APPROVED_BY_HUMAN = "APPROVED_BY_HUMAN"
    BELOW_THRESHOLD = "BELOW_THRESHOLD"
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"
    NO_PROPOSAL = "NO_PROPOSAL"
    REJECTED_NO_CHANGE = "REJECTED_NO_CHANGE"
    REJECTED_INVALID_CONFIDENCE = "REJECTED_INVALID_CONFIDENCE"
    REJECTED_BY_HUMAN = "REJECTED_BY_HUMAN"
    BLOCKED_SENSITIVE_DATA_EGRESS = "BLOCKED_SENSITIVE_DATA_EGRESS"
    NOT_EVALUATED_LIMIT = "NOT_EVALUATED_LIMIT"

    @property
    def applies_correction(self) -> bool:
        return self in (Decision.AUTO_APPLY, Decision.APPROVED_BY_HUMAN)


@dataclass(frozen=True)
class PolicyDecision:
    decision: Decision
    reason: str
    sensitive_category: str | None = None
    approved_by: str | None = None


class ConfidencePolicyEngine:
    def __init__(self, policy: RemediationPolicyConfig, approvals: ApprovalStore | None = None,
                 rejections: RejectionStore | None = None) -> None:
        self.policy = policy
        self.approvals = approvals or ApprovalStore()
        self.rejections = rejections or RejectionStore()

    @property
    def threshold(self) -> Decimal:
        return self.policy.confidence_threshold

    def meets_threshold(self, confidence: Any) -> bool:
        return Decimal(str(confidence)) >= self.threshold

    def evaluate(self, *, rule: RuleDefinition, column: str, original_value: Any, proposed_value: Any,
                 confidence: Any, dataset: str) -> PolicyDecision:
        category = self.policy.sensitive_category(column)
        if proposed_value is None or str(proposed_value).strip() == "":
            return PolicyDecision(Decision.NO_PROPOSAL, "provider returned no correction", category)
        if original_value is not None and str(proposed_value) == str(original_value):
            return PolicyDecision(Decision.REJECTED_NO_CHANGE, "proposed value equals the original value", category)
        try:
            conf = Decimal(str(confidence))
        except Exception:
            return PolicyDecision(Decision.REJECTED_INVALID_CONFIDENCE, f"invalid confidence {confidence!r}", category)
        if not Decimal("0") <= conf <= Decimal("1"):
            return PolicyDecision(Decision.REJECTED_INVALID_CONFIDENCE, f"confidence {confidence} outside [0, 1]", category)
        approval = self.approvals.find(dataset, column, original_value, proposed_value)
        if approval is not None:
            return PolicyDecision(Decision.APPROVED_BY_HUMAN, f"approved by {approval.approved_by} {approval.ticket}".strip(),
                                  category, approval.approved_by)
        rejection = self.rejections.find(dataset, column, original_value, proposed_value)
        if rejection is not None:
            return PolicyDecision(Decision.REJECTED_BY_HUMAN,
                                  f"rejected by {rejection.rejected_by or 'steward'}"
                                  + (f" — {rejection.reason}" if rejection.reason else ""), category)
        if conf < self.threshold:
            return PolicyDecision(Decision.BELOW_THRESHOLD,
                                  f"confidence {conf} below threshold {self.threshold}", category)
        if category is not None:
            return PolicyDecision(Decision.REQUIRES_APPROVAL,
                                  f"sensitive column ({category}) - human approval required regardless of confidence",
                                  category)
        if not rule.remediation.ai_assisted.auto_apply:
            return PolicyDecision(Decision.REQUIRES_APPROVAL, "rule remediation policy requires human review (auto_apply: false)")
        if not self.policy.ai.allow_auto_apply:
            return PolicyDecision(Decision.REQUIRES_APPROVAL, "policy disables automatic AI corrections")
        return PolicyDecision(Decision.AUTO_APPLY, f"confidence {conf} >= threshold {self.threshold}; policy allows auto-apply")
