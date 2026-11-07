"""Remediation / confidence / quality-gate policy configuration (``config/policies/*.yaml``)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from dq_core.context import IntegrationMode
from dq_core.metadata.models import DeterministicAction
from dq_core.metadata.serialization import load_document
from dq_core.runtime.errors import PolicyError
from dq_core.runtime.utils import resolve_path, sha256_of

DEFAULT_CONFIDENCE_THRESHOLD = Decimal("0.90")
DEFAULT_SENSITIVE_CATEGORIES: dict[str, tuple[str, ...]] = {
    "customer_identity": ("customer_id", "customer_name", "national_id", "passport_number", "date_of_birth"),
    "account_identifiers": ("account_id", "account_number", "iban", "card_number"),
    "transaction_amounts": ("transaction_amount", "amount"),
    "regulatory": ("kyc_status", "tax_id", "pan_number", "risk_rating", "sanctions_flag"),
}
DEFAULT_PERMITTED_SENSITIVE_ACTIONS = ("TRIM", "UPPERCASE", "LOOKUP_MAPPING", "DATE_STANDARDIZATION", "DEDUPLICATE")


def parse_threshold(value: Any) -> Decimal:
    """Parse a confidence threshold expressed as a decimal between 0 and 1 (e.g. 0.90)."""
    try:
        threshold = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise PolicyError(f"Confidence threshold '{value}' is not a number; use a decimal between 0 and 1 (e.g. 0.90)") from exc
    if not Decimal("0") <= threshold <= Decimal("1"):
        raise PolicyError(f"Confidence threshold {value} must be between 0 and 1 (e.g. 0.90 for 90%)")
    return threshold


@dataclass(frozen=True)
class AIPolicy:
    enabled: bool = True
    allow_auto_apply: bool = True
    max_requests_per_run: int = 500
    allow_sensitive_values_to_external_providers: bool = False


@dataclass(frozen=True)
class QualityGatePolicy:
    max_quarantine_rate: float = 0.40
    fail_on_dataset_level_errors: bool = True
    fail_on_not_evaluated_rules: bool = True
    min_accepted_records: int = 1


@dataclass(frozen=True)
class RemediationPolicyConfig:
    policy_id: str = "DEFAULT"
    version: str = "1.0"
    description: str = ""
    default_integration_mode: IntegrationMode | None = None
    confidence_threshold: Decimal = DEFAULT_CONFIDENCE_THRESHOLD
    ai: AIPolicy = field(default_factory=AIPolicy)
    sensitive_categories: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: dict(DEFAULT_SENSITIVE_CATEGORIES))
    permitted_sensitive_deterministic_actions: tuple[str, ...] = DEFAULT_PERMITTED_SENSITIVE_ACTIONS
    quality_gate: QualityGatePolicy = field(default_factory=QualityGatePolicy)
    approvals_path: str | None = None
    source_path: str | None = None

    def sensitive_category(self, column: str | None) -> str | None:
        if not column:
            return None
        lowered = column.lower()
        for category, columns in self.sensitive_categories.items():
            if lowered in {c.lower() for c in columns}:
                return category
        return None

    def is_sensitive(self, column: str | None) -> bool:
        return self.sensitive_category(column) is not None

    def deterministic_action_permitted(self, column: str | None, action: DeterministicAction) -> bool:
        if not self.is_sensitive(column):
            return True
        return action.value in self.permitted_sensitive_deterministic_actions

    def with_threshold(self, threshold: Any) -> "RemediationPolicyConfig":
        from dataclasses import replace

        return replace(self, confidence_threshold=parse_threshold(threshold))

    def checksum(self) -> str:
        return sha256_of(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "description": self.description,
            "default_integration_mode": self.default_integration_mode.value if self.default_integration_mode else None,
            "confidence_threshold": str(self.confidence_threshold),
            "ai": self.ai.__dict__,
            "sensitive_categories": {k: list(v) for k, v in self.sensitive_categories.items()},
            "permitted_sensitive_deterministic_actions": list(self.permitted_sensitive_deterministic_actions),
            "quality_gate": self.quality_gate.__dict__,
            "approvals_path": self.approvals_path,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], source_path: str | None = None) -> "RemediationPolicyConfig":
        data = data.get("policy", data)
        base_dir = str(Path(source_path).resolve().parent) if source_path else None
        confidence = data.get("confidence") or {}
        sensitive = data.get("sensitive_data") or {}
        categories = sensitive.get("categories")
        approvals = (data.get("approvals") or {}).get("file")
        mode = data.get("default_integration_mode")
        try:
            return cls(
                policy_id=str(data.get("policy_id", "DEFAULT")),
                version=str(data.get("version", "1.0")),
                description=str(data.get("description", "")),
                default_integration_mode=IntegrationMode.parse(mode) if mode else None,
                confidence_threshold=parse_threshold(confidence.get("threshold", DEFAULT_CONFIDENCE_THRESHOLD)),
                ai=AIPolicy(**(data.get("ai") or {})),
                sensitive_categories={k: tuple(v) for k, v in categories.items()} if categories else dict(DEFAULT_SENSITIVE_CATEGORIES),
                permitted_sensitive_deterministic_actions=tuple(
                    a.upper() for a in sensitive.get("permitted_deterministic_actions", DEFAULT_PERMITTED_SENSITIVE_ACTIONS)),
                quality_gate=QualityGatePolicy(**(data.get("quality_gate") or {})),
                approvals_path=resolve_path(approvals, base_dir) if approvals else None,
                source_path=source_path,
            )
        except TypeError as exc:
            raise PolicyError(f"Invalid policy configuration: {exc}") from exc

    @classmethod
    def load(cls, path: str | Path) -> "RemediationPolicyConfig":
        return cls.from_dict(load_document(path), source_path=str(Path(path).resolve()))
