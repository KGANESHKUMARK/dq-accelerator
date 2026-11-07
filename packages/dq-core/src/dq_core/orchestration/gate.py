"""Quality gate evaluation (dataset-level go / no-go decision for publishing)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from dq_core.policies.models import QualityGatePolicy
from dq_core.validation.models import ValidationReport


@dataclass
class QualityGateResult:
    passed: bool
    checks: list[dict[str, Any]] = field(default_factory=list)

    @property
    def failed_checks(self) -> list[dict[str, Any]]:
        return [c for c in self.checks if not c["passed"]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"failed_checks": [c["check"] for c in self.failed_checks]}


def evaluate_quality_gate(policy: QualityGatePolicy, report: ValidationReport, *, input_records: int,
                          accepted_records: int, quarantined_records: int) -> QualityGateResult:
    checks: list[dict[str, Any]] = []
    rate = round(quarantined_records / input_records, 4) if input_records else 0.0
    checks.append({"check": "max_quarantine_rate", "passed": rate <= policy.max_quarantine_rate,
                   "actual": rate, "threshold": policy.max_quarantine_rate})
    checks.append({"check": "min_accepted_records", "passed": accepted_records >= policy.min_accepted_records,
                   "actual": accepted_records, "threshold": policy.min_accepted_records})
    if policy.fail_on_dataset_level_errors:
        failures = [r.rule_id for r in report.dataset_level_errors()]
        checks.append({"check": "dataset_level_errors", "passed": not failures, "actual": failures, "threshold": []})
    if policy.fail_on_not_evaluated_rules:
        missing = [r.rule_id for r in report.not_evaluated_errors()]
        checks.append({"check": "not_evaluated_error_rules", "passed": not missing, "actual": missing, "threshold": []})
    return QualityGateResult(passed=all(c["passed"] for c in checks), checks=checks)
