"""Validation result models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 2) if whole else 0.0


@dataclass
class RuleResult:
    rule_id: str
    rule_name: str
    rule_type: str
    scope: str
    level: str                      # ROW (record-level failures) | DATASET (dataset-level quality failure)
    severity: str
    dataset: str
    column: str | None
    ruleset_id: str
    ruleset_version: str
    rule_version: str
    status: str                     # PASSED | FAILED | NOT_EVALUATED
    total_records: int
    passed_records: int
    failed_records: int
    pass_percentage: float
    failure_percentage: float
    failure_reasons: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    evaluated_in_batch: bool = False
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    dataset: str
    ruleset_id: str
    ruleset_version: str
    ruleset_checksum: str
    total_records: int
    passed_records: int            # records without ERROR violations
    failed_records: int            # records with >= 1 ERROR violation
    warning_records: int           # records with warnings but no errors
    row_level_results: list[RuleResult] = field(default_factory=list)
    dataset_level_results: list[RuleResult] = field(default_factory=list)
    skipped_rules: list[dict[str, str]] = field(default_factory=list)
    duration_ms: float = 0.0
    started_at: str = ""

    @property
    def results(self) -> list[RuleResult]:
        return self.row_level_results + self.dataset_level_results

    def result(self, rule_id: str) -> RuleResult:
        for r in self.results:
            if r.rule_id == rule_id:
                return r
        raise KeyError(rule_id)

    def failed_rules(self) -> list[RuleResult]:
        return [r for r in self.results if r.status == "FAILED"]

    def dataset_level_errors(self) -> list[RuleResult]:
        return [r for r in self.dataset_level_results if r.status == "FAILED" and r.severity == "ERROR"]

    def not_evaluated_errors(self) -> list[RuleResult]:
        return [r for r in self.results if r.status == "NOT_EVALUATED" and r.severity == "ERROR"]

    @property
    def pass_percentage(self) -> float:
        return pct(self.passed_records, self.total_records)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "ruleset_id": self.ruleset_id,
            "ruleset_version": self.ruleset_version,
            "ruleset_checksum": self.ruleset_checksum,
            "started_at": self.started_at,
            "duration_ms": self.duration_ms,
            "total_records": self.total_records,
            "passed_records": self.passed_records,
            "failed_records": self.failed_records,
            "warning_records": self.warning_records,
            "pass_percentage": self.pass_percentage,
            "row_level_results": [r.to_dict() for r in self.row_level_results],
            "dataset_level_results": [r.to_dict() for r in self.dataset_level_results],
            "skipped_rules": self.skipped_rules,
        }

    def summary_table(self) -> str:
        header = f"{'RULE':<12} {'TYPE':<22} {'LEVEL':<7} {'SEV':<7} {'COLUMN':<18} {'STATUS':<13} {'FAILED':>7} {'PASS%':>7}"
        lines = [header, "-" * len(header)]
        for r in self.results:
            lines.append(f"{r.rule_id:<12} {r.rule_type:<22} {r.level:<7} {r.severity:<7} {str(r.column or '-')[:18]:<18} "
                         f"{r.status:<13} {r.failed_records:>7} {r.pass_percentage:>7.2f}")
        return "\n".join(lines)
