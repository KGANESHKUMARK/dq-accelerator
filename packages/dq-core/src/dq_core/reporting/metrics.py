"""Execution metrics produced for every DQ run."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ExecutionMetrics:
    dq_run_id: str
    pipeline_id: str
    pipeline_run_id: str
    platform: str
    dataset: str
    stage: str
    integration_mode: str
    status: str                      # COMPLETED | SKIPPED_PIPELINE_<STATE>
    ruleset_id: str
    ruleset_version: str
    ruleset_checksum: str
    ruleset_published: bool
    policy_id: str
    policy_version: str
    confidence_threshold: str
    engine_version: str
    spark_version: str | None
    started_at: str
    finished_at: str
    duration_ms: float
    input_records: int = 0
    accepted_records: int = 0
    quarantined_records: int = 0
    removed_duplicates: int = 0
    records_with_warnings: int = 0
    remediated_records_accepted: int = 0
    remediated_records_quarantined: int = 0
    deterministic_corrections: int = 0
    ai_corrections_applied: int = 0
    ai_proposals_evaluated: int = 0
    rules_evaluated: int = 0
    rules_failed: int = 0
    rules_not_evaluated: int = 0
    rules_skipped_not_approved: int = 0
    acceptance_rate: float = 0.0
    quarantine_rate: float = 0.0
    quality_gate_passed: bool = True
    quarantine_status_breakdown: dict[str, int] = field(default_factory=dict)
    phase_durations_ms: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
