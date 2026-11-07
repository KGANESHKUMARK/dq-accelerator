"""DQResult - the contract returned to every host pipeline / adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from dq_core.context import ExecutionContext, IntegrationMode
from dq_core.orchestration.gate import QualityGateResult
from dq_core.profiling.models import ProfileReport
from dq_core.reporting.metrics import ExecutionMetrics
from dq_core.runtime.errors import QualityGateFailed
from dq_core.runtime.utils import to_jsonable
from dq_core.validation.models import ValidationReport


@dataclass
class RemediationResults:
    summary: dict[str, Any] = field(default_factory=dict)
    ai_decisions: list[dict[str, Any]] = field(default_factory=list)
    blocked_by_policy: list[dict[str, Any]] = field(default_factory=list)
    dataframe: DataFrame | None = None            # per-record audit log of corrections / proposals
    removed_duplicates: DataFrame | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"summary": self.summary, "ai_decisions": self.ai_decisions, "blocked_by_policy": self.blocked_by_policy,
                "ai_outputs_are_simulated": any(d.get("simulated") for d in self.ai_decisions)}


@dataclass
class DQResult:
    run_id: str
    mode: IntegrationMode
    execution_context: ExecutionContext
    accepted_dataframe: DataFrame
    quarantine_dataframe: DataFrame
    validation_results: ValidationReport | None
    remediation_results: RemediationResults
    execution_metrics: ExecutionMetrics
    quality_gate: QualityGateResult
    final_validation_results: ValidationReport | None = None
    profile: ProfileReport | None = None
    skipped: bool = False
    skip_reason: str | None = None
    _persisted: list[Any] = field(default_factory=list, repr=False)

    # ------------------------------------------------------------------ convenience
    @property
    def remediation_dataframe(self) -> DataFrame | None:
        return self.remediation_results.dataframe

    def raise_if_gate_failed(self) -> None:
        if not self.quality_gate.passed:
            raise QualityGateFailed(f"Quality gate failed for {self.execution_context.pipeline.dataset}: "
                                    f"{[c['check'] for c in self.quality_gate.failed_checks]}", self.quality_gate)

    def unpersist(self) -> None:
        """Release cached intermediate data (accepted/quarantine frames are recomputed if used later)."""
        for outcome in self._persisted:
            outcome.unpersist()
        self._persisted.clear()

    # ------------------------------------------------------------------ reporting
    def to_dict(self) -> dict[str, Any]:
        return to_jsonable({
            "report_type": "DQ_EXECUTION_REPORT",
            "run_id": self.run_id,
            "integration_mode": self.mode.value,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "execution_context": self.execution_context.to_dict(),
            "execution_metrics": self.execution_metrics.to_dict(),
            "quality_gate": self.quality_gate.to_dict(),
            "validation_results": self.validation_results.to_dict() if self.validation_results else None,
            "final_validation_results": self.final_validation_results.to_dict() if self.final_validation_results else None,
            "remediation_results": self.remediation_results.to_dict(),
            "profile_summary": ({"row_count": self.profile.row_count, "anomalies": self.profile.anomalies,
                                 "primary_key_candidates": self.profile.primary_key_candidates}
                                if self.profile else None),
        })

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def write_report(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")
        return path

    def summary_text(self) -> str:
        m = self.execution_metrics
        lines = [
            f"DQ run {self.run_id}  [{m.integration_mode}]  dataset={m.dataset} stage={m.stage}  status={m.status}",
            f"  rule set {m.ruleset_id}@{m.ruleset_version} (checksum {m.ruleset_checksum[:12]}, published={m.ruleset_published})"
            f"  policy {m.policy_id}@{m.policy_version} threshold={m.confidence_threshold}",
            f"  input={m.input_records} accepted={m.accepted_records} quarantined={m.quarantined_records} "
            f"duplicates_removed={m.removed_duplicates} warnings={m.records_with_warnings}",
            f"  remediated+accepted={m.remediated_records_accepted} remediated-but-quarantined={m.remediated_records_quarantined} "
            f"deterministic_fixes={m.deterministic_corrections} ai_fixes={m.ai_corrections_applied}",
            f"  quality gate: {'PASSED' if self.quality_gate.passed else 'FAILED'}"
            + (f" {[c['check'] for c in self.quality_gate.failed_checks]}" if not self.quality_gate.passed else ""),
            f"  duration={m.duration_ms} ms phases={m.phase_durations_ms}",
        ]
        if m.quarantine_status_breakdown:
            lines.append(f"  quarantine by status: {m.quarantine_status_breakdown}")
        return "\n".join(lines)
