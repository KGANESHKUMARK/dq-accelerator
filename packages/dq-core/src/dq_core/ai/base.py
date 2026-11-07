"""AI remediation provider interface.

Design constraints (enforced by the engine, not by providers):
* requests are batched and de-duplicated by (rule, column, value) and capped per run - never one
  LLM call per row on large datasets;
* providers only return *proposed values*; generated code/SQL is never executed;
* every proposal is evaluated by the confidence policy and revalidated before acceptance.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Sequence

from dq_core.runtime.utils import utc_now_iso


@dataclass(frozen=True)
class ProposalRequest:
    request_id: str
    record_id: str
    record_key: str | None
    rule_id: str
    rule_type: str
    column: str
    original_value: str | None
    failure_reason: str
    dataset: str
    occurrences: int = 1
    context: dict[str, Any] = field(default_factory=dict)  # rule metadata only (allowed values, pattern...)


@dataclass(frozen=True)
class AIProposal:
    request_id: str
    record_id: str
    rule_id: str
    column: str
    original_value: str | None
    proposed_value: str | None
    confidence: float
    reason: str
    evidence: dict[str, Any]
    provider: str
    model_version: str
    simulated: bool
    generated_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AIRemediationProvider(ABC):
    """Interface for real or simulated AI correction providers."""

    name: str = "AIRemediationProvider"
    model_version: str = "unknown"
    is_external: bool = True  # True when values leave the Spark cluster (e.g. a hosted LLM API)
    simulated: bool = False

    @abstractmethod
    def propose(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        """Return exactly one proposal per request (proposed_value=None when no safe correction)."""

    def describe(self) -> dict[str, Any]:
        return {"provider": self.name, "model_version": self.model_version, "is_external": self.is_external,
                "simulated": self.simulated}
