"""Pipeline and execution context contracts shared by every adapter and integration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping

from dq_core.runtime.errors import ConfigurationError


class IntegrationMode(str, Enum):
    """How the engine affects the host pipeline."""

    OBSERVE_ONLY = "OBSERVE_ONLY"  # profile + validate, never change pipeline output
    ENFORCE = "ENFORCE"            # split accepted / quarantined records, no corrections
    REMEDIATE = "REMEDIATE"        # approved corrections + revalidation + quarantine

    @classmethod
    def parse(cls, value: "str | IntegrationMode") -> "IntegrationMode":
        try:
            return value if isinstance(value, IntegrationMode) else cls(str(value).strip().upper())
        except ValueError as exc:
            raise ConfigurationError(f"Unknown integration mode '{value}'. Use one of {[m.value for m in cls]}") from exc


class Stage(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"

    @classmethod
    def parse(cls, value: "str | Stage") -> "Stage":
        try:
            return value if isinstance(value, Stage) else cls(str(value).strip().upper())
        except ValueError as exc:
            raise ConfigurationError(f"Unknown stage '{value}'. Use one of {[s.value for s in cls]}") from exc


@dataclass(frozen=True)
class PipelineContext:
    """Identifies the calling pipeline run. Created by the host pipeline (or an adapter)."""

    pipeline_id: str
    pipeline_run_id: str
    dataset: str
    stage: Stage | str = Stage.SILVER
    platform: str = "SPARK"
    runtime: str = "PYSPARK"
    ruleset_version: str | None = None
    integration_mode: IntegrationMode | str | None = None
    environment: str = "dev"
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.pipeline_id or not self.pipeline_run_id or not self.dataset:
            raise ConfigurationError("PipelineContext requires pipeline_id, pipeline_run_id and dataset")
        object.__setattr__(self, "stage", Stage.parse(self.stage))
        object.__setattr__(self, "platform", str(self.platform).upper())
        object.__setattr__(self, "runtime", str(self.runtime).upper())
        if self.integration_mode is not None:
            object.__setattr__(self, "integration_mode", IntegrationMode.parse(self.integration_mode))
        object.__setattr__(self, "attributes", dict(self.attributes))

    def with_overrides(self, **changes: Any) -> "PipelineContext":
        values = self.to_dict()
        values.update(changes)
        return PipelineContext(**values)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["stage"] = self.stage.value if isinstance(self.stage, Stage) else self.stage
        mode = self.integration_mode
        data["integration_mode"] = mode.value if isinstance(mode, IntegrationMode) else mode
        return data


@dataclass
class ExecutionContext:
    """Engine-side context for one DQ execution (one dataset, one rule set version)."""

    run_id: str
    pipeline: PipelineContext
    mode: IntegrationMode
    started_at: str
    ruleset_id: str
    ruleset_version: str
    ruleset_checksum: str
    policy_id: str
    policy_version: str
    confidence_threshold: float
    engine_version: str
    runtime: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["pipeline"] = self.pipeline.to_dict()
        data["mode"] = self.mode.value
        return data


__all__ = ["IntegrationMode", "Stage", "PipelineContext", "ExecutionContext"]
