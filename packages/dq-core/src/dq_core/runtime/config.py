"""Engine configuration (``config/engine.yaml``)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from dq_core.context import IntegrationMode
from dq_core.runtime.errors import ConfigurationError
from dq_core.runtime.utils import resolve_path

PROFILING_MODES = ("FULL", "SAMPLE", "APPROXIMATE")


@dataclass
class ProfilingSettings:
    mode: str = "FULL"
    sample_fraction: float = 0.1
    sample_seed: int = 42
    max_allowed_values: int = 20
    top_values: int = 10
    top_patterns: int = 5
    exact_distinct_row_limit: int = 5_000_000
    high_null_rate: float = 0.2
    dominant_value_rate: float = 0.9
    outlier_stddevs: float = 3.0
    semantic_match_rate: float = 0.8

    def __post_init__(self) -> None:
        self.mode = str(self.mode).upper()
        if self.mode not in PROFILING_MODES:
            raise ConfigurationError(f"profiling.mode must be one of {PROFILING_MODES}")
        if not 0 < float(self.sample_fraction) <= 1:
            raise ConfigurationError("profiling.sample_fraction must be in (0, 1]")


@dataclass
class ValidationSettings:
    cache_intermediate: bool = True
    collect_failure_examples: bool = True


@dataclass
class RegistrySettings:
    backend: str = "file"                      # "file" (default) or "sqlite"
    db_path: str | None = None                 # sqlite file when backend="sqlite"
    rulesets_path: str | None = None
    pipelines_path: str | None = None
    enforce_published_immutability: bool = True
    require_published_for_enforcement: bool = True
    enforce_pipeline_lifecycle: bool = False


@dataclass
class AISettings:
    provider: str = "mock"
    mock_responses_path: str | None = None
    similarity_fallback: bool = True


@dataclass
class OutputSettings:
    base_path: str | None = None
    reports_path: str | None = None
    format: str = "parquet"
    write_mode: str = "overwrite"


@dataclass
class EngineConfig:
    name: str = "dq-accelerator"
    default_mode: IntegrationMode = IntegrationMode.OBSERVE_ONLY
    include_dq_columns_in_accepted: bool = False
    registry: RegistrySettings = field(default_factory=RegistrySettings)
    profiling: ProfilingSettings = field(default_factory=ProfilingSettings)
    validation: ValidationSettings = field(default_factory=ValidationSettings)
    ai: AISettings = field(default_factory=AISettings)
    outputs: OutputSettings = field(default_factory=OutputSettings)
    logging_level: str = "INFO"
    logging_json: bool = True
    config_path: str | None = None

    @property
    def base_dir(self) -> str | None:
        return str(Path(self.config_path).resolve().parent) if self.config_path else None

    @classmethod
    def from_yaml(cls, path: str | Path) -> "EngineConfig":
        path = Path(path)
        if not path.exists():
            raise ConfigurationError(f"Engine configuration not found: {path}")
        with open(path, "r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle) or {}
        return cls.from_dict(document.get("engine", document), config_path=str(path.resolve()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], config_path: str | None = None) -> "EngineConfig":
        base_dir = str(Path(config_path).resolve().parent) if config_path else None
        try:
            registry = dict(data.get("registry") or {})
            for key in ("rulesets_path", "pipelines_path", "db_path"):
                registry[key] = resolve_path(registry.get(key), base_dir)
            ai = dict(data.get("ai") or {})
            ai["mock_responses_path"] = resolve_path(ai.get("mock_responses_path"), base_dir)
            outputs = dict(data.get("outputs") or {})
            for key in ("base_path", "reports_path"):
                outputs[key] = resolve_path(outputs.get(key), base_dir)
            logging_cfg = data.get("logging") or {}
            return cls(
                name=data.get("name", "dq-accelerator"),
                default_mode=IntegrationMode.parse(data.get("default_mode", "OBSERVE_ONLY")),
                include_dq_columns_in_accepted=bool(data.get("include_dq_columns_in_accepted", False)),
                registry=RegistrySettings(**registry),
                profiling=ProfilingSettings(**(data.get("profiling") or {})),
                validation=ValidationSettings(**(data.get("validation") or {})),
                ai=AISettings(**ai),
                outputs=OutputSettings(**outputs),
                logging_level=str(logging_cfg.get("level", "INFO")),
                logging_json=bool(logging_cfg.get("json", True)),
                config_path=config_path,
            )
        except TypeError as exc:
            raise ConfigurationError(f"Invalid engine configuration: {exc}") from exc

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["default_mode"] = self.default_mode.value
        return data
