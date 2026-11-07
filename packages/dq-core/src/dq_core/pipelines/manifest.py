"""Pipeline onboarding manifest (``config/pipelines/*.yaml``)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from dq_core.context import IntegrationMode, Stage
from dq_core.metadata.serialization import load_document
from dq_core.runtime.errors import ConfigurationError
from dq_core.runtime.utils import resolve_path, sha256_of

SUPPORTED_PLATFORMS = ("DATABRICKS", "STANDALONE_SPARK", "DATAPROC", "FABRIC", "AIRFLOW", "ADF", "PYTHON", "OTHER")
SUPPORTED_RUNTIMES = ("PYSPARK", "SQL", "PYTHON", "STREAMING")
INTEGRATION_MODES = ("INLINE", "ORCHESTRATED", "CLI", "REST")
FORMATS = ("PARQUET", "CSV", "DELTA", "JSON", "TABLE")
_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{2,99}$")


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    format: str
    stages: tuple[Stage, ...]
    rulesets: Mapping[str, str] = field(default_factory=dict)   # stage -> rule set file
    test_data: str | None = None
    test_data_format: str = "CSV"


@dataclass(frozen=True)
class PipelineManifest:
    pipeline_id: str
    pipeline_name: str
    platform: str
    runtime: str
    integration_mode: str
    dq_mode: IntegrationMode
    datasets: tuple[DatasetSpec, ...]
    owner: str = ""
    description: str = ""
    source_code: str | None = None
    policy: str | None = None
    manifest_path: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    def checksum(self) -> str:
        return sha256_of(self.raw)

    def dataset(self, dataset_id: str) -> DatasetSpec:
        for d in self.datasets:
            if d.dataset_id == dataset_id:
                return d
        raise KeyError(dataset_id)

    @classmethod
    def load(cls, path: str | Path) -> "PipelineManifest":
        path = Path(path)
        return cls.from_dict(load_document(path), manifest_path=str(path.resolve()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], manifest_path: str | None = None) -> "PipelineManifest":
        body = data.get("pipeline", data)
        base_dir = str(Path(manifest_path).parent) if manifest_path else None
        errors: list[str] = []
        pipeline_id = str(body.get("pipeline_id", ""))
        if not _ID_RE.match(pipeline_id):
            errors.append("pipeline_id is required (letters, digits, '_', '-', '.')")
        platform = str(body.get("platform", "")).upper()
        if platform not in SUPPORTED_PLATFORMS:
            errors.append(f"platform must be one of {SUPPORTED_PLATFORMS}")
        runtime = str(body.get("runtime", "PYSPARK")).upper()
        if runtime not in SUPPORTED_RUNTIMES:
            errors.append(f"runtime must be one of {SUPPORTED_RUNTIMES}")
        integration = str(body.get("integration_mode", "INLINE")).upper()
        if integration not in INTEGRATION_MODES:
            errors.append(f"integration_mode must be one of {INTEGRATION_MODES}")
        datasets: list[DatasetSpec] = []
        for i, ds in enumerate(body.get("datasets") or []):
            fmt = str(ds.get("format", "PARQUET")).upper()
            if not ds.get("dataset_id"):
                errors.append(f"datasets[{i}].dataset_id is required")
                continue
            if fmt not in FORMATS:
                errors.append(f"datasets[{i}].format must be one of {FORMATS}")
            try:
                stages = tuple(Stage.parse(s) for s in ds.get("stages") or [])
            except ConfigurationError as exc:
                errors.append(f"datasets[{i}]: {exc}")
                stages = ()
            rulesets = {str(k).upper(): resolve_path(v, base_dir) for k, v in (ds.get("rulesets") or {}).items()}
            for stage_name, ruleset_path in rulesets.items():
                if ruleset_path and not Path(ruleset_path).exists():
                    errors.append(f"datasets[{i}] rule set for {stage_name} not found: {ruleset_path}")
            datasets.append(DatasetSpec(dataset_id=str(ds["dataset_id"]), format=fmt, stages=stages, rulesets=rulesets,
                                        test_data=resolve_path(ds.get("test_data"), base_dir),
                                        test_data_format=str(ds.get("test_data_format", "CSV")).upper()))
        if not datasets:
            errors.append("at least one dataset is required")
        try:
            dq_mode = IntegrationMode.parse(body.get("dq_mode", "OBSERVE_ONLY"))
        except ConfigurationError as exc:
            errors.append(str(exc))
            dq_mode = IntegrationMode.OBSERVE_ONLY
        if errors:
            raise ConfigurationError("Invalid pipeline manifest: " + "; ".join(errors))
        return cls(pipeline_id=pipeline_id, pipeline_name=str(body.get("pipeline_name", pipeline_id)), platform=platform,
                   runtime=runtime, integration_mode=integration, dq_mode=dq_mode, datasets=tuple(datasets),
                   owner=str(body.get("owner", "")), description=str(body.get("description", "")),
                   source_code=body.get("source_code"), policy=resolve_path(body.get("policy"), base_dir),
                   manifest_path=manifest_path, raw=dict(body))
