"""Registry factory — selects the storage backend from engine config.

``registry.backend`` in engine.yaml:
  * ``file``   (default) — JSON files under rulesets_path / pipelines_path
  * ``sqlite`` — embedded SQL store at db_path (multi-process / multi-user safe)

The SAME registry interface is returned either way, so engine/CLI/UI code is
backend-agnostic. A PostgreSQL backend plugs in here later without touching
callers — that is the production store; SQLite is the embedded equivalent.
"""

from __future__ import annotations

from pathlib import Path

from dq_core.pipelines.registry import PipelineRegistry
from dq_core.rules.registry import RuleSetRegistry
from dq_core.runtime.config import RegistrySettings
from dq_core.runtime.errors import ConfigurationError


def _backend(registry: RegistrySettings) -> str:
    backend = (registry.backend or "file").lower()
    if backend not in ("file", "sqlite", "nosql"):
        raise ConfigurationError(f"Unsupported registry.backend '{registry.backend}' (expected file|sqlite|nosql)")
    if backend == "sqlite" and not registry.db_path:
        raise ConfigurationError("registry.backend=sqlite requires registry.db_path")
    return backend


def _doc_store(registry: RegistrySettings):
    from dq_core.metadata.store import open_metadata_store
    return open_metadata_store(registry)


def build_ruleset_registry(registry: RegistrySettings) -> RuleSetRegistry | None:
    """Return a RuleSetRegistry (file/sqlite/nosql) per config, or None if unconfigured."""
    backend = _backend(registry)
    if backend == "sqlite":
        from dq_core.registries.sql_store import SqlRuleSetRegistry
        return SqlRuleSetRegistry(registry.db_path)
    if backend == "nosql":
        from dq_core.registries.doc_store import DocRuleSetRegistry
        return DocRuleSetRegistry(_doc_store(registry))
    if not registry.rulesets_path:
        return None
    return RuleSetRegistry(Path(registry.rulesets_path))


def build_pipeline_registry(registry: RegistrySettings) -> PipelineRegistry | None:
    """Return a PipelineRegistry (file/sqlite/nosql) per config, or None if unconfigured."""
    backend = _backend(registry)
    if backend == "sqlite":
        from dq_core.registries.sql_store import SqlPipelineRegistry
        return SqlPipelineRegistry(registry.db_path)
    if backend == "nosql":
        from dq_core.registries.doc_store import DocPipelineRegistry
        return DocPipelineRegistry(_doc_store(registry))
    if not registry.pipelines_path:
        return None
    return PipelineRegistry(Path(registry.pipelines_path))
