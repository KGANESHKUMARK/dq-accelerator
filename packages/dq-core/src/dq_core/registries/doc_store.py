"""NoSQL-backed registries — same behaviour as file/SQLite registries, document-store persistence.

Subclasses override ONLY the storage seams declared on the file registries; every
guarantee is unchanged: immutable published ruleset versions, checksum verification
on read, append-only history, the pipeline lifecycle state machine, offboarding
that retains data/history.

Selected via engine.yaml: ``registry.backend: nosql`` with ``metadata.db_path``
(embedded TinyDB JSON file) or ``metadata.engine: mongodb`` + ``metadata.uri``.
YAML remains the authoring/export format — these registries are the runtime store.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dq_core.metadata.models import RuleSet
from dq_core.metadata.store import MetadataStore, TinyMetadataStore
from dq_core.pipelines.registry import PipelineRegistry
from dq_core.rules.registry import RuleSetRegistry


class DocRuleSetRegistry(RuleSetRegistry):
    """RuleSetRegistry persisting to the NoSQL metadata store. Drop-in replacement."""

    def __init__(self, store: MetadataStore) -> None:
        self.store = store
        self.root = Path("nosql://registry/rulesets")  # diagnostic label only

    def _load_index(self, ruleset_id: str) -> dict[str, Any]:
        doc = self.store.collection("ruleset_index").find_one({"ruleset_id": ruleset_id})
        return doc["index"] if doc else {
            "ruleset_id": ruleset_id, "versions": {}, "active_version": None, "history": []}

    def _save_index(self, ruleset_id: str, index: dict[str, Any]) -> None:
        self.store.collection("ruleset_index").replace_one(
            {"ruleset_id": ruleset_id}, {"ruleset_id": ruleset_id, "index": index}, upsert=True)

    def _read_version_payload(self, ruleset_id: str, version: str) -> dict[str, Any]:
        doc = self.store.collection("ruleset_versions").find_one(
            {"ruleset_id": ruleset_id, "version": version})
        if doc is None:
            from dq_core.runtime.errors import RuleSetNotFoundError
            raise RuleSetNotFoundError(f"{ruleset_id}@{version} version payload missing from NoSQL registry")
        return doc["payload"]

    def _write_version_payload(self, ruleset: RuleSet, checksum: str) -> None:
        self.store.collection("ruleset_versions").insert_one({
            "ruleset_id": ruleset.ruleset_id, "version": ruleset.version,
            "payload": {"ruleset": ruleset.to_dict(), "checksum": checksum}})

    def _list_ids(self) -> list[str]:
        return sorted(d["ruleset_id"] for d in self.store.collection("ruleset_index").find())


class DocPipelineRegistry(PipelineRegistry):
    """PipelineRegistry persisting to the NoSQL metadata store. Drop-in replacement."""

    def __init__(self, store: MetadataStore) -> None:
        self.store = store
        self.root = Path("nosql://registry/pipelines")

    def _read_entry(self, pipeline_id: str) -> dict[str, Any]:
        doc = self.store.collection("pipelines").find_one({"pipeline_id": pipeline_id})
        return doc["entry"] if doc else {}

    def _entry_exists(self, pipeline_id: str) -> bool:
        return self.store.collection("pipelines").find_one({"pipeline_id": pipeline_id}) is not None

    def _write_entry(self, entry: dict[str, Any]) -> None:
        pid = entry["pipeline_id"]
        self.store.collection("pipelines").replace_one(
            {"pipeline_id": pid}, {"pipeline_id": pid, "entry": entry}, upsert=True)

    def _iter_ids(self) -> list[str]:
        return sorted(d["pipeline_id"] for d in self.store.collection("pipelines").find())
