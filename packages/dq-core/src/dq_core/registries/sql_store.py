"""SQL-backed registries — same behaviour as the file registries, SQL persistence.

These subclasses override ONLY the storage seams declared on the file registries;
all guarantees are unchanged: immutable published ruleset versions, checksum
verification on read, append-only history, the full pipeline lifecycle state
machine, and offboarding that retains data/history.

SQLite is the embedded implementation (this POC). The seam design means a
PostgreSQL backend is a drop-in with the same interface — change the store, not
the registry contracts. Selected via engine.yaml: ``registry.backend: sqlite``
with ``registry.db_path``.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from dq_core.metadata.models import RuleSet
from dq_core.pipelines.registry import PipelineRegistry
from dq_core.rules.registry import RuleSetRegistry

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ruleset_index (
    ruleset_id TEXT PRIMARY KEY,
    index_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ruleset_versions (
    ruleset_id TEXT NOT NULL,
    version    TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (ruleset_id, version)
);
CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id TEXT PRIMARY KEY,
    entry_json TEXT NOT NULL
);
"""


class RegistryStore:
    """Shared SQLite connection with row-level JSON persistence.

    Entries are stored as canonical JSON documents keyed by id(+version) — the
    exact same shapes the file registry writes to .json files, so migration is a
    straightforward copy and every consumer sees identical data.
    """

    _cache: dict[str, "RegistryStore"] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def open(cls, db_path: str | Path) -> "RegistryStore":
        key = str(Path(db_path).resolve())
        with cls._cache_lock:
            if key not in cls._cache:
                Path(key).parent.mkdir(parents=True, exist_ok=True)
                cls._cache[key] = cls(key)
        return cls._cache[key]

    def __init__(self, db_path: str | Path) -> None:
        self.path = str(db_path)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ------------------------------------------------------------- ruleset index
    def read_index(self, ruleset_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT index_json FROM ruleset_index WHERE ruleset_id=?",
                                     (ruleset_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def write_index(self, ruleset_id: str, index: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute("INSERT INTO ruleset_index VALUES (?,?) "
                               "ON CONFLICT(ruleset_id) DO UPDATE SET index_json=excluded.index_json",
                               (ruleset_id, json.dumps(index)))
            self._conn.commit()

    def ruleset_ids(self) -> list[str]:
        with self._lock:
            return [r[0] for r in self._conn.execute(
                "SELECT ruleset_id FROM ruleset_index ORDER BY ruleset_id").fetchall()]

    # ------------------------------------------------------------- ruleset versions
    def read_version(self, ruleset_id: str, version: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM ruleset_versions "
                                     "WHERE ruleset_id=? AND version=?", (ruleset_id, version)).fetchone()
        return json.loads(row[0]) if row else None

    def write_version(self, ruleset_id: str, version: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute("INSERT INTO ruleset_versions (ruleset_id,version,payload_json) VALUES (?,?,?)",
                               (ruleset_id, version, json.dumps(payload)))
            self._conn.commit()  # PRIMARY KEY prevents overwrite -> immutability enforced at DB level too

    # ------------------------------------------------------------- pipelines
    def read_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT entry_json FROM pipelines WHERE pipeline_id=?",
                                     (pipeline_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def write_pipeline(self, entry: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute("INSERT INTO pipelines VALUES (?,?) "
                               "ON CONFLICT(pipeline_id) DO UPDATE SET entry_json=excluded.entry_json",
                               (entry["pipeline_id"], json.dumps(entry)))
            self._conn.commit()

    def pipeline_ids(self) -> list[str]:
        with self._lock:
            return [r[0] for r in self._conn.execute(
                "SELECT pipeline_id FROM pipelines ORDER BY pipeline_id").fetchall()]


class SqlRuleSetRegistry(RuleSetRegistry):
    """RuleSetRegistry persisting to SQL instead of files. Drop-in replacement."""

    def __init__(self, db_path: str | Path) -> None:
        self.store = RegistryStore.open(db_path)
        self.root = Path(db_path)  # kept for error messages / diagnostics

    def _load_index(self, ruleset_id: str) -> dict[str, Any]:
        return self.store.read_index(ruleset_id) or {
            "ruleset_id": ruleset_id, "versions": {}, "active_version": None, "history": []}

    def _save_index(self, ruleset_id: str, index: dict[str, Any]) -> None:
        self.store.write_index(ruleset_id, index)

    def _read_version_payload(self, ruleset_id: str, version: str) -> dict[str, Any]:
        payload = self.store.read_version(ruleset_id, version)
        if payload is None:
            from dq_core.runtime.errors import RuleSetNotFoundError
            raise RuleSetNotFoundError(f"{ruleset_id}@{version} version payload missing from SQL registry")
        return payload

    def _write_version_payload(self, ruleset: RuleSet, checksum: str) -> None:
        self.store.write_version(ruleset.ruleset_id, ruleset.version,
                                 {"ruleset": ruleset.to_dict(), "checksum": checksum})

    def _list_ids(self) -> list[str]:
        return self.store.ruleset_ids()


class SqlPipelineRegistry(PipelineRegistry):
    """PipelineRegistry persisting to SQL instead of files. Drop-in replacement."""

    def __init__(self, db_path: str | Path) -> None:
        self.store = RegistryStore.open(db_path)
        self.root = Path(db_path)

    def _read_entry(self, pipeline_id: str) -> dict[str, Any]:
        return self.store.read_pipeline(pipeline_id)

    def _entry_exists(self, pipeline_id: str) -> bool:
        return self.store.read_pipeline(pipeline_id) is not None

    def _write_entry(self, entry: dict[str, Any]) -> None:
        self.store.write_pipeline(entry)

    def _iter_ids(self) -> list[str]:
        return self.store.pipeline_ids()
