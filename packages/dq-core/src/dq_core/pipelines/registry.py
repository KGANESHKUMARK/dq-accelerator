"""Lightweight local pipeline registry with an explicit lifecycle state machine.

States: DRAFT -> REGISTERED -> TESTED -> ACTIVE <-> PAUSED -> DEACTIVATING -> OFFBOARDED

Offboarding disables the DQ integration only. Customer data, quarantine outputs, reports and the
pipeline's execution history are never deleted; the registry entry is retained for audit.
"""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

from dq_core.metadata.serialization import dump_json
from dq_core.pipelines.manifest import PipelineManifest
from dq_core.runtime.errors import LifecycleError
from dq_core.runtime.utils import utc_now_iso


class PipelineState(str, Enum):
    DRAFT = "DRAFT"
    REGISTERED = "REGISTERED"
    TESTED = "TESTED"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DEACTIVATING = "DEACTIVATING"
    OFFBOARDED = "OFFBOARDED"


TRANSITIONS: dict[PipelineState, set[PipelineState]] = {
    PipelineState.DRAFT: {PipelineState.REGISTERED},
    PipelineState.REGISTERED: {PipelineState.TESTED, PipelineState.DEACTIVATING},
    PipelineState.TESTED: {PipelineState.TESTED, PipelineState.ACTIVE, PipelineState.DEACTIVATING},
    PipelineState.ACTIVE: {PipelineState.PAUSED, PipelineState.DEACTIVATING},
    PipelineState.PAUSED: {PipelineState.ACTIVE, PipelineState.TESTED, PipelineState.DEACTIVATING},
    PipelineState.DEACTIVATING: {PipelineState.OFFBOARDED},
    PipelineState.OFFBOARDED: {PipelineState.DRAFT},   # re-onboarding starts a new registration
}
PASS_THROUGH_STATES = {PipelineState.PAUSED, PipelineState.DEACTIVATING, PipelineState.OFFBOARDED}


class PipelineRegistry:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, pipeline_id: str) -> Path:
        return self.root / f"{pipeline_id}.json"

    # ------------------------------------------------------------- storage seams
    # Override these four methods to move the registry to another backend
    # (e.g. SqlPipelineRegistry); the lifecycle state machine above never changes.
    def _read_entry(self, pipeline_id: str) -> dict[str, Any]:
        with open(self._path(pipeline_id), "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _entry_exists(self, pipeline_id: str) -> bool:
        return self._path(pipeline_id).exists()

    def _write_entry(self, entry: dict[str, Any]) -> None:
        dump_json(entry, self._path(entry["pipeline_id"]))

    def _iter_ids(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(p.stem for p in self.root.glob("*.json"))

    def get(self, pipeline_id: str) -> dict[str, Any]:
        if not self._entry_exists(pipeline_id):
            raise LifecycleError(f"Pipeline '{pipeline_id}' is not registered")
        return self._read_entry(pipeline_id)

    def exists(self, pipeline_id: str) -> bool:
        return self._entry_exists(pipeline_id)

    def state(self, pipeline_id: str) -> PipelineState | None:
        return PipelineState(self.get(pipeline_id)["state"]) if self.exists(pipeline_id) else None

    def list(self) -> list[dict[str, Any]]:
        entries = []
        for pid in self._iter_ids():
            data = self._read_entry(pid)
            entries.append({k: data.get(k) for k in ("pipeline_id", "pipeline_name", "platform", "runtime", "state",
                                                     "dq_mode", "updated_at")})
        return entries

    def _save(self, entry: dict[str, Any]) -> None:
        entry["updated_at"] = utc_now_iso()
        self._write_entry(entry)

    def _transition(self, entry: dict[str, Any], target: PipelineState, actor: str, reason: str) -> dict[str, Any]:
        current = PipelineState(entry["state"])
        if target not in TRANSITIONS[current]:
            raise LifecycleError(f"Pipeline {entry['pipeline_id']}: transition {current.value} -> {target.value} is not "
                                 f"allowed (allowed: {sorted(s.value for s in TRANSITIONS[current])})")
        entry["state"] = target.value
        entry.setdefault("history", []).append({"from": current.value, "to": target.value, "at": utc_now_iso(),
                                                "by": actor, "reason": reason})
        self._save(entry)
        return entry

    # ------------------------------------------------------------------ lifecycle operations
    def onboard(self, manifest: PipelineManifest, actor: str = "dqctl") -> dict[str, Any]:
        if self.exists(manifest.pipeline_id):
            entry = self.get(manifest.pipeline_id)
            if PipelineState(entry["state"]) != PipelineState.OFFBOARDED:
                raise LifecycleError(f"Pipeline {manifest.pipeline_id} is already registered (state {entry['state']})")
            self._transition(entry, PipelineState.DRAFT, actor, "re-onboarding")
        else:
            entry = {"pipeline_id": manifest.pipeline_id, "state": PipelineState.DRAFT.value, "history": [],
                     "created_at": utc_now_iso(), "executions": []}
            entry["history"].append({"from": None, "to": PipelineState.DRAFT.value, "at": utc_now_iso(), "by": actor,
                                     "reason": "manifest received"})
        entry.update({"pipeline_name": manifest.pipeline_name, "platform": manifest.platform, "runtime": manifest.runtime,
                      "integration_mode": manifest.integration_mode, "dq_mode": manifest.dq_mode.value,
                      "owner": manifest.owner, "manifest_path": manifest.manifest_path,
                      "manifest_checksum": manifest.checksum(), "manifest": dict(manifest.raw)})
        self._save(entry)
        return self._transition(entry, PipelineState.REGISTERED, actor, "manifest validated")

    def record_test(self, pipeline_id: str, results: dict[str, Any], passed: bool, actor: str = "dqctl") -> dict[str, Any]:
        entry = self.get(pipeline_id)
        entry["last_test"] = {"at": utc_now_iso(), "passed": passed, "results": results}
        if not passed:
            self._save(entry)
            raise LifecycleError(f"Pipeline {pipeline_id} integration test failed: {results}")
        return self._transition(entry, PipelineState.TESTED, actor, "integration test passed")

    def activate(self, pipeline_id: str, actor: str = "dqctl", reason: str = "activated") -> dict[str, Any]:
        return self._transition(self.get(pipeline_id), PipelineState.ACTIVE, actor, reason)

    def pause(self, pipeline_id: str, actor: str = "dqctl", reason: str = "paused") -> dict[str, Any]:
        return self._transition(self.get(pipeline_id), PipelineState.PAUSED, actor, reason)

    def offboard(self, pipeline_id: str, actor: str = "dqctl", reason: str = "offboarded",
                 retained_locations: list[str] | None = None) -> dict[str, Any]:
        entry = self._transition(self.get(pipeline_id), PipelineState.DEACTIVATING, actor, reason)
        entry["offboarding"] = {"at": utc_now_iso(), "by": actor, "reason": reason, "data_deleted": False,
                                "history_retained": True, "retained_locations": retained_locations or []}
        return self._transition(entry, PipelineState.OFFBOARDED, actor, "integration disabled; data and history retained")

    def record_execution(self, pipeline_id: str, summary: dict[str, Any]) -> None:
        if not self.exists(pipeline_id):
            return
        entry = self.get(pipeline_id)
        entry.setdefault("executions", []).append(summary)
        entry["executions"] = entry["executions"][-50:]  # keep the registry file small
        self._save(entry)
