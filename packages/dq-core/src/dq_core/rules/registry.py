"""Local file-based rule set registry with immutable published versions.

Layout (``registry.rulesets_path``)::

    <root>/<ruleset_id>/index.json            versions, checksums, active version, audit history
    <root>/<ruleset_id>/versions/<v>.json     canonical published content (read-only file)

Guarantees:
* a published version can never be overwritten with different content (ImmutableVersionError);
* stored content is checksum-verified on every read (IntegrityError on tampering);
* rule set files used directly from disk are verified against the published checksum;
* rollback = activating an earlier published version (history is append-only).
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import replace
from pathlib import Path
from typing import Any

from dq_core.metadata.models import RuleSet, RuleSetStatus, RuleStatus, version_key
from dq_core.metadata.serialization import dump_json
from dq_core.rules.diff import RuleSetDiff, diff_rulesets
from dq_core.runtime.errors import (
    ImmutableVersionError,
    IntegrityError,
    RuleDefinitionError,
    RuleSetNotFoundError,
)
from dq_core.runtime.utils import utc_now_iso


class RuleSetRegistry:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    # ------------------------------------------------------------------ paths / index
    def _dir(self, ruleset_id: str) -> Path:
        return self.root / ruleset_id

    def _index_path(self, ruleset_id: str) -> Path:
        return self._dir(ruleset_id) / "index.json"

    def _version_path(self, ruleset_id: str, version: str) -> Path:
        return self._dir(ruleset_id) / "versions" / f"{version}.json"

    def _load_index(self, ruleset_id: str) -> dict[str, Any]:
        path = self._index_path(ruleset_id)
        if not path.exists():
            return {"ruleset_id": ruleset_id, "versions": {}, "active_version": None, "history": []}
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _save_index(self, ruleset_id: str, index: dict[str, Any]) -> None:
        dump_json(index, self._index_path(ruleset_id))

    # ------------------------------------------------------------- storage seams
    # The four methods below are the ONLY raw-persistence entry points (besides the
    # index load/save above). Override them to move the registry onto a different
    # backend (e.g. SqlRuleSetRegistry) while keeping every behavioural guarantee
    # (immutability, checksum verify, append-only history) identical.
    def _read_version_payload(self, ruleset_id: str, version: str) -> dict[str, Any]:
        with open(self._version_path(ruleset_id, version), "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_version_payload(self, ruleset: RuleSet, checksum: str) -> None:
        path = self._version_path(ruleset.ruleset_id, ruleset.version)
        path.parent.mkdir(parents=True, exist_ok=True)
        dump_json({"ruleset": ruleset.to_dict(), "checksum": checksum}, path)
        os.chmod(path, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)  # read-only on disk

    def _list_ids(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(p.name for p in self.root.iterdir() if (p / "index.json").exists())

    # ------------------------------------------------------------------ publish
    def publish(self, ruleset: RuleSet, published_by: str = "unknown", activate: bool = False) -> dict[str, Any]:
        """Publish an immutable version. Idempotent for identical content."""
        candidates = [r.rule_id for r in ruleset.rules if r.status == RuleStatus.CANDIDATE]
        if candidates:
            raise RuleDefinitionError(f"Cannot publish CANDIDATE rules (approve or reject them first): {candidates}")
        index = self._load_index(ruleset.ruleset_id)
        checksum = ruleset.checksum()
        existing = index["versions"].get(ruleset.version)
        if existing:
            if existing["checksum"] != checksum:
                raise ImmutableVersionError(
                    f"{ruleset.ref} is already published with checksum {existing['checksum'][:12]}; the supplied "
                    f"content has checksum {checksum[:12]}. Published versions are immutable - create a new version.")
            return existing
        published = replace(ruleset, status=RuleSetStatus.PUBLISHED)
        self._write_version_payload(published, checksum)
        entry = {"version": ruleset.version, "checksum": checksum, "published_at": utc_now_iso(),
                 "published_by": published_by, "source_path": ruleset.source_path,
                 "rule_count": len(ruleset.rules), "parent_version": ruleset.parent_version}
        index["dataset"] = ruleset.dataset
        index["versions"][ruleset.version] = entry
        index["history"].append({"event": "PUBLISHED", "version": ruleset.version, "at": entry["published_at"],
                                 "by": published_by, "checksum": checksum})
        self._save_index(ruleset.ruleset_id, index)
        if activate or index.get("active_version") is None:
            self.activate(ruleset.ruleset_id, ruleset.version, activated_by=published_by,
                          reason="initial activation" if not activate else "activated on publish")
        return entry

    # ------------------------------------------------------------------ read
    def get(self, ruleset_id: str, version: str | None = None) -> RuleSet:
        index = self._load_index(ruleset_id)
        if not index["versions"]:
            raise RuleSetNotFoundError(f"RuleSet '{ruleset_id}' is not registered in {self.root}")
        version = version or index.get("active_version")
        if version in (None, "active"):
            version = index.get("active_version")
        entry = index["versions"].get(version)
        if entry is None:
            raise RuleSetNotFoundError(f"{ruleset_id}@{version} is not published (available: {sorted(index['versions'])})")
        stored = self._read_version_payload(ruleset_id, version)
        ruleset = RuleSet.from_dict(stored["ruleset"])
        actual = ruleset.checksum()
        if actual != entry["checksum"] or actual != stored.get("checksum"):
            raise IntegrityError(f"{ruleset_id}@{version} failed checksum verification "
                                 f"(expected {entry['checksum'][:12]}, found {actual[:12]})")
        return ruleset

    def is_published(self, ruleset_id: str, version: str) -> bool:
        return version in self._load_index(ruleset_id)["versions"]

    def published_checksum(self, ruleset_id: str, version: str) -> str | None:
        entry = self._load_index(ruleset_id)["versions"].get(version)
        return entry["checksum"] if entry else None

    def verify(self, ruleset: RuleSet) -> bool:
        """Verify that ``ruleset`` (e.g. loaded from a YAML file) matches its published version.

        Returns True when the version is published and unchanged, False when it is not published.
        Raises ImmutableVersionError when the published version was modified after publication.
        """
        expected = self.published_checksum(ruleset.ruleset_id, ruleset.version)
        if expected is None:
            return False
        actual = ruleset.checksum()
        if actual != expected:
            raise ImmutableVersionError(
                f"{ruleset.ref} was modified after publication (published checksum {expected[:12]}, file checksum "
                f"{actual[:12]}). Published versions are immutable - create a new version instead.")
        return True

    # ------------------------------------------------------------------ activation / rollback
    def activate(self, ruleset_id: str, version: str, activated_by: str = "unknown", reason: str = "") -> None:
        index = self._load_index(ruleset_id)
        if version not in index["versions"]:
            raise RuleSetNotFoundError(f"Cannot activate unpublished version {ruleset_id}@{version}")
        previous = index.get("active_version")
        index["active_version"] = version
        index["history"].append({"event": "ACTIVATED", "version": version, "previous_version": previous,
                                 "at": utc_now_iso(), "by": activated_by, "reason": reason})
        self._save_index(ruleset_id, index)

    def rollback(self, ruleset_id: str, to_version: str, rolled_back_by: str = "unknown", reason: str = "") -> None:
        self.activate(ruleset_id, to_version, activated_by=rolled_back_by, reason=f"ROLLBACK: {reason}".strip())

    def active_version(self, ruleset_id: str) -> str | None:
        return self._load_index(ruleset_id).get("active_version")

    # ------------------------------------------------------------------ listing / diff
    def list_rulesets(self) -> list[str]:
        return self._list_ids()

    def list_versions(self, ruleset_id: str) -> list[dict[str, Any]]:
        index = self._load_index(ruleset_id)
        active = index.get("active_version")
        versions = sorted(index["versions"].values(), key=lambda e: version_key(e["version"]))
        return [{**e, "active": e["version"] == active} for e in versions]

    def history(self, ruleset_id: str) -> list[dict[str, Any]]:
        return list(self._load_index(ruleset_id)["history"])

    def diff(self, ruleset_id: str, from_version: str, to_version: str) -> RuleSetDiff:
        return diff_rulesets(self.get(ruleset_id, from_version), self.get(ruleset_id, to_version))
