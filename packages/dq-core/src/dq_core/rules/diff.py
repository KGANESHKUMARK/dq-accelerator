"""Version comparison between two rule sets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dq_core.metadata.models import RuleSet

_IGNORED_RULE_FIELDS = {"version"}


@dataclass
class RuleChange:
    rule_id: str
    changes: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"rule_id": self.rule_id, "changes": self.changes}


@dataclass
class RuleSetDiff:
    ruleset_id: str
    from_version: str
    to_version: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    modified: list[RuleChange] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    metadata_changes: list[dict[str, Any]] = field(default_factory=list)
    from_checksum: str = ""
    to_checksum: str = ""

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.modified or self.metadata_changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ruleset_id": self.ruleset_id,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "from_checksum": self.from_checksum,
            "to_checksum": self.to_checksum,
            "summary": {"added": len(self.added), "removed": len(self.removed),
                        "modified": len(self.modified), "unchanged": len(self.unchanged)},
            "added": self.added,
            "removed": self.removed,
            "modified": [m.to_dict() for m in self.modified],
            "unchanged": self.unchanged,
            "metadata_changes": self.metadata_changes,
        }

    def to_text(self) -> str:
        lines = [f"RuleSet {self.ruleset_id}: {self.from_version} -> {self.to_version}",
                 f"  checksum {self.from_checksum[:12]} -> {self.to_checksum[:12]}",
                 f"  added={len(self.added)} removed={len(self.removed)} modified={len(self.modified)} "
                 f"unchanged={len(self.unchanged)}"]
        lines += [f"  + {rule_id}" for rule_id in self.added]
        lines += [f"  - {rule_id}" for rule_id in self.removed]
        for change in self.modified:
            lines.append(f"  ~ {change.rule_id}")
            for c in change.changes:
                lines.append(f"      {c['field']}: {c['from']!r} -> {c['to']!r}")
        for meta in self.metadata_changes:
            lines.append(f"  * {meta['field']}: {meta['from']!r} -> {meta['to']!r}")
        return "\n".join(lines)


def _flatten(prefix: str, value: Any, out: dict[str, Any]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten(f"{prefix}.{k}" if prefix else str(k), v, out)
    else:
        out[prefix] = value


def diff_rulesets(old: RuleSet, new: RuleSet) -> RuleSetDiff:
    result = RuleSetDiff(ruleset_id=new.ruleset_id, from_version=old.version, to_version=new.version,
                         from_checksum=old.checksum(), to_checksum=new.checksum())
    old_rules = {r.rule_id: r for r in old.rules}
    new_rules = {r.rule_id: r for r in new.rules}
    result.added = [rid for rid in new_rules if rid not in old_rules]
    result.removed = [rid for rid in old_rules if rid not in new_rules]
    for rid, new_rule in new_rules.items():
        if rid not in old_rules:
            continue
        a: dict[str, Any] = {}
        b: dict[str, Any] = {}
        _flatten("", {k: v for k, v in old_rules[rid].to_dict().items() if k not in _IGNORED_RULE_FIELDS}, a)
        _flatten("", {k: v for k, v in new_rule.to_dict().items() if k not in _IGNORED_RULE_FIELDS}, b)
        changes = [{"field": key, "from": a.get(key), "to": b.get(key)}
                   for key in sorted(set(a) | set(b)) if a.get(key) != b.get(key)]
        if changes:
            result.modified.append(RuleChange(rid, changes))
        else:
            result.unchanged.append(rid)
    for key in ("description", "owner", "record_key", "stage", "dataset"):
        before, after = old.content_dict()[key], new.content_dict()[key]
        if before != after:
            result.metadata_changes.append({"field": key, "from": before, "to": after})
    if old.lookups != new.lookups:
        result.metadata_changes.append({"field": "lookups", "from": sorted(old.lookups), "to": sorted(new.lookups)})
    return result
