"""YAML / JSON (de)serialisation of rule sets, lookups and other metadata documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from dq_core.metadata.models import RuleSet
from dq_core.runtime.errors import ConfigurationError, RuleDefinitionError
from dq_core.runtime.utils import resolve_path, to_jsonable


def load_document(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise ConfigurationError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        if path.suffix.lower() == ".json":
            return json.load(handle)
        return yaml.safe_load(handle) or {}


def dump_yaml(data: Any, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(to_jsonable(data), handle, sort_keys=False, allow_unicode=True, width=120)
    return path


def dump_json(data: Any, path: str | Path, indent: int = 2) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(to_jsonable(data), handle, indent=indent, ensure_ascii=False)
        handle.write("\n")
    return path


def load_lookup(path: str | Path) -> dict[str, Any]:
    """Load an approved lookup table document: ``lookup: {name, approved, mapping: {...}}``."""
    doc = load_document(path)
    lookup = doc.get("lookup", doc)
    if not isinstance(lookup.get("mapping"), Mapping) or not lookup["mapping"]:
        raise RuleDefinitionError(f"Lookup file {path} must define a non-empty 'mapping'")
    return {
        "name": lookup.get("name", Path(path).stem),
        "version": str(lookup.get("version", "1.0")),
        "approved": lookup.get("approved") is True,
        "approved_by": lookup.get("approved_by", ""),
        "approved_at": str(lookup.get("approved_at", "")),
        "description": lookup.get("description", ""),
        "mapping": {str(k): str(v) for k, v in lookup["mapping"].items()},
    }


def _resolve_lookups(ruleset_doc: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for name, ref in (ruleset_doc.get("lookups") or {}).items():
        if isinstance(ref, str):
            resolved[name] = load_lookup(resolve_path(ref, base_dir))
        elif isinstance(ref, Mapping):
            resolved[name] = dict(ref)
        else:
            raise RuleDefinitionError(f"Lookup '{name}' must be a file path or an inline mapping")
    return resolved


def load_ruleset(path: str | Path) -> RuleSet:
    path = Path(path)
    doc = load_document(path)
    body = dict(doc.get("ruleset", doc))
    body["lookups"] = _resolve_lookups(body, path.resolve().parent)
    return RuleSet.from_dict(body, source_path=str(path.resolve()))


def save_ruleset(ruleset: RuleSet, path: str | Path) -> Path:
    return dump_yaml({"ruleset": ruleset.to_dict()}, path)
