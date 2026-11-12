"""ODCS (Open Data Contract Standard) → DQ candidate ruleset import.

Parses an ODCS YAML/JSON contract and produces a ``RuleSet`` of CANDIDATE
rules — never executable until a steward reviews and publishes, same as
profiler-generated candidates.

Supported ODCS surface (v2/v3 common subset)::

    dataset: customers                       → ruleset.dataset
    schema:                                 → per-field rules
      - name: id
        type: string                        → DATA_TYPE (mapped to Spark type)
        required: true                      → NOT_NULL
        unique: true                        → UNIQUE
        pattern: "^[A-Z]{3}\\d+$"           → REGEX
        format: date-time                   → DATE_FORMAT (common formats)
        enum: [A, B]  /  values: [A, B]     → ALLOWED_VALUES
        minimum: 0  /  maximum: 120         → RANGE
        minLength / maxLength               → STRING_LENGTH
        pii: true                           → description flag (sensitive note)
    quality:                                → dataset-level rules (v3)
      - type: rowCount
        arguments: {min: 1000, max: 1000000}→ ROW_COUNT
      - type: nullValues / nullCount …      → NULL_RATE (best-effort mapping)

Usage::

    dqctl ruleset import-odcs contract.yaml --out customers_odcs.yaml
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping

import yaml

from dq_core.metadata.models import (RuleDefinition, RuleSet, RuleScope, RuleSetStatus,
                                     RuleStatus, RuleType, Severity)
from dq_core.metadata.serialization import dump_json
from dq_core.runtime.errors import RuleDefinitionError
from dq_core.runtime.utils import utc_now_iso

_ODCS_TO_SPARK = {
    "string": "string", "varchar": "string", "text": "string",
    "integer": "int", "int": "int", "long": "long", "bigint": "long",
    "number": "double", "float": "float", "double": "double", "decimal": "decimal(18,4)",
    "boolean": "boolean", "bool": "boolean",
    "date": "date", "datetime": "timestamp", "timestamp": "timestamp",
    "object": "string", "array": "string",
}

_DATE_FORMATS = {
    "date": ["yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "MM/dd/yyyy"],
    "date-time": ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                  "yyyy/MM/dd HH:mm:ss"],
    "time": ["HH:mm:ss", "HH:mm"],
}

_EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def load_odcs(path: str | Path) -> dict[str, Any]:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise RuleDefinitionError(f"ODCS contract must be a mapping: {path}")
    return doc


def _rule(ruleset_id: str, idx: int, name: str, column: str | None, rtype: RuleType,
          config: dict[str, Any] | None = None, severity: Severity = Severity.ERROR,
          description: str = "", scope: RuleScope | None = None,
          evidence: dict[str, Any] | None = None) -> RuleDefinition:
    return RuleDefinition(
        rule_id=f"{ruleset_id.upper().replace('-', '_')[:20]}_{idx:03d}",
        name=name, description=description or name, dataset="", column=column,
        rule_type=rtype, severity=severity, config=config or {}, scope=scope,
        status=RuleStatus.CANDIDATE, version="1.0",
        generation={"source": "odcs_contract", "evidence": evidence or {}, "confidence": 0.9,
                    "profiled_at": utc_now_iso()})


def odcs_to_ruleset(contract: Mapping[str, Any], *, ruleset_id: str | None = None,
                    version: str = "1.0", owner: str = "", stage: str = "SILVER") -> RuleSet:
    """Convert an ODCS contract document into a CANDIDATE RuleSet."""
    dataset = str(contract.get("dataset") or contract.get("name") or "odcs_dataset")
    rid = ruleset_id or f"{dataset}_odcs"
    schema = contract.get("schema") or contract.get("fields") or []
    if isinstance(schema, dict):  # some contracts nest under properties
        schema = schema.get("properties") or schema.get("fields") or []
    if not isinstance(schema, list):
        raise RuleDefinitionError("ODCS contract has no 'schema' field list")

    rules: list[RuleDefinition] = []
    n = 0
    for field_spec in schema:
        if not isinstance(field_spec, dict):
            continue
        col_name = str(field_spec.get("name") or field_spec.get("field") or "")
        if not col_name:
            continue
        n += 1
        prefix = f"{rid[:12].upper()}_{n:03d}"
        desc_bits = []
        if field_spec.get("pii"):
            desc_bits.append("PII/sensitive")
        base_ev = {"odcs_field": col_name, "odcs_type": field_spec.get("type")}

        def add(name: str, rtype: RuleType, config: dict[str, Any] | None = None,
                severity: Severity = Severity.ERROR, scope: RuleScope | None = None) -> None:
            nonlocal n
            rules.append(RuleDefinition(
                rule_id=f"{prefix}_{name.upper()[:12]}", name=f"{col_name}_{name}",
                description=f"ODCS {col_name}: {', '.join(desc_bits) or name}",
                dataset=dataset, column=col_name, rule_type=rtype, severity=severity,
                config=config or {}, scope=scope, status=RuleStatus.CANDIDATE, version="1.0",
                generation={"source": "odcs_contract", "evidence": base_ev,
                            "confidence": 0.9, "profiled_at": utc_now_iso()}))

        # type → DATA_TYPE (only when mapping is meaningful)
        odcs_type = str(field_spec.get("type") or "").lower()
        spark_type = _ODCS_TO_SPARK.get(odcs_type)
        if spark_type and odcs_type not in ("string", "object", "array"):
            add("datatype", RuleType.DATA_TYPE, {"data_type": spark_type})

        if field_spec.get("required") is True:
            add("notnull", RuleType.NOT_NULL, {"treat_blank_as_null": True})
        if field_spec.get("unique") is True:
            add("unique", RuleType.UNIQUE)

        enum_vals = field_spec.get("enum") or field_spec.get("values")
        if isinstance(enum_vals, list) and enum_vals:
            add("allowed", RuleType.ALLOWED_VALUES, {"values": [str(v) for v in enum_vals]})

        pattern = field_spec.get("pattern")
        fmt = str(field_spec.get("format") or "").lower()
        if fmt in _DATE_FORMATS:
            add("datefmt", RuleType.DATE_FORMAT, {"format": _DATE_FORMATS[fmt]})
        elif fmt == "email":
            add("regex", RuleType.REGEX, {"pattern": _EMAIL_RE})
        elif pattern:
            add("regex", RuleType.REGEX, {"pattern": str(pattern)})

        if field_spec.get("minimum") is not None or field_spec.get("maximum") is not None:
            add("range", RuleType.RANGE,
                {"min": field_spec.get("minimum"), "max": field_spec.get("maximum"),
                 "data_type": "double", "inclusive": True})
        if field_spec.get("minLength") is not None or field_spec.get("maxLength") is not None:
            add("strlen", RuleType.STRING_LENGTH,
                {"min": int(field_spec.get("minLength") or 0),
                 "max": int(field_spec.get("maxLength") or 100000)})

    # v3 quality section → dataset-level rules
    for q in contract.get("quality") or []:
        if not isinstance(q, dict):
            continue
        qtype = str(q.get("type") or "").lower()
        args = q.get("arguments") or {}
        n += 1
        if qtype == "rowcount":
            rules.append(RuleDefinition(
                rule_id=f"{rid[:12].upper()}_Q{n:03d}", name="row_count_bounds", dataset=dataset,
                rule_type=RuleType.ROW_COUNT, severity=Severity.ERROR, scope=RuleScope.DATASET_LEVEL,
                config={"min": args.get("min"), "max": args.get("max")}, status=RuleStatus.CANDIDATE,
                version="1.0",
                generation={"source": "odcs_contract", "evidence": {"odcs_quality": qtype},
                            "confidence": 0.9, "profiled_at": utc_now_iso()}))
        elif qtype in ("nullvalues", "nullrate", "completeness"):
            col_name = str(args.get("column") or q.get("column") or "")
            if col_name:
                rules.append(RuleDefinition(
                    rule_id=f"{rid[:12].upper()}_Q{n:03d}", name=f"{col_name}_null_rate", dataset=dataset,
                    column=col_name, rule_type=RuleType.NULL_RATE, severity=Severity.WARNING,
                    scope=RuleScope.DATASET_LEVEL,
                    config={"max_null_rate": float(args.get("maxNullRate", args.get("threshold", 0.05)))},
                    status=RuleStatus.CANDIDATE, version="1.0",
                    generation={"source": "odcs_contract", "evidence": {"odcs_quality": qtype},
                                "confidence": 0.85, "profiled_at": utc_now_iso()}))

    if not rules:
        raise RuleDefinitionError("ODCS contract produced zero rules — check schema/quality sections")

    return RuleSet(ruleset_id=rid, dataset=dataset, version=version, stage=stage,
                   status=RuleSetStatus.DRAFT, description=f"Imported from ODCS contract ({contract.get('apiVersion', '?')})",
                   owner=owner, rules=rules, created_at=utc_now_iso(), source_path=str(contract.get("_source", "")))


def write_ruleset_yaml(ruleset: RuleSet, out_path: str | Path) -> Path:
    """Serialize a ruleset to the canonical ruleset YAML layout."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc = {"ruleset": {
        "ruleset_id": ruleset.ruleset_id, "dataset": ruleset.dataset, "version": ruleset.version,
        "stage": ruleset.stage.value if hasattr(ruleset.stage, "value") else str(ruleset.stage),
        "status": ruleset.status.value, "description": ruleset.description,
        "owner": ruleset.owner, "created_at": ruleset.created_at,
        "record_key": list(ruleset.record_key) if ruleset.record_key else [],
        "rules": [r.to_dict() for r in ruleset.rules],
    }}
    out.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return out
