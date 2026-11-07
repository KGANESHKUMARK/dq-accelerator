"""Metadata model: RuleDefinition, RemediationPolicy and RuleSet.

These are pure-Python dataclasses (no PySpark import) so rule management, versioning and
governance tooling can run without a Spark runtime.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from dq_core.context import Stage
from dq_core.runtime.errors import RuleDefinitionError
from dq_core.runtime.schema import validate_cast_type
from dq_core.runtime.utils import sha256_of


class RuleType(str, Enum):
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    DUPLICATE = "DUPLICATE"
    REGEX = "REGEX"
    RANGE = "RANGE"
    ALLOWED_VALUES = "ALLOWED_VALUES"
    DATA_TYPE = "DATA_TYPE"
    DATE_FORMAT = "DATE_FORMAT"
    STRING_LENGTH = "STRING_LENGTH"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
    SCHEMA_VALIDATION = "SCHEMA_VALIDATION"
    ROW_COUNT = "ROW_COUNT"
    RECONCILIATION = "RECONCILIATION"


class RuleScope(str, Enum):
    ROW_LEVEL = "ROW_LEVEL"
    DATASET_LEVEL = "DATASET_LEVEL"
    CROSS_DATASET = "CROSS_DATASET"


class Severity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class RuleStatus(str, Enum):
    CANDIDATE = "CANDIDATE"      # generated proposal - never executed
    APPROVED = "APPROVED"        # reviewed and approved for execution
    REJECTED = "REJECTED"
    DEPRECATED = "DEPRECATED"


class RuleSetStatus(str, Enum):
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DEPRECATED = "DEPRECATED"


class DeterministicAction(str, Enum):
    TRIM = "TRIM"
    UPPERCASE = "UPPERCASE"
    LOWERCASE = "LOWERCASE"
    LOOKUP_MAPPING = "LOOKUP_MAPPING"
    DATE_STANDARDIZATION = "DATE_STANDARDIZATION"
    NULL_REPLACEMENT = "NULL_REPLACEMENT"
    DEDUPLICATE = "DEDUPLICATE"


class ConditionOperator(str, Enum):
    EQ = "EQ"
    NE = "NE"
    IN = "IN"
    NOT_IN = "NOT_IN"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"


# Default / permitted scopes per rule type.
ROW_RULE_TYPES = {
    RuleType.NOT_NULL, RuleType.UNIQUE, RuleType.DUPLICATE, RuleType.REGEX, RuleType.RANGE,
    RuleType.ALLOWED_VALUES, RuleType.DATA_TYPE, RuleType.DATE_FORMAT, RuleType.STRING_LENGTH,
    RuleType.REFERENTIAL_INTEGRITY,
}
DATASET_RULE_TYPES = {RuleType.SCHEMA_VALIDATION, RuleType.ROW_COUNT, RuleType.RECONCILIATION}
DEFAULT_SCOPE = {t: RuleScope.ROW_LEVEL for t in ROW_RULE_TYPES}
DEFAULT_SCOPE.update({
    RuleType.REFERENTIAL_INTEGRITY: RuleScope.CROSS_DATASET,
    RuleType.SCHEMA_VALIDATION: RuleScope.DATASET_LEVEL,
    RuleType.ROW_COUNT: RuleScope.DATASET_LEVEL,
    RuleType.RECONCILIATION: RuleScope.CROSS_DATASET,
})
COLUMN_REQUIRED = ROW_RULE_TYPES - {RuleType.DUPLICATE} | {RuleType.RECONCILIATION}
ALL_COLUMNS = "ALL_COLUMNS"
_RULE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{1,99}$")
_VERSION_RE = re.compile(r"^\d+(\.\d+){1,2}$")
MAX_REGEX_LENGTH = 500


def _enum(enum_cls: type[Enum], value: Any, what: str) -> Any:
    try:
        return value if isinstance(value, enum_cls) else enum_cls(str(value).strip().upper())
    except ValueError as exc:
        raise RuleDefinitionError(f"Invalid {what} '{value}'. Allowed: {[e.value for e in enum_cls]}") from exc


def version_key(version: str) -> tuple[int, ...]:
    if not _VERSION_RE.match(str(version)):
        raise RuleDefinitionError(f"Invalid version '{version}' (expected e.g. 1.0 or 1.1.0)")
    return tuple(int(p) for p in str(version).split("."))


def _clean(value: Any) -> Any:
    """Deep-copy plain data, normalising tuples to lists for stable serialisation."""
    if isinstance(value, Mapping):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return copy.deepcopy(value)


@dataclass(frozen=True)
class RemediationStep:
    action: DeterministicAction
    params: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | str) -> "RemediationStep":
        if isinstance(data, str):
            return cls(action=_enum(DeterministicAction, data, "remediation action"))
        params = {k: v for k, v in data.items() if k != "action"}
        return cls(action=_enum(DeterministicAction, data.get("action"), "remediation action"), params=_clean(params))

    def to_dict(self) -> dict[str, Any]:
        return {"action": self.action.value, **_clean(self.params)}

    def describe(self) -> str:
        if self.action == DeterministicAction.LOOKUP_MAPPING:
            return f"LOOKUP_MAPPING({self.params.get('lookup')})"
        if self.action == DeterministicAction.NULL_REPLACEMENT:
            return f"NULL_REPLACEMENT({self.params.get('value')!r})"
        return self.action.value


@dataclass(frozen=True)
class AIRemediationSettings:
    enabled: bool = False
    auto_apply: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "auto_apply": self.auto_apply}


@dataclass(frozen=True)
class RemediationPolicy:
    """Per-rule remediation policy: ordered deterministic steps, then optional AI assistance."""

    deterministic: tuple[RemediationStep, ...] = ()
    ai_assisted: AIRemediationSettings = field(default_factory=AIRemediationSettings)

    @property
    def has_deterministic(self) -> bool:
        return bool(self.deterministic)

    @property
    def is_remediable(self) -> bool:
        return self.has_deterministic or self.ai_assisted.enabled

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "RemediationPolicy":
        if not data:
            return cls()
        steps = tuple(RemediationStep.from_dict(s) for s in (data.get("deterministic") or []))
        ai = data.get("ai_assisted") or {}
        return cls(deterministic=steps,
                   ai_assisted=AIRemediationSettings(enabled=bool(ai.get("enabled", False)),
                                                     auto_apply=bool(ai.get("auto_apply", False))))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.deterministic:
            data["deterministic"] = [s.to_dict() for s in self.deterministic]
        if self.ai_assisted.enabled:
            data["ai_assisted"] = self.ai_assisted.to_dict()
        return data


@dataclass(frozen=True)
class RuleCondition:
    """Structured 'applies when' condition (no free-form expressions are ever evaluated)."""

    column: str
    operator: ConditionOperator
    values: tuple[Any, ...] = ()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RuleCondition":
        values = data.get("values", data.get("value", ()))
        if not isinstance(values, (list, tuple)):
            values = (values,)
        cond = cls(column=str(data.get("column", "")), operator=_enum(ConditionOperator, data.get("operator"), "condition operator"),
                   values=tuple(values))
        if not cond.column:
            raise RuleDefinitionError("condition.column is required")
        if cond.operator in (ConditionOperator.IN, ConditionOperator.NOT_IN, ConditionOperator.EQ, ConditionOperator.NE) and not cond.values:
            raise RuleDefinitionError(f"condition operator {cond.operator.value} requires values")
        return cond

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"column": self.column, "operator": self.operator.value}
        if self.values:
            data["values"] = list(self.values)
        return data


@dataclass(frozen=True)
class RuleDefinition:
    rule_id: str
    name: str
    rule_type: RuleType
    dataset: str
    column: str | None = None
    description: str = ""
    scope: RuleScope | None = None
    severity: Severity = Severity.ERROR
    stage: Stage = Stage.SILVER
    config: Mapping[str, Any] = field(default_factory=dict)
    remediation: RemediationPolicy = field(default_factory=RemediationPolicy)
    version: str = "1.0"
    status: RuleStatus = RuleStatus.APPROVED
    condition: RuleCondition | None = None
    tags: tuple[str, ...] = ()
    generation: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_type", _enum(RuleType, self.rule_type, "rule_type"))
        object.__setattr__(self, "severity", _enum(Severity, self.severity, "severity"))
        object.__setattr__(self, "status", _enum(RuleStatus, self.status, "rule status"))
        object.__setattr__(self, "stage", Stage.parse(self.stage))
        scope = self.scope if self.scope is not None else DEFAULT_SCOPE[self.rule_type]
        object.__setattr__(self, "scope", _enum(RuleScope, scope, "scope"))
        object.__setattr__(self, "config", _clean(self.config or {}))
        object.__setattr__(self, "tags", tuple(self.tags or ()))
        self.validate()

    # ------------------------------------------------------------------ validation
    def validate(self) -> None:
        def fail(msg: str) -> None:
            raise RuleDefinitionError(f"Rule {self.rule_id}: {msg}")

        if not _RULE_ID_RE.match(self.rule_id or ""):
            fail("rule_id must start with a letter and contain only letters, digits, '_', '-', '.'")
        if not self.name:
            fail("name is required")
        if not self.dataset:
            fail("dataset is required")
        version_key(self.version)
        if self.rule_type in COLUMN_REQUIRED and not self.column:
            fail(f"{self.rule_type.value} requires 'column'")
        if self.rule_type in DATASET_RULE_TYPES and self.scope == RuleScope.ROW_LEVEL:
            fail(f"{self.rule_type.value} is a dataset-level rule and cannot have ROW_LEVEL scope")
        if self.rule_type in ROW_RULE_TYPES and self.scope == RuleScope.DATASET_LEVEL:
            fail(f"{self.rule_type.value} flags individual records and cannot have DATASET_LEVEL scope")
        cfg = self.config
        t = self.rule_type
        if t == RuleType.REGEX:
            pattern = cfg.get("pattern")
            if not pattern or len(str(pattern)) > MAX_REGEX_LENGTH:
                fail(f"REGEX requires config.pattern (max {MAX_REGEX_LENGTH} chars)")
            try:
                re.compile(str(pattern))
            except re.error as exc:
                fail(f"invalid regex pattern: {exc}")
        elif t == RuleType.RANGE:
            if cfg.get("min") is None and cfg.get("max") is None:
                fail("RANGE requires config.min and/or config.max")
            data_type = str(cfg.get("data_type", "double"))
            if data_type.lower() != "date":
                validate_cast_type(data_type)
                for bound in ("min", "max"):
                    if cfg.get(bound) is not None:
                        try:
                            float(cfg[bound])
                        except (TypeError, ValueError):
                            fail(f"RANGE {bound} must be numeric for data_type {data_type}")
            else:
                for bound in ("min", "max"):
                    if cfg.get(bound) is not None:
                        try:
                            datetime.strptime(str(cfg[bound]), "%Y-%m-%d")
                        except ValueError:
                            fail(f"RANGE {bound} must be an ISO date (yyyy-MM-dd) for data_type date")
        elif t == RuleType.ALLOWED_VALUES:
            values = cfg.get("values")
            if not isinstance(values, list) or not values:
                fail("ALLOWED_VALUES requires a non-empty config.values list")
        elif t == RuleType.DATA_TYPE:
            if not cfg.get("data_type"):
                fail("DATA_TYPE requires config.data_type")
            validate_cast_type(cfg["data_type"])
        elif t == RuleType.DATE_FORMAT:
            if not cfg.get("format"):
                fail("DATE_FORMAT requires config.format (Spark datetime pattern, e.g. yyyy-MM-dd)")
        elif t == RuleType.STRING_LENGTH:
            if cfg.get("min") is None and cfg.get("max") is None:
                fail("STRING_LENGTH requires config.min and/or config.max")
        elif t == RuleType.DUPLICATE:
            keys = cfg.get("keys")
            if not keys:
                fail(f"DUPLICATE requires config.keys (list of business keys or '{ALL_COLUMNS}')")
        elif t == RuleType.REFERENTIAL_INTEGRITY:
            if not cfg.get("reference_dataset") or not cfg.get("reference_column"):
                fail("REFERENTIAL_INTEGRITY requires config.reference_dataset and config.reference_column")
        elif t == RuleType.SCHEMA_VALIDATION:
            if not isinstance(cfg.get("expected_columns"), list) or not cfg["expected_columns"]:
                fail("SCHEMA_VALIDATION requires config.expected_columns")
        elif t == RuleType.ROW_COUNT:
            if cfg.get("min") is None and cfg.get("max") is None:
                fail("ROW_COUNT requires config.min and/or config.max")
        elif t == RuleType.RECONCILIATION:
            for key in ("reference_dataset", "reference_column"):
                if not cfg.get(key):
                    fail(f"RECONCILIATION requires config.{key}")
        for step in self.remediation.deterministic:
            self._validate_step(step)

    def _validate_step(self, step: RemediationStep) -> None:
        def fail(msg: str) -> None:
            raise RuleDefinitionError(f"Rule {self.rule_id}: remediation {step.action.value}: {msg}")

        p = step.params
        if step.action == DeterministicAction.LOOKUP_MAPPING and not p.get("lookup"):
            fail("requires 'lookup' (name of an approved lookup table)")
        if step.action == DeterministicAction.DATE_STANDARDIZATION:
            if not p.get("input_formats") or not p.get("output_format"):
                fail("requires 'input_formats' and 'output_format'")
        if step.action == DeterministicAction.NULL_REPLACEMENT:
            if "value" not in p:
                fail("requires 'value'")
            if p.get("approved") is not True:
                fail("null replacement must be explicitly approved (approved: true, approved_by: ...)")
        if step.action == DeterministicAction.DEDUPLICATE:
            if self.rule_type != RuleType.DUPLICATE:
                fail("DEDUPLICATE is only valid on DUPLICATE rules")
            survivor = p.get("survivor") or {}
            keys = self.config.get("keys")
            if not survivor:
                fail("requires an explicit survivor-selection policy ('survivor')")
            if survivor.get("strategy") == "ANY_IDENTICAL":
                if keys != ALL_COLUMNS:
                    fail(f"survivor strategy ANY_IDENTICAL is only allowed when keys == {ALL_COLUMNS}")
            elif not survivor.get("order_by"):
                fail("survivor policy requires 'order_by' (or strategy ANY_IDENTICAL for exact duplicates)")

    # ------------------------------------------------------------------ helpers
    @property
    def is_row_level(self) -> bool:
        return self.rule_type in ROW_RULE_TYPES

    @property
    def level(self) -> str:
        return "ROW" if self.is_row_level else "DATASET"

    def key_columns(self, available: Sequence[str]) -> list[str]:
        """Columns used by DUPLICATE (business keys) or the single rule column."""
        if self.rule_type == RuleType.DUPLICATE:
            keys = self.config.get("keys")
            if keys == ALL_COLUMNS:
                return [c for c in available if not c.startswith("_dq_")]
            return list(keys)
        return [self.column] if self.column else []

    def referenced_columns(self, available: Sequence[str]) -> list[str]:
        cols = self.key_columns(available) if self.is_row_level else ([self.column] if self.column else [])
        if self.condition is not None:
            cols.append(self.condition.column)
        return [c for c in cols if c]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], defaults: Mapping[str, Any] | None = None) -> "RuleDefinition":
        defaults = defaults or {}
        try:
            return cls(
                rule_id=str(data["rule_id"]),
                name=str(data.get("name") or data["rule_id"]),
                rule_type=data["rule_type"],
                dataset=str(data.get("dataset") or defaults.get("dataset", "")),
                column=data.get("column"),
                description=str(data.get("description", "")),
                scope=data.get("scope"),
                severity=data.get("severity", "ERROR"),
                stage=data.get("stage") or defaults.get("stage", "SILVER"),
                config=data.get("config") or {},
                remediation=RemediationPolicy.from_dict(data.get("remediation")),
                version=str(data.get("version") or defaults.get("version", "1.0")),
                status=data.get("status", "APPROVED"),
                condition=RuleCondition.from_dict(data["condition"]) if data.get("condition") else None,
                tags=tuple(data.get("tags") or ()),
                generation=_clean(data["generation"]) if data.get("generation") else None,
            )
        except KeyError as exc:
            raise RuleDefinitionError(f"Rule definition missing required field {exc}: {dict(data)}") from exc

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "dataset": self.dataset,
            "column": self.column,
            "rule_type": self.rule_type.value,
            "scope": self.scope.value if self.scope else None,
            "severity": self.severity.value,
            "stage": self.stage.value,
            "config": _clean(self.config),
            "remediation": self.remediation.to_dict(),
            "version": self.version,
            "status": self.status.value,
        }
        if self.condition is not None:
            data["condition"] = self.condition.to_dict()
        if self.tags:
            data["tags"] = list(self.tags)
        if self.generation:
            data["generation"] = _clean(self.generation)
        return data


@dataclass(frozen=True)
class RuleSet:
    """A versioned, reviewable collection of rules for one dataset and stage."""

    ruleset_id: str
    dataset: str
    version: str
    rules: tuple[RuleDefinition, ...]
    stage: Stage = Stage.SILVER
    status: RuleSetStatus = RuleSetStatus.DRAFT
    description: str = ""
    owner: str = ""
    record_key: tuple[str, ...] = ()
    lookups: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    parent_version: str | None = None
    created_by: str = ""
    created_at: str = ""
    approved_by: str = ""
    approved_at: str = ""
    change_log: tuple[str, ...] = ()
    source_path: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", Stage.parse(self.stage))
        object.__setattr__(self, "status", _enum(RuleSetStatus, self.status, "ruleset status"))
        object.__setattr__(self, "rules", tuple(self.rules))
        object.__setattr__(self, "record_key", tuple(self.record_key or ()))
        object.__setattr__(self, "change_log", tuple(self.change_log or ()))
        object.__setattr__(self, "lookups", _clean(self.lookups or {}))
        self.validate()

    def validate(self) -> None:
        if not _RULE_ID_RE.match(self.ruleset_id or ""):
            raise RuleDefinitionError(f"Invalid ruleset_id '{self.ruleset_id}'")
        version_key(self.version)
        seen: set[str] = set()
        for rule in self.rules:
            if rule.rule_id in seen:
                raise RuleDefinitionError(f"RuleSet {self.ruleset_id}: duplicate rule_id {rule.rule_id}")
            seen.add(rule.rule_id)
            for step in rule.remediation.deterministic:
                if step.action == DeterministicAction.LOOKUP_MAPPING:
                    name = step.params.get("lookup")
                    if name not in self.lookups:
                        raise RuleDefinitionError(f"Rule {rule.rule_id}: lookup '{name}' is not defined in ruleset.lookups")
                    if self.lookups[name].get("approved") is not True:
                        raise RuleDefinitionError(f"Lookup '{name}' is not approved (approved: true required)")
        if self.status == RuleSetStatus.PUBLISHED:
            unapproved = [r.rule_id for r in self.rules if r.status == RuleStatus.CANDIDATE]
            if unapproved:
                raise RuleDefinitionError(f"Published RuleSet cannot contain CANDIDATE rules: {unapproved}")

    # ------------------------------------------------------------------ queries
    @property
    def ref(self) -> str:
        return f"{self.ruleset_id}@{self.version}"

    def executable_rules(self) -> list[RuleDefinition]:
        """Only explicitly APPROVED rules are ever executed."""
        return [r for r in self.rules if r.status == RuleStatus.APPROVED]

    def skipped_rules(self) -> list[dict[str, str]]:
        return [{"rule_id": r.rule_id, "reason": f"rule status {r.status.value} (only APPROVED rules execute)"}
                for r in self.rules if r.status != RuleStatus.APPROVED]

    def get_rule(self, rule_id: str) -> RuleDefinition:
        for rule in self.rules:
            if rule.rule_id == rule_id:
                return rule
        raise KeyError(rule_id)

    # ------------------------------------------------------------------ serialisation
    def content_dict(self) -> dict[str, Any]:
        """Everything that defines behaviour - used for the immutable checksum."""
        return {
            "ruleset_id": self.ruleset_id,
            "dataset": self.dataset,
            "version": self.version,
            "stage": self.stage.value,
            "description": self.description,
            "owner": self.owner,
            "record_key": list(self.record_key),
            "lookups": _clean(self.lookups),
            "parent_version": self.parent_version,
            "rules": [r.to_dict() for r in self.rules],
        }

    def checksum(self) -> str:
        return sha256_of(self.content_dict())

    def to_dict(self) -> dict[str, Any]:
        data = self.content_dict()
        data.update({
            "status": self.status.value,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
            "change_log": list(self.change_log),
        })
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], source_path: str | None = None) -> "RuleSet":
        data = data.get("ruleset", data)
        defaults = {"dataset": data.get("dataset"), "stage": data.get("stage", "SILVER")}
        try:
            return cls(
                ruleset_id=str(data["ruleset_id"]),
                dataset=str(data["dataset"]),
                version=str(data["version"]),
                stage=data.get("stage", "SILVER"),
                status=data.get("status", "DRAFT"),
                description=str(data.get("description", "")),
                owner=str(data.get("owner", "")),
                record_key=tuple(data.get("record_key") or ()),
                lookups=data.get("lookups") or {},
                parent_version=data.get("parent_version"),
                created_by=str(data.get("created_by", "")),
                created_at=str(data.get("created_at", "")),
                approved_by=str(data.get("approved_by", "")),
                approved_at=str(data.get("approved_at", "")),
                change_log=tuple(data.get("change_log") or ()),
                rules=tuple(RuleDefinition.from_dict(r, defaults) for r in data.get("rules") or []),
                source_path=source_path,
            )
        except KeyError as exc:
            raise RuleDefinitionError(f"RuleSet definition missing required field {exc}") from exc

    # ------------------------------------------------------------------ versioning
    def derive(self, new_version: str, *, add: Iterable[RuleDefinition | Mapping[str, Any]] = (),
               remove: Iterable[str] = (), modify: Mapping[str, Mapping[str, Any]] | None = None,
               description: str | None = None, created_by: str = "", change_log: Iterable[str] = ()) -> "RuleSet":
        """Create a NEW draft version. The source version is never mutated."""
        if version_key(new_version) <= version_key(self.version):
            raise RuleDefinitionError(f"New version {new_version} must be greater than {self.version}")
        remove = set(remove)
        modify = dict(modify or {})
        missing = (remove | set(modify)) - {r.rule_id for r in self.rules}
        if missing:
            raise RuleDefinitionError(f"Cannot modify/remove unknown rules: {sorted(missing)}")
        rules: list[RuleDefinition] = []
        for rule in self.rules:
            if rule.rule_id in remove:
                continue
            if rule.rule_id in modify:
                data = rule.to_dict()
                for key, value in modify[rule.rule_id].items():
                    if key == "config" and isinstance(value, Mapping):
                        data["config"] = {**data["config"], **_clean(value)}
                    else:
                        data[key] = _clean(value)
                data["version"] = new_version
                rule = RuleDefinition.from_dict(data)
            rules.append(rule)
        for item in add:
            new_rule = item if isinstance(item, RuleDefinition) else RuleDefinition.from_dict(item, {"dataset": self.dataset, "stage": self.stage.value})
            rules.append(replace(new_rule, version=new_version))
        return replace(self, version=new_version, rules=tuple(rules), status=RuleSetStatus.DRAFT,
                       parent_version=self.version, description=description or self.description,
                       created_by=created_by, approved_by="", approved_at="", change_log=tuple(change_log),
                       source_path=None)
