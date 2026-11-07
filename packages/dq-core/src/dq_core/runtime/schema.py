"""Schema utilities (pure Python where possible so they can be unit-tested without Spark)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from dq_core.runtime.errors import ConfigurationError

DQ_COLUMN_PREFIX = "_dq_"

_CAST_TYPE_RE = re.compile(
    r"^(string|int|integer|bigint|long|smallint|short|tinyint|byte|double|float|real|boolean|bool|"
    r"date|timestamp|timestamp_ntz|decimal\(\s*\d{1,2}\s*,\s*\d{1,2}\s*\))$",
    re.IGNORECASE,
)
_TYPE_ALIASES = {
    "integer": "int", "long": "bigint", "short": "smallint", "byte": "tinyint", "real": "float",
    "bool": "boolean",
}


def validate_cast_type(type_name: str) -> str:
    """Return a normalised Spark SQL type name from an allow-list (prevents SQL injection)."""
    normalised = re.sub(r"\s+", "", str(type_name).strip().lower())
    if not _CAST_TYPE_RE.match(normalised):
        raise ConfigurationError(f"Unsupported data type '{type_name}'. Allowed: string, int, bigint, smallint, "
                                 "tinyint, double, float, boolean, date, timestamp, timestamp_ntz, decimal(p,s)")
    return _TYPE_ALIASES.get(normalised, normalised)


def normalize_type_name(type_name: str) -> str:
    """Normalise a type string for comparison (``integer`` -> ``int``, removes whitespace)."""
    normalised = re.sub(r"\s+", "", str(type_name).strip().lower())
    return _TYPE_ALIASES.get(normalised, normalised)


def quote_identifier(name: str) -> str:
    """Quote a column name for use in Spark SQL expressions."""
    return "`" + str(name).replace("`", "``") + "`"


def is_dq_column(name: str) -> bool:
    return name.startswith(DQ_COLUMN_PREFIX)


def user_columns(columns: Iterable[str]) -> list[str]:
    return [c for c in columns if not is_dq_column(c)]


def schema_to_list(schema: Any) -> list[dict[str, Any]]:
    """Serialise a Spark StructType into ``[{name, type, nullable}]``."""
    return [{"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable} for f in schema.fields]


@dataclass
class SchemaComparison:
    missing_columns: list[str] = field(default_factory=list)
    unexpected_columns: list[str] = field(default_factory=list)
    type_mismatches: list[dict[str, str]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not (self.missing_columns or self.unexpected_columns or self.type_mismatches)

    def reasons(self) -> list[str]:
        reasons = [f"missing column '{c}'" for c in self.missing_columns]
        reasons += [f"unexpected column '{c}'" for c in self.unexpected_columns]
        reasons += [f"column '{m['column']}' has type {m['actual']} (expected {m['expected']})"
                    for m in self.type_mismatches]
        return reasons

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "missing_columns": self.missing_columns,
                "unexpected_columns": self.unexpected_columns, "type_mismatches": self.type_mismatches}


def compare_schema(expected: Sequence[dict[str, Any]], actual: Sequence[dict[str, Any]], *,
                   allow_extra_columns: bool = False, check_types: bool = True,
                   case_sensitive: bool = False) -> SchemaComparison:
    """Compare an expected column list with an actual one (both ``[{name, type}]``)."""
    def key(name: str) -> str:
        return name if case_sensitive else name.lower()

    actual_map = {key(c["name"]): c for c in actual if not is_dq_column(c["name"])}
    expected_map = {key(c["name"]): c for c in expected}
    result = SchemaComparison()
    for k, col in expected_map.items():
        if k not in actual_map:
            result.missing_columns.append(col["name"])
        elif check_types and col.get("type"):
            exp_t, act_t = normalize_type_name(col["type"]), normalize_type_name(actual_map[k]["type"])
            if exp_t != act_t:
                result.type_mismatches.append({"column": col["name"], "expected": exp_t, "actual": act_t})
    if not allow_extra_columns:
        result.unexpected_columns = [c["name"] for k, c in actual_map.items() if k not in expected_map]
    return result


def missing_columns(required: Iterable[str], available: Iterable[str]) -> list[str]:
    available_lower = {c.lower() for c in available}
    return [c for c in required if c and c.lower() not in available_lower]
