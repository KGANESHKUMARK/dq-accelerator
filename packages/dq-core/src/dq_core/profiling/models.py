"""Profile report models (JSON serialisable)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dq_core.runtime.utils import to_jsonable


@dataclass
class ColumnProfile:
    name: str
    data_type: str
    inferred_type: str
    non_null_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_is_approximate: bool
    unique_count: int | None = None           # values that occur exactly once
    duplicate_value_count: int | None = None  # non-null values that repeat (non_null - distinct)
    min: Any = None
    max: Any = None
    mean: float | None = None
    stddev: float | None = None
    percentiles: dict[str, float] = field(default_factory=dict)
    negative_count: int | None = None
    outlier_count: int | None = None
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    blank_count: int | None = None
    whitespace_count: int | None = None
    numeric_castable_percentage: float | None = None
    integer_castable_percentage: float | None = None
    email_match_percentage: float | None = None
    uppercase_percentage: float | None = None
    date_format_matches: dict[str, float] = field(default_factory=dict)
    dominant_date_format: str | None = None
    date_min: str | None = None
    date_max: str | None = None
    top_values: list[dict[str, Any]] = field(default_factory=list)
    top_patterns: list[dict[str, Any]] = field(default_factory=list)
    candidate_allowed_values: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProfileReport:
    dataset: str
    profiled_at: str
    duration_ms: float
    mode: str
    sampled: bool
    sample_fraction: float | None
    approximate: bool
    row_count: int
    source_row_count: int | None
    column_count: int
    schema: list[dict[str, Any]]
    columns: dict[str, ColumnProfile]
    duplicate_rows: int
    duplicate_groups: int
    primary_key_candidates: list[str]
    near_key_candidates: list[dict[str, Any]]
    anomalies: list[dict[str, Any]]
    schema_anomalies: list[dict[str, Any]]
    spark_jobs: int = 0
    notes: list[str] = field(default_factory=list)

    def column(self, name: str) -> ColumnProfile:
        return self.columns[name]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["columns"] = {k: v.to_dict() for k, v in self.columns.items()}
        return to_jsonable(data)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def write_json(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")
        return path

    def summary_text(self) -> str:
        lines = [f"Profile: {self.dataset}  rows={self.row_count}  columns={self.column_count}  mode={self.mode}"
                 f"{' (sampled ' + str(self.sample_fraction) + ')' if self.sampled else ''}",
                 f"  duplicate rows={self.duplicate_rows} groups={self.duplicate_groups}  "
                 f"primary-key candidates={self.primary_key_candidates or '-'}"]
        header = f"  {'COLUMN':<18} {'TYPE':<10} {'INFERRED':<22} {'NULLS':>6} {'NULL%':>6} {'DISTINCT':>8} {'UNIQUE':>6}  TOP PATTERN"
        lines += [header, "  " + "-" * (len(header) - 2)]
        for c in self.columns.values():
            pattern = c.top_patterns[0]["pattern"] if c.top_patterns else "-"
            lines.append(f"  {c.name:<18} {c.data_type:<10} {c.inferred_type[:22]:<22} {c.null_count:>6} "
                         f"{c.null_percentage:>6.1f} {c.distinct_count:>8} {str(c.unique_count if c.unique_count is not None else '-'):>6}  {pattern}")
        if self.anomalies:
            lines.append("  Anomalies / suspicious distributions:")
            lines += [f"   - [{a['type']}] {a['column']}: {a['detail']}" for a in self.anomalies]
        if self.schema_anomalies:
            lines.append("  Schema anomalies:")
            lines += [f"   - [{a['type']}] {a['column']}: {a['detail']}" for a in self.schema_anomalies]
        return "\n".join(lines)
