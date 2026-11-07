"""Source adapters (CSV, Parquet, JSON, table) built on standard ``spark.read`` APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pyspark.sql import DataFrame

from dq_core.runtime.errors import ConfigurationError

CSV_DEFAULTS = {
    "header": "true",
    "inferSchema": "false",            # Bronze keeps raw text; typing happens after validation
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "mode": "PERMISSIVE",
    "encoding": "UTF-8",
}


@dataclass(frozen=True)
class SourceSpec:
    format: str
    path: str
    options: Mapping[str, Any] = field(default_factory=dict)
    schema: str | None = None   # optional DDL string

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceSpec":
        return cls(format=str(data.get("format", "PARQUET")).upper(), path=str(data["path"]),
                   options=dict(data.get("options") or {}), schema=data.get("schema"))


class CsvSource:
    def __init__(self, path: str, options: Mapping[str, Any] | None = None, schema: str | None = None) -> None:
        self.path, self.options, self.schema = path, {**CSV_DEFAULTS, **(options or {})}, schema

    def read(self, spark: Any) -> DataFrame:
        reader = spark.read.options(**{k: str(v) for k, v in self.options.items()})
        if self.schema:
            reader = reader.schema(self.schema)
        return reader.csv(self.path)


class ParquetSource:
    def __init__(self, path: str, options: Mapping[str, Any] | None = None) -> None:
        self.path, self.options = path, dict(options or {})

    def read(self, spark: Any) -> DataFrame:
        return spark.read.options(**{k: str(v) for k, v in self.options.items()}).parquet(self.path)


def read_source(spark: Any, spec: SourceSpec) -> DataFrame:
    fmt = spec.format.upper()
    if fmt == "CSV":
        return CsvSource(spec.path, spec.options, spec.schema).read(spark)
    if fmt == "PARQUET":
        return ParquetSource(spec.path, spec.options).read(spark)
    if fmt == "JSON":
        return spark.read.options(**{k: str(v) for k, v in spec.options.items()}).json(spec.path)
    if fmt in ("DELTA", "TABLE"):
        return spark.read.table(spec.path) if fmt == "TABLE" else spark.read.format("delta").load(spec.path)
    raise ConfigurationError(f"Unsupported source format '{spec.format}'")
