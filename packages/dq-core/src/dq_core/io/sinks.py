"""Storage sink adapter: DataFrames via ``DataFrame.write`` (any Hadoop-compatible URI) and JSON
reports via Python for local / FUSE paths (``/Volumes``, ``/dbfs``) or via Spark for URIs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from dq_core.runtime.compat import get_spark
from dq_core.runtime.utils import is_uri, join_path, to_jsonable


@dataclass(frozen=True)
class StoragePaths:
    """Medallion layout under a configurable base path (local folder, Volume or cloud URI)."""

    base_path: str

    def layer(self, name: str, *parts: str) -> str:
        return join_path(self.base_path, name, *parts)

    def bronze(self, dataset: str) -> str:
        return self.layer("bronze", dataset)

    def silver(self, dataset: str) -> str:
        return self.layer("silver", dataset)

    def gold(self, dataset: str) -> str:
        return self.layer("gold", dataset)

    def quarantine(self, stage: str, dataset: str) -> str:
        return self.layer("quarantine", stage.lower(), dataset)

    def audit(self, *parts: str) -> str:
        return self.layer("audit", *parts)


class StorageSink:
    def __init__(self, fmt: str = "parquet", mode: str = "overwrite") -> None:
        self.format, self.mode = fmt, mode

    def write(self, df: DataFrame, path: str, *, fmt: str | None = None, mode: str | None = None,
              partition_by: list[str] | None = None) -> str:
        writer = df.write.format(fmt or self.format).mode(mode or self.mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return path

    @staticmethod
    def write_json(obj: Any, path: str, spark_df_for_uri: DataFrame | None = None) -> str:
        text = json.dumps(to_jsonable(obj), indent=2, default=str)
        if is_uri(path) and not path.startswith("file:"):
            if spark_df_for_uri is None:
                raise ValueError("A DataFrame (for its SparkSession) is required to write JSON to a URI")
            spark = get_spark(spark_df_for_uri)
            spark.createDataFrame([(text,)], "value string").coalesce(1).write.mode("overwrite").text(path)
            return path
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text + "\n", encoding="utf-8")
        return str(target)
