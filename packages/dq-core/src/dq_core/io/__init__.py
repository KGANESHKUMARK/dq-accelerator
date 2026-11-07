"""Source and sink adapters (CSV, Parquet, local / URI storage)."""

from dq_core.io.sinks import StoragePaths, StorageSink
from dq_core.io.sources import CsvSource, ParquetSource, SourceSpec, read_source

__all__ = ["StoragePaths", "StorageSink", "CsvSource", "ParquetSource", "SourceSpec", "read_source"]
