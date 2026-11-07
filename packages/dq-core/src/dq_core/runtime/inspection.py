"""Runtime inspection: which Spark, which platform, which Python/JVM."""

from __future__ import annotations

import os
import platform
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

from dq_core.version import __version__

# Environment hints used for platform detection. Databricks sets DATABRICKS_RUNTIME_VERSION on
# every cluster; the Dataproc / Fabric hints are documented assumptions verified only by contract
# tests (see docs/pipeline_adapters.md).
PLATFORM_ENV_HINTS: dict[str, tuple[str, ...]] = {
    "DATABRICKS": ("DATABRICKS_RUNTIME_VERSION",),
    "DATAPROC": ("DATAPROC_IMAGE_VERSION", "DATAPROC_CLUSTER_NAME"),
    "FABRIC": ("FABRIC_RUNTIME_VERSION", "MSFABRIC_RUNTIME_VERSION"),
}


def detect_platform(environ: dict[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    for name, hints in PLATFORM_ENV_HINTS.items():
        if any(env.get(h) for h in hints):
            return name
    return "STANDALONE"


@dataclass
class RuntimeInfo:
    engine_version: str
    python_version: str
    os: str
    platform: str
    cpu_count: int | None
    spark_version: str | None = None
    spark_master: str | None = None
    spark_connect: bool | None = None
    ansi_enabled: str | None = None
    session_timezone: str | None = None
    java_version: str | None = None
    databricks_runtime_version: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _conf(spark: Any, key: str) -> str | None:
    try:
        return spark.conf.get(key)
    except Exception:
        return None


def inspect_runtime(spark: Any | None = None) -> RuntimeInfo:
    info = RuntimeInfo(
        engine_version=__version__,
        python_version=sys.version.split()[0],
        os=f"{platform.system()} {platform.release()} ({platform.machine()})",
        platform=detect_platform(),
        cpu_count=os.cpu_count(),
        databricks_runtime_version=os.environ.get("DATABRICKS_RUNTIME_VERSION"),
    )
    if spark is not None:
        info.spark_version = spark.version
        info.spark_master = _conf(spark, "spark.master")
        info.spark_connect = type(spark).__module__.startswith("pyspark.sql.connect")
        info.ansi_enabled = _conf(spark, "spark.sql.ansi.enabled")
        info.session_timezone = _conf(spark, "spark.sql.session.timeZone")
        if not info.spark_connect:
            try:  # Classic Spark only; JVM access is not available over Spark Connect.
                info.java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
            except Exception:
                info.java_version = None
    return info
