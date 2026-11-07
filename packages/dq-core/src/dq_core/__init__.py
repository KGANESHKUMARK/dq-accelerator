"""dq-core: portable Apache Spark Data Quality Core Engine.

Public API::

    from dq_core import DataQualityEngine
    from dq_core.context import PipelineContext

Pure-metadata modules (rules, policies, registries, manifests) import without PySpark; the
engine itself is imported lazily so the CLI's metadata commands do not need a Spark runtime.
"""

from __future__ import annotations

import logging
from typing import Any

from dq_core.context import IntegrationMode, PipelineContext, Stage
from dq_core.version import __version__

logging.getLogger("dq_core").addHandler(logging.NullHandler())

__all__ = ["DataQualityEngine", "DQResult", "PipelineContext", "IntegrationMode", "Stage", "__version__"]


def __getattr__(name: str) -> Any:
    if name == "DataQualityEngine":
        from dq_core.engine import DataQualityEngine

        return DataQualityEngine
    if name == "DQResult":
        from dq_core.orchestration.result import DQResult

        return DQResult
    raise AttributeError(f"module 'dq_core' has no attribute {name!r}")
