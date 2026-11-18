"""Run metrics recorder — persists per-run quality summaries for trend analysis.

Primary sink: ``metrics`` collection in the NoSQL metadata store (when the
registry backend is a document store). Fallback: append-only JSONL at
``<reports_path>/metrics.jsonl`` — works for every backend.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from dq_core.runtime.utils import utc_now_iso


def record_run_metrics(store: Any, reports_path: str | None, summary: Mapping[str, Any]) -> None:
    """Persist one run summary. Best-effort — never raises."""
    doc = {"recorded_at": utc_now_iso(), **dict(summary)}
    try:
        if store is not None and hasattr(store, "collection"):
            store.collection("metrics").insert_one(doc)
            return
    except Exception:
        pass
    try:
        path = Path(reports_path or "reports") / "metrics.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, default=str) + "\n")
    except Exception:
        pass
