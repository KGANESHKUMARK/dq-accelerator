"""DQ job runner — SQLite-persisted store + threads + SSE progress.

Single-user POC but production-shaped: jobs persist to SQLite so history survives
restarts and multi-process readers see the same state. In-process threads execute
jobs because the SparkSession itself lives in-process (cannot be shared across
uvicorn workers — true scale-out = one job-runner process per cluster behind a
queue). SSE streams live progress so the UI shows real-time updates.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "jobs.db"
DB.parent.mkdir(parents=True, exist_ok=True)

_lock = threading.Lock()
_conn = sqlite3.connect(DB, check_same_thread=False)
_conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY, kind TEXT, status TEXT, submitted_at TEXT,
    started_at TEXT, finished_at TEXT, progress TEXT, result TEXT, error TEXT)""")
_conn.commit()

_events: dict[str, list[dict[str, Any]]] = {}  # job_id -> SSE event queue
_econd = threading.Condition()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _emit(job_id: str, event: str, data: dict[str, Any]) -> None:
    with _econd:
        _events.setdefault(job_id, []).append({"event": event, "data": data})
        _econd.notify_all()


def _update(job_id: str, **fields: Any) -> None:
    with _lock:
        cols = ", ".join(f"{k}=?" for k in fields)
        _conn.execute(f"UPDATE jobs SET {cols} WHERE job_id=?",
                      (*[json.dumps(v) if isinstance(v, (dict, list)) else v for v in fields.values()], job_id))
        _conn.commit()


def submit(kind: str, fn: Callable[[], dict[str, Any]]) -> str:
    job_id = f"JOB-{uuid.uuid4().hex[:10].upper()}"
    with _lock:
        _conn.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)",
                      (job_id, kind, "QUEUED", _now(), None, None, "queued", None, None))
        _conn.commit()
    _emit(job_id, "status", {"status": "QUEUED", "progress": "queued"})

    def _run() -> None:
        _update(job_id, status="RUNNING", started_at=_now(), progress="starting Spark / engine")
        _emit(job_id, "status", {"status": "RUNNING", "progress": "starting Spark / engine"})
        try:
            result = fn()
            _update(job_id, status="COMPLETED", finished_at=_now(), progress="done", result=result)
            _emit(job_id, "status", {"status": "COMPLETED", "progress": "done", "result": result})
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}"
            _update(job_id, status="FAILED", finished_at=_now(), progress="failed", error=err)
            _emit(job_id, "status", {"status": "FAILED", "progress": "failed", "error": err})

    threading.Thread(target=_run, daemon=True, name=f"dq-job-{job_id}").start()
    return job_id


def get(job_id: str) -> dict[str, Any] | None:
    with _lock:
        row = _conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        return None
    keys = ["job_id", "kind", "status", "submitted_at", "started_at",
            "finished_at", "progress", "result", "error"]
    j = dict(zip(keys, row))
    for k in ("result",):
        if j.get(k):
            j[k] = json.loads(j[k])
    return j


def list_jobs(limit: int = 20) -> list[dict[str, Any]]:
    with _lock:
        rows = _conn.execute(
            "SELECT job_id,kind,status,submitted_at,finished_at,progress FROM jobs "
            "ORDER BY submitted_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(zip(("job_id", "kind", "status", "submitted_at",
                      "finished_at", "progress"), r)) for r in rows]


def stream(job_id: str, timeout: float = 0.5) -> Iterator[str]:
    """Yield SSE events for a job; replays already-emitted events on subscribe."""
    seen = 0
    yield ": connected\n\n"
    # Jobs finished before this process started have no event queue — close
    # immediately with a synthetic terminal event so subscribers can fetch the row.
    row0 = get(job_id)
    if row0 and row0["status"] in ("COMPLETED", "FAILED") and not _events.get(job_id):
        yield f"event: status\ndata: {json.dumps({'status': row0['status'], 'progress': row0.get('progress') or 'done'})}\n\n"
        return
    while True:
        with _econd:
            evs = _events.get(job_id, [])
            new = evs[seen:]
            if not new:
                _econd.wait(timeout)
            seen = len(evs)
        for e in new:
            yield f"event: {e['event']}\ndata: {json.dumps(e['data'])}\n\n"
            if e["data"].get("status") in ("COMPLETED", "FAILED"):
                return
        if not new:
            yield ": hb\n\n"  # heartbeat
            if get(job_id) is None:
                return


def progress_cb(job_id: str, msg: str) -> None:
    _update(job_id, progress=msg)
    _emit(job_id, "progress", {"progress": msg})


# ------------------------------------------------------------------- Spark/engine singletons
def get_spark():
    if not hasattr(get_spark, "_session"):
        from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session
        from dq_core.runtime.config import EngineConfig
        cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
        rt = cfg.runtime
        get_spark._session = create_local_spark_session(LocalSparkSettings(
            app_name="dq-accelerator-ui",
            master=rt.spark_master,
            driver_memory=rt.driver_memory,
            shuffle_partitions=rt.shuffle_partitions,
            scratch_dir=str(ROOT / ".tmp")))
        get_spark._session.sparkContext.setLogLevel("ERROR")
    return get_spark._session


def get_engine():
    if not hasattr(get_engine, "_engine"):
        from dq_core.engine import DataQualityEngine
        from dq_core.runtime.config import EngineConfig
        get_engine._engine = DataQualityEngine(
            EngineConfig.from_yaml(ROOT / "config" / "engine.yaml"))
    return get_engine._engine


def reload_engine(restart_spark: bool = False) -> dict:
    """Drop the cached engine (and optionally the Spark session) so next call
    re-reads config/engine.yaml — enables hot config reload from Settings."""
    if hasattr(get_engine, "_engine"):
        delattr(get_engine, "_engine")
    spark_restarted = False
    if restart_spark and hasattr(get_spark, "_session"):
        try:
            get_spark._session.stop()
        except Exception:
            pass
        delattr(get_spark, "_session")
        spark_restarted = True
    # warm the new engine so callers get a ready provider
    return {"reloaded": True, "spark_restarted": spark_restarted,
            "provider": type(get_engine().ai_provider).__name__}
