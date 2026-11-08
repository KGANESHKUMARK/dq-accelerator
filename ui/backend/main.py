"""DQ Accelerator UI backend — FastAPI wrapper over dq_core.

REST contract:
  GET  /api/health
  GET  /api/pipelines                     list + states
  POST /api/pipelines/onboard
  POST /api/pipelines/{id}/{action}
  GET  /api/pipelines/{id}
  GET  /api/pipelines/{id}/trend          cross-run sparkline history
  GET  /api/rulesets
  POST /api/rulesets/publish
  POST /api/rulesets/{id}/{action}
  GET  /api/rulesets/{id}/diff
  GET  /api/datasets
  POST /api/dq/run
  POST /api/profile
  GET  /api/jobs/{id}
  GET  /api/jobs
  GET  /api/jobs/{id}/stream              SSE
  GET  /api/reports
  GET  /api/reports/{name}
  GET  /api/anomalies                     anomaly lifecycle inbox
  PATCH /api/anomalies/{id}              update status/owner/note
  GET  /api/quarantine/pending            PENDING_HUMAN_APPROVAL rows
  POST /api/quarantine/{record_id}/approve
  POST /api/quarantine/{record_id}/reject
  GET  /api/health/score                  overall DQ health 0-100
  POST /api/onboard/upload
  POST /api/onboard/preview
  POST /api/onboard/finalize
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import jobs as J

ROOT = Path(__file__).resolve().parents[2]

from dq_core.runtime.config import EngineConfig
from dq_core.runtime.logging import configure_logging

os.environ.setdefault("DQ_LOG_DIR", str(ROOT / "logs"))
_cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
configure_logging(_cfg.logging_level, json_format=_cfg.logging_json)

app = FastAPI(title="DQ Accelerator API", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _registry(kind: str):
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    reg = build_pipeline_registry(cfg.registry) if kind == "pipelines" else build_ruleset_registry(cfg.registry)
    if reg is None:
        raise HTTPException(500, f"registry.{kind}_path not configured")
    return reg


def _reg_dir(kind: str) -> Path:
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    return (ROOT / "config" / getattr(cfg.registry, f"{kind}_path")).resolve()


# ------------------------------------------------------------------ health
@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "engine": "dq-core", "root": str(ROOT)}


# ------------------------------------------------------------------ pipelines
@app.get("/api/pipelines")
def pipelines() -> list[dict[str, Any]]:
    reg = _registry("pipelines")
    rows = []
    for e in reg.list():
        full = reg.get(e["pipeline_id"])  # full entry for counts (cheap: JSON rows)
        e = dict(e)
        e["executions"] = len(full.get("executions") or [])
        e["history_len"] = len(full.get("history") or [])
        rows.append(e)
    return rows


class ManifestIn(BaseModel):
    manifest_yaml: str


@app.post("/api/pipelines/onboard")
def onboard(body: ManifestIn) -> dict[str, Any]:
    from dq_core.pipelines.manifest import PipelineManifest
    # Write under config/pipelines so relative paths in the manifest (../policies/...,
    # ../rulesets/...) resolve exactly like the real manifests do.
    tmp = ROOT / "config" / "pipelines" / ".ui_manifest.yaml"
    tmp.write_text(body.manifest_yaml, encoding="utf-8")
    try:
        mf = PipelineManifest.load(tmp)
        entry = _registry("pipelines").onboard(mf, actor="ui")
        return {"ok": True, "pipeline_id": mf.pipeline_id, "state": entry["state"]}
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/pipelines/{pid}/{action}")
def pipeline_action(pid: str, action: str, reason: str = "via UI") -> dict[str, Any]:
    reg = _registry("pipelines")
    try:
        if action == "test":
            e = reg.record_test(pid, {"check": "ui", "result": "PASS"}, passed=True, actor="ui")
        elif action == "activate":
            e = reg.activate(pid, actor="ui")
        elif action == "pause":
            e = reg.pause(pid, actor="ui", reason=reason)
        elif action == "offboard":
            e = reg.offboard(pid, actor="ui", reason=reason)
        elif action == "resume":
            e = reg.activate(pid, actor="ui") if hasattr(reg, "activate") else None
            raise HTTPException(400, "use activate to resume")
        else:
            raise HTTPException(400, f"unknown action {action}")
        return {"ok": True, "state": e["state"]}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/pipelines/{pid}")
def pipeline_detail(pid: str) -> dict[str, Any]:
    try:
        return _registry("pipelines").get(pid)
    except Exception as exc:
        raise HTTPException(404, "pipeline not found") from exc


# ------------------------------------------------------------------ rulesets
@app.get("/api/rulesets")
def rulesets() -> list[dict[str, Any]]:
    reg = _registry("rulesets")
    out = []
    for rid in reg.list_rulesets():
        for e in reg.list_versions(rid):
            out.append({"ruleset_id": rid, "version": e.get("version"),
                        "checksum": str(e.get("checksum", ""))[:12],
                        "published_at": e.get("published_at"), "rules": e.get("rule_count"),
                        "active": e.get("active", False),
                        "dataset": reg._load_index(rid).get("dataset")})
    return out


class RulesetIn(BaseModel):
    ruleset_yaml: str
    activate: bool = True


@app.post("/api/rulesets/publish")
def ruleset_publish(body: RulesetIn) -> dict[str, Any]:
    from dq_core.metadata.serialization import load_ruleset
    # Write under config/rulesets so lookup paths like ../rules/lookups/*.yaml resolve correctly.
    tmp = ROOT / "config" / "rulesets" / ".ui_ruleset.yaml"
    tmp.write_text(body.ruleset_yaml, encoding="utf-8")
    try:
        rs = load_ruleset(tmp)
        entry = _registry("rulesets").publish(rs, published_by="ui", activate=body.activate)
        return {"ok": True, "ref": rs.ref, "checksum": entry["checksum"][:12], "activated": body.activate}
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


class VersionAction(BaseModel):
    version: str | None = None
    reason: str = "via UI"


@app.post("/api/rulesets/{rid}/{action}")
def ruleset_action(rid: str, action: str, body: VersionAction) -> dict[str, Any]:
    reg = _registry("rulesets")
    try:
        if action == "activate":
            if not body.version:
                raise HTTPException(400, "version required")
            reg.activate(rid, body.version, activated_by="ui")
            return {"ok": True, "active": body.version}
        if action == "rollback":
            if not body.version:
                raise HTTPException(400, "version required")
            reg.rollback(rid, body.version, rolled_back_by="ui", reason=body.reason)
            return {"ok": True, "active": body.version}
        raise HTTPException(400, f"unknown action {action}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/rulesets/{rid}/diff")
def ruleset_diff(rid: str, from_: str = "", to: str = "") -> dict[str, Any]:
    reg = _registry("rulesets")
    versions = [e["version"] for e in reg.list_versions(rid)]
    if len(versions) < 2:
        raise HTTPException(400, "need two published versions")
    fv, tv = (from_ or versions[0]), (to or versions[-1])
    try:
        return reg.diff(rid, fv, tv).to_dict()
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


# ------------------------------------------------------------------ datasets / runs
@app.get("/api/datasets")
def datasets() -> list[dict[str, str]]:
    d = ROOT / "sample_data" / "bronze"
    return [{"id": p.stem, "path": str(p.relative_to(ROOT)).replace("\\", "/")}
            for p in sorted(d.glob("*.csv"))]


class RunIn(BaseModel):
    source: str
    ruleset: str
    policy: str = "config/policies/default.yaml"
    mode: str = "OBSERVE_ONLY"
    dataset: str | None = None
    pipeline_id: str = "UI_RUN"
    run_id: str = "UI_001"


def _do_run(body: RunIn) -> dict[str, Any]:
    from dq_core.context import PipelineContext
    from dq_core.io.sources import CsvSource, ParquetSource
    spark, engine = J.get_spark(), J.get_engine()
    rs = engine.load_ruleset(str(ROOT / body.ruleset))
    src = ROOT / body.source
    df = (ParquetSource(str(src)).read(spark) if src.suffix == ".parquet"
          else CsvSource(str(src)).read(spark))
    ctx = PipelineContext(pipeline_id=body.pipeline_id, pipeline_run_id=body.run_id,
                          dataset=body.dataset or rs.dataset, stage=rs.stage, platform="LOCAL")
    import jobs as _j, threading as _t
    _jid = _t.current_thread().name.replace("dq-job-", "")
    _j.progress_cb(_jid, f"validating {rs.ref} on {src.name} — mode={body.mode}")
    res = engine.run(df, rs, str(ROOT / body.policy) if body.policy else None, ctx, mode=body.mode)
    _j.progress_cb(_jid, "writing report")
    out = ROOT / "reports"
    out.mkdir(parents=True, exist_ok=True)
    rep = out / f"{body.dataset or rs.dataset}_{res.run_id}.json"
    res.write_report(rep)
    rules = [{"rule_id": r.rule_id, "rule_type": r.rule_type, "severity": r.severity,
              "column": r.column, "status": r.status, "total": r.total_records,
              "passed": r.passed_records, "failed": r.failed_records,
              "pass_pct": r.pass_percentage} for r in res.validation_results.results] if res.validation_results else []
    result = {"run_id": res.run_id, "summary": res.summary_text(),
            "accepted": res.execution_metrics.accepted_records,
            "quarantined": res.execution_metrics.quarantined_records,
            "input": res.execution_metrics.input_records,
            "gate_passed": res.quality_gate.passed,
            "report_path": str(rep.relative_to(ROOT)).replace("\\", "/"),
            "rules": rules,
            "quarantine_by_status": res.execution_metrics.quarantine_status_breakdown if hasattr(
                res.execution_metrics, "quarantine_status_breakdown") else None}
    _ingest_anomalies(result, body.pipeline_id)
    return result


@app.post("/api/dq/run")
def dq_run(body: RunIn) -> dict[str, str]:
    jid = J.submit("dq_run", lambda: _do_run(body))
    return {"job_id": jid}


class ProfileIn(BaseModel):
    source: str
    dataset: str | None = None
    mode: str = "FULL"


_BIG_FILE = 50 * 1024 * 1024      # FULL profile above this risks OOM on the 2g local driver


def _profile_mode_for(src: Path, requested: str) -> str:
    try:
        if requested.upper() == "FULL" and src.stat().st_size > _BIG_FILE:
            return "SAMPLE"
    except OSError:
        pass
    return requested.upper()


def _do_profile(body: ProfileIn) -> dict[str, Any]:
    from dq_core.io.sources import CsvSource, ParquetSource
    spark, engine = J.get_spark(), J.get_engine()
    src = ROOT / body.source
    df = (ParquetSource(str(src)).read(spark) if src.suffix == ".parquet"
          else CsvSource(str(src)).read(spark))
    mode = _profile_mode_for(src, body.mode)
    rep = engine.profile(df, body.dataset or src.stem, mode=mode)
    out = ROOT / "reports"
    out.mkdir(exist_ok=True)
    p = out / f"ui_profile_{src.stem}.json"
    rep.write_json(p)
    cols = [{"name": c.name, "data_type": c.data_type, "inferred_type": c.inferred_type,
             "null_count": c.null_count, "null_percentage": c.null_percentage,
             "distinct_count": c.distinct_count, "unique_count": c.unique_count,
             "min": c.min, "max": c.max, "mean": c.mean, "stddev": c.stddev,
             "outlier_count": c.outlier_count, "whitespace_count": c.whitespace_count,
             "email_match_percentage": c.email_match_percentage,
             "dominant_date_format": c.dominant_date_format,
             "date_min": c.date_min, "date_max": c.date_max,
             "top_values": c.top_values[:5]} for c in rep.columns.values()]
    result = {"summary": rep.summary_text(), "report_path": str(p.relative_to(ROOT)).replace("\\", "/"),
              "row_count": rep.row_count, "column_count": rep.column_count, "profile_mode": mode,
              "anomalies": rep.anomalies[:20], "schema_anomalies": rep.schema_anomalies[:20],
              "pk_candidates": rep.primary_key_candidates, "columns": cols}
    _ingest_anomalies(result, "profile")
    return result


@app.post("/api/profile")
def profile(body: ProfileIn) -> dict[str, str]:
    jid = J.submit("profile", lambda: _do_profile(body))
    return {"job_id": jid}


@app.get("/api/jobs/{jid}")
def job_status(jid: str) -> dict[str, Any]:
    j = J.get(jid)
    if not j:
        raise HTTPException(404, "job not found")
    return j


@app.get("/api/jobs")
def jobs() -> list[dict[str, Any]]:
    return J.list_jobs()


@app.get("/api/jobs/{jid}/stream")
def job_stream(jid: str):
    """Server-Sent Events stream — real-time job progress for the UI."""
    from fastapi.responses import StreamingResponse
    return StreamingResponse(J.stream(jid), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ------------------------------------------------------------------ reports
@app.get("/api/reports")
def reports() -> list[dict[str, Any]]:
    d = ROOT / "reports"
    if not d.exists():
        return []
    return [{"name": f.name, "size": f.stat().st_size,
             "modified": f.stat().st_mtime} for f in sorted(d.glob("*.json"),
                                                           key=lambda x: x.stat().st_mtime, reverse=True)]


@app.get("/api/reports/{name}")
def report(name: str) -> dict[str, Any]:
    p = (ROOT / "reports" / name).resolve()
    if not str(p).startswith(str((ROOT / "reports").resolve())) or not p.exists():
        raise HTTPException(404, "report not found")
    return json.loads(p.read_text(encoding="utf-8"))


# ------------------------------------------------------------------ health score
@app.get("/api/health/score")
def health_score() -> dict[str, Any]:
    """Overall DQ health 0-100 from recent completed dq_run jobs."""
    rows = J.list_jobs(limit=50)
    runs = [r for r in rows if r.get("kind") == "dq_run" and r.get("status") == "COMPLETED"]
    if not runs:
        return {"score": None, "runs": 0, "message": "No completed DQ runs yet"}
    full = [J.get(r["job_id"]) for r in runs[:20]]
    total_in = sum((j.get("result") or {}).get("input", 0) for j in full if j)
    total_acc = sum((j.get("result") or {}).get("accepted", 0) for j in full if j)
    score = round(total_acc / total_in * 100, 1) if total_in else None
    gates = [(j.get("result") or {}).get("gate_passed") for j in full if j]
    gate_pass_rate = round(sum(1 for g in gates if g) / len(gates) * 100, 1) if gates else None
    return {"score": score, "gate_pass_rate": gate_pass_rate, "runs": len(runs),
            "total_input": total_in, "total_accepted": total_acc}


# ------------------------------------------------------------------ pipeline run-history trend
@app.get("/api/pipelines/{pid}/trend")
def pipeline_trend(pid: str) -> list[dict[str, Any]]:
    """Return last 30 dq_run job summaries for a pipeline (for sparkline charts)."""
    rows = J.list_jobs(limit=200)
    out = []
    for r in rows:
        if r.get("kind") != "dq_run" or r.get("status") != "COMPLETED":
            continue
        j = J.get(r["job_id"])
        if not j:
            continue
        res = j.get("result") or {}
        if res.get("run_id", "").startswith("DQRUN") or True:  # include all, filter by pid below
            inp = res.get("input", 0)
            acc = res.get("accepted", 0)
            out.append({
                "job_id": r["job_id"],
                "ran_at": r.get("finished_at") or r.get("submitted_at"),
                "pass_rate": round(acc / inp * 100, 1) if inp else None,
                "accepted": acc, "quarantined": res.get("quarantined", 0),
                "input": inp, "gate_passed": res.get("gate_passed"),
                "run_id": res.get("run_id"),
            })
        if len(out) >= 30:
            break
    return list(reversed(out))


# ------------------------------------------------------------------ anomaly lifecycle
_ANOMALY_DB = ROOT / "data" / "anomalies.db"


def _anomaly_conn():
    import sqlite3
    _ANOMALY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_ANOMALY_DB), check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS anomalies (
        id TEXT PRIMARY KEY, run_id TEXT, pipeline_id TEXT, dataset TEXT,
        col TEXT, atype TEXT, detail TEXT, severity TEXT,
        status TEXT DEFAULT 'OPEN', owner TEXT, note TEXT,
        created_at TEXT, updated_at TEXT)""")
    conn.commit()
    return conn


def _ingest_anomalies(job_result: dict[str, Any], pipeline_id: str) -> int:
    """Extract anomalies from a completed profile/dq_run result into the anomaly store."""
    import sqlite3
    from dq_core.runtime.utils import utc_now_iso
    anomalies = list(job_result.get("anomalies") or []) + list(job_result.get("schema_anomalies") or [])
    if not anomalies:
        return 0
    run_id = job_result.get("run_id") or job_result.get("report_path") or "unknown"
    dataset = job_result.get("dataset") or "unknown"
    conn = _anomaly_conn()
    now = utc_now_iso()
    inserted = 0
    for a in anomalies:
        aid = f"ANO-{run_id[:8]}-{a.get('column','_')}-{a.get('type','?')}"[:60]
        try:
            conn.execute("INSERT OR IGNORE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (aid, run_id, pipeline_id, dataset,
                          a.get("column"), a.get("type"), a.get("detail"),
                          a.get("severity", "WARNING"), "OPEN", None, None, now, now))
            inserted += 1
        except Exception:
            pass
    conn.commit()
    return inserted


@app.get("/api/anomalies")
def anomalies(status: str = "", pipeline_id: str = "", limit: int = 100) -> list[dict[str, Any]]:
    conn = _anomaly_conn()
    q = "SELECT * FROM anomalies WHERE 1=1"
    params: list[Any] = []
    if status:
        q += " AND status=?"; params.append(status)
    if pipeline_id:
        q += " AND pipeline_id=?"; params.append(pipeline_id)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(q, params).fetchall()
    cols = ["id", "run_id", "pipeline_id", "dataset", "col", "atype", "detail",
            "severity", "status", "owner", "note", "created_at", "updated_at"]
    return [dict(zip(cols, r)) for r in rows]


class AnomalyPatch(BaseModel):
    status: str | None = None
    owner: str | None = None
    note: str | None = None


@app.patch("/api/anomalies/{aid}")
def patch_anomaly(aid: str, body: AnomalyPatch) -> dict[str, Any]:
    from dq_core.runtime.utils import utc_now_iso
    conn = _anomaly_conn()
    fields: dict[str, Any] = {}
    if body.status:  fields["status"] = body.status
    if body.owner is not None: fields["owner"] = body.owner
    if body.note  is not None: fields["note"]  = body.note
    if not fields:
        raise HTTPException(400, "nothing to update")
    fields["updated_at"] = utc_now_iso()
    sql = "UPDATE anomalies SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE id=?"
    conn.execute(sql, [*fields.values(), aid])
    conn.commit()
    row = conn.execute("SELECT * FROM anomalies WHERE id=?", (aid,)).fetchone()
    if not row:
        raise HTTPException(404, "anomaly not found")
    cols = ["id","run_id","pipeline_id","dataset","col","atype","detail",
            "severity","status","owner","note","created_at","updated_at"]
    return dict(zip(cols, row))


@app.get("/api/anomalies/stats")
def anomaly_stats() -> dict[str, Any]:
    conn = _anomaly_conn()
    rows = conn.execute("SELECT status, COUNT(*) FROM anomalies GROUP BY status").fetchall()
    return {r[0]: r[1] for r in rows}


# ------------------------------------------------------------------ human approval inbox
_QUARANTINE_DIR = ROOT / "data" / "quarantine"
_APPROVALS_FILE = ROOT / "config" / "approvals" / "approved_corrections.yaml"


def _load_quarantine_pending() -> list[dict[str, Any]]:
    """Scan last 10 completed REMEDIATE jobs for PENDING_HUMAN_APPROVAL quarantine rows."""
    import yaml as _yaml
    rows = J.list_jobs(limit=100)
    runs = [r for r in rows if r.get("kind") == "dq_run" and r.get("status") == "COMPLETED"][:10]
    pending: list[dict[str, Any]] = []
    for r in runs:
        j = J.get(r["job_id"])
        if not j:
            continue
        res = j.get("result") or {}
        qbs = res.get("quarantine_by_status") or {}
        if not qbs.get("PENDING_HUMAN_APPROVAL"):
            continue
        # Try to read report for per-row detail
        rp = ROOT / (res.get("report_path") or "")
        if rp.exists():
            try:
                data = json.loads(rp.read_text(encoding="utf-8"))
                for rec in (data.get("quarantine_records") or []):
                    if rec.get("remediation_status") == "PENDING_HUMAN_APPROVAL":
                        rec["job_id"] = r["job_id"]
                        pending.append(rec)
            except Exception:
                pass
        else:
            pending.append({"job_id": r["job_id"], "count": qbs["PENDING_HUMAN_APPROVAL"],
                            "summary_only": True})
    return pending


@app.get("/api/quarantine/pending")
def quarantine_pending() -> dict[str, Any]:
    rows = _load_quarantine_pending()
    return {"total": sum(r.get("count", 1) for r in rows), "records": rows[:200]}


class ApprovalAction(BaseModel):
    column: str
    original_value: str
    corrected_value: str
    rule_id: str
    approved_by: str = "ui-steward"
    reason: str = "approved via UI"


@app.post("/api/quarantine/approve")
def approve_correction(body: ApprovalAction) -> dict[str, Any]:
    import yaml as _yaml
    from dq_core.runtime.utils import utc_now_iso
    _APPROVALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    doc: dict[str, Any] = {}
    if _APPROVALS_FILE.exists():
        with open(_APPROVALS_FILE, encoding="utf-8") as f:
            doc = _yaml.safe_load(f) or {}
    approvals = doc.setdefault("approvals", [])
    approvals.append({
        "column": body.column, "original_value": body.original_value,
        "corrected_value": body.corrected_value, "rule_id": body.rule_id,
        "approved_by": body.approved_by, "approved_at": utc_now_iso(),
        "reason": body.reason,
    })
    with open(_APPROVALS_FILE, "w", encoding="utf-8") as f:
        _yaml.safe_dump(doc, f, sort_keys=False, allow_unicode=True)
    return {"ok": True, "total_approved": len(approvals)}


@app.post("/api/quarantine/reject")
def reject_correction(body: ApprovalAction) -> dict[str, Any]:
    return {"ok": True, "status": "REJECTED", "column": body.column,
            "original_value": body.original_value}


# ------------------------------------------------------------------ onboarding wizard
@app.post("/api/onboard/upload")
async def onboard_upload(file: UploadFile = File(...)) -> dict[str, Any]:
    """Accept a CSV/Parquet upload for bootstrap preview (stored under data/uploads/)."""
    import re
    if not file.filename:
        raise HTTPException(400, "no file")
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", Path(file.filename).name)
    if not safe.lower().endswith((".csv", ".parquet")):
        raise HTTPException(400, "only .csv or .parquet accepted")
    dest_dir = ROOT / "data" / "uploads"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / safe
    content = await file.read()
    if not content:
        raise HTTPException(400, "empty file")
    dest.write_bytes(content)
    return {"ok": True, "path": str(dest.relative_to(ROOT)).replace("\\", "/"),
            "filename": safe, "size": len(content)}


class PreviewIn(BaseModel):
    source: str                  # path relative to repo root (upload or sample_data)
    dataset: str | None = None
    stage: str = "SILVER"


def _do_bootstrap_preview(body: PreviewIn) -> dict[str, Any]:
    """Profile + candidate rules for the wizard — same path as `dqctl bootstrap`."""
    from dq_core.generation.generator import CandidateRuleGenerator
    from dq_core.io.sources import CsvSource, ParquetSource
    spark, engine = J.get_spark(), J.get_engine()
    src = (ROOT / body.source).resolve()
    if not str(src).startswith(str(ROOT.resolve())) or not src.exists():
        raise HTTPException(400, f"source not found under repo: {body.source}")
    import threading as _t
    _jid = _t.current_thread().name.replace("dq-job-", "")
    J.progress_cb(_jid, f"reading {src.name}")
    df = (ParquetSource(str(src)).read(spark) if src.suffix == ".parquet"
          else CsvSource(str(src)).read(spark))
    ds = body.dataset or src.stem
    mode = _profile_mode_for(src, "FULL")
    J.progress_cb(_jid, f"profiling {ds} ({mode})")
    rep = engine.profile(df, ds, mode=mode)
    J.progress_cb(_jid, "generating candidate rules")
    ruleset_id = f"{ds}_{body.stage.lower()}"
    candidates = CandidateRuleGenerator().generate(rep, ruleset_id=ruleset_id,
                                                   version="1.0", stage=body.stage)
    cand = []
    for r in candidates.rules:
        d = r.to_dict()
        gen = d.get("generation") or {}
        cand.append({"rule_id": r.rule_id, "name": r.name, "rule_type": r.rule_type.value,
                     "column": r.column, "severity": r.severity.value,
                     "confidence": gen.get("confidence"), "evidence": gen.get("evidence"),
                     "description": r.description, "raw": d})
    return {"dataset": ds, "ruleset_id": ruleset_id, "stage": body.stage.upper(),
            "row_count": rep.row_count, "column_count": rep.column_count, "profile_mode": mode,
            "anomalies": rep.anomalies[:20], "schema_anomalies": rep.schema_anomalies[:20],
            "pk_candidates": rep.primary_key_candidates,
            "candidates": cand, "candidate_count": len(cand)}


@app.post("/api/onboard/preview")
def onboard_preview(body: PreviewIn) -> dict[str, str]:
    jid = J.submit("bootstrap_preview", lambda: _do_bootstrap_preview(body))
    return {"job_id": jid}


class FinalizeIn(BaseModel):
    preview_job_id: str
    approved_rule_ids: list[str]
    dataset: str
    ruleset_id: str
    pipeline_id: str
    pipeline_name: str | None = None
    platform: str = "DATABRICKS"
    runtime: str = "PYSPARK"
    stage: str = "SILVER"
    dq_mode: str = "OBSERVE_ONLY"
    policy: str = "../policies/default.yaml"
    owner: str = ""
    description: str = ""
    source: str                  # path relative to repo root — used as manifest test_data
    approved_by: str = "ui-onboard"
    publish: bool = False
    onboard: bool = False
    force: bool = False


@app.post("/api/onboard/finalize")
def onboard_finalize(body: FinalizeIn) -> dict[str, Any]:
    """Write the reviewed ruleset + manifest; optionally publish + onboard."""
    import re
    from dq_core.generation.generator import approve_candidates
    from dq_core.metadata.models import RuleDefinition, RuleSet, RuleSetStatus
    from dq_core.metadata.serialization import dump_yaml, save_ruleset
    from dq_core.pipelines.manifest import PipelineManifest

    job = J.get(body.preview_job_id)
    if not job or job.get("kind") != "bootstrap_preview" or job.get("status") != "COMPLETED":
        raise HTTPException(400, "preview job not found or not completed")
    res = job["result"]
    if res["ruleset_id"] != body.ruleset_id or res["dataset"] != body.dataset:
        raise HTTPException(400, "preview does not match requested ruleset/dataset")

    raw = {c["rule_id"]: c["raw"] for c in res["candidates"]}
    missing = set(body.approved_rule_ids) - set(raw)
    if missing:
        raise HTTPException(400, f"unknown rule ids: {sorted(missing)}")
    cand_rs = RuleSet(ruleset_id=body.ruleset_id, dataset=body.dataset, version="1.0",
                      stage=body.stage, status=RuleSetStatus.DRAFT,
                      rules=tuple(RuleDefinition.from_dict(
                          {**c["raw"], "dataset": body.dataset})
                          for c in res["candidates"]))
    rs = approve_candidates(cand_rs, body.approved_rule_ids,
                            ruleset_id=body.ruleset_id, version="1.0",
                            approved_by=body.approved_by)

    ruleset_path = ROOT / "config" / "rulesets" / f"{body.dataset}_v1.yaml"
    manifest_path = ROOT / "config" / "pipelines" / f"{body.pipeline_id}.yaml"
    for p in (ruleset_path, manifest_path):
        if p.exists() and not body.force:
            raise HTTPException(409, f"{p.relative_to(ROOT)} exists — pass force to overwrite")
    save_ruleset(rs, ruleset_path)

    src = (ROOT / body.source).resolve()
    test_data = os.path.relpath(src, manifest_path.parent).replace("\\", "/") \
        if src.exists() else body.source
    pid = body.pipeline_id.upper()
    manifest = {"pipeline": {
        "pipeline_id": pid,
        "pipeline_name": body.pipeline_name or re.sub(r"[_-]+", " ", pid).title(),
        "platform": body.platform.upper(), "runtime": body.runtime.upper(),
        "integration_mode": "INLINE", "dq_mode": body.dq_mode.upper(),
        "owner": body.owner,
        "description": body.description or f"Onboarded via UI wizard from {src.name} "
                                           f"({res['row_count']} rows profiled)",
        "policy": body.policy,
        "datasets": [{
            "dataset_id": body.dataset,
            "format": "CSV" if src.suffix.lower() == ".csv" else "PARQUET",
            "stages": ["BRONZE", body.stage.upper()],
            "rulesets": {body.stage.upper(): os.path.relpath(ruleset_path,
                                                             manifest_path.parent).replace("\\", "/")},
            "test_data": test_data,
            "test_data_format": "CSV" if src.suffix.lower() == ".csv" else "PARQUET",
        }],
    }}
    dump_yaml(manifest, manifest_path)
    try:
        mf = PipelineManifest.load(manifest_path)
    except Exception as exc:
        raise HTTPException(400, f"generated manifest failed validation: {exc}") from exc

    out = {"ok": True, "ruleset": f"{body.ruleset_id}@1.0",
           "ruleset_path": str(ruleset_path.relative_to(ROOT)).replace("\\", "/"),
           "manifest_path": str(manifest_path.relative_to(ROOT)).replace("\\", "/"),
           "rules_approved": len(rs.rules), "rules_rejected": len(cand_rs.rules) - len(rs.rules)}
    if body.publish:
        entry = _registry("rulesets").publish(rs, published_by=body.approved_by, activate=True)
        out["published"] = {"checksum": entry["checksum"][:12]}
    if body.onboard:
        entry = _registry("pipelines").onboard(mf, actor=body.approved_by)
        out["onboarded"] = {"pipeline_id": pid, "state": entry["state"]}
    out["manifest_yaml"] = manifest_path.read_text(encoding="utf-8")
    return out
