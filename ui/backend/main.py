"""DQ Accelerator UI backend — FastAPI wrapper over dq_core.

REST contract (also usable by orchestrators):
  GET  /api/health
  GET  /api/pipelines                     list registered pipelines + states
  POST /api/pipelines/onboard             {manifest_yaml} -> register
  POST /api/pipelines/{id}/{action}       action in test|activate|pause|offboard
  GET  /api/pipelines/{id}                detail incl. history + executions
  GET  /api/rulesets                      published rulesets + versions + active
  POST /api/rulesets/publish              {ruleset_yaml, activate}
  POST /api/rulesets/{id}/{action}        action in activate|rollback (+version)
  GET  /api/rulesets/{id}/diff?from=&to=  version diff
  GET  /api/datasets                      sample CSVs available for runs
  POST /api/dq/run                        submit DQ job -> {job_id}
  POST /api/profile                       submit profile job -> {job_id}
  GET  /api/jobs/{id}                     job status/result
  GET  /api/jobs                          recent jobs
  GET  /api/reports                       list JSON reports
  GET  /api/reports/{name}                report content
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
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
    return {"run_id": res.run_id, "summary": res.summary_text(),
            "accepted": res.execution_metrics.accepted_records,
            "quarantined": res.execution_metrics.quarantined_records,
            "input": res.execution_metrics.input_records,
            "gate_passed": res.quality_gate.passed,
            "report_path": str(rep.relative_to(ROOT)).replace("\\", "/"),
            "rules": rules,
            "quarantine_by_status": res.execution_metrics.quarantine_status_breakdown if hasattr(
                res.execution_metrics, "quarantine_status_breakdown") else None}


@app.post("/api/dq/run")
def dq_run(body: RunIn) -> dict[str, str]:
    jid = J.submit("dq_run", lambda: _do_run(body))
    return {"job_id": jid}


class ProfileIn(BaseModel):
    source: str
    dataset: str | None = None
    mode: str = "FULL"


def _do_profile(body: ProfileIn) -> dict[str, Any]:
    from dq_core.io.sources import CsvSource, ParquetSource
    spark, engine = J.get_spark(), J.get_engine()
    src = ROOT / body.source
    df = (ParquetSource(str(src)).read(spark) if src.suffix == ".parquet"
          else CsvSource(str(src)).read(spark))
    rep = engine.profile(df, body.dataset or src.stem, mode=body.mode)
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
    return {"summary": rep.summary_text(), "report_path": str(p.relative_to(ROOT)).replace("\\", "/"),
            "row_count": rep.row_count, "column_count": rep.column_count,
            "anomalies": rep.anomalies[:20], "schema_anomalies": rep.schema_anomalies[:20],
            "pk_candidates": rep.primary_key_candidates, "columns": cols}


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
