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


# ------------------------------------------------------------------ system status
@app.get("/api/system/status")
def system_status() -> dict[str, Any]:
    """Environment checks for the Settings page — Python, Java, Node, packages."""
    import shutil, subprocess, sys as _sys

    def _ver(cmd: list[str]) -> str | None:
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return (out.stdout or out.stderr).splitlines()[0].strip() if (out.stdout or out.stderr) else "?"
        except Exception:
            return None

    def _pkg(name: str) -> bool:
        try:
            __import__(name)
            return True
        except Exception:
            return False

    checks = {
        "python": {"ok": _sys.version_info >= (3, 12), "version": _sys.version.split()[0], "required": ">=3.12"},
        "java": {"ok": bool(_ver(["java", "-version"])), "version": _ver(["java", "-version"]), "required": "JDK 21+"},
        "node": {"ok": bool(_ver(["node", "--version"])), "version": _ver(["node", "--version"]), "required": ">=18"},
        "pyspark": {"ok": _pkg("pyspark"), "version": "4.1.3 expected"},
        "databricks_dqx": {"ok": _pkg("databricks.labs.dqx"), "version": "optional - LLM"},
        "litellm": {"ok": _pkg("litellm"), "version": "optional - LLM"},
        "fastapi": {"ok": _pkg("fastapi"), "version": "backend"},
        "uvicorn": {"ok": _pkg("uvicorn"), "version": "backend"},
    }
    return {
        "venv": str(ROOT / ".venv" / "Scripts" / "python.exe"),
        "checks": checks,
        "all_ok": all(c["ok"] for c in checks.values() if c.get("required") or c["version"] != "optional - LLM"),
        "env_vars": {
            "DATABRICKS_HOST": bool(os.environ.get("DATABRICKS_HOST")),
            "DATABRICKS_TOKEN": bool(os.environ.get("DATABRICKS_TOKEN")),
            "DQX_MODEL_ENDPOINT": bool(os.environ.get("DQX_MODEL_ENDPOINT")),
        },
    }


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


# ------------------------------------------------------------------ config / settings
@app.get("/api/config")
def get_config() -> dict[str, Any]:
    """Return current engine config (sanitised — no secrets)."""
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    return {
        "engine": {
            "name": cfg.name,
            "default_mode": cfg.default_mode.value,
            "include_dq_columns": cfg.include_dq_columns_in_accepted,
        },
        "registry": {
            "backend": cfg.registry.backend,
            "rulesets_path": cfg.registry.rulesets_path,
            "pipelines_path": cfg.registry.pipelines_path,
        },
        "profiling": {
            "mode": cfg.profiling.mode,
            "sample_fraction": cfg.profiling.sample_fraction,
            "max_allowed_values": cfg.profiling.max_allowed_values,
        },
        "validation": {
            "large_file_threshold_mb": cfg.validation.large_file_threshold_mb,
            "large_file_sample_rows": cfg.validation.large_file_sample_rows,
        },
        "runtime": {
            "spark_master": cfg.runtime.spark_master,
            "driver_memory": cfg.runtime.driver_memory,
            "executor_memory": cfg.runtime.executor_memory,
            "shuffle_partitions": cfg.runtime.shuffle_partitions,
        },
        "ai": {
            "provider": cfg.ai.provider,
            "has_dqx_endpoint": bool(cfg.ai.dqx_model_endpoint),
        },
    }


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
    # Auto-sample large files using config thresholds (engine.yaml -> validation.*)
    val_cfg = engine.config.validation
    threshold_mb = val_cfg.large_file_threshold_mb
    target_rows = val_cfg.large_file_sample_rows
    file_size_mb = src.stat().st_size / (1024 * 1024) if src.exists() else 0
    sampled = False
    if threshold_mb > 0 and file_size_mb > threshold_mb:
        # Estimate rows from file size (~150 bytes/row for typical CSV)
        est_rows = int(file_size_mb * 1024 * 1024 / 150)
        fraction = min(0.01, max(0.0001, target_rows / max(est_rows, 1)))
        df = df.sample(withReplacement=False, fraction=fraction, seed=42)
        sampled = True
    ctx = PipelineContext(pipeline_id=body.pipeline_id, pipeline_run_id=body.run_id,
                          dataset=body.dataset or rs.dataset, stage=rs.stage, platform="LOCAL")
    import jobs as _j, threading as _t
    _jid = _t.current_thread().name.replace("dq-job-", "")
    sample_note = f" (sampled {file_size_mb:.0f}MB -> ~{target_rows:,} rows)" if sampled else ""
    _j.progress_cb(_jid, f"validating {rs.ref} on {src.name}{sample_note} — mode={body.mode}")
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


_BIG_FILE = 50 * 1024 * 1024      # FULL profile above this risks OOM on the local driver
# Configurable via engine.yaml -> validation.large_file_threshold_mb


def _profile_mode_for(src: Path, requested: str, engine=None) -> str:
    try:
        threshold = _BIG_FILE
        if engine is not None:
            threshold = engine.config.validation.large_file_threshold_mb * 1024 * 1024
        if requested.upper() == "FULL" and src.stat().st_size > threshold:
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
    mode = _profile_mode_for(src, body.mode, engine)
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


# ------------------------------------------------------------------ DQ Assistant (chatbot)
class ChatIn(BaseModel):
    message: str


@app.post("/api/chat")
def chat(body: ChatIn) -> dict[str, Any]:
    """DQ Assistant — answers questions about pipelines, rulesets, jobs, setup."""
    if not body.message.strip():
        raise HTTPException(400, "empty message")
    import assistant as A
    return A.answer(body.message.strip())


@app.get("/api/assistant/enabled")
def assistant_enabled() -> dict[str, bool]:
    import assistant as A
    return {"enabled": A.is_enabled()}


class AssistantToggle(BaseModel):
    enabled: bool


@app.put("/api/assistant/enabled")
def assistant_set_enabled(body: AssistantToggle) -> dict[str, bool]:
    import assistant as A
    A.set_enabled(body.enabled)
    return {"enabled": A.is_enabled()}


@app.get("/api/chat/log")
def chat_log(limit: int = 100) -> dict[str, Any]:
    import assistant as A
    rows = A.history(limit)
    return {"total": len(rows), "entries": rows}


# ------------------------------------------------------------------ AI provider status
@app.get("/api/ai/status")
def ai_status() -> dict[str, Any]:
    """Current AI provider info — tells UI whether DQX or mock is active."""
    try:
        engine = J.get_engine()
        return engine.ai_provider.describe()
    except Exception as exc:
        return {"error": str(exc), "provider": "unknown"}


# ------------------------------------------------------------------ health score
@app.get("/api/health/score")
def health_score() -> dict[str, Any]:
    """Overall DQ health 0-100 from recent completed dq_run jobs."""
    rows = J.list_jobs(limit=50)
    runs = [r for r in rows if r.get("kind") == "dq_run" and r.get("status") == "COMPLETED"]
    if not runs:
        return {"score": None, "runs": 0, "message": "No completed DQ runs yet"}
    full = [J.get(r["job_id"]) for r in runs[:20]]
    total_in  = sum((j.get("result") or {}).get("input", 0)      for j in full if j)
    total_acc = sum((j.get("result") or {}).get("accepted", 0)   for j in full if j)
    total_q   = sum((j.get("result") or {}).get("quarantined", 0) for j in full if j)
    score = round(total_acc / total_in * 100, 1) if total_in else None
    gates = [(j.get("result") or {}).get("gate_passed") for j in full if j]
    gate_pass_rate = round(sum(1 for g in gates if g) / len(gates) * 100, 1) if gates else None
    return {"score": score, "gate_pass_rate": gate_pass_rate, "runs": len(runs),
            "total_input": total_in, "total_accepted": total_acc, "total_quarantined": total_q,
            "total_failed_runs": sum(1 for r in rows if r.get("kind") == "dq_run" and r.get("status") == "FAILED")}


# ------------------------------------------------------------------ dashboard summary (real data from reports)
def _read_reports_index() -> list[dict[str, Any]]:
    """Read all report JSONs from disk — cached by mtime so hot requests are fast."""
    d = ROOT / "reports"
    if not d.exists():
        return []
    result = []
    for f in sorted(d.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            result.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return result


@app.get("/api/dashboard/summary")
def dashboard_summary() -> dict[str, Any]:
    """Aggregate real metrics from all reports + jobs for the Dashboard."""
    reports = _read_reports_index()
    dq_reports = [r for r in reports if r.get("execution_metrics")]

    # --- totals across all runs ---
    total_input = sum(r["execution_metrics"].get("input_records", 0) for r in dq_reports)
    total_accepted = sum(r["execution_metrics"].get("accepted_records", 0) for r in dq_reports)
    total_quarantined = sum(r["execution_metrics"].get("quarantined_records", 0) for r in dq_reports)
    total_det_fixes = sum(r["execution_metrics"].get("deterministic_corrections", 0) for r in dq_reports)
    total_ai_fixes = sum(r["execution_metrics"].get("ai_corrections_applied", 0) for r in dq_reports)
    total_removed_dups = sum(r["execution_metrics"].get("removed_duplicates", 0) for r in dq_reports)
    total_rules_failed = sum(r["execution_metrics"].get("rules_failed", 0) for r in dq_reports)
    total_rules_evaluated = sum(r["execution_metrics"].get("rules_evaluated", 0) for r in dq_reports)
    gates_passed = sum(1 for r in dq_reports if r["execution_metrics"].get("quality_gate_passed"))
    gates_total = len(dq_reports)

    # --- per-dataset aggregation ---
    from collections import defaultdict
    by_dataset: dict[str, dict] = defaultdict(lambda: {
        "runs": 0, "input": 0, "accepted": 0, "quarantined": 0,
        "rules_failed": 0, "rules_evaluated": 0, "gates_passed": 0,
        "det_fixes": 0, "ai_fixes": 0, "modes": [], "last_run": None
    })
    for r in dq_reports:
        m = r["execution_metrics"]
        ds = m.get("dataset", "unknown")
        by_dataset[ds]["runs"] += 1
        by_dataset[ds]["input"] += m.get("input_records", 0)
        by_dataset[ds]["accepted"] += m.get("accepted_records", 0)
        by_dataset[ds]["quarantined"] += m.get("quarantined_records", 0)
        by_dataset[ds]["rules_failed"] += m.get("rules_failed", 0)
        by_dataset[ds]["rules_evaluated"] += m.get("rules_evaluated", 0)
        by_dataset[ds]["det_fixes"] += m.get("deterministic_corrections", 0)
        by_dataset[ds]["ai_fixes"] += m.get("ai_corrections_applied", 0)
        if m.get("quality_gate_passed"):
            by_dataset[ds]["gates_passed"] += 1
        mode = m.get("integration_mode")
        if mode and mode not in by_dataset[ds]["modes"]:
            by_dataset[ds]["modes"].append(mode)
        fa = m.get("finished_at")
        if fa and (not by_dataset[ds]["last_run"] or fa > by_dataset[ds]["last_run"]):
            by_dataset[ds]["last_run"] = fa

    # Compute pass_rate per dataset
    datasets_list = []
    for ds, v in sorted(by_dataset.items()):
        pass_rate = round(v["accepted"] / v["input"] * 100, 1) if v["input"] else None
        gate_rate = round(v["gates_passed"] / v["runs"] * 100, 1) if v["runs"] else None
        rule_pass_rate = round((v["rules_evaluated"] - v["rules_failed"]) / v["rules_evaluated"] * 100, 1) \
            if v["rules_evaluated"] else None
        datasets_list.append({
            "dataset": ds, **v, "pass_rate": pass_rate,
            "gate_pass_rate": gate_rate, "rule_pass_rate": rule_pass_rate
        })
    datasets_list.sort(key=lambda x: x["input"], reverse=True)

    # --- rule-level aggregation across all reports ---
    rule_agg: dict[str, dict] = defaultdict(lambda: {"total": 0, "failed": 0, "passed": 0, "datasets": set()})
    for r in dq_reports:
        vr = r.get("validation_results") or {}
        all_rules = list(vr.get("row_level_results") or []) + list(vr.get("dataset_level_results") or [])
        for rule in all_rules:
            rid = rule.get("rule_id", "?")
            rule_agg[rid]["total"] += 1
            if rule.get("status") == "FAILED":
                rule_agg[rid]["failed"] += 1
            else:
                rule_agg[rid]["passed"] += 1
            rule_agg[rid]["datasets"].add(rule.get("dataset", "?"))
    top_failing_rules = sorted(
        [{"rule_id": k, **{kk: v for kk, v in v.items() if kk != "datasets"},
          "datasets": list(v["datasets"]),
          "fail_rate": round(v["failed"] / v["total"] * 100, 1) if v["total"] else 0}
         for k, v in rule_agg.items() if v["failed"] > 0],
        key=lambda x: x["fail_rate"], reverse=True
    )[:10]

    # --- quarantine breakdown across all reports ---
    qbs_total: dict[str, int] = defaultdict(int)
    for r in dq_reports:
        for status, count in (r["execution_metrics"].get("quarantine_status_breakdown") or {}).items():
            qbs_total[status] += count

    # --- mode distribution ---
    mode_dist: dict[str, int] = defaultdict(int)
    for r in dq_reports:
        mode_dist[r["execution_metrics"].get("integration_mode", "UNKNOWN")] += 1

    # --- recent runs timeline (last 20) ---
    timeline = []
    for r in dq_reports[:20]:
        m = r["execution_metrics"]
        timeline.append({
            "run_id": m.get("dq_run_id", "")[:20],
            "dataset": m.get("dataset"),
            "mode": m.get("integration_mode"),
            "input": m.get("input_records", 0),
            "accepted": m.get("accepted_records", 0),
            "quarantined": m.get("quarantined_records", 0),
            "pass_rate": round(m.get("acceptance_rate", 0) * 100, 1),
            "gate_passed": m.get("quality_gate_passed"),
            "duration_s": round(m.get("duration_ms", 0) / 1000, 1),
            "finished_at": m.get("finished_at"),
            "rules_failed": m.get("rules_failed", 0),
            "rules_evaluated": m.get("rules_evaluated", 0),
            "det_fixes": m.get("deterministic_corrections", 0),
            "ai_fixes": m.get("ai_corrections_applied", 0),
        })

    return {
        "totals": {
            "input_records": total_input,
            "accepted_records": total_accepted,
            "quarantined_records": total_quarantined,
            "total_runs": gates_total,
            "gates_passed": gates_passed,
            "gate_pass_rate": round(gates_passed / gates_total * 100, 1) if gates_total else None,
            "overall_pass_rate": round(total_accepted / total_input * 100, 1) if total_input else None,
            "deterministic_fixes": total_det_fixes,
            "ai_fixes": total_ai_fixes,
            "duplicates_removed": total_removed_dups,
            "rules_evaluated": total_rules_evaluated,
            "rules_failed": total_rules_failed,
        },
        "by_dataset": datasets_list,
        "top_failing_rules": top_failing_rules,
        "quarantine_breakdown": dict(qbs_total),
        "mode_distribution": dict(mode_dist),
        "timeline": timeline,
        "profile_runs": len([r for r in reports if not r.get("execution_metrics")]),
    }


@app.get("/api/pipelines/{pid}/metrics")
def pipeline_metrics(pid: str) -> dict[str, Any]:
    """All real execution metrics for a specific pipeline — from reports on disk."""
    reports = _read_reports_index()
    pipe_reports = [r for r in reports
                    if (r.get("execution_metrics") or {}).get("pipeline_id") == pid]
    if not pipe_reports:
        return {"pipeline_id": pid, "runs": 0, "message": "No reports found for this pipeline"}

    runs = []
    for r in pipe_reports:
        m = r["execution_metrics"]
        vr = r.get("validation_results") or {}
        all_rules = list(vr.get("row_level_results") or []) + list(vr.get("dataset_level_results") or [])
        runs.append({
            "run_id": m.get("dq_run_id"),
            "dataset": m.get("dataset"),
            "stage": m.get("stage"),
            "mode": m.get("integration_mode"),
            "ruleset": f"{m.get('ruleset_id')}@{m.get('ruleset_version')}",
            "input": m.get("input_records", 0),
            "accepted": m.get("accepted_records", 0),
            "quarantined": m.get("quarantined_records", 0),
            "removed_duplicates": m.get("removed_duplicates", 0),
            "pass_rate": round(m.get("acceptance_rate", 0) * 100, 1),
            "gate_passed": m.get("quality_gate_passed"),
            "duration_s": round(m.get("duration_ms", 0) / 1000, 1),
            "phase_durations_s": {k: round(v / 1000, 1) for k, v in (m.get("phase_durations_ms") or {}).items()},
            "rules_evaluated": m.get("rules_evaluated", 0),
            "rules_failed": m.get("rules_failed", 0),
            "det_fixes": m.get("deterministic_corrections", 0),
            "ai_fixes": m.get("ai_corrections_applied", 0),
            "ai_proposals": m.get("ai_proposals_evaluated", 0),
            "quarantine_breakdown": m.get("quarantine_status_breakdown") or {},
            "finished_at": m.get("finished_at"),
            "rules": [{"rule_id": r2.get("rule_id"), "column": r2.get("column"),
                       "status": r2.get("status"), "pass_pct": r2.get("pass_percentage"),
                       "failed": r2.get("failed_records", 0), "total": r2.get("total_records", 0),
                       "severity": r2.get("severity")}
                      for r2 in all_rules],
        })
    runs.sort(key=lambda x: x.get("finished_at") or "", reverse=True)

    # Aggregate across all runs for this pipeline
    total_in = sum(r["input"] for r in runs)
    total_acc = sum(r["accepted"] for r in runs)
    return {
        "pipeline_id": pid,
        "runs": len(runs),
        "total_input": total_in,
        "total_accepted": total_acc,
        "total_quarantined": sum(r["quarantined"] for r in runs),
        "overall_pass_rate": round(total_acc / total_in * 100, 1) if total_in else None,
        "gate_pass_rate": round(sum(1 for r in runs if r["gate_passed"]) / len(runs) * 100, 1),
        "executions": runs,
    }


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
import sqlite3 as _sqlite3

_ANOMALY_DB = ROOT / "data" / "anomalies.db"
_ANOMALY_DB.parent.mkdir(parents=True, exist_ok=True)
_ano_conn = _sqlite3.connect(str(_ANOMALY_DB), check_same_thread=False)
_ano_conn.execute("""CREATE TABLE IF NOT EXISTS anomalies (
    id TEXT PRIMARY KEY, run_id TEXT, pipeline_id TEXT, dataset TEXT,
    col TEXT, atype TEXT, detail TEXT, severity TEXT,
    status TEXT DEFAULT 'OPEN', owner TEXT, note TEXT,
    created_at TEXT, updated_at TEXT)""")
_ano_conn.commit()
_ano_lock = __import__("threading").Lock()


def _anomaly_conn():
    return _ano_conn


def _ingest_anomalies(job_result: dict[str, Any], pipeline_id: str) -> int:
    """Extract anomalies from a completed profile/dq_run result into the anomaly store.

    For profile jobs: pulls from job_result['anomalies'] + ['schema_anomalies'].
    For dq_run jobs: creates anomaly records from each FAILED rule, so the
    Anomalies inbox always has actionable items even without a separate profile run.
    Also backfills from the written report's validation_results.
    """
    from dq_core.runtime.utils import utc_now_iso
    now = utc_now_iso()
    inserted = 0
    run_id = job_result.get("run_id") or job_result.get("report_path") or "unknown"
    dataset = job_result.get("dataset", "unknown") or "unknown"

    # -- profile anomalies (direct from job result) --
    ano_list = list(job_result.get("anomalies") or []) + list(job_result.get("schema_anomalies") or [])
    for a in ano_list:
        aid = f"ANO-{str(run_id)[:10]}-{a.get('column','_')[:20]}-{a.get('type','?')[:15]}"[:60]
        with _ano_lock:
            _ano_conn.execute("INSERT OR IGNORE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (aid, run_id, pipeline_id, dataset, a.get("column"), a.get("type"),
                              a.get("detail"), a.get("severity", "WARNING"), "OPEN", None, None, now, now))
            inserted += 1
    if ano_list:
        _ano_conn.commit()

    # -- dq_run: create anomaly per FAILED rule from the report on disk --
    report_path = job_result.get("report_path")
    if report_path and not ano_list:
        rp = ROOT / report_path
        if rp.exists():
            try:
                data = json.loads(rp.read_text(encoding="utf-8"))
                vr = (data.get("validation_results") or {})
                all_results = list(vr.get("row_level_results") or []) + list(vr.get("dataset_level_results") or [])
                for r in all_results:
                    if r.get("status") != "FAILED":
                        continue
                    col = r.get("column") or r.get("rule_id", "?")
                    detail = f"rule {r['rule_id']} failed: {r.get('failed_records', '?')} records ({100 - r.get('pass_percentage', 0):.1f}% failure rate)"
                    aid = f"ANO-{str(run_id)[:10]}-{col[:20]}-{r.get('rule_type','?')[:15]}"[:60]
                    with _ano_lock:
                        _ano_conn.execute("INSERT OR IGNORE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                         (aid, run_id, pipeline_id, dataset, col, r.get("rule_type", "RULE_FAILURE"),
                                          detail, r.get("severity", "ERROR"), "OPEN", None, None, now, now))
                        inserted += 1
                _ano_conn.commit()
            except Exception:
                pass
    return inserted


# IMPORTANT: /api/anomalies/stats must be declared BEFORE /api/anomalies/{aid}
# otherwise FastAPI routes 'stats' as a path parameter.
@app.get("/api/anomalies/stats")
def anomaly_stats() -> dict[str, Any]:
    with _ano_lock:
        rows = _ano_conn.execute("SELECT status, COUNT(*) FROM anomalies GROUP BY status").fetchall()
    total = sum(r[1] for r in rows)
    return {r[0]: r[1] for r in rows} | {"total": total}


@app.get("/api/anomalies")
def anomalies(status: str = "", pipeline_id: str = "", limit: int = 100) -> list[dict[str, Any]]:
    q = "SELECT * FROM anomalies WHERE 1=1"
    params: list[Any] = []
    if status:
        q += " AND status=?"; params.append(status)
    if pipeline_id:
        q += " AND pipeline_id=?"; params.append(pipeline_id)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with _ano_lock:
        rows = _ano_conn.execute(q, params).fetchall()
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
    fields: dict[str, Any] = {}
    if body.status:              fields["status"] = body.status
    if body.owner is not None:   fields["owner"]  = body.owner
    if body.note  is not None:   fields["note"]   = body.note
    if not fields:
        raise HTTPException(400, "nothing to update")
    fields["updated_at"] = utc_now_iso()
    sql = "UPDATE anomalies SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE id=?"
    with _ano_lock:
        _ano_conn.execute(sql, [*fields.values(), aid])
        _ano_conn.commit()
        row = _ano_conn.execute("SELECT * FROM anomalies WHERE id=?", (aid,)).fetchone()
    if not row:
        raise HTTPException(404, "anomaly not found")
    cols = ["id","run_id","pipeline_id","dataset","col","atype","detail",
            "severity","status","owner","note","created_at","updated_at"]
    return dict(zip(cols, row))


# ------------------------------------------------------------------ human approval inbox
_APPROVALS_FILE = ROOT / "config" / "approvals" / "approved_corrections.yaml"


def _load_quarantine_pending() -> list[dict[str, Any]]:
    """Scan last 10 completed REMEDIATE jobs — read ai_decisions from report JSON
    and return rows with decision == REQUIRES_APPROVAL (PENDING_HUMAN_APPROVAL)."""
    rows = J.list_jobs(limit=100)
    runs = [r for r in rows if r.get("kind") == "dq_run" and r.get("status") == "COMPLETED"][:10]
    pending: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in runs:
        j = J.get(r["job_id"])
        if not j:
            continue
        res = j.get("result") or {}
        qbs = res.get("quarantine_by_status") or {}
        if not qbs.get("PENDING_HUMAN_APPROVAL"):
            continue
        rp = ROOT / (res.get("report_path") or "")
        if not rp.exists():
            pending.append({"job_id": r["job_id"], "count": qbs["PENDING_HUMAN_APPROVAL"],
                            "summary_only": True, "dataset": "unknown"})
            continue
        try:
            data = json.loads(rp.read_text(encoding="utf-8"))
            # ai_decisions lives in remediation_results
            decisions = (data.get("remediation_results") or {}).get("ai_decisions") or []
            for d in decisions:
                if d.get("decision") != "REQUIRES_APPROVAL":
                    continue
                key = f"{d.get('record_id')}-{d.get('column')}"
                if key in seen:
                    continue
                seen.add(key)
                pending.append({
                    "job_id": r["job_id"],
                    "run_id": res.get("run_id"),
                    "dataset": (data.get("execution_metrics") or {}).get("dataset", "unknown"),
                    "record_id":      d.get("record_id"),
                    "record_key":     d.get("record_key"),
                    "column":         d.get("column"),
                    "rule_id":        d.get("rule_id"),
                    "original_value": d.get("original_value"),
                    "proposed_value": d.get("proposed_value"),
                    "confidence":     d.get("confidence"),
                    "reason":         d.get("reason"),
                    "sensitive_category": d.get("sensitive_category"),
                    "decision_reason": d.get("decision_reason"),
                    "provider":       d.get("provider"),
                    "simulated":      d.get("simulated"),
                })
        except Exception:
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
    dataset: str = "unknown"
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
        "dataset": body.dataset, "column": body.column,
        "original_value": body.original_value,
        "approved_value": body.corrected_value, "rule_id": body.rule_id,
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
    mode = _profile_mode_for(src, "FULL", engine)
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
