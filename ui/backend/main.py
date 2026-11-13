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
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import jobs as J

ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    """Load KEY=VALUE pairs from a .env file into os.environ (no override)."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv(ROOT / ".env")

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


class PipelineRunIn(BaseModel):
    dataset_id: str | None = None
    mode: str | None = None                 # defaults to manifest dq_mode
    halt_on_gate_failure: bool = False      # stop downstream stages if a gate fails


# NOTE: must stay ABOVE /api/pipelines/{pid}/{action} — Starlette matches in
# registration order and "{action}" would otherwise swallow "run".
@app.post("/api/pipelines/{pid}/run")
def pipeline_run(pid: str, body: PipelineRunIn) -> dict[str, str]:
    try:
        jid = J.submit("pipeline_run", lambda: _do_pipeline_run(pid, body))
    except J.JobConflict as e:
        raise HTTPException(409, str(e))
    return {"job_id": jid}


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


@app.get("/api/rulesets/files")
def ruleset_files() -> list[str]:
    """YAML filenames in config/rulesets — for ad-hoc run dropdown."""
    d = ROOT / "config" / "rulesets"
    return sorted(p.name for p in d.glob("*.yaml")) if d.exists() else []


@app.get("/api/policies/files")
def policy_files() -> list[str]:
    """YAML filenames in config/policies — for policy dropdowns."""
    d = ROOT / "config" / "policies"
    return sorted(p.name for p in d.glob("*.yaml")) if d.exists() else []


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
def _prog(msg: str) -> None:
    """Emit a granular progress update for the job running on this thread.
    Messages may carry a 'phase=<key> |' prefix the UI maps to phase chips."""
    import threading as _t
    name = _t.current_thread().name
    if not name.startswith("dq-job-"):
        return
    J.progress_cb(name.replace("dq-job-", ""), msg)


def _spark_engine() -> tuple[Any, Any]:
    """Start/reuse Spark + engine, reporting each step. Returns (spark, engine)."""
    t0 = time.monotonic()
    _prog("phase=spark | starting Spark session (first run after boot can take ~30–60s)")
    spark = J.get_spark()
    _prog("phase=spark | loading engine + AI provider")
    engine = J.get_engine()
    _prog(f"phase=spark | Spark + engine ready in {time.monotonic() - t0:.1f}s")
    return spark, engine


def _read_source(spark: Any, src: Path):
    from dq_core.io.sources import CsvSource, ParquetSource
    size_mb = src.stat().st_size / (1024 * 1024) if src.exists() else 0.0
    _prog(f"phase=read | reading {src.name} ({size_mb:.1f} MB)")
    df = (ParquetSource(str(src)).read(spark) if src.suffix == ".parquet"
          else CsvSource(str(src)).read(spark))
    cols = df.columns
    shown = ", ".join(cols[:8]) + ("…" if len(cols) > 8 else "")
    _prog(f"phase=read | loaded {len(cols)} columns: {shown}")
    return df


_RUN_PHASE_LABELS = {
    "bronze": "phase=read | persisting raw bronze layer + ingest manifest",
    "silver_setup": "phase=validate | loading silver ruleset + profiling",
    "silver_write": "phase=report | writing silver accepted + quarantine outputs",
    "gold_setup": "phase=validate | loading gold ruleset (strict re-check)",
    "gold_write": "phase=report | writing gold accepted + quarantine outputs",
    "reports": "phase=report | writing run reports (silver/gold/profile JSON)",
    "profiling": "phase=profile | profiling dataset statistics",
    "validation": "phase=validate | validating all rules against the data",
    "remediation_deterministic": "phase=remediate | applying deterministic fixes",
    "remediation_ai": "phase=ai | AI corrections via LLM provider",
    "revalidation": "phase=revalidate | re-validating corrected rows",
    "quarantine": "phase=quarantine | building quarantine + quality gate",
}


@app.get("/api/datasets")
def datasets() -> list[dict[str, Any]]:
    out: list[dict[str, str]] = []
    for d in (ROOT / "sample_data" / "bronze", ROOT / "sample_data" / "demo", ROOT / "data" / "uploads"):
        if not d.exists():
            continue
        for p in sorted(d.glob("*.csv")) + sorted(d.glob("*.parquet")) + sorted(d.glob("*.json")):
            rows = None
            if p.suffix.lower() == ".parquet":
                try:
                    import pyarrow.parquet as _pq
                    rows = _pq.read_metadata(str(p)).num_rows
                except Exception:
                    pass
            out.append({"id": p.stem, "path": str(p.relative_to(ROOT)).replace("\\", "/"),
                        "size_mb": round(p.stat().st_size / 1e6, 1), "rows": rows})
    return out


class RunIn(BaseModel):
    source: str
    ruleset: str
    source_format: str = "AUTO"
    policy: str = "config/policies/default.yaml"
    mode: str = "OBSERVE_ONLY"
    dataset: str | None = None
    pipeline_id: str = "UI_RUN"
    run_id: str = "UI_001"


def _do_run(body: RunIn) -> dict[str, Any]:
    from dq_core.context import PipelineContext
    t0 = time.monotonic()
    spark, engine = _spark_engine()
    rs = engine.load_ruleset(str(ROOT / body.ruleset))
    src = ROOT / body.source
    if (body.source_format or "AUTO").upper() in ("TABLE", "DELTA", "JSON") or not src.exists():
        df = _read_any_source(spark, body.source, body.source_format)
    else:
        df = _read_source(spark, src)
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
        _prog(f"phase=read | sampling large file → ~{target_rows:,} rows")
    ctx = PipelineContext(pipeline_id=body.pipeline_id, pipeline_run_id=body.run_id,
                          dataset=body.dataset or rs.dataset, stage=rs.stage, platform="LOCAL")
    try:
        n_rules = len(rs.executable_rules())
    except Exception:
        n_rules = len(getattr(rs, "rules", []) or [])
    sample_note = f" (sampled {file_size_mb:.0f}MB -> ~{target_rows:,} rows)" if sampled else ""
    _prog(f"phase=validate | validating {rs.ref} on {src.name}{sample_note} — mode={body.mode}, {n_rules} rules")
    res = engine.run(df, rs, str(ROOT / body.policy) if body.policy else None, ctx, mode=body.mode,
                     on_phase=lambda k: _prog(_RUN_PHASE_LABELS.get(k, f"phase=validate | {k}")))
    _prog(f"phase=report | engine finished in {time.monotonic() - t0:.1f}s — writing report")
    out = ROOT / "reports"
    out.mkdir(parents=True, exist_ok=True)
    rep = out / f"{body.dataset or rs.dataset}_{res.run_id}.json"
    res.write_report(rep)
    rules = [{"rule_id": r.rule_id, "rule_type": r.rule_type, "severity": r.severity,
              "column": r.column, "status": r.status, "total": r.total_records,
              "passed": r.passed_records, "failed": r.failed_records,
              "pass_pct": r.pass_percentage} for r in res.validation_results.results] if res.validation_results else []
    result = {"run_id": res.run_id, "pipeline_id": body.pipeline_id, "summary": res.summary_text(),
            "accepted": res.execution_metrics.accepted_records,
            "quarantined": res.execution_metrics.quarantined_records,
            "input": res.execution_metrics.input_records,
            "gate_passed": res.quality_gate.passed,
            "report_path": str(rep.relative_to(ROOT)).replace("\\", "/"),
            "rules": rules,
            "quarantine_by_status": res.execution_metrics.quarantine_status_breakdown if hasattr(
                res.execution_metrics, "quarantine_status_breakdown") else None,
            "phases": getattr(res.execution_metrics, "phase_durations_ms", None)}
    _ingest_anomalies(result, body.pipeline_id)
    return result


@app.post("/api/dq/run")
def dq_run(body: RunIn) -> dict[str, str]:
    try:
        jid = J.submit("dq_run", lambda: _do_run(body))
    except J.JobConflict as e:
        raise HTTPException(409, str(e))
    return {"job_id": jid}


# ------------------------------------------------------------------ medallion pipeline run
_STAGE_ORDER = {"BRONZE": 0, "SILVER": 1, "GOLD": 2}


def _load_pipeline_manifest(pid: str):
    """Store-first manifest resolution: NoSQL `manifests` collection, YAML fallback + write-through."""
    from dq_core.metadata import resolver as _res
    from dq_core.pipelines.manifest import PipelineManifest
    try:
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            raw = _res.manifest_from_store(store, pid)
            if raw:
                return PipelineManifest.from_dict(raw)
    except Exception:
        pass
    mdir = ROOT / "config" / "pipelines"
    direct = mdir / f"{pid}.yaml"
    if direct.exists():
        mf = PipelineManifest.load(direct)
        try:
            store = _res.store_from_engine(J.get_engine())
            if store is not None:
                _res.save_manifest(store, mf)
        except Exception:
            pass
        return mf
    for p in sorted(mdir.glob("*.yaml")):
        try:
            mf = PipelineManifest.load(p)
            if mf.pipeline_id == pid:
                return mf
        except Exception:
            continue
    raise HTTPException(404, f"pipeline manifest not found for {pid}")


def _stage_prog(stage: str, key: str) -> None:
    tag, _, text = _RUN_PHASE_LABELS.get(key, "phase=validate | step").partition(" | ")
    _prog(f"{tag} | [{stage}] {text}")


def _output_base() -> Path:
    from dq_core.runtime.config import EngineConfig
    raw = Path(EngineConfig.from_yaml(ROOT / "config" / "engine.yaml").outputs.base_path or "data")
    return raw if raw.is_absolute() else (ROOT / "config" / raw).resolve()


def _do_pipeline_run(pid: str, body: PipelineRunIn) -> dict[str, Any]:
    """Medallion execution with physical persistence.

    Raw input is always written to Bronze. Silver and Gold use the existing
    MedallionRunner so accepted, quarantine, audit, and report outputs are
    written before the API returns. Gold remains optional when no Gold ruleset
    is configured.
    """
    from dq_core.io.sinks import StoragePaths, StorageSink
    from dq_core.orchestration.medallion import MedallionRunner
    t0 = time.monotonic()
    spark, engine = _spark_engine()
    mf = _load_pipeline_manifest(pid)
    if not mf.datasets:
        raise HTTPException(400, f"manifest {pid} declares no datasets")
    ds = next((d for d in mf.datasets
               if d.dataset_id == (body.dataset_id or mf.datasets[0].dataset_id)), None)
    if ds is None:
        raise HTTPException(400, f"dataset '{body.dataset_id}' not in manifest {pid}")
    src = ds.test_data
    sp = Path(src)
    tfmt = (ds.test_data_format or "").upper()
    if tfmt in ("TABLE", "DELTA", "JSON") or not sp.exists():
        df = _read_any_source(spark, src, tfmt if tfmt in ("TABLE", "DELTA", "JSON") else "AUTO")
    else:
        df = _read_source(spark, sp)
    mode = body.mode or getattr(mf.dq_mode, "value", str(mf.dq_mode))
    policy = str(mf.policy) if mf.policy else str(ROOT / "config" / "policies" / "default.yaml")
    pipe_run_id = f"{pid}-{int(t0)}"
    platform = getattr(mf.platform, "value", mf.platform) if mf.platform else "LOCAL"

    _out = engine.config.outputs
    paths = StoragePaths(str(_output_base()), catalog=getattr(_out, "catalog", "") or "")
    sink = StorageSink(fmt=getattr(_out, "format", "parquet") or "parquet")
    silver_ruleset = ds.rulesets.get("SILVER")
    if not silver_ruleset:
        raise HTTPException(400, f"{pid}/{ds.dataset_id} has no SILVER ruleset")
    gold_ruleset = ds.rulesets.get("GOLD")
    _prog(f"phase=read | landing raw data and running persisted medallion stages under {paths.base_path}")
    result = MedallionRunner(engine, paths, sink).run(
        df, ds.dataset_id, pipeline_id=pid, pipeline_run_id=pipe_run_id,
        silver_ruleset=silver_ruleset, policy=policy, silver_mode=mode,
        gold_ruleset=gold_ruleset, source_path=src, profile=False, platform=str(platform),
        on_phase=_stage_prog)
    stages_out = [
        {"stage": "BRONZE", "status": "PERSISTED", "rows": result.bronze_rows,
         "written_paths": {"raw": result.written_paths.get("bronze", paths.bronze(ds.dataset_id))}},
        _stage_payload("SILVER", result.silver_result, result.written_paths, "silver"),
    ]
    if result.gold_result:
        stages_out.append(_stage_payload("GOLD", result.gold_result, result.written_paths, "gold"))
    gate_all = all(s.get("gate_passed", True) for s in stages_out if s["status"] == "COMPLETED")
    return {
        "pipeline_id": pid, "dataset": ds.dataset_id, "mode": mode,
        "pipeline_run_id": pipe_run_id, "stages": stages_out, "halted": False,
        "gate_passed": gate_all, "written_paths": result.written_paths,
        "duration_ms": round((time.monotonic() - t0) * 1000, 2),
        "summary": (f"Pipeline {pid} / {ds.dataset_id} [{mode}] — physical medallion outputs written, "
                    f"gate={'PASS' if gate_all else 'FAIL'}, {time.monotonic() - t0:.1f}s"),
    }


def _stage_payload(stage: str, result, written: dict[str, str], prefix: str) -> dict[str, Any]:
    m = result.execution_metrics
    pillars = result.quality_pillars()
    outcomes = (result.remediation_results.outcome_audit or {}).get("counts", {})
    return {
        "stage": stage, "status": "COMPLETED", "run_id": result.run_id,
        "ruleset": f"{m.ruleset_id}@{m.ruleset_version}",
        "input": m.input_records, "accepted": m.accepted_records,
        "quarantined": m.quarantined_records, "gate_passed": result.quality_gate.passed,
        "duration_ms": m.duration_ms,
        "quality_score": pillars.get("overall_score"),
        "weak_pillars": [k for k, v in (pillars.get("pillars") or {}).items()
                         if v.get("status") == "MEASURED" and v.get("score", 100) < 90],
        "recommendation": pillars.get("domain_recommendation"),
        "outcome_counts": outcomes,
        "written_paths": {k.removeprefix(prefix + "_"): v for k, v in written.items() if k.startswith(prefix + "_")},
    }


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
    spark, engine = _spark_engine()
    src = ROOT / body.source
    df = _read_source(spark, src)
    mode = _profile_mode_for(src, body.mode, engine)
    ds = body.dataset or src.stem
    _prog(f"phase=profile | profiling {ds} ({mode}) — computing column statistics")
    rep = engine.profile(df, ds, mode=mode, on_phase=lambda m: _prog(f"phase=profile | {m}"))
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
    try:
        jid = J.submit("profile", lambda: _do_profile(body))
    except J.JobConflict as e:
        raise HTTPException(409, str(e))
    return {"job_id": jid}


@app.get("/api/jobs/{jid}")
def job_status(jid: str) -> dict[str, Any]:
    j = J.get(jid)
    if not j:
        raise HTTPException(404, "job not found")
    r = j.get("result")
    if isinstance(r, dict) and "run" in r and "accepted" not in r:
        stages = (r.get("run") or {}).get("stages") or []
        silver = next((s for s in stages if s.get("stage") == "SILVER"), {})
        import glob as _g
        ds = r.get("dataset") or ""
        reps = sorted(_g.glob(str(ROOT / "reports" / f"{ds}*silver*.json")), key=os.path.getmtime)
        r.update({"input": silver.get("input") or silver.get("input_rows"),
                  "accepted": silver.get("accepted"), "quarantined": silver.get("quarantined"),
                  "gate_passed": (r.get("run") or {}).get("gate_passed"),
                  "report_path": reps[-1] if reps else ""})
    return j


@app.get("/api/jobs")
def jobs() -> list[dict[str, Any]]:
    return J.list_jobs()


@app.post("/api/jobs/{jid}/cancel")
def job_cancel(jid: str) -> dict[str, Any]:
    j = J.get(jid)
    if not j:
        raise HTTPException(404, "job not found")
    if j.get("status") not in ("QUEUED", "RUNNING"):
        raise HTTPException(400, f"job is {j.get('status')} — cannot cancel")
    J.request_cancel(jid)
    return {"ok": True, "job_id": jid}


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
@app.post("/api/engine/reload")
def engine_reload(restart_spark: bool = False) -> dict[str, Any]:
    """Hot-reload engine.yaml without restarting the backend process.
    Use after editing config (provider, memory, thresholds). Spark restart is
    optional — required only when changing runtime.* settings."""
    return J.reload_engine(restart_spark=restart_spark)


@app.get("/api/ai/status")
def ai_status() -> dict[str, Any]:
    """Current AI provider info — tells UI whether DQX or mock is active."""
    try:
        engine = J.get_engine()
        return engine.ai_provider.describe()
    except Exception as exc:
        return {"error": str(exc), "provider": "unknown"}


# ------------------------------------------------------------------ metadata store
_META_COLLECTIONS = ("ruleset_index", "ruleset_versions", "pipelines", "manifests",
                     "policies", "approvals", "rejections", "profiles", "candidates", "jobs")


def _meta_engine():
    from dq_core import DataQualityEngine
    return DataQualityEngine(EngineConfig.from_yaml(ROOT / "config" / "engine.yaml"))


@app.get("/api/metadata/status")
def metadata_status() -> dict[str, Any]:
    """Metadata store backend + per-collection document counts."""
    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    reg = cfg.registry
    out: dict[str, Any] = {"backend": (reg.backend or "file").lower(),
                           "metadata_engine": getattr(reg.metadata, "engine", None),
                           "db_path": getattr(reg.metadata, "db_path", None),
                           "uri": getattr(reg.metadata, "uri", None)}
    try:
        engine = _meta_engine()
        store = getattr(getattr(engine, "ruleset_registry", None), "store", None)
        out["store_type"] = type(store).__name__ if store is not None else ("sqlite" if hasattr(getattr(engine, "ruleset_registry", None), "store") else "file")
        if store is not None and hasattr(store, "collection"):
            out["collections"] = {n: store.collection(n).count() for n in _META_COLLECTIONS}
            out["store_path"] = getattr(store, "path", None)
    except Exception as exc:
        out["error"] = str(exc)[:300]
    return out


@app.get("/api/metadata/collection/{name}")
def metadata_collection(name: str, limit: int = 50) -> dict[str, Any]:
    """Browse documents in a metadata collection (NoSQL backends only)."""
    if name not in _META_COLLECTIONS:
        raise HTTPException(404, f"Unknown collection '{name}'")
    engine = _meta_engine()
    store = getattr(getattr(engine, "ruleset_registry", None), "store", None)
    if store is None or not hasattr(store, "collection"):
        raise HTTPException(400, "Current registry backend is not a document store")
    coll = store.collection(name)
    return {"collection": name, "count": coll.count(), "docs": coll.find()[:limit]}


@app.get("/api/metrics/recent")
def metrics_recent(limit: int = 30, dataset: str | None = None) -> dict[str, Any]:
    """Recent DQ run metrics for trend views — NoSQL metrics collection, JSONL fallback."""
    engine = _meta_engine()
    store = getattr(getattr(engine, "ruleset_registry", None), "store", None)
    docs: list[dict[str, Any]] = []
    if store is not None and hasattr(store, "collection"):
        try:
            docs = store.collection("metrics").find()
        except Exception:
            docs = []
    if not docs:  # JSONL fallback for file/sqlite backends
        import json as _json
        p = _output_base() / "metrics.jsonl"
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines()[-limit * 3:]:
                try:
                    docs.append(_json.loads(line))
                except Exception:
                    continue
    docs = [d for d in docs if not dataset or d.get("dataset") == dataset]
    docs.sort(key=lambda d: str(d.get("recorded_at") or d.get("finished_at") or ""), reverse=True)
    docs = docs[:limit]
    scores = [float(d["quality_score"]) for d in reversed(docs) if d.get("quality_score") is not None]
    return {"count": len(docs), "runs": docs,
            "sparkline": scores[-30:], "avg_score": round(sum(scores) / len(scores), 2) if scores else None}


class MetadataSwitchIn(BaseModel):
    backend: str                     # file | sqlite | nosql
    engine: str | None = None        # tinydb | mongodb | dynamodb (nosql only)


@app.post("/api/metadata/switch")
def metadata_switch(body: MetadataSwitchIn) -> dict[str, Any]:
    """Switch registry.backend (+ metadata.engine) in engine.yaml, then hot-reload."""
    import re
    backend = body.backend.lower()
    if backend not in ("file", "sqlite", "nosql"):
        raise HTTPException(400, "backend must be file|sqlite|nosql")
    engine = (body.engine or "tinydb").lower()
    if engine not in ("tinydb", "mongodb", "dynamodb"):
        raise HTTPException(400, "engine must be tinydb|mongodb|dynamodb")
    path = ROOT / "config" / "engine.yaml"
    text = path.read_text(encoding="utf-8")
    # registry.backend line (first 'backend:' inside registry block)
    text = re.sub(r"(registry:\s*\n(?:.*\n)*?\s*backend:\s*)\S+", rf"\g<1>{backend}", text, count=1)
    if backend == "nosql":
        text = re.sub(r"(metadata:\s*\n(?:.*\n)*?\s*engine:\s*)\S+", rf"\g<1>{engine}", text, count=1)
    path.write_text(text, encoding="utf-8")
    return {"ok": True, "backend": backend, "engine": engine if backend == "nosql" else None,
            "note": "call /api/engine/reload (restart_spark=false) to apply"}


# ------------------------------------------------------------------ health score
@app.get("/api/health/score")
def health_score() -> dict[str, Any]:
    """Overall DQ health 0-100 from recent completed dq_run jobs."""
    rows = J.list_jobs(limit=50)
    runs = [r for r in rows if r.get("kind") in ("dq_run", "auto_e2e") and r.get("status") == "COMPLETED"]
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
            "total_failed_runs": sum(1 for r in rows if r.get("kind") in ("dq_run", "auto_e2e") and r.get("status") == "FAILED")}


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
    # Map run_id -> pipeline_id from reports on disk (fallback for old jobs
    # that didn't store pipeline_id in the job result)
    run2pid: dict[str, str] = {}
    for rep in _read_reports_index():
        m = rep.get("execution_metrics") or {}
        rid = m.get("dq_run_id") or m.get("run_id")
        if rid and m.get("pipeline_id"):
            run2pid[rid] = m["pipeline_id"]
    out = []
    for r in rows:
        if r.get("kind") not in ("dq_run", "auto_e2e") or r.get("status") != "COMPLETED":
            continue
        j = J.get(r["job_id"])
        if not j:
            continue
        res = j.get("result") or {}
        run_id = res.get("run_id", "")
        job_pid = res.get("pipeline_id") or run2pid.get(run_id, "")
        if job_pid != pid:                       # ← filter to THIS pipeline only
            continue
        inp = res.get("input", 0)
        acc = res.get("accepted", 0)
        out.append({
            "job_id": r["job_id"],
            "ran_at": r.get("finished_at") or r.get("submitted_at"),
            "pass_rate": round(acc / inp * 100, 1) if inp else None,
            "accepted": acc, "quarantined": res.get("quarantined", 0),
            "input": inp, "gate_passed": res.get("gate_passed"),
            "run_id": run_id,
        })
        if len(out) >= 30:
            break
    return list(reversed(out))


@app.get("/api/pipelines/{pid}/journey")
def pipeline_journey(pid: str) -> dict[str, Any]:
    """Quality journey for one pipeline: latest-run medallion stage scores
    (Bronze→Silver→Gold with ruleset versions) + score-over-runs history."""
    rows = J.list_jobs(limit=200)
    runs: list[dict[str, Any]] = []
    for r in rows:
        if r.get("kind") not in ("pipeline_run", "dq_run", "auto_e2e") or r.get("status") != "COMPLETED":
            continue
        j = J.get(r["job_id"])
        res = (j or {}).get("result") or {}
        res_pid = res.get("pipeline_id") or (res.get("run") or {}).get("pipeline_id")
        if res_pid != pid:
            continue
        stages = res.get("stages") or (res.get("run") or {}).get("stages") or []
        if not stages:  # dq_run single-stage results carry scores differently
            score = ((res.get("quality_gate") or {}).get("quality_score")
                     or (res.get("summary") or {}).get("quality_score"))
            if score is None:
                continue
            stages = [{"stage": res.get("stage") or "SILVER", "quality_score": score,
                       "ruleset": res.get("ruleset"), "accepted": res.get("accepted"),
                       "quarantined": res.get("quarantined"), "gate_passed": res.get("gate_passed")}]
        runs.append({
            "job_id": r["job_id"],
            "ran_at": r.get("finished_at") or r.get("submitted_at"),
            "gate_passed": res.get("gate_passed"),
            "stages": stages,
        })
    if not runs:
        return {"pipeline_id": pid, "stages": [], "history": []}
    latest = runs[0]

    def _norm_stage(s: dict[str, Any]) -> dict[str, Any]:
        score = s.get("quality_score")
        if score is None and s.get("pass_rate") is not None:
            score = s["pass_rate"]
        return {"stage": s.get("stage"), "score": score,
                "ruleset": s.get("ruleset"), "status": s.get("status"),
                "accepted": s.get("accepted"), "quarantined": s.get("quarantined"),
                "rows": s.get("rows") or s.get("input"),
                "gate": s.get("gate_passed", s.get("gate"))}

    stages = [_norm_stage(s) for s in latest["stages"]]
    history = []
    for run in runs[:20]:
        scores = [float(s["quality_score"]) for s in run["stages"]
                  if isinstance(s.get("quality_score"), (int, float))]
        if not scores:  # fall back to pass_rate style fields
            scores = [float(s["pass_rate"]) for s in run["stages"]
                      if isinstance(s.get("pass_rate"), (int, float))]
        versions = sorted({str(s.get("ruleset")) for s in run["stages"] if s.get("ruleset")})
        history.append({"ran_at": run["ran_at"], "score": round(sum(scores) / len(scores), 1) if scores else None,
                        "versions": versions, "gate_passed": run["gate_passed"]})
    return {"pipeline_id": pid, "latest_run_at": latest["ran_at"],
            "latest_job_id": latest["job_id"], "stages": stages, "history": list(reversed(history))}


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
_REJECTIONS_FILE = ROOT / "config" / "approvals" / "rejected_corrections.yaml"


def _load_yaml_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    import yaml as _yaml
    with open(path, encoding="utf-8") as f:
        return (_yaml.safe_load(f) or {}).get("rejections" if "rejected" in path.name else "approvals") or []


def _actioned_keys() -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str]]]:
    """(strict, loose) keys for corrections already approved or rejected.

    strict = (dataset, column, original_value, proposed/approved_value)
    loose  = (column, original_value, proposed/approved_value) — used when the
             pending row's dataset is unknown."""
    strict: set[tuple[str, str, str, str]] = set()
    loose: set[tuple[str, str, str]] = set()
    for path, value_key in ((_APPROVALS_FILE, "approved_value"), (_REJECTIONS_FILE, "proposed_value")):
        for item in _load_yaml_list(path):
            ds = str(item.get("dataset") or "")
            col = str(item.get("column") or "")
            orig = str(item.get("original_value") or "")
            val = str(item.get(value_key) or item.get("corrected_value") or "")
            if col and orig:
                strict.add((ds, col, orig, val))
                loose.add((col, orig, val))
    return strict, loose


def _is_actioned(rec: dict[str, Any], strict: set, loose: set) -> bool:
    col = str(rec.get("column") or "")
    orig = str(rec.get("original_value") or "")
    prop = str(rec.get("proposed_value") or "")
    ds = str(rec.get("dataset") or "")
    if (ds, col, orig, prop) in strict:
        return True
    return ds in ("", "unknown") and (col, orig, prop) in loose


def _load_quarantine_pending() -> list[dict[str, Any]]:
    """Scan last 10 completed REMEDIATE jobs — read ai_decisions from report JSON
    and return rows with decision == REQUIRES_APPROVAL (PENDING_HUMAN_APPROVAL)."""
    rows = J.list_jobs(limit=100)
    runs = [r for r in rows if r.get("kind") in ("dq_run", "auto_e2e") and r.get("status") == "COMPLETED"][:10]
    pending: list[dict[str, Any]] = []
    seen: set[str] = set()
    strict_keys, loose_keys = _actioned_keys()
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
                rec = {
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
                }
                if _is_actioned(rec, strict_keys, loose_keys):
                    continue
                pending.append(rec)
        except Exception:
            pending.append({"job_id": r["job_id"], "count": qbs["PENDING_HUMAN_APPROVAL"],
                            "summary_only": True})
    return pending


@app.get("/api/quarantine/pending")
def quarantine_pending() -> dict[str, Any]:
    rows = _load_quarantine_pending()
    return {"total": sum(r.get("count", 1) for r in rows), "records": rows[:200]}


@app.get("/api/quarantine/decisions")
def quarantine_decisions() -> dict[str, Any]:
    """Audit trail of steward decisions — approved corrections + rejections."""
    approvals = [{k: item.get(k) for k in
                  ("dataset", "column", "original_value", "approved_value", "rule_id",
                   "approved_by", "approved_at", "reason")}
                 for item in _load_yaml_list(_APPROVALS_FILE)]
    rejections = [{k: item.get(k) for k in
                   ("dataset", "column", "original_value", "proposed_value", "rule_id",
                    "rejected_by", "rejected_at", "reason")}
                  for item in _load_yaml_list(_REJECTIONS_FILE)]
    return {"approvals": approvals[::-1], "rejections": rejections[::-1]}


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
    try:  # dual-write into NoSQL approvals collection when doc store active
        from dq_core.metadata import resolver as _res
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            _res.record_approval(store, approvals[-1])
    except Exception:
        pass
    return {"ok": True, "total_approved": len(approvals)}


@app.post("/api/quarantine/reject")
def reject_correction(body: ApprovalAction) -> dict[str, Any]:
    import yaml as _yaml
    from dq_core.runtime.utils import utc_now_iso
    _REJECTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    doc: dict[str, Any] = {}
    if _REJECTIONS_FILE.exists():
        with open(_REJECTIONS_FILE, encoding="utf-8") as f:
            doc = _yaml.safe_load(f) or {}
    rejections = doc.setdefault("rejections", [])
    rejections.append({
        "dataset": body.dataset, "column": body.column,
        "original_value": body.original_value,
        "proposed_value": body.corrected_value, "rule_id": body.rule_id,
        "rejected_by": body.approved_by, "rejected_at": utc_now_iso(),
        "reason": body.reason,
    })
    with open(_REJECTIONS_FILE, "w", encoding="utf-8") as f:
        _yaml.safe_dump(doc, f, sort_keys=False, allow_unicode=True)
    try:  # dual-write into NoSQL rejections collection when doc store active
        from dq_core.metadata import resolver as _res
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            _res.record_rejection(store, rejections[-1])
    except Exception:
        pass
    return {"ok": True, "status": "REJECTED", "column": body.column,
            "original_value": body.original_value}


# ------------------------------------------------------------------ stage data preview
_STAGE_DIRS = {"bronze": "bronze", "silver": "silver", "gold": "gold"}


@app.get("/api/data/available")
def data_available() -> dict[str, Any]:
    """List datasets available per medallion stage (dirs under data/)."""
    out: dict[str, list[str]] = {}
    for stage in _STAGE_DIRS:
        d = ROOT / "data" / stage
        out[stage] = sorted(p.name for p in d.iterdir() if p.is_dir()) if d.exists() else []
    q = ROOT / "data" / "quarantine"
    out["quarantine"] = sorted(p.name for p in q.iterdir() if p.is_dir()) if q.exists() else []
    return out


@app.get("/api/data/preview")
def data_preview(dataset: str, stage: str = "silver", limit: int = 50) -> dict[str, Any]:
    """Preview actual output rows — reads the stage parquet dir via Spark."""
    stage = stage.lower()
    if stage == "quarantine":
        # quarantine layout: data/quarantine/<substage>/<dataset>
        base = ROOT / "data" / "quarantine"
        hits = [p for p in base.glob(f"*/{dataset}") if p.is_dir()]
        if not hits:
            raise HTTPException(404, f"no quarantine data for {dataset}")
        path = hits[0]
    else:
        if stage not in _STAGE_DIRS:
            raise HTTPException(400, f"stage must be one of {list(_STAGE_DIRS)}|quarantine")
        path = (ROOT / "data" / _STAGE_DIRS[stage] / dataset).resolve()
        if not str(path).startswith(str(ROOT.resolve())) or not path.exists():
            raise HTTPException(404, f"no {stage} data for {dataset}")
    spark = J.get_spark()
    df = spark.read.parquet(str(path))
    rows = [r.asDict(recursive=True) for r in df.limit(min(limit, 200)).collect()]
    cols = df.columns
    return {"dataset": dataset, "stage": stage, "columns": cols,
            "rows": rows, "shown": len(rows)}


@app.get("/api/data/source-preview")
def source_preview(source: str, limit: int = 20) -> dict[str, Any]:
    """Quick peek at any source — columns + first N rows (no full scan)."""
    spark = J.get_spark()
    df = _read_any_source(spark, source, "AUTO")
    rows = [r.asDict(recursive=True) for r in df.limit(min(limit, 100)).collect()]
    return {"source": source, "columns": df.columns, "rows": rows, "shown": len(rows)}


@app.get("/api/profiles")
def profiles_list() -> list[dict[str, Any]]:
    """Stored profile baselines (written during onboard preview) — newest first."""
    from dq_core.metadata import resolver as _res
    store = _res.store_from_engine(J.get_engine())
    if store is None:
        return []
    docs = store.collection("profiles").find({})
    docs.sort(key=lambda d: str(d.get("profiled_at") or ""), reverse=True)
    return [{"dataset": d.get("dataset"), "ruleset_id": d.get("ruleset_id"),
             "stage": d.get("stage"), "profiled_at": d.get("profiled_at"),
             "row_count": d.get("row_count"), "profile_mode": d.get("profile_mode")}
            for d in docs]


@app.get("/api/profiles/{dataset}")
def profile_detail(dataset: str) -> dict[str, Any]:
    """Latest stored profile for a dataset — full column statistics."""
    from dq_core.metadata import resolver as _res
    store = _res.store_from_engine(J.get_engine())
    if store is None:
        raise HTTPException(404, "metadata store unavailable")
    docs = [d for d in store.collection("profiles").find({"dataset": dataset})]
    if not docs:
        raise HTTPException(404, f"no profile for {dataset}")
    docs.sort(key=lambda d: str(d.get("profiled_at") or ""), reverse=True)
    return docs[0]


@app.get("/api/tables")
def tables() -> list[str]:
    """Tables/views visible to the shared Spark session."""
    spark = J.get_spark()
    return [r.tableName for r in spark.catalog.listTables()]


class TableIn(BaseModel):
    source: str                  # repo-relative file to register as a view
    view_name: str


@app.post("/api/tables/register")
def register_table(body: TableIn) -> dict[str, Any]:
    """Register a local file as a Spark temp view — makes TABLE sources work locally."""
    spark = J.get_spark()
    src = (ROOT / body.source).resolve()
    if not str(src).startswith(str(ROOT.resolve())) or not src.exists():
        raise HTTPException(400, f"source not found under repo: {body.source}")
    df = _read_source(spark, src)
    df.createOrReplaceTempView(body.view_name)
    return {"ok": True, "view": body.view_name, "rows": df.count()}


@app.get("/api/ai/activity")
def ai_activity(limit: int = 150) -> dict[str, Any]:
    """Flattened AI remediation activity across recent reports — decisions + outcomes."""
    out: list[dict[str, Any]] = []
    rdir = ROOT / "reports"
    files = sorted(rdir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:30] if rdir.exists() else []
    prov: dict[str, Any] = {}
    for f in files:
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        rr = d.get("remediation_results") or {}
        prov = (rr.get("summary") or {}).get("ai_provider") or prov
        sim = rr.get("ai_outputs_are_simulated", prov.get("simulated"))
        for dec in (rr.get("ai_decisions") or []):
            out.append({"report": f.stem, "dataset": d.get("dataset"), "kind": "AI_DECISION",
                        **dec, "simulated": sim})
        for it in ((rr.get("outcome_audit") or {}).get("items") or []):
            out.append({"report": f.stem, "dataset": d.get("dataset"), "kind": "OUTCOME",
                        **it, "simulated": sim})
        if len(out) >= limit:
            break
    return {"total": len(out), "records": out[:limit], "provider": prov}


# ------------------------------------------------------------------ auto end-to-end
class AutoIn(BaseModel):
    source: str
    dataset: str | None = None
    pipeline_id: str
    stage: str = "SILVER"
    source_format: str = "AUTO"
    platform: str = "STANDALONE_SPARK"
    policy: str = "config/policies/default.yaml"
    owner: str = "auto-e2e"
    mode: str = "REMEDIATE"
    approve_confidence: float = 0.0   # auto-approve candidates at/above this


@app.post("/api/auto/run")
def auto_run(body: AutoIn) -> dict[str, Any]:
    """One-shot E2E: profile -> auto-approve rules -> publish+onboard -> governed DQ run."""
    jid = J.submit("auto_e2e", lambda: _do_auto_e2e(body))
    return {"job_id": jid}


def _purge_pipeline_artifacts(ruleset_id: str, pipeline_id: str) -> None:
    """Remove prior ruleset/manifest/pipeline artifacts so auto-e2e reruns are idempotent.

    Published ruleset versions are immutable — a regenerated ruleset for the same
    ruleset_id gets a different checksum (fresh profile evidence), so re-publishing
    @1.0 would fail verification. Purging first gives each auto run a clean slate.
    """
    import json as _json
    from dq_core.metadata import resolver as _res
    store = _res.store_from_engine(J.get_engine())
    if store is not None:
        for coll in ("ruleset_versions", "ruleset_index", "pipelines", "manifests"):
            try:
                col = store.collection(coll)
                for d in col.find({}):
                    if ruleset_id.lower() in _json.dumps(d).lower() or pipeline_id.lower() in _json.dumps(d).lower():
                        for m in ("delete_one", "remove_one", "remove", "delete"):
                            if hasattr(col, m):
                                try:
                                    q = {"_id": d["_id"]} if "_id" in d else {
                                        k: d[k] for k in ("ruleset_id", "version", "pipeline_id") if k in d}
                                    getattr(col, m)(q)
                                    break
                                except Exception:
                                    pass
            except Exception:
                pass
    ds_name = ruleset_id.rsplit("_", 1)[0].lower()   # ruleset_id = "<dataset>_<stage>"
    for p in (ROOT / "config" / "rulesets", ROOT / "config" / "pipelines"):
        if p.exists():
            for f in p.glob("*.yaml"):
                stem = f.stem.lower()
                if stem.startswith(ds_name) or pipeline_id.lower() in stem:
                    try:
                        f.unlink()
                    except Exception:
                        pass


def _do_auto_e2e(body: AutoIn) -> dict[str, Any]:
    _prog("phase=profile | auto-e2e: profiling source")
    res = _do_bootstrap_preview(PreviewIn(source=body.source, dataset=body.dataset,
                                        stage=body.stage, source_format=body.source_format))
    approved = [c["rule_id"] for c in res["candidates"]
                if (c.get("confidence") or 0) >= body.approve_confidence]
    ds = body.dataset or res["dataset"]
    _prog(f"phase=profile | profiled {res.get('row_count') or '?'} rows · "
          f"{len(res.get('anomalies') or [])} anomalies · {len(res.get('candidates') or [])} candidate rules · "
          f"{len(approved)} auto-approved")
    _purge_pipeline_artifacts(res["ruleset_id"], body.pipeline_id.upper())
    _prog(f"phase=validate | auto-e2e: {len(approved)}/{res['candidate_count']} rules approved — writing ruleset + manifest")
    plat = body.platform.upper()
    if plat in ("LOCAL", "LOCAL_SPARK", "SPARK"):
        plat = "STANDALONE_SPARK"
    fout = onboard_finalize(FinalizeIn(
        preview_result=res, approved_rule_ids=approved, dataset=ds,
        ruleset_id=res["ruleset_id"], pipeline_id=body.pipeline_id.upper(),
        platform=plat, stage=body.stage, dq_mode=body.mode,
        policy=body.policy, owner=body.owner, source=body.source,
        source_format=body.source_format, publish=True, onboard=True, force=True))
    # ── Gold stage: stricter re-validation — full Bronze→Silver→Gold in one run ──
    try:
        _prog("phase=validate | auto-e2e: building GOLD ruleset (stricter re-check)")
        import yaml as _yaml
        from dq_core.metadata.models import RuleSet as _RS
        from dq_core.metadata.serialization import dump_yaml as _dump
        approved_set = set(approved)
        gold_dict = {"ruleset_id": f"{ds}_gold", "version": "1.0", "stage": "GOLD",
                     "dataset": ds, "rules": []}
        for c in res["candidates"]:
            if c["rule_id"] in approved_set and isinstance(c.get("raw"), dict):
                raw = dict(c["raw"]); raw["stage"] = "GOLD"; raw["ruleset_id"] = f"{ds}_gold"
                raw["status"] = "APPROVED"
                gold_dict["rules"].append(raw)
        if gold_dict["rules"]:
            gold_rs = _RS.from_dict(gold_dict)
            gpath = ROOT / "config" / "rulesets" / f"{ds}_gold_v1.yaml"
            from dq_core.metadata.serialization import save_ruleset as _sv
            _sv(gold_rs, gpath)
            _registry("rulesets").publish(gold_rs, published_by=body.owner or "auto-e2e", activate=True)
            mpath = ROOT / "config" / "pipelines" / f"{body.pipeline_id.upper()}.yaml"
            m = _yaml.safe_load(mpath.read_text(encoding="utf-8"))
            rel = os.path.relpath(gpath, mpath.parent).replace("\\", "/")
            m["pipeline"]["datasets"][0]["rulesets"]["GOLD"] = rel
            if "GOLD" not in m["pipeline"]["datasets"][0].get("stages", []):
                m["pipeline"]["datasets"][0].setdefault("stages", []).append("GOLD")
            _dump(m, mpath)
            fout["gold_ruleset"] = f"{ds}_gold@1.0"
    except Exception as exc:
        print(f"[auto-e2e] gold setup failed (silver still runs): {exc}")
    _prog(f"phase=validate | ruleset {fout.get('ruleset')} + manifest written · "
          f"pipeline {body.pipeline_id.upper()} registered · gold ruleset "
          f"{fout.get('gold_ruleset') or 'n/a'}")
    _prog(f"phase=read | auto-e2e: running governed pipeline {body.pipeline_id.upper()} (bronze→silver→gold)")
    run = _do_pipeline_run(body.pipeline_id.upper(), PipelineRunIn(dataset_id=ds, mode=body.mode))
    stages = run.get("stages") or []
    silver = next((s for s in stages if s.get("stage") == "SILVER"), {})
    import glob as _g
    reps = sorted(_g.glob(str(ROOT / "reports" / f"{ds}*silver*.json")), key=os.path.getmtime)
    return {"dataset": ds, "ruleset": fout.get("ruleset"), "rules_approved": len(approved),
            "rules_rejected": fout.get("rules_rejected"), "run": run,
            "input": silver.get("input") or silver.get("input_rows"),
            "accepted": silver.get("accepted"), "quarantined": silver.get("quarantined"),
            "gate_passed": run.get("gate_passed"),
            "report_path": reps[-1] if reps else ""}


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
    source: str                  # path relative to repo root, OR table name / URI for TABLE|DELTA|JSON
    dataset: str | None = None
    stage: str = "SILVER"
    source_format: str = "AUTO"  # AUTO|CSV|PARQUET|JSON|DELTA|TABLE


def _read_any_source(spark: Any, source: str, fmt: str):
    """Read a source by explicit format — files stay under repo root; TABLE/DELTA/URI pass through."""
    from dq_core.io.sources import read_source, SourceSpec
    f = (fmt or "AUTO").upper()
    if f in ("TABLE", "DELTA", "JSON"):
        _prog(f"phase=read | reading {f} source {source}")
        return read_source(spark, SourceSpec(format=f, path=source))
    src = (ROOT / source).resolve()
    if not str(src).startswith(str(ROOT.resolve())) or not src.exists():
        raise HTTPException(400, f"source not found under repo: {source}")
    if f in ("CSV", "PARQUET"):
        _prog(f"phase=read | reading {source} ({f})")
        return read_source(spark, SourceSpec(format=f, path=str(src)))
    return _read_source(spark, src)  # AUTO — suffix inference


def _do_bootstrap_preview(body: PreviewIn) -> dict[str, Any]:
    """Profile + candidate rules for the wizard — same path as `dqctl bootstrap`."""
    from dq_core.generation.generator import CandidateRuleGenerator
    spark, engine = _spark_engine()
    ds = body.dataset or Path(body.source).stem
    src_path = (ROOT / body.source).resolve()
    mode = _profile_mode_for(src_path, "FULL", engine) if src_path.suffix and src_path.exists() else "SAMPLE"
    df = _read_any_source(spark, body.source, body.source_format)
    _prog(f"phase=profile | profiling {ds} ({mode}) — computing column statistics")
    rep = engine.profile(df, ds, mode=mode, on_phase=lambda m: _prog(f"phase=profile | {m}"))
    # drift vs last stored baseline profile (profiles collection — written at finalize)
    try:
        from dq_core.metadata import resolver as _res
        from dq_core.profiling.drift import detect_drift
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            docs = store.collection("profiles").find({"dataset": ds})
            if docs:
                base_cols = ((docs[-1].get("profile") or {}).get("columns")) or {}
                for f in detect_drift(rep.columns, base_cols):
                    rep.anomalies.append(f.to_anomaly())
    except Exception:
        pass
    _prog("phase=profile | generating candidate rules from profile")
    ruleset_id = f"{ds}_{body.stage.lower()}"
    # persist profile baseline — feeds drift detection + Profiles viewer (best-effort)
    try:
        from dq_core.metadata import resolver as _res
        from dq_core.runtime.utils import utc_now_iso
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            store.collection("profiles").insert_one({
                "dataset": ds, "ruleset_id": ruleset_id, "stage": body.stage,
                "profiled_at": utc_now_iso(), "row_count": getattr(rep, "row_count", None),
                "profile_mode": mode, "profile": rep.to_dict()})
    except Exception as exc:
        print(f"[onboard] profile persist failed: {exc}")
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
    try:
        jid = J.submit("bootstrap_preview", lambda: _do_bootstrap_preview(body))
    except J.JobConflict as e:
        raise HTTPException(409, str(e))
    return {"job_id": jid}


class FinalizeIn(BaseModel):
    preview_job_id: str | None = None
    preview_result: dict | None = None   # inline result for auto-e2e (skips job lookup)
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
    source_format: str = "AUTO"  # AUTO|CSV|PARQUET|JSON|DELTA|TABLE
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

    if body.preview_result is not None:
        res = body.preview_result
    else:
        job = J.get(body.preview_job_id or "")
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
    src_fmt = (body.source_format or "AUTO").upper()
    if src_fmt == "AUTO":
        src_fmt = ({"csv": "CSV", "parquet": "PARQUET", "json": "JSON"}
                   .get(src.suffix.lower().lstrip("."), "PARQUET") if src.exists() else "TABLE")
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
            "format": src_fmt,
            "stages": ["BRONZE", body.stage.upper()],
            "rulesets": {body.stage.upper(): os.path.relpath(ruleset_path,
                                                             manifest_path.parent).replace("\\", "/")},
            "test_data": test_data,
            "test_data_format": src_fmt,
        }],
    }}
    dump_yaml(manifest, manifest_path)
    try:
        mf = PipelineManifest.load(manifest_path)
    except Exception as exc:
        raise HTTPException(400, f"generated manifest failed validation: {exc}") from exc

    try:  # persist onboarding artifacts to NoSQL meta collections (best-effort)
        from dq_core.metadata import resolver as _res
        store = _res.store_from_engine(J.get_engine())
        if store is not None:
            _res.save_manifest(store, mf)
            store.collection("candidates").insert_one({
                "ruleset_id": body.ruleset_id, "dataset": body.dataset, "version": "1.0",
                "stage": body.stage, "candidate_count": len(res["candidates"]),
                "approved_count": len(rs.rules), "rules": [c["raw"] for c in res["candidates"]],
                "created_at": __import__("dq_core.runtime.utils", fromlist=["utc_now_iso"]).utc_now_iso()})
            prof = res.get("profile") or res.get("profile_report")
            if prof:
                store.collection("profiles").insert_one({
                    "dataset": body.dataset, "ruleset_id": body.ruleset_id,
                    "profiled_at": prof.get("profiled_at") if isinstance(prof, dict) else None,
                    "row_count": res.get("row_count"), "profile": prof})
    except Exception as exc:
        print(f"[onboard] finalize persist failed: {exc}")
    out = {"ok": True, "ruleset": f"{body.ruleset_id}@1.0",
           "ruleset_path": str(ruleset_path.relative_to(ROOT)).replace("\\", "/"),
           "manifest_path": str(manifest_path.relative_to(ROOT)).replace("\\", "/"),
           "rules_approved": len(rs.rules), "rules_rejected": len(cand_rs.rules) - len(rs.rules)}
    warnings: list[str] = []
    if body.publish:
        try:
            entry = _registry("rulesets").publish(rs, published_by=body.approved_by, activate=True)
            out["published"] = {"checksum": entry["checksum"][:12]}
        except Exception as exc:
            warnings.append(f"publish failed: {str(exc)[:200]}")
    if body.onboard:
        try:
            entry = _registry("pipelines").onboard(mf, actor=body.approved_by)
            out["onboarded"] = {"pipeline_id": pid, "state": entry["state"]}
        except Exception as exc:
            warnings.append(f"onboard failed: {str(exc)[:200]}")
    if warnings:
        out["warnings"] = warnings
    out["manifest_yaml"] = manifest_path.read_text(encoding="utf-8")
    return out
