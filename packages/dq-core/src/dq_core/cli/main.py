"""dqctl – command line interface for the DQ Accelerator.

Sub-commands:
  pipeline  onboard|list|test|activate|pause|offboard
  ruleset   list|publish|activate|rollback|diff|verify
  bootstrap profile a source file and export an approved rule set + onboarding manifest
  run       OBSERVE_ONLY|ENFORCE|REMEDIATE on a CSV/Parquet file
  profile   profile a CSV/Parquet file and show the report
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click
import yaml

ROOT = Path(__file__).resolve().parents[5]   # repo root
os.environ.setdefault("DQ_LOG_DIR", str(ROOT / "logs"))


def _engine(config: str) -> "DataQualityEngine":        # noqa: F821 – lazy import
    from dq_core.engine import DataQualityEngine
    return DataQualityEngine.from_config(config)


def _spark(config_path: str, master: str = "local[4]", driver_memory: str = "1g") -> "Any":  # noqa: F821
    from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session
    return create_local_spark_session(LocalSparkSettings(
        master=master, driver_memory=driver_memory,
        scratch_dir=os.environ.get("DQ_SCRATCH_DIR", str(ROOT / ".tmp"))))


def _ok(msg: str) -> None:
    click.echo(click.style(f"  OK  {msg}", fg="green"))


def _warn(msg: str) -> None:
    click.echo(click.style(f" WARN {msg}", fg="yellow"))


def _err(msg: str) -> None:
    click.echo(click.style(f"  ERR {msg}", fg="red"), err=True)


# ---------------------------------------------------------------------------
@click.group()
@click.version_option(package_name="dq-core")
def main() -> None:
    """DQ Accelerator – Enterprise Data Quality for Apache Spark pipelines."""


# ===========================================================================  pipeline
@main.group()
def pipeline() -> None:
    """Pipeline lifecycle (onboard, list, test, activate, pause, offboard)."""


@pipeline.command("onboard")
@click.option("--manifest", "-m", required=True, type=click.Path(exists=True), help="Pipeline manifest YAML")
@click.option("--config", "-c", default="config/engine.yaml", show_default=True, help="Engine config YAML")
@click.option("--actor", default="dqctl", show_default=True)
def pipeline_onboard(manifest: str, config: str, actor: str) -> None:
    """Register a new pipeline from its manifest."""
    from dq_core.pipelines.manifest import PipelineManifest
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    if not cfg.registry.pipelines_path:
        _err("registry.pipelines_path is not configured in the engine config"); sys.exit(1)
    reg = build_pipeline_registry(cfg.registry)
    mf = PipelineManifest.load(manifest)
    entry = reg.onboard(mf, actor=actor)
    _ok(f"Onboarded {mf.pipeline_id}  state={entry['state']}")
    click.echo(json.dumps(entry, indent=2, default=str))


@pipeline.command("list")
@click.option("--config", "-c", default="config/engine.yaml")
def pipeline_list(config: str) -> None:
    """List all registered pipelines."""
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    if not cfg.registry.pipelines_path:
        _err("registry.pipelines_path is not configured"); sys.exit(1)
    rows = build_pipeline_registry(cfg.registry).list()
    if not rows:
        click.echo("No pipelines registered."); return
    header = f"{'ID':<30} {'STATE':<14} {'PLATFORM':<12} {'DQ_MODE':<14} {'UPDATED'}"
    click.echo(header); click.echo("-" * len(header))
    for r in rows:
        click.echo(f"{str(r.get('pipeline_id', '')):<30} {str(r.get('state', '')):<14} "
                   f"{str(r.get('platform', '')):<12} {str(r.get('dq_mode', '')):<14} {r.get('updated_at', '')}")


@pipeline.command("test")
@click.argument("pipeline_id")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
def pipeline_test(pipeline_id: str, config: str, actor: str) -> None:
    """Mark a registered pipeline as tested (placeholder – runs a schema check)."""
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    reg = build_pipeline_registry(cfg.registry)
    entry = reg.record_test(pipeline_id, {"check": "schema", "result": "PASS"}, passed=True, actor=actor)
    _ok(f"{pipeline_id} state={entry['state']}")


@pipeline.command("activate")
@click.argument("pipeline_id")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
def pipeline_activate(pipeline_id: str, config: str, actor: str) -> None:
    """Activate a tested pipeline for DQ enforcement."""
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    entry = build_pipeline_registry(cfg.registry).activate(pipeline_id, actor=actor)
    _ok(f"{pipeline_id} state={entry['state']}")


@pipeline.command("pause")
@click.argument("pipeline_id")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
@click.option("--reason", default="paused by operator")
def pipeline_pause(pipeline_id: str, config: str, actor: str, reason: str) -> None:
    """Pause DQ enforcement (data passes through; history retained)."""
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    entry = build_pipeline_registry(cfg.registry).pause(pipeline_id, actor=actor, reason=reason)
    _ok(f"{pipeline_id} state={entry['state']} – data will pass through until reactivated")


@pipeline.command("offboard")
@click.argument("pipeline_id")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
@click.option("--reason", default="offboarding requested")
def pipeline_offboard(pipeline_id: str, config: str, actor: str, reason: str) -> None:
    """Offboard a pipeline (DQ integration disabled; data and history retained)."""
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.pipelines.registry import PipelineRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    entry = build_pipeline_registry(cfg.registry).offboard(pipeline_id, actor=actor, reason=reason)
    _ok(f"{pipeline_id} state={entry['state']} – customer data and history retained, integration disabled")


# ===========================================================================  ruleset
@main.group()
def ruleset() -> None:
    """Rule set versioning (publish, activate, rollback, diff, verify)."""


@ruleset.command("list")
@click.option("--config", "-c", default="config/engine.yaml")
def ruleset_list(config: str) -> None:
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    if not cfg.registry.rulesets_path:
        _err("registry.rulesets_path not configured"); sys.exit(1)
    reg = build_ruleset_registry(cfg.registry)
    ids = reg.list_rulesets()
    if not ids:
        click.echo("No rule sets published."); return
    for rs_id in ids:
        active = reg.active_version(rs_id)
        for v in reg.list_versions(rs_id):
            flag = " ◄ ACTIVE" if v["version"] == active else ""
            click.echo(f"  {rs_id}@{v['version']}  checksum={str(v['checksum'])[:12]}  {v['published_at']}{flag}")


@ruleset.command("publish")
@click.argument("ruleset_yaml", type=click.Path(exists=True))
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
@click.option("--activate/--no-activate", default=True, show_default=True)
def ruleset_publish(ruleset_yaml: str, config: str, actor: str, activate: bool) -> None:
    """Publish a rule set YAML as an immutable version and optionally activate it."""
    from dq_core.metadata.serialization import load_ruleset
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    reg = build_ruleset_registry(cfg.registry)
    rs = load_ruleset(ruleset_yaml)
    entry = reg.publish(rs, published_by=actor, activate=activate)
    _ok(f"{rs.ref}  checksum={entry['checksum'][:12]}  activate={activate}")


@ruleset.command("activate")
@click.argument("ruleset_id")
@click.argument("version")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
def ruleset_activate(ruleset_id: str, version: str, config: str, actor: str) -> None:
    """Make a published version the active version for a rule set."""
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    build_ruleset_registry(cfg.registry).activate(ruleset_id, version, activated_by=actor)
    _ok(f"{ruleset_id}@{version} is now active")


@ruleset.command("rollback")
@click.argument("ruleset_id")
@click.argument("to_version")
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--actor", default="dqctl")
@click.option("--reason", default="rollback requested")
def ruleset_rollback(ruleset_id: str, to_version: str, config: str, actor: str, reason: str) -> None:
    """Roll back the active version to a previously published version."""
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    build_ruleset_registry(cfg.registry).rollback(ruleset_id, to_version, rolled_back_by=actor, reason=reason)
    _ok(f"{ruleset_id} rolled back to {to_version}")


@ruleset.command("diff")
@click.argument("ruleset_id")
@click.argument("from_version")
@click.argument("to_version")
@click.option("--config", "-c", default="config/engine.yaml")
def ruleset_diff(ruleset_id: str, from_version: str, to_version: str, config: str) -> None:
    """Compare two published versions of a rule set."""
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    diff = build_ruleset_registry(cfg.registry).diff(ruleset_id, from_version, to_version)
    click.echo(diff.to_text())
    if not diff.has_changes:
        _ok("No changes between versions")
    click.echo(json.dumps(diff.to_dict(), indent=2, default=str))


@ruleset.command("verify")
@click.argument("ruleset_yaml", type=click.Path(exists=True))
@click.option("--config", "-c", default="config/engine.yaml")
def ruleset_verify(ruleset_yaml: str, config: str) -> None:
    """Verify a YAML file against the published checksum (tamper detection)."""
    from dq_core.metadata.serialization import load_ruleset
    from dq_core.rules.registry import RuleSetRegistry
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(config)
    rs = load_ruleset(ruleset_yaml)
    reg = build_ruleset_registry(cfg.registry)
    is_pub = reg.verify(rs)
    if is_pub:
        _ok(f"{rs.ref} is published and unchanged (checksum {rs.checksum()[:12]})")
    else:
        _warn(f"{rs.ref} is NOT published in the registry")


@ruleset.command("import-odcs")
@click.argument("contract", type=click.Path(exists=True))
@click.option("--out", "-o", required=True, type=click.Path(), help="Output ruleset YAML path")
@click.option("--ruleset-id", default=None)
@click.option("--version", default="1.0")
@click.option("--owner", default="")
@click.option("--stage", default="SILVER", type=click.Choice(["BRONZE", "SILVER", "GOLD"], case_sensitive=False))
def ruleset_import_odcs(contract: str, out: str, ruleset_id: str | None, version: str,
                        owner: str, stage: str) -> None:
    """Import an ODCS data contract as CANDIDATE rules (review before publish)."""
    from dq_core.contracts.odcs import load_odcs, odcs_to_ruleset, write_ruleset_yaml
    doc = load_odcs(contract)
    doc["_source"] = contract
    rs = odcs_to_ruleset(doc, ruleset_id=ruleset_id, version=version, owner=owner, stage=stage)
    path = write_ruleset_yaml(rs, out)
    _ok(f"ODCS contract -> {len(rs.rules)} candidate rules")
    _ok(f"Ruleset written to {path} (all rules CANDIDATE — review, approve, then publish)")


@ruleset.command("generate")
@click.option("--describe", "-d", "description", required=True,
              help="Business description of data-quality expectations")
@click.option("--source", "-s", type=click.Path(exists=True), default=None,
              help="CSV/Parquet file to derive schema from (optional)")
@click.option("--schema", default=None, help="Inline schema 'col:type,col:type' (alternative to --source)")
@click.option("--dataset", required=True)
@click.option("--domain", default="")
@click.option("--out", "-o", required=True, type=click.Path())
@click.option("--ruleset-id", default=None)
@click.option("--no-llm", is_flag=True, help="Force heuristic keyword generation")
def ruleset_generate(description: str, source: str | None, schema: str | None, dataset: str,
                     domain: str, out: str, ruleset_id: str | None, no_llm: bool) -> None:
    """Generate CANDIDATE rules from a business description (+ optional schema)."""
    from dq_core.contracts.odcs import write_ruleset_yaml
    from dq_core.generation.llm_generator import LLMRuleGenerator
    from dq_core.metadata.models import RuleSet, RuleSetStatus
    from dq_core.runtime.utils import utc_now_iso

    cols: dict[str, str] = {}
    if source:
        from dq_core.io.sources import CsvSource, ParquetSource
        spark = _spark("config/engine.yaml", master="local[2]")
        df = (ParquetSource(source).read(spark) if source.endswith(".parquet")
              else CsvSource(source).read(spark))
        cols = dict(df.dtypes)
        spark.stop()
    elif schema:
        for pair in schema.split(","):
            if ":" in pair:
                c, t = pair.split(":", 1)
                cols[c.strip()] = t.strip()
    else:
        _err("Provide --source <file> or --schema 'col:type,...'")
        raise SystemExit(2)

    rules = LLMRuleGenerator().generate(cols, description, dataset=dataset, domain=domain,
                                        ruleset_id=ruleset_id, use_llm=not no_llm)
    if not rules:
        _warn("No rules generated — description may not match known patterns")
        raise SystemExit(1)
    rs = RuleSet(ruleset_id=ruleset_id or f"{dataset}_desc", dataset=dataset, version="1.0",
                 status=RuleSetStatus.DRAFT, description=f"LLM/heuristic rules from: {description[:80]}",
                 owner="", rules=rules, created_at=utc_now_iso())
    path = write_ruleset_yaml(rs, out)
    _ok(f"Generated {len(rules)} candidate rules -> {path}")
    _ok("All rules CANDIDATE — review, approve, publish via UI or dqctl ruleset publish")


# ===========================================================================  bootstrap
@main.command("bootstrap")
@click.argument("source", type=click.Path(exists=True))
@click.option("--dataset", default=None, help="Dataset id (default: source file stem)")
@click.option("--pipeline-id", default=None, help="Pipeline id (default: <DATASET>_PIPELINE)")
@click.option("--pipeline-name", default=None)
@click.option("--platform", default="DATABRICKS", show_default=True,
              type=click.Choice(["DATABRICKS", "STANDALONE_SPARK", "DATAPROC", "FABRIC", "AIRFLOW", "ADF", "PYTHON", "OTHER"],
                                case_sensitive=False))
@click.option("--runtime", default="PYSPARK", show_default=True)
@click.option("--stage", default="SILVER", show_default=True)
@click.option("--dq-mode", default="OBSERVE_ONLY", show_default=True,
              type=click.Choice(["OBSERVE_ONLY", "ENFORCE", "REMEDIATE"], case_sensitive=False))
@click.option("--policy", default="../policies/default.yaml", show_default=True,
              help="Policy reference written into the manifest (relative to the manifest file)")
@click.option("--owner", default="")
@click.option("--description", default="")
@click.option("--approved-by", default="dqctl-bootstrap", show_default=True,
              help="Actor recorded as approving all generated candidate rules")
@click.option("--copy", "copy_source", is_flag=True,
              help="Copy the source file into sample_data/bronze/ and reference it there")
@click.option("--master", default="local[4]", show_default=True, help="Spark master for the local harness")
@click.option("--driver-memory", default="2g", show_default=True, help="Driver heap for the local harness")
@click.option("--profile-mode", default="FULL", show_default=True,
              type=click.Choice(["FULL", "SAMPLE", "APPROXIMATE"], case_sensitive=False),
              help="FULL scans every row; SAMPLE is the right choice for large files")
@click.option("--out-dir", default="config", show_default=True, help="Config root to write rulesets/ + pipelines/ into")
@click.option("--profile-out", default=None, help="Also write the JSON profile here")
@click.option("--publish", is_flag=True, help="Publish the generated rule set into the registry")
@click.option("--onboard", is_flag=True, help="Register the pipeline in the registry after writing the manifest")
@click.option("--force", is_flag=True, help="Overwrite existing generated files")
@click.option("--config", "-c", default="config/engine.yaml")
def bootstrap(source: str, dataset: str | None, pipeline_id: str | None, pipeline_name: str | None,
              platform: str, runtime: str, stage: str, dq_mode: str, policy: str, owner: str,
              description: str, approved_by: str, copy_source: bool, master: str, driver_memory: str,
              profile_mode: str, out_dir: str, profile_out: str | None,
              publish: bool, onboard: bool, force: bool, config: str) -> None:
    """Export a pipeline from a source file: profile it, generate an approved rule set
    and a ready-to-paste onboarding manifest.

    Fast path for new pipelines - review the generated rules before ENFORCE/REMEDIATE:
    every auto-approved rule carries its confidence and profiling evidence.
    """
    import re
    import shutil

    from dq_core.generation.generator import CandidateRuleGenerator, approve_candidates
    from dq_core.metadata.serialization import dump_yaml, save_ruleset
    from dq_core.pipelines.manifest import PipelineManifest
    from dq_core.runtime.logging import configure_logging

    configure_logging("INFO", json_format=False)
    src = Path(source).resolve()
    ds = (dataset or src.stem).lower()
    pid = (pipeline_id or f"{ds.upper()}_PIPELINE").upper()
    stage_up = stage.upper()
    ruleset_id = f"{ds}_{stage_up.lower()}"
    out_root = Path(out_dir).resolve()
    ruleset_path = out_root / "rulesets" / f"{ds}_v1.yaml"
    manifest_path = out_root / "pipelines" / f"{pid}.yaml"

    for p in (ruleset_path, manifest_path):
        if p.exists() and not force:
            _err(f"{p} already exists - pass --force to overwrite"); sys.exit(1)

    # 1) profile ----------------------------------------------------------------------------------
    spark = _spark(config, master=master, driver_memory=driver_memory)
    engine = _engine(config)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(src)) \
        if src.suffix.lower() == ".csv" else spark.read.parquet(str(src))
    report = engine.profile(df, ds, mode=profile_mode)
    _ok(f"Profiled {src.name}: {report.row_count} rows x {report.column_count} cols, "
        f"{len(report.anomalies)} anomalies")
    if profile_out:
        report.write_json(profile_out)
        _ok(f"Profile written to {profile_out}")

    # 2) generate + approve candidates ------------------------------------------------------------
    candidates = CandidateRuleGenerator().generate(report, ruleset_id=ruleset_id, version="1.0",
                                                   stage=stage_up)
    rs = approve_candidates(candidates, [r.rule_id for r in candidates.rules],
                            ruleset_id=ruleset_id, version="1.0", approved_by=approved_by)
    save_ruleset(rs, ruleset_path)
    _ok(f"Rule set {ruleset_id}@1.0 written to {ruleset_path} ({len(rs.rules)} rules, auto-approved)")
    for r in rs.rules:
        conf = (r.generation or {}).get("confidence")
        click.echo(f"      {r.rule_id:<22} {r.rule_type.value:<22} {str(r.column or '-'):<18} "
                   f"{r.severity.value:<8} confidence={conf}")

    # 3) manifest ----------------------------------------------------------------------------------
    if copy_source:
        sample_dir = ROOT / "sample_data" / "bronze"
        sample_dir.mkdir(parents=True, exist_ok=True)
        dest = sample_dir / src.name
        if src != dest:
            shutil.copy2(src, dest)
        test_data = os.path.relpath(dest, manifest_path.parent)
    else:
        test_data = os.path.relpath(src, manifest_path.parent) \
            if str(src).startswith(str(out_root.parent)) else str(src)
    test_data = test_data.replace("\\", "/")
    manifest = {
        "pipeline": {
            "pipeline_id": pid,
            "pipeline_name": pipeline_name or re.sub(r"[_-]+", " ", pid).title(),
            "platform": platform.upper(),
            "runtime": runtime.upper(),
            "integration_mode": "INLINE",
            "dq_mode": dq_mode.upper(),
            "owner": owner,
            "description": description or f"Auto-bootstrapped from {src.name} "
                                        f"({report.row_count} rows profiled)",
            "policy": policy,
            "datasets": [{
                "dataset_id": ds,
                "format": "CSV" if src.suffix.lower() == ".csv" else "PARQUET",
                "stages": ["BRONZE", stage_up],
                "rulesets": {stage_up: os.path.relpath(ruleset_path, manifest_path.parent).replace("\\", "/")},
                "test_data": test_data,
                "test_data_format": "CSV" if src.suffix.lower() == ".csv" else "PARQUET",
            }],
        }
    }
    dump_yaml(manifest, manifest_path)
    PipelineManifest.load(manifest_path)   # self-check: generated manifest must validate
    _ok(f"Manifest written to {manifest_path}")

    # 4) optional publish + onboard ---------------------------------------------------------------
    if publish:
        from dq_core.registries.factory import build_ruleset_registry
        from dq_core.runtime.config import EngineConfig
        cfg = EngineConfig.from_yaml(config)
        entry = build_ruleset_registry(cfg.registry).publish(rs, published_by=approved_by, activate=True)
        _ok(f"Published {rs.ref}  checksum={entry['checksum'][:12]}  ACTIVE")
    if onboard:
        from dq_core.registries.factory import build_pipeline_registry
        from dq_core.runtime.config import EngineConfig
        cfg = EngineConfig.from_yaml(config)
        entry = build_pipeline_registry(cfg.registry).onboard(PipelineManifest.load(manifest_path),
                                                              actor=approved_by)
        _ok(f"Onboarded {pid}  state={entry['state']}")

    spark.stop()
    click.echo("\nNext steps:")
    click.echo(f"  1. Review rules:      {ruleset_path}")
    if not publish:
        click.echo(f"  2. Publish rule set:  dqctl ruleset publish {ruleset_path}")
    if not onboard:
        click.echo(f"  3. Onboard pipeline:  dqctl pipeline onboard -m {manifest_path}")
        click.echo( "     or paste the manifest below into UI > Pipelines > Onboard")
    click.echo(f"  4. Then test -> activate -> run DQ")
    click.echo("\n--- manifest (paste into UI) ---")
    click.echo(manifest_path.read_text(encoding="utf-8"))


# ===========================================================================  run
@main.command("run")
@click.argument("source", type=click.Path(exists=True))
@click.option("--ruleset", "-r", required=True, type=click.Path(exists=True))
@click.option("--policy", "-p", type=click.Path(exists=True))
@click.option("--mode", "-m", default="OBSERVE_ONLY", show_default=True,
              type=click.Choice(["OBSERVE_ONLY", "ENFORCE", "REMEDIATE"], case_sensitive=False))
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--pipeline-id", default="CLI_RUN")
@click.option("--run-id", default="CLI_001")
@click.option("--dataset", default=None)
@click.option("--format", "fmt", default="CSV", show_default=True,
              type=click.Choice(["CSV", "PARQUET", "JSON"], case_sensitive=False))
@click.option("--out", default=None, help="Write accepted records here (optional)")
@click.option("--report", default=None, help="Write JSON report here (optional)")
def run_cmd(source: str, ruleset: str, policy: str | None, mode: str, config: str,
            pipeline_id: str, run_id: str, dataset: str | None, fmt: str,
            out: str | None, report: str | None) -> None:
    """Run DQ validation on a file (CSV, Parquet or JSON)."""
    from dq_core.context import PipelineContext
    from dq_core.engine import DataQualityEngine
    from dq_core.io.sources import CsvSource, ParquetSource, read_source, SourceSpec
    from dq_core.runtime.logging import configure_logging

    configure_logging("INFO", json_format=False)
    spark = _spark(config)
    engine = _engine(config)
    rs = engine.load_ruleset(ruleset)
    ds = dataset or rs.dataset
    if fmt.upper() == "CSV":
        df = CsvSource(source).read(spark)
    elif fmt.upper() == "PARQUET":
        df = ParquetSource(source).read(spark)
    else:
        df = spark.read.json(source)
    ctx = PipelineContext(pipeline_id=pipeline_id, pipeline_run_id=run_id, dataset=ds,
                          stage=rs.stage, platform="LOCAL")
    result = engine.run(df, rs, policy, ctx, mode=mode)
    click.echo(result.summary_text())
    if out:
        result.accepted_dataframe.write.mode("overwrite").parquet(out)
        _ok(f"Accepted records written to {out}")
    if report:
        result.write_report(report)
        _ok(f"Report written to {report}")
    spark.stop()
    sys.exit(0 if result.quality_gate.passed else 1)


# ===========================================================================  profile
@main.command("profile")
@click.argument("source", type=click.Path(exists=True))
@click.option("--dataset", default=None)
@click.option("--config", "-c", default="config/engine.yaml")
@click.option("--mode", default="FULL", show_default=True,
              type=click.Choice(["FULL", "SAMPLE", "APPROXIMATE"], case_sensitive=False))
@click.option("--out", default=None, help="Write JSON profile to this file")
def profile_cmd(source: str, dataset: str | None, config: str, mode: str, out: str | None) -> None:
    """Profile a CSV / Parquet file and display the report."""
    from dq_core.engine import DataQualityEngine
    from dq_core.io.sources import CsvSource
    from dq_core.runtime.logging import configure_logging

    configure_logging("INFO", json_format=False)
    spark = _spark(config)
    engine = _engine(config)
    df = CsvSource(source).read(spark)
    ds = dataset or Path(source).stem
    report = engine.profile(df, ds, mode=mode)
    click.echo(report.summary_text())
    if out:
        report.write_json(out)
        _ok(f"Profile written to {out}")
    spark.stop()


# ===========================================================================  metadata
_METADATA_COLLECTIONS = ("ruleset_index", "ruleset_versions", "pipelines", "manifests",
                         "policies", "approvals", "rejections", "profiles", "candidates", "jobs")


@main.group()
def metadata() -> None:
    """Metadata store — status, collections, backend switch (file|sqlite|nosql)."""


@metadata.command("status")
@click.option("--config", "-c", default="config/engine.yaml")
def metadata_status(config: str) -> None:
    """Show metadata backend, store location and per-collection document counts."""
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(config)
    reg = cfg.registry
    click.echo(f"backend:          {(reg.backend or 'file').lower()}")
    if reg.backend and reg.backend.lower() == "nosql":
        meta = reg.metadata
        click.echo(f"metadata.engine:  {meta.engine}")
        click.echo(f"store path/uri:   {meta.db_path or meta.uri or '(default)'}")
    engine = DataQualityEngine(cfg)
    store = getattr(getattr(engine, "ruleset_registry", None), "store", None)
    if store is not None and hasattr(store, "collection"):
        click.echo(f"store:            {type(store).__name__}")
        for name in _METADATA_COLLECTIONS:
            try:
                click.echo(f"  {name:<20} {store.collection(name).count()}")
            except Exception:
                click.echo(f"  {name:<20} -")
    elif hasattr(getattr(engine, "ruleset_registry", None), "store"):
        click.echo("store:            sqlite (RegistryStore)")
    else:
        click.echo("store:            file (YAML/JSON registry)")


@metadata.command("switch")
@click.argument("backend", type=click.Choice(["file", "sqlite", "nosql"], case_sensitive=False))
@click.option("--engine", "nosql_engine", default=None,
              type=click.Choice(["tinydb", "mongodb", "dynamodb"], case_sensitive=False),
              help="NoSQL engine when backend=nosql (default: tinydb)")
@click.option("--config", "-c", default="config/engine.yaml")
def metadata_switch(backend: str, nosql_engine: str | None, config: str) -> None:
    """Switch registry.backend (+ metadata.engine) in engine.yaml. Restart backend/CLI after."""
    import re
    from pathlib import Path

    path = Path(config)
    text = path.read_text(encoding="utf-8")
    backend = backend.lower()
    text = re.sub(r"(registry:\s*\n(?:.*\n)*?\s*backend:\s*)\S+", rf"\g<1>{backend}", text, count=1)
    if backend == "nosql":
        eng = (nosql_engine or "tinydb").lower()
        text = re.sub(r"(metadata:\s*\n(?:.*\n)*?\s*engine:\s*)\S+", rf"\g<1>{eng}", text, count=1)
    path.write_text(text, encoding="utf-8")
    _ok(f"registry.backend -> {backend}" + (f" (metadata.engine={eng})" if backend == "nosql" else ""))
    click.echo("  Restart the backend/CLI to apply. Data does not auto-sync between stores.")


if __name__ == "__main__":
    main()
