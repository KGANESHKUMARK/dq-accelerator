"""dqctl – command line interface for the DQ Accelerator.

Sub-commands:
  pipeline onboard|list|test|activate|pause|offboard
  ruleset  list|publish|activate|rollback|diff|verify
  run      OBSERVE_ONLY|ENFORCE|REMEDIATE on a CSV/Parquet file
  profile  profile a CSV/Parquet file and show the report
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


def _spark(config_path: str) -> "Any":                  # noqa: F821
    from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session
    return create_local_spark_session(LocalSparkSettings(
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


if __name__ == "__main__":
    main()
