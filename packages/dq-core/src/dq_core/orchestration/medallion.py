"""Bronze → Silver → Gold medallion pipeline orchestrator.

BRONZE  : preserve raw input, capture metadata, profile, generate candidate rules.
SILVER  : load approved rules, validate, remediate (deterministic + AI), revalidate,
          separate accepted from quarantine.
GOLD    : validate business constraints (referential integrity, aggregates, row counts),
          apply quality gate, write publication-ready data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_core.context import IntegrationMode, PipelineContext, Stage
from dq_core.engine import DataQualityEngine
from dq_core.io.ingestion import write_ingest_manifest
from dq_core.io.sinks import StoragePaths, StorageSink
from dq_core.metadata.models import RuleSet
from dq_core.orchestration.result import DQResult
from dq_core.policies.models import RemediationPolicyConfig
from dq_core.profiling.models import ProfileReport
from dq_core.runtime.compat import get_spark
from dq_core.runtime.logging import get_logger, log_event
from dq_core.runtime.utils import Stopwatch, utc_now_iso

_logger = get_logger("orchestration.medallion")


@dataclass
class MedallionResult:
    pipeline_id: str
    pipeline_run_id: str
    dataset: str
    bronze_rows: int
    bronze_path: str
    silver_result: DQResult
    gold_result: DQResult | None
    profile: ProfileReport | None
    phase_durations_ms: dict[str, float] = field(default_factory=dict)
    written_paths: dict[str, str] = field(default_factory=dict)
    started_at: str = ""
    finished_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_run_id": self.pipeline_run_id,
            "dataset": self.dataset,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "bronze_rows": self.bronze_rows,
            "bronze_path": self.bronze_path,
            "phase_durations_ms": self.phase_durations_ms,
            "written_paths": self.written_paths,
            "silver": self.silver_result.to_dict(),
            "gold": self.gold_result.to_dict() if self.gold_result else None,
            "profile": {"row_count": self.profile.row_count,
                        "primary_key_candidates": self.profile.primary_key_candidates,
                        "anomaly_count": len(self.profile.anomalies)} if self.profile else None,
        }

    def summary_text(self) -> str:
        lines = [
            f"=== Medallion Run  pipeline={self.pipeline_id}  dataset={self.dataset} ===",
            f"  BRONZE : {self.bronze_rows} rows  path={self.bronze_path}",
            "  SILVER : " + self.silver_result.summary_text().replace("\n", "\n           "),
        ]
        if self.gold_result:
            lines.append("  GOLD   : " + self.gold_result.summary_text().replace("\n", "\n           "))
        lines.append(f"  Phases : {self.phase_durations_ms}")
        return "\n".join(lines)


class MedallionRunner:
    """
    Thin wrapper around :class:`~dq_core.engine.DataQualityEngine` that drives the Bronze
    → Silver → Gold flow with configurable paths, modes and rule sets.
    """

    def __init__(self, engine: DataQualityEngine, paths: StoragePaths, sink: StorageSink | None = None) -> None:
        self.engine = engine
        self.paths = paths
        self.sink = sink or StorageSink(fmt=engine.config.outputs.format or "parquet",
                                        mode=engine.config.outputs.write_mode or "overwrite")

    def run(self, raw_df: DataFrame, dataset: str, *, pipeline_id: str, pipeline_run_id: str,
            silver_ruleset: RuleSet | str | Path, policy: RemediationPolicyConfig | str | Path | None = None,
            silver_mode: IntegrationMode | str = IntegrationMode.REMEDIATE,
            gold_ruleset: RuleSet | str | Path | None = None,
            gold_reference_data: Mapping[str, DataFrame] | None = None,
            source_path: str | Path | None = None,
            profile: bool = True, write_bronze: bool = True, write_silver: bool = True,
            write_gold: bool = True, write_quarantine: bool = True, write_reports: bool = True,
            platform: str = "LOCAL") -> MedallionResult:
        started = utc_now_iso()
        phases: dict[str, float] = {}
        written: dict[str, str] = {}
        spark = get_spark(raw_df)

        # --- BRONZE ---------------------------------------------------------------
        with Stopwatch() as sw:
            bronze_path = self.paths.bronze(dataset)
            bronze_rows = raw_df.persist(StorageLevel.MEMORY_AND_DISK).count()
            if write_bronze:
                self.sink.write(raw_df, bronze_path)
                written["bronze"] = bronze_path
                receipt = write_ingest_manifest(dataset, source_path or bronze_path, bronze_path, raw_df)
                written["bronze_ingest_manifest"] = receipt.manifest_path
        phases["bronze_write"] = sw.elapsed_ms
        log_event(_logger, "bronze_complete", dataset=dataset, rows=bronze_rows, path=bronze_path)

        # --- SILVER ---------------------------------------------------------------
        with Stopwatch() as sw:
            silver_ctx = PipelineContext(pipeline_id=pipeline_id, pipeline_run_id=pipeline_run_id,
                                         dataset=dataset, stage=Stage.SILVER, platform=platform)
            silver_rs = self.engine.load_ruleset(silver_ruleset)
            silver_pol = self.engine.load_policy(policy)
            profile_report = self.engine.profile(raw_df, dataset) if profile else None
        phases["silver_setup"] = sw.elapsed_ms

        with Stopwatch() as sw:
            silver_result = self.engine.run(raw_df, silver_rs, silver_pol, silver_ctx, mode=silver_mode)
        phases["silver_dq"] = sw.elapsed_ms
        if write_silver:
            with Stopwatch() as sw:
                p = self.paths.silver(dataset)
                self.sink.write(silver_result.accepted_dataframe, p)
                written["silver_accepted"] = p
                if write_quarantine:
                    qp = self.paths.quarantine("silver", dataset)
                    self.sink.write(silver_result.quarantine_dataframe, qp)
                    written["silver_quarantine"] = qp
                if silver_result.remediation_results.removed_duplicates is not None:
                    rdp = self.paths.quarantine("silver", f"{dataset}_removed_duplicates")
                    self.sink.write(silver_result.remediation_results.removed_duplicates, rdp)
                    written["silver_removed_duplicates"] = rdp
                if silver_result.remediation_results.dataframe is not None:
                    alp = self.paths.audit("silver", f"{dataset}_remediation_log")
                    self.sink.write(silver_result.remediation_results.dataframe, alp)
                    written["silver_audit_log"] = alp
            phases["silver_write"] = sw.elapsed_ms
        log_event(_logger, "silver_complete", dataset=dataset, accepted=silver_result.execution_metrics.accepted_records,
                  quarantined=silver_result.execution_metrics.quarantined_records)

        # --- GOLD -----------------------------------------------------------------
        gold_result: DQResult | None = None
        if gold_ruleset is not None:
            with Stopwatch() as sw:
                gold_ctx = PipelineContext(pipeline_id=pipeline_id, pipeline_run_id=pipeline_run_id,
                                           dataset=dataset, stage=Stage.GOLD, platform=platform)
                gold_rs = self.engine.load_ruleset(gold_ruleset)
                gold_pol = self.engine.load_policy(policy)
                ref = dict(gold_reference_data or {})
                gold_result = self.engine.run(silver_result.accepted_dataframe, gold_rs, gold_pol, gold_ctx,
                                              mode=IntegrationMode.ENFORCE, reference_data=ref)
            phases["gold_dq"] = sw.elapsed_ms
            if write_gold:
                with Stopwatch() as sw:
                    gp = self.paths.gold(dataset)
                    self.sink.write(gold_result.accepted_dataframe, gp)
                    written["gold_accepted"] = gp
                    if write_quarantine:
                        gqp = self.paths.quarantine("gold", dataset)
                        self.sink.write(gold_result.quarantine_dataframe, gqp)
                        written["gold_quarantine"] = gqp
                phases["gold_write"] = sw.elapsed_ms
            log_event(_logger, "gold_complete", dataset=dataset,
                      accepted=gold_result.execution_metrics.accepted_records,
                      quarantined=gold_result.execution_metrics.quarantined_records,
                      gate_passed=gold_result.quality_gate.passed)

        # --- REPORTS --------------------------------------------------------------
        if write_reports and self.engine.config.outputs.reports_path:
            rp = Path(self.engine.config.outputs.reports_path)
            rp.mkdir(parents=True, exist_ok=True)
            sr = rp / f"{dataset}_silver_{silver_result.run_id}.json"
            sr.write_text(silver_result.to_json(), encoding="utf-8")
            written["silver_report"] = str(sr)
            if gold_result:
                gr = rp / f"{dataset}_gold_{gold_result.run_id}.json"
                gr.write_text(gold_result.to_json(), encoding="utf-8")
                written["gold_report"] = str(gr)
            if profile_report:
                pp = rp / f"{dataset}_profile_{silver_result.run_id}.json"
                pp.write_text(profile_report.to_json(), encoding="utf-8")
                written["profile_report"] = str(pp)

        raw_df.unpersist()
        return MedallionResult(pipeline_id=pipeline_id, pipeline_run_id=pipeline_run_id, dataset=dataset,
                               bronze_rows=bronze_rows, bronze_path=bronze_path, silver_result=silver_result,
                               gold_result=gold_result, profile=profile_report, phase_durations_ms=phases,
                               written_paths=written, started_at=started, finished_at=utc_now_iso())
