"""Mode-aware DQ execution: OBSERVE_ONLY, ENFORCE and REMEDIATE.

REMEDIATE flow::

    validate -> split (untouched | candidates) -> deterministic fixes (+ dedupe)
             -> intermediate validation -> AI proposals -> confidence policy -> apply eligible
             -> FINAL revalidation of (untouched + remediated) with the same rule set version
             -> accepted (no ERROR violations) | quarantine (+ status) | removed duplicates
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_core.ai.base import AIRemediationProvider
from dq_core.context import ExecutionContext, IntegrationMode, PipelineContext
from dq_core.metadata.models import RuleSet
from dq_core.orchestration.gate import QualityGateResult, evaluate_quality_gate
from dq_core.orchestration.result import DQResult, RemediationResults
from dq_core.policies.approvals import ApprovalStore
from dq_core.policies.confidence import ConfidencePolicyEngine, Decision
from dq_core.policies.models import RemediationPolicyConfig
from dq_core.profiling.profiler import DataProfiler
from dq_core.quarantine.builder import build_quarantine, empty_quarantine_like
from dq_core.remediation.ai_remediation import AIRemediator
from dq_core.remediation.deterministic import DeterministicRemediator
from dq_core.reporting.metrics import ExecutionMetrics
from dq_core.revalidation.revalidator import Revalidator
from dq_core.runtime.compat import col, get_spark
from dq_core.runtime.logging import get_logger, log_event
from dq_core.runtime.schema import user_columns
from dq_core.runtime.utils import Stopwatch, new_run_id, utc_now_iso
from dq_core.validation import columns as C
from dq_core.validation.engine import ValidationEngine
from dq_core.version import __version__

_logger = get_logger("orchestration")
_APPLIED = (Decision.AUTO_APPLY.value, Decision.APPROVED_BY_HUMAN.value)


def _project_accepted(df: DataFrame, columns: list[str], include_dq: bool) -> DataFrame:
    extra = [C.RECORD_ID, C.STATUS, C.FIXES] if include_dq else []
    return df.select(*[col(c) for c in columns], *[F.col(c) for c in extra if c in df.columns])


def _remediation_log(final: DataFrame, removed: DataFrame | None, run_id: str, dataset: str) -> DataFrame:
    def base(frame: DataFrame, outcome: Any) -> list[Any]:
        return [F.lit(run_id).alias("dq_run_id"), F.lit(dataset).alias("dataset_id"),
                F.col(C.RECORD_ID).alias("record_ref"), F.col(C.RECORD_KEY).alias("record_key"), outcome.alias("record_outcome")]

    outcome = F.when(F.col(C.ERROR_COUNT) == 0, F.lit("ACCEPTED")).otherwise(F.lit("QUARANTINED"))
    fix_cols = ["rule_id", "column", "source", "action", "original_value", "new_value", "confidence", "decision",
                "reason", "provider", "model_version", "simulated"]
    fixes = (final.select(*base(final, outcome), F.explode(C.FIXES).alias("f"))
             .select("dq_run_id", "dataset_id", "record_ref", "record_key", "record_outcome",
                     *[F.col(f"f.{c}").alias(c) for c in fix_cols]))
    proposals = (final.select(*base(final, outcome), F.explode(C.AI_PROPOSALS).alias("p"))
                 .where(~F.col("p.decision").isin(*_APPLIED))
                 .select("dq_run_id", "dataset_id", "record_ref", "record_key", "record_outcome",
                         F.col("p.rule_id").alias("rule_id"), F.col("p.column").alias("column"), F.lit("AI").alias("source"),
                         F.lit("AI_PROPOSAL_NOT_APPLIED").alias("action"), F.col("p.original_value").alias("original_value"),
                         F.col("p.proposed_value").alias("new_value"), F.col("p.confidence").alias("confidence"),
                         F.col("p.decision").alias("decision"),
                         F.concat_ws(" | ", F.col("p.decision_reason"), F.col("p.reason")).alias("reason"),
                         F.col("p.provider").alias("provider"), F.col("p.model_version").alias("model_version"),
                         F.col("p.simulated").alias("simulated")))
    log = fixes.unionByName(proposals)
    if removed is not None:
        removed_log = (removed.select(*base(removed, F.lit("REMOVED_DUPLICATE")), F.explode(C.FIXES).alias("f"))
                       .select("dq_run_id", "dataset_id", "record_ref", "record_key", "record_outcome",
                               *[F.col(f"f.{c}").alias(c) for c in fix_cols]))
        log = log.unionByName(removed_log)
    return log


class DQRunner:
    def __init__(self, validation: ValidationEngine, profiler: DataProfiler, provider: AIRemediationProvider,
                 include_dq_columns: bool = False) -> None:
        self.validation = validation
        self.profiler = profiler
        self.provider = provider
        self.include_dq_columns = include_dq_columns

    def run(self, df: DataFrame, ruleset: RuleSet, policy: RemediationPolicyConfig, context: PipelineContext,
            mode: IntegrationMode, *, reference_data: Mapping[str, DataFrame] | None = None, profile: bool = False,
            ruleset_published: bool = False, runtime: dict[str, Any] | None = None) -> DQResult:
        run_id = new_run_id()
        started = utc_now_iso()
        total_sw = Stopwatch().__enter__()
        phases: dict[str, float] = {}
        exec_ctx = ExecutionContext(
            run_id=run_id, pipeline=context, mode=mode, started_at=started, ruleset_id=ruleset.ruleset_id,
            ruleset_version=ruleset.version, ruleset_checksum=ruleset.checksum(), policy_id=policy.policy_id,
            policy_version=policy.version, confidence_threshold=float(policy.confidence_threshold),
            engine_version=__version__, runtime=runtime or {})
        log_event(_logger, "dq_run_started", run_id=run_id, pipeline_id=context.pipeline_id, dataset=context.dataset,
                  stage=context.stage.value, mode=mode.value, ruleset=ruleset.ref)
        input_columns = user_columns(df.columns)
        profile_report = None
        if profile:
            with Stopwatch() as sw:
                profile_report = self.profiler.profile(df, context.dataset)
            phases["profiling"] = sw.elapsed_ms

        with Stopwatch() as sw:
            initial = self.validation.validate(df, ruleset, reference_data=reference_data)
        phases["validation"] = sw.elapsed_ms
        persisted: list[Any] = [initial]
        report = initial.report
        remediation = RemediationResults()
        final_report = None
        removed_count = 0
        fix_stats = {"corrected_passed": 0, "corrected_failed": 0, "deterministic_fixes": 0, "ai_fixes": 0}

        if mode == IntegrationMode.OBSERVE_ONLY:
            with Stopwatch() as sw:
                accepted = df  # the pipeline output is never modified in OBSERVE_ONLY
                quarantine = build_quarantine(initial.failed(), exec_ctx, mode)
                accepted_count, quarantined_count = report.total_records, report.failed_records
            phases["quarantine"] = sw.elapsed_ms
        elif mode == IntegrationMode.ENFORCE:
            with Stopwatch() as sw:
                accepted = _project_accepted(initial.passed(), input_columns, self.include_dq_columns)
                quarantine = build_quarantine(initial.failed(), exec_ctx, mode)
                accepted_count, quarantined_count = report.passed_records, report.failed_records
            phases["quarantine"] = sw.elapsed_ms
        else:
            with Stopwatch() as sw:
                remediable = [r.rule_id for r in ruleset.executable_rules() if r.remediation.is_remediable]
                needs = (F.col(C.ERROR_COUNT) > 0) | F.exists(F.col(C.VIOLATIONS), lambda v: v["rule_id"].isin(remediable))
                untouched = initial.annotated.where(~needs)
                candidates = initial.annotated.where(needs)
                deterministic = DeterministicRemediator(policy).apply(candidates, ruleset)
                intermediate = self.validation.validate(deterministic.dataframe, ruleset, reference_data=reference_data)
                persisted.append(intermediate)
                policy_engine = ConfidencePolicyEngine(policy, ApprovalStore.load(policy.approvals_path))
                ai = AIRemediator(self.provider, policy_engine, policy).apply(intermediate.annotated, ruleset, run_id)
            phases["remediation"] = sw.elapsed_ms
            with Stopwatch() as sw:
                final_input = (C.ensure_remediation_columns(C.strip_annotations(untouched))
                               .unionByName(C.ensure_remediation_columns(C.strip_annotations(ai.dataframe))))
                reval = Revalidator(self.validation).revalidate(final_input, ruleset, reference_data)
                final = reval.validation
                fix_stats = reval.stats
            phases["revalidation"] = sw.elapsed_ms
            persisted.append(final)
            with Stopwatch() as sw:
                final_report = final.report
                accepted = _project_accepted(final.passed(), input_columns, self.include_dq_columns)
                quarantine = build_quarantine(final.failed(), exec_ctx, mode)
                accepted_count, quarantined_count = final_report.passed_records, final_report.failed_records
                removed_count = deterministic.removed.count() if deterministic.removed is not None else 0
                decisions = Counter(d["decision"] for d in ai.decisions)
                remediation = RemediationResults(
                    summary={
                        "deterministic_rules_applied": sorted(set(deterministic.applied_rules)),
                        "deterministic_corrections": fix_stats["deterministic_fixes"],
                        "ai_provider": self.provider.describe(),
                        "ai_requests_sent": ai.requests_sent,
                        "ai_provider_calls": ai.provider_calls,
                        "ai_requests_truncated": ai.truncated,
                        "ai_decisions_by_type": dict(decisions),
                        "ai_corrections_applied": fix_stats["ai_fixes"],
                        "confidence_threshold": str(policy.confidence_threshold),
                        "duplicates_removed": removed_count,
                        "records_remediated_and_accepted": fix_stats["corrected_passed"],
                        "records_remediated_but_quarantined": fix_stats["corrected_failed"],
                    },
                    ai_decisions=ai.decisions, blocked_by_policy=deterministic.blocked + ai.blocked,
                    dataframe=_remediation_log(final.annotated, deterministic.removed, run_id, context.dataset),
                    removed_duplicates=deterministic.removed)
            phases["quarantine"] = sw.elapsed_ms

        breakdown = {r["remediation_status"]: int(r["count"])
                     for r in quarantine.groupBy("remediation_status").count().collect()} if quarantined_count else {}
        gate_report = final_report or report
        gate = evaluate_quality_gate(policy.quality_gate, gate_report, input_records=report.total_records,
                                     accepted_records=accepted_count, quarantined_records=quarantined_count)
        total_sw.__exit__(None, None, None)
        results = gate_report.results
        metrics = ExecutionMetrics(
            dq_run_id=run_id, pipeline_id=context.pipeline_id, pipeline_run_id=context.pipeline_run_id,
            platform=context.platform, dataset=context.dataset, stage=context.stage.value, integration_mode=mode.value,
            status="COMPLETED", ruleset_id=ruleset.ruleset_id, ruleset_version=ruleset.version,
            ruleset_checksum=exec_ctx.ruleset_checksum, ruleset_published=ruleset_published,
            policy_id=policy.policy_id, policy_version=policy.version,
            confidence_threshold=str(policy.confidence_threshold), engine_version=__version__,
            spark_version=get_spark(df).version, started_at=started, finished_at=utc_now_iso(),
            duration_ms=total_sw.elapsed_ms, input_records=report.total_records, accepted_records=accepted_count,
            quarantined_records=quarantined_count, removed_duplicates=removed_count,
            records_with_warnings=gate_report.warning_records,
            remediated_records_accepted=fix_stats["corrected_passed"],
            remediated_records_quarantined=fix_stats["corrected_failed"],
            deterministic_corrections=fix_stats["deterministic_fixes"], ai_corrections_applied=fix_stats["ai_fixes"],
            ai_proposals_evaluated=len(remediation.ai_decisions),
            rules_evaluated=sum(r.status != "NOT_EVALUATED" for r in results),
            rules_failed=sum(r.status == "FAILED" for r in results),
            rules_not_evaluated=sum(r.status == "NOT_EVALUATED" for r in results),
            rules_skipped_not_approved=len(report.skipped_rules),
            acceptance_rate=round(accepted_count / report.total_records, 4) if report.total_records else 0.0,
            quarantine_rate=round(quarantined_count / report.total_records, 4) if report.total_records else 0.0,
            quality_gate_passed=gate.passed, quarantine_status_breakdown=breakdown, phase_durations_ms=phases)
        log_event(_logger, "dq_run_completed", run_id=run_id, dataset=context.dataset, mode=mode.value,
                  input=metrics.input_records, accepted=accepted_count, quarantined=quarantined_count,
                  gate_passed=gate.passed, duration_ms=metrics.duration_ms)
        return DQResult(run_id=run_id, mode=mode, execution_context=exec_ctx, accepted_dataframe=accepted,
                        quarantine_dataframe=quarantine, validation_results=report, remediation_results=remediation,
                        execution_metrics=metrics, quality_gate=gate, final_validation_results=final_report,
                        profile=profile_report, _persisted=persisted)

    def skipped(self, df: DataFrame, ruleset: RuleSet, policy: RemediationPolicyConfig, context: PipelineContext,
                mode: IntegrationMode, reason: str, state: str) -> DQResult:
        """Pass-through result for PAUSED / OFFBOARDED pipelines: data is returned untouched."""
        run_id = new_run_id("DQSKIP")
        now = utc_now_iso()
        exec_ctx = ExecutionContext(run_id=run_id, pipeline=context, mode=mode, started_at=now,
                                    ruleset_id=ruleset.ruleset_id, ruleset_version=ruleset.version,
                                    ruleset_checksum=ruleset.checksum(), policy_id=policy.policy_id,
                                    policy_version=policy.version,
                                    confidence_threshold=float(policy.confidence_threshold), engine_version=__version__)
        metrics = ExecutionMetrics(
            dq_run_id=run_id, pipeline_id=context.pipeline_id, pipeline_run_id=context.pipeline_run_id,
            platform=context.platform, dataset=context.dataset, stage=context.stage.value, integration_mode=mode.value,
            status=f"SKIPPED_PIPELINE_{state}", ruleset_id=ruleset.ruleset_id, ruleset_version=ruleset.version,
            ruleset_checksum=exec_ctx.ruleset_checksum, ruleset_published=False, policy_id=policy.policy_id,
            policy_version=policy.version, confidence_threshold=str(policy.confidence_threshold),
            engine_version=__version__, spark_version=get_spark(df).version, started_at=now, finished_at=now,
            duration_ms=0.0)
        log_event(_logger, "dq_run_skipped", pipeline_id=context.pipeline_id, state=state, reason=reason)
        return DQResult(run_id=run_id, mode=mode, execution_context=exec_ctx, accepted_dataframe=df,
                        quarantine_dataframe=empty_quarantine_like(df), validation_results=None,
                        remediation_results=RemediationResults(), execution_metrics=metrics,
                        quality_gate=QualityGateResult(passed=True, checks=[{"check": "skipped", "passed": True,
                                                                             "actual": state, "threshold": None}]),
                        skipped=True, skip_reason=reason)
