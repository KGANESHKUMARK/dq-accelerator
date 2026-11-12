"""DataQualityEngine - the public facade of dq-core.

The engine works on *existing* Spark DataFrames: it never creates or stops a SparkSession and
always uses ``dataframe.sparkSession``. The same API is used by Databricks notebooks/jobs,
standalone Spark applications and any other PySpark-compatible runtime.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from pyspark.sql import DataFrame

from dq_core.ai import create_provider
from dq_core.ai.base import AIRemediationProvider
from dq_core.context import IntegrationMode, PipelineContext, Stage
from dq_core.generation.generator import CandidateRuleGenerator
from dq_core.metadata.models import RuleSet
from dq_core.metadata.serialization import load_ruleset
from dq_core.orchestration.result import DQResult
from dq_core.orchestration.runner import DQRunner
from dq_core.pipelines.registry import PASS_THROUGH_STATES, PipelineRegistry
from dq_core.policies.models import RemediationPolicyConfig
from dq_core.profiling.models import ProfileReport
from dq_core.profiling.profiler import DataProfiler
from dq_core.rules.registry import RuleSetRegistry
from dq_core.runtime.compat import get_spark
from dq_core.runtime.config import EngineConfig
from dq_core.runtime.errors import ConfigurationError
from dq_core.runtime.inspection import inspect_runtime
from dq_core.runtime.logging import get_logger, log_event
from dq_core.validation.engine import ValidationEngine, ValidationOutcome

_logger = get_logger("engine")


class DataQualityEngine:
    def __init__(self, config: EngineConfig | None = None, *, ai_provider: AIRemediationProvider | None = None) -> None:
        self.config = config or EngineConfig()
        self.profiler = DataProfiler(self.config.profiling)
        self.validator = ValidationEngine(self.config.validation)
        self.generator = CandidateRuleGenerator(max_allowed_values=self.config.profiling.max_allowed_values)
        self.ai_provider = ai_provider or create_provider(self.config.ai.provider,
                                                          mock_responses_path=self.config.ai.mock_responses_path,
                                                          similarity_fallback=self.config.ai.similarity_fallback)
        reg = self.config.registry
        from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
        self.ruleset_registry = build_ruleset_registry(reg)
        self.pipeline_registry = build_pipeline_registry(reg)

    @classmethod
    def from_config(cls, path: str | Path, **kwargs: Any) -> "DataQualityEngine":
        return cls(EngineConfig.from_yaml(path), **kwargs)

    # ------------------------------------------------------------------ metadata loading
    def load_ruleset(self, ruleset: RuleSet | str | Path) -> RuleSet:
        """Accept a RuleSet, a YAML/JSON file path, or a registry reference '<ruleset_id>@<version|active>'."""
        if isinstance(ruleset, RuleSet):
            return ruleset
        text = str(ruleset)
        if "@" in text and not Path(text).exists():
            if self.ruleset_registry is None:
                raise ConfigurationError("Registry reference used but no registry.rulesets_path is configured")
            ruleset_id, version = text.split("@", 1)
            return self.ruleset_registry.get(ruleset_id, None if version == "active" else version)
        return load_ruleset(text)

    def load_policy(self, policy: RemediationPolicyConfig | str | Path | None) -> RemediationPolicyConfig:
        if policy is None:
            return RemediationPolicyConfig()
        if isinstance(policy, RemediationPolicyConfig):
            return policy
        # Store-first: NoSQL `policies` collection canonical, YAML fallback + write-through
        try:
            from dq_core.metadata import resolver as _res
            from dq_core.metadata.serialization import load_document
            store = _res.store_from_engine(self)
            path = Path(policy)
            if store is not None:
                key = path.stem
                raw = _res.policy_from_store(store, key)
                if raw is not None:
                    return RemediationPolicyConfig.from_dict(raw, source_path=str(path))
                if path.exists():
                    doc = load_document(path)
                    _res.save_policy(store, key, doc)
                    return RemediationPolicyConfig.from_dict(doc, source_path=str(path.resolve()))
        except Exception:
            pass
        return RemediationPolicyConfig.load(policy)

    # ------------------------------------------------------------------ profiling & generation
    def profile(self, dataframe: DataFrame, dataset: str, *, mode: str | None = None,
                sample_fraction: float | None = None) -> ProfileReport:
        return self.profiler.profile(dataframe, dataset, mode=mode, sample_fraction=sample_fraction)

    def generate_candidate_rules(self, profile: ProfileReport, *, related_profiles: Mapping[str, ProfileReport] | None = None,
                                 stage: Stage | str = Stage.SILVER, version: str = "0.1") -> RuleSet:
        return self.generator.generate(profile, related_profiles=related_profiles, stage=stage, version=version)

    def validate(self, dataframe: DataFrame, ruleset: RuleSet | str | Path, *,
                 reference_data: Mapping[str, DataFrame] | None = None) -> ValidationOutcome:
        return self.validator.validate(dataframe, self.load_ruleset(ruleset), reference_data=reference_data)

    # ------------------------------------------------------------------ main entry point
    def run(self, dataframe: DataFrame, ruleset: RuleSet | str | Path,
            policy: RemediationPolicyConfig | str | Path | None = None, context: PipelineContext | None = None, *,
            mode: IntegrationMode | str | None = None, reference_data: Mapping[str, DataFrame] | None = None,
            profile: bool = False, confidence_threshold: float | str | None = None,
            on_phase: Callable[[str], None] | None = None) -> DQResult:
        """Run profiling (optional), validation and - depending on the mode - enforcement or remediation."""
        rules = self.load_ruleset(ruleset)
        policy_cfg = self.load_policy(policy)
        if confidence_threshold is not None:
            policy_cfg = policy_cfg.with_threshold(confidence_threshold)
        context = context or PipelineContext(pipeline_id="ADHOC", pipeline_run_id="ADHOC", dataset=rules.dataset,
                                             stage=rules.stage)
        resolved_mode = IntegrationMode.parse(mode or context.integration_mode or policy_cfg.default_integration_mode
                                              or self.config.default_mode)
        if context.ruleset_version and context.ruleset_version != rules.version:
            raise ConfigurationError(f"Context expects rule set version {context.ruleset_version} but "
                                     f"{rules.ref} was supplied")
        published = False
        if self.ruleset_registry is not None and self.config.registry.enforce_published_immutability:
            published = self.ruleset_registry.verify(rules)  # raises if a published version was modified
        if (resolved_mode != IntegrationMode.OBSERVE_ONLY and self.config.registry.require_published_for_enforcement
                and self.ruleset_registry is not None and not published):
            raise ConfigurationError(f"{rules.ref} is not published. Publish the reviewed rule set "
                                     f"(dqctl ruleset publish) before running in {resolved_mode.value} mode.")
        store = getattr(self.ruleset_registry, "store", None)
        runner = DQRunner(self.validator, self.profiler, self.ai_provider, self.config.include_dq_columns_in_accepted,
                          alerts=self.config.alerts, metadata_store=store,
                          reports_path=self.config.outputs.reports_path)
        if self.pipeline_registry is not None and self.config.registry.enforce_pipeline_lifecycle:
            state = self.pipeline_registry.state(context.pipeline_id)
            if state in PASS_THROUGH_STATES:
                return runner.skipped(dataframe, rules, policy_cfg, context, resolved_mode,
                                      f"pipeline {context.pipeline_id} is {state.value}; data passed through unchanged",
                                      state.value)
            if state is None:
                log_event(_logger, "pipeline_not_registered", pipeline_id=context.pipeline_id,
                          note="running without lifecycle governance")
        runtime = inspect_runtime(get_spark(dataframe)).to_dict()
        result = runner.run(dataframe, rules, policy_cfg, context, resolved_mode, reference_data=reference_data,
                            profile=profile, ruleset_published=published, runtime=runtime, on_phase=on_phase)
        if self.pipeline_registry is not None and self.pipeline_registry.exists(context.pipeline_id):
            m = result.execution_metrics
            self.pipeline_registry.record_execution(context.pipeline_id, {
                "dq_run_id": m.dq_run_id, "pipeline_run_id": m.pipeline_run_id, "dataset": m.dataset, "stage": m.stage,
                "mode": m.integration_mode, "ruleset": f"{m.ruleset_id}@{m.ruleset_version}",
                "input": m.input_records, "accepted": m.accepted_records, "quarantined": m.quarantined_records,
                "gate_passed": m.quality_gate_passed, "finished_at": m.finished_at})
        return result
