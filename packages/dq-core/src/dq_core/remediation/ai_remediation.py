"""AI-assisted remediation step.

1. collect the DISTINCT failing (rule, column, value) combinations for AI-enabled rules, capped by
   ``policy.ai.max_requests_per_run`` (bounded driver collection, one batched provider call);
2. ask the provider for proposals; 3. let the confidence policy decide per proposal;
4. join decisions back to every affected record (Spark broadcast join) and apply only
   AUTO_APPLY / APPROVED_BY_HUMAN corrections. The runner revalidates all records afterwards.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_core.ai.base import AIRemediationProvider, ProposalRequest
from dq_core.metadata.models import RuleDefinition, RuleSet, RuleType
from dq_core.policies.confidence import ConfidencePolicyEngine, Decision
from dq_core.policies.models import RemediationPolicyConfig
from dq_core.runtime.compat import col, first_or_null, get_spark
from dq_core.runtime.logging import get_logger, log_event
from dq_core.validation import columns as C
from dq_core.validation.expressions import default_message

_logger = get_logger("remediation.ai")
_APPLY = [Decision.AUTO_APPLY.value, Decision.APPROVED_BY_HUMAN.value]


@dataclass
class AIOutcome:
    dataframe: DataFrame
    decisions: list[dict[str, Any]] = field(default_factory=list)
    requests_sent: int = 0
    provider_calls: int = 0
    truncated: bool = False
    blocked: list[dict[str, Any]] = field(default_factory=list)


def _proposal_for(column: str) -> Any:
    return lambda p: (p["column"] == F.lit(column)) & p["decision"].isin(*_APPLY)


def _request_context(rule: RuleDefinition) -> dict[str, Any]:
    context: dict[str, Any] = {"rule_type": rule.rule_type.value, "rule_description": rule.description}
    if rule.rule_type == RuleType.ALLOWED_VALUES:
        context["allowed_values"] = list(rule.config.get("values", []))
    if rule.rule_type == RuleType.REGEX:
        context["pattern"] = rule.config.get("pattern")
    if rule.rule_type in (RuleType.DATA_TYPE, RuleType.RANGE):
        context["data_type"] = rule.config.get("data_type")
    return context


class AIRemediator:
    def __init__(self, provider: AIRemediationProvider, policy_engine: ConfidencePolicyEngine,
                 policy: RemediationPolicyConfig) -> None:
        self.provider = provider
        self.policy_engine = policy_engine
        self.policy = policy

    def apply(self, df: DataFrame, ruleset: RuleSet, run_id: str) -> AIOutcome:
        df = C.ensure_remediation_columns(df)
        eligible = {r.rule_id: r for r in ruleset.executable_rules()
                    if r.remediation.ai_assisted.enabled and r.is_row_level and r.column}
        if not eligible or not self.policy.ai.enabled:
            return AIOutcome(dataframe=df)
        order = {rid: i for i, rid in enumerate(eligible)}
        exploded = (df.select(C.RECORD_ID, C.RECORD_KEY, F.explode(C.VIOLATIONS).alias("v"))
                    .where(F.col("v.rule_id").isin(list(eligible))))
        grouped = (exploded.groupBy(F.col("v.rule_id").alias("rule_id"), F.col("v.value").alias("value"))
                   .agg(F.min(C.RECORD_ID).alias("record_id"), F.min(C.RECORD_KEY).alias("record_key"),
                        F.count(F.lit(1)).alias("occurrences"))
                   .orderBy("rule_id", "value"))
        limit = int(self.policy.ai.max_requests_per_run)
        rows = grouped.limit(limit + 1).collect()  # bounded: distinct failing values only
        truncated = len(rows) > limit
        rows = rows[:limit]

        requests: list[ProposalRequest] = []
        blocked: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            rule = eligible[row["rule_id"]]
            if (self.provider.is_external and self.policy.is_sensitive(rule.column)
                    and not self.policy.ai.allow_sensitive_values_to_external_providers):
                blocked.append({"rule_id": rule.rule_id, "column": rule.column, "value": row["value"],
                                "decision": Decision.BLOCKED_SENSITIVE_DATA_EGRESS.value})
                continue
            requests.append(ProposalRequest(
                request_id=f"{run_id}-AI-{i + 1:04d}", record_id=row["record_id"], record_key=row["record_key"],
                rule_id=rule.rule_id, rule_type=rule.rule_type.value, column=rule.column,
                original_value=row["value"], failure_reason=default_message(rule), dataset=ruleset.dataset,
                occurrences=int(row["occurrences"]), context=_request_context(rule)))
        proposals = self.provider.propose(requests) if requests else []

        decisions: list[dict[str, Any]] = []
        for request, proposal in zip(requests, proposals):
            rule = eligible[request.rule_id]
            verdict = self.policy_engine.evaluate(rule=rule, column=request.column, original_value=request.original_value,
                                                  proposed_value=proposal.proposed_value, confidence=proposal.confidence,
                                                  dataset=ruleset.dataset)
            decisions.append({
                "rule_order": order[request.rule_id], "request_id": request.request_id, "record_id": request.record_id,
                "record_key": request.record_key, "rule_id": request.rule_id, "column": request.column,
                "original_value": request.original_value, "proposed_value": proposal.proposed_value,
                "confidence": float(proposal.confidence), "decision": verdict.decision.value,
                "decision_reason": verdict.reason, "sensitive_category": verdict.sensitive_category,
                "reason": proposal.reason, "evidence": json.dumps(proposal.evidence, sort_keys=True),
                "provider": proposal.provider, "model_version": proposal.model_version,
                "simulated": bool(proposal.simulated), "occurrences": request.occurrences,
                "threshold": str(self.policy_engine.threshold)})
        log_event(_logger, "ai_proposals_evaluated", dataset=ruleset.dataset, requests=len(requests),
                  provider=self.provider.name, truncated=truncated,
                  auto_applied=sum(d["decision"] in _APPLY for d in decisions))
        if not decisions:
            return AIOutcome(dataframe=df, requests_sent=len(requests), truncated=truncated, blocked=blocked,
                             provider_calls=1 if requests else 0)

        spark = get_spark(df)
        fields = [f.name for f in C.PROPOSAL_TYPE.fields]
        decision_df = spark.createDataFrame([tuple(d[f] for f in fields) for d in decisions], C.PROPOSAL_TYPE)
        matched = exploded.join(F.broadcast(decision_df),
                                (F.col("v.rule_id") == decision_df["rule_id"]) & F.col("v.value").eqNullSafe(decision_df["original_value"]),
                                "inner")
        per_record = matched.groupBy(C.RECORD_ID).agg(
            F.array_sort(F.collect_list(F.struct(*[decision_df[f].alias(f) for f in fields]))).alias("_dq_new_props"))
        df = (df.join(per_record, C.RECORD_ID, "left")
                .withColumn(C.AI_PROPOSALS, F.concat(F.col(C.AI_PROPOSALS),
                                                     F.coalesce(F.col("_dq_new_props"), C.empty_array(C.PROPOSALS_TYPE))))
                .drop("_dq_new_props"))

        updates: dict[str, Column] = {}
        fix_entries: list[Column] = []
        for column in sorted({d["column"] for d in decisions if d["decision"] in _APPLY}):
            chosen = first_or_null(F.filter(F.col(C.AI_PROPOSALS), _proposal_for(column)))
            applies = chosen.isNotNull()
            fix_entries.append(F.when(applies, F.struct(
                chosen["rule_id"].alias("rule_id"), F.lit(column).alias("column"),
                F.when(chosen["decision"] == F.lit(Decision.APPROVED_BY_HUMAN.value), F.lit("HUMAN_APPROVED_AI"))
                 .otherwise(F.lit("AI")).alias("source"),
                F.lit("AI_CORRECTION").alias("action"), col(column).cast("string").alias("original_value"),
                chosen["proposed_value"].alias("new_value"), chosen["confidence"].alias("confidence"),
                chosen["provider"].alias("provider"), chosen["model_version"].alias("model_version"),
                chosen["decision"].alias("decision"), chosen["reason"].alias("reason"),
                chosen["simulated"].alias("simulated"))))
            updates[column] = F.when(applies, chosen["proposed_value"]).otherwise(col(column))
        if updates:  # one projection for all AI corrections
            new_fixes = F.concat(F.col(C.FIXES), F.filter(F.array(*fix_entries), lambda f: f.isNotNull()).cast(C.FIXES_TYPE))
            df = df.select(*[updates[c].alias(c) if c in updates else col(c) for c in df.columns if c != C.FIXES],
                           new_fixes.alias(C.FIXES))
        return AIOutcome(dataframe=df, decisions=decisions, requests_sent=len(requests),
                         provider_calls=1 if requests else 0, truncated=truncated, blocked=blocked)
