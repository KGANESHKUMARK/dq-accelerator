"""Deterministic remediation: TRIM, UPPERCASE, LOWERCASE, approved lookup mapping, configured date
standardisation, explicitly approved null replacement and configured duplicate removal.

Corrections are only applied to records that violate the rule owning the remediation policy,
every change is captured in ``_dq_fixes`` (original and new value), and every corrected record is
revalidated by the runner afterwards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dq_core.metadata.models import ALL_COLUMNS, DeterministicAction, RemediationStep, RuleDefinition, RuleSet, RuleType
from dq_core.policies.models import RemediationPolicyConfig
from dq_core.runtime.compat import col, safe_to_timestamp
from dq_core.runtime.errors import RemediationError
from dq_core.runtime.schema import user_columns
from dq_core.validation import columns as C

TEXT_ACTIONS = {DeterministicAction.TRIM, DeterministicAction.UPPERCASE, DeterministicAction.LOWERCASE,
                DeterministicAction.LOOKUP_MAPPING, DeterministicAction.DATE_STANDARDIZATION,
                DeterministicAction.NULL_REPLACEMENT}


@dataclass
class DeterministicOutcome:
    dataframe: DataFrame                 # remediated records (survivors)
    removed: DataFrame | None            # records removed by DEDUPLICATE (with audit entries)
    blocked: list[dict[str, Any]] = field(default_factory=list)
    applied_rules: list[str] = field(default_factory=list)


def _violates(rule_id: str) -> Column:
    return F.exists(F.col(C.VIOLATIONS), lambda v: v["rule_id"] == F.lit(rule_id))


def apply_step(expr: Column, step: RemediationStep, lookups: Mapping[str, Mapping[str, Any]]) -> Column:
    p = step.params
    if step.action == DeterministicAction.TRIM:
        return F.trim(expr)
    if step.action == DeterministicAction.UPPERCASE:
        return F.upper(expr)
    if step.action == DeterministicAction.LOWERCASE:
        return F.lower(expr)
    if step.action == DeterministicAction.LOOKUP_MAPPING:
        mapping = lookups[p["lookup"]]["mapping"]
        result: Column | None = None
        for source, target in mapping.items():
            result = (F.when(expr == F.lit(source), F.lit(target)) if result is None
                      else result.when(expr == F.lit(source), F.lit(target)))
        return result.otherwise(expr) if result is not None else expr
    if step.action == DeterministicAction.DATE_STANDARDIZATION:
        output = str(p["output_format"])
        candidates = [F.date_format(safe_to_timestamp(expr, str(fmt)), output) for fmt in p["input_formats"]]
        return F.coalesce(*candidates, expr)
    if step.action == DeterministicAction.NULL_REPLACEMENT:
        return F.coalesce(F.when(F.trim(expr) != F.lit(""), expr), F.lit(str(p["value"])))
    raise RemediationError(f"Unsupported deterministic action {step.action}")  # pragma: no cover


def _fix_struct(rule: RuleDefinition, column: str, action: str, before: Column, after: Column, reason: str) -> Column:
    return F.struct(
        F.lit(rule.rule_id).alias("rule_id"), F.lit(column).alias("column"), F.lit("DETERMINISTIC").alias("source"),
        F.lit(action).alias("action"), before.cast("string").alias("original_value"),
        after.cast("string").alias("new_value"), F.lit(None).cast("double").alias("confidence"),
        F.lit("dq-core").alias("provider"), F.lit(None).cast("string").alias("model_version"),
        F.lit("APPLIED").alias("decision"), F.lit(reason).alias("reason"), F.lit(False).alias("simulated"))


class DeterministicRemediator:
    def __init__(self, policy: RemediationPolicyConfig) -> None:
        self.policy = policy

    def apply(self, df: DataFrame, ruleset: RuleSet) -> DeterministicOutcome:
        df = C.ensure_remediation_columns(df)
        dtypes = dict(df.dtypes)
        available = user_columns(df.columns)
        outcome = DeterministicOutcome(dataframe=df, removed=None)
        # Compose every correction into ONE projection (rules on the same column chain in rule order).
        current: dict[str, Column] = {}
        fix_entries: list[Column] = []
        for rule in ruleset.executable_rules():
            steps = [s for s in rule.remediation.deterministic if s.action in TEXT_ACTIONS]
            if not steps or not rule.column:
                continue
            column = rule.column
            if column not in dtypes:
                continue
            not_permitted = [s.action.value for s in steps if not self.policy.deterministic_action_permitted(column, s.action)]
            if not_permitted:
                outcome.blocked.append({"rule_id": rule.rule_id, "column": column, "actions": not_permitted,
                                        "reason": f"not permitted on sensitive column ({self.policy.sensitive_category(column)}); "
                                                  "human approval required"})
                continue
            if dtypes[column] != "string":
                outcome.blocked.append({"rule_id": rule.rule_id, "column": column, "actions": [s.action.value for s in steps],
                                        "reason": f"text remediation requires a string column (found {dtypes[column]})"})
                continue
            before = current.get(column, col(column))
            failing = _violates(rule.rule_id)
            new_value = before
            for step in steps:
                new_value = apply_step(new_value, step, ruleset.lookups)
            changed = failing & ~new_value.eqNullSafe(before)
            action = ">".join(s.describe() for s in steps)
            fix_entries.append(F.when(changed, _fix_struct(rule, column, action, before, new_value,
                                                           f"{rule.rule_id} deterministic remediation")))
            current[column] = F.when(failing, new_value).otherwise(before)
            outcome.applied_rules.append(rule.rule_id)
        if current:
            new_fixes = F.concat(F.col(C.FIXES), F.filter(F.array(*fix_entries), lambda f: f.isNotNull()).cast(C.FIXES_TYPE))
            df = df.select(*[current[c].alias(c) if c in current else col(c) for c in df.columns if c != C.FIXES],
                           new_fixes.alias(C.FIXES))

        removed_frames: list[DataFrame] = []
        for rule in ruleset.executable_rules():
            dedupe = [s for s in rule.remediation.deterministic if s.action == DeterministicAction.DEDUPLICATE]
            if rule.rule_type != RuleType.DUPLICATE or not dedupe:
                continue
            df, removed = self._deduplicate(df, rule, dedupe[0], available)
            removed_frames.append(removed)
            outcome.applied_rules.append(rule.rule_id)
        outcome.dataframe = df
        if removed_frames:
            removed_all = removed_frames[0]
            for frame in removed_frames[1:]:
                removed_all = removed_all.unionByName(frame)
            outcome.removed = removed_all
        return outcome

    @staticmethod
    def _deduplicate(df: DataFrame, rule: RuleDefinition, step: RemediationStep,
                     available: list[str]) -> tuple[DataFrame, DataFrame]:
        keys = rule.key_columns(available)
        survivor = step.params.get("survivor") or {}
        order: list[Column] = []
        for spec in survivor.get("order_by") or []:
            spec = {"column": spec} if isinstance(spec, str) else spec
            c = col(spec["column"])
            descending = str(spec.get("direction", "ASC")).upper() == "DESC"
            order.append(c.desc_nulls_last() if descending else c.asc_nulls_last())
        order.append(F.col(C.RECORD_ID).asc())  # deterministic tie-breaker
        in_scope = _violates(rule.rule_id)
        window = Window.partitionBy(*[col(k) for k in keys]).orderBy(*order)
        full = window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ranked = (df.withColumn("_dq_dup_rank", F.when(in_scope, F.row_number().over(window)))
                    .withColumn("_dq_dup_survivor", F.first(F.col(C.RECORD_ID)).over(full)))
        strategy = survivor.get("strategy") or f"order_by {survivor.get('order_by')}"
        survivors = ranked.where(F.col("_dq_dup_rank").isNull() | (F.col("_dq_dup_rank") == 1))
        removed = ranked.where(F.col("_dq_dup_rank") > 1)
        key_label = ALL_COLUMNS if rule.config.get("keys") == ALL_COLUMNS else ",".join(keys)
        fix = F.struct(
            F.lit(rule.rule_id).alias("rule_id"), F.lit(key_label).alias("column"), F.lit("DETERMINISTIC").alias("source"),
            F.lit("DEDUPLICATE").alias("action"), F.col(C.RECORD_ID).alias("original_value"),
            F.col("_dq_dup_survivor").alias("new_value"), F.lit(None).cast("double").alias("confidence"),
            F.lit("dq-core").alias("provider"), F.lit(None).cast("string").alias("model_version"),
            F.lit("REMOVED_DUPLICATE").alias("decision"),
            F.concat(F.lit(f"removed duplicate (keys={key_label}, survivor policy={strategy}); survivor="),
                     F.col("_dq_dup_survivor")).alias("reason"),
            F.lit(False).alias("simulated"))
        removed = removed.withColumn(C.FIXES, F.concat(F.col(C.FIXES), F.array(fix)))
        return (survivors.drop("_dq_dup_rank", "_dq_dup_survivor"), removed.drop("_dq_dup_rank", "_dq_dup_survivor"))
