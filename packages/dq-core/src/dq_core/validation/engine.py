"""Metadata-driven validation engine (native PySpark, single pass for row-level rules)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_core.metadata.models import RuleDefinition, RuleSet, RuleType
from dq_core.runtime.compat import col, compact, safe_cast
from dq_core.runtime.config import ValidationSettings
from dq_core.runtime.logging import get_logger, log_event
from dq_core.runtime.schema import compare_schema, missing_columns, schema_to_list, user_columns
from dq_core.runtime.utils import Stopwatch, utc_now_iso
from dq_core.validation import columns as C
from dq_core.validation.expressions import RowCheck, build_row_check, default_message, ri_hit_column
from dq_core.validation.models import RuleResult, ValidationReport, pct

_logger = get_logger("validation")


@dataclass
class ValidationOutcome:
    annotated: DataFrame
    report: ValidationReport
    persisted: list[DataFrame] = field(default_factory=list)

    def passed(self) -> DataFrame:
        return self.annotated.where(F.col(C.ERROR_COUNT) == 0)

    def failed(self) -> DataFrame:
        return self.annotated.where(F.col(C.ERROR_COUNT) > 0)

    def unpersist(self) -> None:
        for df in self.persisted:
            try:
                df.unpersist()
            except Exception:  # pragma: no cover - e.g. Spark Connect without caching support
                pass


def _severity_is(severity: str) -> Any:
    # Single-argument lambda: PySpark passes (element, index) to two-argument lambdas.
    return lambda v: v["severity"] == F.lit(severity)


def _try_persist(df: DataFrame) -> tuple[DataFrame, bool]:
    try:
        return df.persist(StorageLevel.MEMORY_AND_DISK), True
    except Exception:  # Databricks serverless / some Spark Connect setups do not support persist
        return df, False


class ValidationEngine:
    def __init__(self, settings: ValidationSettings | None = None) -> None:
        self.settings = settings or ValidationSettings()

    # ------------------------------------------------------------------ public API
    def validate(self, df: DataFrame, ruleset: RuleSet, *, reference_data: Mapping[str, DataFrame] | None = None,
                 rules: Sequence[RuleDefinition] | None = None, cache: bool | None = None) -> ValidationOutcome:
        started = utc_now_iso()
        total_sw = Stopwatch().__enter__()
        reference_data = dict(reference_data or {})
        cache = self.settings.cache_intermediate if cache is None else cache
        selected = list(rules) if rules is not None else ruleset.executable_rules()
        skipped = ruleset.skipped_rules() if rules is None else []

        base = C.add_record_columns(C.strip_annotations(df), ruleset.record_key)
        available = user_columns(base.columns)
        dtypes = dict(base.dtypes)

        evaluable: list[RuleDefinition] = []
        not_evaluated: list[tuple[RuleDefinition, str]] = []
        for rule in selected:
            reason = self._preflight(rule, available, reference_data)
            (not_evaluated.append((rule, reason)) if reason else evaluable.append(rule))

        row_rules = [r for r in evaluable if r.is_row_level]
        dataset_rules = [r for r in evaluable if not r.is_row_level]

        # Cross-dataset (referential integrity) lookups: left join against distinct reference keys.
        # join_strategy in rule config controls scale behaviour:
        #   "auto"      (default) - no hint; Spark/AQE chooses (broadcasts when the
        #               deduplicated key side is under the broadcast threshold).
        #   "broadcast" - force broadcast of the distinct reference keys; best when
        #               the dimension table is small but the fact side is huge.
        #   "merge"     - force sort-merge join; right choice when the reference
        #               side is itself too large to broadcast.
        for index, rule in enumerate(row_rules):
            if rule.rule_type == RuleType.REFERENTIAL_INTEGRITY:
                ref = reference_data[rule.config["reference_dataset"]]
                key_col = f"_dq_ri_key_{index}"
                keys = (ref.select(col(rule.config["reference_column"]).cast("string").alias(key_col))
                        .where(F.col(key_col).isNotNull()).distinct()
                        .withColumn(ri_hit_column(index), F.lit(True)))
                strategy = str(rule.config.get("join_strategy", "auto")).lower()
                if strategy == "broadcast":
                    keys = F.broadcast(keys)
                elif strategy == "merge":
                    keys = keys.hint("merge")
                base = base.join(keys, col(rule.column).cast("string") == F.col(key_col), "left").drop(key_col)

        checks: list[RowCheck] = [build_row_check(rule, i, available, dtypes) for i, rule in enumerate(row_rules)]
        flagged = base.select("*", *[chk.failure.alias(chk.flag) for chk in checks])
        if checks:
            structs = [F.when(F.col(chk.flag), F.struct(
                F.lit(chk.rule.rule_id).alias("rule_id"), F.lit(chk.rule.name).alias("rule_name"),
                F.lit(chk.rule.rule_type.value).alias("rule_type"),
                F.lit(chk.rule.column or ",".join(chk.rule.key_columns(available)[:5])).alias("column"),
                F.lit(chk.rule.severity.value).alias("severity"), F.lit(chk.message).alias("message"),
                chk.value.alias("value"))) for chk in checks]
            violations = compact(F.array(*structs)).cast(C.VIOLATIONS_TYPE)
        else:
            violations = C.empty_array(C.VIOLATIONS_TYPE)
        annotated = flagged.select("*", violations.alias(C.VIOLATIONS))
        errors, warnings, infos = (F.size(F.filter(F.col(C.VIOLATIONS), _severity_is(s))) for s in ("ERROR", "WARNING", "INFO"))
        annotated = annotated.select(
            "*", errors.alias(C.ERROR_COUNT), warnings.alias(C.WARNING_COUNT), infos.alias(C.INFO_COUNT),
            F.when(errors > 0, F.lit("FAILED")).when(warnings > 0, F.lit("PASSED_WITH_WARNINGS"))
             .otherwise(F.lit("PASSED")).alias(C.STATUS))
        persisted: list[DataFrame] = []
        if cache:
            annotated, ok = _try_persist(annotated)
            if ok:
                persisted.append(annotated)

        # Single aggregation pass for all row-level rules.
        with Stopwatch() as batch_sw:
            aggs = [F.count(F.lit(1)).alias("__total"),
                    F.sum((F.col(C.ERROR_COUNT) > 0).cast("int")).alias("__failed"),
                    F.sum(((F.col(C.ERROR_COUNT) == 0) & (F.col(C.WARNING_COUNT) > 0)).cast("int")).alias("__warn")]
            for i, chk in enumerate(checks):
                aggs.append(F.sum(F.col(chk.flag).cast("int")).alias(f"__f{i}"))
                if self.settings.collect_failure_examples:
                    aggs.append(F.min(F.when(F.col(chk.flag), chk.value)).alias(f"__min{i}"))
                    aggs.append(F.max(F.when(F.col(chk.flag), chk.value)).alias(f"__max{i}"))
            stats = annotated.agg(*aggs).collect()[0].asDict()
        total = int(stats["__total"] or 0)

        row_results: list[RuleResult] = []
        for i, chk in enumerate(checks):
            failed = int(stats.get(f"__f{i}") or 0)
            examples = [] if not self.settings.collect_failure_examples else sorted(
                {str(v) for v in (stats.get(f"__min{i}"), stats.get(f"__max{i}")) if v is not None})
            row_results.append(self._result(ruleset, chk.rule, total, failed, [chk.message] if failed else [],
                                            examples, batch_sw.elapsed_ms, batch=True))
        dataset_results: list[RuleResult] = []
        for rule in dataset_rules:
            with Stopwatch() as sw:
                passed, reasons, details = self._evaluate_dataset_rule(rule, base, total, reference_data)
            failed = 0 if passed else total
            dataset_results.append(self._result(ruleset, rule, total, failed, reasons, [], sw.elapsed_ms,
                                                batch=False, details=details, dataset_level=True))
        for rule, reason in not_evaluated:
            result = self._result(ruleset, rule, total, 0, [reason], [], 0.0, batch=False,
                                  dataset_level=not rule.is_row_level)
            result.status, result.passed_records, result.pass_percentage = "NOT_EVALUATED", 0, 0.0
            (row_results if rule.is_row_level else dataset_results).append(result)

        order = {r.rule_id: i for i, r in enumerate(ruleset.rules)}
        row_results.sort(key=lambda r: order.get(r.rule_id, 0))
        dataset_results.sort(key=lambda r: order.get(r.rule_id, 0))
        drop_cols = [chk.flag for chk in checks] + [c for c in annotated.columns if c.startswith("_dq_ri_hit_")]
        total_sw.__exit__(None, None, None)
        report = ValidationReport(
            dataset=ruleset.dataset, ruleset_id=ruleset.ruleset_id, ruleset_version=ruleset.version,
            ruleset_checksum=ruleset.checksum(), total_records=total,
            passed_records=total - int(stats["__failed"] or 0), failed_records=int(stats["__failed"] or 0),
            warning_records=int(stats["__warn"] or 0), row_level_results=row_results,
            dataset_level_results=dataset_results, skipped_rules=skipped, duration_ms=total_sw.elapsed_ms,
            started_at=started)
        log_event(_logger, "validation_completed", dataset=ruleset.dataset, ruleset=ruleset.ref, total=total,
                  failed=report.failed_records, rules=len(selected), duration_ms=report.duration_ms)
        return ValidationOutcome(annotated=annotated.drop(*drop_cols), report=report, persisted=persisted)

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _preflight(rule: RuleDefinition, available: Sequence[str], reference_data: Mapping[str, DataFrame]) -> str | None:
        needed = rule.referenced_columns(available) if rule.rule_type != RuleType.SCHEMA_VALIDATION else []
        missing = missing_columns(needed, available)
        if missing:
            return f"missing column(s): {', '.join(missing)}"
        if rule.rule_type in (RuleType.REFERENTIAL_INTEGRITY, RuleType.RECONCILIATION):
            ref_name = rule.config["reference_dataset"]
            if ref_name not in reference_data:
                return f"reference dataset '{ref_name}' not supplied (pass reference_data={{'{ref_name}': df}})"
            if rule.config["reference_column"] not in reference_data[ref_name].columns:
                return f"reference column '{rule.config['reference_column']}' missing in '{ref_name}'"
        return None

    @staticmethod
    def _result(ruleset: RuleSet, rule: RuleDefinition, total: int, failed: int, reasons: list[str],
                examples: list[str], duration_ms: float, *, batch: bool, details: dict[str, Any] | None = None,
                dataset_level: bool = False) -> RuleResult:
        passed = total - failed
        return RuleResult(
            rule_id=rule.rule_id, rule_name=rule.name, rule_type=rule.rule_type.value, scope=rule.scope.value,
            level="DATASET" if dataset_level else "ROW", severity=rule.severity.value, dataset=rule.dataset,
            column=rule.column, ruleset_id=ruleset.ruleset_id, ruleset_version=ruleset.version,
            rule_version=rule.version, status="FAILED" if (failed or (dataset_level and reasons)) else "PASSED",
            total_records=total, passed_records=passed, failed_records=failed,
            pass_percentage=pct(passed, total) if total else (0.0 if failed else 100.0),
            failure_percentage=pct(failed, total), failure_reasons=reasons, examples=examples,
            duration_ms=duration_ms, evaluated_in_batch=batch, details=details or {})

    @staticmethod
    def _evaluate_dataset_rule(rule: RuleDefinition, df: DataFrame, total: int,
                               reference_data: Mapping[str, DataFrame]) -> tuple[bool, list[str], dict[str, Any]]:
        cfg = rule.config
        if rule.rule_type == RuleType.SCHEMA_VALIDATION:
            comparison = compare_schema(cfg["expected_columns"], schema_to_list(df.schema),
                                        allow_extra_columns=bool(cfg.get("allow_extra_columns", False)),
                                        check_types=bool(cfg.get("check_types", True)))
            return comparison.passed, comparison.reasons(), comparison.to_dict()
        if rule.rule_type == RuleType.ROW_COUNT:
            lo, hi = cfg.get("min"), cfg.get("max")
            ok = (lo is None or total >= int(lo)) and (hi is None or total <= int(hi))
            reason = [] if ok else [f"row count {total} outside [{lo if lo is not None else 0}, {hi if hi is not None else 'inf'}]"]
            return ok, reason, {"row_count": total, "min": lo, "max": hi}
        if rule.rule_type == RuleType.RECONCILIATION:
            aggregate = str(cfg.get("aggregate", "SUM")).upper()
            ref = reference_data[cfg["reference_dataset"]]

            def measure(frame: DataFrame, column: str) -> Decimal:
                if aggregate == "COUNT":
                    return Decimal(frame.count())
                value = frame.agg(F.sum(safe_cast(column, "decimal(38,6)"))).collect()[0][0]
                return Decimal(str(value or 0))

            actual, expected = measure(df, rule.column), measure(ref, cfg["reference_column"])
            tolerance = Decimal(str(cfg.get("tolerance", 0)))
            ok = abs(actual - expected) <= tolerance
            details = {"aggregate": aggregate, "value": float(actual), "reference_value": float(expected),
                       "difference": float(actual - expected), "tolerance": float(tolerance)}
            reason = [] if ok else [f"{aggregate}({rule.column})={actual} differs from {cfg['reference_dataset']}."
                                    f"{cfg['reference_column']}={expected} beyond tolerance {tolerance}"]
            return ok, reason, details
        return True, [], {}  # pragma: no cover


__all__ = ["ValidationEngine", "ValidationOutcome", "default_message"]
