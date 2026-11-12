"""Translate rule metadata into native PySpark Column expressions (no eval, no generated code).

Every builder returns a boolean *failure* expression that is never NULL. Values used in
expressions come from validated configuration and are passed as literals / allow-listed types.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dq_core.metadata.models import ConditionOperator, RuleCondition, RuleDefinition, RuleType
from dq_core.runtime.compat import col, safe_cast, safe_to_date, safe_to_timestamp
from dq_core.runtime.errors import ValidationExecutionError
from dq_core.runtime.schema import validate_cast_type


@dataclass
class RowCheck:
    rule: RuleDefinition
    flag: str
    failure: Column
    value: Column
    message: str


def ri_hit_column(index: int) -> str:
    return f"_dq_ri_hit_{index}"


def condition_expr(condition: RuleCondition) -> Column:
    c = col(condition.column)
    values = [str(v) for v in condition.values]
    op = condition.operator
    if op == ConditionOperator.IS_NULL:
        return c.isNull()
    if op == ConditionOperator.IS_NOT_NULL:
        return c.isNotNull()
    s = c.cast("string")
    if op == ConditionOperator.EQ:
        return s == F.lit(values[0])
    if op == ConditionOperator.NE:
        return s.isNull() | (s != F.lit(values[0]))
    if op == ConditionOperator.IN:
        return s.isin(values)
    if op == ConditionOperator.NOT_IN:
        return s.isNull() | ~s.isin(values)
    raise ValidationExecutionError(f"Unsupported condition operator {op}")  # pragma: no cover


def _formats(value: str | Sequence[str]) -> list[str]:
    return [value] if isinstance(value, str) else [str(v) for v in value]


def default_message(rule: RuleDefinition) -> str:
    cfg, c = rule.config, rule.column
    t = rule.rule_type
    if cfg.get("message"):
        return str(cfg["message"])
    if t == RuleType.NOT_NULL:
        return f"{c} is null or blank"
    if t == RuleType.UNIQUE:
        return f"{c} value is not unique"
    if t == RuleType.DUPLICATE:
        keys = cfg.get("keys")
        return f"duplicate record for keys {keys}"
    if t == RuleType.REGEX:
        return f"{c} does not match pattern {cfg.get('pattern')}"
    if t == RuleType.RANGE:
        return f"{c} outside range [{cfg.get('min', '-inf')}, {cfg.get('max', '+inf')}]"
    if t == RuleType.ALLOWED_VALUES:
        return f"{c} is not an allowed value"
    if t == RuleType.DATA_TYPE:
        return f"{c} cannot be converted to {cfg.get('data_type')}"
    if t == RuleType.DATE_FORMAT:
        return f"{c} is not a valid date in format {cfg.get('format')}"
    if t == RuleType.STRING_LENGTH:
        return f"length of {c} outside [{cfg.get('min', 0)}, {cfg.get('max', 'inf')}]"
    if t == RuleType.REFERENTIAL_INTEGRITY:
        return f"{c} not found in {cfg.get('reference_dataset')}.{cfg.get('reference_column')}"
    if t == RuleType.NULL_RATE:
        return f"null rate on {c} exceeds {cfg.get('max_null_rate', cfg.get('max_null_percent'))}"
    if t == RuleType.DISTINCT_RATIO:
        return f"distinct ratio on {c} below {cfg.get('min_distinct_ratio', cfg.get('min_distinct_percent'))}"
    if t == RuleType.FRESHNESS:
        return f"{c} data is older than max_age {cfg.get('max_age', cfg.get('max_age_seconds'))}"
    if t == RuleType.COLUMN_AGGREGATE:
        return f"{cfg.get('aggregate', 'SUM')}({c}) outside [{cfg.get('min', '-inf')}, {cfg.get('max', '+inf')}]"
    return rule.description or rule.name


def build_row_check(rule: RuleDefinition, index: int, available: Sequence[str],
                    dtypes: Mapping[str, str]) -> RowCheck:
    cfg, c, t = rule.config, rule.column, rule.rule_type
    flag = f"_dq_flag_{index}"
    value: Column = col(c).cast("string") if c else F.lit(None).cast("string")

    if t == RuleType.NOT_NULL:
        failure = col(c).isNull()
        if cfg.get("treat_blank_as_null", True) and dtypes.get(c, "string") == "string":
            failure = failure | (F.trim(col(c)) == F.lit(""))
    elif t == RuleType.UNIQUE:
        window = Window.partitionBy(col(c))
        failure = col(c).isNotNull() & (F.count(F.lit(1)).over(window) > F.lit(1))
    elif t == RuleType.DUPLICATE:
        keys = rule.key_columns(available)
        window = Window.partitionBy(*[col(k) for k in keys])
        failure = F.count(F.lit(1)).over(window) > F.lit(1)
        value = F.concat_ws("|", *[F.coalesce(col(k).cast("string"), F.lit("<null>")) for k in keys]) if len(keys) <= 5 \
            else F.lit(f"{len(keys)} key columns")
    elif t == RuleType.REGEX:
        failure = col(c).isNotNull() & ~col(c).cast("string").rlike(str(cfg["pattern"]))
    elif t == RuleType.RANGE:
        data_type = str(cfg.get("data_type", "double")).lower()
        if data_type == "date":
            fmt = str(cfg.get("format", "yyyy-MM-dd"))
            typed = safe_to_date(col(c).cast("string"), fmt)
            lo = F.to_date(F.lit(str(cfg["min"]))) if cfg.get("min") is not None else None
            hi = F.to_date(F.lit(str(cfg["max"]))) if cfg.get("max") is not None else None
        else:
            type_sql = validate_cast_type(data_type)
            typed = safe_cast(c, type_sql)
            lo = F.lit(cfg["min"]).cast(type_sql) if cfg.get("min") is not None else None
            hi = F.lit(cfg["max"]).cast(type_sql) if cfg.get("max") is not None else None
        inclusive = bool(cfg.get("inclusive", True))
        out_of_range = F.lit(False)
        if lo is not None:
            out_of_range = out_of_range | ((typed < lo) if inclusive else (typed <= lo))
        if hi is not None:
            out_of_range = out_of_range | ((typed > hi) if inclusive else (typed >= hi))
        failure = typed.isNotNull() & out_of_range
    elif t == RuleType.ALLOWED_VALUES:
        values = [str(v) for v in cfg["values"]]
        expr = col(c).cast("string")
        if not cfg.get("case_sensitive", True):
            expr, values = F.upper(expr), [v.upper() for v in values]
        failure = expr.isNotNull() & ~expr.isin(values)
    elif t == RuleType.DATA_TYPE:
        as_text = col(c).cast("string")
        failure = col(c).isNotNull() & (F.trim(as_text) != F.lit("")) & safe_cast(c, str(cfg["data_type"])).isNull()
    elif t == RuleType.DATE_FORMAT:
        as_text = col(c).cast("string")
        parsed = F.coalesce(*[safe_to_timestamp(as_text, fmt) for fmt in _formats(cfg["format"])])
        failure = col(c).isNotNull() & parsed.isNull()
    elif t == RuleType.STRING_LENGTH:
        length = F.length(col(c).cast("string"))
        bad = F.lit(False)
        if cfg.get("min") is not None:
            bad = bad | (length < F.lit(int(cfg["min"])))
        if cfg.get("max") is not None:
            bad = bad | (length > F.lit(int(cfg["max"])))
        failure = col(c).isNotNull() & bad
    elif t == RuleType.REFERENTIAL_INTEGRITY:
        failure = col(c).isNotNull() & F.col(ri_hit_column(index)).isNull()
    else:  # pragma: no cover - guarded by RuleDefinition.is_row_level
        raise ValidationExecutionError(f"{t.value} is not a row-level rule")

    if rule.condition is not None:
        failure = condition_expr(rule.condition) & failure
    return RowCheck(rule=rule, flag=flag, failure=F.coalesce(failure, F.lit(False)), value=value,
                    message=default_message(rule))
