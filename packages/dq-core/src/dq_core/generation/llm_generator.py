"""LLM-assisted rule generation from business descriptions.

Two paths, same output contract (CANDIDATE RuleDefinitions):

* **LLM path** — sends dataset schema + business description to the configured
  LLM (LiteLLM → Databricks Model Serving when ``DATABRICKS_HOST``/
  ``DATABRICKS_TOKEN``/``DQX_MODEL_ENDPOINT`` are set) and parses a JSON rule
  list back into validated RuleDefinitions.
* **Heuristic fallback** — zero-dependency keyword mapper ("email" → REGEX,
  "positive amount" → RANGE min 0, "unique id" → UNIQUE…). Used when no LLM is
  configured or the call fails — keeps generation working everywhere.

Every generated rule carries ``generation.source`` = ``llm_description`` or
``heuristic_description`` + confidence + evidence, status CANDIDATE — never
executable until steward review, identical to profiler-generated candidates.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Iterable, Mapping, Sequence

from dq_core.metadata.models import (RuleDefinition, RuleScope, RuleStatus,
                                     RuleType, Severity)
from dq_core.runtime.utils import utc_now_iso

_RULE_TYPE_ALIASES = {
    "not_null": RuleType.NOT_NULL, "notnull": RuleType.NOT_NULL, "required": RuleType.NOT_NULL,
    "unique": RuleType.UNIQUE, "regex": RuleType.REGEX, "pattern": RuleType.REGEX,
    "range": RuleType.RANGE, "allowed_values": RuleType.ALLOWED_VALUES,
    "allowedvalues": RuleType.ALLOWED_VALUES, "enum": RuleType.ALLOWED_VALUES,
    "data_type": RuleType.DATA_TYPE, "datatype": RuleType.DATA_TYPE,
    "date_format": RuleType.DATE_FORMAT, "dateformat": RuleType.DATE_FORMAT,
    "string_length": RuleType.STRING_LENGTH, "strlen": RuleType.STRING_LENGTH,
    "duplicate": RuleType.DUPLICATE, "row_count": RuleType.ROW_COUNT,
    "null_rate": RuleType.NULL_RATE, "distinct_ratio": RuleType.DISTINCT_RATIO,
    "freshness": RuleType.FRESHNESS, "column_aggregate": RuleType.COLUMN_AGGREGATE,
}

_PROMPT_TEMPLATE = """You are a data-quality rule designer. Given a dataset schema and a
business description, output a JSON array of validation rules. Each rule:
{{"name": str, "column": str|null, "rule_type": str, "severity": "ERROR"|"WARNING",
  "config": {{...}}, "description": str}}.
Allowed rule_types: NOT_NULL, UNIQUE, DUPLICATE, REGEX, RANGE, ALLOWED_VALUES,
DATA_TYPE, DATE_FORMAT, STRING_LENGTH, ROW_COUNT, NULL_RATE, DISTINCT_RATIO,
FRESHNESS, COLUMN_AGGREGATE.
RULES: only use columns that exist in the schema. RANGE config: {{"min":n,"max":n}}.
ALLOWED_VALUES config: {{"values":[...]}}. REGEX config: {{"pattern":"..."}}.
NULL_RATE config: {{"max_null_rate":0.05}}. FRESHNESS config: {{"max_age":"30 days","column":"ts_col"}}.
Output ONLY the JSON array, no prose.

SCHEMA: {schema}
DESCRIPTION: {description}
DOMAIN HINT: {domain}"""


class LLMRuleGenerator:
    """Generates CANDIDATE rules from a business description + dataset schema."""

    def __init__(self, *, model: str | None = None, timeout: int = 60) -> None:
        self.model = model or os.environ.get("DQX_MODEL_ENDPOINT") or "databricks-meta-llama-3-3-70b-instruct"
        self.timeout = timeout

    # ------------------------------------------------------------------ public
    def generate(self, schema: Mapping[str, str], description: str, *,
                 dataset: str, domain: str = "", ruleset_id: str | None = None,
                 use_llm: bool = True) -> list[RuleDefinition]:
        """schema: {column: spark_type}. Returns CANDIDATE rules."""
        raw_rules: list[dict[str, Any]] = []
        source = "heuristic_description"
        if use_llm and self._llm_available():
            try:
                raw_rules = self._llm_rules(schema, description, domain)
                source = "llm_description"
            except Exception:
                raw_rules = []
        if not raw_rules:
            raw_rules = self._heuristic_rules(schema, description)
        return [self._to_rule(r, dataset, ruleset_id or f"{dataset}_desc", source) for r in raw_rules]

    # ------------------------------------------------------------------ LLM path
    def _llm_available(self) -> bool:
        return bool(os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"))

    def _llm_rules(self, schema: Mapping[str, str], description: str, domain: str) -> list[dict[str, Any]]:
        prompt = _PROMPT_TEMPLATE.format(
            schema=json.dumps(dict(schema), indent=None), description=description, domain=domain or "general")
        try:
            from litellm import completion
        except ImportError:
            import litellm  # type: ignore
            completion = litellm.completion
        resp = completion(model=f"databricks/{self.model}", messages=[{"role": "user", "content": prompt}],
                          timeout=self.timeout)
        text = resp.choices[0].message.content or ""
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if not match:
            return []
        parsed = json.loads(match.group(0))
        return [r for r in parsed if isinstance(r, dict) and r.get("rule_type")]

    # ------------------------------------------------------------- heuristic path
    # (description pattern, column predicate (name_lower, type_lower)->bool, config, rule_type)
    _NUMERIC = ("int", "bigint", "long", "double", "float", "decimal", "smallint", "tinyint")
    _KEYWORDS = [
        (r"\be-?mail\b", lambda c, t: "mail" in c or "email" in c,
         {"pattern": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"}, RuleType.REGEX),
        (r"\bphone\b|\bmobile\b", lambda c, t: "phone" in c or "mobile" in c or "contact" in c,
         {"pattern": r"^\+?[0-9][0-9\s\-()]{6,}$"}, RuleType.REGEX),
        (r"\bpositive\b|\bnon-?negative\b|\bgreater than or equal", lambda c, t: t in LLMRuleGenerator._NUMERIC,
         {"min": 0}, RuleType.RANGE),
        (r"\bamount\b.{0,20}\b(positive|non-?negative)\b|\b(positive|non-?negative)\b.{0,20}\bamount\b",
         lambda c, t: "amount" in c or "price" in c or "cost" in c or t in LLMRuleGenerator._NUMERIC,
         {"min": 0}, RuleType.RANGE),
        (r"\bunique\b", lambda c, t: "id" in c or "key" in c or "uuid" in c or "code" in c,
         {}, RuleType.UNIQUE),
        (r"\brequired\b|\bnot null\b|\bmandatory\b",
         lambda c, t: any(w in c for w in ("id", "key", "code", "name", "mail", "phone")),
         {}, RuleType.NOT_NULL),
        (r"\bdate\b(?!-?time)|\bbirth\b|\bdob\b",
         lambda c, t: "date" in c or "_dt" in c or c.endswith("dt") or "dob" in c or "birth" in c,
         {"format": ["yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "MM/dd/yyyy"]}, RuleType.DATE_FORMAT),
        (r"\btimestamp\b|\bcreated\b|\bupdated\b",
         lambda c, t: "time" in c or "_at" in c or "created" in c or "updated" in c or t == "timestamp",
         {"format": ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"]}, RuleType.DATE_FORMAT),
        (r"\bfresh\b|\brecent\b|\btimely\b|\bstale\b",
         lambda c, t: "date" in c or "time" in c or "_at" in c or "created" in c or "updated" in c,
         {"max_age": "30 days"}, RuleType.FRESHNESS),
        (r"\bno nulls\b|\bcompleteness\b|\bnull free\b",
         lambda c, t: True, {"max_null_rate": 0.01}, RuleType.NULL_RATE),
    ]

    def _heuristic_rules(self, schema: Mapping[str, str], description: str) -> list[dict[str, Any]]:
        text = description.lower()
        rules: list[dict[str, Any]] = []
        for pattern, pred, cfg, rtype in self._KEYWORDS:
            if not re.search(pattern, text):
                continue
            targets = [c for c, t in schema.items() if pred(c.lower(), str(t).lower())][:3]
            for col_name in targets:
                rules.append({"name": f"{col_name}_{rtype.value.lower()}", "column": col_name,
                              "rule_type": rtype.value, "severity": "ERROR",
                              "config": dict(cfg),
                              "description": f"heuristic: '{pattern}' matched description"})
        return rules

    # ------------------------------------------------------------------ mapping
    def _to_rule(self, raw: Mapping[str, Any], dataset: str, ruleset_id: str, source: str) -> RuleDefinition:
        rtype_raw = str(raw.get("rule_type", "")).lower().replace("-", "_")
        rtype = _RULE_TYPE_ALIASES.get(rtype_raw)
        if rtype is None:
            for k, v in _RULE_TYPE_ALIASES.items():
                if rtype_raw == v.value.lower():
                    rtype = v
                    break
        if rtype is None:
            rtype = RuleType.NOT_NULL  # safe default — still requires review
        column = raw.get("column")
        config = dict(raw.get("config") or {})
        # normalize common LLM config shapes
        if rtype == RuleType.RANGE:
            config.setdefault("inclusive", True)
        try:
            severity = Severity(str(raw.get("severity", "ERROR")).upper())
        except ValueError:
            severity = Severity.ERROR
        scope = RuleScope.DATASET_LEVEL if rtype in (
            RuleType.ROW_COUNT, RuleType.NULL_RATE, RuleType.DISTINCT_RATIO,
            RuleType.FRESHNESS, RuleType.COLUMN_AGGREGATE, RuleType.SCHEMA_VALIDATION) else None
        idx = abs(hash((raw.get("name"), column))) % 9000 + 100
        return RuleDefinition(
            rule_id=f"{ruleset_id[:12].upper()}_{idx}", name=str(raw.get("name") or f"llm_{rtype.value.lower()}"),
            description=str(raw.get("description") or f"LLM-generated {rtype.value} rule"),
            dataset=dataset, column=str(column) if column else None, rule_type=rtype,
            severity=severity, config=config, scope=scope, status=RuleStatus.CANDIDATE,
            version="1.0",
            generation={"source": source, "evidence": {"llm_model": self.model} if source == "llm_description"
                        else {"heuristic": True}, "confidence": 0.75 if source == "llm_description" else 0.6,
                        "profiled_at": utc_now_iso()})
