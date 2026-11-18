"""Unit tests for Tier-1/2 additions: ODCS import, LLM/heuristic rule gen, dataset rule types."""

from __future__ import annotations

import pytest

from dq_core.contracts.odcs import odcs_to_ruleset, write_ruleset_yaml
from dq_core.generation.llm_generator import LLMRuleGenerator
from dq_core.metadata.models import RuleStatus, RuleType
from dq_core.runtime.errors import RuleDefinitionError
from dq_core.validation.engine import _parse_age_seconds


# ------------------------------------------------------------------ ODCS

def _contract() -> dict:
    return {
        "apiVersion": "v3", "kind": "DataContract", "dataset": "customers",
        "schema": [
            {"name": "customer_id", "type": "string", "required": True, "unique": True,
             "pattern": "^CUST-[0-9]{6}$"},
            {"name": "email", "type": "string", "required": True, "format": "email", "pii": True},
            {"name": "age", "type": "integer", "minimum": 0, "maximum": 120},
            {"name": "segment", "type": "string", "enum": ["RETAIL", "BUSINESS"]},
        ],
        "quality": [{"type": "rowCount", "arguments": {"min": 10, "max": 1000000}}],
    }


def test_odcs_produces_candidate_rules() -> None:
    rs = odcs_to_ruleset(_contract(), ruleset_id="cust_odcs")
    assert rs.dataset == "customers"
    types = {r.rule_type for r in rs.rules}
    assert RuleType.NOT_NULL in types and RuleType.UNIQUE in types
    assert RuleType.REGEX in types and RuleType.ALLOWED_VALUES in types
    assert RuleType.RANGE in types and RuleType.ROW_COUNT in types
    assert all(r.status == RuleStatus.CANDIDATE for r in rs.rules)  # never executable pre-review
    assert all(r.generation and r.generation.get("source") == "odcs_contract" for r in rs.rules)


def test_odcs_rejects_empty_contract() -> None:
    with pytest.raises(RuleDefinitionError):
        odcs_to_ruleset({"dataset": "x", "schema": []})


def test_odcs_yaml_roundtrip(tmp_path) -> None:
    rs = odcs_to_ruleset(_contract())
    out = write_ruleset_yaml(rs, tmp_path / "rs.yaml")
    assert out.exists() and "CANDIDATE" in out.read_text()


# ------------------------------------------------------------------ heuristic rule gen

def test_heuristic_targets_right_columns() -> None:
    schema = {"customer_id": "string", "email": "string", "age": "int", "segment": "string"}
    rules = LLMRuleGenerator().generate(schema, "email must be valid, age positive, customer_id unique",
                                        dataset="customers", use_llm=False)
    by_name = {r.name: r for r in rules}
    assert by_name["email_regex"].column == "email"
    assert by_name["age_range"].column == "age" and by_name["age_range"].config["min"] == 0
    assert by_name["customer_id_unique"].column == "customer_id"
    assert all(r.status == RuleStatus.CANDIDATE for r in rules)


def test_generate_no_match_returns_empty() -> None:
    rules = LLMRuleGenerator().generate({"x": "int"}, "zzz qqq unrelated", dataset="d", use_llm=False)
    assert rules == []


# ------------------------------------------------------------------ dataset rule helpers

def test_parse_age_seconds() -> None:
    assert _parse_age_seconds(3600) == 3600.0
    assert _parse_age_seconds("30 days") == 30 * 86400
    assert _parse_age_seconds("12 hours") == 12 * 3600
    assert _parse_age_seconds("2 weeks") == 2 * 604800
    assert _parse_age_seconds("30d") == 30 * 86400
    assert _parse_age_seconds("12h") == 12 * 3600
    with pytest.raises(ValueError):
        _parse_age_seconds("bogus")
    with pytest.raises(ValueError):
        _parse_age_seconds(None)
