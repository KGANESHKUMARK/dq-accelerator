"""Unit tests for rule metadata models (pure Python, no Spark)."""

from __future__ import annotations

import pytest

from dq_core.metadata.models import (
    ALL_COLUMNS, DeterministicAction, RemediationPolicy, RemediationStep,
    RuleDefinition, RuleSet, RuleSetStatus, RuleStatus, RuleType, Severity,
)
from dq_core.runtime.errors import ImmutableVersionError, RuleDefinitionError


# ------------------------------------------------------------------ RuleDefinition
class TestRuleDefinition:
    def test_valid_not_null(self):
        r = RuleDefinition(rule_id="R.001", name="n", rule_type=RuleType.NOT_NULL,
                           dataset="d", column="c", status=RuleStatus.APPROVED)
        assert r.rule_id == "R.001"
        assert r.is_row_level
        assert r.level == "ROW"

    def test_invalid_rule_id(self):
        with pytest.raises(RuleDefinitionError, match="rule_id"):
            RuleDefinition(rule_id="1bad", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c")

    def test_regex_requires_pattern(self):
        with pytest.raises(RuleDefinitionError, match="pattern"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.REGEX, dataset="d", column="c", config={})

    def test_regex_invalid_pattern(self):
        with pytest.raises(RuleDefinitionError, match="invalid regex"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.REGEX, dataset="d", column="c",
                           config={"pattern": "["})

    def test_range_requires_bound(self):
        with pytest.raises(RuleDefinitionError, match="min"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.RANGE, dataset="d", column="c", config={})

    def test_allowed_values_requires_list(self):
        with pytest.raises(RuleDefinitionError, match="values"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.ALLOWED_VALUES, dataset="d", column="c",
                           config={"values": []})

    def test_duplicate_requires_keys(self):
        with pytest.raises(RuleDefinitionError, match="keys"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.DUPLICATE, dataset="d", config={})

    def test_schema_validation_no_column(self):
        r = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.SCHEMA_VALIDATION, dataset="d",
                           config={"expected_columns": [{"name": "id"}]})
        assert not r.is_row_level

    def test_null_replacement_requires_approved(self):
        with pytest.raises(RuleDefinitionError, match="approved"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c",
                           remediation=RemediationPolicy(deterministic=(
                               RemediationStep(action=DeterministicAction.NULL_REPLACEMENT,
                                               params={"value": "X"}),)))

    def test_deduplicate_requires_survivor_policy(self):
        with pytest.raises(RuleDefinitionError, match="survivor"):
            RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.DUPLICATE, dataset="d",
                           config={"keys": ALL_COLUMNS},
                           remediation=RemediationPolicy(deterministic=(
                               RemediationStep(action=DeterministicAction.DEDUPLICATE, params={}),)))

    def test_from_dict_roundtrip(self):
        r = RuleDefinition(rule_id="R.OK", name="test", rule_type=RuleType.UNIQUE,
                           dataset="ds", column="id", severity=Severity.WARNING)
        r2 = RuleDefinition.from_dict(r.to_dict())
        assert r == r2


# ------------------------------------------------------------------ RuleSet
class TestRuleSet:
    def _simple_ruleset(self, version="1.0", status=RuleSetStatus.DRAFT) -> RuleSet:
        return RuleSet(ruleset_id="test_ruleset", dataset="d", version=version, status=status,
                       rules=(RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL,
                                            dataset="d", column="c"),))

    def test_basic(self):
        rs = self._simple_ruleset()
        assert rs.ref == "test_ruleset@1.0"
        assert rs.checksum()

    def test_duplicate_rule_ids_rejected(self):
        rule = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c")
        with pytest.raises(RuleDefinitionError, match="duplicate rule_id"):
            RuleSet(ruleset_id="test_rs2", dataset="d", version="1.0", rules=(rule, rule))

    def test_cannot_publish_candidate_rules(self):
        rule = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d",
                              column="c", status=RuleStatus.CANDIDATE)
        with pytest.raises(RuleDefinitionError, match="CANDIDATE"):
            RuleSet(ruleset_id="test_rs3", dataset="d", version="1.0",
                    status=RuleSetStatus.PUBLISHED, rules=(rule,))

    def test_derive_new_version(self):
        v1 = self._simple_ruleset("1.0")
        v1_1 = v1.derive("1.1", modify={"R1": {"severity": "WARNING"}}, change_log=["bump severity"])
        assert v1_1.version == "1.1"
        assert v1_1.parent_version == "1.0"
        assert v1_1.get_rule("R1").severity.value == "WARNING"
        assert v1.get_rule("R1").severity.value == "ERROR"   # original unchanged

    def test_derive_requires_higher_version(self):
        v1 = self._simple_ruleset("1.0")
        with pytest.raises(RuleDefinitionError, match="greater"):
            v1.derive("0.9")

    def test_checksum_deterministic(self):
        v1a = self._simple_ruleset()
        v1b = self._simple_ruleset()
        assert v1a.checksum() == v1b.checksum()

    def test_checksum_changes_with_content(self):
        v1 = self._simple_ruleset("1.0")
        v1_1 = v1.derive("1.1", modify={"R1": {"severity": "WARNING"}})
        assert v1.checksum() != v1_1.checksum()


# ------------------------------------------------------------------ Registry
class TestRuleSetRegistry:
    def test_publish_and_get(self, tmp_path):
        from dq_core.rules.registry import RuleSetRegistry

        rule = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c")
        rs = RuleSet(ruleset_id="test_reg_rs", dataset="d", version="1.0", rules=(rule,))
        reg = RuleSetRegistry(tmp_path / "reg")
        entry = reg.publish(rs, "test_user")
        assert entry["checksum"] == rs.checksum()
        loaded = reg.get("test_reg_rs", "1.0")
        assert loaded.version == "1.0"
        assert loaded.checksum() == rs.checksum()

    def test_immutable_version_error(self, tmp_path):
        from dq_core.rules.registry import RuleSetRegistry
        from dataclasses import replace

        rule = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c")
        rs = RuleSet(ruleset_id="test_immut_rs", dataset="d", version="1.0", rules=(rule,))
        reg = RuleSetRegistry(tmp_path / "reg2")
        reg.publish(rs, "u")
        # Build a different rule set with the SAME version 1.0 (simulating a file edit after publication)
        rule2 = RuleDefinition(rule_id="R2", name="n2", rule_type=RuleType.UNIQUE, dataset="d", column="c")
        rs_modified = RuleSet(ruleset_id="test_immut_rs", dataset="d", version="1.0", rules=(rule, rule2))
        assert rs_modified.checksum() != rs.checksum(), "checksums must differ for this test to be meaningful"
        with pytest.raises(ImmutableVersionError):
            reg.publish(rs_modified, "u")

    def test_rollback(self, tmp_path):
        from dq_core.rules.registry import RuleSetRegistry

        rule = RuleDefinition(rule_id="R1", name="n", rule_type=RuleType.NOT_NULL, dataset="d", column="c")
        rs1 = RuleSet(ruleset_id="test_rb_rs", dataset="d", version="1.0", rules=(rule,))
        rs2 = rs1.derive("1.1", add=[RuleDefinition(rule_id="R2", name="n2", rule_type=RuleType.UNIQUE,
                                                    dataset="d", column="c")])
        reg = RuleSetRegistry(tmp_path / "reg3")
        reg.publish(rs1, "u")
        reg.publish(rs2, "u", activate=True)
        assert reg.active_version("test_rb_rs") == "1.1"
        reg.rollback("test_rb_rs", "1.0", reason="test")
        assert reg.active_version("test_rb_rs") == "1.0"
