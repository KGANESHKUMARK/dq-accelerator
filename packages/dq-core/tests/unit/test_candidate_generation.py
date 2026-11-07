"""Unit tests: candidate rule generation from profiling evidence (no Spark needed)."""

from __future__ import annotations

import pytest

from dq_core.context import Stage
from dq_core.generation.generator import CandidateRuleGenerator, approve_candidates
from dq_core.metadata.models import RuleSetStatus, RuleStatus, RuleType
from dq_core.profiling.models import ColumnProfile, ProfileReport
from dq_core.runtime.errors import RuleDefinitionError


def _col(name, *, inferred="text", dtype="string", nulls=0, rows=100, **kw) -> ColumnProfile:
    base = dict(name=name, data_type=dtype, inferred_type=inferred,
                non_null_count=rows - nulls, null_count=nulls,
                null_percentage=round(100 * nulls / rows, 2),
                distinct_count=min(rows - nulls, kw.pop("distinct", rows - nulls)),
                distinct_is_approximate=False)
    base.update(kw)
    return ColumnProfile(**base)


def _report(columns: dict[str, ColumnProfile], *, rows=100, dups=0, dup_groups=0,
            pk=(), near=(), dataset="t_dataset") -> ProfileReport:
    return ProfileReport(
        dataset=dataset, profiled_at="2026-01-01T00:00:00Z", duration_ms=1.0, mode="FULL",
        sampled=False, sample_fraction=None, approximate=False, row_count=rows,
        source_row_count=rows, column_count=len(columns),
        schema=[{"name": n, "type": c.data_type} for n, c in columns.items()],
        columns=columns, duplicate_rows=dups, duplicate_groups=dup_groups,
        primary_key_candidates=list(pk), near_key_candidates=list(near),
        anomalies=[], schema_anomalies=[])


class TestCandidateGeneration:
    def test_not_null_candidate_for_clean_column(self):
        rep = _report({"id": _col("id", inferred="identifier")})
        rs = CandidateRuleGenerator().generate(rep)
        not_null = [r for r in rs.rules if r.rule_type == RuleType.NOT_NULL and r.column == "id"]
        assert len(not_null) == 1
        assert not_null[0].status == RuleStatus.CANDIDATE
        assert not_null[0].generation["confidence"] >= 0.9

    def test_no_not_null_candidate_when_mostly_null(self):
        rep = _report({"x": _col("x", nulls=50)})   # 50% nulls > 5% threshold
        rs = CandidateRuleGenerator().generate(rep)
        assert not [r for r in rs.rules if r.rule_type == RuleType.NOT_NULL and r.column == "x"]

    def test_unique_candidate_for_primary_key(self):
        rep = _report({"id": _col("id", inferred="identifier", distinct=100)}, pk=["id"])
        rs = CandidateRuleGenerator().generate(rep)
        uniq = [r for r in rs.rules if r.rule_type == RuleType.UNIQUE and r.column == "id"]
        assert len(uniq) == 1 and uniq[0].generation["confidence"] == 0.95

    def test_email_regex_candidate(self):
        rep = _report({"email": _col("email", inferred="email", email_match_percentage=97.0)})
        rs = CandidateRuleGenerator().generate(rep)
        regex = [r for r in rs.rules if r.rule_type == RuleType.REGEX and r.column == "email"]
        assert len(regex) == 1
        assert "@" in regex[0].config["pattern"]

    def test_date_format_candidate(self):
        rep = _report({"d": _col("d", inferred="date (stored as string)",
                               date_format_matches={"yyyy-MM-dd": 98.0},
                               dominant_date_format="yyyy-MM-dd")})
        rs = CandidateRuleGenerator().generate(rep)
        df = [r for r in rs.rules if r.rule_type == RuleType.DATE_FORMAT]
        assert len(df) == 1 and df[0].config["format"] == "yyyy-MM-dd"

    def test_range_candidate_for_numeric(self):
        rep = _report({"amt": _col("amt", inferred="integer", dtype="bigint",
                                  min=1, max=500, percentiles={"p25": 100.0, "p75": 300.0})})
        rs = CandidateRuleGenerator().generate(rep)
        rng = [r for r in rs.rules if r.rule_type == RuleType.RANGE and r.column == "amt"]
        assert len(rng) == 1
        assert rng[0].config["min"] <= rng[0].config["max"]

    def test_allowed_values_candidate_for_categorical(self):
        top = [{"value": "A", "count": 40}, {"value": "B", "count": 35}, {"value": "C", "count": 25}]
        rep = _report({"status": _col("status", inferred="categorical",
                                     top_values=top, candidate_allowed_values=["A", "B", "C"])})
        rs = CandidateRuleGenerator().generate(rep)
        av = [r for r in rs.rules if r.rule_type == RuleType.ALLOWED_VALUES]
        assert len(av) == 1 and set(av[0].config["values"]) == {"A", "B", "C"}

    def test_string_length_candidate_is_warning(self):
        rep = _report({"name": _col("name", inferred="text", min_length=3, max_length=30, avg_length=12.0)})
        rs = CandidateRuleGenerator().generate(rep)
        sl = [r for r in rs.rules if r.rule_type == RuleType.STRING_LENGTH]
        assert len(sl) == 1 and sl[0].severity.value == "WARNING"

    def test_duplicate_candidate_when_duplicates_found(self):
        rep = _report({"a": _col("a")}, dups=5, dup_groups=2)
        rs = CandidateRuleGenerator().generate(rep)
        dup = [r for r in rs.rules if r.rule_type == RuleType.DUPLICATE]
        assert len(dup) == 1 and dup[0].config["keys"]

    def test_no_duplicate_candidate_when_clean(self):
        rep = _report({"a": _col("a")}, dups=0)
        rs = CandidateRuleGenerator().generate(rep)
        assert not [r for r in rs.rules if r.rule_type == RuleType.DUPLICATE]

    def test_referential_integrity_candidate(self):
        rep = _report({"customer_id": _col("customer_id", inferred="identifier")})
        other = _report({"customer_id": _col("customer_id", inferred="identifier", distinct=100)},
                        pk=["customer_id"], dataset="customers")
        rs = CandidateRuleGenerator().generate(rep, related_profiles={"customers": other})
        ri = [r for r in rs.rules if r.rule_type == RuleType.REFERENTIAL_INTEGRITY]
        assert len(ri) == 1
        assert ri[0].config["reference_dataset"] == "customers"

    def test_schema_validation_always_generated(self):
        rep = _report({"a": _col("a")})
        rs = CandidateRuleGenerator().generate(rep)
        sv = [r for r in rs.rules if r.rule_type == RuleType.SCHEMA_VALIDATION]
        assert len(sv) == 1
        assert sv[0].config["expected_columns"][0]["name"] == "a"

    def test_all_candidates_have_evidence_and_confidence(self):
        rep = _report({"id": _col("id", inferred="identifier", distinct=100),
                       "email": _col("email", inferred="email", email_match_percentage=99.0)},
                      pk=["id"])
        rs = CandidateRuleGenerator().generate(rep)
        assert rs.rules, "expected at least one candidate"
        for r in rs.rules:
            assert r.status == RuleStatus.CANDIDATE
            assert 0.0 <= r.generation["confidence"] <= 1.0
            assert r.generation["evidence"], f"{r.rule_id} missing evidence"
            assert r.generation["generator"].startswith("dq-core-generator")

    def test_candidate_ruleset_is_draft_and_never_published(self):
        rep = _report({"a": _col("a")})
        rs = CandidateRuleGenerator().generate(rep)
        assert rs.status == RuleSetStatus.DRAFT
        with pytest.raises(RuleDefinitionError):
            # Simulate publishing candidates directly – must be blocked by model validation
            RuleSetStatus.PUBLISHED  # noqa: B018 - documentation of intent
            rs_published = type(rs)(ruleset_id=rs.ruleset_id, dataset=rs.dataset, version=rs.version,
                                    rules=rs.rules, stage=rs.stage, status=RuleSetStatus.PUBLISHED,
                                    lookups=rs.lookups)
            assert rs_published  # unreachable if validation fires


class TestApproveCandidates:
    def _candidates(self):
        rep = _report({"id": _col("id", inferred="identifier", distinct=100),
                       "email": _col("email", inferred="email", email_match_percentage=99.0)},
                      pk=["id"])
        return CandidateRuleGenerator().generate(rep)

    def test_approve_selected_rules(self):
        cands = self._candidates()
        ids = [r.rule_id for r in cands.rules][:2]
        approved = approve_candidates(cands, ids, ruleset_id="t_silver", version="1.0",
                                      approved_by="steward")
        assert len(approved.rules) == 2
        assert all(r.status == RuleStatus.APPROVED for r in approved.rules)
        assert approved.status == RuleSetStatus.DRAFT
        assert approved.approved_by == "steward"

    def test_reject_others_drops_unselected(self):
        cands = self._candidates()
        one = cands.rules[0].rule_id
        approved = approve_candidates(cands, [one], ruleset_id="t_silver", version="1.0",
                                      approved_by="s")
        assert len(approved.rules) == 1 and approved.rules[0].rule_id == one

    def test_unknown_rule_id_rejected(self):
        cands = self._candidates()
        with pytest.raises(RuleDefinitionError, match="Unknown candidate"):
            approve_candidates(cands, ["GEN_NOPE_999"], ruleset_id="test_unknown", version="1.0", approved_by="s")

    def test_override_applies_config(self):
        cands = self._candidates()
        target = next(r for r in cands.rules if r.rule_type == RuleType.STRING_LENGTH
                      or r.config.get("min") is not None) if any(
            r.rule_type == RuleType.STRING_LENGTH for r in cands.rules) else cands.rules[0]
        approved = approve_candidates(cands, [target.rule_id], ruleset_id="test_override", version="1.0",
                                      approved_by="s",
                                      overrides={target.rule_id: {"severity": "WARNING"}})
        assert approved.rules[0].severity.value == "WARNING"

    def test_stage_passthrough(self):
        rep = _report({"a": _col("a")})
        rs = CandidateRuleGenerator().generate(rep, stage=Stage.GOLD)
        assert rs.stage == Stage.GOLD
        assert all(r.stage == Stage.GOLD for r in rs.rules)
