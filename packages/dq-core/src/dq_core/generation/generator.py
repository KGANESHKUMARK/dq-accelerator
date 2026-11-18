"""Candidate rule generation from profiling evidence.

Every generated rule has status CANDIDATE plus ``generation`` metadata (confidence, evidence).
CANDIDATE rules are never executed: a data steward must review, adjust and approve them
(``approve_candidates``) and the resulting rule set must be published before ENFORCE/REMEDIATE.
"""

from __future__ import annotations

import math
from typing import Any, Mapping

from dq_core.context import Stage
from dq_core.metadata.models import ALL_COLUMNS, RuleDefinition, RuleSet, RuleSetStatus, RuleStatus, RuleType
from dq_core.profiling.models import ColumnProfile, ProfileReport
from dq_core.profiling.patterns import EMAIL_REGEX, pattern_to_regex
from dq_core.runtime.errors import RuleDefinitionError
from dq_core.runtime.utils import utc_now_iso
from dq_core.version import __version__

GENERATOR_VERSION = f"dq-core-generator-{__version__}"


def _numeric_inferred(p: ColumnProfile) -> bool:
    return p.inferred_type.startswith(("integer", "decimal")) or p.data_type in (
        "tinyint", "smallint", "int", "bigint", "float", "double") or p.data_type.startswith("decimal")


class CandidateRuleGenerator:
    def __init__(self, max_allowed_values: int = 20, pattern_share: float = 0.9) -> None:
        self.max_allowed_values = max_allowed_values
        self.pattern_share = pattern_share

    def generate(self, profile: ProfileReport, *, ruleset_id: str | None = None, version: str = "0.1",
                 stage: Stage | str = Stage.SILVER, related_profiles: Mapping[str, ProfileReport] | None = None,
                 created_by: str = GENERATOR_VERSION) -> RuleSet:
        stage = Stage.parse(stage)
        dataset = profile.dataset
        proposals: list[dict[str, Any]] = []

        def propose(rule_type: RuleType, column: str | None, confidence: float, evidence: dict[str, Any],
                    config: dict[str, Any] | None = None, severity: str = "ERROR", description: str = "") -> None:
            proposals.append({"rule_type": rule_type.value, "column": column, "config": config or {}, "severity": severity,
                              "description": description, "confidence": round(min(max(confidence, 0.0), 1.0), 4),
                              "evidence": evidence})

        rows = profile.row_count
        for name, p in profile.columns.items():
            # NOT_NULL ---------------------------------------------------------------------------
            if rows and p.null_percentage <= 5.0 and p.inferred_type != "empty":
                propose(RuleType.NOT_NULL, name, 0.95 if p.null_count == 0 else 0.75,
                        {"null_count": p.null_count, "null_percentage": p.null_percentage, "rows": rows},
                        description=f"{name} is populated in {100 - p.null_percentage:.1f}% of rows")
            # UNIQUE -----------------------------------------------------------------------------
            near = {n["column"]: n for n in profile.near_key_candidates}
            if name in profile.primary_key_candidates:
                propose(RuleType.UNIQUE, name, 0.95, {"distinct_count": p.distinct_count, "rows": rows},
                        description=f"{name} is unique in the profiled data (primary key candidate)")
            elif name in near and name.lower().endswith("_id"):
                propose(RuleType.UNIQUE, name, 0.8, {"unique_ratio": near[name]["unique_ratio"],
                                                     "duplicate_value_count": near[name]["duplicate_value_count"]},
                        description=f"{name} looks like an identifier ({near[name]['unique_ratio']:.1%} unique)")
            # DATA_TYPE / DATE_FORMAT / REGEX(email) ---------------------------------------------
            if p.data_type == "string" and p.inferred_type == "integer (stored as string)":
                propose(RuleType.DATA_TYPE, name, (p.integer_castable_percentage or 0) / 100,
                        {"integer_castable_percentage": p.integer_castable_percentage}, {"data_type": "bigint"},
                        description=f"{name} holds whole numbers stored as text")
            elif p.data_type == "string" and p.inferred_type == "decimal (stored as string)":
                propose(RuleType.DATA_TYPE, name, (p.numeric_castable_percentage or 0) / 100,
                        {"numeric_castable_percentage": p.numeric_castable_percentage}, {"data_type": "decimal(18,2)"},
                        description=f"{name} holds decimal numbers stored as text")
            if p.inferred_type == "date (stored as string)" and p.dominant_date_format:
                propose(RuleType.DATE_FORMAT, name, p.date_format_matches[p.dominant_date_format] / 100,
                        {"date_format_matches": p.date_format_matches, "date_min": p.date_min, "date_max": p.date_max},
                        {"format": p.dominant_date_format}, description=f"{name} is a date in {p.dominant_date_format}")
            if p.inferred_type == "email":
                propose(RuleType.REGEX, name, (p.email_match_percentage or 0) / 100,
                        {"email_match_percentage": p.email_match_percentage}, {"pattern": EMAIL_REGEX},
                        description=f"{name} contains e-mail addresses")
            # REGEX (structural pattern) ---------------------------------------------------------
            elif (p.top_patterns and p.inferred_type in ("text",) and p.non_null_count
                  and p.top_patterns[0]["count"] >= self.pattern_share * p.non_null_count
                  and len(p.top_patterns[0]["pattern"]) <= 20 and any(ch in p.top_patterns[0]["pattern"] for ch in "9")):
                top = p.top_patterns[0]
                propose(RuleType.REGEX, name, top["count"] / p.non_null_count,
                        {"dominant_pattern": top["pattern"], "share": round(top["count"] / p.non_null_count, 4),
                         "other_patterns": p.top_patterns[1:]}, {"pattern": pattern_to_regex(top["pattern"])},
                        description=f"{name} follows the structural pattern {top['pattern']}")
            # RANGE ------------------------------------------------------------------------------
            if _numeric_inferred(p) and p.percentiles.get("p25") is not None and p.min is not None:
                q1, q3 = p.percentiles["p25"], p.percentiles["p75"]
                iqr = q3 - q1
                lower = max(float(p.min), q1 - 1.5 * iqr)
                upper = min(float(p.max), q3 + 1.5 * iqr)
                integer = p.inferred_type.startswith("integer") or p.data_type in ("int", "bigint", "smallint", "tinyint")
                lower, upper = (math.floor(lower), math.ceil(upper)) if integer else (round(lower, 2), round(upper, 2))
                if integer:
                    dtype = "int" if max(abs(lower), abs(upper)) <= 2_147_483_647 else "bigint"
                else:
                    dtype = "decimal(18,2)"
                propose(RuleType.RANGE, name, 0.6,
                        {"observed_min": p.min, "observed_max": p.max, "p25": q1, "p75": q3, "outliers": p.outlier_count,
                         "method": "IQR fences clipped to observed range - business review required"},
                        {"min": lower, "max": upper, "data_type": dtype},
                        description=f"{name} typically between {lower} and {upper}")
            # ALLOWED_VALUES ---------------------------------------------------------------------
            if p.candidate_allowed_values and p.inferred_type == "categorical":
                counts = {t["value"]: t["count"] for t in p.top_values}
                frequent = max(counts.values()) if counts else 0
                rare = [v for v, c in counts.items() if c == 1 and frequent >= 3]
                allowed = [v for v in p.candidate_allowed_values if v not in rare]
                if allowed:
                    propose(RuleType.ALLOWED_VALUES, name, 0.85 if not rare else 0.7,
                            {"value_counts": counts, "excluded_rare_values": rare}, {"values": allowed},
                            description=f"{name} has {len(allowed)} frequent categorical values")
            # STRING_LENGTH ----------------------------------------------------------------------
            if p.inferred_type == "text" and p.max_length:
                propose(RuleType.STRING_LENGTH, name, 0.6, {"min_length": p.min_length, "max_length": p.max_length,
                                                             "avg_length": p.avg_length},
                        {"min": max(1, int(p.min_length or 1)), "max": int(p.max_length)},
                        severity="WARNING", description=f"{name} length observed between {p.min_length} and {p.max_length}")
        # DUPLICATE -------------------------------------------------------------------------------
        if profile.duplicate_rows:
            propose(RuleType.DUPLICATE, None, 0.9, {"duplicate_rows": profile.duplicate_rows,
                                                    "duplicate_groups": profile.duplicate_groups},
                    {"keys": ALL_COLUMNS}, description="exact duplicate records were found")
        # REFERENTIAL_INTEGRITY ---------------------------------------------------------------------
        for other_name, other in (related_profiles or {}).items():
            keys = set(other.primary_key_candidates) | {n["column"] for n in other.near_key_candidates}
            for name in profile.columns:
                if name.lower().endswith("_id") and name in keys and name not in profile.primary_key_candidates:
                    propose(RuleType.REFERENTIAL_INTEGRITY, name, 0.8,
                            {"reference_dataset": other_name, "reference_key_candidate": name},
                            {"reference_dataset": other_name, "reference_column": name},
                            description=f"{name} appears to reference {other_name}.{name}")
        # SCHEMA_VALIDATION -------------------------------------------------------------------------
        propose(RuleType.SCHEMA_VALIDATION, None, 0.9, {"column_count": profile.column_count},
                {"expected_columns": [{"name": c["name"], "type": c["type"]} for c in profile.schema],
                 "allow_extra_columns": False}, description="schema observed at profiling time")

        rules = []
        for i, proposal in enumerate(proposals, start=1):
            rule_type = RuleType(proposal["rule_type"])
            column = proposal["column"]
            rules.append(RuleDefinition(
                rule_id=f"GEN_{dataset.upper()}_{i:03d}", name=f"{(column or dataset)}_{rule_type.value.lower()}",
                rule_type=rule_type, dataset=dataset, column=column, description=proposal["description"],
                severity=proposal["severity"], stage=stage, config=proposal["config"], version=version,
                status=RuleStatus.CANDIDATE,
                generation={"generator": GENERATOR_VERSION, "confidence": proposal["confidence"],
                            "evidence": proposal["evidence"], "profiled_at": profile.profiled_at,
                            "profile_mode": profile.mode}))
        return RuleSet(ruleset_id=ruleset_id or f"{dataset}_{stage.value.lower()}_candidates", dataset=dataset,
                       version=version, stage=stage, status=RuleSetStatus.DRAFT, rules=tuple(rules),
                       description=f"CANDIDATE rules generated from profiling evidence for {dataset} - review required",
                       record_key=tuple(profile.primary_key_candidates[:1]), created_by=created_by,
                       created_at=utc_now_iso(), change_log=("generated candidates - NOT APPROVED",))


def approve_candidates(candidates: RuleSet, rule_ids: list[str], *, ruleset_id: str, version: str, approved_by: str,
                       overrides: Mapping[str, Mapping[str, Any]] | None = None, reject_others: bool = True) -> RuleSet:
    """Steward review: approve selected candidates (optionally adjusted) into a new DRAFT rule set."""
    overrides = dict(overrides or {})
    unknown = (set(rule_ids) | set(overrides)) - {r.rule_id for r in candidates.rules}
    if unknown:
        raise RuleDefinitionError(f"Unknown candidate rule ids: {sorted(unknown)}")
    rules = []
    for rule in candidates.rules:
        if rule.rule_id not in rule_ids:
            continue
        data = rule.to_dict()
        for key, value in overrides.get(rule.rule_id, {}).items():
            data[key] = {**data.get("config", {}), **value} if key == "config" else value
        data.update({"status": RuleStatus.APPROVED.value, "version": version})
        rules.append(RuleDefinition.from_dict(data))
    return RuleSet(ruleset_id=ruleset_id, dataset=candidates.dataset, version=version, stage=candidates.stage,
                   status=RuleSetStatus.DRAFT, rules=tuple(rules), record_key=candidates.record_key,
                   description=f"Approved from candidates {candidates.ref}", approved_by=approved_by,
                   approved_at=utc_now_iso(), created_by=candidates.created_by,
                   change_log=(f"approved {len(rules)} of {len(candidates.rules)} candidate rules",))
