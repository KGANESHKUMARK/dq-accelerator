"""Post-remediation revalidation.

Corrected records are re-run through the *same* rule set version (together with untouched
records, so uniqueness / duplicate rules see the final dataset). A correction that does not pass
revalidation is quarantined with status CORRECTION_FAILED_REVALIDATION.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_core.metadata.models import RuleSet
from dq_core.validation import columns as C
from dq_core.validation.engine import ValidationEngine, ValidationOutcome


@dataclass
class RevalidationOutcome:
    validation: ValidationOutcome
    stats: dict[str, int] = field(default_factory=dict)


class Revalidator:
    def __init__(self, engine: ValidationEngine) -> None:
        self.engine = engine

    def revalidate(self, df: DataFrame, ruleset: RuleSet, reference_data: Mapping[str, DataFrame] | None = None) -> RevalidationOutcome:
        outcome = self.engine.validate(C.ensure_remediation_columns(C.strip_annotations(df)), ruleset,
                                       reference_data=reference_data)
        corrected = F.size(F.col(C.FIXES)) > 0
        ok = F.col(C.ERROR_COUNT) == 0
        row = outcome.annotated.agg(
            F.sum((corrected & ok).cast("int")).alias("corrected_passed"),
            F.sum((corrected & ~ok).cast("int")).alias("corrected_failed"),
            F.sum(F.size(F.filter(F.col(C.FIXES), lambda f: f["source"] == F.lit("DETERMINISTIC")))).alias("deterministic_fixes"),
            F.sum(F.size(F.filter(F.col(C.FIXES), lambda f: f["source"] != F.lit("DETERMINISTIC")))).alias("ai_fixes"),
        ).collect()[0].asDict()
        stats: dict[str, Any] = {k: int(v or 0) for k, v in row.items()}
        return RevalidationOutcome(validation=outcome, stats=stats)
